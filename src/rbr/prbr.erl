% @copyright 2012-2014 Zuse Institute Berlin,

%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.

%% @author Florian Schintke <schintke@zib.de>
%% @doc    Generic paxos round based register (prbr) implementation.
%%         The read/write store alias acceptor.
%% @end
%% @version $Id$
-module(prbr).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").

-define(PDB, db_prbr).

%%% the prbr has to be embedded into a gen_component using it.
%%% The state it operates on has to be passed to the on handler
%%% correctly. All messages it handles start with the token
%%% prbr to support a generic, embeddable on-handler trigger.

%%% functions for module where embedded into
-export([on/2, init/1]).
-export([check_config/0]).
-export([noop_read_filter/1]).  %% See rbrcseq for explanation.
-export([noop_write_filter/3]). %% See rbrcseq for explanation.
-export([new/2]).
-export([set_entry/2]).
-export([tester_create_write_filter/1]).

%% let fetch the number of DB entries
-export([get_load/1]).

%% only for unittests
-export([tab2list_raw_unittest/1]).

-ifdef(with_export_type_support).
-export_type([message/0]).
-export_type([state/0]).
-export_type([read_filter/0]).
-export_type([write_filter/0]).
-endif.

%% read_filter(custom_data() | no_value_yet) -> read_info()
-type read_filter() :: fun((term()) -> term()).

%% write_filter(OldLocalDBentry :: custom_data(),
%%              InfosToUpdateOutdatedEntry :: info_passed_from_read_to_write(),
%%              ValueForWriteOperation:: Value())
%% -> custom_data()
-type write_filter() :: fun((term(), term(), term()) -> term()).

-type state() :: ?PDB:db().

-type message() ::
        {prbr, read, DB :: dht_node_state:db_selector(),
         WasConsistentLookup :: boolean(),
         Proposer :: comm:mypid(), ?RT:key(), InRound,
         read_filter()}
      | {prbr, write, DB :: dht_node_state:db_selector(),
         WasConsistentLookup :: boolean(),
         Proposer :: comm:mypid(), ?RT:key(), InRound,
         Value :: term(), PassedToUpdate :: term(), write_filter()}.


%% improvements to usual paxos:

%% for reads the client has just to send a unique identifier and the
%% acceptor provides a valid round number. The actual round number of
%% the request is then the tuple {round_number, unique_id}.

%% A proposer then may receive answers with different round numbers
%% and selects that one with the highest round, where he also takes
%% the value from.

%% on read the acceptor can assign the next round number. They remain
%% unique as we get the node_id in the read request and that is part of
%% the round number.

%% so there are no longer any read denies, all reads succeed.
-type entry() :: { any(), %% key
                   pr:pr(), %% r_read
                   pr:pr(), %% r_write
                   any()    %% value
                 }.

%% Messages to expect from this module
-spec msg_read_reply(comm:mypid(), Consistency::boolean(),
                     pr:pr(), any(), pr:pr())
             -> ok.
msg_read_reply(Client, Cons, YourRound, Val, LastWriteRound) ->
    comm:send(Client, {read_reply, Cons, YourRound, Val, LastWriteRound}).

-spec msg_write_reply(comm:mypid(), Consistency::boolean(),
                      any(), pr:pr(), pr:pr()) -> ok.
msg_write_reply(Client, Cons, Key, UsedWriteRound, YourNextRoundForWrite) ->
    comm:send(Client, {write_reply, Cons, Key, UsedWriteRound, YourNextRoundForWrite}).

-spec msg_write_deny(comm:mypid(), Consistency::boolean(), any(), pr:pr())
                    -> ok.
msg_write_deny(Client, Cons, Key, NewerRound) ->
    comm:send(Client, {write_deny, Cons, Key, NewerRound}).

-spec noop_read_filter(term()) -> term().
noop_read_filter(X) -> X.

-spec noop_write_filter(Old :: term(), WF :: term(), Val :: term()) -> term().
noop_write_filter(_, _, X) -> X.

%% initialize: return initial state.
-spec init(atom()) -> state().
init(DBName) -> ?PDB:new(atom_to_list(DBName)).

-spec on(message(), state()) -> state().
on({prbr, read, _DB, Cons, Proposer, Key, ProposerUID, ReadFilter}, TableName) ->
    ?TRACE("prbr:read: ~p in round ~p~n", [Key, ProposerUID]),
    KeyEntry = get_entry(Key, TableName),

    %% assign a valid next read round number
    AssignedReadRound = next_read_round(KeyEntry, ProposerUID),
%%    trace_mpath:log_info(self(), {list_to_atom(lists:flatten(io_lib:format("read:~p", [entry_val(KeyEntry)])))}),
    msg_read_reply(Proposer, Cons, AssignedReadRound,
                   ReadFilter(entry_val(KeyEntry)),
                   entry_r_write(KeyEntry)),

    NewKeyEntry = entry_set_r_read(KeyEntry, AssignedReadRound),
%%    log:log("read~n"
%%            "Key: ~p~n"
%%            "Val: ~p", [Key, NewKeyEntry]),
    _ = set_entry(NewKeyEntry, TableName),
    TableName;

on({prbr, write, _DB, Cons, Proposer, Key, InRound, Value, PassedToUpdate, WriteFilter}, TableName) ->
    ?TRACE("prbr:write for key: ~p in round ~p~n", [Key, InRound]),
    KeyEntry = get_entry(Key, TableName),
    _ = case writable(KeyEntry, InRound) of
            {ok, NewKeyEntry, NextWriteRound} ->
                NewVal = WriteFilter(entry_val(NewKeyEntry),
                                     PassedToUpdate, Value),
%%                case kvx =/= _DB of
%%                    true ->
%%                log:log("write ok~n"
%%                        "Key: ~p~n"
%%                        "Ent: ~p~n"
%%                        "Val: ~p", [Key, KeyEntry, NewVal]);
%%                    _ -> ok
%%                end,
                msg_write_reply(Proposer, Cons, Key, InRound, NextWriteRound),
                set_entry(entry_set_val(NewKeyEntry, NewVal), TableName);
            {dropped, NewerRound} ->
%%                case kvx =/= _DB of
%%                    true ->
%%                log:log("write deny~n"
%%                        "Key: ~p~n"
%%                        "Val: ~p", [Key, KeyEntry]);
%%                    _ -> ok
%%                end,
                %% log:pal("Denied ~p ~p ~p~n", [Key, InRound, NewerRound]),
                msg_write_deny(Proposer, Cons, Key, NewerRound)
        end,
    TableName;

on({prbr, tab2list, DB, Client}, TableName) ->
    comm:send_local(Client, {DB, tab2list(TableName)}),
    TableName;

on({prbr, tab2list_raw, DB, Client}, TableName) ->
    comm:send_local(Client, {DB, tab2list_raw(TableName)}),
    TableName.

-spec get_entry(any(), state()) -> entry().
get_entry(Id, TableName) ->
    case ?PDB:get(TableName, Id) of
        {}    -> new(Id);
        Entry -> Entry
    end.

-spec set_entry(entry(), state()) -> state().
set_entry(NewEntry, TableName) ->
    _ = ?PDB:set(TableName, NewEntry),
    TableName.

-spec get_load(state()) -> non_neg_integer().
get_load(State) -> ?PDB:get_load(State).

-spec tab2list(state()) -> [{any(),any()}].
tab2list(State) ->
    %% without prbr own data
    Entries = tab2list_raw(State),
    [ {element(1,X), element(4,X)} || X <- Entries].

-spec tab2list_raw_unittest(state()) -> [entry()].
tab2list_raw_unittest(State) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    tab2list_raw(State).

-spec tab2list_raw(state()) -> [entry()].
tab2list_raw(State) ->
    %% with prbr own data
    ?PDB:tab2list(State).

%% operations for abstract data type entry()

-spec new(any()) -> entry().
new(Key) ->
    new(Key, prbr_bottom).

-spec new(any(), any()) -> entry().
new(Key, Val) ->
    {Key,
     %% Note: atoms < pids, so this is a good default.
     _R_Read = pr:new(0, '_'),
     %% Note: atoms < pids, so this is a good default.
     _R_Write = pr:new(0, '_'),
     _Value = Val}.


%% -spec entry_key(entry()) -> any().
%% entry_key(Entry) -> element(1, Entry).
%% -spec entry_set_key(entry(), any()) -> entry().
%% entry_set_key(Entry, Key) -> setelement(2, Entry, Key).
-spec entry_r_read(entry()) -> pr:pr().
entry_r_read(Entry) -> element(2, Entry).
-spec entry_set_r_read(entry(), pr:pr()) -> entry().
entry_set_r_read(Entry, Round) -> setelement(2, Entry, Round).
-spec entry_r_write(entry()) -> pr:pr().
entry_r_write(Entry) -> element(3, Entry).
-spec entry_set_r_write(entry(), pr:pr()) -> entry().
entry_set_r_write(Entry, Round) -> setelement(3, Entry, Round).
-spec entry_val(entry()) -> any().
entry_val(Entry) -> element(4, Entry).
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Value) -> setelement(4, Entry, Value).

-spec next_read_round(entry(), any()) -> pr:pr().
next_read_round(Entry, ProposerUID) ->
    LatestSeenRead = pr:get_r(entry_r_read(Entry)),
    LatestSeenWrite = pr:get_r(entry_r_write(Entry)),
    pr:new(util:max(LatestSeenRead, LatestSeenWrite) + 1, ProposerUID).



-spec writable(entry(), pr:pr()) -> {ok, entry(),
                                     NextWriteRound :: pr:pr()} |
                                    {dropped,
                                     NewerSeenRound :: pr:pr()}.
writable(Entry, InRound) ->
    LatestSeenRead = entry_r_read(Entry),
    LatestSeenWrite = entry_r_write(Entry),
    if (InRound >= LatestSeenRead) andalso (InRound > LatestSeenWrite) ->
           T1Entry = entry_set_r_write(Entry, InRound),
           %% prepare fast_paxos for this client:
           NextWriteRound = next_read_round(T1Entry,
                                            pr:get_id(InRound)),
           %% assume this token was seen in a read already, so no one else
           %% can interfere without paxos noticing it
           T2Entry = entry_set_r_read(T1Entry, NextWriteRound),
           {ok, T2Entry, NextWriteRound};
       true ->
           %% proposer may not have latest value for a clean content
           %% check, and another proposer is concurrently active, so
           %% we do not prepare a fast_paxos for this client, but let
           %% the other proposer the chance to pass read and write
           %% phase.  The denied proposer has to perform a read and write
           %% phase on its own (including a new content check).
           {dropped, util:max(LatestSeenRead, LatestSeenWrite)}
    end.

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> true.
check_config() -> true.

-spec tester_create_write_filter(0) -> write_filter().
tester_create_write_filter(0) -> fun prbr:noop_write_filter/3.

