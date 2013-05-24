% @copyright 2012-2013 Zuse Institute Berlin,

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

-define(PDB, pdb_ets).

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

%% let users retrieve their uid from an assigned round number.
-export([r_with_id_get_id/1]).

%% let users retrieve their smallest possible round for fast_write on
%% entry creation.
-export([smallest_round/1]).

-ifdef(with_export_type_support).
-export_type([message/0]).
-export_type([state/0]).
-export_type([r_with_id/0]).
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

-type state() :: ?PDB:tableid().

-type message() ::
        {prbr, read, DB :: dht_node_state:db_selector(),
         Proposer :: comm:mypid(), ?RT:key(), InRound,
         read_filter()}
      | {prbr, write, DB :: dht_node_state:db_selector(),
         Proposer :: comm:mypid(), ?RT:key(), InRound,
         Value :: term(), PassedToUpdate :: term(), write_filter()}.

%% r_with_id() has to be unique for this key system wide
%% r_with_id() has to be comparable with < and =<
%% if the caller process may handle more than one request at a time for the
%% same key, it has to be unique for each request.
-type r_with_id() :: {non_neg_integer(), any()}.
%% for example
%% -type r_with_id() :: {pos_integer(), comm:mypid_plain()}.
%% -type r_with_id() :: {pos_integer(), {dht_node_id(), lease_epoch()}}.

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
                   r_with_id(), %% r_read
                   r_with_id(), %% r_write
                   any()    %% value
                 }.

%% Messages to expect from this module
-spec msg_read_reply(comm:mypid(), r_with_id(), any(), r_with_id())
             -> ok.
msg_read_reply(Client, YourRound, Val, LastWriteRound) ->
    comm:send(Client, {read_reply, YourRound, Val, LastWriteRound}).

-spec msg_write_reply(comm:mypid(), any(), r_with_id(), r_with_id()) -> ok.
msg_write_reply(Client, Key, UsedWriteRound, YourNextRoundForWrite) ->
    comm:send(Client, {write_reply, Key, UsedWriteRound, YourNextRoundForWrite}).

-spec msg_write_deny(comm:mypid(), any(), r_with_id()) -> ok.
msg_write_deny(Client, Key, NewerRound) ->
    comm:send(Client, {write_deny, Key, NewerRound}).

-spec noop_read_filter(term()) -> term().
noop_read_filter(X) -> X.

-spec noop_write_filter(Old :: term(), WF :: term(), Val :: term()) -> term().
noop_write_filter(_, _, X) -> X.

%% initialize: return initial state.
-spec init(atom()) -> state().
init(DBName) -> ?PDB:new(DBName, [ordered_set, protected]).

-spec on(message(), state()) -> state().
on({prbr, read, _DB, Proposer, Key, ProposerUID, ReadFilter}, TableName) ->
    ?TRACE("prbr:read: ~p in round ~p~n", [Key, ProposerUID]),
    KeyEntry = get_entry(Key, TableName),

    %% assign a valid next read round number
    TheRound = next_read_round(KeyEntry, ProposerUID),
%%    trace_mpath:log_info(self(), {list_to_atom(lists:flatten(io_lib:format("read:~p", [entry_val(KeyEntry)])))}),
    msg_read_reply(Proposer, TheRound,
                   ReadFilter(entry_val(KeyEntry)),
                   entry_r_write(KeyEntry)),

    NewKeyEntry = entry_set_r_read(KeyEntry, TheRound),
    _ = set_entry(NewKeyEntry, TableName),
    TableName;

on({prbr, write, _DB, Proposer, Key, InRound, Value, PassedToUpdate, WriteFilter}, TableName) ->
    ?TRACE("prbr:write for key: ~p in round ~p~n", [Key, InRound]),
    KeyEntry = get_entry(Key, TableName),
    _ = case writable(KeyEntry, InRound) of
            {ok, NewKeyEntry, NextWriteRound} ->
                NewVal = WriteFilter(entry_val(NewKeyEntry),
                                     PassedToUpdate, Value),
                msg_write_reply(Proposer, Key, InRound, NextWriteRound),
                set_entry(entry_set_val(NewKeyEntry, NewVal), TableName);
            {dropped, NewerRound} ->
                %% log:pal("Denied ~p ~p ~p~n", [Key, InRound, NewerRound]),
                msg_write_deny(Proposer, Key, NewerRound)
        end,
    TableName.

-spec get_entry(any(), state()) -> entry().
get_entry(Id, TableName) ->
    case ?PDB:get(Id, TableName) of
        undefined -> new(Id);
        Entry -> Entry
    end.

-spec set_entry(entry(), state()) -> state().
set_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName),
    TableName.

%% As the round number contains the client's pid, they are still
%% unique.  Two clients using their smallest_round for a fast write
%% concurrently are separated, because we do not use plain Paxos, but
%% assign a succesful writer immediately the next round_number for a
%% follow up fast_write by increasing our read_round already on the
%% write.  So, the smallest_round of the second client becomes invalid
%% when the first one writes.  In consequence, at most one proposer
%% can perform a successful fast_write with its smallest_round. Voila!
-spec smallest_round(comm:mypid()) -> r_with_id().
smallest_round(Pid) -> {0, Pid}.

%% operations for abstract data type entry()

-spec new(any()) -> entry().
new(Key) ->
    new(Key, prbr_bottom).

-spec new(any(), any()) -> entry().
new(Key, Val) ->
    {Key,
     _R_Read = {0, '_'},  %% Note: atoms < pids, so this is a good default.
     _R_Write = {0, '_'}, %% Note: atoms < pids, so this is a good default.
     _Value = Val}.


%% -spec entry_key(entry()) -> any().
%% entry_key(Entry) -> element(1, Entry).
%% -spec entry_set_key(entry(), any()) -> entry().
%% entry_set_key(Entry, Key) -> setelement(2, Entry, Key).
-spec entry_r_read(entry()) -> r_with_id().
entry_r_read(Entry) -> element(2, Entry).
-spec entry_set_r_read(entry(), r_with_id()) -> entry().
entry_set_r_read(Entry, Round) -> setelement(2, Entry, Round).
-spec entry_r_write(entry()) -> r_with_id().
entry_r_write(Entry) -> element(3, Entry).
-spec entry_set_r_write(entry(), r_with_id()) -> entry().
entry_set_r_write(Entry, Round) -> setelement(3, Entry, Round).
-spec entry_val(entry()) -> any().
entry_val(Entry) -> element(4, Entry).
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Value) -> setelement(4, Entry, Value).

-spec next_read_round(entry(), any()) -> r_with_id().
next_read_round(Entry, ProposerUID) ->
    LatestSeenRead = r_with_id_get_r(entry_r_read(Entry)),
    LatestSeenWrite = r_with_id_get_r(entry_r_write(Entry)),
    r_with_id_new(util:max(LatestSeenRead, LatestSeenWrite) + 1, ProposerUID).



-spec writable(entry(), r_with_id()) -> {ok, entry(),
                                         NextWriteRound :: r_with_id()} |
                                        {dropped,
                                         NewerSeenRound :: r_with_id()}.
writable(Entry, InRound) ->
    LatestSeenRead = entry_r_read(Entry),
    LatestSeenWrite = entry_r_write(Entry),
    case (InRound >= LatestSeenRead)
        andalso (InRound > LatestSeenWrite) of
        true ->
            T1Entry = entry_set_r_write(Entry, InRound),
            %% prepare fast_paxos for this client:
            NextWriteRound = next_read_round(T1Entry,
                                             r_with_id_get_id(InRound)),
            %% assume this token was seen in a read already, so no one else
            %% can interfere without paxos noticing it
            T2Entry = entry_set_r_read(T1Entry, NextWriteRound),
            {ok, T2Entry, NextWriteRound};
        false ->
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

-spec r_with_id_new(non_neg_integer(), any()) -> r_with_id().
r_with_id_new(Counter, ProposerUID) -> {Counter, ProposerUID}.

-spec r_with_id_get_r(r_with_id()) -> non_neg_integer().
r_with_id_get_r(RwId) -> element(1, RwId).

-spec r_with_id_get_id(r_with_id()) -> any().
r_with_id_get_id(RwId) -> element(2, RwId).
