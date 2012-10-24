% @copyright 2012 Zuse Institute Berlin,

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
%% @doc Generic paxos round based register (prbr) implementation.
%% The read/write store alias acceptor.
%% @end
%% @version $Id:$
-module(prbr).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("client_types.hrl").

-define(PDB, pdb_ets).

%%% the prbr has to be embedded into a gen_component using it.
%%% The state it operates on has to be passed to the on handler
%%% correctly. All messages it handles start with the token
%%% prbr to support a generic, embeddable on-handler trigger.

%%% functions for module where embedded into
-export([on/2, init/1]).
-export([check_config/0]).
-export([noop_read_filter/1]).

%% let users retrieve their uid from an assigned round number.
-export([r_with_id_get_id/1]).

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
-type write_filter() :: fun((term(), term()) -> term()).

-type state() :: ?PDB:tableid().

-type message() ::
        {prbr, read, DB :: dht_node_state:db_selector(),
         Proposer :: comm:mypid(), client_key(), InRound,
         read_filter()}
      | {prbr, write, DB :: dht_node_state:db_selector(),
         Proposer :: comm:mypid(), client_key(), InRound,
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
%% unique as we get the node_id in the read request and it is part of
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

-spec msg_write_reply(comm:mypid(), any(), r_with_id()) -> ok.
msg_write_reply(Client, Key, R_write) ->
    comm:send(Client, {write_reply, Key, R_write}).

-spec msg_write_deny(comm:mypid(), any(), r_with_id()) -> ok.
msg_write_deny(Client, Key, NewerRound) ->
    comm:send(Client, {write_deny, Key, NewerRound}).

-spec noop_read_filter(term()) -> term().
noop_read_filter(X) -> X.

%% initialize: return initial state.
-spec init([]) -> state().
init([]) -> ?PDB:new(?MODULE, [set, protected]).

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
            {ok, NewKeyEntry} ->
                msg_write_reply(Proposer, Key, InRound),
                NewVal = WriteFilter(entry_val(NewKeyEntry),
                                     PassedToUpdate, Value),
                set_entry(entry_set_val(NewKeyEntry, NewVal), TableName);
            {dropped, NewerRound} ->
                ct:pal("Denied ~p ~p ~p~n", [Key, InRound, NewerRound]),
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

%% operations for abstract data type entry()

-spec new(any()) -> entry().
new(Key) ->
    {Key,
     _R_Read = {0, '_'},
     _R_Write = {0, '_'},
     _Value = prbr_bottom}.


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

-spec writable(entry(), r_with_id()) -> {ok, entry()} |
                                        {dropped, r_with_id()}.
writable(Entry, InRound) ->
    LatestSeenRead = entry_r_read(Entry),
    LatestSeenWrite = entry_r_write(Entry),
    case (InRound >= LatestSeenRead)
        andalso (InRound > LatestSeenWrite) of
        true ->  {ok, entry_set_r_write(Entry, InRound)};
        false -> {dropped, util:max(LatestSeenRead, LatestSeenWrite)}
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
