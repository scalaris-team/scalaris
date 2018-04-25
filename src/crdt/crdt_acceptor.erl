% @copyright 2012-2018 Zuse Institute Berlin,

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

%% @author Jan Skrzypczak
%% @doc    Paxos register for CRDT's. Implements the role of acceptor
%% @end
-module(crdt_acceptor).
-author('skrzypczak@zib.de').

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").

-define(PDB, db_prbr).
%%% functions for module where embedded into
-export([on/2, init/1, close/1, close_and_delete/1]).
-export([check_config/0]).
-export([new/2]).
-export([set_entry/2]).
-export([get_entry/2]).
-export([entry_key/1]).
-export([entry_val/2]).
-export([entry_set_val/2]).

%% let fetch the number of DB entries
-export([get_load/1]).

%% only for unittests
-export([tab2list_raw_unittest/1]).

%% only during recover
-export([tab2list/1]).

-export_type([state/0]).
-export_type([entry/0]).

-type state() :: ?PDB:db().

%% so there are no longer any read denies, all reads succeed.
-type entry() :: { any(), %% key
                   pr:pr(), %% r_read
                   pr:pr(), %% r_write  %%TODO: write roudns are not needed???
                   any() %% value
                 }.

%% Messages to expect from this module
-spec msg_update_reply(comm:mypid(), any(), crdt:crdt()) -> ok.
msg_update_reply(Client, ReqId, CVal) ->
    comm:send(Client, {update_reply, ReqId, CVal}).

-spec msg_merge_reply(comm:mypid(), any()) -> ok.
msg_merge_reply(Client, ReqId) ->
    comm:send(Client, {merge_reply, ReqId, done}).

-spec msg_query_reply(comm:mypid(), any(), any()) -> ok.
msg_query_reply(Client, ReqId, QueryResult) ->
    comm:send(Client, {query_reply, ReqId, QueryResult}).

-spec msg_prepare_reply(comm:mypid(), any(), pr:pr(), pr:pr(), crdt:crdt()) -> ok.
msg_prepare_reply(Client, ReqId, ReadRound, WriteRound, CVal) ->
    comm:send(Client, {prepare_reply, ReqId, ReadRound, WriteRound, CVal}).

-spec msg_prepare_deny(comm:mypid(), any(), pr:pr(), pr:pr()) -> ok.
msg_prepare_deny(Client, ReqId, TriedReadRound, RequiredReadRound) ->
    comm:send(Client, {read_deny, ReqId, inc, TriedReadRound, RequiredReadRound}).

-spec msg_vote_reply(comm:mypid(), any()) -> ok.
msg_vote_reply(Client, ReqId) ->
    comm:send(Client, {vote_reply, ReqId, done}).

-spec msg_vote_deny(comm:mypid(), any(), pr:pr(), pr:pr()) -> ok.
msg_vote_deny(Client, ReqId, TriedWriteRound, RequiredReadRound) ->
    comm:send(Client, {read_deny, ReqId, inc, TriedWriteRound, RequiredReadRound}).


%% initialize: return initial state.
-spec init(atom() | tuple()) -> state().
init(DBName) -> ?PDB:new(DBName).


%% @doc Closes the given DB (it may be recoverable using open/1 depending on
%%      the DB back-end).
-spec close(state()) -> true.
close(State) -> ?PDB:close(State).

%% @doc Closes the given DB and deletes all contents (this DB can thus not be
%%      re-opened using open/1).
-spec close_and_delete(state()) -> true.
close_and_delete(State) -> ?PDB:close_and_delete(State).

-spec on(tuple(), state()) -> state().
on({crdt_acceptor, update, _Cons, Proposer, ReqId, Key, DataType, UpdateFun}, TableName) ->
    ?TRACE("crdt_acceptor:update: ~p ~p ", [Key, Proposer]),
    Entry = get_entry(Key, TableName),

    Keys = lists:sort(replication:get_keys(Key)),
    Tmp = lists:dropwhile(fun(E) -> E =/= Key end, Keys),
    ThisReplicaId = length(Keys) - length(Tmp) + 1,

    CVal = entry_val(Entry, DataType),
    NewCVal = DataType:apply_update(UpdateFun, ThisReplicaId, CVal),
    ?ASSERT(DataType:lteq(CVal, NewCVal)),

    NewEntry = entry_set_val(Entry, NewCVal),
    _ = set_entry(NewEntry, TableName),

    msg_update_reply(Proposer, ReqId, NewCVal),
    TableName;

on({crdt_acceptor, merge, _Cons, Proposer, ReqId, Key, DataType, CValToMerge}, TableName) ->
    ?TRACE("crdt_acceptor:merge: ~p ~p", [Key, Proposer]),
    Entry = get_entry(Key, TableName),

    CVal = entry_val(Entry, DataType),
    NewCVal = DataType:merge(CVal, CValToMerge),
    ?ASSERT(DataType:lteq(CVal, NewCVal)),

    NewEntry = entry_set_val(Entry, NewCVal),
    _ = set_entry(NewEntry, TableName),

    msg_merge_reply(Proposer, ReqId),
    TableName;

%% eventual consistent read
on({crdt_acceptor, query_req, _Cons, Proposer, ReqId, Key, DataType, QueryFun}, TableName) ->
    ?TRACE("crdt:query_req: ~p", [Key]),
    Entry = get_entry(Key, TableName),

    CVal = entry_val(Entry, DataType),
    QueryResult = DataType:apply_query(QueryFun, CVal),

    msg_query_reply(Proposer, ReqId, QueryResult),
    TableName;

%% SC-read phase 1
on({crdt_acceptor, prepare, _Cons, Proposer, ReqId, Key, DataType, Round, CValToMerge}, TableName) ->
    ?TRACE("crdt:prepare: ~p in round ~p~n", [Key, ProposalRound]),
    Entry = get_entry(Key, TableName),
    CVal = entry_val(Entry, DataType),
    NewCVal = DataType:merge(CValToMerge, CVal),
    NewEntry = entry_set_val(Entry, NewCVal),

    ProposalRound =
        case Round of
            {inc, Id} ->
                OldRound =  entry_r_read(NewEntry),
                pr:new(pr:get_r(OldRound) + 1, Id);
            _ ->
                Round
        end,

    _ = case pr:get_r(ProposalRound) > pr:get_r(entry_r_read(NewEntry)) of
         true ->
            CurrentWriteRound = entry_r_write(NewEntry),
            NewEntry2 = entry_set_r_read(Entry, ProposalRound),
            _ = set_entry(NewEntry2, TableName),
            msg_prepare_reply(Proposer, ReqId, ProposalRound, CurrentWriteRound,
                              NewCVal);
         _ ->
            _ = set_entry(NewEntry, TableName),
            msg_prepare_deny(Proposer, ReqId, ProposalRound, entry_r_read(NewEntry))
    end,
    TableName;

%% SC-read phase 2
on({crdt_acceptor, vote, _Cons, Proposer, ReqId, Key, DataType, ProposalRound, CValToMerge}, TableName) ->
    ?TRACE("prbr:vote for key: ~p in round ~p~n", [Key, ProposalRound]),
    Entry = get_entry(Key, TableName),
    CurrentReadRound = entry_r_read(Entry),
    CVal = entry_val(Entry, DataType),
    NewCVal = DataType:merge(CVal, CValToMerge),
    ?ASSERT(DataType:lteq(CVal, NewCVal)),
    NewEntry = entry_set_val(Entry, NewCVal),

    _ = case ProposalRound =:= CurrentReadRound orelse
             pr:get_r(ProposalRound) > pr:get_r(CurrentReadRound) of
            true ->
                NewEntry2 = entry_set_r_read(NewEntry, ProposalRound),
                NewEntry3 = entry_set_r_write(NewEntry2, ProposalRound),
                _ = set_entry(NewEntry3, TableName),
                msg_vote_reply(Proposer, ReqId);
            false ->
                _ = set_entry(NewEntry, TableName),
                msg_vote_deny(Proposer, ReqId, ProposalRound, CurrentReadRound)
        end,
    TableName;

on({crdt_acceptor, get_entry, _DB, Client, Key}, TableName) ->
    comm:send_local(Client, {entry, get_entry(Key, TableName)}),
    TableName;

on({crdt_acceptor, tab2list_raw, DB, Client}, TableName) ->
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
    [ {entry_key(X), entry_val(X)} || X <- Entries].

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
    new(Key, crdt_bottom).

-spec new(any(), any()) -> entry().
new(Key, Val) ->
    {Key,
     %% Note: atoms < pids, so this is a good default.
     _R_Read = pr:new(0, '_'),
     %% Note: atoms < pids, so this is a good default.
     _R_Write = pr:new(0, '_'),
     _Value = Val
     }.

-spec entry_key(entry()) -> any().
entry_key(Entry) -> element(1, Entry).
-spec entry_r_read(entry()) -> pr:pr().
entry_r_read(Entry) -> element(2, Entry).
-spec entry_set_r_read(entry(), pr:pr()) -> entry().
entry_set_r_read(Entry, Round) -> setelement(2, Entry, Round).
-spec entry_r_write(entry()) -> pr:pr().
entry_r_write(Entry) -> element(3, Entry).
-spec entry_set_r_write(entry(), pr:pr()) -> entry().
entry_set_r_write(Entry, Round) -> setelement(3, Entry, Round).
-spec entry_val(entry()) -> crdt:crdt() | crdt_bottom.
entry_val(Entry) -> element(4, Entry).
-spec entry_val(entry(), module()) -> crdt:crdt().
entry_val(Entry, DataType) ->
    case entry_val(Entry) of
        crdt_bottom ->
            DataType:new();
        Any -> Any
    end.
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Value) -> setelement(4, Entry, Value).


%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() -> true.

