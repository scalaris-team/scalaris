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
%% @doc    Acceptor implementation from "Generalized Lattice Agreement"
%%  TODO:
%%      - right now proposerlist is lazily extended
%% @end
-module(gla_acceptor).
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

-export_type([state/0]).
-export_type([entry/0]).

-type state() :: {?PDB:db(), [comm:mypid()]}.

%% so there are no longer any read denies, all reads succeed.
-type entry() :: {
                   any(), %% key
                   any() %% value
                 }.

%% Messages to expect from this module
-spec msg_get_val_reply(comm:mypid(), ?RT:key(), crdt:crdt_module(), crdt:crdt()) -> ok.
msg_get_val_reply(Client, Key, DataType, UpdatedVal) ->
    comm:send(Client, {proposal_val_reply, Key, DataType, UpdatedVal}).

-spec msg_ack_reply(comm:mypid(), ?RT:key(), pr:pr(), crdt:crdt()) -> ok.
msg_ack_reply(Client, Key, ProposalNumber, ProposalValue) ->
    comm:send(Client, {ack_reply, Key, ProposalNumber, ProposalValue}).

-spec msg_learner_ack_reply(comm:mypid(), ?RT:key(), [comm:mypid()], crdt:crdt_module(), pr:pr(), crdt:crdt(), any()) -> ok.
msg_learner_ack_reply(Proposer, Key, Clients, DataType, ProposalNumber, ProposalValue, ProposerId) ->
    comm:send(Proposer, {learner_ack_reply, Key, Clients, DataType, ProposalNumber, ProposalValue, ProposerId}).

-spec msg_nack_reply(comm:mypid(), ?RT:key(), pr:pr(), crdt:crdt()) -> ok.
msg_nack_reply(Client, Key, ProposalNumber, ProposalValue) ->
    comm:send(Client, {nack_reply, Key, ProposalNumber, ProposalValue}).


%% initialize: return initial state.
-spec init(atom() | tuple()) -> state().
init(DBName) -> {?PDB:new(DBName), []}.

%% @doc Closes the given DB (it may be recoverable using open/1 depending on
%%      the DB back-end).
-spec close(state()) -> true.
close(_State={TableName, _}) -> ?PDB:close(TableName).

%% @doc Closes the given DB and deletes all contents (this DB can thus not be
%%      re-opened using open/1).
-spec close_and_delete(state()) -> true.
close_and_delete(_State={TableName, _}) -> ?PDB:close_and_delete(TableName).

-spec on(tuple(), state()) -> state().
on({gla_acceptor, get_proposal_value, _Cons, Proposer, Key, DataType, UpdateFun}, State={TableName, _ProposerList}) ->
    ?TRACE("gla_acceptor:update: ~p ~p ", [Key, Proposer]),
    Entry = get_entry(Key, TableName),

    Keys = lists:sort(replication:get_keys(Key)),
    Tmp = lists:dropwhile(fun(E) -> E =/= Key end, Keys),
    ThisReplicaId = length(Keys) - length(Tmp) + 1,

    CVal = entry_val(Entry, DataType),
    NewCVal = DataType:apply_update(UpdateFun, ThisReplicaId, CVal),
    NewEntry = entry_set_val(Entry, NewCVal),
    _ = set_entry(NewEntry, TableName),

    msg_get_val_reply(Proposer, Key, DataType, NewCVal),
    trace_mpath:log_info(self(), {acceptor_update,
                                  key, Key,
                                  old_value, CVal,
                                  new_value, NewCVal}),
    State;


on({gla_acceptor, propose, _Cons, Proposer, Key, Clients, ProposalNumber, ProposedValue, DataType},
   _State={TableName, ProposerList}) ->
    ?TRACE("crdt_acceptor:propose: ~p ~p ~n ~p ~p", [Key, Proposer, ProposalNumber, ProposedValue]),
    NewProposerList = case lists:member(Proposer, ProposerList) of
                          true -> ProposerList;
                          false -> [Proposer | ProposerList]
                      end,

    Entry = get_entry(Key, TableName),
    AcceptedValue = entry_val(Entry, DataType),

    case DataType:lteq(AcceptedValue, ProposedValue) of
        true ->
            NewEntry = entry_set_val(Entry, ProposedValue),
            _ = set_entry(NewEntry, TableName),

            _ = [msg_learner_ack_reply(Learner, Key, Clients, DataType, ProposalNumber, ProposedValue, Proposer)
                 || Learner <- NewProposerList], %% each proposer is also a learner
            msg_ack_reply(Proposer, Key, ProposalNumber, ProposedValue);
        false ->
            NewEntry = entry_set_val(Entry, DataType:merge(AcceptedValue, ProposedValue)),
            _ = set_entry(NewEntry, TableName),
            msg_nack_reply(Proposer, Key, ProposalNumber, AcceptedValue)
    end,

    {TableName, NewProposerList}.

-spec get_entry(any(), ?PDB:db()) -> entry().
get_entry(Id, TableName) ->
    case ?PDB:get(TableName, Id) of
        {}    -> new(Id);
        Entry -> Entry
    end.

-spec set_entry(entry(), ?PDB:db()) -> ?PDB:db().
set_entry(NewEntry, TableName) ->
    _ = ?PDB:set(TableName, NewEntry),
    TableName.


-spec new(any()) -> entry().
new(Key) ->
    new(Key, gla_bottom).

-spec new(any(), any()) -> entry().
new(Key, Val) ->
    {Key, _Value = Val}.

-spec entry_key(entry()) -> any().
entry_key(Entry) -> element(1, Entry).
-spec entry_val(entry()) -> crdt:crdt() | gla_bottom.
entry_val(Entry) -> element(2, Entry).
-spec entry_val(entry(), crdt:crdt_module()) -> crdt:crdt().
entry_val(Entry, DataType) ->
    case entry_val(Entry) of
        gla_bottom -> DataType:new();
        Any -> Any
    end.
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Value) -> setelement(2, Entry, Value).


%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() -> true.

