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
-export([entry_val/1]).
-export([entry_set_val/2]).

-export_type([state/0]).
-export_type([entry/0]).

-type state() :: {?PDB:db(), [comm:mypid()]}.

-type entry() :: {
                   any(), %% key
                   gset:crdt() | gla_bottom, %% value
                   any()
                 }.

%% Messages to expect from this module
-spec msg_ack_reply(comm:mypid(), ?RT:key(), pr:pr()) -> ok.
msg_ack_reply(Client, Key, ProposalNumber) ->
    comm:send(Client, {ack_reply, Key, ProposalNumber}).

-spec msg_learner_ack_reply(comm:mypid(), ?RT:key(), crdt:crdt_module(), pr:pr(), gset:crdt(), any()) -> ok.
msg_learner_ack_reply(Proposer, Key, DataType, ProposalNumber, ProposalValue, ProposerId) ->
    comm:send(Proposer, {learner_ack_reply, Key, DataType, ProposalNumber, ProposalValue, ProposerId}).

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

%% implements acceptor action Accept and Reject
-spec on(tuple(), state()) -> state().
on({gla_acceptor, propose, _Cons, Proposer, Key, DataType, ProposalNumber, PartialProposedValue},
   _State={TableName, ProposerList}) ->
    ?TRACE("crdt_acceptor:propose: ~p ~p ~n ~p ~p", [Key, Proposer, ProposalNumber, PartialProposedValue]),
    % TODO since no initial discovery is implemented, add unknown proposers to our list
    NewProposerList = case lists:member(Proposer, ProposerList) of
                          true -> ProposerList;
                          false -> [Proposer | ProposerList]
                      end,

    TEntry = get_entry(Key, TableName),
    Entry = entry_update_proposed(TEntry, Proposer, PartialProposedValue),
    ProposedValue = entry_proposed(Entry, Proposer),

    AcceptedValue = entry_val(Entry),

    case gset:lteq(AcceptedValue, ProposedValue) of
        true ->
            %% Accept action
            NewEntry = entry_set_val(Entry, ProposedValue),
            _ = set_entry(NewEntry, TableName),

            _ = [msg_learner_ack_reply(Learner, Key, DataType, ProposalNumber, ProposedValue, Proposer)
                 || Learner <- NewProposerList], %% each proposer is also a learner
            msg_ack_reply(Proposer, Key, ProposalNumber);
        false ->
            %% Reject action
            MergedValue = gset:merge(AcceptedValue, ProposedValue),
            NewEntry = entry_set_val(Entry, MergedValue),
            _ = set_entry(NewEntry, TableName),
            MissingValues = gset:subtract(AcceptedValue, ProposedValue),
            msg_nack_reply(Proposer, Key, ProposalNumber, MissingValues)
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

-spec new(any(), gset:crdt() | gla_bottom) -> entry().
new(Key, Val) ->
    {Key, Val, dict:new()}.

-spec entry_key(entry()) -> any().
entry_key(Entry) -> element(1, Entry).

-spec entry_val(entry()) -> gset:crdt() | gla_bottom.
entry_val(Entry) ->
    case element(2, Entry) of
        gla_bottom -> gset:new();
        Any -> Any
    end.
-spec entry_set_val(entry(), gset:crdt()) -> entry().
entry_set_val(Entry, Value) -> setelement(2, Entry, Value).

-spec entry_proposed(entry(), any()) -> gset:crdt().
entry_proposed(Entry, Proposer) -> dict:fetch(Proposer, element(3, Entry)).
-spec entry_update_proposed(entry(), any(), gset:crdt()) -> entry().
entry_update_proposed(Entry, Proposer, PartialVal) ->
    Dict = element(3, Entry),
    Dict2 = dict:update(Proposer, fun(E) -> gset:merge(PartialVal, E) end, PartialVal, Dict),
    setelement(3, Entry, Dict2).

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() -> true.

