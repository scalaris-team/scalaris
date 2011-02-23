% @copyright 2009-2011 Zuse Institute Berlin,
%            2010 onScale solutions GmbH

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
%% @doc Part of a generic Paxos-Consensus implementation
%%      The role of a acceptor.
%% @end
%% @version $Id$
-module(acceptor).
-author('schintke@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-behaviour(gen_component).

-include("scalaris.hrl").

%%% public interface for initiating a paxos acceptor for a new PaxosID
-export([start_paxosid/2, start_paxosid_local/3, start_paxosid/3]).
-export([stop_paxosids/2]).
-export([add_learner/3]).
-export([msg_accepted/4]).
%%% functions for gen_component module and supervisor callbacks
-export([start_link/2]).
-export([on/2, init/1]).
-export([check_config/0]).

%% Messages to expect from this module
-spec msg_ack(comm:mypid(), any(), non_neg_integer(), any(), non_neg_integer())
             -> ok.
msg_ack(Proposer, PaxosID, InRound, Val, Raccepted) ->
    comm:send(Proposer, {acceptor_ack, PaxosID, InRound, Val, Raccepted}).

-spec msg_nack(comm:mypid(), any(), non_neg_integer()) -> ok.
msg_nack(Proposer, PaxosID, NewerRound) ->
    comm:send(Proposer, {acceptor_nack, PaxosID, NewerRound}).

-spec msg_naccepted(comm:mypid(), any(), non_neg_integer()) -> ok.
msg_naccepted(Proposer, PaxosID, NewerRound) ->
    comm:send(Proposer, {acceptor_naccepted, PaxosID, NewerRound}).

-spec msg_accepted(comm:mypid(), any(), non_neg_integer(), any()) -> ok.
msg_accepted(Learner, PaxosID, Raccepted, Val) ->
    comm:send(Learner, {acceptor_accepted, PaxosID, Raccepted, Val}).

%%% public function to initiate a new paxos instance
%%% gets a
%%%   PaxosID: has to be unique in the system, user has to care about this
%%%   Acceptors: a list of paxos_acceptor processes, that are used
%%%   Proposal: if no consensus is available beforehand, this proposer proposes this
%%%   Majority: how many responses from acceptors have to be collected?
%%%   InitialRound (optional): start with paxos round number (default 1)
%%%     if InitialRound is 0, a Fast-Paxos is executed
-spec start_paxosid(any(), [ comm:mypid() ]) -> ok.
start_paxosid(PaxosID, Learners) ->
    Acceptor = pid_groups:get_my(paxos_acceptor),
    start_paxosid_local(Acceptor, PaxosID, Learners).

-spec start_paxosid_local(pid(), any(), [ comm:mypid() ]) -> ok.
start_paxosid_local(LAcceptor, PaxosID, Learners) ->
    %% find the groups acceptor process
    Message = {acceptor_initialize, PaxosID, Learners},
    comm:send_local(LAcceptor, Message).

-spec start_paxosid(comm:mypid(), any(), [ comm:mypid() ]) -> ok.
start_paxosid(Acceptor, PaxosID, Learners) ->
    comm:send(Acceptor, {acceptor_initialize, PaxosID, Learners}).

-spec stop_paxosids(comm:mypid(), list(any())) -> ok.
stop_paxosids(Acceptor, PaxosIds) ->
    comm:send(Acceptor, {acceptor_deleteids, PaxosIds}).

-spec add_learner(comm:mypid(), any(), comm:mypid()) -> ok.
add_learner(Acceptor, PaxosID, Learner) ->
    comm:send(Acceptor, {acceptor_add_learner, PaxosID, Learner}).

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname(), pid_groups:pidname()) -> {ok, pid()}.
start_link(DHTNodeGroup, PidName) ->
    gen_component:start_link(?MODULE,
                             [],
                             [{pid_groups_join_as, DHTNodeGroup, PidName}]).

%% initialize: return initial state.
-spec init([]) -> atom().
init([]) ->
    ?TRACE("Starting acceptor for DHT node: ~p~n", [pid_groups:my_groupname()]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = list_to_atom(pid_groups:my_groupname() ++ "_acceptor"),
    %%pdb:new(TableName, [set, protected, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    TableName = pdb:new(?MODULE, [set, protected]),
    _State = TableName.

-spec on(comm:message(), atom()) -> atom().
on({acceptor_initialize, PaxosID, Learners}, ETSTableName = State) ->
    ?TRACE("acceptor:initialize for paxos id: Pid ~p Learners ~p~n", [PaxosID, Learners]),
    {_, StateForID} = my_get_entry(PaxosID, ETSTableName),
    case acceptor_state:get_learners(StateForID) of
        Learners -> log:log(error, "dupl. acceptor init for id ~p", [PaxosID]);
        _ ->
            NewState = acceptor_state:set_learners(StateForID, Learners),
            my_set_entry(NewState, ETSTableName),
            case acceptor_state:accepted(NewState) of
                true  -> inform_learners(PaxosID, NewState);
                false -> ok
            end
    end,
    State;

% need Sender & PaxosID
on({proposer_prepare, Proposer, PaxosID, InRound}, ETSTableName = State) ->
    ?TRACE("acceptor:prepare for paxos id: ~p round ~p~n", [PaxosID,InRound]),
    {ErrCode, StateForID} = my_get_entry(PaxosID, ETSTableName),
    case ErrCode of
        new -> msg_delay:send_local(
                 config:read(acceptor_noinit_timeout) div 1000, self(),
                 {acceptor_delete_if_no_learner, PaxosID});
        _ -> ok
    end,
    case acceptor_state:add_prepare_msg(StateForID, InRound) of
        {ok, NewState} ->
            my_set_entry(NewState, ETSTableName),
            msg_ack(Proposer, PaxosID, InRound,
                    acceptor_state:get_value(NewState),
                    acceptor_state:get_raccepted(NewState));
        {dropped, NewerRound} -> msg_nack(Proposer, PaxosID, NewerRound)
    end,
    State;

on({proposer_accept, Proposer, PaxosID, InRound, InProposal}, ETSTableName = State) ->
    ?TRACE("acceptor:accept for paxos id: ~p round ~p~n", [PaxosID, InRound]),
    {ErrCode, StateForID} = my_get_entry(PaxosID, ETSTableName),
    case ErrCode of
        new -> msg_delay:send_local((config:read(tx_timeout) * 4) div 1000, self(),
                         {acceptor_delete_if_no_learner, PaxosID});
        _ -> ok
    end,
    _ = case acceptor_state:add_accept_msg(StateForID, InRound, InProposal) of
        {ok, NewState} ->
            my_set_entry(NewState, ETSTableName),
            inform_learners(PaxosID, NewState);
        {dropped, NewerRound} -> msg_naccepted(Proposer, PaxosID, NewerRound)
    end,
    State;

on({acceptor_deleteids, ListOfPaxosIDs}, ETSTableName = State) ->
    ?TRACE("acceptor:deleteids~n", []),
    _ = [pdb:delete(Id, ETSTableName) || Id <- ListOfPaxosIDs],
    State;

on({acceptor_delete_if_no_learner, PaxosID}, ETSTableName = State) ->
    ?TRACE("acceptor:delete_if_no_learner~n", []),
    {ErrCode, StateForID} = my_get_entry(PaxosID, ETSTableName),
    case {ErrCode, acceptor_state:get_learners(StateForID)} of
        {new, _} -> ok; %% already deleted
        {_, []} ->
            %% io:format("Deleting unhosted acceptor id~n"),
            pdb:delete(PaxosID, ETSTableName);
        {_, _} -> ok %% learners are registered
    end,
    State;

on({acceptor_add_learner, PaxosID, Learner}, ETSTableName = State) ->
    ?TRACE("acceptor:add_learner~n", []),
    {ErrCode, StateForID} = my_get_entry(PaxosID, ETSTableName),
    case ErrCode of
        new -> ok; %% do not support adding learners without prior initialize
        ok ->
            case acceptor_state:accepted(StateForID) of
                true -> inform_learner(Learner,  PaxosID, StateForID);
                false -> ok
            end,
            NewLearners = [Learner | acceptor_state:get_learners(StateForID)],
            NStateForID = acceptor_state:set_learners(StateForID, NewLearners),
            my_set_entry(NStateForID, ETSTableName)
    end,
    State.

my_get_entry(Id, TableName) ->
    case pdb:get(Id, TableName) of
        undefined -> {new, acceptor_state:new(Id)};
        Entry -> {ok, Entry}
    end.

my_set_entry(NewEntry, TableName) ->
    pdb:set(NewEntry, TableName).

inform_learners(PaxosID, State) ->
    ?TRACE("acceptor:inform_learners: PaxosID ~p Learners ~p Decision ~p~n",
           [PaxosID, acceptor_state:get_learners(State), acceptor_state:get_value(State)]),
    [ inform_learner(X, PaxosID, State)
      || X <- acceptor_state:get_learners(State) ].

inform_learner(Learner, PaxosID, StateForID) ->
    msg_accepted(Learner, PaxosID,
                 acceptor_state:get_raccepted(StateForID),
                 acceptor_state:get_value(StateForID)).

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(acceptor_noinit_timeout) and
    config:is_greater_than_equal(acceptor_noinit_timeout, 1000) and
    config:is_greater_than_equal(tx_timeout, 1000/4) and
    config:is_greater_than(acceptor_noinit_timeout, tx_timeout).
