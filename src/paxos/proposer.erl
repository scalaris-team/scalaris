% @copyright 2009, 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
%                 onScale solutions GmbH

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
%% @doc Description : Part of generic Paxos-Consensus implementation
%%           The role of a proposer.
%% @end
-module(proposer).
-author('schintke@onscale.de').
-vsn('$Id$').

%-define(TRACE(X,Y), ct:pal(X,Y)).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-behaviour(gen_component).

-include("scalaris.hrl").

%%% public interface for triggering a paxos proposer executed in any process
%%% a Fast-Paxos is triggered by giving 0 as initial round number explicitly
-export([start_paxosid/6,start_paxosid/7]).
-export([start_paxosid_with_proxy/7,start_paxosid_with_proxy/8]).
-export([stop_paxosids/2]).
-export([trigger/2]).
-export([msg_accept/5]).

%%% functions for gen_component module and supervisor callbacks
-export([start_link/1, start_link/2]).
-export([on/2, init/1]).

%%% public function to start a new paxos instance gets as parameters:
%%%   PaxosID: has to be unique in the system, user has to care about this
%%%   Acceptors: a list of paxos_acceptor processes, that are used
%%%   Proposal: if no consensus is available beforehand, this proposer proposes this
%%%   Majority: how many responses from acceptors have to be collected?
%%%   MaxProposers: the maximum number of proposers used for this paxos instance
%%%     (it is used to generate unique round numbers. Offset is the given initialRound)
%%%   InitialRound (optional): start with paxos round number (default 1)
%%%     if InitialRound is 0, a Fast-Paxos is executed
%%%     InitialRound must be unique for all proposers of a paxosid:
%%%       1 <= initialRound <= MaxProposers for normal paxos
%%%       0 <= initialRound < MaxProposers if 1 proposer uses fast paxos

msg_prepare(Dest, ReplyTo, PaxosID, Round) ->
    ?TRACE("Sending proposer_prepare: ~p, ~p~n", [PaxosID, Round]),
    Msg = {proposer_prepare, ReplyTo, PaxosID, Round},
    comm:send(Dest, Msg).

msg_accept(Dest, ReplyTo, PaxosID, Round, Value) ->
    ?TRACE("Sending proposer_accept ~p, ~p Proposal ~p~n", [PaxosID, Round, Value]),
    Msg = {proposer_accept, ReplyTo, PaxosID, Round, Value},
    comm:send(Dest, Msg).

start_paxosid(Proposer, PaxosID, Acceptors, Proposal,
              Majority, MaxProposers) ->
    start_paxosid(Proposer, PaxosID, Acceptors, Proposal,
                  Majority, MaxProposers, 1).
start_paxosid(Proposer, PaxosID, Acceptors, Proposal,
              Majority, MaxProposers, InitialRound) ->
    Msg = {proposer_initialize, PaxosID, Acceptors, Proposal,
           Majority, MaxProposers, InitialRound},
    comm:send(Proposer, Msg).

start_paxosid_with_proxy(Proxy, Proposer, PaxosID, Acceptors, Proposal,
                         Majority, MaxProposers) ->
    start_paxosid_with_proxy(Proxy, Proposer, PaxosID, Acceptors, Proposal,
                             Majority, MaxProposers, 1).
start_paxosid_with_proxy(Proxy, Proposer, PaxosID, Acceptors, Proposal,
                         Majority, MaxProposers, InitialRound) ->
    Msg = {proposer_initialize, PaxosID, Acceptors, Proposal,
           Majority, MaxProposers, InitialRound, Proxy},
    comm:send(Proposer, Msg).

stop_paxosids(Proposer, PaxosIds) ->
    comm:send(Proposer, {proposer_deleteids, PaxosIds}).

trigger(Proposer, PaxosID) ->
    comm:send(Proposer, {proposer_trigger, PaxosID}).

%% be startable via supervisor, use gen_component
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    start_link(InstanceId, []).

-spec start_link(instanceid(), [any()]) -> {ok, pid()}.
start_link(InstanceId, Options) ->
    gen_component:start_link(?MODULE,
                             [InstanceId, Options],
                             [{register, InstanceId, paxos_proposer}]).

%% initialize: return initial state.
-spec init([instanceid() | [any()]]) -> any().
init([_InstanceID, _Options]) ->
    ?TRACE("Starting proposer for instance: ~p~n", [_InstanceID]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = list_to_atom(lists:flatten(io_lib:format("~p_proposer", [InstanceID]))),
    %%pdb:new(TableName, [set, protected, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    TableName = pdb:new(?MODULE, [set, private]),
    _State = TableName.

on({proposer_initialize, PaxosID, Acceptors, Proposal,
    Majority, MaxProposers, InitialRound},
   State) ->
    on({proposer_initialize, PaxosID, Acceptors, Proposal,
        Majority, MaxProposers, InitialRound,
        _ReplyTo = comm:this()},
       State);

on({proposer_initialize, PaxosID, Acceptors, Proposal, Majority,
    MaxProposers, InitialRound, ReplyTo},
   ETSTableName = State) ->
    ?TRACE("proposer:initialize for paxos id: ~p round ~p~n", [PaxosID,InitialRound]),
    case pdb:get(PaxosID, ETSTableName) of
        undefined ->
            pdb:set(proposer_state:new(PaxosID, ReplyTo, Acceptors, Proposal,
                                       Majority, MaxProposers, InitialRound),
                    ETSTableName);
        _ ->
            io:format("Duplicate proposer:initialize for paxos id ~p~n", [PaxosID]),
            io:format("Just triggering instead~n")
    end,
    on({proposer_trigger, PaxosID, InitialRound}, State);

% trigger new proposer round
on({proposer_trigger, PaxosID}, ETSTableName = State) ->
    ?TRACE("proposer:trigger for paxos id ~p with auto round increment~n", [PaxosID]),
    case pdb:get(PaxosID, ETSTableName) of
        undefined -> ok;
        StateForID ->
            TmpState = proposer_state:reset_state(StateForID),
            NewState = proposer_state:inc_round(TmpState),
            pdb:set(NewState, ETSTableName),
            on({proposer_trigger, PaxosID, proposer_state:get_round(NewState)}, State)
    end,
    State;

%% trigger for given round is needed for initial round without auto-increment
%% and fast forward, but be careful:
%% Rounds must always have the form "InitialRound + x * MaxProposers"
on({proposer_trigger, PaxosID, Round}, ETSTableName = State) ->
    ?TRACE("proposer:trigger for paxos id ~p and round ~p~n", [PaxosID, Round]),
    case pdb:get(PaxosID, ETSTableName) of
        undefined -> ok;
        StateForID ->
            Acceptors = proposer_state:get_acceptors(StateForID),
            ReplyTo = proposer_state:get_replyto(StateForID),
            Proposal = proposer_state:get_proposal(StateForID),
            case Round of
                0 -> [msg_accept(X, ReplyTo,
                                 PaxosID, Round,
                                 Proposal)
                      || X <- Acceptors];
                _ -> [msg_prepare(X, ReplyTo, PaxosID, Round)
                      || X <- Acceptors]
            end,
            case Round > proposer_state:get_round(StateForID) of
                true ->
                    pdb:set(proposer_state:set_round(StateForID, Round),
                            ETSTableName);
                false -> ok
            end
    end,
    State;

on({acceptor_ack, PaxosID, Round, Value, RLast}, ETSTableName = State) ->
    ?TRACE("proposer:ack for paxos id ~p round ~p~n", [PaxosID, Round]),
    case pdb:get(PaxosID, ETSTableName) of
        undefined ->
            %% What to do when this PaxosID does not already exist? Think!
            %% -> Proposers don't get messages, they not requested.
            ok;
        StateForID ->
            case proposer_state:add_ack_msg(StateForID, Round, Value, RLast) of
                {ok, NewState} ->
                    %% ?TRACE("NEW State: ~p~n", [NewState]),
                    pdb:set(NewState, ETSTableName);
                {majority_acked, NewState} ->
                    %%   multicast accept(Round, Latest_value) to Acceptors
                    %% ?TRACE("NEW State: ~p majority accepted~n", [NewState]),
                    pdb:set(NewState, ETSTableName),
                    Acceptors = proposer_state:get_acceptors(NewState),
                    ReplyTo = proposer_state:get_replyto(NewState),
                    LatestVal = proposer_state:get_latest_value(NewState),
                    [msg_accept(X, ReplyTo, PaxosID, Round, LatestVal)
                     || X <- Acceptors]
            end
    end,
    State;

on({acceptor_nack, PaxosID, Round}, _ETSTableName = State) ->
    ?TRACE("proposer:nack for paxos id ~p and round ~p is newest seen~n",
           [PaxosID, Round]),
    start_new_higher_round(PaxosID, Round, State),
    State;

on({acceptor_naccepted, PaxosID, Round}, _ETSTableName = State) ->
    ?TRACE("proposer:naccepted for paxos id ~p and round ~p is newest seen~n",
           [PaxosID, Round]),
    start_new_higher_round(PaxosID, Round, State),
    State;

on({proposer_deleteids, ListOfPaxosIDs}, ETSTableName = State) ->
    [pdb:delete(Id, ETSTableName) || Id <- ListOfPaxosIDs],
    State;

on(_, _State) ->
    unknown_event.

start_new_higher_round(PaxosID, Round, ETSTableName) ->
    case pdb:get(PaxosID, ETSTableName) of
        undefined -> ok;
        StateForID ->
            MyRound = proposer_state:get_round(StateForID),
            %% check whether outdated nack message? (we get them from each acceptor)
            case MyRound < Round of
                true ->
                    MaxProposers = proposer_state:get_max_proposers(StateForID),
                    Factor = (Round - MyRound) div MaxProposers + 1,
                    NextRound = MyRound + Factor * MaxProposers,
                    %% let other prop. more time (NextRound ms) to achieve consensus
                    TmpState = proposer_state:reset_state(StateForID),
                    pdb:set(proposer_state:set_round(TmpState, NextRound), ETSTableName),
%%                     comm:send_local_after(NextRound, self(),
%%                                              {proposer_trigger, PaxosID,
%%                                               NextRound});
                    comm:send_local(self(),
                                       {proposer_trigger, PaxosID,
                                        NextRound});
                false -> dropped
            end
    end.
