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
%% @doc Part of generic Paxos-Consensus implementation
%%       The state needed for a single proposer instance.
%% @end
-module(proposer_state).
-author('schintke@onscale.de').
-vsn('$Id$').

%% Operations on proposer_state
-export([new/6, new/7]).
-export([get_replyto/1]).
-export([get_latest_value/1]).
-export([set_latest_value/2]).
-export([get_acceptors/1]).
-export([get_proposal/1]).
-export([get_majority/1]).
-export([get_max_proposers/1]).
-export([get_round/1]).
-export([set_round/2]).
-export([inc_round/1]).
-export([get_ack_count/1]).
-export([inc_ack_count/1]).
-export([add_ack_msg/4]).
-export([reset_state/1]).

%% proposer_state: {PaxosID,
%%                  ReplyTo,
%%                  Acceptors,
%%                  Own_proposal,
%%                  Majority,
%%                  MaxProposers,
%%                  Round, (current round)
%%                  RLast_highest,
%%                  Latest_value,
%%                  Ack_rec_count}
%% Sample: {[P1, P2, P3], 7, prepared, 3, abort, 3}

new(PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers) ->
    new(PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers, 0).

new(PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers, Round) ->
    {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers,
     Round, 0, paxos_no_value_yet, 0}.

get_replyto(State) ->           element(2, State).
get_acceptors(State) ->         element(3, State).
get_proposal(State) ->          element(4, State).
get_majority(State) ->          element(5, State).
get_max_proposers(State) ->     element(6, State).
get_round(State) ->             element(7, State).
set_round(State, Round) ->      setelement(7, State, Round).
get_latest_value(State) ->      element(9, State).
set_latest_value(State, Val) -> setelement(9, State, Val).
get_ack_count(State) ->         element(10, State).
inc_ack_count(State) ->         setelement(10, State,
                                           get_ack_count(State) + 1).
inc_round(State) ->
    set_round(State, get_round(State) + get_max_proposers(State)).

reset_state(State) ->
     {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers, Round, RLast, Value, _} = State,
     {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers, Round, RLast, Value, 0}.

add_ack_msg(InState, InAckRound, InAckValue, InAckRLast) ->
    {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers,
     StateRound, StateRLast_highest, StateLatestValue, AckRecCount}
        = InState,
    NewState =
        %% check r == state.Round == current?
        case InAckRound =:= StateRound of
            true ->
                %% increment AckRecCount
                %% if Rlast > state.RLast_highest
                %%   update Rlast_highest and Latest_value
                {NewRLast_highest, NewLatestValue} =
                    case InAckRLast > StateRLast_highest of
                        true -> {InAckRLast, InAckValue};
                        false -> {StateRLast_highest, StateLatestValue}
                    end,
                {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers,
                 StateRound, NewRLast_highest, NewLatestValue, AckRecCount + 1};
            false ->
                %% if InAckRound > StateRound ->
                %%   reset state and start counting for InAckRound again
                case (InAckRound > StateRound) of
                    false -> InState; %% old ack message -> drop it
                    true ->
                        % we only receive msgs for round we started ourselves
                        log:log(error, "~nmust not happen~n~n"),
                        InState
                end
        end,
    case get_ack_count(NewState) =:= get_majority(NewState) of
        %% if collected majority of answers
        %% ignore acks greater than majority. An 'accept'
        %%   was sent to all acceptors when majority was gathered
        true ->
            %% io:format("Proposer: majority acked in round ~p~n",
            %%           [get_round(NewState)]),
            {majority_acked,
             case get_latest_value(NewState) of
                 paxos_no_value_yet ->
                     set_latest_value(NewState, Proposal);
                 _ -> NewState
             end
            };
        false -> {ok, NewState}
    end.
    %% multicast accept(Round, Latest_value) to Acceptors
    %% (done in on() handler)
