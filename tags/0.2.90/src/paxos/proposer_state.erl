% @copyright 2009-2011 Zuse Institute Berlin,
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
%% @version $Id$
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

-type proposer_state() ::
        { any(),            % paxos_id
          comm:mypid(),     % ReplyTo,
          [ comm:mypid() ], % Acceptors,
          any(),            % Own_proposal (e.g. prepared / abort),
          pos_integer(),    % Majority,
          pos_integer(),    % MaxProposers,
          non_neg_integer(),    % Round, (current round)
          non_neg_integer(),    % RLast_highest,
          any(),            % Latest_value,
          non_neg_integer()     % Ack_rec_count
        }.

-spec new(any(), comm:mypid(), [comm:mypid()], any(),
          pos_integer(), pos_integer()) -> proposer_state().
new(PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers) ->
    new(PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers, 0).

-spec new(any(), comm:mypid(), [comm:mypid()], any(), pos_integer(), pos_integer(), non_neg_integer()) -> proposer_state().
new(PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers, Round) ->
    {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers,
     Round, 0, paxos_no_value_yet, 0}.

-spec get_replyto(proposer_state()) -> comm:mypid().
get_replyto(State) ->           element(2, State).
-spec get_acceptors(proposer_state()) -> [ comm:mypid() ].
get_acceptors(State) ->         element(3, State).
-spec get_proposal(proposer_state()) -> any().
get_proposal(State) ->          element(4, State).
-spec get_majority(proposer_state()) -> pos_integer().
get_majority(State) ->          element(5, State).
-spec get_max_proposers(proposer_state()) -> pos_integer().
get_max_proposers(State) ->     element(6, State).
-spec get_round(proposer_state()) -> non_neg_integer().
get_round(State) ->             element(7, State).
-spec set_round(proposer_state(), non_neg_integer()) -> proposer_state().
set_round(State, Round) ->      setelement(7, State, Round).
-spec get_latest_value(proposer_state()) -> any().
get_latest_value(State) ->      element(9, State).
-spec set_latest_value(proposer_state(), any()) -> proposer_state().
set_latest_value(State, Val) -> setelement(9, State, Val).
-spec get_ack_count(proposer_state()) -> non_neg_integer().
get_ack_count(State) ->         element(10, State).
-spec inc_ack_count(proposer_state()) -> proposer_state().
inc_ack_count(State) ->         setelement(10, State,
                                           get_ack_count(State) + 1).
-spec inc_round(proposer_state()) -> proposer_state().
inc_round(State) ->
    set_round(State, get_round(State) + get_max_proposers(State)).

-spec reset_state(proposer_state()) -> proposer_state().
reset_state(State) ->
    setelement(10, State, 0).

-spec add_ack_msg(proposer_state(), non_neg_integer(), any(),
                  non_neg_integer()) -> {ok | majority_acked, proposer_state()}.
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
                        log:log(error, "~nmust not happen~n"),
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
