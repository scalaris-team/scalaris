% @copyright 2009-2012 Zuse Institute Berlin,
%                      onScale solutions GmbH

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

-compile({inline, [state_get_replyto/1,
                   state_get_acceptors/1,
                   state_get_proposal/1,
                   state_get_majority/1,
                   state_get_max_proposers/1,
                   state_get_round/1, state_set_round/2,
                   state_get_latest_value/1, state_set_latest_value/2,
                   state_get_ack_count/1, %state_inc_ack_count/1,
                   state_inc_round/1,
                   state_reset_state/1
                   ]}).

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

-spec state_new(any(), comm:mypid(), [comm:mypid()], any(), pos_integer(), pos_integer(), non_neg_integer()) -> proposer_state().
state_new(PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers, Round) ->
    {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers,
     Round, 0, paxos_no_value_yet, 0}.

-spec state_get_replyto(proposer_state()) -> comm:mypid().
state_get_replyto(State) ->           element(2, State).
-spec state_get_acceptors(proposer_state()) -> [ comm:mypid() ].
state_get_acceptors(State) ->         element(3, State).
-spec state_get_proposal(proposer_state()) -> any().
state_get_proposal(State) ->          element(4, State).
-spec state_get_majority(proposer_state()) -> pos_integer().
state_get_majority(State) ->          element(5, State).
-spec state_get_max_proposers(proposer_state()) -> pos_integer().
state_get_max_proposers(State) ->     element(6, State).
-spec state_get_round(proposer_state()) -> non_neg_integer().
state_get_round(State) ->             element(7, State).
-spec state_set_round(proposer_state(), non_neg_integer()) -> proposer_state().
state_set_round(State, Round) ->      setelement(7, State, Round).
-spec state_get_latest_value(proposer_state()) -> any().
state_get_latest_value(State) ->      element(9, State).
-spec state_set_latest_value(proposer_state(), any()) -> proposer_state().
state_set_latest_value(State, Val) -> setelement(9, State, Val).
-spec state_get_ack_count(proposer_state()) -> non_neg_integer().
state_get_ack_count(State) ->         element(10, State).
%% -spec state_inc_ack_count(proposer_state()) -> proposer_state().
%% state_inc_ack_count(State) ->         setelement(10, State,
%%                                            state_get_ack_count(State) + 1).
-spec state_inc_round(proposer_state()) -> proposer_state().
state_inc_round(State) ->
    state_set_round(State, state_get_round(State) + state_get_max_proposers(State)).

-spec state_reset_state(proposer_state()) -> proposer_state().
state_reset_state(State) ->           setelement(10, State, 0).

-compile({nowarn_unused_function, state_add_ack_msg_feeder/4}).
-spec state_add_ack_msg_feeder(InState, InAckRound, InAckValue, InAckRLast)
        -> {InState, InAckRound, InAckValue, InAckRLast}
            when is_subtype(InState, proposer_state()),
                 is_subtype(InAckRound, non_neg_integer()),
                 is_subtype(InAckValue, any()),
                 is_subtype(InAckRLast, non_neg_integer()).
state_add_ack_msg_feeder(InState, InAckRound, InAckValue, InAckRLast) ->
    StateRound = state_get_round(InState),
    {state_set_round(InState, erlang:max(StateRound, InAckRound)),
     erlang:min(StateRound, InAckRound), InAckValue, InAckRLast}.

-spec state_add_ack_msg(proposer_state(), non_neg_integer(), any(),
                        non_neg_integer()) -> {ok | majority_acked, proposer_state()}.
state_add_ack_msg(InState, InAckRound, InAckValue, InAckRLast) ->
    {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers,
     StateRound, StateRLast_highest, StateLatestValue, AckRecCount}
        = InState,
    NewState =
        %% check r == state.Round == current?
        if InAckRound =:= StateRound ->
               %% increment AckRecCount
               %% if Rlast > state.RLast_highest
               %%   update Rlast_highest and Latest_value
               {NewRLast_highest, NewLatestValue} =
                   if InAckRLast > StateRLast_highest ->
                          {InAckRLast, InAckValue};
                      true ->
                          {StateRLast_highest, StateLatestValue}
                   end,
               {PaxosID, ReplyTo, Acceptors, Proposal, Majority, MaxProposers,
                StateRound, NewRLast_highest, NewLatestValue, AckRecCount + 1};
           true ->
               %% InAckRound =< StateRound -> old ack message, drop it
               %% InAckRound > StateRound should not happen
               %% (we only receive msgs for round we started ourselves)
               ?DBG_ASSERT2(InAckRound =< StateRound,
                            {got_higher_round, InAckRound, StateRound}),
               InState
        end,
    case state_get_ack_count(NewState) =:= state_get_majority(NewState) of
        %% if collected majority of answers
        %% ignore acks greater than majority. An 'accept'
        %%   was sent to all acceptors when majority was gathered
        true ->
            %% io:format("Proposer: majority acked in round ~p~n",
            %%           [get_round(NewState)]),
            {majority_acked,
             case state_get_latest_value(NewState) of
                 paxos_no_value_yet ->
                     state_set_latest_value(NewState, Proposal);
                 _ -> NewState
             end
            };
        false -> {ok, NewState}
    end.
    %% multicast accept(Round, Latest_value) to Acceptors
    %% (done in on() handler)
