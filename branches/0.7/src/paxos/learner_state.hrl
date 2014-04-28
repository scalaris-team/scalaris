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
%%      The state needed for a single learner instance.
%% @end
%% @version $Id$

-compile({inline, [%state_get_paxosid/1,
                   state_get_majority/1, state_set_majority/2,
                   state_get_process_to_inform/1, state_set_process_to_inform/2,
                   state_get_client_cookie/1, state_set_client_cookie/2,
                   state_get_accepted_count/1, state_set_accepted_count/2,
                   state_inc_accepted_count/1,
                   state_get_round/1, state_set_round/2,
                   state_get_value/1, state_set_value/2
                  ]}).

-type proc_to_inform() :: comm:mypid() | none.
-type learner_state() ::
        {any(),             % PaxosID,
         pos_integer(),     % Majority,
         proc_to_inform(),  % ProcessToInform
         any(),             % ClientCookie
         non_neg_integer(), % AcceptedCount
         non_neg_integer(), % Round
         any()              % Value
        }.
%% Value stored to accept messages for a paxos id before learner is
%% initialized. (and for sanity checks)

-spec state_new(any(), pos_integer(), proc_to_inform(), any()) -> learner_state().
state_new(PaxosID, Majority, ProcessToInform, ClientCookie) ->
    {PaxosID, Majority, ProcessToInform, ClientCookie,
     0, 0, paxos_no_value_yet}.

%% -spec state_get_paxosid(learner_state()) -> any().
%% state_get_paxosid(State) -> element(1, State).
-spec state_get_majority(learner_state()) -> pos_integer().
state_get_majority(State) -> element(2, State).
-spec state_set_majority(learner_state(), pos_integer()) -> learner_state().
state_set_majority(State, Majority) -> setelement(2, State, Majority).
-spec state_get_process_to_inform(learner_state()) -> proc_to_inform().
state_get_process_to_inform(State) -> element(3, State).
-spec state_set_process_to_inform(learner_state(), comm:mypid()) -> learner_state().
state_set_process_to_inform(State, Pid) -> setelement(3, State, Pid).
-spec state_get_client_cookie(learner_state()) -> any().
state_get_client_cookie(State) -> element(4, State).
-spec state_set_client_cookie(learner_state(), any()) -> learner_state().
state_set_client_cookie(State, Pid) -> setelement(4, State, Pid).
-spec state_get_accepted_count(learner_state()) -> non_neg_integer().
state_get_accepted_count(State) -> element(5, State).
-spec state_set_accepted_count(learner_state(), non_neg_integer()) -> learner_state().
state_set_accepted_count(State, Num) -> setelement(5, State, Num).
-spec state_inc_accepted_count(learner_state()) -> learner_state().
state_inc_accepted_count(State) -> setelement(5, State, element(5, State) + 1).
-spec state_get_round(learner_state()) -> non_neg_integer().
state_get_round(State) -> element(6, State).
-spec state_set_round(learner_state(), non_neg_integer()) -> learner_state().
state_set_round(State, Round) -> setelement(6, State, Round).
-spec state_get_value(learner_state()) -> any().
state_get_value(State) -> element(7, State).
-spec state_set_value(learner_state(), any()) -> learner_state().
state_set_value(State, Value) -> setelement(7, State, Value).

-spec state_add_accepted_msg(learner_state(), non_neg_integer(), any())
        -> dropped | {ok | majority_accepted, learner_state()}.
state_add_accepted_msg(State, Round, Value) ->
    StateRound = state_get_round(State),
    if Round >= StateRound ->
           TmpState = if Round > StateRound ->
                             % reset round and accepted:
                             state_set_round(
                               state_set_accepted_count(State, 0), Round);
                         true -> State
                      end,
           Tmp2State = state_set_value(TmpState, Value),
           NewState = state_inc_accepted_count(Tmp2State),
           case state_get_accepted_count(NewState)
                    =:= state_get_majority(NewState) of
               true -> {majority_accepted, NewState};
               false -> {ok, NewState}
           end;
       true -> dropped % outdated round, silently drop it
    end.
