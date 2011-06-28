%  @copyright 2007-2011 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Replicated-state machine
%% @end
%% @version $Id$
-module(rsm_tob).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([deliver/3]).

%-define(LOG(X,Y), ct:log(X,Y)).
-define(LOG(X,Y), ok).

-spec deliver(group_types:paxos_id(), group_types:proposal(),
              group_state:state()) -> group_state:state().
deliver(PaxosId, paxos_no_value_yet, State) ->
    % @todo
    Proposer = comm:make_global(pid_groups:get_my(paxos_proposer)),
    proposer:trigger(Proposer, PaxosId),
    State;
deliver(PaxosId, Proposal, State) ->
    View = rsm_state:get_view(State),
    NextPaxosId = rsm_view:get_next_expected_decision_id(View),
    IsCurrentDecision = is_current_decision(NextPaxosId, PaxosId),
    IsFutureDecision = is_future_decision(NextPaxosId, PaxosId),
    IsPastDecision = is_past_decision(NextPaxosId, PaxosId),
    IsDecisionFromOtherGroupId = is_decision_from_other_rsm_id(NextPaxosId, PaxosId),
    if
        IsCurrentDecision ->
            deliver_postponed_decisions(deliver_current_decision(PaxosId, Proposal, State));
        IsFutureDecision ->
            postpone_future_decision(PaxosId, Proposal, State);
        IsPastDecision ->
            State;
        IsDecisionFromOtherGroupId ->
            ?LOG("panic decision from other group !?!: ~p, ~p, ~p~n",
                      [NextPaxosId, PaxosId, Proposal]),
            State
    end.

% @doc deliver this decision
%      check whether any postponed decision may be delivered as well
%      cleanup old paxos state
%      trigger next paxos
-spec deliver_current_decision(rsm_state:paxos_id(), rsm_state:proposal(),
                               rsm_state:state()) ->
    rsm_state:state().
deliver_current_decision(PaxosId, Proposal, OldState) ->
    % deliver this decision
    NewState = rsm_ops:execute_decision(OldState, PaxosId, Proposal),
    % cleanup old paxos state
    rsm_paxos_utils:cleanup_paxos_states(PaxosId),
    % check for postponed decisions
    % trigger next paxos
    View = rsm_state:get_view(NewState),
    deliver_postponed_decisions(rsm_state:set_view(NewState, rsm_paxos_utils:init_paxos(View))).

% @doc postpone a future decision and trigger decision on intermediate paxos'
-spec postpone_future_decision(rsm_state:paxos_id(), rsm_state:proposal(),
                               rsm_state:state()) ->
    rsm_state:state().
postpone_future_decision({GroupId, Version} = PaxosId, Proposal,
                         State) ->
    View = rsm_state:get_view(State),
    {GroupId, CurrentVersion} = rsm_view:get_next_expected_decision_id(View),
    % trigger missing decisions
    MissingVersions = lists:seq(CurrentVersion, Version - 1),
    Proposer = comm:make_global(pid_groups:get_my(paxos_proposer)),
    % @todo is this the correct way to trigger the missing paxi?
    _ = [proposer:trigger(Proposer, {GroupId, V}) || V <- MissingVersions],
    rsm_state:set_view(State,
     rsm_view:postpone_decision(View, PaxosId, Proposal)).

% @doc check whether any postponed decisions can be delivered now
-spec deliver_postponed_decisions(rsm_state:state()) -> rsm_state:state().
deliver_postponed_decisions(OldState) ->
    View = rsm_state:get_view(OldState),
    NextPaxosId = rsm_view:get_next_expected_decision_id(View),
    PostponedDecisions = rsm_view:get_postponed_decisions(View),
    case lists:keyfind(NextPaxosId, 1, PostponedDecisions) of
        {NextPaxosId, Decision} ->
            NewView = rsm_view:remove_postponed_decision(View,
                                                         NextPaxosId,
                                                         Decision),
            NewState = rsm_state:set_view(OldState,
                                          NewView),
            deliver_postponed_decisions(rsm_ops:execute_decision(NewState,
                                                                 NextPaxosId,
                                                                 Decision));
        false ->
            OldState
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% total order over decisions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc is this the expected decision
-spec is_current_decision(rsm_state:paxos_id(), rsm_state:paxos_id()) -> boolean().
is_current_decision(NextPaxosId, PaxosId) ->
    PaxosId == NextPaxosId.

% @doc is this a future decision?
-spec is_future_decision(rsm_state:paxos_id(), rsm_state:paxos_id()) -> boolean().
is_future_decision({RSMId, ExpVersion} = _NextPaxosId, {RSMId, Version} = _PaxosId) ->
    ExpVersion < Version;
is_future_decision(_NextPaxosId, _PaxosId) ->
    false.

% @doc is this a previous decision?
-spec is_past_decision(rsm_state:paxos_id(), rsm_state:paxos_id()) -> boolean().
is_past_decision({GroupId, ExpVersion} = _NextPaxosId, {GroupId, Version} = _PaxosId) ->
    Version < ExpVersion;
is_past_decision(_NextPaxosId, _PaxosId) ->
    false.

% @doc is this a previous decision?
-spec is_decision_from_other_rsm_id(rsm_state:paxos_id(), rsm_state:paxos_id()) -> boolean().
is_decision_from_other_rsm_id({RSMId, _ExpVersion} = _NextPaxosId, {RSMId, _Version} = _PaxosId) ->
    false;
is_decision_from_other_rsm_id(_NextPaxosId, _PaxosId) ->
    true.

