%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @version $Id$
-module(group_ops_split_group).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("group.hrl").

-export([ops_request/2, ops_decision/3, rejected_proposal/3]).

-type(proposal_type() :: {group_split, Proposer::comm:mypid(),
                          SplitKey::?RT:key(), LeftGroup::list(comm:mypid()),
                          RightGroup::list(comm:mypid())}).

% @doc we got a request to split this group, do sanity checks and propose the split
-spec ops_request(State::joined_state(),
                  Proposal::proposal_type()) -> joined_state().
ops_request({joined, NodeState, GroupState, TriggerState} = State,
            {group_split, Pid, SplitKey, LeftGroup, RightGroup} = Proposal) ->
    CurrentMemberList = group_state:get_members(GroupState),
    SplitIsInRange = intervals:in(SplitKey,
                                  group_state:get_interval(GroupState)),
    % is LeftGroup+RightGroup the current member list
    % and the SplitKey is in current range?
    case {lists:sort(LeftGroup ++ RightGroup) == lists:sort(CurrentMemberList),
          SplitIsInRange} of
        {true, true} ->
            case group_paxos_utils:propose(Proposal, GroupState) of
                {success, NewGroupState} ->
                    io:format("proposed split~n", []),
                    {joined, NodeState, NewGroupState, TriggerState};
                _ ->
                    comm:send(Pid, {group_split_response, retry}),
                    State
            end;
        {true, false} ->
            comm:send(Pid, {group_split_response, retry_key_out_of_range}),
            State;
        {false, true} ->
            comm:send(Pid, {group_split_response, retry_outdated_member_list}),
            State;
        {false, false} ->
            comm:send(Pid, {group_split_response, member_list_has_changed}),
            State
    end.

% @doc it was decided to split our group: execute the split
-spec ops_decision(State::joined_state(),
                   Proposal::proposal_type(),
                   PaxosId::any()) -> joined_state().
ops_decision({joined, NodeState, GroupState, TriggerState} = State,
             {group_split, Proposer, SplitKey, LeftGroup, RightGroup} = Proposal,
             PaxosId) ->
    io:format("decided split~n", []),
    CurrentPaxosId = group_state:get_next_expected_decision_id(GroupState),
    case CurrentPaxosId == PaxosId of
        true ->
            case group_state:get_proposal(GroupState, PaxosId) of
                {value, Proposal} -> %my_proposal_was_accepted
                    NewGroupState = execute_decision(GroupState, NodeState, Proposal),
                    comm:send(Proposer, {group_split_response, success}),
                    {joined, NodeState, NewGroupState,
                     TriggerState};
                none -> % I had no proposal for this paxos instance
                    NewGroupState = execute_decision(GroupState, NodeState, Proposal),
                    {joined, NodeState, NewGroupState, TriggerState};
                {value, OtherProposal} -> % my_proposal_was_rejected ->
                    NewGroupState = execute_decision(GroupState, NodeState, Proposal),
                    NewState = {joined, NodeState, NewGroupState, TriggerState},
                    group_ops:report_rejection(NewState, PaxosId, OtherProposal)
            end;
        false ->
            %@todo
            io:format("panic! 2~n", []),
            State
    end.

execute_decision(GroupState, NodeState, {group_split, _Proposer, SplitKey, LeftGroup, RightGroup}) ->
    NewGroupState = split_group(GroupState, SplitKey, LeftGroup,
                                RightGroup),
    group_utils:notify_neighbors(NodeState, GroupState,
                                 NewGroupState),
    update_fd(),
    NewGroupState.

-spec split_group(GroupState::group_state:group_state(),
                  SplitKey::?RT:key(),
                  LeftGroup::list(comm:mypid()),
                  RightGroup::list(comm:mypid())) ->
    group_state:group_state().
split_group(GroupState, SplitKey, LeftGroup, RightGroup) ->
    OldInterval = group_state:get_interval(GroupState),
    OldGroupId = group_state:get_group_id(GroupState),
    {'[', LowerBound, UpperBound, ')'} = intervals:get_bounds(OldInterval),
    case lists:member(comm:this(), LeftGroup) of
        true ->
            NewGroupId = group_state:get_new_group_id(OldGroupId, left),
            NewInterval = intervals:new('[', LowerBound, SplitKey, ')'),
            group_state:split_group(GroupState, NewGroupId, NewInterval,
                                         LeftGroup);
        false ->
            NewGroupId = group_state:get_new_group_id(OldGroupId, right),
            NewInterval = intervals:new('[', SplitKey, UpperBound, ')'),
            group_state:split_group(GroupState, NewGroupId, NewInterval,
                                         RightGroup)
    end.

update_fd() ->
    %@todo
    ok.

-spec rejected_proposal(joined_state(), proposal_type(), paxos_id()) ->
    joined_state().
rejected_proposal(State,
                  {group_split, Proposer, _SplitKey, _LeftGroup, _RightGroup},
                  _PaxosId) ->
    comm:send(Proposer, {group_split_response, retry}),
    State.
