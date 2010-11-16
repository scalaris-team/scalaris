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

-export([ops_request/2, ops_decision/4, rejected_proposal/3]).

-type(proposal_type() :: {group_split, Proposer::comm:mypid(),
                          SplitKey::?RT:key(), LeftGroup::list(comm:mypid()),
                          RightGroup::list(comm:mypid())}).

% @doc we got a request to split this group, do sanity checks and propose the split
-spec ops_request(State::group_state:state(),
                  Proposal::proposal_type()) -> group_state:state().
ops_request(State, {group_split, Pid, SplitKey, LeftGroup, RightGroup} = Proposal) ->
    View = group_state:get_view(State),
    CurrentMemberList = group_view:get_members(View),
    SplitIsInRange = intervals:in(SplitKey,
                                  group_view:get_interval(View)),
    % is LeftGroup+RightGroup the current member list
    % and the SplitKey is in current range?
    case {lists:sort(LeftGroup ++ RightGroup) == lists:sort(CurrentMemberList),
          SplitIsInRange} of
        {true, true} ->
            case group_paxos_utils:propose(Proposal, View) of
                {success, NewView} ->
                    io:format("proposed split~n", []),
                    group_state:set_view(State, NewView);
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
-spec ops_decision(State::group_state:state(),
                   Proposal::proposal_type(),
                   PaxosId::any(), Hint::group_types:decision_hint()) ->
    group_state:state().
ops_decision(State,
             {group_split, Proposer, SplitKey, LeftGroup, RightGroup} = _Proposal,
             PaxosId, Hint) ->
    case Hint of
      my_proposal_won -> comm:send(Proposer, {group_split_response, success});
        _ -> ok
    end,
    io:format("decided split~n", []),
    View = group_state:get_view(State),
    NewView = group_view:recalculate_index(group_view:remove_proposal(
                      split_group(View, SplitKey, LeftGroup, RightGroup),
                      PaxosId)),
    NodeState = group_state:get_node_state(State),
    group_utils:notify_neighbors(NodeState, View, NewView),
    update_fd(),
    Range = group_view:get_interval(NewView),
    DB = group_state:get_db(State),
    DB2 = group_db:prune_out_of_range_entries(DB, Range),
    group_state:set_view(group_state:set_db(State, DB2), NewView).

-spec split_group(View::group_view:view(),
                  SplitKey::?RT:key(),
                  LeftGroup::list(comm:mypid()),
                  RightGroup::list(comm:mypid())) ->
    group_view:view().
split_group(View, SplitKey, LeftGroup, RightGroup) ->
    OldInterval = group_view:get_interval(View),
    OldGroupId = group_view:get_group_id(View),
    {Left, Right} = split_at(SplitKey, OldInterval),
    case lists:member(comm:this(), LeftGroup) of
        true ->
            NewGroupId = group_view:get_new_group_id(OldGroupId, left),
            group_view:split_group(View, NewGroupId, Left, LeftGroup);
        false ->
            NewGroupId = group_view:get_new_group_id(OldGroupId, right),
            group_view:split_group(View, NewGroupId, Right, RightGroup)
    end.

update_fd() ->
    %@todo
    ok.

-spec rejected_proposal(group_state:state(), proposal_type(),
                        group_types:paxos_id()) ->
    group_state:state().
rejected_proposal(State,
                  {group_split, Proposer, _SplitKey, _LeftGroup, _RightGroup},
                  _PaxosId) ->
    comm:send(Proposer, {group_split_response, retry}),
    State.

% @doc split given interval at SplitKey, respect previous brackets
split_at(SplitKey, OldInterval) ->
    {'[', LowerBound, UpperBound, UpperBr} = intervals:get_bounds(OldInterval),
     Left = intervals:new('[', LowerBound, SplitKey, ')'),
     Right = intervals:new('[', SplitKey, UpperBound, UpperBr),
                           {Left, Right}.
