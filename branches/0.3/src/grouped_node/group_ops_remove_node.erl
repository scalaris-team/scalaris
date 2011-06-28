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
%% @version $Id$
-module(group_ops_remove_node).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([ops_request/2, ops_decision/4, rejected_proposal/3]).

-type(proposal_type()::{group_node_remove, Pid::comm:mypid(), Proposer::comm:mypid()}).

% @doc we got a request to remove a node from this group, do sanity checks and propose the removal
-spec ops_request(State::group_state:state(),
                  Proposal::proposal_type()) -> group_state:state().
ops_request(State, {group_node_remove, DeadPid, Proposer} = _Proposal) ->
    View = group_state:get_view(State),
    case group_view:is_member(View, DeadPid) of
        false ->
            comm:send(Proposer, {group_node_remove_response,
                                 is_no_member, DeadPid}),
            State;
        true ->
            case group_paxos_utils:propose({group_node_remove, DeadPid, Proposer}, View) of
                {success, NewView} ->
                    group_state:set_view(State, NewView);
                _ ->
                    comm:send(Proposer, {group_node_remove_response, retry, DeadPid}),
                    State
            end
    end.


% @doc it was decided to remove a node from our group: execute the removal
-spec ops_decision(State::group_state:state(),
                   Proposal::proposal_type(),
                   PaxosId::group_types:paxos_id(),
                   Hint::group_types:decision_hint()) -> group_state:state().
ops_decision(State, {group_node_remove, Pid, _Proposer} = _Proposal, PaxosId, _Hint) ->
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    NewView = group_view:recalculate_index(group_view:remove_node(
                      group_view:remove_proposal(View,
                                                  PaxosId),
                      Pid)),
    group_utils:notify_neighbors(NodeState, View, NewView),
    % notify dead node
    Pred = group_local_state:get_predecessor(NodeState),
    Succ = group_local_state:get_successor(NodeState),
    comm:send(Pid, {group_state, NewView, Pred, Succ}),
    fd:unsubscribe(Pid),
    group_state:set_view(State, NewView).

-spec rejected_proposal(group_state:state(), proposal_type(),
                        group_types:paxos_id()) ->
    group_state:state().
rejected_proposal(State, {group_node_remove, Pid, Proposer}, _PaxosId) ->
    comm:send(Proposer, {group_node_remove_response, retry, Pid}),
    State.
