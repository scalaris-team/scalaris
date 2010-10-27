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
-module(group_ops_join_node).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([ops_request/2, ops_decision/4, rejected_proposal/3]).

-type(proposal_type()::{group_node_join, Pid::comm:mypid(),
                        Acceptor::comm:mypid(), Learner::comm:mypid()}).

% @doc we got a request to join a node to from this group, do sanity checks and
%      propose the join
-spec ops_request(State::group_state:state(),
                  Proposal::proposal_type()) -> group_state:state().
ops_request(State, {group_node_join, Pid, Acceptor, Learner}) ->
    View = group_state:get_view(State),
    case group_view:is_member(View, Pid) of
        true ->
            comm:send(Pid, {group_node_join_response, is_already_member}),
            State;
        false ->
            case group_paxos_utils:propose({group_node_join, Pid, Acceptor,
                                            Learner}, View) of
                {success, NewView} ->
                    PaxosId = group_view:get_next_expected_decision_id(NewView),
                    io:format("proposed join of ~p in ~p~n", [Pid, PaxosId]),
                    group_state:set_view(State, NewView);
                _ ->
                    comm:send(Pid, {group_node_join_response, retry, propose_rejected}),
                    State
            end
    end.

% @doc it was decided to add a node to our group: execute the join
-spec ops_decision(State::group_state:state(),
                   Proposal::proposal_type(),
                   PaxosId::group_types:paxos_id(),
                   Hint::group_types:decision_hint()) -> group_state:state().
ops_decision(State, {group_node_join, Pid, Acceptor, Learner} = Proposal,
             PaxosId, _Hint) ->
    io:format("adding ~p at ~p~n", [Pid, PaxosId]),
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    NewView = group_view:recalculate_index(group_view:add_node(
                      group_view:remove_proposal(View,
                                                 PaxosId),
                      Pid, Acceptor, Learner)),
    group_utils:notify_neighbors(NodeState, View,
                                 NewView),
    Pred = group_local_state:get_predecessor(NodeState),
    Succ = group_local_state:get_successor(NodeState),
    comm:send(Pid, {group_state, NewView, Pred, Succ}),
    fd:subscribe(Pid),
    group_state:set_view(State, NewView).

-spec rejected_proposal(group_state:state(), proposal_type(),
                        group_types:paxos_id()) ->
    group_state:state().
rejected_proposal(State, {group_node_join, Pid, _Acceptor, _Learner},
                  _PaxosId) ->
    comm:send(Pid, {group_node_join_response, retry, different_proposal_accepted}),
    State.
