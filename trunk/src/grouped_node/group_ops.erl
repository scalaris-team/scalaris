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
-module(group_ops).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("group.hrl").

-export([execute_decision/3, report_rejection/3]).

-spec report_rejection(joined_state(), paxos_id(), proposal()) ->
    joined_state().
report_rejection(State, PaxosId, Proposal) ->
    case Proposal of
        {group_split, _Pid, _SplitKey, _LeftGroup, _RightGroup} ->
            group_ops_split_group:rejected_proposal(State, Proposal,
                                                    PaxosId);
        {group_node_remove, _Pid} ->
            group_ops_remove_node:rejected_proposal(State, Proposal,
                                                    PaxosId);
        {group_node_join, _Pid, _Acceptor, _Learner} ->
            group_ops_join_node:rejected_proposal(State, Proposal,
                                                  PaxosId)
    end.

% @doc execute decision
-spec execute_decision(joined_state(), paxos_id(), proposal()) ->
    joined_state().
execute_decision(State, PaxosId, Decision) ->
    case Decision of
        {group_split, _Pid, _SplitKey, _LeftGroup, _RightGroup} ->
            group_ops_split_group:ops_decision(State, Decision,
                                               PaxosId);
        {group_node_remove, _Pid} ->
            group_ops_remove_node:ops_decision(State, Decision,
                                               PaxosId);
        {group_node_join, _Pid, _Acceptor, _Learner} ->
            group_ops_join_node:ops_decision(State, Decision,
                                             PaxosId)
    end.

