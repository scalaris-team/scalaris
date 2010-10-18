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

-export([execute_decision/3, report_rejection/3]).

-spec report_rejection(group_types:joined_state(), group_types:paxos_id(),
                       group_types:proposal()) ->
    group_types:joined_state().
report_rejection(State, PaxosId, Proposal) ->
    case Proposal of
        {group_split, _Pid, _SplitKey, _LeftGroup, _RightGroup} ->
            group_ops_split_group:rejected_proposal(State, Proposal, PaxosId);
        {group_node_remove, _Pid} ->
            group_ops_remove_node:rejected_proposal(State, Proposal, PaxosId);
        {group_node_join, _Pid, _Acceptor, _Learner} ->
            group_ops_join_node:rejected_proposal(State, Proposal, PaxosId);
        {read, _, _, _, _, _} ->
            group_ops_db:rejected_proposal(State, Proposal, PaxosId);
        {write, _, _, _, _, _} ->
            group_ops_db:rejected_proposal(State, Proposal, PaxosId)
    end.

% @doc execute decision
-spec execute_decision(group_types:joined_state(), group_types:paxos_id(),
                       group_types:proposal()) ->
    group_types:joined_state().
execute_decision({joined, _, GroupState, _} = State, PaxosId, Proposal) ->
    PaxosId = group_state:get_next_expected_decision_id(GroupState), %assert
    case group_state:get_proposal(GroupState, PaxosId) of
        {value, Proposal} -> % my proposal was accepted
            dispatch_decision(State, PaxosId, Proposal, my_proposal_won);
        none -> % I had no proposal for this paxos instance
            dispatch_decision(State, PaxosId, Proposal, had_no_proposal);
        {value, OtherProposal} -> % my proposal was rejected
            NewState = dispatch_decision(State, PaxosId, Proposal, had_no_proposal),
            group_ops:report_rejection(NewState, PaxosId, OtherProposal)
    end.

-spec dispatch_decision(group_types:joined_state(), group_types:paxos_id(),
                        group_types:proposal(), group_types:decision_hint()) ->
    group_types:joined_state().
dispatch_decision(State, PaxosId, {group_split, _, _, _, _} = Decision, Hint) ->
    group_ops_split_group:ops_decision(State, Decision, PaxosId, Hint);
dispatch_decision(State, PaxosId, {group_node_remove, _} = Decision, Hint) ->
    group_ops_remove_node:ops_decision(State, Decision, PaxosId, Hint);
dispatch_decision(State, PaxosId, {group_node_join, _, _, _} = Decision, Hint) ->
    group_ops_join_node:ops_decision(State, Decision, PaxosId, Hint);
dispatch_decision(State, PaxosId, {read, _, _, _, _, _} = Decision, Hint) ->
    group_ops_db:ops_decision(State, Decision, PaxosId, Hint);
dispatch_decision(State, PaxosId, {write, _, _, _, _, _} = Decision, Hint) ->
    group_ops_db:ops_decision(State, Decision, PaxosId, Hint).


