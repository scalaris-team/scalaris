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
-module(rsm_ops).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([execute_decision/3, report_rejection/3]).

-spec report_rejection(rsm_state:state(), rsm_state:paxos_id(),
                       rsm_state:proposal()) ->
    rsm_state:state().
report_rejection(State, PaxosId, Proposal) ->
    case Proposal of
        {add_node, _Pid} ->
            rsm_ops_add_node:rejected_proposal(State, Proposal, PaxosId);
        {remove_node, _Pid} ->
            rsm_ops_remove_node:rejected_proposal(State, Proposal, PaxosId);
        {deliver_message, _} ->
            rsm_ops_deliver:rejected_proposal(State, Proposal, PaxosId)
    end.

% @doc execute decision
-spec execute_decision(rsm_state:state(), rsm_state:paxos_id(),
                       rsm_state:proposal()) ->
    rsm_state:state().
execute_decision(State, PaxosId, Proposal) ->
    View = rsm_state:get_view(State),
    PaxosId = rsm_view:get_next_expected_decision_id(View), %assert
    case rsm_view:get_proposal(View, PaxosId) of
        {value, Proposal} -> % my proposal was accepted
            dispatch_decision(State, PaxosId, Proposal, my_proposal_won);
        none -> % I had no proposal for this paxos instance
            dispatch_decision(State, PaxosId, Proposal, had_no_proposal);
        {value, OtherProposal} -> % my proposal was rejected
            NewState = dispatch_decision(State, PaxosId, Proposal, had_no_proposal),
            rsm_ops:report_rejection(NewState, PaxosId, OtherProposal)
    end.

-spec dispatch_decision(rsm_state:state(), rsm_state:paxos_id(),
                        rsm_state:proposal(), rsm_state:decision_hint()) ->
    rsm_state:state().
dispatch_decision(State, PaxosId, {add_node, _, _, _} = Decision, Hint) ->
    rsm_ops_add_node:ops_decision(State, Decision, PaxosId, Hint);
dispatch_decision(State, PaxosId, {remove_node, _} = Decision, Hint) ->
    rsm_ops_remove_node:ops_decision(State, Decision, PaxosId, Hint);
dispatch_decision(State, PaxosId, {deliver, _Message, _Proposer} = Decision, Hint) ->
    rsm_ops_deliver:ops_decision(State, Decision, PaxosId, Hint).


