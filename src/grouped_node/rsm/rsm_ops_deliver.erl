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
-module(rsm_ops_deliver).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([ops_request/2, ops_decision/4, rejected_proposal/3]).

-type(proposal_type()::{deliver, Message::any()}).

% @doc we got a request to deliver a message to this group
-spec ops_request(State::rsm_state:state(),
                  Proposal::proposal_type()) -> rsm_state:state().
ops_request(State, {deliver, Message, Proposer}) ->
    View = rsm_state:get_view(State),
    case rsm_paxos_utils:propose({deliver, Message, Proposer}, View) of
        {success, NewView} ->
            PaxosId = rsm_view:get_next_expected_decision_id(NewView),
            io:format("proposed deliver of ~p in ~p~n", [Message, PaxosId]),
            rsm_state:set_view(State, NewView);
        _ ->
            comm:send(Proposer, {deliver_response, retry, propose_rejected}),
            State
    end.

% @doc it was decided to add a node to our group: execute the join
-spec ops_decision(State::rsm_state:state(),
                   Proposal::proposal_type(),
                   PaxosId::rsm_state:paxos_id(),
                   Hint::rsm_state:decision_hint()) -> rsm_state:state().
ops_decision(State, {deliver, Message, Proposer} = _Proposal,
             PaxosId, _Hint) ->
    %io:format("adding ~p at ~p~n", [Pid, PaxosId]),
    View = rsm_state:get_view(State),
    NewView = rsm_view:increment_version(rsm_view:remove_proposal(View, PaxosId)),
    Module = rsm_state:get_app_module(State),
    AppState = rsm_state:get_app_state(State),
    NewAppState = Module:on(Message, AppState),
    rsm_state:set_app_state(rsm_state:set_view(State, NewView), NewAppState).

-spec rejected_proposal(rsm_state:state(), proposal_type(),
                        rsm_state:paxos_id()) ->
    rsm_state:state().
rejected_proposal(State, {add_node, Pid, _Acceptor, _Learner},
                  _PaxosId) ->
    comm:send(Pid, {add_node_response, retry, different_proposal_accepted}),
    State.
