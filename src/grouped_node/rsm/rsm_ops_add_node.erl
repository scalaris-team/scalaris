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
-module(rsm_ops_add_node).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([ops_request/2, ops_decision/4, rejected_proposal/3]).

-ifndef(PRINT).
-define(PRINT(Var), ct:log("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-type(proposal_type()::{add_node, Pid::comm:mypid(),
                        Acceptor::comm:mypid(), Learner::comm:mypid()}).

% @doc we got a request to add a node to this rsm, do sanity checks and
%      propose the join
-spec ops_request(State::rsm_state:state(),
                  Proposal::proposal_type()) -> rsm_state:state().
ops_request(State, {add_node, Pid, Acceptor, Learner}) ->
    View = rsm_state:get_view(State),
    case rsm_view:is_member(View, Pid) of
        true ->
            comm:send(Pid, {add_node_response, is_already_member}),
            State;
        false ->
            case rsm_paxos_utils:propose({add_node, Pid, Acceptor,
                                            Learner}, View) of
                {success, NewView} ->
                    rsm_state:set_view(State, NewView);
                _ ->
                    comm:send(Pid, {add_node_response, retry, propose_rejected}),
                    State
            end
    end.

% @doc it was decided to add a node to our group: execute the join
-spec ops_decision(State::rsm_state:state(),
                   Proposal::proposal_type(),
                   PaxosId::rsm_state:paxos_id(),
                   Hint::rsm_state:decision_hint()) -> rsm_state:state().
ops_decision(State, {add_node, Pid, Acceptor, Learner} = _Proposal,
             PaxosId, _Hint) ->
    ?PRINT(Pid),
    %io:format("adding ~p at ~p~n", [Pid, PaxosId]),
    View = rsm_state:get_view(State),
    NewView = rsm_view:recalculate_index(rsm_view:add_node(
                                           rsm_view:remove_proposal(View, PaxosId),
                                           Pid, Acceptor, Learner)),
    NewState = rsm_state:set_view(State, NewView),
    comm:send(Pid, {state_update, NewState}),
    fd:subscribe(Pid),
    NewState.

-spec rejected_proposal(rsm_state:state(), proposal_type(),
                        rsm_state:paxos_id()) ->
    rsm_state:state().
rejected_proposal(State, {add_node, Pid, _Acceptor, _Learner},
                  _PaxosId) ->
    comm:send(Pid, {add_node_response, retry, different_proposal_accepted}),
    State.
