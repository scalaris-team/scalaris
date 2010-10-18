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
-module(group_ops_db).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("group.hrl").

-export([ops_request/2, ops_decision/4, rejected_proposal/3]).

-type(proposal_type()::
      {read, Key::?RT:key(), Value::any(), Client::comm:mypid(),
       Proposer::comm:mypid()}
      | {write, Key::?RT:key(), Value::any(), Version::pos_integer(), Client::comm:mypid(),
         Proposer::comm:mypid()}).

-spec ops_request(State::group_types:joined_state(),
                  Proposal::proposal_type()) -> group_types:joined_state().
ops_request({joined, NodeState, GroupState, TriggerState} = State,
            {read, Key, _Value, _Version, Client, _Proposer} = Proposal) ->
    case group_paxos_utils:propose(Proposal, GroupState) of
        {success, NewGroupState} ->
            PaxosId = group_state:get_next_expected_decision_id(NewGroupState),
            ?LOG("read ~p in ~p~n", [Key, PaxosId]),
            {joined, NodeState, NewGroupState, TriggerState};
        _ ->
            comm:send(Client, {read_response, retry, propose_rejected}),
            State
    end;
ops_request({joined, NodeState, GroupState, TriggerState} = State,
            {write, Key, _Value, _Version, Client, _Proposer} = Proposal) ->
    case group_paxos_utils:propose(Proposal, GroupState) of
        {success, NewGroupState} ->
            PaxosId = group_state:get_next_expected_decision_id(NewGroupState),
            ?LOG("write ~p in ~p~n", [Key, PaxosId]),
            {joined, NodeState, NewGroupState, TriggerState};
        _ ->
            comm:send(Client, {write_response, retry, propose_rejected}),
            State
    end.

-spec ops_decision(State::group_types:joined_state(),
                   Proposal::proposal_type(),
                   PaxosId::group_types:paxos_id(),
                   Hint::group_types:decision_hint()) -> group_types:joined_state().
ops_decision({joined, _NodeState, _GroupState, _TriggerState} = State,
             {read, _Key, Value, Version, Client, Proposer} = _Proposal,
             _PaxosId, _Hint) ->
    case Proposer == comm:this() of
        true ->
            comm:send(Client, {paxos_read_response, {value, Value, Version}}),
            State;
        false ->
            State
    end;
ops_decision({joined, NodeState, GroupState, TriggerState},
             {write, Key, Value, Version, Client, Proposer} = _Proposal,
             _PaxosId, _Hint) ->
    DB = ?DB:write(group_local_state:get_db(NodeState), Key, Value, Version),
    NewNodeState = group_local_state:set_db(NodeState, DB),
    case Proposer == comm:this() of
        true ->
            comm:send(Client, {paxos_write_response, {ok, Version}}),
            {joined, NewNodeState, GroupState, TriggerState};
        false ->
            {joined, NewNodeState, GroupState, TriggerState}
    end.

-spec rejected_proposal(group_types:joined_state(), proposal_type(),
                        group_types:paxos_id()) ->
    group_types:joined_state().
rejected_proposal(State,
                  {group_node_join, Pid, _Acceptor, _Learner},
                  _PaxosId) ->
    comm:send(Pid, {group_node_join_response, retry, different_proposal_accepted}),
    State.
