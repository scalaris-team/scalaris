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
      {read, Key::?RT:key(), Value::any(), Version::pos_integer(), Client::comm:mypid(),
       Proposer::comm:mypid()}
      | {write, Key::?RT:key(), Value::any(), Version::pos_integer(), Client::comm:mypid(),
         Proposer::comm:mypid()}).

-spec ops_request(State::group_state:state(),
                  Proposal::proposal_type()) -> group_state:state().
ops_request(State, {read, _Key, _Value, _Version, Client, _Proposer} = Proposal) ->
    View = group_state:get_view(State),
    case group_paxos_utils:propose(Proposal, View) of
        {success, NewView} ->
            _PaxosId = group_view:get_next_expected_decision_id(View),
            ?LOG("read ~p in ~p~n", [_Key, _PaxosId]),
            group_state:set_view(State, NewView);
        _ ->
            comm:send(Client, {read_response, retry, propose_rejected}),
            State
    end;
ops_request(State, {write, Key, _Value, Version, Client, _Proposer} = Proposal) ->
    View = group_state:get_view(State),
    DB = group_state:get_db(State),
    case group_db:get_version(DB, Key) of
        unknown ->
            case group_paxos_utils:propose(Proposal, View) of
                {success, NewView} ->
                    _PaxosId = group_view:get_next_expected_decision_id(NewView),
                    group_state:set_view(State, NewView);
                _ ->
                    comm:send(Client, {write_response, retry, propose_rejected}),
                    State
            end;
        CurrentVersion ->
            case {Version =< CurrentVersion, group_paxos_utils:propose(Proposal, View)} of
                {true, _} ->
                    comm:send(Client, {write_response, retry, old_version}),
                    State;
                {false, {success, NewView}} ->
                    _PaxosId = group_view:get_next_expected_decision_id(NewView),
                    ?LOG("write ~p in ~p~n", [_Key, _PaxosId]),
                    group_state:set_view(State, NewView);
                _ ->
                    comm:send(Client, {write_response, retry, propose_rejected}),
                    State
            end
    end.

-spec ops_decision(State::group_state:state(),
                   Proposal::proposal_type(),
                   PaxosId::group_types:paxos_id(),
                   Hint::group_types:decision_hint()) -> group_state:state().
ops_decision(State, {read, _Key, Value, Version, Client, Proposer} = _Proposal,
             PaxosId, _Hint) ->
    View = group_state:get_view(State),
    NewView = group_view:remove_proposal(View, PaxosId),
    case Proposer == comm:this() of
        true ->
            comm:send(Client, {paxos_read_response, {value, Value, Version}}),
            group_state:set_view(State, NewView);
        false ->
            group_state:set_view(State, NewView)
    end;
ops_decision(State, {write, Key, Value, Version, Client, Proposer} = _Proposal,
             PaxosId, _Hint) ->
    DB = group_state:get_db(State),
    DB2 = group_db:write(DB, Key, Value, Version),
    View = group_state:get_view(State),
    NewView = group_view:remove_proposal(View, PaxosId),
    case Proposer == comm:this() of
        true ->
            comm:send(Client, {paxos_write_response, {ok, Version}}),
            group_state:set_view(group_state:set_db(State, DB2), NewView);
        false ->
            group_state:set_view(group_state:set_db(State, DB2), NewView)
    end.

-spec rejected_proposal(group_state:state(), proposal_type(),
                        group_types:paxos_id()) ->
    group_state:state().
rejected_proposal(State,
                  {read, _Key, _Value, _Version, Client, _Proposer},
                  _PaxosId) ->
    comm:send(Client, {paxos_read_response, {retry, different_proposal_accepted}}),
    State;
rejected_proposal(State,
                  {write, _Key, _Value, _Version, Client, _Proposer},
                  _PaxosId) ->
    comm:send(Client, {paxos_write_response, {retry, different_proposal_accepted}}),
    State.
