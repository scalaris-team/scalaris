% @copyright 2009-2012 Zuse Institute Berlin,
%                      onScale solutions GmbH

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Part of generic Paxos-Consensus implementation
%%           The state needed for a single acceptor instance.
%% @end
%% @version $Id$

-compile({inline, [state_get_learners/1, state_set_learners/2,
                   state_get_rack/1, state_set_rack/2,
                   state_get_raccepted/1, state_set_raccepted/2,
                   state_get_value/1, state_set_value/2
                   ]}).

-type acceptor_state() ::
        { any(),             %% PaxosID,
          [ comm:mypid() ],  %% Learners,
          non_neg_integer(), %% Rack
          non_neg_integer() | -1, %% Raccepted
          any()              %% Value
        }.

-spec state_new(any()) -> acceptor_state().
state_new(PaxosID) ->
    {PaxosID, _Learners = [], _Rack = 0, _Raccepted = -1, paxos_no_value_yet}.
-spec state_get_learners(acceptor_state()) -> [ comm:mypid() ].
state_get_learners(State) ->           element(2, State).
-spec state_set_learners(acceptor_state(), [ comm:mypid() ]) -> acceptor_state().
state_set_learners(State, Learners) -> setelement(2, State, Learners).
-spec state_get_rack(acceptor_state()) -> non_neg_integer().
state_get_rack(State) ->             element(3, State).
-spec state_set_rack(acceptor_state(), non_neg_integer()) -> acceptor_state().
state_set_rack(State, Round) ->      setelement(3, State, Round).
-spec state_get_raccepted(acceptor_state()) -> non_neg_integer() | -1.
state_get_raccepted(State) ->        element(4, State).
-spec state_set_raccepted(acceptor_state(), non_neg_integer()) -> acceptor_state().
state_set_raccepted(State, Round) -> setelement(4, State, Round).
-spec state_get_value(acceptor_state()) -> any().
state_get_value(State) ->            element(5, State).
-spec state_set_value(acceptor_state(), any()) -> acceptor_state().
state_set_value(State, Value) ->     setelement(5, State, Value).

-spec state_add_prepare_msg(acceptor_state(), non_neg_integer())
        -> {ok, acceptor_state()} | {dropped, non_neg_integer()}.
state_add_prepare_msg(State, InRound) ->
    Rack = state_get_rack(State),
    case (InRound > Rack) andalso (InRound > state_get_raccepted(State)) of
        true -> {ok, state_set_rack(State, InRound)};
        false -> {dropped, Rack}
    end.

-spec state_add_accept_msg(acceptor_state(), non_neg_integer(), any())
        -> {ok, acceptor_state()} | {dropped, non_neg_integer()}.
state_add_accept_msg(State, InRound, InProposal) ->
    Rack = state_get_rack(State),
    case (InRound >= Rack) andalso (InRound > state_get_raccepted(State)) of
        true ->
            NewState1 = state_set_raccepted(State, InRound),
            NewState2 = state_set_value(NewState1, InProposal),
            {ok, NewState2};
        false -> {dropped, Rack}
    end.

-spec state_accepted(acceptor_state()) -> boolean().
state_accepted(State) -> paxos_no_value_yet =/= state_get_value(State).
