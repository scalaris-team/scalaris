% @copyright 2009, 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
%                 onScale solutions GmbH

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
-module(acceptor_state).
-author('schintke@onscale.de').
-vsn('$Id$').

%% Operations on acceptor_state
-export([new/1]).
-export([get_learners/1, set_learners/2]).
-export([get_raccepted/1]).
-export([get_value/1]).
-export([add_prepare_msg/2]).
-export([add_accept_msg/3]).

-type acceptor_state() ::
        { any(),             %% PaxosID,
          [ comm:mypid() ],  %% Learners,
          non_neg_integer(), %% Rack
          non_neg_integer() | -1, %% Raccepted
          any()              %% Value
        }.

-spec new(any()) -> acceptor_state().
new(PaxosID) ->
    {PaxosID, _Learners = [], _Rack = 0, _Raccepted = -1, paxos_no_value_yet}.
-spec get_learners(acceptor_state()) -> [ comm:mypid() ].
get_learners(State) ->           element(2, State).
-spec set_learners(acceptor_state(), [ comm:mypid() ]) -> acceptor_state().
set_learners(State, Learners) -> setelement(2, State, Learners).
-spec get_rack(acceptor_state()) -> non_neg_integer().
get_rack(State) ->             element(3, State).
-spec set_rack(acceptor_state(), non_neg_integer()) -> acceptor_state().
set_rack(State, Round) ->      setelement(3, State, Round).
-spec get_raccepted(acceptor_state()) -> non_neg_integer().
get_raccepted(State) ->        element(4, State).
-spec set_raccepted(acceptor_state(), non_neg_integer()) -> acceptor_state().
set_raccepted(State, Round) -> setelement(4, State, Round).
-spec get_value(acceptor_state()) -> any().
get_value(State) ->            element(5, State).
-spec set_value(acceptor_state(), any()) -> acceptor_state().
set_value(State, Value) ->     setelement(5, State, Value).

-spec add_prepare_msg(acceptor_state(), non_neg_integer()) ->
                      {ok, acceptor_state()} | {dropped, non_neg_integer()}.
add_prepare_msg(State, InRound) ->
    Rack = get_rack(State),
    case (InRound > Rack) andalso (InRound > get_raccepted(State)) of
        true -> {ok, set_rack(State, InRound)};
        false -> {dropped, Rack}
    end.

-spec add_accept_msg(acceptor_state(), non_neg_integer(), any()) ->
                      {ok, acceptor_state()} | {dropped, non_neg_integer()}.
add_accept_msg(State, InRound, InProposal) ->
    Rack = get_rack(State),
    case (InRound >= Rack) andalso (InRound > get_raccepted(State)) of
        true ->
            NewState1 = set_raccepted(State, InRound),
            NewState2 = set_value(NewState1, InProposal),
            {ok, NewState2};
        false -> {dropped, Rack}
    end.
