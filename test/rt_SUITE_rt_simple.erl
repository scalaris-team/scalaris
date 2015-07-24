% @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Helper for rt_chord unit tests.
%% @end
%% @version $Id$
-module(rt_SUITE_rt_simple).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

number_to_key(N) -> N.

create_rt(_RT_Keys, [Succ | _DHTNodes] = _Nodes, _Neighbors) ->
    node:new(Succ, number_to_key(1), 0).

check_next_hop(State, Succ, N, _NodeExp) ->
    Neighbors = dht_node_state:get(State, neighbors),
    ERT = dht_node_state:get(State, rt),
    Exp = case intervals:in(number_to_key(N), nodelist:succ_range(Neighbors)) of
              true -> succ;
              false -> Succ
          end,
    ?equals_w_note(?RT:next_hop(Neighbors, ERT, number_to_key(N)), Exp, io_lib:format("~B", [N])).

-spec check_split_key_half(Begin::?RT:key(), End::?RT:key() | ?PLUS_INFINITY_TYPE, SplitKey::?RT:key()) -> true.
check_split_key_half(Begin, End, SplitKey) ->
    rt_SUITE_rt_chord:check_split_key_half(Begin, End, SplitKey).

-spec check_split_key(Begin::?RT:key(), End::?RT:key() | ?PLUS_INFINITY_TYPE, SplitKey::?RT:key(), {SplitFracA::1..100, SplitFracB::0..100}) -> true.
check_split_key(Begin, End, SplitKey, SplitFraction) ->
    rt_SUITE_rt_chord:check_split_key(Begin, End, SplitKey, SplitFraction).

additional_tests(_Config) ->
    ok.
