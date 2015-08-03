% @copyright 2012 Zuse Institute Berlin

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

%% @author Magnus Mueller <mamuelle@informatik.hu-berlin.de>
%% @doc    Helper for rt_frt unit tests.
%% @end
%% @version $Id$
-module(rt_SUITE_rt_frt).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

number_to_key(N) -> N.

create_rt(RT_Keys, [_Succ | _DHTNodes] = Nodes, Neighbors) ->
    RT1 = gb_trees:from_orddict(
            [begin
                 Key = number_to_key(N),
                 {Key, {Key, 0, lists:nth(Idx, Nodes), none}}
             end || {N, Idx} <- RT_Keys]),
    RT2 = lists:foldl( fun(Node, AccIn) -> Id = node:id(Node),
                                           gb_trees:enter(Id, {Id, node:id_version(Node), node:pidX(Node), none}, AccIn)
                       end, RT1, nodelist:succs(Neighbors) ++ nodelist:preds(Neighbors)),
    {length(Nodes), RT2}.

check_next_hop(State, _Succ, N, NodeExp) ->
    Neighbors = dht_node_state:get(State, neighbors),
    ERT = dht_node_state:get(State, rt),
    ?equals_w_note(?RT:next_hop(Neighbors, ERT, number_to_key(N)), NodeExp, io_lib:format("~B", [N])).

-spec check_split_key_half(Begin::?RT:key(), End::?RT:key() | ?PLUS_INFINITY_TYPE, SplitKey::?RT:key()) -> true.
check_split_key_half(Begin, End, SplitKey) ->
    BeginToSplitKey = ?RT:get_range(Begin, SplitKey),
    SplitKeyToEnd = ?RT:get_range(SplitKey, End),
    ?equals_pattern_w_note(
        BeginToSplitKey,
        Result when Result == SplitKeyToEnd orelse Result == (SplitKeyToEnd - 1),
        io_lib:format("SplitKey: ~.0p", [SplitKey])).

-spec check_split_key(Begin::?RT:key(), End::?RT:key() | ?PLUS_INFINITY_TYPE, SplitKey::?RT:key(), {SplitFracA::1..100, SplitFracB::0..100}) -> true.
check_split_key(Begin, End, SplitKey, SplitFraction) ->
    FullRange = ?RT:get_range(Begin, End),
%%     ct:pal("FullRange: ~.0p", [FullRange]),
    BeginToSplitKey = case Begin of
                          SplitKey -> 0;
                          _ -> ?RT:get_range(Begin, SplitKey)
                      end,
    %%     ct:pal("BeginToSplitKeyRange: ~.0p, ~.0p", [BeginToSplitKey, SplitKey]),

    ?equals_pattern_w_note(
        BeginToSplitKey,
        Range when Range == (FullRange * erlang:element(1, SplitFraction)) div erlang:element(2, SplitFraction),
        io_lib:format("FullRange * Factor = ~.0p, SplitKey: ~.0p",
                      [(FullRange * erlang:element(1, SplitFraction)) div erlang:element(2, SplitFraction), SplitKey])).

additional_tests(_Config) ->
    tester:test(rt_frt, get_random_key_from_generator, 3, 2000, [{threads, 2}]) .
