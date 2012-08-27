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

%%% @author Magnus Mueller <mamuelle@informatik.hu-berlin.de>
%%% @doc    Helper for rt_frtchord unit tests.
%%% @end
%% @version $Id$
-module(rt_SUITE_rt_frtchord).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

number_to_key(N) -> N.

create_rt(RT_Keys, [_Succ | _DHTNodes] = Nodes) ->
    gb_trees:from_orddict(
      [begin
           Key = number_to_key(N),
           {Key, node:new(lists:nth(Idx, Nodes), Key, 0)}
       end || {N, Idx} <- RT_Keys]).

check_next_hop(State, _Succ, N, NodeExp) ->
    ?equals_w_note(?RT:next_hop(State, number_to_key(N)), NodeExp, io_lib:format("~B", [N])).

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
    test_rt_integrity().

% set up routing tables and check that the entries are well connected
test_rt_integrity() ->
    MyNode = node:new(self(), number_to_key(0), 0),
    pid_groups:join_as("rt_SUITE", dht_node),
    Neighbors = nodelist:new_neighborhood(MyNode),
    RT = ?RT:init(Neighbors), % this will send a message which we will ignore
    test_rt_integrity_init(RT, MyNode),
    test_rt_integrity_init_stabilize_1(RT, MyNode),
    test_rt_integrity_init_stabilize_2(RT, MyNode)
    .

assert_connected(RT) -> 
    ?assert_w_note(rt_frtchord:check_rt_integrity(RT), true)
    .

test_rt_integrity_init(RT, _SourceNode) ->
    assert_connected(RT).

test_rt_integrity_init_stabilize_1(RT, SourceNode) ->
    % change the neighborhood and recheck the rt integrity
    Pred = node:new(rt_SUITE:fake_dht_node(".pred"), number_to_key(random:uniform(10000)), 0),
    Neighbors = nodelist:new_neighborhood(Pred, SourceNode),
    NewRT = ?RT:init_stabilize(Neighbors, RT),
    assert_connected(NewRT)
    .

test_rt_integrity_init_stabilize_2(RT, SourceNode) ->
    % change the neighborhood and recheck the rt integrity
    Succ = node:new(rt_SUITE:fake_dht_node(".succ"), number_to_key(random:uniform(10000)), 0),
    Pred = node:new(rt_SUITE:fake_dht_node(".pred"), number_to_key(random:uniform(10000)), 0),
    Neighbors = nodelist:new_neighborhood(Pred, SourceNode, Succ),
    NewRT = ?RT:init_stabilize(Neighbors, RT),
    assert_connected(NewRT)
    .
