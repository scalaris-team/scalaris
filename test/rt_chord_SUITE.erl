% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Unit tests for src/rt_chord.erl.
%%% @end
%% @version $Id$
-module(rt_chord_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [next_hop, next_hop2, tester_get_split_key, tester_get_split_key_half].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    ok = unittest_helper:fix_cwd(),
    error_logger:tty(true),
    Pid = unittest_helper:start_process(
            fun() ->
                    {ok, _GroupsPid} = pid_groups:start_link(),
                    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
                    {ok, _ConfigPid} = config:start_link2([{config, [{log_path, PrivDir}]}]),
                    {ok, _LogPid} = log:start_link(),
                    {ok, _CommPid} = sup_comm_layer:start_link(),
                    comm_server:set_local_address({127,0,0,1}, unittest_helper:get_scalaris_port())
            end),
    [{wrapper_pid, Pid} | Config2].

end_per_suite(Config) ->
    {wrapper_pid, Pid} = lists:keyfind(wrapper_pid, 1, Config),
    error_logger:tty(false),
    log:set_log_level(none),
    exit(Pid, kill),
    unittest_helper:stop_pid_groups(),
    _ = unittest_helper:end_per_suite(Config),
    ok.

next_hop(_Config) ->
    MyNode = node:new(self(), 0, 0),
    pid_groups:join_as("performance_SUITE", dht_node),
    Succ = node:new(fake_dht_node(".succ"), 1, 0),
    Pred = node:new(fake_dht_node(".pred"), 1000000, 0),
    Neighbors = nodelist:new_neighborhood(Pred, MyNode, Succ),
    DHTNodes = [fake_dht_node(io_lib:format(".node~B", [X])) || X <- lists:seq(1, 6)],
    RT = gb_trees:from_orddict([{1, Succ},
                                {2, node:new(lists:nth(1, DHTNodes), 2, 0)},
                                {4, node:new(lists:nth(2, DHTNodes), 4, 0)},
                                {8, node:new(lists:nth(3, DHTNodes), 8, 0)},
                                {16, node:new(lists:nth(4, DHTNodes), 16, 0)},
                                {32, node:new(lists:nth(5, DHTNodes), 32, 0)},
                                {64, node:new(lists:nth(6, DHTNodes), 64, 0)}]),
    RMState = rm_loop:unittest_create_state(Neighbors, false),
    % note: dht_node_state:new/3 will call pid_groups:get_my(paxos_proposer)
    % which will fail here -> however, we don't need this process
    DB = ?DB:new(),
    State = dht_node_state:new(RT, RMState, DB),
    config:write(rt_size_use_neighbors, 0),
    ?equals(rt_chord:next_hop(State, 0), lists:nth(6, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 1), node:pidX(Succ)), % succ is responsible
    ?equals(rt_chord:next_hop(State, 2), node:pidX(Succ)),
    ?equals(rt_chord:next_hop(State, 3), lists:nth(1, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 7), lists:nth(2, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 9), lists:nth(3, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 31), lists:nth(4, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 64), lists:nth(5, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 65), lists:nth(6, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 1000), lists:nth(6, DHTNodes)),
    
    [exit(Node, kill) || Node <- DHTNodes],
%%     exit(node:pidX(MyNode), kill),
    exit(node:pidX(Succ), kill),
    exit(node:pidX(Pred), kill),
    ?DB:close(DB),
    ok.

next_hop2(_Config) ->
    MyNode = node:new(self(), 0, 0),
    pid_groups:join_as("performance_SUITE", dht_node),
    Succ = node:new(fake_dht_node(".succ"), 1, 0),
    SuccSucc = node:new(fake_dht_node(".succ.succ"), 2, 0),
    Pred = node:new(fake_dht_node(".pred"), 1000000, 0),
    DHTNodes = [fake_dht_node(io_lib:format(".node~B", [X])) || X <- lists:seq(1, 6)],
    RT = gb_trees:from_orddict([{1, Succ},
                                {4, node:new(lists:nth(2, DHTNodes), 4, 0)},
                                {8, node:new(lists:nth(3, DHTNodes), 8, 0)},
                                {16, node:new(lists:nth(4, DHTNodes), 16, 0)},
                                {32, node:new(lists:nth(5, DHTNodes), 32, 0)},
                                {64, node:new(lists:nth(6, DHTNodes), 64, 0)}]),
    Neighbors = nodelist:add_node(nodelist:new_neighborhood(Pred, MyNode, Succ),
                                  SuccSucc, 2, 2),
    RMState = rm_loop:unittest_create_state(Neighbors, false),
    % note: dht_node_state:new/3 will call pid_groups:get_my(paxos_proposer)
    % which will fail here -> however, we don't need this process
    DB = ?DB:new(),
    State = dht_node_state:new(RT, RMState, DB),
    config:write(rt_size_use_neighbors, 10),
    ?equals(rt_chord:next_hop(State, 0), node:pidX(Pred)),
    ?equals(rt_chord:next_hop(State, 1), node:pidX(Succ)), % succ is responsible
    ?equals(rt_chord:next_hop(State, 2), node:pidX(Succ)),
    ?equals(rt_chord:next_hop(State, 3), node:pidX(SuccSucc)),
    ?equals(rt_chord:next_hop(State, 7), lists:nth(2, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 9), lists:nth(3, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 31), lists:nth(4, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 64), lists:nth(5, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 65), lists:nth(6, DHTNodes)),
    ?equals(rt_chord:next_hop(State, 1000), lists:nth(6, DHTNodes)),

    [exit(Node, kill) || Node <- DHTNodes],
%%     exit(node:pidX(MyNode), kill),
    exit(node:pidX(Succ), kill),
    exit(node:pidX(SuccSucc), kill),
    exit(node:pidX(Pred), kill),
    ?DB:close(DB),
    ok.

-spec prop_get_split_key_half(Begin::rt_chord:key(), End::rt_chord:key()) -> true.
prop_get_split_key_half(Begin, End) ->
    SplitKey = rt_chord:get_split_key(Begin, End, {1, 2}),
    
    case Begin =:= End of
        true -> ok; % full range, no need to check interval
        _ ->
            ?equals_w_note(intervals:in(SplitKey, intervals:new('[', Begin, End, ']')), true,
                           io_lib:format("SplitKey: ~.0p", [SplitKey]))
    end,

    BeginToSplitKey = rt_chord:get_range(Begin, SplitKey),
    SplitKeyToEnd = rt_chord:get_range(SplitKey, End),
    ?equals_pattern_w_note(
        BeginToSplitKey,
        Result when Result == SplitKeyToEnd orelse Result == (SplitKeyToEnd - 1),
        io_lib:format("SplitKey: ~.0p", [SplitKey])),
    true.

tester_get_split_key_half(_Config) ->
    tester:test(?MODULE, prop_get_split_key_half, 2, 10000).

-spec prop_get_split_key(Begin::rt_chord:key(), End::rt_chord:key(), SplitFracA::1..100, SplitFracB::0..100) -> true.
prop_get_split_key(Begin, End, SplitFracA, SplitFracB) ->
    FullRange = rt_chord:get_range(Begin, End),
%%     ct:pal("FullRange: ~.0p", [FullRange]),
    
    SplitFraction = case SplitFracA =< SplitFracB of
                        true -> {SplitFracA, SplitFracB};
                        _    -> {SplitFracB, SplitFracA}
                    end,
    
%%     ct:pal("Begin: ~.0p, End: ~.0p, SplitFactor: ~.0p", [Begin, End, SplitFraction]),
    SplitKey = rt_chord:get_split_key(Begin, End, SplitFraction),
    
    case erlang:element(1, SplitFraction) =:= 0 of
        true -> ?equals(SplitKey, Begin);
        _ ->
            case Begin =:= End of
                true -> ok; % full range, no need to check interval
                _ ->
                    ?equals_w_note(intervals:in(SplitKey, intervals:new('[', Begin, End, ']')), true,
                                   io_lib:format("SplitKey: ~.0p", [SplitKey]))
            end,
            BeginToSplitKey = case Begin of
                                  SplitKey -> 0;
                                  _ -> rt_chord:get_range(Begin, SplitKey)
                              end,
            %%     ct:pal("BeginToSplitKeyRange: ~.0p, ~.0p", [BeginToSplitKey, SplitKey]),
            
            ?equals_pattern_w_note(
                BeginToSplitKey,
                Range when Range == (FullRange * erlang:element(1, SplitFraction)) div erlang:element(2, SplitFraction),
                io_lib:format("FullRange * Factor = ~.0p, SplitKey: ~.0p",
                              [(FullRange * erlang:element(1, SplitFraction)) div erlang:element(2, SplitFraction), SplitKey]))
    end,
    true.

tester_get_split_key(_Config) ->
    tester:test(?MODULE, prop_get_split_key, 4, 10000).

%% helpers

fake_dht_node(Suffix) ->
    unittest_helper:start_subprocess(
      fun() -> pid_groups:join_as("rt_chord_SUITE_group" ++ Suffix, dht_node) end).
