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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Unit tests for src/dht_node_join.erl in combination with
%%         src/dht_node_move.erl.
%% @end
%% @version $Id$
-module(join_leave_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [add_9, rm_5, add_9_rm_5,
     add_2x3_load, add_2x3_load_v2,
     add_3_rm_2_load, add_3_rm_2_load_v2].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    ct:pal("Starting unittest ~p", [ct:get_status()]),
    Config.

end_per_suite(_Config) ->
    unittest_helper:stop_ring(),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        add_3_rm_2_load ->
            {skip, "no graceful leave yet"};
        add_3_rm_2_load_v2 ->
            {skip, "no graceful leave yet"};
        _ ->
            unittest_helper:make_ring(1),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

add_9(_Config) ->
    stop_time(fun add_9_test/0, "add_9").

add_9_test() ->
    admin:add_nodes(9),
    check_size(10).

rm_5(_Config) ->
    admin:add_nodes(9),
    check_size(10),
    stop_time(fun rm_5_test/0, "rm_5").

rm_5_test() ->
    admin:del_nodes(5),
    check_size(5).

add_9_rm_5(_Config) ->
    stop_time(fun add_9_rm_5_test/0, "add_9_rm_5").

add_9_rm_5_test() ->
    admin:add_nodes(9),
    check_size(10),
    admin:del_nodes(5),
    check_size(5).

add_2x3_load(_Config) ->
    stop_time(fun add_2x3_load_test/0, "add_2x3_load"),
    Ring = statistics:get_ring_details(),
    ?equals(statistics:get_total_load(Ring), 4).

add_2x3_load_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    admin:add_nodes(3),
    check_size(7),
    unittest_helper:wait_for_process_to_die(BenchPid).

add_2x3_load_v2(_Config) ->
    stop_time(fun add_2x3_load_v2_test/0, "add_2x3_load_v2"),
    Ring = statistics:get_ring_details(),
    ?equals(statistics:get_total_load(Ring), 4).

add_2x3_load_v2_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment_v2(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    admin:add_nodes(3),
    check_size(7),
    unittest_helper:wait_for_process_to_die(BenchPid).

add_3_rm_2_load(_Config) ->
    stop_time(fun add_3_rm_2_load_test/0, "add_2x3_load"),
    Ring = statistics:get_ring_details(),
    ?equals(statistics:get_total_load(Ring), 4).

add_3_rm_2_load_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    admin:del_nodes(2),
    check_size(2),
    unittest_helper:wait_for_process_to_die(BenchPid).

add_3_rm_2_load_v2(_Config) ->
    stop_time(fun add_3_rm_2_load_v2_test/0, "add_2x3_load_v2"),
    Ring = statistics:get_ring_details(),
    ?equals(statistics:get_total_load(Ring), 4).

add_3_rm_2_load_v2_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment_v2(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    admin:del_nodes(2),
    check_size(2),
    unittest_helper:wait_for_process_to_die(BenchPid).

-spec stop_time(F::fun(() -> any()), Tag::string()) -> ok.
stop_time(F, Tag) ->
    Start = erlang:now(),
    F(),
    Stop = erlang:now(),
    ElapsedTime = timer:now_diff(Stop, Start) / 1000000.0,
    Frequency = 1 / ElapsedTime,
    ct:pal("~p took ~ps: ~p1/s",
           [Tag, ElapsedTime, Frequency]),
    ok.

-spec check_size(Size::pos_integer()) -> ok.
check_size(Size) ->
    unittest_helper:check_ring_size(Size),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size(Size).
