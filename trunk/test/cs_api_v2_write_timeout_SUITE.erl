% @copyright 2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc    Unit tests for src/cs_api_v2.erl (strange unit test behavior after
%%         stopping a ring).
%% @end
%% @version $Id$
-module(cs_api_v2_write_timeout_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [
     write_test_mult
    ].

suite() ->
    [
     {timetrap, {seconds, 60}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    Config.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

write_test_mult(_Config) ->
    % first ring:
    write_test(),
%%     timer:sleep(10000),
    % second ring and more:
    write_test(),
    write_test(),
    write_test(),
    write_test(),
    write_test(),
    write_test(),
    write_test().

-spec write_test() -> ok.
write_test() -> 
    OldRegistered = erlang:registered(),
    OldProcesses = unittest_helper:get_processes(),
    unittest_helper:make_ring(1),
    Self = self(),
    BenchPid1 = erlang:spawn(fun() ->
                                     {Time, _} = util:tc(cs_api_v2, write, [1, 1]),
                                     comm:send_local(Self, {time, Time}),
                                     ct:pal("~.0pus~n", [Time])
                             end),
    receive {time, FirstWriteTime} -> ok
    end,
    unittest_helper:wait_for_process_to_die(BenchPid1),
    BenchPid2 = erlang:spawn(fun() ->
                                     {Time, _} = util:tc(cs_api_v2, write, [2, 2]),
                                     comm:send_local(Self, {time, Time}),
                                     ct:pal("~.0pus~n", [Time])
                             end),
    receive {time, SecondWriteTime} -> ok
    end,
    unittest_helper:wait_for_process_to_die(BenchPid2),
    dht_node_move_SUITE:check_size2_v2(4  * 2),
    unittest_helper:stop_ring(),
%%     randoms:stop(), %doesn't matter
    inets:stop(),
    unittest_helper:kill_new_processes(OldProcesses),
    {_, _, OnlyNewReg} =
        util:split_unique(OldRegistered, erlang:registered()),
    ct:pal("NewReg: ~.0p~n", [OnlyNewReg]),
    ?equals_pattern(FirstWriteTime, X when X =< 1000000),
    ?equals_pattern(SecondWriteTime, X when X =< 1000000).
