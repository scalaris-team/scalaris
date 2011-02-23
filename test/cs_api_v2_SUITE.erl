%  Copyright 2008-2011 Zuse Institute Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : cs_api_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/cs_api.erl
%%%
%%% Created :  31 Jul 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(cs_api_v2_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [read, write, test_and_set, write_test_race_mult_rings].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        write_test_race_mult_rings ->
            Config;
        _ ->
            % stop ring from previous test case (it may have run into a timeout)
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

read(_Config) ->
    ?equals(cs_api_v2:read("ReadKey"), {fail, not_found}),
    ?equals(cs_api_v2:write("ReadKey", "IsSet"), ok),
    ?equals(cs_api_v2:read("ReadKey"), "IsSet"),
    ok.

write(_Config) ->
    ?equals(cs_api_v2:write("WriteKey", "IsSet"), ok),
    ?equals(cs_api_v2:read("WriteKey"), "IsSet"),
    ?equals(cs_api_v2:write("WriteKey", "IsSet2"), ok),
    ?equals(cs_api_v2:read("WriteKey"), "IsSet2"),
    ok.

test_and_set(_Config) ->
    ?equals(cs_api_v2:test_and_set("TestAndSetKey", "", "IsSet"), ok),
    ?equals(cs_api_v2:test_and_set("TestAndSetKey", "", "IsSet"), {fail, {key_changed, "IsSet"}}),
    ?equals(cs_api_v2:test_and_set("TestAndSetKey", "IsSet", "IsSet2"), ok),
    ok.


%% @doc Test for cs_api_v2:write taking at least 2s after stopping a ring and
%%      starting a new one.
write_test_race_mult_rings(Config) ->
    % first ring:
    write_test(Config),
%%     timer:sleep(10000),
    % second ring and more:
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config).

-spec write_test(Config::[tuple()]) -> ok.
write_test(Config) -> 
    OldRegistered = erlang:registered(),
    OldProcesses = unittest_helper:get_processes(),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}]}]),
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
    _ = inets:stop(),
    unittest_helper:kill_new_processes(OldProcesses),
    {_, _, OnlyNewReg} =
        util:split_unique(OldRegistered, erlang:registered()),
    ct:pal("NewReg: ~.0p~n", [OnlyNewReg]),
    ?equals_pattern(FirstWriteTime, X when X =< 1000000),
    ?equals_pattern(SecondWriteTime, X when X =< 1000000).
