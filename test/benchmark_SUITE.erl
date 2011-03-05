%  Copyright 2008, 2011 Zuse Institute Berlin
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
%%% File    : benchmark_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Runs the basic benchmarks from src/bench_server.erl
%%%               The results are stored in several files in the main
%%%               directory, so that the buildbot can fetch the data
%%%               from there.
%%%
%%% Created :  18 Mar 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(benchmark_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [run_increment_1_1000, run_increment_10_100,
     run_read_1_100000, run_read_10_10000].

suite() -> [ {timetrap, {seconds, 120}} ].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    Config2.

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

run_increment_1_1000(_Config) ->
    Threads    = 1,
    Iterations = 10000,
    Start = erlang:now(),
    bench_server:run_increment(Threads, Iterations),
    Stop = erlang:now(),
    RunTime = timer:now_diff(Stop, Start),
    write_result("result_increment_1_10000.txt", Threads * Iterations / RunTime * 1000000.0),
    ok.

run_increment_10_100(_Config) ->
    Threads    = 10,
    Iterations = 1000,
    Start = erlang:now(),
    bench_server:run_increment(Threads, Iterations),
    Stop = erlang:now(),
    RunTime = timer:now_diff(Stop, Start),
    write_result("result_increment_10_1000.txt", Threads * Iterations / RunTime * 1000000.0),
    ok.

run_read_1_100000(_Config) ->
    Threads    = 1,
    Iterations = 100000,
    Start = erlang:now(),
    bench_server:run_read(Threads, Iterations),
    Stop = erlang:now(),
    RunTime = timer:now_diff(Stop, Start),
    write_result("result_read_1_100000.txt", Threads * Iterations / RunTime * 1000000.0),
    ok.

run_read_10_10000(_Config) ->
    Threads    = 10,
    Iterations = 10000,
    Start = erlang:now(),
    bench_server:run_read(Threads, Iterations),
    Stop = erlang:now(),
    RunTime = timer:now_diff(Stop, Start),
    write_result("result_read_10_10000.txt", Threads * Iterations / RunTime * 1000000.0),
    ok.

write_result(Filename, Result) ->
    % make_ring switched to the bin sub-dir...go to top-level:
    _ = file:set_cwd(".."),
    {ok, F} = file:open(Filename, [write]),
    io:fwrite(F, "~p~n", [Result]),
    _ = file:close(F),
    _ = file:set_cwd("bin"),
    ok.
