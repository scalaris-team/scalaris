%% @copyright 2018 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Helper for jsonifying bench results.
-module(bench_json_helper).
-author('schuett@zib.de').

-export([result_to_json/1, json_to_result/1]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec json_to_result(term()) -> term().
json_to_result(Result) ->
    jsonbench_to_result(Result).

-spec result_to_json(list()) -> {struct, [{Key::atom(), Value::term()}]}.
result_to_json(List) ->
    {struct, [value_to_json(KeyX, ValueX) || {KeyX, ValueX} <- List]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% result to json
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
value_to_json(servers, ServerList) ->
    {servers, {array, [ server_to_json(Server) || Server <- ServerList]}};
value_to_json(threads_per_vm, ThreadsPerVM) ->
    {threads_per_vm, ThreadsPerVM};
value_to_json(iterations, Iterations) ->
    {iterations, Iterations};
value_to_json(statistics, Statistics) ->
    {statistics, {array, [statistic_to_json(S) || S <- Statistics]}};
value_to_json(wall_clock_time, WallClockTime) ->
    {wall_clock_time, WallClockTime};
value_to_json(wall_clock_throughput, WallClockTP) ->
    {wall_clock_throughput, WallClockTP};
value_to_json(wall_clock_latency, WallClockLat) ->
    {wall_clock_latency, WallClockLat};
value_to_json(min_throughput_overall, MinTP) ->
    {min_throughput_overall, MinTP};
value_to_json(min_throughput_each, MinTPAll) ->
    {min_throughput_each, {array, MinTPAll}};
value_to_json(mean_throughput_overall, MeanTP) ->
    {mean_throughput_overall, MeanTP};
value_to_json(mean_throughput_each, MeanTPAll) ->
    {mean_throughput_each, {array, MeanTPAll}};
value_to_json(max_throughput_overall, MaxTP) ->
    {max_throughput_overall, MaxTP};
value_to_json(max_throughput_each, MaxTPAll) ->
    {max_throughput_each, {array, MaxTPAll}};
value_to_json(min_latency_each, MinLatAll) ->
    {min_latency_each, {array, MinLatAll}};
value_to_json(avg_latency_each, AvgLatAll) ->
    {avg_latency_each, {array, AvgLatAll}};
value_to_json(max_latency_each, MaxLatAll) ->
    {max_latency_each, {array, MaxLatAll}};
value_to_json(avg_latency_overall, AvgLat) ->
    {avg_latency_overall, AvgLat};
value_to_json(avg_exec_time, AvgExTimeAll) ->
    {avg_exec_time, {array, AvgExTimeAll}};
value_to_json(aborts, Aborts) ->
    {aborts, {array, Aborts}}.

server_to_json({{A,B,C,D},Port,Server}) ->
    TheIP = lists:flatten(io_lib:format("~w.~w.~w.~w", [A,B,C,D])),
    {struct, [{port, Port},
              {server, erlang:atom_to_list(Server)},
              {ip, TheIP}]
    }.

statistic_to_json({WallClockTime, MinTime, MeanTime, MaxTime, Variance, Aborts}) ->
    {struct, [{wall_clock_time, WallClockTime}, {min_time, MinTime}, {mean_time, MeanTime},
              {max_time, MaxTime}, {variance, Variance}, {aborts, Aborts}]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% json to result
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

jsonbench_to_result({struct, KVL}) ->
    [json_to_value(KV) || KV <- KVL].


json_to_value({"servers", {array, ServerList}}) ->
    {servers, [ json_to_server(Server) || Server <- ServerList]};
json_to_value({"statistics", {array, Statistics}}) ->
    {statistics, [json_to_statistics(Statistic) || Statistic <- Statistics]};
json_to_value({"min_throughput_each", {array, MinTPAll}}) ->
    {min_throughput_each, MinTPAll};
json_to_value({"mean_throughput_each", {array, MeanTPAll}}) ->
    {mean_throughput_each, MeanTPAll};
json_to_value({"max_throughput_each", {array, MaxTPAll}}) ->
    {max_throughput_each, MaxTPAll};
json_to_value({"min_latency_each", {array, MinLatAll}}) ->
    {min_latency_each, MinLatAll};
json_to_value({"avg_latency_each", {array, AvgLatAll}}) ->
    {avg_latency_each, AvgLatAll};
json_to_value({"max_latency_each", {array, MaxLatAll}}) ->
    {max_latency_each, MaxLatAll};
json_to_value({"avg_exec_time", {array, AvgExTimeAll}}) ->
    {avg_exec_time, AvgExTimeAll};
json_to_value({"aborts", {array, Aborts}}) ->
    {aborts, Aborts};
json_to_value({Key, Value}) -> %% fallthrough
    {list_to_atom(Key), Value}.

json_to_server({struct, [{"port", Port}, {"server", Server}, {"ip", IP}]}) ->
    {IP, Port, list_to_atom(Server)}.

json_to_statistics({struct, [{"wall_clock_time", WallClockTime}, {"min_time", MinTime},
                             {"mean_time", MeanTime}, {"max_time", MaxTime}, {"variance", Variance},
                             {"aborts", Aborts}]}) ->
    {WallClockTime, MinTime, MeanTime, MaxTime, Variance, Aborts}.
