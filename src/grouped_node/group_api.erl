%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc    grouped_node API
%% @end
%% @version $Id$
-module(group_api).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([read/1, write/2, bench_read/1, bench_write/1]).

timeout() -> 1000.

read(Key) ->
    service_per_vm ! {get_dht_nodes, comm:this()},
    receive
        {get_dht_nodes_response, []} ->
            {error, no_node_found};
        {get_dht_nodes_response, Nodes} ->
            comm:send(hd(Nodes), {ops, {read, Key, comm:this()}}),
            receive
                {read_response, Key, Value} ->
                    {Key, Value}
            after
                timeout() ->
                    {error, timeout}
            end
    after
        timeout() ->
            {error, timeout}
    end.

write(Key, Value) ->
    service_per_vm ! {get_dht_nodes, comm:this()},
    receive
        {get_dht_nodes_response, []} ->
            {error, no_node_found};
        {get_dht_nodes_response, Nodes} ->
            comm:send(hd(Nodes), {ops, {write, Key, Value, comm:this()}}),
            receive
                {write_response, Key, Value} ->
                    {Key, Value}
            after
                timeout() ->
                    {error, timeout}
            end
    after
        timeout() ->
            {error, timeout}
    end.

bench_read(Iterations) ->
    Before = erlang:now(),
    run_read(Iterations),
    After = erlang:now(),
    Time = timer:now_diff(After, Before),
    io:format("time: ~w~n", [Time / 1000000.0]),
    io:format("1/s : ~w~n", [Iterations / Time * 1000000.0]),
    ok.

bench_write(Iterations) ->
    Before = erlang:now(),
    run_write(Iterations),
    After = erlang:now(),
    Time = timer:now_diff(After, Before),
    io:format("time: ~w~n", [Time / 1000000.0]),
    io:format("1/s : ~w~n", [Iterations / Time * 1000000.0]),
    ok.

run_read(0) ->
    ok;
run_read(Iterations) ->
    Res = group_api:read(foo),
    run_read(Iterations - 1).

run_write(0) ->
    ok;
run_write(Iterations) ->
    Res = group_api:write(foo, bar),
    run_write(Iterations - 1).
