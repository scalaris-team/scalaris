%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Christian Hennig <hennig@zib.de>
%% @doc Master for local benchmark.
%% @end
%% @version $Id$
-module(bench_master).
-author('hennig@zib.de').
-vsn('$Id$').

-export([run_1/0, start/0]).
 
%%
%% API Functions
%%
-spec start() -> pid().
start() ->
    ok = application:start(boot_cs),
    timer:sleep(1000),
    erlang:spawn(?MODULE, run_1, []).

-spec run_1() -> no_return().
run_1() ->
    Size = list_to_integer(os:getenv("NODES_VM")),
    Worker = list_to_integer(os:getenv("WORKER")),
    Iterations = list_to_integer(os:getenv("ITERATIONS")),
    RingSize = list_to_integer(os:getenv("RING_SIZE")),
    io:format("Start ~p Nodes with ~p Clients per VMs and ~p Iterations~n",[Size,Worker,Iterations]),
    _ = admin:add_nodes(Size-1),
    timer:sleep(1000),
    check_ring_size(RingSize),
    wait_for_stable_ring(),
    timer:sleep(pointerBaseStabilizationInterval()+8000),
    bench_server:run_increment(Worker, Iterations),
    timer:sleep(3000),
    bench_server:run_read(Worker, Iterations),
    io:format("~p~n",[util:get_proc_in_vms(admin_server)]),
    _ = [comm:send(Pid, {halt, 1}) || Pid <- util:get_proc_in_vms(admin_server)],
    init:stop(1),
    receive nothing -> ok end.

%%
%% Local Functions
%%

wait_for_stable_ring() ->
    R = admin:check_ring(),
    
    case R of
	ok ->
	    ok;
	_ ->
	    timer:sleep(1000),
            wait_for_stable_ring()
    end.

check_ring_size(Size) ->
    boot_server:number_of_nodes(),
    RSize = receive
        {get_list_length_response,L} ->
            L
    end,
    
    case (RSize =:= Size) of
	true ->
	    ok;
	_ ->
	    timer:sleep(1000),
	    check_ring_size(Size)
    end.

%% @doc the interval between two finger/pointer stabilization runs
-spec pointerBaseStabilizationInterval() -> pos_integer().
pointerBaseStabilizationInterval() ->
    config:read(pointer_base_stabilization_interval).
