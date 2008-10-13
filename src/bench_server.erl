%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : bench_server.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : bench server 
%%%
%%% Created :  13 Oct 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @doc This is a small server for running benchmarks
%% @version $Id$
-module(bench_server).

-author('schuett@zib.de').
-vsn('$Id').

-export([start_link/0, start/0, run_increment/2, bench_increment/2]).

-include("chordsharp.hrl").

%%====================================================================
%% public interface
%%====================================================================
%% @doc run an increment benchmark (i++) on all nodes
run_increment(ThreadsPerVM, Iterations) ->
    ServerList = get_nodes(),
    Msg = {bench_increment, ThreadsPerVM, Iterations, cs_send:this()},
    Before = erlang:now(),
    [cs_send:send(Server, Msg) || Server <- ServerList],
    Times = [receive {bench_increment, done, Time} -> Time end || _Server <- ServerList],
    After = erlang:now(),
    RunTime = timer:now_diff(After, Before),
    io:format("servers: ~p threads/vm: ~p iterations: ~p~n", [length(ServerList), ThreadsPerVM, Iterations]),
    io:format("total time: ~p~n", [RunTime / 1000000.0]),
    io:format("1/s: ~p~n", [length(ServerList) * ThreadsPerVM * Iterations / RunTime * 1000000.0]),
    ok.
    
%%====================================================================
%% benchmarks
%%====================================================================
%% @doc run the increment bench locally
-spec(bench_increment/3 :: (integer(), integer(), any()) -> ok).
bench_increment(Threads, Iterations, Owner) ->
    {Time, _} = timer:tc(?MODULE, bench_increment, [Threads, Iterations]),
    cs_send:send(Owner, {bench_increment, done, Time}),
    ok.

-spec(bench_increment/2 :: (integer(), integer()) -> ok).
bench_increment(0, _Iterations) ->
    ok;
bench_increment(Threads, Iterations) ->
    Self = self(),
    spawn(fun () -> 
		  randoms:init(),
		  Key = ?RT:getRandomNodeId(),
		  commit = transstore.transaction_api:single_write(Key, 0),
		  bench_increment:process(Self, 
					  Key, 
					  Iterations) 
	  end),
    bench_increment(Threads - 1, Iterations),
    receive
	{done, _} ->
	    ok
    end.

%%====================================================================
%% main loop
%%====================================================================
loop() ->
    receive
	{bench_increment, Threads, Iterations, Owner} ->
	    spawn(fun () -> 
			  bench_increment(Threads, Iterations, Owner) 
		  end),
	    loop()
    end.
%%====================================================================
%% startup functions
%%====================================================================
start() ->
    register(bench_server, self()),
    loop().

%% @doc spawns a bench_server
-spec(start_link/0 :: () -> {ok, pid()}).
start_link() ->
    {ok, spawn_link(?MODULE, start, [])}.

%%====================================================================
%% helper functions
%%====================================================================
-spec(get_nodes/0 :: () -> list()).
get_nodes() ->
    Nodes = lists:sort(boot_server:node_list()),
    [cs_send:get(bench_server, CSNode) || CSNode <- util:uniq(Nodes)].
    
    

