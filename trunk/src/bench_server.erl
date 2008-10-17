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
%%%-----------------------------------------------------------------------------
%%% File    : bench_server.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : bench server 
%%%
%%% Created :  13 Oct 2008 by Thorsten Schuett <schuett@zib.de>
%%%-----------------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @doc This is a small server for running benchmarks
%% @version $Id$
-module(bench_server).

-author('schuett@zib.de').
-vsn('$Id').

-export([start_link/0, start/0, run_increment/2, run_read/2, run_increment/3,
	bench_runner/3]).

-include("chordsharp.hrl").

%%==============================================================================
%% public interface
%%==============================================================================
%% @doc run an increment benchmark (i++) on all nodes
run_increment(ThreadsPerVM, Iterations) ->
    Msg = {bench_increment, ThreadsPerVM, Iterations, cs_send:this()},
    runner(ThreadsPerVM, Iterations, [], Msg).

run_increment(ThreadsPerVM, Iterations, Options) ->
    Msg = {bench_increment, ThreadsPerVM, Iterations, cs_send:this()},
    runner(ThreadsPerVM, Iterations, Options, Msg).

%% @doc run an read benchmark on all nodes
run_read(ThreadsPerVM, Iterations) ->
    Msg = {bench_read, ThreadsPerVM, Iterations, cs_send:this()},
    runner(ThreadsPerVM, Iterations, [], Msg).

runner(ThreadsPerVM, Iterations, Options, Message) ->
    ServerList = get_nodes(),
    Before = erlang:now(),
    _Times = case lists:member(profile, Options) of
	false ->
    		[cs_send:send(Server, Message) || Server <- ServerList],
    		[receive {done, Time} -> Time end || _Server <- ServerList];
	true ->
	    	Result = fprof:apply(fun () ->
		    		[cs_send:send(Server, Message) || Server <- ServerList],
    				[receive {done, Time} -> Time end || _Server <- ServerList]
			    end, 
			    [], [{procs, process_dictionary:get_all_pids()}]),
    		fprof:profile(),
    		%fprof:analyse(),
    		fprof:analyse([{cols, 140}, details, callers, totals, {dest, []}]), 
		Result
	end,
    After = erlang:now(),
    RunTime = timer:now_diff(After, Before),
    io:format("servers: ~p threads/vm: ~p iterations: ~p~n", 
	[length(ServerList), ThreadsPerVM, Iterations]),
    io:format("total time: ~p~n", [RunTime / 1000000.0]),
    io:format("1/s: ~p~n", 
	[length(ServerList) * ThreadsPerVM * Iterations / RunTime * 1000000.0]),
    ok.
    
%%==============================================================================
%% benchmarks
%%==============================================================================
%% @doc run the increment bench locally
-spec(bench_increment/3 :: (integer(), integer(), any()) -> ok).
bench_increment(Threads, Iterations, Owner) ->
    Bench = fun (Parent) -> 
	          randoms:init(),
	          Key = get_and_init_key(),
		  bench_increment:process(Parent, 
			                  Key, 
                                          Iterations) 
	  end,
    {Time, _} = timer:tc(?MODULE, bench_runner, [Threads, Iterations, Bench]),
    cs_send:send(Owner, {done, Time}),
    ok.

%% @doc run the read bench locally
-spec(bench_read/3 :: (integer(), integer(), any()) -> ok).
bench_read(Threads, Iterations, Owner) ->
    Bench = fun (Parent) -> 
	          randoms:init(),
	          Key = get_and_init_key(),
		  run_bench_read(Parent, 
			     Key, 
		             Iterations)
	  end,
    {Time, _} = timer:tc(?MODULE, bench_runner, [Threads, Iterations, Bench]),
    cs_send:send(Owner, {done, Time}),
    ok.

-spec(bench_runner/3 :: (integer(), integer(), any()) -> ok).
bench_runner(0, _Iterations, _Bench) ->
    ok;
bench_runner(Threads, Iterations, Bench) ->
    Self = self(),
    spawn(fun () ->
	Bench(Self)
    end),
    bench_runner(Threads - 1, Iterations, Bench),
    receive
	{done, _} ->
	    ok
    end.

run_bench_read(Owner, _Key, 0) ->
    Owner ! {done, ok};
run_bench_read(Owner, Key, Iterations) ->
    case transstore.transaction_api:quorum_read(Key) of
	{fail, _Reason} ->
	    run_bench_read(Owner, Key, Iterations);
	{_Value, _Version} ->
	    run_bench_read(Owner, Key, Iterations - 1)
    end.
%%==============================================================================
%% main loop
%%==============================================================================
loop() ->
    receive
	{bench_increment, Threads, Iterations, Owner} ->
	    spawn(fun () -> 
			  bench_increment(Threads, Iterations, Owner) 
		  end),
	    loop();
	{bench_read, Threads, Iterations, Owner} ->
	    spawn(fun () -> 
			  bench_read(Threads, Iterations, Owner) 
		  end),
	    loop()
    end.
%%==============================================================================
%% startup functions
%%==============================================================================
start() ->
    register(bench_server, self()),
    loop().

%% @doc spawns a bench_server
-spec(start_link/0 :: () -> {ok, pid()}).
start_link() ->
    {ok, spawn_link(?MODULE, start, [])}.

%%==============================================================================
%% helper functions
%%==============================================================================
-spec(get_nodes/0 :: () -> list()).
get_nodes() ->
    Nodes = lists:sort(boot_server:node_list()),
    util:uniq([cs_send:get(bench_server, CSNode) || CSNode <- Nodes]).
    
get_and_init_key() ->
    Key = ?RT:getRandomNodeId(),
    case transstore.transaction_api:single_write(Key, 0) of
      commit ->
        Key;
      {fail, abort} ->
        get_and_init_key()
    end.

