% @copyright 2007-2012 Zuse Institute Berlin

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
%% @doc This is a small server for running benchmarks
%% @end
-module(bench).
-author('schuett@zib.de').
-vsn('$Id: bench.erl 1814 2011-06-21 15:01:58Z schuett $').

%% public interface
-export([increment/2, increment_with_histo/2, increment/3, quorum_read/2,
         read_read/2, load_start/1, load_stop/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% @doc run an increment benchmark (i++) on all nodes
-spec increment(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> ok.
increment(ThreadsPerVM, Iterations) ->
    Msg = {bench, increment, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, [verbose], Msg).

%% @doc run an increment benchmark (i++) on all nodes
-spec increment_with_histo(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> ok.
increment_with_histo(ThreadsPerVM, Iterations) ->
    Msg = {bench, increment_with_histo, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, [verbose], Msg).

%% @doc run an increment benchmark on all nodes (with a user-specified key)
-spec increment(ThreadsPerVM::pos_integer(), Iterations::pos_integer(),
                Key::client_key()) -> ok.
increment(ThreadsPerVM, Iterations, Key) ->
    case init_key(Key, 100) of
        failed ->
            ok;
        _ ->
            Msg = {bench, increment_with_key, ThreadsPerVM, Iterations, comm:this(), Key},
            manage_run(ThreadsPerVM, Iterations, [verbose], Msg)
    end.

%% @doc run an read benchmark on all nodes
-spec quorum_read(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> ok.
quorum_read(ThreadsPerVM, Iterations) ->
    Msg = {bench, quorum_read, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, [verbose], Msg).

%% @doc run an read benchmark on all nodes
-spec read_read(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> ok.
read_read(ThreadsPerVM, Iterations) ->
    Msg = {bench, read_read, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, [verbose], Msg).

-spec load_start(Gap::pos_integer()) -> ok.
load_start(Gap) ->
    ServerList = util:get_proc_in_vms(bench_server),
    Msg = {load_start, Gap},
    _ = [comm:send(Server, Msg) || Server <- ServerList],
    ok.

-spec load_stop() -> ok.
load_stop() ->
    ServerList = util:get_proc_in_vms(bench_server),
    Msg = {load_stop},
    _ = [comm:send(Server, Msg) || Server <- ServerList],
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% common functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc spread run over the available VMs and collect results
%% (executed in clients context)
-spec manage_run(ThreadsPerVM::pos_integer(), Iterations::pos_integer(),
                 Options::[locally | verbose | profile | {copies, non_neg_integer()}],
                 Message::comm:message()) -> ok.
manage_run(ThreadsPerVM, Iterations, Options, Message) ->
    Pid = self(),
    TraceMPath = erlang:get(trace_mpath),
    spawn(fun() ->
                  erlang:put(trace_mpath, TraceMPath),
                  Msg = setelement(5, Message, comm:this()),
                  Res = manage_run_internal(ThreadsPerVM, Iterations, Options,
                                            Msg),
                  comm:send_local(Pid, Res)
          end),
    receive
        ?SCALARIS_RECV({ok}, %% ->
            ok)
    end.

-spec manage_run_internal(ThreadsPerVM::pos_integer(), Iterations::pos_integer(),
                 Options::[locally | verbose | profile | {copies, non_neg_integer()}],
                 Message::comm:message()) -> {ok}.
manage_run_internal(ThreadsPerVM, Iterations, Options, Message) ->
    ServerList = util:get_proc_in_vms(bench_server),
    %% io:format("~p~n", [ServerList]),
    Before = erlang:now(),
    _ = [comm:send(Server, Message) || Server <- ServerList],
    fd:subscribe(ServerList),
    io:format("Collecting results... ~n"),
    Statistics = collect(length(ServerList), []),
    fd:unsubscribe(ServerList),
    io:format("stats: ~p ~n", [Statistics]),
    {MinTP, MeanTP, MaxTP} = lists:foldl(
                               fun (Stats, {Min, Mean, Max}) ->
                                       {Min + Iterations / element(4, Stats) * 1000000.0 * ThreadsPerVM,
                                        Mean + Iterations / element(3, Stats) * 1000000.0 * ThreadsPerVM,
                                        Max + Iterations / element(2, Stats) * 1000000.0 * ThreadsPerVM}
                     end, {0.0, 0.0, 0.0}, Statistics),
    After = erlang:now(),
    case lists:member(verbose, Options) of
        true ->
            RunTime = timer:now_diff(After, Before),
            io:format("servers: ~p threads/vm: ~p iterations: ~p~n",
                      [length(ServerList), ThreadsPerVM, Iterations]),
            io:format("wall clock time        : ~p~n", [RunTime / 1000000.0]),
            io:format("wall clock throughput  : ~p~n", [length(ServerList) * ThreadsPerVM * Iterations / RunTime * 1000000.0]),
            io:format("wall clock avg. latency: ~p ms~n", [ RunTime / 1000.0 / Iterations ]),
            io:format("min. throughput: ~p ~p (1/s)~n",
                      [MinTP, [Iterations / element(4, Node) * 1000000.0 || Node <- Statistics]]),
            io:format("avg. throughput: ~p ~p (1/s)~n",
                      [MeanTP, [Iterations / element(3, Node) * 1000000.0 || Node <- Statistics]]),
            io:format("max. throughput: ~p ~p (1/s)~n",
                      [MaxTP, [Iterations / element(2, Node) * 1000000.0 || Node <- Statistics]]),
            io:format("min. latency   : ~p (ms)~n",
                      [[element(2, Node) / Iterations / 1000.0 || Node <- Statistics]]),
            io:format("avg. latency   : ~p (ms)~n",
                      [[element(3, Node) / Iterations / 1000.0 || Node <- Statistics]]),
            io:format("max. latency   : ~p (ms)~n",
                      [[element(4, Node) / Iterations / 1000.0 || Node <- Statistics]]),
            io:format("avg. ex. time  : ~p (ms)~n",
                      [[element(3, Node) / 1000.0 || Node <- Statistics]]),
            %io:format("std. dev.(%)   : ~p~n",
            %          [[math:sqrt(element(5, Node)) / element(3, Node) * 100 || Node <- Statistics]]),
            io:format("aborts         : ~p~n",
                      [[element(6, Node) || Node <- Statistics]]);
            %io:format("Statistics: ~p~n", [ Statistics ]);
        false -> ok
    end,
    {ok}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init_key(Key::client_key(), non_neg_integer()) -> failed | client_key().
init_key(_Key, 0) ->
    io:format("init_key failed~n", []),
    failed;
init_key(Key, Count) ->
    case api_tx:write(Key, 0) of
        {ok} ->
            Key;
        {fail, abort} ->
            init_key(Key, Count - 1);
        {fail, timeout} ->
            init_key(Key, Count - 1)
    end.

collect(0, L) ->
    L;
collect(Length, L) ->
    receive
        ?SCALARIS_RECV({done, X, WallClockTime, MeanTime, Variance, MinTime, MaxTime, Aborts}, %% ->
         begin
             io:format("BS: ~p @ ~p~n",[WallClockTime, X]),
             collect(Length - 1, [{WallClockTime, MinTime, MeanTime, MaxTime, Variance, Aborts} | L])
          end);
        ?SCALARIS_RECV({crash, Pid}, %% ->
           begin
               io:format("ignoring ~p, because it crashed", [Pid]),
               collect(Length - 1, L)
           end)
    end.

