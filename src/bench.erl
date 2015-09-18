% @copyright 2007-2015 Zuse Institute Berlin

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
%% @version $Id$
-module(bench).
-author('schuett@zib.de').
-vsn('$Id$').

%% public interface
-export([increment/2, increment_o/3,
         increment_with_histo/2, increment_with_histo_o/3,
         increment/3, increment_o/4,
         quorum_read/2, quorum_read_o/3,
         read_read/2, read_read_o/3,
         load_start/1, load_stop/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

-type options() :: [locally | print | verbose | profile
                    | {copies, non_neg_integer()}].

%% @doc run an increment benchmark (i++) on all nodes
-spec increment(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> {ok, [{atom(), term()},...]}.
increment(ThreadsPerVM, Iterations) ->
    increment_o(ThreadsPerVM, Iterations, [print, verbose]).

%% @doc run an increment benchmark (i++) on all nodes with custom options
-spec increment_o(ThreadsPerVM::pos_integer(), Iterations::pos_integer(), Options::options()) -> {ok, [{atom(), term()},...]}.
increment_o(ThreadsPerVM, Iterations, Options) ->
    Msg = {bench, increment, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, Options, Msg).

%% @doc run an increment benchmark (i++) on all nodes
-spec increment_with_histo(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> {ok, [{atom(), term()},...]}.
increment_with_histo(ThreadsPerVM, Iterations) ->
    increment_with_histo_o(ThreadsPerVM, Iterations, [print, verbose]).

%% @doc run an increment benchmark (i++) on all nodes with custom options
-spec increment_with_histo_o(ThreadsPerVM::pos_integer(), Iterations::pos_integer(), Options::options()) -> {ok, [{atom(), term()},...]}.
increment_with_histo_o(ThreadsPerVM, Iterations, Options) ->
    Msg = {bench, increment_with_histo, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, Options, Msg).

%% @doc run an increment benchmark on all nodes (with a user-specified key)
-spec increment(ThreadsPerVM::pos_integer(), Iterations::pos_integer(),
                Key::client_key()) -> {ok, [{atom(), term()},...]} | {failed, init_per_key}.
increment(ThreadsPerVM, Iterations, Key) ->
    increment_o(ThreadsPerVM, Iterations, Key, [print, verbose]).

%% @doc run an increment benchmark on all nodes (with a user-specified key) with custom options
-spec increment_o(ThreadsPerVM::pos_integer(), Iterations::pos_integer(),
                Key::client_key(), Options::options()) -> {ok, [{atom(), term()},...]} | {failed, init_per_key}.
increment_o(ThreadsPerVM, Iterations, Key, Options) ->
    case init_key(Key, 100) of
        failed ->
            {failed, init_per_key};
        _ ->
            Msg = {bench, increment_with_key, ThreadsPerVM, Iterations, comm:this(), Key},
            manage_run(ThreadsPerVM, Iterations, Options, Msg)
    end.

%% @doc run an read benchmark on all nodes
-spec quorum_read(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> {ok, [{atom(), term()},...]}.
quorum_read(ThreadsPerVM, Iterations) ->
    quorum_read_o(ThreadsPerVM, Iterations, [print, verbose]).

%% @doc run an read benchmark on all nodes with custom options
-spec quorum_read_o(ThreadsPerVM::pos_integer(), Iterations::pos_integer(), Options::options()) -> {ok, [{atom(), term()},...]}.
quorum_read_o(ThreadsPerVM, Iterations, Options) ->
    Msg = {bench, quorum_read, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, Options, Msg).

%% @doc run an read benchmark on all nodes
-spec read_read(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> {ok, [{atom(), term()},...]}.
read_read(ThreadsPerVM, Iterations) ->
    read_read_o(ThreadsPerVM, Iterations, [print, verbose]).

%% @doc run an read benchmark on all nodes with custom options
-spec read_read_o(ThreadsPerVM::pos_integer(), Iterations::pos_integer(), Options::options()) -> {ok, [{atom(), term()},...]}.
read_read_o(ThreadsPerVM, Iterations, Options) ->
    Msg = {bench, read_read, ThreadsPerVM, Iterations, comm:this(), undefined},
    manage_run(ThreadsPerVM, Iterations, Options, Msg).

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
                 Options::options(),
                 Message::comm:message()) -> {ok, [{atom(), term()},...]}.
manage_run(ThreadsPerVM, Iterations, Options, Message) ->
    Pid = self(),
    %% function is spawned to catch superfluous reply messages after
    %% actual workload is done.
    Child = spawn_link(fun() ->
                  %% to get the newly spawned process in the
                  %% proto_sched schedule we start it working by
                  %% sending it a message (which also performs the
                  %% infection).
                  trace_mpath:thread_yield(), %noop, as not infected
                  receive
                      ?SCALARIS_RECV({start_work},%% ->
                                     ok)
                      end,
                   Msg = setelement(5, Message, comm:this()),
                   Res = manage_run_internal(ThreadsPerVM, Iterations, Options,
                                                    Msg),
                   comm:send_local(Pid, Res)
                  end),
    comm:send_local(Child, {start_work}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({ok, Result}, %% ->
            {ok, Result})
    end.

-spec manage_run_internal(ThreadsPerVM::pos_integer(), Iterations::pos_integer(),
                 Options::options(), Message::comm:message())
                         -> {ok, [{atom(), term()},...]}.
manage_run_internal(ThreadsPerVM, Iterations, Options, Message) ->
    Verbose = lists:member(verbose, Options),
    Print = lists:member(print, Options),
    ServerList = util:get_proc_in_vms(bench_server),
    %% io:format("~p~n", [ServerList]),
    Before = os:timestamp(),
    _ = [comm:send(Server, Message) || Server <- ServerList],
    fd:subscribe(self(), ServerList),
    ?IIF(Print, io:format("Collecting results... ~n"), ok),
    Statistics = collect(length(ServerList), [], Print),
    fd:unsubscribe(self(), ServerList),
    ?IIF(Print, io:format("stats: ~p ~n", [Statistics]), ok),
    {MinTP, MeanTP, MaxTP} = lists:foldl(
                               fun (Stats, {Min, Mean, Max}) ->
                                       {Min + Iterations / element(4, Stats) * 1000000.0 * ThreadsPerVM,
                                        Mean + Iterations / element(3, Stats) * 1000000.0 * ThreadsPerVM,
                                        Max + Iterations / element(2, Stats) * 1000000.0 * ThreadsPerVM}
                     end, {0.0, 0.0, 0.0}, Statistics),
    After = os:timestamp(),
    RunTime = timer:now_diff(After, Before),
    NrServers = length(ServerList),
    WallClockTime = RunTime / 1000000.0,
    WallClockTP = NrServers * ThreadsPerVM * Iterations / RunTime * 1000000.0,
    WallClockLat = RunTime / 1000.0 / Iterations,
    MinTPAll     = [Iterations / element(4, Node) * 1000000.0 || Node <- Statistics],
    MeanTPAll    = [Iterations / element(3, Node) * 1000000.0 || Node <- Statistics],
    MaxTPAll     = [Iterations / element(2, Node) * 1000000.0 || Node <- Statistics],
    MinLatAll    = [element(2, Node) / Iterations / 1000.0 || Node <- Statistics],
    AvgLatAll    = [element(3, Node) / Iterations / 1000.0 || Node <- Statistics],
    MaxLatAll    = [element(4, Node) / Iterations / 1000.0 || Node <- Statistics],
    AvgLat       = lists:foldl(fun (Lat, Acc) ->
                                       Lat + Acc
                               end, 0.0, AvgLatAll) / NrServers,
    AvgExTimeAll = [element(3, Node) / 1000.0 || Node <- Statistics],
    Aborts = [element(6, Node) || Node <- Statistics],
    case Verbose andalso Print of
        true ->
            io:format("servers: ~p threads/vm: ~p iterations: ~p~n",
                      [NrServers, ThreadsPerVM, Iterations]),
            io:format("wall clock time        : ~p~n", [WallClockTime]),
            io:format("wall clock throughput  : ~p~n", [WallClockTP]),
            io:format("wall clock avg. latency: ~p ms~n", [WallClockLat]),
            io:format("min. throughput: ~p ~p (1/s)~n", [MinTP, MinTPAll]),
            io:format("avg. throughput: ~p ~p (1/s)~n", [MeanTP, MeanTPAll]),
            io:format("max. throughput: ~p ~p (1/s)~n", [MaxTP, MaxTPAll]),
            io:format("min. latency   : ~p (ms)~n", [MinLatAll]),
            io:format("avg. latency   : ~p (ms)~n", [AvgLatAll]),
            io:format("max. latency   : ~p (ms)~n", [MaxLatAll]),
            io:format("avg. ex. time  : ~p (ms)~n", [AvgExTimeAll]),
            %io:format("std. dev.(%)   : ~p~n",
            %          [[math:sqrt(element(5, Node)) / element(3, Node) * 100 || Node <- Statistics]]),
            io:format("aborts         : ~p~n", [Aborts]);
            %io:format("Statistics: ~p~n", [ Statistics ]);
        false -> ok
    end,
    Result = [{servers, ServerList},
              {threads_per_vm, ThreadsPerVM},
              {iterations, Iterations},
              {statistics, Statistics},
              {wall_clock_time, WallClockTime},
              {wall_clock_throughput, WallClockTP},
              {wall_clock_latency, WallClockLat},
              {min_troughput_overall, MinTP},
              {min_troughput_each, MinTPAll},
              {mean_troughput_overall, MeanTP},
              {mean_troughput_each, MeanTPAll},
              {max_troughput_overall, MaxTP},
              {max_troughput_each, MaxTPAll},
              {min_latency_each, MinLatAll},
              {avg_latency_each, AvgLatAll},
              {max_latency_each, MaxLatAll},
              {avg_latency_overall, AvgLat},
              {avg_exec_time, AvgExTimeAll},
              {aborts, Aborts}
             ],
    {ok, Result}.

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
        {ok}                 -> Key;
        {fail, abort, [Key]} -> init_key(Key, Count - 1)
    end.

collect(0, L, _Print)     -> L;
collect(Length, L, Print) ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({done, X, WallClockTime, MeanTime, Variance, MinTime, MaxTime, Aborts}, %% ->
         begin
             Done = erlang:length(L),
             ?IIF(Print, io:format("BS [~p/~p]: ~p @ ~p~n",[Done+1, Done+Length, WallClockTime, X]), ok),
             collect(Length - 1, [{WallClockTime, MinTime, MeanTime, MaxTime, Variance, Aborts} | L], Print)
          end);
        ?SCALARIS_RECV({fd_notify, crash, Pid, _Reason}, %% ->
           begin
               ?IIF(Print, io:format("ignoring ~p, because it crashed~n", [Pid]), ok),
               collect(Length - 1, L, Print)
           end);
        ?SCALARIS_RECV({fd_notify, _Event, _Pid, _Reason}, %% ->
           begin
               collect(Length, L, Print)
           end)
    end.

