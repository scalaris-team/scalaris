% @copyright 2007-2011 Zuse Institute Berlin

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
-module(bench_server).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-export([start_link/0, init/1, on/2]).

-include("scalaris.hrl").

-export([run_threads/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% the server code
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(comm:message(), ok) -> ok.
on({bench_increment, Threads, Iterations, Owner}, State) ->
    Bench = bench_fun:increment(Iterations),
    {Time, {MinTime, AvgTime, MaxTime, Aborts}} = util:tc(?MODULE, run_threads,
                                                        [Threads, Bench]),
    comm:send(Owner, {done, comm_server:get_local_address_port(),
                      Time, MinTime, AvgTime, MaxTime, Aborts}),
    State;

on({bench_read, Threads, Iterations, Owner}, State) ->
    Bench = bench_fun:quorum_read(Iterations),
    {Time, {MinTime, AvgTime, MaxTime, Aborts}} = util:tc(?MODULE, run_threads,
                                                        [Threads, Bench]),
    comm:send(Owner, {done, comm_server:get_local_address_port(),
                      Time, MinTime, AvgTime, MaxTime, Aborts}),
    State;

on({bench_read_read, Threads, Iterations, Owner}, State) ->
    Bench = bench_fun:read_read(Iterations),
    {Time, {MinTime, AvgTime, MaxTime, Aborts}} = util:tc(?MODULE, run_threads,
                                                        [Threads, Bench]),
    comm:send(Owner, {done, comm_server:get_local_address_port(),
                      Time, MinTime, AvgTime, MaxTime, Aborts}),
    State.

-spec init([]) -> ok.
init([]) ->
    % load bench module
    _X = bench:module_info(),
    ok.

%% @doc spawns a bench_server
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_component:start_link(?MODULE, [], [{erlang_register, bench_server}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc spawns threads and collect statistics
-spec run_threads(integer(),
                  fun((Parent::comm:erl_local_pid()) -> any())) ->
    {MinTime::integer(), AvgTime::float(), MaxTime::integer(), Aborts::integer()}.
run_threads(Threads, Bench) ->
    Self = self(),
    util:for_to(1, Threads, fun(_X) -> spawn(fun()->Bench(Self) end) end),
    ThreadStats = util:for_to_ex(1, Threads, fun(_X) ->
                                                     receive
                                                         {done, Time, Aborts} ->
                                                             {Time, Aborts}
                                                     end
                                             end),
    VMStats = calc_vm_stats(ThreadStats, Threads),
    VMStats.

% @doc aggregate statistics from the different threads
-spec calc_vm_stats(list(), integer()) -> {integer(), float(), integer(), integer()}.
calc_vm_stats(ThreadStats, Threads) ->
    {FirstTime, FirstAborts} = hd(ThreadStats),
    F = fun ({Time, Aborts}, {MinTime, AggTime, MaxTime, AggAborts}) ->
                {util:min(Time, MinTime), AggTime + Time,
                 util:max(Time, MaxTime), AggAborts + Aborts}
        end,
    {MinTime, AggTime, MaxTime, Aborts}
        = lists:foldl(F, {FirstTime, 0, FirstTime, FirstAborts}, ThreadStats),
    {MinTime, AggTime / Threads, MaxTime, Aborts}.
