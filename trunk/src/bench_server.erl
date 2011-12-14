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

-record(state,
        {load_pid :: pid() | ok
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% the server code
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(comm:message(), #state{}) -> #state{}.
on({load_start, Gap}, State) ->
    Pid = spawn(fun() ->
                        bench_load:start(Gap)
                end),
    State#state{load_pid=Pid};
on({load_stop}, #state{load_pid=Pid} = State) ->
    Pid ! {load_stop},
    State#state{load_pid=ok};
on({bench, Op, Threads, Iterations, Owner, Param}, State) ->
    Bench = case Op of
                increment_with_histo ->
                    bench_fun:increment_with_histo(Iterations);
                increment ->
                    bench_fun:increment(Iterations);
                increment_with_key ->
                    bench_fun:increment_with_key(Iterations, Param);
                quorum_read ->
                    bench_fun:quorum_read(Iterations);
                read_read ->
                    bench_fun:read_read(Iterations)
            end,
    {Time, {MeanTime, Variance, MinTime, MaxTime, Aborts}} = util:tc(?MODULE, run_threads,
                                                        [Threads, Bench]),
    comm:send(Owner, {done, comm_server:get_local_address_port(),
                      Time, MeanTime, Variance, MinTime, MaxTime, Aborts}),
    State.

-spec init([]) -> #state{}.
init([]) ->
    % load bench module
    _X = bench:module_info(),
    #state{load_pid=ok}.

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
    {Mean::float(), Variance::float(), Min::non_neg_integer(),
     Max::non_neg_integer(), Aborts::non_neg_integer()}.
run_threads(Threads, Bench) ->
    Self = self(),
    util:for_to(1, Threads, fun(_X) -> spawn(fun()->Bench(Self) end) end),
    collect(Threads).

% @doc see http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#On-line_algorithm
-spec collect(pos_integer()) ->
    {Mean::float(), Variance::float(), Min::non_neg_integer(),
     Max::non_neg_integer(), Aborts::non_neg_integer()}.
collect(1) ->
    receive {done, Time, Aborts} ->
            {Time, 0.0, Time, Time, Aborts}
    end;
collect(Threads) ->
    {Mean, M2, Min, Max, Aborts} =
        receive {done, Time, TAborts} ->
                collect(Threads - 1, 1, Time, 0.0, Time, Time, TAborts)
        end,
    {Mean, M2 / (Threads - 1), Min, Max, Aborts}.

collect(0, _N, Mean, M2, Min, Max, Aborts) ->
    {Mean, M2, Min, Max, Aborts};
collect(ThreadsLeft, N, Mean, M2, Min, Max, AggAborts) ->
    receive {done, Time, Aborts} ->
            Delta = Time - Mean,
            NewMean = Mean + Delta / (N + 1),
            collect(ThreadsLeft - 1, N + 1, NewMean, M2 + Delta*(Time - NewMean),
                    erlang:min(Time, Min), erlang:max(Time, Max), AggAborts + Aborts)
    end.
