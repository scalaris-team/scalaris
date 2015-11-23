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
-module(bench_server).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-export([start_link/1, init/1, on/2]).

-include("scalaris.hrl").

-export([run_threads/2]).

-include("gen_component.hrl").

-record(state,
        {load_pid             :: pid() | ok,
         bench_owner   = ok   :: comm:mypid() | ok,
         bench_start   = ok   :: erlang_timestamp() | ok,
         bench_threads = 0    :: ThreadsLeft::non_neg_integer(),
         bench_data    = null :: {N::non_neg_integer(), Mean::float(), M2::float(),
                                  Min::non_neg_integer(), Max::non_neg_integer(),
                                  AggAborts::non_neg_integer()} | null
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
    BenchStart = os:timestamp(),
    run_threads(Threads, Bench),
    State#state{bench_start = BenchStart,
                bench_owner = Owner,
                bench_threads = Threads};
on({done, Time, Aborts},
   State = #state{bench_owner = BenchOwner,
                  bench_start = BenchStart,
                  bench_threads = BenchThreads,
                  bench_data = BenchData})
  when BenchOwner =/= ok andalso BenchStart =/= ok andalso BenchThreads > 0 ->
  % see http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#On-line_algorithm
    NewBenchData =
        case BenchData of
            null ->
                {1, Time, 0.0, Time, Time, Aborts};
            {N, Mean, M2, Min, Max, AggAborts} ->
                Delta = Time - Mean,
                NewMean = Mean + Delta / (N + 1),
                {N + 1, NewMean, M2 + Delta*(Time - NewMean),
                 erlang:min(Time, Min), erlang:max(Time, Max),
                 AggAborts + Aborts}
        end,
    if BenchThreads =:= 1 ->
           RepTime = timer:now_diff(os:timestamp(), BenchStart),
           {NewN, RepMeanTime, NewM2, RepMinTime, RepMaxTime, RepAborts} = NewBenchData,
           RepVariance = if NewN =:= 1 -> 0.0;
                            true       -> NewM2 / (NewN - 1)
                         end,
           comm:send(BenchOwner, {done, comm_server:get_local_address_port(),
                                  RepTime, RepMeanTime, RepVariance,
                                  RepMinTime, RepMaxTime, RepAborts}),
           State#state{bench_owner = ok,
                       bench_start = ok,
                       bench_threads = 0,
                       bench_data = null};
       true ->
           State#state{bench_threads = BenchThreads - 1,
                       bench_data = NewBenchData}
    end.

-spec init([]) -> #state{}.
init([]) ->
    % load bench module
    _X = bench:module_info(),
    #state{load_pid=ok}.

%% @doc spawns a bench_server
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [], [{pid_groups_join_as, ServiceGroup, ?MODULE},
                                                             {erlang_register, ?MODULE}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc spawns threads and collect statistics
-spec run_threads(integer(),
                  fun((Parent::comm:erl_local_pid()) -> any())) -> ok.
run_threads(Threads, Bench) ->
    Self = self(),
    util:for_to(1, Threads,
       fun(_X) ->
           Pid = spawn_link(fun()->
                         trace_mpath:thread_yield(),
                         receive
                             ?SCALARIS_RECV({start_thread}, ok)
                         end,
                         log:log(debug, "Bench is infected ~p", [proto_sched:infected()]),
                         Bench(Self),
                         trace_mpath:thread_yield()
                       end),
                         log:log(debug, "Sending to spawned Bench ~p", [proto_sched:infected()]),
               comm:send_local(Pid, {start_thread})
       end),
    ok.
