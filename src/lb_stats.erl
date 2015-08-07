%  @copyright 2014-2015 Zuse Institute Berlin
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

%% @author Maximilian Michels <michels@zib.de>
%% @doc Active load balancing stats module which implements collecting
%%      and accessing stats for the lb_active module.
%% @version $Id$
-module(lb_stats).
-author('michels@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

%-define(TRACE(X,Y), ok).
-define(TRACE(X,Y), io:format("lb_stats: " ++ X, Y)).

%% get split key based on the request histogram
-export([get_request_histogram_split_key/3]).

%% for db monitoring
-export([init/0, init_db_histogram/1, update_db_histogram/2, update_db_monitor/2]).
-export([set_ignore_db_requests/1, get_ignore_db_requests/0]).
%% Metrics
-export([get_load_metric/0, get_request_metric/0, default_value/1]).
%% Triggered by lb_active
-export([trigger/0, trigger_routine/0]).
%% config checked by lb_active
-export([check_config/0]).

-export_type([load/0]).

-type load() :: number().

-type load_metric() :: cpu | mem | reductions.
-type request_metric() :: db_histogram.

%% possible metrics
% items, cpu, mem, db_reads, db_writes, db_requests,
% transactions, tx_latency, net_throughput, net_latency
%% available metrics
-define(LOAD_METRICS, [cpu, mem, reductions]).
-define(REQUEST_METRICS, [db_reads, db_writes, db_all]).

%%%%%%%%%%%%%%%%%%%%%%%% Monitoring values %%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init() -> ok.
init() ->
    case collect_stats() of
        true ->
            %% cpu_sup not available on all OSs
            catch cpu_sup:util(), %% throw away first util value
            {InitialReductions, Timestamp} = get_reductions(),
            set_last_reductions(InitialReductions, Timestamp),
            ResolutionSecs = config:read(lb_active_monitor_resolution) div 1000,
            {MegaSecs, Secs, _Microsecs} = os:timestamp(),
            %% synchronize the start time for all monitors to a divisible of the monitor interval
            StartTime = {MegaSecs, Secs - Secs rem ResolutionSecs + ResolutionSecs, 0},
            % only store newest value in rrd, monitor stores more values
            RRD = rrd:create(ResolutionSecs * 1000000, 1, gauge, StartTime),
            monitor:client_monitor_set_value(lb_active, cpu, RRD),
            monitor:client_monitor_set_value(lb_active, mem, RRD),
            monitor:monitor_set_value(lb_active, reductions, RRD);
        _ ->
            ok
    end.

-spec trigger_routine() -> ok.
trigger_routine() ->
    trigger(),
    %% cpu_sup not available on all OSs
    CPU = try
              erlang:round(cpu_sup:util())
          catch
              _ -> 0
              %% can we use the avg1 instead, somehow?
              %% _ -> erlang:round(cpu_sup:avg1())
          end,

    %% actual keys memsup returns depends on the OS
    MemInfo = memsup:get_system_memory_data(),
    {free_memory, FreeMemory} = lists:keyfind(free_memory, 1, MemInfo),
    {total_memory, TotalMemory} = lists:keyfind(total_memory, 1, MemInfo),

    MEM = FreeMemory / TotalMemory * 100,
    {NewReductions, NewTimestamp} = get_reductions(),
    {OldReductions, OldTimestamp} = get_last_reductions(),
    TimeDiff = timer:now_diff(NewTimestamp, OldTimestamp) div 1000000,
    if TimeDiff > 0 -> % let at least one second pass
           ReductionsPerSec = (NewReductions - OldReductions) div TimeDiff,
           set_last_reductions(NewReductions, NewTimestamp),
           DhtNodeMonitor = pid_groups:get_my(dht_node_monitor),
           comm:send_local(DhtNodeMonitor, {db_report}),
           monitor:monitor_set_value(lb_active, reductions, fun(Old) -> rrd:add(NewTimestamp, ReductionsPerSec, Old) end);
       true -> ok
    end,
    monitor:client_monitor_set_value(lb_active, cpu, fun(Old) -> rrd:add(NewTimestamp, CPU, Old) end),
    monitor:client_monitor_set_value(lb_active, mem, fun(Old) -> rrd:add(NewTimestamp, MEM, Old) end).

-compile({inline, [update_db_monitor/2]}).
%% @doc Updates the local rrd for reads or writes and checks for reporting
-spec update_db_monitor(Type::db_reads | db_writes, Key::?RT:key()) -> ok.
update_db_monitor(Type, Key) ->
    case lb_active:is_enabled() andalso not get_ignore_db_requests() andalso
             (config:read(lb_active_request_metric) =:= Type orelse
                  config:read(lb_active_request_metric) =:= db_all) of
        true ->
            DhtNodeMonitor = pid_groups:get_my(dht_node_monitor),
            comm:send_local(DhtNodeMonitor, {db_op, Key});
        _ -> ok
    end.

-compile({inline, [init_db_histogram/1]}).
%% @doc Called by dht node process to initialize the db monitors
-spec init_db_histogram(PredId::?RT:key()) -> rrd:rrd().
init_db_histogram(PredId) ->
    HistogramSize = config:read(lb_active_histogram_size),
    HistogramType = {histogram_rt, HistogramSize, PredId},
    MonitorResSecs = config:read(lb_active_monitor_resolution) div 1000,
    {MegaSecs, Secs, _Microsecs} = os:timestamp(),
    %% synchronize the start time for all monitors to a divisible of the monitor interval
    StartTime = {MegaSecs, Secs - Secs rem MonitorResSecs + MonitorResSecs, 0},
    % only store newest value in rrd, monitor stores more values
    RRD  = rrd:create(MonitorResSecs*1000000, 1, HistogramType, StartTime),
    Monitor = pid_groups:get_my(monitor),
    monitor:clear_rrds(Monitor, [{lb_active, db_histogram}]),
    monitor:monitor_set_value(lb_active, db_histogram, RRD),
    RRD.

-compile({inline, [update_db_histogram/2]}).
%% @doc Updates the local rrd for reads or writes and checks for reporting
-spec update_db_histogram(Key::?RT:key(), OldHistogram::rrd:rrd()) -> rrd:rrd().
update_db_histogram(Key, OldHistogram) ->
    NewHistogram = %% only update the histogram if necessary
        case config:read(lb_active_balance) of
            requests ->
                NewHist = rrd:add_now(Key, OldHistogram),
                monitor:check_report(lb_active, db_histogram, OldHistogram, NewHist),
                NewHist;
            _ -> OldHistogram
        end,
    NewHistogram.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metrics       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_load_metric() -> unknown | load().
get_load_metric() ->
    Metric = config:read(lb_active_load_metric),
    Value = case get_load_metric(Metric) of
                unknown -> unknown;
                Val     -> erlang:round(Val)
            end,
    Value.

-spec get_load_metric(load_metric()) -> unknown | load().
get_load_metric(cpu)        -> get_vm_metric(cpu);
get_load_metric(reductions) -> get_dht_metric(reductions);
get_load_metric(mem)        -> get_vm_metric(mem);
get_load_metric(_)          -> throw(metric_not_available).

-spec get_request_metric() -> integer().
get_request_metric() ->
    case get_dht_metric(db_histogram) of
        unknown -> unknown;
        Val -> erlang:round(Val)
    end.

-spec get_vm_metric(load_metric()) -> unknown | load().
get_vm_metric(Metric) ->
    ClientMonitorPid = pid_groups:pid_of(clients_group, monitor),
    get_metric(ClientMonitorPid, Metric).

-spec get_dht_metric(load_metric() | request_metric()) -> unknown | load().
get_dht_metric(Metric) ->
    MonitorPid = pid_groups:get_my(monitor),
    get_metric(MonitorPid, Metric).

-spec get_metric(pid(), load_metric() | request_metric()) -> unknown | load().
get_metric(MonitorPid, Metric) ->
    case monitor:get_rrds(MonitorPid, [{lb_active, Metric}]) of
        [{_Process, _Key, undefined}] ->
            unknown;
        [{_Process, _Key, RRD}] ->
            RRDVals = rrd:get_all_values(asc, RRD),
            %% max number of consecutive non-undefined values at the end of RRDVals
            NumDefined = lists:foldl(fun(undefined, _Acc) -> 0;
                                        (_, Acc)          -> Acc + 1
                                     end, 0, RRDVals),
            NumNeeded = config:read(lb_active_monitor_history_min),
            if NumDefined >= NumNeeded ->
                   NumValues = rrd:get_count(RRD),
                   DefinedVals = lists:sublist(RRDVals, NumValues - NumDefined + 1, NumDefined),
                   Type = rrd:get_type(RRD),
                   UnpackedVals = lists:map(fun(Val) -> get_value_type(Val, Type) end, DefinedVals),
                   avg_weighted(UnpackedVals);
               true -> unknown
            end
    end.

-spec get_value_type(RRD::rrd:data_type(), Type::rrd:timeseries_type()) -> unknown | number().
get_value_type(undefined, _Type) ->
    unknown;
get_value_type(Value, _Type) when is_number(Value) ->
    Value;
get_value_type(Value, {histogram_rt, _Size, _BaseKey}) ->
    histogram_rt:get_num_inserts(Value).

%% @doc returns the weighted average of a list using an increasing weight
-spec avg_weighted([number()]) -> number().
avg_weighted([]) ->
    0;
avg_weighted(List) ->
    avg_weighted(List, _Weight=1, _Normalize=0, _Sum=0).

%% @doc returns the weighted average of a list using increasing weight
-spec avg_weighted([number(),...], Weight::pos_integer(), Normalize::non_neg_integer(), Sum::number()) -> float().
avg_weighted([], _Weight, N, Sum) ->
    Sum/N;
avg_weighted([Element | Other], Weight, N, Sum) ->
    %% linear weight
    CurrentWeight = Weight,
    %% quadratic weight
    %CurrentWeight = Weight * Weight,
    %% exponential weight
    %CurrentWeight = util:pow(2, Weight),
    avg_weighted(Other, Weight + 1, N + CurrentWeight, Sum + CurrentWeight * Element).

%% @doc returns a split key from the request histogram at a given time (if available)
-spec get_request_histogram_split_key(TargetLoad::pos_integer(),
                                      Direction::forward | backward,
                                      Items::non_neg_integer())
        -> {?RT:key(), TakenLoad::non_neg_integer()} | failed.
get_request_histogram_split_key(TargetLoad, Direction, Items) ->
    MonitorPid = pid_groups:get_my(monitor),
    case monitor:get_rrds(MonitorPid, [{lb_active, db_histogram}]) of
        [{_Process, _Key, undefined}] ->
            ?TRACE("No request histogram available because rrd is undefined.~n", []),
            failed;
        [{_Process, _Key, RRD}] ->
            %% values from oldest to newest
            AllValues = rrd:get_all_values(asc, RRD),
            NumNeeded = config:read(lb_active_monitor_history_min),
            %% check if we have enough recent values
            NumDefined = lists:foldl(fun(El, Acc) ->
                                             if El =:= undefined -> 0;
                                                true -> Acc + 1
                                             end
                                     end, 0, AllValues),
            if NumDefined >= NumNeeded ->
                   ?TRACE("Got ~p histograms to compute split key~n", [NumDefined]),
                   %% merge all histograms with weight (the older the lower the weight)
                   NumAll = rrd:get_count(RRD),
                   Values = lists:sublist(AllValues, NumAll - NumDefined + 1, NumDefined),
                   {AllHists, _} = lists:foldl(
                                     fun(Hist, {AccHist, Weight}) ->
                                             {histogram_rt:merge_weighted(AccHist, Hist, Weight), Weight + 1}
                                     end, {hd(Values), 2}, tl(Values)),
                   %% normalize afterwards
                   Histogram = histogram_rt:normalize_count((NumDefined*(NumDefined+1)) div 2, AllHists),
                   % check if enough requests have been inserted into the histogram
                   EntriesAvailable = histogram_rt:get_num_inserts(Histogram),
                   Confidence = config:read(lb_active_request_confidence),
                   if Items =:= 0 orelse EntriesAvailable / Items < Confidence ->
                          ?TRACE("Confidence too low (below ~p) for request balancing~n", [Confidence]),
                          failed;
                      true ->
                          {Status, Key, TakenLoad} =
                              case Direction of
                                  forward -> histogram_rt:foldl_until(TargetLoad, Histogram);
                                  backward -> histogram_rt:foldr_until(TargetLoad, Histogram)
                              end,
                          case Status of
                              fail -> failed;
                              ok -> {Key, TakenLoad}
                          end
                   end;
               true ->
                   ?TRACE("Not enough request histograms available: ~p~n", [NumDefined]),
                   failed
            end
    end.

%% @doc get the reductions of all processes in the pid group
-spec get_reductions() -> {non_neg_integer(), erlang_timestamp()}.
get_reductions() ->
    MyGroupPids = pid_groups:my_members(),
    AllReductions =
        [begin
             {reductions, N} = erlang:process_info(Pid, reductions),
             N
         end || Pid <- MyGroupPids, Pid =/= self()],
    {lists:sum(AllReductions), os:timestamp()}.

-spec set_last_reductions(non_neg_integer(), erlang_timestamp()) -> ok.
set_last_reductions(Reductions, Timestamp) ->
    erlang:put(reductions, {Reductions, Timestamp}),
    ok.

-spec get_last_reductions() -> {non_neg_integer(), erlang_timestamp()}.
get_last_reductions() ->
    erlang:get(reductions).

%%%%%%%%%%%%%%%%%%%%%%%%%% Util %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sets a default value if the value is unknown
-spec default_value(Val::unknown | number()) -> number().
default_value(unknown) -> 0;
default_value(Val)     -> Val.

-compile({inline, [set_ignore_db_requests/1, get_ignore_db_requests/0]}).
%% @doc Sets an indicator for lb_stats to stop monitoring requests during slides
-spec set_ignore_db_requests(boolean()) -> ok.
set_ignore_db_requests(Bool) -> erlang:put(ignore_db_requests, Bool), ok.

%% @doc Flag for the dht_node process to check if the current message is a slide message
-spec get_ignore_db_requests() -> boolean().
get_ignore_db_requests() -> erlang:get(ignore_db_requests) =:= true.

-spec collect_stats() -> boolean().
collect_stats() ->
    lists:member(config:read(lb_active_load_metric), ?LOAD_METRICS).

-spec trigger() -> ok.
trigger() ->
    Interval = config:read(lb_active_monitor_interval) div 1000,
    msg_delay:send_trigger(Interval, {lb_stats_trigger}).


%% @doc config check by lb_active module
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(lb_active_load_metric, ?LOAD_METRICS) and

    config:cfg_is_in(lb_active_request_metric, ?REQUEST_METRICS) and

    config:cfg_is_integer(lb_active_histogram_size) and
    config:cfg_is_greater_than(lb_active_histogram_size, 0) and

    config:cfg_is_integer(lb_active_monitor_resolution) and
    config:cfg_is_greater_than(lb_active_monitor_resolution, 0) and

    config:cfg_is_integer(lb_active_monitor_interval) and
    config:cfg_is_greater_than(lb_active_monitor_interval, 0) and

    config:cfg_is_less_than(lb_active_monitor_interval, config:read(lb_active_monitor_resolution)) and

    config:cfg_is_integer(lb_active_monitor_history_min) and
    config:cfg_is_greater_than(lb_active_monitor_history_min, 0) and

    config:cfg_is_integer(lb_active_monitor_history_max) and
    config:cfg_is_greater_than_equal(lb_active_monitor_history_max, config:read(lb_active_monitor_history_min)) and

    config:cfg_is_float(lb_active_request_confidence) and
    config:cfg_is_greater_than_equal(lb_active_request_confidence, 0.0).
