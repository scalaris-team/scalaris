%  @copyright 2014 Zuse Institute Berlin
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

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), io:format("lb_stats: " ++ X, Y)).

%% get split key based on the request histogram
-export([get_request_histogram_split_key/3]).

%% for db monitoring
-export([init/0, init_db_rrd/1, update_db_rrd/2, update_db_monitor/2]).
-export([monitor_db/0]).
%% Metrics
-export([get_load_metric/0, get_request_metric/0, default_value/1]).
%% Triggered by lb_active
-export([trigger_routine/0]).
%% config checked by lb_active
-export([check_config/0]).

-ifdef(with_export_type_support).
-export_type([load/0]).
-endif.

-type load() :: number().

-type load_metric() :: cpu | mem | reductions.
-type request_metric() :: db_reads | db_writes.

%% possible metrics
% items, cpu, mem, db_reads, db_writes, db_requests,
% transactions, tx_latency, net_throughput, net_latency
%% available metrics
-define(LOAD_METRICS, [cpu, mem, reductions]).
-define(REQUEST_METRICS, [db_reads, db_writes]).

%%%%%%%%%%%%%%%%%%%%%%%% Monitoring values %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init() -> ok.
init() ->
    case collect_stats() of
        true ->
            _ = application:start(sasl),   %% required by os_mon.
            _ = application:start(os_mon), %% for monitoring cpu and memory usage.
            _ = cpu_sup:util(), %% throw away first util value
            InitialReductions = get_reductions(),
            set_last_reductions(InitialReductions),
            Resolution = config:read(lb_active_monitor_resolution),
            % only store newest value in rrd, monitor stores more values
            RRD = rrd:create(Resolution * 1000, 1, gauge),
            monitor:client_monitor_set_value(lb_active, cpu, RRD),
            monitor:client_monitor_set_value(lb_active, mem, RRD),
            monitor:monitor_set_value(lb_active, reductions, RRD),
            trigger();
        _ ->
            ok
    end.

-spec trigger_routine() -> ok.
trigger_routine() ->
    trigger(),
    CPU = cpu_sup:util(),
    MEM = case memsup:get_system_memory_data() of
              [{system_total_memory, _Total},
               {free_swap, _FreeSwap},
               {total_swap, _TotalSwap},
               {cached_memory, _CachedMemory},
               {buffered_memory, _BufferedMemory},
               {free_memory, FreeMemory},
               {total_memory, TotalMemory}] ->
                  FreeMemory / TotalMemory * 100
          end,
    Reductions = get_reductions() - get_last_reductions(),
    monitor:client_monitor_set_value(lb_active, cpu, fun(Old) -> rrd:add_now(CPU, Old) end),
    monitor:client_monitor_set_value(lb_active, mem, fun(Old) -> rrd:add_now(MEM, Old) end),
    monitor:monitor_set_value(lb_active, reductions, fun(Old) -> rrd:add_now(Reductions, Old) end).

-compile({inline, [init_db_rrd/1]}).
%% @doc Called by dht node process to initialize the db monitors
-spec init_db_rrd(Id::?RT:key()) -> rrd:rrd().
init_db_rrd(Id) ->
    Type = config:read(lb_active_db_monitor),
    HistogramSize = config:read(lb_active_histogram_size),
    HistogramType = {histogram_rt, HistogramSize, Id},
    MonitorResSecs = config:read(lb_active_monitor_resolution) div 1000,
    {MegaSecs, Secs, _Microsecs} = os:timestamp(),
    %% synchronize the start time for all monitors to a divisible of the monitor interval
    StartTime = {MegaSecs, Secs - Secs rem MonitorResSecs + MonitorResSecs, 0},
    % only store newest value in rrd, monitor stores more values
    RRD  = rrd:create(MonitorResSecs*1000000, 1, HistogramType, StartTime),
    Monitor = pid_groups:get_my(monitor),
    monitor:clear_rrds(Monitor, [{lb_active, Type}]),
    monitor:monitor_set_value(lb_active, Type, RRD),
    RRD.

-compile({inline, [update_db_monitor/2]}).
%% @doc Updates the local rrd for reads or writes and checks for reporting
-spec update_db_monitor(Type::db_reads | db_writes, Value::?RT:key()) -> ok.
update_db_monitor(Type, Value) ->
    case monitor_db() andalso config:read(lb_active_db_monitor) =:= Type of
        true ->
            DhtNodeMonitor = pid_groups:get_my(dht_node_monitor),
            comm:send_local(DhtNodeMonitor, {db_op, Value});
        _ -> ok
    end.

-compile({inline, [update_db_rrd/2]}).
%% @doc Updates the local rrd for reads or writes and checks for reporting
-spec update_db_rrd(Value::?RT:key(), RRD::rrd:rrd()) -> rrd:rrd().
update_db_rrd(Key, OldRRD) ->
    Type = config:read(lb_active_db_monitor),
    NewRRD = rrd:add_now(Key, OldRRD),
    monitor:check_report(lb_active, Type, OldRRD, NewRRD),
    NewRRD.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metrics       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_load_metric() -> unknown | load().
get_load_metric() ->
    Metric = config:read(lb_active_load_metric),
    Value = case get_load_metric(Metric) of
                unknown -> unknown;
                Val     -> util:round(Val, 2)
            end,
    %io:format("Load: ~p~n", [Value]),
    Value.

-spec get_load_metric(load_metric()) -> unknown | load().
get_load_metric(cpu)        -> get_vm_metric(cpu);
get_load_metric(reductions) -> get_dht_metric(reductions);
get_load_metric(mem)        -> get_vm_metric(mem);
get_load_metric(_)          -> throw(metric_not_available).

-spec get_request_metric() -> integer().
get_request_metric() ->
    Metric = config:read(lb_active_request_metric),
    Value = case get_request_metric(Metric) of
                unknown -> 0;
                Val -> erlang:round(Val)
            end,
    %io:format("Requests: ~p~n", [Value]),
    Value.

-spec get_request_metric(request_metric()) -> unknown | load().
get_request_metric(Metric) ->
    case Metric of
        db_reads -> get_dht_metric(db_reads);
        db_writes -> get_dht_metric(db_writes)
        %db_requests  -> get_request_metric(db_reads, Mode) +
        %                get_request_metric(db_writes, Mode); %% TODO
    end.

-spec get_vm_metric(load_metric()) -> unknown | load().
get_vm_metric(Metric) ->
    ClientMonitorPid = pid_groups:pid_of("clients_group", monitor),
    get_metric(ClientMonitorPid, Metric).

-spec get_dht_metric(load_metric() | request_metric()) -> unknown | load().
get_dht_metric(Metric) ->
    MonitorPid = pid_groups:get_my(monitor),
    get_metric(MonitorPid, Metric).

-spec get_metric(pid(), load_metric() | request_metric()) -> unknown | load().
get_metric(MonitorPid, Metric) ->
    [{_Process, _Key, RRD}] = monitor:get_rrds(MonitorPid, [{lb_active, Metric}]),
    case RRD of
        undefined ->
            unknown;
        RRD ->
            RRDVals = rrd:get_all_values(desc, RRD),
            Type = rrd:get_type(RRD),
            Vals = lists:map(fun(Val) -> get_value_type(Val, Type) end, RRDVals),
            %io:format("~p Vals: ~p~n", [Metric, Vals]),
            ?IIF(lists:member(unknown, Vals), unknown, avg_weighted(Vals))
    end.

-spec get_value_type(RRD::rrd:data_type(), Type::rrd:timeseries_type()) -> unknown | number().
get_value_type(undefined, _Type) ->
    unknown;
get_value_type(Value, _Type) when is_number(Value) ->
    Value;
get_value_type(Value, {histogram_rt, _Size, _BaseKey}) ->
    histogram_rt:get_num_inserts(Value).

%% @doc returns the weighted average of a list using decreasing weight
-spec avg_weighted([number()]) -> number().
avg_weighted([]) ->
    0;
avg_weighted(List) ->
    avg_weighted(List, _Weight=length(List), _Normalize=0, _Sum=0).

%% @doc returns the weighted average of a list using decreasing weight
-spec avg_weighted([], Weight::0, Normalize::pos_integer(), Sum::number()) -> unknown | float();
                  ([number()| unknown,...], Weight::pos_integer(), Normalize::non_neg_integer(), Sum::number()) -> unknown | float().
avg_weighted([], 0, 0, _Sum) ->
    unknown;
avg_weighted([], 0, N, Sum) ->
    Sum/N;
avg_weighted([unknown | Other], Weight, N, Sum) ->
    avg_weighted(Other, Weight - 1, N + Weight, Sum);
avg_weighted([Element | Other], Weight, N, Sum) ->
    avg_weighted(Other, Weight - 1, N + Weight, Sum + Weight * Element).

%% @doc returns a split key from the request histogram at a given time (if available)
-spec get_request_histogram_split_key(TargetLoad::pos_integer(),
                                      Direction::forward | backward,
                                      Items::non_neg_integer())
        -> {?RT:key(), TakenLoad::non_neg_integer()} | failed.
get_request_histogram_split_key(TargetLoad, Direction, Items) ->
    MonitorPid = pid_groups:get_my(monitor),
    RequestMetric = config:read(lb_active_request_metric),
    [{_Process, _Key, RRD}] = monitor:get_rrds(MonitorPid, [{lb_active, RequestMetric}]),
    case RRD of
        undefined ->
            log:log(warn, "No request histogram available because rrd is undefined."),
            failed;
        RRD ->
            AllValues = rrd:get_all_values(asc, RRD),
            NumValues = rrd:get_count(RRD),
            AllDefined = lists:all(fun(El) -> El =/= undefined end, AllValues),
            if AllDefined ->
                   ?TRACE("Got ~p histograms to compute split key~n", [NumValues]),
                   %% merge all histograms with weight (the older the lower the weight)
                   {AllHists, _} = lists:foldl(
                                     fun(Hist, {AccHist, Weight}) ->
                                             io:format("Weight: ~p~n", [Weight]),
                                             {histogram_rt:merge_weighted(AccHist, Hist, Weight), Weight + 1}
                                     end, {hd(AllValues), 2}, tl(AllValues)),
                   %% normalize afterwards
                   Histogram = histogram_rt:normalize_count(NumValues, AllHists),
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
                   log:log(warn, "Not enough request histograms available. Not enough data collected."),
                   failed
            end
    end.

%% @doc get the reductions of all processes in the pid group
-spec get_reductions() -> non_neg_integer().
get_reductions() ->
    MyGroupPids = pid_groups:my_members(),
    AllReductions =
        [begin
             {reductions, N} = erlang:process_info(Pid, reductions),
             N
         end || Pid <- MyGroupPids, Pid =/= self()],
    lists:sum(AllReductions).

-spec set_last_reductions(non_neg_integer()) -> ok.
set_last_reductions(Reductions) ->
    erlang:put(reductions, Reductions),
    ok.

-spec get_last_reductions() -> non_neg_integer().
get_last_reductions() ->
    erlang:get(reductions).

%%%%%%%%%%%%%%%%%%%%%%%%%% Util %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sets a default value if the value is unknown
-spec default_value(Val::unknown | number()) -> number().
default_value(unknown) -> 0;
default_value(Val)     -> Val.

-spec collect_stats() -> boolean().
collect_stats() ->
    Metrics = [cpu, mem, reductions],
    lists:member(config:read(lb_active_load_metric), Metrics).

-spec trigger() -> ok.
trigger() ->
    Interval = config:read(lb_active_monitor_interval) div 1000,
    msg_delay:send_trigger(Interval, {collect_stats}).

-compile({inline, [monitor_db/0]}).
-spec monitor_db() -> boolean().
monitor_db() ->
    lb_active:is_enabled() andalso config:read(lb_active_db_monitor) =/= none.

%% @doc config check registered in config.erl
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(lb_active_load_metric, ?LOAD_METRICS) and

    config:cfg_is_in(lb_active_request_metric, ?REQUEST_METRICS) and

    config:cfg_is_in(lb_active_balance_metric, [items, requests]) and

    config:cfg_is_integer(lb_active_histogram_size) and
    config:cfg_is_greater_than(lb_active_histogram_size, 0) and

    config:cfg_is_integer(lb_active_monitor_resolution) and
    config:cfg_is_greater_than(lb_active_monitor_resolution, 0) and

    config:cfg_is_integer(lb_active_monitor_interval) and
    config:cfg_is_greater_than(lb_active_monitor_interval, 0) and

    config:cfg_is_less_than(lb_active_monitor_interval, config:read(lb_active_monitor_resolution)) and

    config:cfg_is_integer(lb_active_monitor_history) and
    config:cfg_is_greater_than(lb_active_monitor_history, 0) and

    config:cfg_is_float(lb_active_request_confidence) and
    config:cfg_is_greater_than(lb_active_request_confidence, 0.0) and

    config:cfg_is_in(lb_active_db_monitor, [none, db_reads, db_writes]).
