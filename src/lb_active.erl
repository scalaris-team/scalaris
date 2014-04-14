%  @copyright 2014 Zuse Institute Berlin

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
%% @doc Active load balancing bootstrap module
%% @version $Id$
-module(lb_active).
-author('michels@zib.de').
-vsn('$Id$').

-behavior(gen_component).

-include("scalaris.hrl").
-include("record_helpers.hrl").

%%-define(TRACE(X,Y), ok).
-define(TRACE(X,Y), io:format(X,Y)).

%% startup
-export([start_link/1, init/1, check_config/0]).
%% gen_component
-export([on_inactive/2, on/2]).
%% for calls from the dht node
-export([handle_dht_msg/2]).
%% for db monitoring
-export([init_db_monitors/0, update_db_monitor/2]).
%% get load
-export([get_utilization/1]).

-type lb_message() :: {lb_active, comm:message()}.

-type (state() :: {}). %% state of lb module

%% list of active load balancing modules available
-define(MODULES_AVAIL, [lb_active_karger, lb_active_directories]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Initialization %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start this process as a gen component and register it in the dht node group
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun on_inactive/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, lb_active}]).


%% @doc Initialization of monitoring values
-spec init([]) -> state().
init([]) ->
    case collect_stats() of
        true ->
            %% TODO configure minutes to collect
            LongTerm  = rrd:create(60 * 5 * 1000000, 5, {timing, '%'}),
            ShortTerm = rrd:create(10 * 1000000, 1, gauge),
            monitor:client_monitor_set_value(lb_active, cpu5min, LongTerm),
            monitor:client_monitor_set_value(lb_active, cpu10sec, ShortTerm),
            monitor:client_monitor_set_value(lb_active, mem5min, LongTerm),
            monitor:client_monitor_set_value(lb_active, mem10sec, ShortTerm),
            application:start(sasl),   %% required by os_mon.
            application:start(os_mon), %% for monitoring cpu and memory usage.
            trigger(collect_stats);
        _ ->
            ok
    end,
    trigger(lb_trigger),
    {}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Startup message handler %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Handles all messages until enough monitor data has been collected.
-spec on_inactive(comm:message(), state()) -> state().
on_inactive({lb_trigger}, State) ->
    trigger(lb_trigger),
    case monitor_vals_appeared() of
        true ->
            InitState = call_module(init, []),
            ?TRACE("All monitor data appeared. Activating active load balancing~n", []),
            gen_component:change_handler(InitState, fun on/2);
        _    ->
            State
    end;

on_inactive({collect_stats} = Msg, State) ->
    on(Msg, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Main message handler %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc On handler after initialization
-spec on(comm:message(), state()) -> state().
on({collect_stats}, State) ->
    trigger(collect_stats),
    CPU = cpu_sup:util(),
    MEM = case memsup:get_system_memory_data() of
              [{system_total_memory, Total},
               {free_swap, FreeSwap},
               {total_swap, TotalSwap},
               {cached_memory, CachedMemory},
               {buffered_memory, BufferedMemory},
               {free_memory, FreeMemory},
               {total_memory, TotalMemory}] ->
                  FreeMemory / TotalMemory * 100
          end,
    monitor:client_monitor_set_value(lb_active, cpu10sec, fun(Old) -> rrd:add_now(CPU, Old) end),
    monitor:client_monitor_set_value(lb_active, cpu5min, fun(Old) -> rrd:add_now(CPU, Old) end),
    monitor:client_monitor_set_value(lb_active, mem10sec, fun(Old) -> rrd:add_now(MEM, Old) end),
    monitor:client_monitor_set_value(lb_active, mem5min, fun(Old) -> rrd:add_now(MEM, Old) end),
    %io:format("CPU utilization: ~p~n", [CPU]),
    State;

on({lb_trigger} = Msg, State) ->
    %% module can decide whether to trigger
    %% trigger(lb_trigger),
    call_module(handle_msg, [Msg, State]);

on({web_debug_info, Requestor}, State) ->
    KVList =
        [{"active module", webhelpers:safe_html_string("~p", [get_lb_module()])}
        ],
    Return = KVList ++ call_module(get_web_debug_key_value, [State]),
    comm:send_local(Requestor, {web_debug_info_reply, Return}),
    State;

on(Msg, State) ->
    call_module(handle_msg, [Msg, State]).

%%%%%%%%%%%%%%%%%%%%%%%% Calls from dht_node %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Process load balancing messages sent to the dht node
-spec handle_dht_msg(lb_message(), dht_node_state:state()) -> dht_node_state:state().
handle_dht_msg({lb_active, Msg}, DhtState) ->
    call_module(handle_dht_msg, [Msg, DhtState]).

%%%%%%%%%%%%%%%%%%%%%%%% Monitoring values %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Called by db process to initialize monitors
-spec init_db_monitors() -> ok.
init_db_monitors() ->
    case config:read(lb_active_monitor_db) of
        true ->
            Reads   = rrd:create(15 * 1000000, 3, {timing_with_hist, count}),
            Writes  = rrd:create(15 * 1000000, 3, {timing_with_hist, count}),
            monitor:proc_set_value(lb_active, reads, Reads),
            monitor:proc_set_value(lb_active, writes, Writes);
        _ -> ok
    end.

%% @doc Updates the local rrd for reads or writes and checks for reporting
-spec update_db_monitor(reads | writes, ?RT:key()) -> ok.
update_db_monitor(Type, Key) ->
    case config:read(lb_active_monitor_db) of
        true ->
            %% TODO Normalize key because histogram might contain circular elements, e.g. [MAXVAL, 0, 1]
            % KeyNorm = Key + intervals:get_bounds(_)
            monitor:proc_set_value(lb_active, Type, fun(Old) -> rrd:add_now(Key, Old) end);
            % monitor:proc_check_timeslot(lb_active, Type);
        _ -> ok
    end.

-spec monitor_vals_appeared() -> boolean().
monitor_vals_appeared() ->
    MonitorPid = pid_groups:get_my(monitor),
    ClientMonitorPid = pid_groups:pid_of("clients_group", monitor),
    LocalKeys = monitor:get_rrd_keys(MonitorPid),
    ClientKeys = monitor:get_rrd_keys(ClientMonitorPid),
    ReqLocalKeys =  case collect_stats() of
                        true -> [{lb_active, reads}, {lb_active, writes}, {api_tx, req_list}];
                        _ ->    [{api_tx, req_list}]
                    end,
    ReqClientKeys = case collect_stats() of
                        % %{api_tx, req_list},
                        true -> [{monitor_perf, mem_total}, {monitor_perf, rcv_bytes}, {monitor_perf, send_bytes}, {lb_active, cpu10sec}]; %{lb_active, cpu5min}],],
                        _    -> [{monitor_perf, mem_total}, {monitor_perf, rcv_bytes}, {monitor_perf, send_bytes}]
                    end,
    AllLocalAvailable  = lists:foldl(fun(Key, Acc) -> Acc andalso lists:member(Key, LocalKeys) end, true, ReqLocalKeys),
    AllClientAvailable = lists:foldl(fun(Key, Acc) -> Acc andalso lists:member(Key, ClientKeys) end, true, ReqClientKeys),
    %io:format("LocalKeys: ~p~n", [LocalKeys]),
    %io:format("ClientKeys: ~p~n", [ClientKeys]),
    AllLocalAvailable andalso AllClientAvailable.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metrics       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-spec get_load_info(dht_node_state:state()) -> load_info().
get_utilization(DhtState) ->
    %% TODO what to do with the dht state?
    _Utilization =
        case config:read(lb_active_metric) of
            cpu -> get_vm_metric(cpu10sec) / 100;
            mem -> get_vm_metric(mem10sec) / 100;
            _ -> log:log(warn, "~p: Falling back to default metric", [?MODULE])
        end,
    %% TODO remove this
    randoms:rand_uniform(0, 101) / 100.

-spec get_vm_metric(atom()) -> ok.
get_vm_metric(Key) ->
    ClientMonitorPid = pid_groups:pid_of("clients_group", monitor),
    get_metric(ClientMonitorPid, Key).

get_dht_metric(Key) ->
    MonitorPid = pid_groups:get_my(monitor),
    get_metric(MonitorPid, Key).

%% TODO
get_metric(MonitorPid, Key) ->
    [{lb_active, Key, RRD}] = monitor:get_rrds(MonitorPid, [{lb_active, Key}]),
    _Value = rrd:get_value_by_offset(RRD, 0).


%%%%%%%%%%%%%%%%%%%%%%%%%%%% Util %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec call_module(atom(), list()) -> state().
call_module(Fun, Args) ->
    apply(get_lb_module(), Fun, Args). 

-spec get_lb_module() -> atom() | failed.
get_lb_module() ->
    config:read(lb_active_module).

-spec collect_stats() -> boolean().
collect_stats() ->
    config:read(lb_active_collect_stats).

-spec trigger(atom()) -> ok.
trigger(Trigger) ->
    Interval = config:read(lb_active_interval),
    msg_delay:send_trigger(Interval div 1000, {Trigger}).

%% @doc config check registered in config.erl
-spec check_config() -> boolean().
check_config() ->
    config:cfg_exists(lb_active_module) andalso
    config:cfg_is_in(lb_active_module, ?MODULES_AVAIL) andalso
    config:cfg_exists(lb_active_interval) andalso
    config:cfg_is_greater_than(lb_active_interval, 0) andalso
    config:cfg_exists(lb_active_monitor_db) andalso
    config:cfg_exists(lb_active_metric) andalso
    apply(get_lb_module(), check_config, []).
