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
%% @doc Active load balancing service process
%% @version $Id$
-module(lb_active_metrics).
-author('michels@zib.de').
-vsn('$Id$').

-behavior(gen_component).
-behavior(lb_active_beh).

-include("scalaris.hrl").
-include("record_helpers.hrl").

%% for gen_component behavior
-export([start_link/1, init/1]).
-export([on_inactive/2, on/2]).
%% for lb_active_beh behavior
-export([process_lb_msg/2]).
-export([check_config/0]).

-export([init_db_monitors/0, update_db_monitor/2]).
-export([request_mutex/0]).

% Metrics getter
-export([get_metric/2, get_dht_metric/1, get_vm_metric/1]).

%% testing
-export([test_request_mutex1/0, test_request_mutex2/0]).

-define(TRACE(X,Y), ok).
%%-define(TRACE(X,Y), io:format(X,Y)).

-record(state, {%% time requested has to be a logical clock!
                mutex_requested = nil :: erlang:now() | nil,
                mutex_replies_pending = 0,
                mutex_monitors = []
      }).

-type (state() :: #state{}).

-type (message() :: {lb_trigger}                         |
                    {collect_stats}                      |
                    {mutex_init}                         |
                    {mutex_request, erlang:now(), pid()} |
                    {mutex_reply, erlang:now(), pid()}
       ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Initialization %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start this process as a gen component and register it in the dht node group
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on_inactive/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, lb_active_metrics}]).

%% @doc Initialization of monitoring values
-spec init([]) -> state().
init([]) ->
    CPU  = rrd:create(60 * 5 * 1000000, 5, {timing, '%'}),
    CPU2 = rrd:create(10 * 1000000, 1, gauge),
    monitor:client_monitor_set_value(lb_active, cpu5min, CPU),
    monitor:client_monitor_set_value(lb_active, cpu10sec, CPU2),
    application:start(sasl),   %% required by os_mon.
    application:start(os_mon), %% for monitoring cpu usage.
    trigger(collect_stats),
    trigger(lb_trigger),
    #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Startup message handler %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Handles all messages until enough monitor data has been collected.
-spec on_inactive(message(), state()) -> state().
on_inactive({lb_trigger}, State) ->
    trigger(lb_trigger),
    case monitor_vals_appeared() of
        true -> gen_component:change_handler(State, fun on/2);
        _    -> State
    end;

on_inactive({collect_stats} = Msg, State) ->
    on(Msg, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Main message handler %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc On handler after initialization
-spec on(message(), state()) -> state().
on({collect_stats}, State) ->
    trigger(collect_stats),
    CPU = cpu_sup:util(),
    monitor:client_monitor_set_value(lb_active, cpu10sec, fun(Old) -> rrd:add_now(CPU, Old) end),
    monitor:client_monitor_set_value(lb_active, cpu5min, fun(Old) -> rrd:add_now(CPU, Old) end),
    %io:format("CPU utilization: ~p~n", [CPU]),
    State;

on({lb_trigger}, State) ->
    %Metric = compute_lb_metric(),
    State;

%%%%%%%%%%%%%%%%%%%%%%%% Mutex %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Communicates with other load balancing processes in other nodes to
%%      execute a function mutual exlusively.
%% @end        
%% @ref Ricart-Agrawala algorithm
%%      Maekawa, M.,Oldehoeft, A.,Oldehoeft, R.(1987). 
%%      Operating Systems: Advanced Concept.
%%      Benjamin/Cummings Publishing Company, Inc.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Start request to enter critical section
on({mutex_init}, State) ->
    Pids = pid_groups:find_all(?MODULE),
    Timestamp = erlang:now(),
    Monitors = [begin
             Monitor = erlang:monitor(process, Pid),
             comm:send_local(Pid, {mutex_request, Timestamp, self()}),
             Monitor 
         end || Pid <- Pids],
    State#state{mutex_requested = Timestamp, mutex_replies_pending = length(Pids), mutex_monitors = Monitors};

%% @doc Mutex requests for load balancing processes in this scalaris instance
on({mutex_request, Timestamp, Pid} = Msg, State) ->
    MutexRequested = State#state.mutex_requested,
    case Pid =:= self() orelse MutexRequested =:= nil 
        orelse happened_before(Timestamp, MutexRequested) of
        true  ->
            %% Other process requested before us or we didn't request a mutex
            %io:format("~p: Replying to mutex~n", [self()]),
            comm:send_local(Pid, {mutex_reply, erlang:now(), self()});
        false ->
            %% We requested before this request, therefore delay reply
            %io:format("~p: Skipping message~n", [self()]),
            comm:send_local(self(), Msg)
    end,
    State;

%% @doc Collect replies here
on({mutex_reply, _Timestamp, _Pid}, State) ->
    RepliesPending = State#state.mutex_replies_pending,
    MutexRequested = State#state.mutex_requested,
    case RepliesPending =< 0 andalso MutexRequested =/= nil of
        true ->
            _ = [erlang:demonitor(Ref, [flush]) || Ref <- State#state.mutex_monitors],
            State2 = State#state{mutex_requested = nil, mutex_replies_pending = 0, mutex_monitors = []},
            gen_component:post_op({mutex_critical_section}, State2);
        _    -> 
            State#state{mutex_replies_pending = RepliesPending - 1}
    end;

%% @doc Process is in critical section
on({mutex_critical_section}, State) ->
    io:format("~p: I'm in critical section~n", [self()]),
    timer:sleep(1000),
    io:format("~p: I'm leaving the critical section~n", [self()]),
    State;

on({'DOWN', MonitorRef, process, _Pid, _Info}, State) ->
    Monitors = State#state.mutex_monitors,
    Pending  = State#state.mutex_replies_pending,
    State#state{mutex_monitors = lists:delete(MonitorRef, Monitors), mutex_replies_pending = Pending - 1}.

%% @doc To be executed in dht node group to perform a mutual 
%%      exlusive load balancing action
-spec request_mutex() -> ok.
request_mutex() ->
    Pid = pid_groups:get_my(?MODULE),
    comm:send_local(Pid, {mutex_init}).

test_request_mutex1() ->
    Pids = pid_groups:find_all(?MODULE),
    Pid = lists:nth(randoms:rand_uniform(1, length(Pids)), Pids),
    comm:send_local(Pid, {mutex_init}).
test_request_mutex2() ->
    Pids = pid_groups:find_all(?MODULE),
    _ = [comm:send_local(Pid, {mutex_init}) || Pid <- Pids].

%%%%%%%%%%%%%%%%%%%%%%%% Calls from dht_node %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Process load balancing messages sent to the dht node
-spec process_lb_msg(lb_active:lb_message(), dht_node_state:state()) -> dht_node_state:state(). 
process_lb_msg({lb_active, Msg}, DhtState) ->
    io:format("Got message ~p~n", [Msg]),
    DhtState.

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
%% TODO report in trigger and not every time 
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
    ReqLocalKeys  = [{lb_active, reads}, {lb_active, writes}, {api_tx, req_list}],
    ReqClientKeys = [{monitor_perf, mem_total}, {monitor_perf, rcv_bytes}, {monitor_perf, send_bytes},{api_tx, req_list},
                     {lb_active, cpu10sec}], %{lb_active, cpu5min}],],
    AllLocalAvailable  = lists:foldl(fun(Key, Acc) -> Acc andalso lists:member(Key, LocalKeys) end, true, ReqLocalKeys),
    AllClientAvailable = lists:foldl(fun(Key, Acc) -> Acc andalso lists:member(Key, ClientKeys) end, true, ReqClientKeys),
    io:format("LocalKeys: ~p~n", [LocalKeys]),
    io:format("ClientKeys: ~p~n", [ClientKeys]),
    AllLocalAvailable andalso AllClientAvailable.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metrics       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
    Value = rrd:get_value_by_offset(RRD, 0),
    io:format("~p: ~p~n", [Key, Value]).    


%%%%%%%%%%%%%%%%%%%%%%%%%%%%       Utils       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

trigger(lb_trigger) ->
    msg_delay:send_trigger(10, {lb_trigger});

trigger(collect_stats) ->
    msg_delay:send_trigger(10, {collect_stats}).

%% @doc true if X happened before Y otherwise false
-spec happened_before(erlang:timestamp(), erlang:timestamp()) -> boolean().
happened_before(X, Y) ->
    timer:now_diff(X, Y) > 0.

%% TODO
-spec check_config() -> boolean().
check_config() ->
    true.