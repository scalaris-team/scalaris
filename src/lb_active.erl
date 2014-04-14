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
-define(TRACE(X,Y), io:format("lb_active: " ++ X, Y)).

%% startup
-export([start_link/1, init/1, check_config/0]).
%% gen_component
-export([on_inactive/2, on/2]).
%% for calls from the dht node
-export([handle_dht_msg/2]).
%% for db monitoring
-export([init_db_monitors/0, update_db_monitor/2]).
%% Metrics
-export([get_load_metric/0, get_load_metric/1, default_value/0]).
% Load Balancing
-export([balance_nodes/3, balance_nodes/4, balance_noop/1]).

-type lb_message() :: comm:message(). %% TODO more specific?

-type module_state() :: tuple(). %% TODO more specific

-type state() :: module_state(). %% state of lb module

-record(lb_op, {id = ?required(id, lb_op)                           :: uid:global_uid(),
                type = ?required(type, lb_op)                       :: slide_pred | slide_succ | jump,
                %% receives load
                light_node = ?required(light, lb_op)                :: node:node_type(),
                light_node_succ = ?required(light_node_succ, lb_op) :: node:node_type(),
                %% sheds load
                heavy_node = ?required(heavy, lb_op)                :: node:node_type(),
                target = ?required(target, lb_op)                   :: ?RT:key(),
                %% time of the oldest data used for the decision for this lb_op
                data_time = ?required(data_time, lb_op)             :: erlang:timestamp(),
                time = os:timestamp()                               :: erlang:timestamp()
               }).

-type lb_op() :: #lb_op{}.

-type metric() :: items | cpu | mem | db_reads | db_writes | db_requests | transactions | tx_latency | net_throughput.

%% available metrics
-define(METRICS, [items, cpu, mem, db_reads, db_writes, db_requests, transactions, tx_latency, net_throughput]).

%% list of active load balancing modules available
-define(MODULES, [lb_active_karger, lb_active_directories]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Initialization %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start this process as a gen component and register it in the dht node group
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun on_inactive/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, lb_active}]).


%% @doc Initialization of monitoring values
-spec init([]) -> state().
init([]) ->
    set_time_last_balance(),
    case collect_stats() of
        true ->
            application:start(sasl),   %% required by os_mon.
            application:start(os_mon), %% for monitoring cpu and memory usage.
            trigger(collect_stats);
        _ -> ok
    end,
    init_stats(),
    trigger(lb_trigger),
    %% keep the node id in state, currently needed to normalize histogram
    %%     rm_loop:subscribe(
    %%        self(), ?MODULE, fun rm_loop:subscribe_dneighbor_change_filter/3,
    %%        fun(_,_,_,_,_) -> comm:send_local(self(), {get_node_details, This, node_id}) end, inf),
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
            %% change handler and initialize module
            gen_component:change_handler(InitState, fun on/2);
        _    ->
            State
    end;

on_inactive({collect_stats} = Msg, State) ->
    on(Msg, State);

on_inactive(Msg, State) ->
    %% at the moment, we simply ignore lb messages.
    ?TRACE("Unknown message received ~p~n. Ignoring.", [Msg]),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Main message handler %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc On handler after initialization
-spec on(comm:message(), state()) -> state().
on({collect_stats}, State) ->
    trigger(collect_stats),
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
    monitor:client_monitor_set_value(lb_active, cpu10sec, fun(Old) -> rrd:add_now(CPU, Old) end),
    %monitor:client_monitor_set_value(lb_active, cpu5min, fun(Old) -> rrd:add_now(CPU, Old) end),
    monitor:client_monitor_set_value(lb_active, mem10sec, fun(Old) -> rrd:add_now(MEM, Old) end),
    %monitor:client_monitor_set_value(lb_active, mem5min, fun(Old) -> rrd:add_now(MEM, Old) end),
    State;

on({lb_trigger} = Msg, State) ->
    %% module can decide whether to trigger
    %% trigger(lb_trigger),
    call_module(handle_msg, [Msg, State]);

%% Gossip response before balancing takes place
on({gossip_reply, LightNode, HeavyNode, LightNodeSucc, Options,
    {gossip_get_values_best_response, LoadInfo}}, State) ->
    %% check the load balancing configuration by using
    %% the standard deviation from the gossip process.

    Size = gossip_load:load_info_get(size, LoadInfo),
    ItemsStdDev = gossip_load:load_info_get(stddev, LoadInfo),
    ItemsAvg = gossip_load:load_info_get(avgLoad, LoadInfo),
    Metrics =
        case gossip_load:load_info_get(other, LoadInfo) of
            [] -> [{avg, ItemsAvg}, {stddev, ItemsStdDev}];
            _ -> [{stddev, gossip_load:load_info_other_get(stddev, gossip_load_lb_metric, LoadInfo)},
                  {avg, gossip_load:load_info_other_get(avgLoad, gossip_load_lb_metric, LoadInfo)}]
        end,
    OptionsNew = [{dht_size, Size} | Metrics ++ Options],

    HeavyPid = node:pidX(lb_info:get_node(HeavyNode)),
    comm:send(HeavyPid, {lb_active, balance, HeavyNode, LightNode, LightNodeSucc, OptionsNew}),

    State;

%% lb_op received from dht_node and to be executed
on({balance_phase1, Op}, State) ->
    case op_pending() of
        true  -> ?TRACE("Pending op. Won't jump or slide. Discarding op ~p~n", [Op]);
        false ->
            case old_data(Op) of
                true -> ?TRACE("Old data. Discarding op ~p~n", [Op]);
                false ->
                    set_pending_op(Op),
                    case Op#lb_op.type of
                        jump ->
                            %% tell the succ of the light node in case of a jump
                            LightNodeSuccPid = node:pidX(Op#lb_op.light_node_succ),
                            comm:send(LightNodeSuccPid, {balance_phase2a, Op, comm:this()}, [{group_member, lb_active}]);
                        _ -> 
                            %% set pending op at other node
                            LightNodePid = node:pidX(Op#lb_op.light_node),
                            comm:send(LightNodePid, {balance_phase2b, Op, comm:this()}, [{group_member, lb_active}])
                    end
            end
    end,
    State;

%% Received by the succ of the light node which takes the light nodes' load
%% in case of a jump.
on({balance_phase2a, Op, ReplyPid}, State) ->
    case op_pending() of
        true -> ?TRACE("Pending op in phase2a. Discarding op ~p and replying~n", [Op]),
                comm:send(ReplyPid, {balance_failed, Op});
        false ->
            case old_data(Op) of
                true -> ?TRACE("Old data. Discarding op ~p~n", [Op]),
                        comm:send(ReplyPid, {balance_failed, Op});
                false ->
                    set_pending_op(Op),
                    LightNodePid = node:pidX(Op#lb_op.light_node),
                    comm:send(LightNodePid, {balance_phase2b, Op, ReplyPid}, [{group_member, lb_active}])
            end
    end,
    State;

%% The light node which receives load from the heavy node and initiates the lb op.
on({balance_phase2b, Op, ReplyPid}, State) ->
    case op_pending() of
        true -> ?TRACE("Pending op in phase2b. Discarding op ~p and replying~n", [Op]),
                comm:send(ReplyPid, {balance_failed, Op});
        false ->
            case old_data(Op) of
                true -> ?TRACE("Old data. Discarding op ~p~n", [Op]),
                        comm:send(ReplyPid, {balance_failed, Op#lb_op.id});
                false ->
                    set_pending_op(Op),
                    OpId = Op#lb_op.id,
                    Pid = node:pidX(Op#lb_op.light_node),
                    TargetKey = Op#lb_op.target,
                                set_pending_op(Op),
                    ?TRACE("Type: ~p Heavy: ~p Light: ~p Target: ~p~n", [Op#lb_op.type, Op#lb_op.heavy_node, Op#lb_op.light_node, TargetKey]),
                    case Op#lb_op.type of
                        jump ->
                            %% TODO replace with send_local
                            comm:send(Pid, {move, start_jump, TargetKey, {jump, OpId}, comm:this()});
                        slide_pred ->
                            comm:send(Pid, {move, start_slide, pred, TargetKey, {slide_pred, OpId}, comm:this()});
                        slide_succ ->
                            comm:send(Pid, {move, start_slide, succ, TargetKey, {slide_succ, OpId}, comm:this()})
                    end
            end
    end,
    State;

on({balance_failed, OpId}, State) ->
    case get_pending_op() of
        undefined -> ?TRACE("Received balance_failed but OpId ~p was not pending~n", [OpId]);
        Op when Op#lb_op.id =:= OpId ->
            ?TRACE("Clearing pending op because of balance_failed ~p~n", [OpId]),
            set_pending_op(undefined);
        Op ->
            ?TRACE("Received balance_failed answer but OpId ~p didn't match pending id ~p~n", [OpId, Op#lb_op.id])
    end,
    State;

%% success does not imply the slide or jump was successfull. however,
%% slide or jump failures should very rarly occur because of the locking
%% and stale data detection.
on({balance_success, OpId}, State) ->
    case get_pending_op() of
        undefined -> ?TRACE("Received answer but OpId ~p was not pending~n", [OpId]);
        Op when Op#lb_op.id =:= OpId ->
            ?TRACE("Clearing pending op ~p~n", [OpId]),
            comm:send_local(self(), {reset_monitors}),
            set_pending_op(undefined),
            set_time_last_balance();
        Op ->
            ?TRACE("Received answer but OpId ~p didn't match pending id ~p~n", [OpId, Op#lb_op.id])
    end,
    State;

%% received reply at the sliding/jumping node
on({move, result, {_JumpOrSlide, OpId}, _Status}, State) ->
    ?TRACE("~p status with id ~p: ~p~n", [_JumpOrSlide, OpId, _Status]),
    case get_pending_op() of
        undefined -> ?TRACE("Received answer but OpId ~p was not pending~n", [OpId]);
        Op when Op#lb_op.id =:= OpId ->
            ?TRACE("Clearing pending op and replying to other node ~p~n", [OpId]),
            HeavyNodePid = node:pidX(Op#lb_op.heavy_node),
            comm:send(HeavyNodePid, {balance_success, OpId}, [{group_member, lb_active}]),
            comm:send_local(self(), {reset_monitors}),
            set_pending_op(undefined),
            set_time_last_balance(),
            case Op#lb_op.type of
                jump ->
                    %% also reply to light node succ in case of jump
                    LightNodeSucc = Op#lb_op.light_node_succ,
                    LightNodeSuccPid = node:pidX(LightNodeSucc),
                    comm:send(LightNodeSuccPid, {balance_success, OpId}, [{group_member, lb_active}]);
                _ ->
                    ok
            end;
        Op ->
            ?TRACE("Received answer but OpId ~p didn't match pending id ~p~n", [OpId, Op#lb_op.id])
    end,
    State;

on({reset_monitors}, State) ->
    init_stats(),
    init_db_monitors(),
    gen_component:change_handler(State, fun on_inactive/2);

on({web_debug_info, Requestor}, State) ->
    KVList =
        [{"active module", webhelpers:safe_html_string("~p", [get_lb_module()])},
         {"metric"       , webhelpers:safe_html_string("~p", [config:read(lb_active_metric)])},
         {"metric value:", webhelpers:safe_html_string("~p", [get_load_metric()])},
         {"last balance:", webhelpers:safe_html_string("~p", [get_time_last_balance()])},
         {"pending op:",   webhelpers:safe_html_string("~p", [get_pending_op()])}
        ],
    Return = KVList ++ call_module(get_web_debug_kv, [State]),
    comm:send_local(Requestor, {web_debug_info_reply, Return}),
    State;

on(Msg, State) ->
    call_module(handle_msg, [Msg, State]).

%%%%%%%%%%%%%%%%%%%%%%% Load Balancing %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

balance_nodes(HeavyNode, LightNode, Options) ->
    balance_nodes(HeavyNode, LightNode, nil, Options).

%% TODO -spec balance_nodes(lb_info:lb_info(), lb_info:lb_info()) -> ok.
balance_nodes(HeavyNode, LightNode, LightNodeSucc, Options) ->
    case config:read(lb_active_use_gossip) of
        true -> %% Retrieve global info from gossip before balancing
            GossipPid = pid_groups:get_my(gossip),
            LBActivePid = pid_groups:get_my(lb_active),
            Envelope = {gossip_reply, LightNode, HeavyNode, LightNodeSucc, Options, '_'},
            ReplyPid = comm:reply_as(LBActivePid, 6, Envelope),
            comm:send_local(GossipPid, {get_values_best, {gossip_load, default}, ReplyPid});
        _ ->
            HeavyPid = node:pidX(lb_info:get_node(HeavyNode)),
            comm:send(HeavyPid, {lb_active, balance, HeavyNode, LightNode, LightNodeSucc, Options})
    end.

%% no op but we sent back simulation results
balance_noop(Options) ->
    case proplists:get_value(simulate, Options) of
        undefined -> ok;
        ReqId ->
            ReplyTo = proplists:get_value(reply_to, Options),
            Id = proplists:get_value(id, Options),
            comm:send(ReplyTo, {simulation_result, Id, ReqId, 0})
    end.

-spec get_pending_op() -> undefined | lb_op().
get_pending_op() ->
    erlang:get(pending_op).

-spec set_pending_op(lb_op()) -> ok.
set_pending_op(Op) ->
    erlang:put(pending_op, Op),
    ok.

-spec op_pending() -> boolean().
op_pending() ->
    case erlang:get(pending_op) of
        undefined -> false;
        OtherOp ->
            case old_op(OtherOp) of
                false -> true;
                true -> %% remove old op
                    ?TRACE("Removing old op ~p~n", [OtherOp]),
                    erlang:erase(pending_op),
                    false
            end
    end.

-spec old_op(lb_op()) -> boolean().
old_op(Op) ->
    %% TODO Parameterize
    %% config:read(lb_active_wait_for_pending_ops)
    Threshold = 10000 * 1000, %% microseconds
    ?TRACE("Diff:~p~n", [timer:now_diff(os:timestamp(), Op#lb_op.time)]),
    timer:now_diff(os:timestamp(), Op#lb_op.time) > Threshold.

-spec old_data(lb_op()) -> boolean().
old_data(Op) ->
    LastBalanceTime = erlang:get(time_last_balance),
    DataTime = Op#lb_op.data_time,
    timer:now_diff(LastBalanceTime, DataTime) > 0.

get_time_last_balance() ->
    erlang:get(time_last_balance).

set_time_last_balance() ->
    erlang:put(time_last_balance, os:timestamp()),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%% Calls from dht_node %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Process load balancing messages sent to the dht node
-spec handle_dht_msg(lb_message(), dht_node_state:state()) -> dht_node_state:state().

%% We received a jump or slide operation from a LightNode.
%% In either case, we'll compute the target id and send out
%% the jump or slide message to the LightNode.
handle_dht_msg({lb_active, balance, HeavyNode, LightNode, LightNodeSucc, Options}, DhtState) ->
    %% TODO check for active state here before doing anything?
    case lb_info:get_node(HeavyNode) =/= dht_node_state:get(DhtState, node) of
        true -> ?TRACE("I was mistaken for the HeavyNode. Doing nothing~n", []), ok;
        false ->
            %% TODO What are the benefits/disadvantages of getting the load info again?
            MyNode = lb_info:new(dht_node_state:details(DhtState)),
            JumpOrSlide = %case lb_info:neighbors(MyNode, LightNode) of
                          case LightNodeSucc =:= nil of
                              true  -> slide;
                              false -> jump
                          end,
            TargetLoad = lb_info:get_target_load(JumpOrSlide, MyNode, LightNode),
            {From, To, Direction} =
                case JumpOrSlide =:= jump orelse lb_info:is_succ(MyNode, LightNode) of
                    true  -> %% Jump or heavy node is succ of light node
                        {dht_node_state:get(DhtState, pred_id), dht_node_state:get(DhtState, node_id), forward}; %% TODO node_id-1 ?
                    false -> %% Light node is succ of heavy node
                        {dht_node_state:get(DhtState, node_id), dht_node_state:get(DhtState, pred_id), backward}
                end,
            {SplitKey, TakenLoad} = dht_node_state:get_split_key(DhtState, From, To, TargetLoad, Direction),
            ?TRACE("SplitKey: ~p TargetLoad: ~p TakenLoad: ~p~n", [SplitKey, TargetLoad, TakenLoad]),
            %% Report TakenLoad in case of simulation or exec load balancing decision
            case proplists:get_value(simulate, Options) of
                 undefined ->
                     ItPaysOff = %% this is a verbose name
                         case proplists:is_defined(dht_size, Options) of
                             %% gossip information available
                             true ->
                                 S = config:read(lb_active_gossip_stddev_threshold),
                                 DhtSize = proplists:get_value(dht_size, Options),
                                 %Avg = proplists:get_value(avg, Options), %% TODO AVG?
                                 StdDev = proplists:get_value(stddev, Options),
                                 Variance = StdDev * StdDev,
                                 VarianceChange =
                                     case JumpOrSlide of
                                         slide -> lb_info:get_load_change_slide(TakenLoad, DhtSize, HeavyNode, LightNode);
                                         jump -> lb_info:get_load_change_jump(TakenLoad, DhtSize, HeavyNode, LightNode, LightNodeSucc)
                                     end,
                                 VarianceNew = Variance + VarianceChange,
                                 StdDevNew = ?IIF(VarianceNew > 0, math:sqrt(VarianceNew), StdDev),
                                 ?TRACE("New StdDev: ~p Old StdDev: ~p~n", [StdDevNew, StdDev]),
                                 StdDevNew < StdDev * (1 - S / DhtSize);
                             %% gossip not available, skipping this test
                             false -> true
                         end,
                     case ItPaysOff of
                        false -> ?TRACE("No balancing: stddev was not reduced enough.~n", []);
                        true ->
                            ?TRACE("Sending out lb op.~n", []),
                            OpId = uid:get_global_uid(),
                            Type =  if  JumpOrSlide =:= jump -> jump;
                                        Direction =:= forward -> slide_succ;
                                        Direction =:= backward -> slide_pred
                                    end,
                            OldestDataTime = if Type =:= jump ->
                                                    lb_info:get_oldest_data_time([LightNode, HeavyNode, LightNodeSucc]);
                                                true ->
                                                    lb_info:get_oldest_data_time([LightNode, HeavyNode])
                                             end,
                            Op = #lb_op{id = OpId, type = Type,
                                        light_node = lb_info:get_node(LightNode),
                                        light_node_succ = lb_info:get_succ(LightNode),
                                        heavy_node = lb_info:get_node(HeavyNode),
                                        target = SplitKey,
                                        data_time = OldestDataTime},
                            LBModule = pid_groups:get_my(?MODULE),
                            comm:send_local(LBModule, {balance_phase1, Op})
                      end;
                ReqId -> %% compute result of simulation and reply
                    LoadChange =
                        case JumpOrSlide of
                            slide -> lb_info:get_load_change_slide(TakenLoad, HeavyNode, LightNode);
                            jump  -> lb_info:get_load_change_jump(TakenLoad, HeavyNode, LightNode, LightNodeSucc)
                        end,
                    ReplyTo = proplists:get_value(reply_to, Options),
                    Id = proplists:get_value(id, Options),
                    comm:send(ReplyTo, {simulation_result, Id, ReqId, LoadChange})
            end
    end,
    DhtState;

handle_dht_msg(Msg, DhtState) ->
    call_module(handle_dht_msg, [Msg, DhtState]).

%%%%%%%%%%%%%%%%%%%%%%%% Monitoring values %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Called by db process to initialize monitors
-spec init_db_monitors() -> ok.
init_db_monitors() ->
    case monitor_db() of
        true ->
            MonitorRes = config:read(lb_active_monitor_resolution),
            Reads  = rrd:add_now(0, rrd:create(MonitorRes * 1000, 3, {timing_with_hist, count})),
            Writes = rrd:add_now(0, rrd:create(MonitorRes * 1000, 3, {timing_with_hist, count})),
            monitor:proc_set_value(lb_active, db_reads, Reads),
            monitor:proc_set_value(lb_active, db_writes, Writes),
            monitor:monitor_set_value(lb_active, db_reads, Reads),
            monitor:monitor_set_value(lb_active, db_writes, Writes);
        _ -> ok
    end.

%% @doc Updates the local rrd for reads or writes and checks for reporting
-spec update_db_monitor(Type::db_reads | db_writes, Value::?RT:key()) -> ok;
                       (Type::items, Value::non_neg_integer())   -> ok.
update_db_monitor(Type, Value) ->
    case monitor_db() of
        true ->
%%             if
%%                 Type =:= reads orelse Type =:= writes ->
%%                     %% TODO Normalize key because histogram might contain circular elements, e.g. [MAXVAL, 0, 1]
%%
%%             end,
            monitor:proc_set_value(lb_active, Type, fun(Old) -> rrd:add_now(Value, Old) end);
        _ -> ok
    end.

-spec init_stats() -> ok.
init_stats() ->
    case collect_stats() of
        true ->
            Interval = config:read(lb_active_monitor_resolution),
            %LongTerm  = rrd:create(60 * 5 * 1000000, 5, {timing, '%'}),
            ShortTerm = rrd:create(Interval * 1000, 1, gauge),
            %monitor:client_monitor_set_value(lb_active, cpu5min, LongTerm),
            monitor:client_monitor_set_value(lb_active, cpu10sec, ShortTerm),
            %monitor:client_monitor_set_value(lb_active, mem5min, LongTerm),
            monitor:client_monitor_set_value(lb_active, mem10sec, ShortTerm);
        _ ->
            ok
    end.

-spec monitor_vals_appeared() -> boolean().
monitor_vals_appeared() ->
    case erlang:get(metric_available) of
        true -> true;
        _ ->
            case get_load_metric() of
                unknown ->
                    false;
                _Metric ->
                    erlang:put(metric_available, true),
                    true
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metrics       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_load_metric() -> unknown | items | number().
get_load_metric() ->
    Metric = config:read(lb_active_metric),
    case get_load_metric(Metric) of
        unknown -> io:format("Unknown metric.~n"), 0;
        Val -> Val
    end.

-define(DEFAULT(Val, Default), case Val of
                         unknown -> Default;
                         Val -> Val
                     end).

-spec get_load_metric(metric()) -> unknown | items | number().
get_load_metric(Metric) ->
            case Metric of
                cpu          -> get_vm_metric({lb_active, cpu10sec}, count) / 100;
                mem          -> get_vm_metric({lb_active, mem10sec}, count) / 100;
                db_reads     -> get_dht_metric({lb_active, db_reads}, count);
                db_writes    -> get_dht_metric({lb_active, db_writes}, count);
                db_requests  -> get_load_metric(db_reads) + get_load_metric(db_writes);
                tx_latency   -> get_dht_metric({api_tx, req_list}, avg);
                transactions -> get_dht_metric({api_tx, req_list}, count);
                items        -> items;
                _            -> throw(metric_not_available)
    end.

-spec get_vm_metric(metric(), avg | count) -> unknown | number().
get_vm_metric(Metric, Type) ->
    ClientMonitorPid = pid_groups:pid_of("clients_group", monitor),
    get_metric(ClientMonitorPid, Metric, Type).

get_dht_metric(Metric, Type) ->
    MonitorPid = pid_groups:get_my(monitor),
    get_metric(MonitorPid, Metric, Type).

-spec get_metric(pid(), monitor:table_index(), avg | count) -> unknown | number().
get_metric(MonitorPid, Metric, Type) ->
    [{_Process, _Key, RRD}] = monitor:get_rrds(MonitorPid, [Metric]),
    case RRD of
        undefined ->
            unknown;
        RRD ->
            SlotLength = rrd:get_slot_length(RRD),
            io:format("Slot Length: ~p~n", [SlotLength]),
            {MegaSecs, Secs, MicroSecs} = os:timestamp(),
            %% get stable value off an old slot
            Value = rrd:get_value(RRD, {MegaSecs, Secs, MicroSecs-SlotLength}),
            io:format("VAlue: ~p~n", [Value]),
            get_value_type(Value, Type)
    end.

-spec get_value_type(rrd:data_type(), avg | count) -> unknown | number().
get_value_type(undefined, _Type) ->
    unknown;
get_value_type(Value, _Type) when is_number(Value) ->
    Value;
get_value_type(Value, count) when is_tuple(Value) ->
    element(3, Value);
get_value_type(Value, avg) when is_tuple(Value) ->
    try
        element(1, Value) / element(3, Value)
    catch
        error:badarith -> unknown
    end.

%% TODO
default_value() ->
    case config:read(lb_active_metric) of
        db_reads -> 0
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%% Util %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec call_module(atom(), list()) -> state().
call_module(Fun, Args) ->
    apply(get_lb_module(), Fun, Args). 

-spec get_lb_module() -> atom() | failed.
get_lb_module() ->
    config:read(lb_active_module).

-spec collect_stats() -> boolean().
collect_stats() ->
    Metrics = [cpu, mem, db_reads, db_writes, db_requests, transactions],
    lists:member(config:read(lb_active_metric), Metrics).

-spec monitor_db() -> boolean().
monitor_db() ->
    case erlang:get(monitor_db) of
        undefined ->
            Metrics = [db_reads, db_writes, db_requests],
            Status = lists:member(config:read(lb_active_metric), Metrics),
            erlang:put(monitor_db, Status),
            Status;
        Val -> Val
    end.

-spec trigger(atom()) -> ok.
trigger(Trigger) ->
    Interval =
        case Trigger of
            lb_trigger -> config:read(lb_active_interval);
            collect_stats -> config:read(lb_active_monitor_interval)
        end,
    msg_delay:send_trigger(Interval div 1000, {Trigger}).

%% @doc config check registered in config.erl
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(lb_active_module, ?MODULES) andalso

    config:cfg_is_integer(lb_active_interval) andalso
    config:cfg_is_greater_than(lb_active_interval, 0) andalso

    config:cfg_is_in(lb_active_metric, ?METRICS) andalso

    config:cfg_is_bool(lb_active_use_gossip) andalso
    config:cfg_is_greater_than(lb_active_gossip_stddev_threshold, 0) andalso

    config:cfg_is_integer(lb_active_histogram_size) andalso
    config:cfg_is_greater_than(lb_active_histogram_size, 0) andalso

    config:cfg_is_integer(lb_active_monitor_resolution) andalso
    config:cfg_is_greater_than(lb_active_monitor_resolution, 0) andalso

    config:cfg_is_integer(lb_active_monitor_interval) andalso
    config:cfg_is_greater_than(lb_active_monitor_interval, 0) andalso

    config:cfg_is_less_than(lb_active_monitor_interval, config:read(lb_active_monitor_resolution)) andalso

    apply(get_lb_module(), check_config, []).
