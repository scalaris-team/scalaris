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
%% @doc Active load balancing core module
%% @version $Id$
-module(lb_active).
-author('michels@zib.de').
-vsn('$Id$').

-behavior(gen_component).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), io:format("lb_active: " ++ X, Y)).

%% startup
-export([start_link/1, init/1, check_config/0, is_enabled/0]).
%% gen_component
-export([on_inactive/2, on/2]).
%% for calls from the dht node
-export([handle_dht_msg/2]).
% Load Balancing
-export([balance_nodes/3, balance_nodes/4, balance_noop/1]).
-export([get_last_db_monitor_init/1]).

-ifdef(with_export_type_support).
-export_type([dht_message/0, state/0]).
-endif.

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

-type options() :: [tuple()].

-type message_inactive() :: {collect_stats} |
                            {lb_trigger} |
                            {reset_monitors}.

-type message() :: {collect_stats} |
                   {lb_trigger} |
                   {reset_monitors} |
                   {gossip_reply, LightNode::lb_info:lb_info(), HeavyNode::lb_info:lb_info(), LightNodeSucc::lb_info:lb_info(),
                    Options::options(), {gossip_get_values_best_response, LoadInfo::gossip_load:load_info()}} |
                   {balance_phase1, Op::lb_op()} |
                   {balance_phase2a, Op::lb_op(), Pid::comm:mypid()} |
                   {balance_phase2b, Op::lb_op(), Pid::comm:mypid()} |
                   {balance_failed, OpId::uid:global_uid()} |
                   {balance_success, OpId::uid:global_uid()} |
                   {move, result, Tag::{jump | slide_pred | slide_succ, OpId::uid:global_uid()}, Result::ok | dht_node_move:abort_reason()} |
                   {web_debug_info, Requestor::pid()}.

-type dht_message() :: {lb_active, reset_db_monitors} |
                       {lb_active, balance,
                        HeavyNode::lb_info:lb_info(), LightNode::lb_info:lb_info(),
                        LightNodeSucc::lb_info:lb_info(), Options::options()}.

-type module_state() :: tuple().

-record(my_state, {last_balance = os:timestamp() :: erlang:timestamp(),
                   last_db_monitor_reset = os:timestamp() :: erlang:timestamp(),
                   pending_op = nil :: lb_op() | nil}).

-type my_state() :: #my_state{}.

-opaque state() :: {my_state(), module_state()}.

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
    lb_stats:init(),
    trigger(),
    %% keep the node id in state, currently needed to normalize histogram
    rm_loop:subscribe(
       self(), ?MODULE, fun rm_loop:subscribe_dneighbor_change_slide_filter/3,
       fun(Pid, _Tag, _Old, _New, _Reason) ->
           %% send reset message to dht node and lb_active process
           comm:send_local(self(), {lb_active, reset_db_monitors}),
           comm:send_local(Pid, {reset_monitors})
       end, inf),
    DhtNode = pid_groups:get_my(dht_node),
    comm:send_local(DhtNode, {lb_active, reset_db_monitors}),
    comm:send_local(self(), {reset_monitors}),

    {_MyState = #my_state{}, _ModuleState = {}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Startup message handler %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Handles all messages until enough monitor data has been collected.
-spec on_inactive(message_inactive(), state()) -> state().
on_inactive({lb_trigger}, {MyState, _ModuleState} = State) ->
    trigger(),
    case lb_stats:monitor_vals_appeared(MyState) of
        true ->
            InitState = call_module(init, []),
            ?TRACE("All monitor data appeared. Activating active load balancing~n", []),
            %% change handler and initialize module
            gen_component:change_handler({MyState, InitState}, fun on/2);
        _    ->
            State
    end;

on_inactive({collect_stats} = Msg, State) ->
    on(Msg, State);

on_inactive({reset_monitors} = Msg, State) ->
    on(Msg, State);

on_inactive({web_debug_info, _Pid} = Msg, State) ->
    on(Msg, State);

on_inactive(_Msg, State) ->
    %% TODO at the moment, we simply ignore lb messages.
    ?TRACE("Unknown message ~p~n. Ignoring.~n", [_Msg]),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Main message handler %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc On handler after initialization
-spec on(message(), state()) -> state().
on({collect_stats}, State) ->
    lb_stats:trigger_routine(),
    State;

on({lb_trigger} = Msg, {MyState, ModuleState}) ->
    %% module can decide whether to trigger
    %% trigger(lb_trigger),
    ModuleState2 = call_module(handle_msg, [Msg, ModuleState]),
    {MyState, ModuleState2};

%% Gossip response before balancing takes place
on({gossip_reply, LightNode, HeavyNode, LightNodeSucc, Options,
    {gossip_get_values_best_response, LoadInfo}}, State) ->
    %% check the load balancing configuration by using
    %% the standard deviation from the gossip process.
    Size = gossip_load:load_info_get(size, LoadInfo),
    GossipModule = lb_active_gossip_request_metric,
    Metrics =   [{avgItems, gossip_load:load_info_get(avgLoad, LoadInfo)},
                 {stddevItems, gossip_load:load_info_get(stddev, LoadInfo)}]
                ++
                [{avgRequests, gossip_load:load_info_other_get(avgLoad, GossipModule, LoadInfo)},
                 {stddevRequests, gossip_load:load_info_other_get(stddev, GossipModule, LoadInfo)}],

    OptionsNew = [{dht_size, Size} | Metrics ++ Options],

    HeavyPid = node:pidX(lb_info:get_node(HeavyNode)),
    comm:send(HeavyPid, {lb_active, balance, HeavyNode, LightNode, LightNodeSucc, OptionsNew}),

    State;

%% lb_op received from dht_node and to be executed
on({balance_phase1, Op}, {MyState, ModuleState} = State) ->
    OpPending = op_pending(MyState),
    OldData = old_data(Op, MyState),
    if
        OpPending ->
            ?TRACE("Phase1: Pending op. Won't jump or slide. Discarding op ~p~n", [Op]),
            State;
        OldData ->
            ?TRACE("Phase1: Old data in lb_op. Won't jump or slide. Discarding op ~p~n", [Op]),
            State;
        true ->
            MyState2 = set_pending_op(Op, MyState),
            case Op#lb_op.type of
                jump ->
                    %% tell the succ of the light node in case of a jump
                    LightNodeSuccPid = node:pidX(Op#lb_op.light_node_succ),
                    comm:send(LightNodeSuccPid, {balance_phase2a, Op, comm:this()}, [{group_member, lb_active}]);
                _ ->
                    %% set pending op at other node
                    LightNodePid = node:pidX(Op#lb_op.light_node),
                    comm:send(LightNodePid, {balance_phase2b, Op, comm:this()}, [{group_member, lb_active}])
            end,
            {MyState2, ModuleState}
    end;

%% Received by the succ of the light node which takes the light nodes' load
%% in case of a jump.
on({balance_phase2a, Op, ReplyPid}, {MyState, ModuleState} = State) ->
    OpPending = op_pending(MyState),
    OldData = old_data(Op, MyState),
    if
        OpPending ->
            ?TRACE("Phase2a: Pending op. Discarding op ~p and replying~n", [Op]),
            comm:send(ReplyPid, {balance_failed, Op}),
            State;
        OldData ->
            ?TRACE("Phase2a: Old data in lb_op. Won't jump or slide. Discarding op ~p~n", [Op]),
            State;
        true ->
            MyState2 = set_pending_op(Op, MyState),
            LightNodePid = node:pidX(Op#lb_op.light_node),
            comm:send(LightNodePid, {balance_phase2b, Op, ReplyPid}, [{group_member, lb_active}]),
            {MyState2, ModuleState}
    end;

%% The light node which receives load from the heavy node and initiates the lb op.
on({balance_phase2b, Op, ReplyPid}, {MyState, ModuleState} = State) ->
    OpPending = op_pending(MyState),
    OldData = old_data(Op, MyState),
    if
        OpPending ->
            ?TRACE("Phase2b: Pending op. Discarding op ~p and replying~n", [Op]),
            comm:send(ReplyPid, {balance_failed, Op}),
            State;
        OldData ->
            ?TRACE("Phase2b: Old data in lb_op. Won't jump or slide. Discarding op ~p~n", [Op]),
            State;
        true ->
            OpId = Op#lb_op.id,
            _Pid = node:pidX(Op#lb_op.light_node),
            TargetKey = Op#lb_op.target,
            MyState2 = set_pending_op(Op, MyState),
            ?TRACE("Type: ~p Heavy: ~p Light: ~p Target: ~p~n", [Op#lb_op.type, Op#lb_op.heavy_node, Op#lb_op.light_node, TargetKey]),
            MyDHT = pid_groups:get_my(dht_node),
            ?DBG_ASSERT(_Pid =:= comm:make_global(MyDHT)),
            case Op#lb_op.type of
                jump ->
                    comm:send_local(MyDHT, {move, start_jump, TargetKey, {jump, OpId}, comm:this()});
                slide_pred ->
                    comm:send_local(MyDHT, {move, start_slide, pred, TargetKey, {slide_pred, OpId}, comm:this()});
                slide_succ ->
                    comm:send_local(MyDHT, {move, start_slide, succ, TargetKey, {slide_succ, OpId}, comm:this()})
            end,
            {MyState2, ModuleState}
    end;

on({balance_failed, OpId}, {MyState, ModuleState} = State) ->
    case get_pending_op(MyState) of
        nil ->
            ?TRACE("Received balance_failed but OpId ~p was not pending~n", [OpId]),
            State;
        Op when Op#lb_op.id =:= OpId ->
            ?TRACE("Clearing pending op because of balance_failed ~p~n", [OpId]),
            MyState2 = set_pending_op(nil, MyState),
            {MyState2, ModuleState};
        _Op ->
            ?TRACE("Received balance_failed answer but OpId ~p didn't match pending id ~p~n", [OpId, _Op#lb_op.id]),
            State
    end;

%% success does not imply the slide or jump was successfull. however,
%% slide or jump failures should very rarly occur because of the locking
%% and stale data detection.
on({balance_success, OpId}, {MyState, ModuleState} = State) ->
    case get_pending_op(MyState) of
        nil ->
            ?TRACE("Received answer but OpId ~p was not pending~n", [OpId]),
            State;
        Op when Op#lb_op.id =:= OpId ->
            ?TRACE("Clearing pending op ~p~n", [OpId]),
            comm:send_local(self(), {reset_monitors}),
            MyState2 = set_pending_op(nil, MyState),
            MyState3 = set_time_last_balance(MyState2),
            {MyState3, ModuleState};
        _Op ->
            ?TRACE("Received answer but OpId ~p didn't match pending id ~p~n", [OpId, _Op#lb_op.id]),
            State
    end;

%% received reply at the sliding/jumping node
on({move, result, {_JumpOrSlide, OpId}, _Status}, {MyState, ModuleState} = State) ->
    ?TRACE("~p status with id ~p: ~p~n", [_JumpOrSlide, OpId, _Status]),
    case get_pending_op(MyState) of
        nil ->
            ?TRACE("Received answer but OpId ~p was not pending~n", [OpId]),
            State;
        Op when Op#lb_op.id =:= OpId ->
            ?TRACE("Clearing pending op and replying to other node ~p~n", [OpId]),
            HeavyNodePid = node:pidX(Op#lb_op.heavy_node),
            comm:send(HeavyNodePid, {balance_success, OpId}, [{group_member, lb_active}]),
            comm:send_local(self(), {reset_monitors}),
            MyState2 = set_pending_op(nil, MyState),
            MyState3 = set_time_last_balance(MyState2),
            case Op#lb_op.type of
                jump ->
                    %% also reply to light node succ in case of jump
                    LightNodeSucc = Op#lb_op.light_node_succ,
                    LightNodeSuccPid = node:pidX(LightNodeSucc),
                    comm:send(LightNodeSuccPid, {balance_success, OpId}, [{group_member, lb_active}]);
                _ ->
                    ok
            end,
            {MyState3, ModuleState};
        _Op ->
            ?TRACE("Received answer but OpId ~p didn't match pending id ~p~n", [OpId, _Op#lb_op.id]),
            State
    end;

on({reset_monitors}, {MyState, ModuleState}) ->
    lb_stats:init(),
    ?TRACE("Reseting monitors ~n", []),
    MyState2 = set_last_db_monitor_init(MyState),
    gen_component:change_handler({MyState2, ModuleState}, fun on_inactive/2);

on({web_debug_info, Requestor}, {MyState, ModuleState} = State) ->
    KVList =
        [{"active module", webhelpers:safe_html_string("~p", [get_lb_module()])},
         {"load metric", webhelpers:safe_html_string("~p", [config:read(lb_active_load_metric)])},
         {"load metric value:", webhelpers:safe_html_string("~p", [lb_stats:get_load_metric()])},
         {"request metric", webhelpers:safe_html_string("~p", [config:read(lb_active_request_metric)])},
         {"request metric value", webhelpers:safe_html_string("~p", [lb_stats:get_request_metric()])},
         {"balance with", webhelpers:safe_html_string("~p", [config:read(lb_active_balance_metric)])},
         {"last balance:", webhelpers:safe_html_string("~p", [get_time_last_balance(MyState)])},
         {"pending op:",   webhelpers:safe_html_string("~p", [get_pending_op(MyState)])},
         {"last db monitor init:", webhelpers:safe_html_string("~p", [get_last_db_monitor_init(MyState)])}
        ],
    case get_lb_module() of
        none ->
            Return = KVList;
        _ ->
            Seperator = {"module", ""},
            Return = KVList ++ [Seperator | call_module(get_web_debug_kv, [ModuleState])]
    end,
    comm:send_local(Requestor, {web_debug_info_reply, Return}),
    State;

on(Msg, {MyState, ModuleState}) ->
    ModuleState2 = call_module(handle_msg, [Msg, ModuleState]),
    {MyState, ModuleState2}.

%%%%%%%%%%%%%%%%%%%%%%% Load Balancing %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec balance_nodes(lb_info:lb_info(), lb_info:lb_info(), options()) -> ok.
balance_nodes(HeavyNode, LightNode, Options) ->
    balance_nodes(HeavyNode, LightNode, nil, Options).

-spec balance_nodes(lb_info:lb_info(), lb_info:lb_info(), lb_info:lb_info() | nil, options()) -> ok.
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

-spec balance_noop(options()) -> ok.
%% no op but we send back simulation results
balance_noop(Options) ->
    case proplists:get_value(simulate, Options) of
        undefined -> ok;
        ReqId ->
            ReplyTo = proplists:get_value(reply_to, Options),
            Id = proplists:get_value(id, Options),
            comm:send(ReplyTo, {simulation_result, Id, ReqId, {nil, 0}})
    end.

%%%%%%%%%%%%%%%%%%%%%%%% Calls from dht_node %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Process load balancing messages sent to the dht node
-spec handle_dht_msg(dht_message(), dht_node_state:state()) -> dht_node_state:state().

handle_dht_msg({lb_active, reset_db_monitors}, DhtState) ->
    case lb_stats:monitor_db() of
        true ->
            MyPredId = dht_node_state:get(DhtState, pred_id),
            DhtNodeMonitor = dht_node_state:get(DhtState, monitor_proc),
            comm:send_local(DhtNodeMonitor, {db_op_init, MyPredId});
        false -> ok
    end,
    DhtState;

%% We received a jump or slide operation from a LightNode.
%% In either case, we'll compute the target id and send out
%% the jump or slide message to the LightNode.
handle_dht_msg({lb_active, balance, HeavyNode, LightNode, LightNodeSucc, Options}, DhtState) ->
    %% check if we are the correct node
    case lb_info:get_node(HeavyNode) =/= dht_node_state:get(DhtState, node) of
        true -> ?TRACE("I was mistaken for the HeavyNode. Doing nothing~n", []), ok;
        false ->
            %% get our load info again to have the newest data available
            MyNode = lb_info:new(dht_node_state:details(DhtState)),
            JumpOrSlide = %case lb_info:neighbors(MyNode, LightNode) of
                case LightNodeSucc =:= nil of
                    true  -> slide;
                    false -> jump
                end,

                ProposedTargetLoadItems = lb_info:get_target_load(items, JumpOrSlide, MyNode, LightNode),
                ProposedTargetLoadRequests = lb_info:get_target_load(requests, JumpOrSlide, MyNode, LightNode),

                {TargetLoadItems, TargetLoadRequests} =
                    case gossip_available(Options) of
                        true -> AvgItems = proplists:get_value(avgItems, Options),
                                AvgRequests = proplists:get_value(avgRequests, Options),
                                %% don't take away more items than the average
                                {?IIF(ProposedTargetLoadItems > AvgItems,
                                     trunc(AvgItems), ProposedTargetLoadItems),
                                 ?IIF(ProposedTargetLoadRequests > AvgRequests,
                                     trunc(AvgRequests), ProposedTargetLoadRequests)
                                };
                        false -> {ProposedTargetLoadItems, ProposedTargetLoadRequests}
                    end,

                {From, To, Direction} =
                    case JumpOrSlide =:= jump orelse lb_info:is_succ(MyNode, LightNode) of
                        true  -> %% Jump or heavy node is succ of light node
                            {dht_node_state:get(DhtState, pred_id), dht_node_state:get(DhtState, node_id), forward};
                        false -> %% Light node is succ of heavy node
                            {dht_node_state:get(DhtState, node_id), dht_node_state:get(DhtState, pred_id), backward}
                    end,

                {Metric, {SplitKey, TakenLoad}} =
                    case config:read(lb_active_balance_metric) of %% TODO getter
                        items ->
                            {items, dht_node_state:get_split_key(DhtState, From, To, TargetLoadItems, Direction)};
                        requests ->
                            case lb_stats:get_request_histogram_split_key(TargetLoadRequests, Direction, lb_info:get_time(HeavyNode)) of
                                %% TODO fall back in a more clever way / abort lb request
                                failed ->
                                    log:log(warn, "get_request_histogram failed. falling back to item balancing.~n", []),
                                    {items, dht_node_state:get_split_key(DhtState, From, To, TargetLoadItems, Direction)};
                                Val -> {requests, Val}
                            end
                    end,

                ?TRACE("SplitKey: ~p TargetLoadItems: ~p TargetLoadRequests: ~p TakenLoad: ~p Metric: ~p~n",
                       [SplitKey, TargetLoadItems, TargetLoadRequests, TakenLoad, Metric]),

            case is_simulation(Options) of

                true -> %% compute result of simulation and reply
                    ReqId = proplists:get_value(simulate, Options),
                    LoadChange =
                        case JumpOrSlide of
                            slide -> lb_info:get_load_change_slide(Metric, TakenLoad, HeavyNode, LightNode);
                            jump  -> lb_info:get_load_change_jump(Metric, TakenLoad, HeavyNode, LightNode, LightNodeSucc)
                        end,
                    ReplyTo = proplists:get_value(reply_to, Options),
                    Id = proplists:get_value(id, Options),
                    comm:send(ReplyTo, {simulation_result, Id, ReqId, {Metric, LoadChange}});

                false -> %% perform balancing
                    StdDevTest =
                        case gossip_available(Options) of
                            true ->
                                S = config:read(lb_active_gossip_stddev_threshold),
                                DhtSize = proplists:get_value(dht_size, Options),
                                StdDev =
                                    if Metric =:= items ->
                                           proplists:get_value(stddevItems, Options);
                                       Metric =:= requests ->
                                           proplists:get_value(stddevRequests, Options)
                                    end,
                                Variance = StdDev * StdDev,
                                VarianceChange =
                                    case JumpOrSlide of
                                        slide -> lb_info:get_load_change_slide(Metric, TakenLoad, DhtSize, HeavyNode, LightNode);
                                        jump -> lb_info:get_load_change_jump(Metric, TakenLoad, DhtSize, HeavyNode, LightNode, LightNodeSucc)
                                    end,
                                VarianceNew = Variance + VarianceChange,
                                StdDevNew = ?IIF(VarianceNew >= 0, math:sqrt(VarianceNew), StdDev),
                                ?TRACE("New StdDev: ~p Old StdDev: ~p Metric: ~p~n", [StdDevNew, StdDev, Metric]),
                                StdDevNew < StdDev * (1 - S / DhtSize);
                            %% gossip not available, skipping this test
                            false -> true
                        end,

                    case StdDevTest andalso TakenLoad > 0 of
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
                    end
            end

    end,
    DhtState;

handle_dht_msg(Msg, DhtState) ->
    call_module(handle_dht_msg, [Msg, DhtState]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%% Util %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [is_enabled/0]}).
-spec is_enabled() -> boolean().
is_enabled() ->
    config:read(lb_active).

-spec call_module(atom(), list()) -> module_state() | dht_node_state:state().
call_module(Fun, Args) ->
    case get_lb_module() of
        none ->
            {};
        Module ->
            apply(Module, Fun, Args)
    end.

-spec get_lb_module() -> atom() | failed.
get_lb_module() ->
    config:read(lb_active_module).

-spec get_pending_op(my_state()) -> nil | lb_op().
get_pending_op(MyState) ->
    MyState#my_state.pending_op.

-spec set_pending_op(nil | lb_op(), my_state()) -> my_state().
set_pending_op(Op, MyState) ->
    MyState#my_state{pending_op = Op}.

-spec op_pending(my_state()) -> boolean().
op_pending(MyState) ->
    case MyState#my_state.pending_op of
        nil -> false;
        Op ->
            case old_op(Op) of
                false -> true;
                true ->
                    ?TRACE("Ignoring old op ~p~n", [Op]),
                    false
            end
    end.

%% @doc Checks if an lb_op has been pending for a long time
-spec old_op(lb_op()) -> boolean().
old_op(Op) ->
    Threshold = config:read(lb_active_wait_for_pending_ops),
    timer:now_diff(os:timestamp(), Op#lb_op.time) div 1000 > Threshold.

%% @doc Checks if an lb_op contains old data
-spec old_data(lb_op(), my_state()) -> boolean().
old_data(Op, MyState) ->
    LastBalanceTime = get_time_last_balance(MyState),
    DataTime = Op#lb_op.data_time,
    timer:now_diff(LastBalanceTime, DataTime) > 0.

-spec set_last_db_monitor_init(my_state()) -> my_state().
set_last_db_monitor_init(MyState) ->
    MyState#my_state{last_db_monitor_reset = os:timestamp()}.

-spec get_last_db_monitor_init(my_state()) -> erlang:timestamp().
get_last_db_monitor_init(MyState) ->
    MyState#my_state.last_db_monitor_reset.

-spec get_time_last_balance(my_state()) -> erlang:timestamp().
get_time_last_balance(MyState) ->
    MyState#my_state.last_balance.

-spec set_time_last_balance(my_state()) -> my_state().
set_time_last_balance(MyState) ->
    MyState#my_state{last_balance = os:timestamp()}.

-spec gossip_available(options()) -> boolean().
gossip_available(Options) ->
    proplists:is_defined(dht_size, Options) andalso
        proplists:is_defined(avgItems, Options) andalso
        proplists:is_defined(stddevItems, Options) andalso
        proplists:is_defined(avgRequests, Options) andalso
        proplists:is_defined(stddevRequests, Options).

-spec is_simulation(options()) -> boolean().
is_simulation(Options) ->
    proplists:is_defined(simulate, Options).

-spec trigger() -> ok.
trigger() ->
    Interval = config:read(lb_active_interval) div 1000,
    msg_delay:send_trigger(Interval, {lb_trigger}).

-spec check_for_gossip_modules() -> boolean().
check_for_gossip_modules() ->
    RequiredModule = lb_active_gossip_request_metric,
    Fun = fun(Value) -> lists:member(RequiredModule, Value) end,
    Msg = io_lib:format("~p required when lb_active_use_gossip enabled.~n", [RequiredModule]),
    not config:read(lb_active) orelse not config:read(lb_active_use_gossip) orelse
        config:cfg_test_and_error(gossip_load_additional_modules, Fun, Msg).

-spec check_module_config() -> boolean().
check_module_config() ->
    case get_lb_module() of
        none -> true;
        Module -> apply(Module, check_config, [])
    end.

%% @doc config check registered in config.erl
-spec check_config() -> boolean().
check_config() ->

    config:cfg_is_bool(lb_active) and

    config:cfg_is_in(lb_active_module, [none | ?MODULES]) and

    config:cfg_is_integer(lb_active_interval) and
    config:cfg_is_greater_than(lb_active_interval, 0) and

    config:cfg_is_bool(lb_active_use_gossip) and
    config:cfg_is_greater_than(lb_active_gossip_stddev_threshold, 0) and

    config:cfg_is_integer(lb_active_wait_for_pending_ops) and
    config:cfg_is_greater_than(lb_active_wait_for_pending_ops, 0) and

    check_for_gossip_modules() and

    lb_stats:check_config() and

    check_module_config().
