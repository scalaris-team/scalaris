%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    T-Man ring maintenance
%%% @end
%% @version $Id$
%% @reference Mark Jelasity, Ozalp Babaoglu. T-Man: Gossip-Based Overlay
%% Topology Management. Engineering Self-Organising Systems 2005:1-15
-module(rm_tman).
-author('hennig@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behavior(gen_component).
-behavior(rm_beh).

-export([start_link/1]).
-export([init/1, on/2]).

-export([get_base_interval/0, get_min_interval/0, get_max_interval/0,
         check_config/0]).

-type(state() :: {Id             :: ?RT:key(),
                  Neighborhood   :: nodelist:neighborhood(),
                  RandomViewSize :: pos_integer(),
                  Interval       :: pos_integer(),
                  TriggerState   :: trigger:state(),
                  Cache          :: [node:node_type()], % random cyclon nodes
                  Churn          :: boolean()}
     | {uninit, QueuedMessages::[cs_send:message()], TriggerState :: trigger:state()}).

% accepted messages
-type(message() ::
    {init, Id::?RT:key(), Me::node_details:node_type(), Predecessor::node_details:node_type(), Successor::node:node_type()} |
    {trigger} |
    {cy_cache, Cache::nodelist:nodelist()} |
    {rm_buffer, OtherNeighbors::nodelist:neighborhood(), RequestPredsMinCount::non_neg_integer(), RequestSuccsMinCount::non_neg_integer()} |
    {rm_buffer_response, OtherNeighbors::nodelist:neighborhood()} |
    {zombie, Node::node:node_type()} |
    {crash, DeadPid::cs_send:mypid()} |
    {'$gen_cast', {debug_info, Requestor::cs_send:erl_local_pid()}} |
    {check_ring, Token::non_neg_integer(), Master::node:node_type()} |
    {init_check_ring, Token::non_neg_integer()} |
    {notify_new_pred, Pred::node:node_type()} |
    {notify_new_succ, Succ::node:node_type()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a chord-like ring maintenance process, registers it with the
%%      process dictionary and returns its pid for use by a supervisor.
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    Trigger = config:read(ringmaintenance_trigger),
    gen_component:start_link(?MODULE, Trigger, [{register, InstanceId, ring_maintenance}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(module()) -> {uninit, QueuedMessages::[], TriggerState::trigger:state()}.
init(Trigger) ->
    log:log(info,"[ RM ~p ] starting ring maintainer TMAN~n", [cs_send:this()]),
    TriggerState = trigger:init(Trigger, ?MODULE),
    cs_send:send_local(get_pid_dnc() , {subscribe, self()}),
    cs_send:send_local(get_cs_pid(), {init_rm, self()}),
    {uninit, [], TriggerState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc message handler
-spec on(message(), state()) -> state().
on({init, Id, Me, Predecessor, Successor}, {uninit, QueuedMessages, TriggerState}) ->
    Neighborhood = nodelist:new_neighborhood(Predecessor, Me, Successor),
    NewTriggerState = trigger:first(TriggerState),
    fd:subscribe(lists:usort([node:pidX(Predecessor), node:pidX(Successor)])),
    cyclon:get_subset_rand_next_interval(1),
    cs_send:send_queued_messages(QueuedMessages),
    {Id, Neighborhood, config:read(cyclon_cache_size),
     stabilizationInterval_min(), NewTriggerState, [], true};

on(Msg, {uninit, QueuedMessages, TriggerState}) ->
    {uninit, [Msg | QueuedMessages], TriggerState};

on({trigger},
   {Id, Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    % Trigger an update of the Random view
    %
    RndView = get_RndView(RandViewSize, Cache),
    %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
    {Pred, Succ} = get_safe_pred_succ(Neighborhood, RndView),
    %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
    % Test for being alone:
    NewTriggerState =
        case not (nodelist:has_real_pred(Neighborhood) orelse
                      nodelist:has_real_succ(Neighborhood)) of
            true ->
                %TODO: do we need to tell the DHT node here? doesn't it already know?
                rm_beh:update_neighbors(Neighborhood),
                TriggerState;
            false ->
                RequestPredsMinCount =
                    case nodelist:has_real_pred(Neighborhood) of
                        true  -> get_pred_list_length() - length(nodelist:preds(Neighborhood));
                        false -> get_pred_list_length()
                    end,
                RequestSuccsMinCount =
                    case nodelist:has_real_succ(Neighborhood) of
                        true  -> get_succ_list_length() - length(nodelist:succs(Neighborhood));
                        false -> get_succ_list_length()
                    end,
                Message = {rm_buffer, Neighborhood, RequestPredsMinCount, RequestSuccsMinCount},
                cs_send:send_to_group_member(node:pidX(Succ), ring_maintenance,
                                             Message),
                case Pred =/= Succ of
                    true ->
                        cs_send:send_to_group_member(node:pidX(Pred),
                                                     ring_maintenance,
                                                     Message);
                    false ->
                        ok
                end,
                trigger:next(TriggerState, base_interval)
        end,
   {Id, Neighborhood, RandViewSize, Interval, NewTriggerState, Cache, Churn};

% got empty cyclon cache
on({cy_cache, []},
   {_Id, _Neighborhood, RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State)  ->
    % ignore empty cache from cyclon
    cyclon:get_subset_rand_next_interval(RandViewSize),
    State;

% got cyclon cache
on({cy_cache, NewCache},
   {Id, Neighborhood, RandViewSize, Interval, TriggerState, _Cache, Churn}) ->
    % increase RandViewSize (no error detected):
    RandViewSizeNew =
        case (RandViewSize < config:read(cyclon_cache_size)) of
            true  -> RandViewSize + 1;
            false -> RandViewSize
        end,
    % trigger new cyclon cache request
    cyclon:get_subset_rand_next_interval(RandViewSizeNew),
    MyRndView = get_RndView(RandViewSizeNew, NewCache),
    {NewNeighborhood, NewInterval, NewChurn} =
        update_view(Neighborhood, MyRndView,
                    nodelist:mk_neighborhood(NewCache, nodelist:node(Neighborhood), get_pred_list_length(), get_succ_list_length()),
                    Interval, Churn),
    {Id, NewNeighborhood, RandViewSizeNew, NewInterval, TriggerState, NewCache, NewChurn};

% got shuffle request
on({rm_buffer, OtherNeighbors, RequestPredsMinCount, RequestSuccsMinCount},
   {Id, Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    MyRndView = get_RndView(RandViewSize, Cache),
    MyView = lists:append(nodelist:to_list(Neighborhood), MyRndView),
    OtherNode = nodelist:node(OtherNeighbors),
    OtherNodeId = node:id(OtherNode),
    OtherLastPredId = node:id(lists:last(nodelist:preds(OtherNeighbors))),
    OtherLastSuccId = node:id(lists:last(nodelist:succs(OtherNeighbors))),
    NeighborsToSendTmp = nodelist:mk_neighborhood(MyView, OtherNode, get_pred_list_length(), get_succ_list_length()),
    NeighborsToSend = 
        case (OtherNodeId =:= OtherLastPredId) orelse (OtherNodeId =:= OtherLastSuccId) of
            true  -> NeighborsToSendTmp;
            false ->
                nodelist:filter_min_length(NeighborsToSendTmp,
                                           fun(N) ->
                                                   util:is_between_stab(OtherNodeId, node:id(N), OtherLastSuccId) orelse
                                                   util:is_between_stab(OtherLastPredId, node:id(N), OtherNodeId)
                                           end,
                                           RequestPredsMinCount,
                                           RequestSuccsMinCount)
        end,
    cs_send:send_to_group_member(node:pidX(nodelist:node(OtherNeighbors)),
                                 ring_maintenance,
                                 {rm_buffer_response, NeighborsToSend}),
    {NewNeighborhood, NewInterval, NewChurn} =
        update_view(Neighborhood, MyRndView, OtherNeighbors, Interval, Churn),
    NewTriggerState = trigger:next(TriggerState, NewInterval),
    {Id, NewNeighborhood, RandViewSize, NewInterval, NewTriggerState, Cache, NewChurn};

on({rm_buffer_response, OtherNeighbors},
   {Id, Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    MyRndView = get_RndView(RandViewSize, Cache),
    {NewNeighborhood, NewInterval, NewChurn} =
        update_view(Neighborhood, MyRndView, OtherNeighbors, Interval, Churn),
    % increase RandViewSize (no error detected):
    NewRandViewSize =
        case RandViewSize < config:read(cyclon_cache_size) of
            true ->  RandViewSize + 1;
            false -> RandViewSize
        end,
    NewTriggerState = trigger:next(TriggerState, NewInterval),
    {Id, NewNeighborhood, NewRandViewSize, NewInterval, NewTriggerState, Cache, NewChurn};

% dead-node-cache reported dead node to be alive again
on({zombie, Node}, {Id, Neighborhood, RandViewSize, _Interval, TriggerState, Cache, Churn})  ->
    NewTriggerState = trigger:next(TriggerState, now_and_min_interval),
    {Id, Neighborhood, RandViewSize, stabilizationInterval_min(), NewTriggerState, [Node | Cache], Churn};

% failure detector reported dead node
on({crash, DeadPid},
   {Id, Neighborhood, _RandViewSize, _Interval, TriggerState, Cache, Churn}) ->
    EvalFun = fun dn_cache:add_zombie_candidate/1,
    NewNeighborhood = nodelist:remove(DeadPid, Neighborhood, EvalFun),
    NewCache = nodelist:remove(DeadPid, Cache, EvalFun),
    update_dht_node(Neighborhood, NewNeighborhood),
    update_failuredetector(Neighborhood, NewNeighborhood),
    NewTriggerState = trigger:next(TriggerState, now_and_min_interval),
    {Id, NewNeighborhood, 0, stabilizationInterval_min(), NewTriggerState, NewCache, Churn};

on({'$gen_cast', {debug_info, Requestor}},
   {_Id, Neighborhood, _RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State) ->
    cs_send:send_local(Requestor,
                       {debug_info_response,
                        [{"self", lists:flatten(io_lib:format("~p", [nodelist:node(Neighborhood)]))},
                         {"preds", lists:flatten(io_lib:format("~p", [nodelist:preds(Neighborhood)]))},
                         {"succs", lists:flatten(io_lib:format("~p", [nodelist:succs(Neighborhood)]))}]}),
    State;

% trigger by admin:dd_check_ring
on({check_ring, Token, Master},
   {_Id,  Neighborhood, _RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State) ->
    Me = nodelist:node(Neighborhood),
    case {Token, Master} of
        {0, Me} ->
            io:format(" [RM ] CheckRing   OK  ~n");
        {0, _} ->
            io:format(" [RM ] CheckRing  reach TTL in Node ~p not in ~p~n", [Master, Me]);
        {Token, Me} ->
            io:format(" [RM ] Token back with Value: ~p~n",[Token]);
        {Token, _} ->
            {Pred, _Succ} = get_safe_pred_succ(Neighborhood, []),
            cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance,
                                         {check_ring, Token - 1, Master})
    end,
    State;

% trigger by admin:dd_check_ring
on({init_check_ring, Token},
   {_Id, Neighborhood, _RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State) ->
    Me = nodelist:node(Neighborhood),
    {Pred, _Succ} = get_safe_pred_succ(Neighborhood, []),
    cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance,
                                 {check_ring, Token - 1, Me}),
    State;

on({notify_new_pred, NewPred},
   {Id, Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    NewNeighborhood = nodelist:add_node(Neighborhood, NewPred, get_pred_list_length(), get_succ_list_length()),
    update_dht_node(Neighborhood, NewNeighborhood),
    update_failuredetector(Neighborhood, NewNeighborhood),
    {Id, NewNeighborhood, RandViewSize, Interval, TriggerState, Cache, Churn};

on({notify_new_succ, NewSucc},
   {Id, Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    NewNeighborhood = nodelist:add_node(Neighborhood, NewSucc, get_pred_list_length(), get_succ_list_length()),
    update_dht_node(Neighborhood, NewNeighborhood),
    update_failuredetector(Neighborhood, NewNeighborhood),
    NewTriggerState = trigger:next(TriggerState, now_and_min_interval),
    {Id, NewNeighborhood, RandViewSize, Interval, NewTriggerState, Cache, Churn};

on(_, _State) ->
    unknown_event.

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_atom(ringmaintenance_trigger) and

    config:is_integer(stabilization_interval_base) and
    config:is_greater_than(stabilization_interval_base, 0) and

    config:is_integer(stabilization_interval_min) and
    config:is_greater_than(stabilization_interval_min, 0) and
    config:is_greater_than_equal(stabilization_interval_base, stabilization_interval_min) and

    config:is_integer(stabilization_interval_max) and
    config:is_greater_than(stabilization_interval_max, 0) and
    config:is_greater_than_equal(stabilization_interval_max, stabilization_interval_min) and
    config:is_greater_than_equal(stabilization_interval_max, stabilization_interval_base) and

    config:is_integer(cyclon_cache_size) and
    config:is_greater_than(cyclon_cache_size, 2) and

    config:is_integer(succ_list_length) and
    config:is_greater_than_equal(succ_list_length, 1) and

    config:is_integer(pred_list_length) and
    config:is_greater_than_equal(pred_list_length, 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc get a peer form the cycloncache which is alive
-spec get_RndView(integer(), [node:node_type()]) -> [node:node_type()].
get_RndView(N, Cache) ->
    lists:sublist(Cache, N).

% @doc Check if change of failuredetector is necessary
-spec update_failuredetector(
        OldNeighborhood::nodelist:neighborhood(),
        NewNeighborhood::nodelist:neighborhood()) -> ok.
update_failuredetector(OldNeighborhood, NewNeighborhood) ->
    case (OldNeighborhood =/= NewNeighborhood) of
        true ->
            OldView = lists:append(nodelist:preds(OldNeighborhood), nodelist:succs(OldNeighborhood)),
            NewView = lists:append(nodelist:preds(NewNeighborhood), nodelist:succs(NewNeighborhood)),
            OldPids = [node:pidX(Node) || Node <- OldView],
            NewPids = [node:pidX(Node) || Node <- NewView],
            fd:update_subscriptions(OldPids, NewPids),
            ok;
        false ->
            ok
    end.

%% @doc Inform the dht_node of new [succ|pred] if necessary, i.e. the lists
%%      changed.
-spec update_dht_node(
        OldNeighborhood::nodelist:neighborhood(),
        NewNeighborhood::nodelist:neighborhood()) -> ok.
update_dht_node(OldNeighborhood, NewNeighborhood) ->
    case (OldNeighborhood =/= NewNeighborhood) of
        true  -> rm_beh:update_neighbors(NewNeighborhood);
        false -> ok
    end.

%% @doc Gets the node's current successor and predecessor in a safe way.
%%      If either is unknown, the random view is used to get a replacement. If
%%      this doesn't help either, the own node is returned as this is the
%%      current node's view.
-spec get_safe_pred_succ(
        Neighborhood::nodelist:neighborhood(), RndView::[node:node_type()]) ->
              {Pred::node:node_type(), Succ::node:node_type()}.
get_safe_pred_succ(Neighborhood, RndView) ->
    case (not nodelist:has_real_pred(Neighborhood)) orelse
             (not nodelist:has_real_succ(Neighborhood)) of
        true ->
            NewNeighbors = nodelist:add_nodes(Neighborhood, RndView, 1, 1),
            {nodelist:pred(NewNeighbors), nodelist:succ(NewNeighbors)};
        false ->
            {nodelist:pred(Neighborhood), nodelist:succ(Neighborhood)}
    end.

% @doc adapt the Tman-interval
-spec new_interval(
        OldNeighborhood::nodelist:neighborhood(),
        NewNeighborhood::nodelist:neighborhood(),
        Interval::trigger:interval(), Churn::boolean()) ->
              min_interval | max_interval.
new_interval(OldNeighborhood, NewNeighborhood, _Interval, Churn) ->
    case Churn orelse has_churn(OldNeighborhood, NewNeighborhood) of
        true ->
            % increasing the ring maintenance frequency
            min_interval;
        false ->
            max_interval
    end.

% @doc is there churn in the system
-spec has_churn(
        OldNeighborhood::nodelist:neighborhood(),
        NewNeighborhood::nodelist:neighborhood()) -> boolean().
has_churn(OldNeighborhood, NewNeighborhood) ->
    OldNeighborhood =/= NewNeighborhood.

-spec update_view(
        Neighborhood::nodelist:neighborhood(), RndView::[node:node_type()],
        OtherNeighborhood::nodelist:neighborhood(),
        Interval::trigger:interval(), Churn::boolean()) ->
              {NewNeighborhood::nodelist:neighborhood(),
               NewInterval::min_interval | max_interval, NewChurn::boolean()}.
update_view(OldNeighborhood, MyRndView, OtherNeighborhood, Interval, Churn) ->
    NewNeighborhood1 = nodelist:add_nodes(OldNeighborhood, MyRndView, get_pred_list_length(), get_succ_list_length()),
    NewNeighborhood2 = nodelist:merge(NewNeighborhood1, OtherNeighborhood, get_pred_list_length(), get_succ_list_length()),
    update_dht_node(OldNeighborhood, NewNeighborhood2),
    update_failuredetector(OldNeighborhood, NewNeighborhood2),
    NewInterval = new_interval(OldNeighborhood, NewNeighborhood2, Interval, Churn),
    NewChurn = has_churn(OldNeighborhood, NewNeighborhood2),
    {NewNeighborhood2, NewInterval, NewChurn}.

-spec get_pid_dnc() -> pid() | failed.
get_pid_dnc() ->
    process_dictionary:get_group_member(dn_cache).

% get Pid of assigned dht_node
-spec get_cs_pid() -> pid() | failed.
get_cs_pid() ->
    process_dictionary:get_group_member(dht_node).

-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(stabilization_interval_base).

-spec get_min_interval() -> pos_integer().
get_min_interval() ->
    config:read(stabilization_interval_min).

-spec get_max_interval() -> pos_integer().
get_max_interval() ->
    config:read(stabilization_interval_max).

-spec get_pred_list_length() -> pos_integer().
get_pred_list_length() ->
    config:read(pred_list_length).

-spec get_succ_list_length() -> pos_integer().
get_succ_list_length() ->
    config:read(succ_list_length).

%% @doc the interval between two stabilization runs Min
-spec stabilizationInterval_min() -> pos_integer().
stabilizationInterval_min() ->
    config:read(stabilization_interval_min).
