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
%% @doc For active load balancing:
%%          - contains information about a node
%%          - provides functions to evaluate an lb operation
%% @version $Id$
-module(lb_info).
-author('michels@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([new/1, is_valid/1]).
-export([get_load/1, get_reqs/1, get_node/1, get_succ/1, get_items/1, get_time/1]).
-export([is_succ/2, neighbors/2, get_target_load/3, get_target_load/4]).
%% without dht size
-export([get_load_change_slide/4, get_load_change_jump/5]).
%% with dht size available
-export([get_load_change_slide/5, get_load_change_jump/6]).
-export([get_oldest_data_time/1]).
% util
-export([bound/3]).

-export_type([lb_info/0]).

-type load() :: unknown | number().

-record(lb_info, {load  = unknown                   :: unknown | load(),
                  reqs  = unknown                   :: unknown | load(),
                  items = ?required(lb_info, items) :: load(),
                  node  = ?required(lb_info, node)  :: node:node_type(),
                  succ  = ?required(lb_info, succ)  :: node:node_type(),
                  time  = os:timestamp()            :: erlang_timestamp()
                 }).

-opaque lb_info() :: #lb_info{}.

%% @doc Creates a new record to hold essential load balancing values
-spec new(NodeDetails::node_details:node_details()) -> lb_info().
new(NodeDetails) ->
    Items = node_details:get(NodeDetails, load),
    %% lb_stats:get_load_metric(), can be unknown
    SystemLoad = node_details:get(NodeDetails, load2),
    %% lb_stats:get_request_metric(), can be unknown
    Requests = node_details:get(NodeDetails, load3),
    Load = case config:read(lb_active_balance) of
               items -> Items;
               requests ->
                   case config:read(lb_active_fall_back_to_items) of
                       true ->
                           try
                               (erlang:round(math:sqrt(Items)) + Requests) * SystemLoad
                           catch
                               error:badarith -> unknown
                           end;
                       _ -> Requests
                   end
           end,
    #lb_info{load  = Load,
             reqs  = Requests,
             items = Items,
             node  = node_details:get(NodeDetails, node),
             succ  = node_details:get(NodeDetails, succ)}.

-spec get_load(LBInfo::lb_info()) -> load().
get_load (#lb_info{load  = Load }) -> Load.

-spec get_reqs(LBInfo::lb_info()) -> load().
get_reqs (#lb_info{reqs  = Requests}) -> Requests.

-spec get_items(LBInfo::lb_info()) -> load().
get_items(#lb_info{items = Items}) -> Items.

-spec get_node(LBInfo::lb_info()) -> node:node_type().
get_node (#lb_info{node  = Node }) -> Node.

-spec get_succ(LBInfo::lb_info()) -> node:node_type().
get_succ (#lb_info{succ  = Succ }) -> Succ.

-spec get_time(LBInfo::lb_info()) -> erlang_timestamp().
get_time (#lb_info{time  = Time }) -> Time.

-spec is_succ(Node1::lb_info(), Node2::lb_info()) -> boolean().
is_succ(Succ, Node) ->
    get_succ(Node) =:= get_node(Succ).

-spec neighbors(Node1::lb_info(), Node2::lb_info()) -> boolean().
neighbors(Node1, Node2) ->
    is_succ(Node1, Node2) orelse is_succ(Node2, Node1).

-spec is_valid(Info::lb_info()) -> boolean().
is_valid(Info) -> is_number(get_load(Info)).

%% @doc The number of db entries the heavy node will give to the light node
-spec get_target_load(items | requests, Op::slide | jump, HeavyNode::lb_info(), LightNode::lb_info()) -> non_neg_integer().
get_target_load(items, JumpOrSlide, HeavyNode, LightNode) ->
    get_target_load(JumpOrSlide, get_items(HeavyNode), get_items(LightNode));
get_target_load(requests, JumpOrSlide, HeavyNode, LightNode) ->
    get_target_load(JumpOrSlide, get_reqs(HeavyNode), get_reqs(LightNode)).

%% @doc The number of db entries the heavy node will give to the light node (weighted)
-spec get_target_load(Op::slide | jump, HeavyNode::load(), LightNode::load())
                    -> non_neg_integer().
get_target_load(_Op, unknown, _LightNode) -> 0;
get_target_load(_Op, _HeavyNode, unknown) -> 0;
get_target_load(slide, HeavyNode, LightNode) ->
    TotalItems = HeavyNode + LightNode,
    AvgItems = trunc(TotalItems) div 2,
    ItemsToShed = HeavyNode - AvgItems,
    bound(0, ItemsToShed, HeavyNode);
get_target_load(jump, HeavyNode, _LightNode) ->
    AvgItems = trunc(HeavyNode) div 2,
    ItemsToShed = HeavyNode - AvgItems,
    bound(0, ItemsToShed, HeavyNode).

%% @doc Calculates the change in Variance
%% no dht size available
-spec get_load_change_slide(Metric::items | requests, TakenLoad::non_neg_integer(),
                            HeavyNode::lb_info(), LightNode::lb_info()) -> LoadChange::number().
get_load_change_slide(Metric, TakenLoad, HeavyNode, LightNode) ->
    get_load_change_slide(Metric, TakenLoad, 1, HeavyNode, LightNode).

%% @doc Calculates the change in Variance
%% dht size available
-spec get_load_change_slide(Metric::items | requests, TakenLoad::non_neg_integer(), DhtSize::pos_integer(),
                            HeavyNode::lb_info(), LightNode::lb_info()) -> LoadChange::number().
get_load_change_slide(Metric, TakenLoad, DhtSize, HeavyNode, LightNode) ->
    MetricFun = get_metric_fun(Metric),
    get_load_change_diff(DhtSize, MetricFun(HeavyNode), MetricFun(HeavyNode) - TakenLoad) +
        get_load_change_diff(DhtSize, MetricFun(LightNode), MetricFun(LightNode) + TakenLoad).

%% @doc Calculates the change in Variance
%% no dht size available
-spec get_load_change_jump(Metric::items | requests, TakenLoad::non_neg_integer(),
                           HeavyNode::lb_info(), LightNode::lb_info(), LightNodeSucc::lb_info()) -> LoadChange::number().
get_load_change_jump(Metric, TakenLoad, HeavyNode, LightNode, LightNodeSucc) ->
    get_load_change_jump(Metric, TakenLoad, 1, HeavyNode, LightNode, LightNodeSucc).

%% @doc Calculates the change in Variance
%% dht size available
-spec get_load_change_jump(Metric::items | requests, TakenLoad::non_neg_integer(), DhtSize::pos_integer(),
                           HeavyNode::lb_info(), LightNode::lb_info(), LightNodeSucc::lb_info()) -> LoadChange::number().
get_load_change_jump(Metric, TakenLoad, DhtSize, HeavyNode, LightNode, LightNodeSucc) ->
    MetricFun = get_metric_fun(Metric),
    get_load_change_diff(DhtSize, MetricFun(LightNode), TakenLoad) +
        get_load_change_diff(DhtSize, MetricFun(LightNodeSucc), MetricFun(LightNodeSucc) + MetricFun(LightNode)) +
        get_load_change_diff(DhtSize, MetricFun(HeavyNode), MetricFun(HeavyNode) - TakenLoad).

-spec get_load_change_diff(pos_integer(), non_neg_integer(), non_neg_integer()) -> load().
get_load_change_diff(DhtSize, OldLoad, NewLoad) ->
    NewLoad * NewLoad / DhtSize - OldLoad * OldLoad / DhtSize.

-spec get_metric_fun(Metric::items | requests) -> fun((lb_info()) -> load()).
get_metric_fun(items)    -> fun get_items/1;
get_metric_fun(requests) -> fun get_reqs/1.

-spec get_oldest_data_time([lb_info()]) -> OldestTime::erlang_timestamp().
get_oldest_data_time([Node | Other]) ->
    get_oldest_data_time(Other, get_time(Node)).

-spec get_oldest_data_time([lb_info()], Oldest::erlang_timestamp()) -> OldestTime::erlang_timestamp().
get_oldest_data_time([], Oldest) ->
    Oldest;
get_oldest_data_time([Node | Other], Oldest) ->
    OldestNew = erlang:min(get_time(Node), Oldest),
    get_oldest_data_time(Other, OldestNew).

-spec bound(LowerBound::number(), Value::number(), UpperBound::number()) -> number().
bound(LowerBound, Value, UpperBound) ->
    if Value < LowerBound -> LowerBound;
       Value > UpperBound -> UpperBound;
       true -> Value
    end.
