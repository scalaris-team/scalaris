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
%% @doc For active load balancing: lb_info contains information about a node
%% @version $Id$
-module(lb_info).
-author('michels@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([new/1]).
-export([get_load/1, get_node/1, get_succ/1, get_items/1, get_time/1]).
-export([is_succ/2, neighbors/2, get_target_load/3, get_target_load/5]).
%% without dht size
-export([get_load_change_slide/3, get_load_change_jump/4]).
%% with dht size available
-export([get_load_change_slide/4, get_load_change_jump/5]).
-export([get_oldest_data_time/1]).

-type(load() :: number()).

-record(lb_info, {load  = ?required(lb_info, load)  :: load(),
                  items = ?required(lb_info, items) :: load(),
                  node  = ?required(lb_info, node)  :: node:node_type(),
                  succ  = ?required(lb_info, succ)  :: node:node_type(),
                  time  = os:timestamp()            :: erlang:timestamp()
                 }).

-opaque(lb_info() :: #lb_info{}).

-ifdef(with_export_type_support).
-export_type([lb_info/0]).
-endif.

%% Convert node details to lb_info
-spec new(node_details:node_details()) -> lb_info().
new(NodeDetails) ->
    Items = node_details:get(NodeDetails, load),
    Load = case lb_active:get_load_metric() of
               items -> Items;
               Metric -> Metric
           end,
    #lb_info{load  = Load,
             items = Items,
             node  = node_details:get(NodeDetails, node),
             succ  = node_details:get(NodeDetails, succ)}.

-spec get_load(lb_info()) -> load() | node:node_type().
get_load (#lb_info{load  = Load }) -> Load.
get_items(#lb_info{items = Items}) -> Items.
get_node (#lb_info{node  = Node }) -> Node.
get_succ (#lb_info{succ  = Succ }) -> Succ.
get_time (#lb_info{time  = Time }) -> Time.

is_succ(Succ, Node) ->
    get_succ(Node) =:= get_node(Succ).

-spec neighbors(Node1::lb_info(), Node2::lb_info()) -> boolean().
neighbors(Node1, Node2) ->
    is_succ(Node1, Node2) orelse is_succ(Node2, Node1).

%% @doc The number of db entries the heavy node will give to the light node
-spec get_target_load(Op::slide | jump, HeavyNode::lb_info(), LightNode::lb_info()) -> non_neg_integer().
get_target_load(JumpOrSlide, HeavyNode, LightNode) ->
    case config:read(lb_active_metric) of
        items -> get_target_load(JumpOrSlide, HeavyNode, 1, LightNode, 1);
        _ -> get_target_load(JumpOrSlide, HeavyNode, get_load(HeavyNode), LightNode, get_load(LightNode))
    end.

-define(BOUND(X,Y,Z), begin From = X, To = Z, V = Y,
                      if   V < From -> From;
                           V > To -> To;
                           true     -> V
                      end end).

%% @doc The number of db entries the heavy node will give to the light node (weighted)
-spec get_target_load(Op::slide | jump, HeavyNode::lb_info(), WeightHeavy::number(),
                                        LightNode::lb_info(), WeightLight::number())
                    -> non_neg_integer().
get_target_load(slide, HeavyNode, WeightHeavy, LightNode, WeightLight) ->
    TotalItems = get_items(HeavyNode) + get_items(LightNode),
    AvgItems = TotalItems div 2,
    Factor = try WeightLight / WeightHeavy catch error:badarith -> 1 end,
    ItemsToShed = get_items(HeavyNode) - trunc(Factor * AvgItems),
    ?BOUND(0, ItemsToShed, get_items(HeavyNode));
get_target_load(jump, HeavyNode, WeightHeavy, _LightNode, WeightLight) ->
    AvgItems = get_items(HeavyNode) div 2,
    Factor = try WeightLight / WeightHeavy catch error:badarith -> 1 end,
    ItemsToShed = get_items(HeavyNode) - trunc(Factor * AvgItems),
    ?BOUND(0, ItemsToShed, get_items(HeavyNode)).

%% TODO generic load change
%% @doc Calculates the change in Variance
%% no dht size available
-spec get_load_change_slide(TakenLoad::non_neg_integer(), HeavyNode::lb_info(), LightNode::lb_info()) -> LoadChange::integer().
get_load_change_slide(TakenLoad, HeavyNode, LightNode) -> 
    get_load_change_slide(TakenLoad, 1, HeavyNode, LightNode).

%% @doc Calculates the change in Variance
%% dht size available
-spec get_load_change_slide(TakenLoad::non_neg_integer(), DhtSize::pos_integer(), HeavyNode::lb_info(), LightNode::lb_info()) -> LoadChange::integer().
get_load_change_slide(TakenLoad, DhtSize, HeavyNode, LightNode) ->
    get_load_change_diff(DhtSize, get_load(HeavyNode), get_load(HeavyNode) - TakenLoad) +
        get_load_change_diff(DhtSize, get_load(LightNode), get_load(LightNode) + TakenLoad).

%% @doc Calculates the change in Variance
%% no dht size available
-spec get_load_change_jump(TakenLoad::non_neg_integer(), HeavyNOde::lb_info(), LightNode::lb_info(), LightNodeSucc::lb_info()) -> LoadChange::integer().
get_load_change_jump(TakenLoad, HeavyNode, LightNode, LightNodeSucc) ->
    get_load_change_jump(TakenLoad, 1, HeavyNode, LightNode, LightNodeSucc).

%% @doc Calculates the change in Variance
%% dht size available
-spec get_load_change_jump(non_neg_integer(), pos_integer(), lb_info(), lb_info(), lb_info()) -> LoadChange::integer().
get_load_change_jump(TakenLoad, DhtSize, HeavyNode, LightNode, LightNodeSucc) ->
    get_load_change_diff(DhtSize, get_load(LightNode), TakenLoad) +
        get_load_change_diff(DhtSize, get_load(LightNodeSucc), get_load(LightNodeSucc) + get_load(LightNode)) +
        get_load_change_diff(DhtSize, get_load(HeavyNode), get_load(HeavyNode) - TakenLoad).

-spec get_load_change_diff(pos_integer(), non_neg_integer(), non_neg_integer()) -> integer().
get_load_change_diff(DhtSize, OldItemLoad, NewItemLoad) ->
    NewItemLoad * NewItemLoad / DhtSize - OldItemLoad * OldItemLoad / DhtSize.

-spec get_oldest_data_time([lb_info()]) -> OldestTime::erlang:timestamp().
get_oldest_data_time([Node | Other]) ->
    get_oldest_data_time(Other, get_time(Node)).

-spec get_oldest_data_time([lb_info()], Oldest::lb_info()) -> OldestTime::erlang:timestamp().
get_oldest_data_time([], Oldest) ->
    Oldest;
get_oldest_data_time([Node | Other], Oldest) ->
    OldestNew = erlang:min(get_time(Node), Oldest),
    get_oldest_data_time(Other, OldestNew).
