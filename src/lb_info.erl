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
-module(lb_info).
-author('michels@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([new/1]).
-export([get_load/1, get_node/1, get_succ/1, get_items/1]).
-export([is_succ/2, neighbors/2, get_target_load/3]).

-type(load() :: number()).

-record(lb_info, {load  = ?required(lb_info, load)  :: load(),
                  items = ?required(lb_info, items) :: load(),
                  node  = ?required(lb_info, node)  :: node:node_type(),
                  succ  = ?required(lb_info, succ)  :: node:node_type()
                 }).

-opaque(lb_info() :: #lb_info{}).

-ifdef(with_export_type_support).
-export_type([lb_info/0]).
-endif.

%% Convert node details to lb_info
-spec new(node_details:node_details()) -> lb_info().
new(NodeDetails) ->
    #lb_info{load  = lb_active:get_load_metric(),
             items = node_details:get(NodeDetails, load),
             node  = node_details:get(NodeDetails, node),
             succ  = node_details:get(NodeDetails, succ)}.

-spec get_load(lb_info()) -> load() | node:node_type().
get_load (#lb_info{load  = unknown}) -> throw(no_load_available);
get_load (#lb_info{load  = Load }) -> Load.
get_items(#lb_info{items = Items}) -> Items.
get_node (#lb_info{node  = Node }) -> Node.
get_succ (#lb_info{succ  = Succ }) -> Succ.

is_succ(Succ, Node) ->
    get_succ(Node) =:= get_node(Succ).

-spec neighbors(lb_info(), lb_info()) -> boolean().
neighbors(Node1, Node2) ->
    is_succ(Node1, Node2) orelse is_succ(Node2, Node1).

%% @doc The number of db entries the heavy node will give to the light node
-spec get_target_load(slide | jump, lb_info(), lb_info()) -> non_neg_integer().
get_target_load(slide, HeavyNode, LightNode) ->
    (get_items(HeavyNode) + get_items(LightNode)) div 2;
get_target_load(jump, HeavyNode, _LightNode) ->
     get_items(HeavyNode) div 2.