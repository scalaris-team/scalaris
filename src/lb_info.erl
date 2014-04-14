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
-export([get_load/1, get_node/1, get_succ/1]).
-export([neighbors/2, get_target_load/3]).

-type(load() :: number()).

-record(lb_info, {load    = ?required(lb_info, load) :: load(),
                  db_keys = ?required(lb_info, keys) :: load(),
                  node    = ?required(lb_info, node) :: node:node_type(),
                  succ    = ?required(lb_info, succ) :: node:node_type()
                 }).

-opaque(lb_info() :: #lb_info{}).

-ifdef(with_export_type_support).
-export_type([lb_info/0]).
-endif.

%% Convert node details to lb_info
-spec new(node_details:node_details()) -> lb_info().
new(NodeDetails) ->
    #lb_info{load = get_load_metric(),
             db_keys = node_details:get(NodeDetails, load),
             node = node_details:get(NodeDetails, node),
             succ = node_details:get(NodeDetails, succ)}.

-spec get_load(lb_info()) -> load() | node:node_type().
get_load(#lb_info{load = Load}) -> Load.
get_db_keys(#lb_info{db_keys = Keys}) -> Keys.
%get_util(#lb_info{util = Util}) -> Util.
get_node(#lb_info{node = Node}) -> Node.
get_succ(#lb_info{succ = Succ}) -> Succ.

-spec neighbors(lb_info(), lb_info()) -> boolean().
neighbors(Node1, Node2) ->
    get_succ(Node1) =:= get_node(Node2) orelse get_succ(Node2) =:= get_node(Node1).

-spec get_target_load(slide | jump, lb_info(), lb_info()) -> non_neg_integer().
get_target_load(slide, HeavyNode, LightNode) ->
    (get_db_keys(HeavyNode) + get_db_keys(LightNode)) div 2;
get_target_load(jump, HeavyNode, _LightNode) ->
     get_db_keys(HeavyNode) div 2.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metrics       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-spec get_load_info(dht_node_state:state()) -> load_info().
get_load_metric() ->
    _Utilization =
        case config:read(lb_active_metric) of
            cpu        -> get_vm_metric(cpu10sec) / 100;
            mem        -> get_vm_metric(mem10sec) / 100;
            tx_latency -> get_dht_metric(api_tx, req_list);
            keys       -> get_dht_metric(lb_active, keys);
            _          -> log:log(warn, "~p: Falling back to default metric", [?MODULE]),
                          get_dht_metric(lb_active, keys)
        end,
    %% TODO remove this
    randoms:rand_uniform(0, 101) / 100.

-spec get_vm_metric(atom()) -> ok.
get_vm_metric(Key) ->
    get_vm_metric(lb_active, Key).

get_vm_metric(Process, Key) ->
    ClientMonitorPid = pid_groups:pid_of("clients_group", monitor),
    get_metric(ClientMonitorPid, Process, Key).

get_dht_metric(Key) ->
    MonitorPid = pid_groups:get_my(monitor),
    get_metric(MonitorPid, lb_active, Key).

get_dht_metric(Process, Key) ->
    MonitorPid = pid_groups:get_my(monitor),
    get_metric(MonitorPid, Process, Key).

get_metric(MonitorPid, Process, Key) ->
    [{Process, Key, RRD}] = monitor:get_rrds(MonitorPid, [{Process, Key}]),
    _Value = rrd:get_value_by_offset(RRD, 0).