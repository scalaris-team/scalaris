%  @copyright 2007-2017 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Supervisor for each DHT node that is responsible for keeping
%%         processes running that run for themselves.
%%
%%         If one of the supervised processes fails, only the failed process
%%         will be re-started!
%% @end
%% @version $Id$
-module(sup_dht_node).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/1, init/1]).
-export([supspec/1, childs/1]).

-spec start_link({DHTNodeGroup::pid_groups:groupname(), Options::[tuple()]})
        -> {ok, Pid::pid(), pid_groups:groupname()} | ignore |
               {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link({DHTNodeGroup, Options}) ->
    case supervisor:start_link(?MODULE, [{DHTNodeGroup, Options}]) of
        {ok, Pid} -> {ok, Pid, DHTNodeGroup};
        X         -> X
    end.

%% userdevguide-begin sup_dht_node:init
-spec init([{pid_groups:groupname(), [tuple()]}])
        -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                  PeriodInSeconds::pos_integer()}, []}}.
init([{DHTNodeGroup, _Options}] = X) ->
    pid_groups:join_as(DHTNodeGroup, ?MODULE),
    supspec(X).
%% userdevguide-end sup_dht_node:init

-spec supspec(any()) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()}, []}}.
supspec(_) ->
    {ok, {{one_for_one, 10, 1}, []}}.

-spec childs([{pid_groups:groupname(), Options::[tuple()]}]) ->
                    [ProcessDescr::supervisor:child_spec()].
childs([{DHTNodeGroup, Options}]) ->
    Autoscale =
        case config:read(autoscale) of
            true -> sup:worker_desc(autoscale, autoscale, start_link, [DHTNodeGroup]);
            _ -> []
        end,
    DBValCache =
        sup:worker_desc(dht_node_db_cache, dht_node_db_cache, start_link,
                             [DHTNodeGroup]),
    DC_Clustering =
        sup:worker_desc(dc_clustering, dc_clustering, start_link,
                             [DHTNodeGroup]),
    DeadNodeCache =
        sup:worker_desc(deadnodecache, dn_cache, start_link,
                             [DHTNodeGroup]),
    Delayer =
        sup:worker_desc(msg_delay, msg_delay, start_link,
                             [DHTNodeGroup]),
    Gossip =
        sup:worker_desc(gossip, gossip, start_link, [DHTNodeGroup]),

    VivaldiLatency =
        sup:worker_desc(vivaldi_latency, vivaldi_latency, start_link, [DHTNodeGroup]),

    GossipCyclonFeeder =
        sup:worker_desc(gossip_cyclon_feeder, gossip_cyclon_feeder, start_link, [DHTNodeGroup]),

    LBActive =
        case config:read(lb_active) of
            true -> sup:worker_desc(lb_active, lb_active, start_link, [DHTNodeGroup]);
            _ -> []
        end,
    Reregister =
        sup:worker_desc(dht_node_reregister, dht_node_reregister,
                             start_link, [DHTNodeGroup]),
    RMLeases =
        sup:worker_desc(rm_leases, rm_leases,
                             start_link, [DHTNodeGroup]),
    RoutingTable =
        sup:worker_desc(routing_table, rt_loop, start_link,
                             [DHTNodeGroup]),
    SupDHTNodeCore_AND =
        sup:supervisor_desc(sup_dht_node_core, sup_dht_node_core,
                                 start_link, [DHTNodeGroup, Options]),
    %% SupMr = case config:read(mr_enable) of
    %%     true ->
    %%         util:sup_supervisor_desc(sup_mr, sup_mr, start_link, [DHTNodeGroup]);
    %%     _ -> []
    %% end,
    Monitor =
        sup:worker_desc(monitor, monitor, start_link, [DHTNodeGroup]),
    RepUpdate =
        case config:read(rrepair_enabled) of
            true -> sup:worker_desc(rrepair, rrepair, start_link, [DHTNodeGroup]);
            _ -> []
        end,
    SnapshotLeader =
        sup:worker_desc(snapshot_leader, snapshot_leader, start_link,
                        [DHTNodeGroup]),
    SupWPool =
        sup:supervisor_desc(sup_wpool, sup_wpool, start_link, [DHTNodeGroup]),
    WPool = sup:worker_desc(wpool, wpool, start_link, [DHTNodeGroup, Options]),

    lists:flatten([ %% RepUpd may be [] and lists:flatten eliminates this
                    Monitor,
                    Delayer,
                    Reregister,
                    DBValCache,
                    DeadNodeCache,
                    RoutingTable,
                    DC_Clustering,
                    VivaldiLatency,
                    Gossip,
                    GossipCyclonFeeder,
                    SnapshotLeader,
                    SupWPool,
                    WPool,
                    SupDHTNodeCore_AND,
                    RepUpdate,
                    Autoscale,
                    RMLeases,
                    LBActive
           ]).
