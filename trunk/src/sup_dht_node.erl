%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%%-------------------------------------------------------------------
%%% File    sup_dht_node.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Supervisor for each DHT node that is responsible for keeping
%%%         processes running that run for themselves.
%%%
%%%         If one of the supervised processes fails, only the failed process
%%%         will be re-started!
%%% @end
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(sup_dht_node).
-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/1, start_link/0, init/1]).

-spec start_link([any()]) -> {ok, Pid::pid()} | ignore | {error, Error::{already_started, Pid::pid()} | term()}.
start_link(Options) ->
    supervisor:start_link(?MODULE, [Options]).
start_link() ->
    supervisor:start_link(?MODULE, [[]]).

%% userdevguide-begin sup_dht_node:init
-spec init([[any()]]) -> {ok, {{one_for_one, MaxRetries::pos_integer(), PeriodInSeconds::pos_integer()}, [ProcessDescr::any()]}}.
init([Options]) ->
    InstanceId = string:concat("dht_node_", randoms:getRandomId()),
    process_dictionary:register_process(InstanceId, sup_dht_node, self()),
    boot_server:connect(),
    KeyHolder =
        util:sup_worker_desc(idholder, idholder, start_link,
                             [InstanceId]),
    Supervisor_AND =
        util:sup_supervisor_desc(cs_supervisor_and, sup_dht_node_core, start_link,
                                 [InstanceId, Options]),
    RingMaintenance =
        util:sup_worker_desc(?RM, ?RM, start_link, [InstanceId]),
    RoutingTable =
        util:sup_worker_desc(routingtable, rt_loop, start_link, [InstanceId]),
    DeadNodeCache =
        util:sup_worker_desc(deadnodecache, dn_cache, start_link, [InstanceId]),
    Vivaldi =
        util:sup_worker_desc(vivaldi, vivaldi, start_link, [InstanceId]),
    Reregister =
        util:sup_worker_desc(dht_node_reregister, dht_node_reregister, start_link,
                             [InstanceId]),
    DC_Clustering =
        util:sup_worker_desc(dc_clustering, dc_clustering, start_link,
                             [InstanceId]),
    Cyclon =
        util:sup_worker_desc(cyclon, cyclon, start_link, [InstanceId]),
    Gossip =
        util:sup_worker_desc(gossip, gossip, start_link, [InstanceId]),
    Self_Man =
        util:sup_worker_desc(self_man, self_man, start_link, [InstanceId]),
    {ok, {{one_for_one, 10, 1},
          [
           Self_Man,
           Reregister,
           KeyHolder,
           RoutingTable,
           Supervisor_AND,
           Cyclon,
           DeadNodeCache,
           RingMaintenance,
           Vivaldi,
           DC_Clustering,
		   Gossip
           %% _RSE
          ]}}.
%% userdevguide-end sup_dht_node:init
