%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
-module(sup_group_node).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/1, start_link/0, init/1]).

-spec start_link([any()]) -> {ok, Pid::pid()} | ignore |
                             {error, Error::{already_started, Pid::pid()} |
                                             shutdown | term()}.
start_link(Options) ->
    supervisor:start_link(?MODULE, [Options]).

-spec start_link() -> {ok, Pid::pid()} | ignore |
                          {error, Error::{already_started, Pid::pid()} |
                           shutdown | term()}.
start_link() ->
    supervisor:start_link(?MODULE, [[]]).

%% userdevguide-begin sup_dht_node:init
-spec init([[any()]]) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                                PeriodInSeconds::pos_integer()},
                               [ProcessDescr::any()]}}.
init([Options]) ->
    InstanceId = string:concat("group_node_", randoms:getRandomId()),
    pid_groups:join_as(InstanceId, sup_dht_node),
    boot_server:connect(),
    Supervisor_AND =
        util:sup_supervisor_desc(cs_supervisor_and, sup_group_node_core, start_link,
                                 [InstanceId, Options]),
    _RingMaintenance =
        util:sup_worker_desc(?RM, ?RM, start_link, [InstanceId]),
    _RoutingTable =
        util:sup_worker_desc(routingtable, rt_loop, start_link, [InstanceId]),
    _DeadNodeCache =
        util:sup_worker_desc(deadnodecache, dn_cache, start_link, [InstanceId]),
    _Vivaldi =
        util:sup_worker_desc(vivaldi, vivaldi, start_link, [InstanceId]),
    Reregister =
        util:sup_worker_desc(dht_node_reregister, dht_node_reregister, start_link,
                             [InstanceId]),
    _DC_Clustering =
        util:sup_worker_desc(dc_clustering, dc_clustering, start_link,
                             [InstanceId]),
    _Cyclon =
        util:sup_worker_desc(cyclon, cyclon, start_link, [InstanceId]),
    _Gossip =
        util:sup_worker_desc(gossip, gossip, start_link, [InstanceId]),
    {ok, {{one_for_one, 10, 1},
          [
           Reregister,
           %RoutingTable,
           Supervisor_AND
           %Cyclon,
           %DeadNodeCache,
           %RingMaintenance
           %Vivaldi,
           %DC_Clustering,
           %Gossip
           %% _RSE
          ]}}.
%% userdevguide-end sup_dht_node:init
