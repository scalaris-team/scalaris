%  Copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : cs_sup_or.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Supervisor for chord# nodes
%%%
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(cs_sup_or).
-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).
-include("../include/scalaris.hrl").

-export([start_link/1, start_link/0, init/1]).

start_link(Options) ->
    supervisor:start_link(?MODULE, [Options]).
start_link() ->
    supervisor:start_link(?MODULE, [[]]).

%% userdevguide-begin cs_sup_or:init
init([Options]) ->
    InstanceId = string:concat("cs_node_", randoms:getRandomId()),
    process_dictionary:register_process(InstanceId, cs_sup_or, self()),
    boot_server:connect(),
    KeyHolder =
        util:sup_worker_desc(cs_keyholder, cs_keyholder, start_link,
                             [InstanceId]),
    Supervisor_AND =
        util:sup_supervisor_desc(cs_supervisor_and, cs_sup_and, start_link,
                                 [InstanceId, Options]),
    RingMaintenance =
        util:sup_worker_desc(?RM, util, parameterized_start_link,
                             [?RM:new(config:read(ringmaintenance_trigger)),
                              [InstanceId]]),
    RoutingTable =
        util:sup_worker_desc(routingtable, rt_loop, start_link, [InstanceId]),
    DeadNodeCache =
        util:sup_worker_desc(deadnodecache, dn_cache, start_link, [InstanceId]),
    Vivaldi =
        util:sup_worker_desc(vivaldi, vivaldi, start_link, [InstanceId]),
    CS_Reregister =
        util:sup_worker_desc(cs_reregister, cs_reregister, start_link,
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
           CS_Reregister,
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
%% userdevguide-end cs_sup_or:init
