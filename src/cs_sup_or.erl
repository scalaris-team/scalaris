%  Copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%% @copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
    boot_server:connect(),
    KeyHolder =
        util:sup_worker_desc(cs_keyholder, cs_keyholder, start_link,
                             [InstanceId]),
    _RSE =
        util:sup_worker_desc(rse_chord, rse_chord, start_link,
                             [InstanceId]),
    Supervisor_AND =
        util:sup_supervisor_desc(cs_supervisor_and, cs_sup_and, start_link,
                                 [InstanceId, Options]),
    RingMaintenance =
        util:sup_worker_desc(?RM, util, parameterized_start_link,
                             [?RM:new(config:read(ringmaintenance_trigger)),
                              [InstanceId]]),
    RoutingTable =
        util:sup_worker_desc(routingtable, util, parameterized_start_link,
                             [rt_loop:new(config:read(routingtable_trigger)),
                              [InstanceId]]),
    DeadNodeCache =
        util:sup_worker_desc(deadnodecache, util, parameterized_start_link,
                             [dn_cache:new(config:read(dn_cache_trigger)),
                              [InstanceId]]),
    Vivaldi =
        util:sup_worker_desc(vivaldi, util, parameterized_start_link,
                             [vivaldi:new(config:read(vivaldi_trigger)),
                              [InstanceId]]),
    CS_Reregister =
        util:sup_worker_desc(cs_reregister, util, parameterized_start_link,
                             [cs_reregister:new(config:read(cs_reregister_trigger)),
                              [InstanceId]]),
    DC_Clustering =
        util:sup_worker_desc(dc_clustering, dc_clustering, start_link,
                             [InstanceId]),
    Cyclon =
        util:sup_worker_desc(cyclon, util, parameterized_start_link,
                             [cyclon:new(config:read(cyclon_trigger)),
                              [InstanceId]]),
    Self_Man =
        util:sup_worker_desc(self_man, self_man, start_link,
                             [InstanceId]),
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
           Vivaldi
           %% ,DC_Clustering
           %% _RSE
          ]}}.
%% userdevguide-end cs_sup_or:init
