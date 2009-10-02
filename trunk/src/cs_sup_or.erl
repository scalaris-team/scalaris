%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_sup_or).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).

-include("../include/scalaris.hrl").

-export([start_link/1, start_link/0, init/1]).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link(Options) ->
    supervisor:start_link(?MODULE, [Options]).

start_link() ->
    supervisor:start_link(?MODULE, [[]]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using 
%% supervisor:start_link/[2,3], this function is called by the new process 
%% to find out about restart strategy, maximum restart frequency and child 
%% specifications.
%%--------------------------------------------------------------------
%% userdevguide-begin cs_sup_or:init
init([Options]) ->
    InstanceId = string:concat("cs_node_", randoms:getRandomId()),
    boot_server:connect(),
    KeyHolder =
	{cs_keyholder,
	 {cs_keyholder, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    _RSE =
	{rse_chord,
	 {rse_chord, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    Supervisor_AND = 
	{cs_supervisor_and,
	 {cs_sup_and, start_link, [InstanceId, Options]},
	 permanent,
	 brutal_kill,
	 supervisor,
	 []},
    RingMaintenance =
	{?RM,
	 {?RM, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    RoutingTable =
	{routingtable,
	 {util, parameterized_start_link, [rt_loop:new(config:read(routingtable_trigger)),
                                           [InstanceId]]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    DeadNodeCache = 
	{deadnodecache,
	 {dn_cache, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    Vivaldi =
        {vivaldi,
         {vivaldi, start_link, [InstanceId]},
         permanent,
         brutal_kill,
         worker,
         []},
    DC_Clustering =
        {dc_clustering,
         {dc_clustering, start_link, [InstanceId]},
         permanent,
         brutal_kill,
         worker,
         []},
     Cyclon =
	{cyclon,
	 {cyclon, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    Self_Man =
	{self_man,
	 {self_man, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    {ok, {{one_for_one, 10, 1},
	  [
           Self_Man,
	   KeyHolder,
           RoutingTable,
           Supervisor_AND,
           Cyclon,
           DeadNodeCache,
           RingMaintenance,
           Vivaldi
           %,DC_Clustering
	  
	   %_RSE
	  ]}}.
%% userdevguide-end cs_sup_or:init
