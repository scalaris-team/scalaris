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
%%% File    : boot_sup.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Supervisor for boot nodes
%%%
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: boot_sup.erl 479 2008-06-12 06:38:54Z schuett $
-module(boot_sup).

-author('schuett@zib.de').
-vsn('$Id: boot_sup.erl 479 2008-06-12 06:38:54Z schuett $ ').

-behaviour(supervisor).

-export([start_link/0, init/1]).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
    Link = supervisor:start_link({local, main_sup}, ?MODULE, []),
    cs_sup_standalone:scan_environment(),
    Link.

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
init(_Args) ->
    randoms:init(),
    crypto:start(),
    InstanceId = string:concat("boot_server_", randoms:getRandomId()),
    error_logger:logfile({open, "cs.log"}),
    inets:start(),
    FailureDetector =
	{failure_detector,
	 {failuredetector, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 [failure_detector]
     },
    Node =
	{boot_server,
	 {boot_server, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    Config =
	{config,
	 {config, start_link, [["scalaris.cfg", "scalaris.local.cfg"]]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
%%     XMLRPC = 
%% 	{boot_xmlrpc,
%% 	 {boot_xmlrpc, start_link, [InstanceId]},
%% 	 permanent,
%% 	 brutal_kill,
%% 	 worker,
%% 	 []},
    Logger = 
	{boot_logger,
	 {boot_logger, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    Collector = 
	{boot_collector,
	 {boot_collector, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   MessageStatisticsCollector = 
	{cs_message_collector,
	 {cs_message, start_link, [InstanceId]},
	 temporary, %permanent,
	 brutal_kill,
	 worker,
	 []},
   CSNode = 
	{cs_node,
	 {cs_sup_or, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    YAWS = 
	{yaws,
	 {yaws_wrapper, start_link, ["../docroot", 
				     [{listen, {0,0,0,0}}, {opaque, InstanceId}], 
				     [{max_open_conns, 800}, {access_log, false}]
				    ]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   CommPort = 
	{comm_port,
	 {comm_layer.comm_layer, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   BenchServer = 
	{bench_server,
	 {bench_server, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    {ok, {{one_for_one, 10, 1},
	  [
	   Config,
 	   CommPort,
	   MessageStatisticsCollector,
	   FailureDetector,
	   %XMLRPC,
	   Logger,
	   Collector,
	   Node,
	   YAWS,
	   BenchServer,
	   CSNode
	   ]}}.
    
