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
%% @version $Id$
-module(boot_sup).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).

-include("autoconf.hrl").

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
-ifdef(HAVE_TCERL).
start_tcerl() ->
    tcerl:start().
-else.
start_tcerl() ->
    ok.
-endif.

-ifdef(SIMULATION).
init(_Args) ->
    randoms:start(),
    InstanceId = string:concat("boot_server_", randoms:getRandomId()),
    %error_logger:logfile({open, preconfig:cs_log_file()}),
    inets:start(),
    start_tcerl(),
    _Tracer = {
      tracer,
      {tracer, start_link, []},
      permanent,
      brutal_kill,
      worker,
      []      
     },
    FailureDetector = {
      failure_detector2,
      {failuredetector2, start_link, []},
      permanent,
      brutal_kill,
      worker,
      []      
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
	 {config, start_link, [[preconfig:config(), preconfig:local_config()]]},
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
	{logger,
	 {log, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   CSNode = 
	{cs_node,
	 {cs_sup_or, start_link, [[first]]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    YAWS = 
	{yaws,
	 {yaws_wrapper, start_link, [preconfig:docroot(), 
				     [{port, preconfig:yaws_port()},{listen, {0,0,0,0}}, {opaque, InstanceId}], 
				     [{max_open_conns, 800}, {access_log, false}, {logdir, preconfig:log_path()}]
				    ]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   CommPort = 
	{comm_port,
	 {comm_layer, start_link, []},
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
   AdminServer = 
	{admin_server,
	 {admin, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},

    {ok, {{one_for_one, 10, 1},
	  [
	   Config,
	   Logger,
	   %Tracer,
	   %CommPort,
	   FailureDetector,
	   AdminServer,
	   %XMLRPC,
	   
	   Node,
	   %YAWS,
	   %BenchServer,
	   CSNode
	   ]}}.
-else.
    init(_Args) ->
    
    randoms:start(),
    InstanceId = string:concat("boot_server_", randoms:getRandomId()),
    %error_logger:logfile({open, preconfig:cs_log_file()}),
    inets:start(),
    start_tcerl(),
    _Tracer = {
      tracer,
      {tracer, start_link, []},
      permanent,
      brutal_kill,
      worker,
      []
     },
    FailureDetector = {
      failure_detector2,
      {failuredetector2, start_link, []},
      permanent,
      brutal_kill,
      worker,
      []
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
	 {config, start_link, [[preconfig:config(), preconfig:local_config()]]},
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
	{logger,
	 {log, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   CSNode =
	{cs_node,
	 {cs_sup_or, start_link, [[first]]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    YAWS =
	{yaws,
	 {yaws_wrapper, start_link, [preconfig:docroot(),
				     [{port, preconfig:yaws_port()},{listen, {0,0,0,0}}, {opaque, InstanceId}],
				     [{max_open_conns, 800}, {access_log, false}, {logdir, preconfig:log_path()}]
				    ]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   CommPort =
	{comm_port,
	 {comm_layer, start_link, []},
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
   AdminServer =
	{admin_server,
	 {admin, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},

    {ok, {{one_for_one, 10, 1},
	    [
     Config,
     Logger,
     %Tracer,
     CommPort,
     FailureDetector,
     AdminServer,
     %XMLRPC,

     Node,
     YAWS,
     BenchServer,
     CSNode
     ]}}.

-endif.