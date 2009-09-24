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
%%% File    : cs_sup_standalone.erl
%%% Author  : Thorsten Schuett <schuett@csr-pc11.zib.de>
%%% Description : Supervisor for "standalone" mode
%%%
%%% Created : 17 Aug 2007 by Thorsten Schuett <schuett@csr-pc11.zib.de>
%%%-------------------------------------------------------------------
-module(cs_sup_standalone).

-behaviour(supervisor).

-include("autoconf.hrl").

%% API
-export([start_link/0, scan_environment/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
    Link = supervisor:start_link({local, main_sup}, ?MODULE, []),
    scan_environment(),
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

init([]) ->
    randoms:start(),
    inets:start(),
    %util:logger(),
    start_tcerl(),
    error_logger:logfile({open, preconfig:cs_log_file()}),
    Config =
	{config,
	 {config, start_link, [[preconfig:config(), preconfig:local_config()]]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    FailureDetector = {
      failure_detector2,
      {failuredetector2, start_link, []},
      permanent,
      brutal_kill,
      worker,
      []      
     },
    CommunicationPort = {
      comm_port,
      {comm_layer, start_link, []},
      permanent,
      brutal_kill,
      worker,
      []
     },
     Logger = 
	{logger,
	 {log, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    ChordSharp = 
	{chordsharp,
	 {cs_sup_or, start_link, []},
	 permanent,
	 brutal_kill,
	 supervisor,
	 [cs_sup_or]
     },
    YAWS = 
	{yaws,
	 {yaws_wrapper, try_link, [preconfig:docroot(),
                                   [{port, preconfig:yaws_port()}, {listen, {0,0,0,0}}],
                                   [{max_open_conns, 800}, {access_log, false}, {logdir, preconfig:log_path()}]]},
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
   Ganglia =
	{ganglia_server,
	 {ganglia, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
   MonitorTiming =
	{monitor_timing,
	 {monitor_timing, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    {ok,{{one_for_all,10,1}, [
			      Config,
                              Logger,
                              MonitorTiming,
			      FailureDetector,
			      CommunicationPort,
			      AdminServer,
			      YAWS,
			      BenchServer,
                              Ganglia,
			      ChordSharp
			     ]}}.

%%====================================================================
%% Internal functions
%%====================================================================

scan_environment() ->
    loadInstances(preconfig:cs_instances()),
    ok.

loadInstances(undefined) ->
    ok;
loadInstances(Instances) ->
    admin:add_nodes(Instances - 1).
