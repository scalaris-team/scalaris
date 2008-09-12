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
%% @version $Id: cs_sup_or.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_sup_or).

-author('schuett@zib.de').
-vsn('$Id: cs_sup_or.erl 463 2008-05-05 11:14:22Z schuett $ ').

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
    supervisor:start_link(?MODULE, []).

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
init([]) ->
    randoms:init(),
    InstanceId = string:concat("cs_node_", randoms:getRandomId()),
    FailureDetector =
	{failure_detector,
	 {failuredetector, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 [failure_detector]
     },
    KeyHolder =
	{cs_keyholder,
	 {cs_keyholder, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    MessageStatisticsCollector = 
	{cs_message_collector,
	 {cs_message, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    Supervisor_AND = 
	{cs_supervisor_and,
	 {cs_sup_and, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 supervisor,
	 []},
%    XMLRPC = 
%	{cs_xmlrpc,
%	 {cs_xmlrpc, start_link, []},
%	 permanent,
%	 brutal_kill,
%	 worker,
%	 []},
    {ok, {{one_for_one, 10, 1},
	  [
%	   XMLRPC,
	   KeyHolder,
	   MessageStatisticsCollector,
	   FailureDetector,
	   Supervisor_AND
	  ]}}.
    
