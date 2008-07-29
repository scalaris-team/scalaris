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
%%% File    : cs_sup_and.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Supervisor for chord# nodes
%%%
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_sup_and.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_sup_and).

-author('schuett@zib.de').
-vsn('$Id: cs_sup_and.erl 463 2008-05-05 11:14:22Z schuett $ ').

-behaviour(supervisor).

-include("database.hrl").

-export([start_link/1, init/1]).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link(InstanceId) ->
    supervisor:start_link(?MODULE, [InstanceId]).

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
init([InstanceId]) ->
    Node =
	{cs_node,
	 {cs_node, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    DB =
	{?DB,
	 {?DB, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    {ok, {{one_for_all, 10, 1},
	  [
	   DB,
	   Node
	  ]}}.
    
