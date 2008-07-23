%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : comm_port_sup.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created : 04 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: comm_port_sup.erl 463 2008-05-05 11:14:22Z schuett $
-module(comm_layer.comm_port_sup).

-author('schuett@zib.de').
-vsn('$Id: comm_port_sup.erl 463 2008-05-05 11:14:22Z schuett $ ').

-behaviour(supervisor).

-import(supervisor).
-import(randoms).
-import(string).
-import(config).

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
    InstanceId = string:concat("comm_port_", randoms:getRandomId()),
    Config =
        {config2,
         {config, start_link, [["scalaris.cfg", "scalaris.local.cfg"], InstanceId]},
         permanent,
         brutal_kill,
         worker,
         []},
    CommPort =
	{comm_port,
	 {comm_layer.comm_port, start_link, []},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    CommAcceptor =
	{comm_acceptor,
	 {comm_layer.comm_acceptor, start_link, [InstanceId]},
	 permanent,
	 brutal_kill,
	 worker,
	 []},
    {ok, {{one_for_all, 10, 1},
	  [
	   Config,
	   CommPort,
	   CommAcceptor
	  ]}}.
    

