%  Copyright 2008, 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%% @copyright 2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(comm_port_sup).

-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link(?MODULE, []).

init([]) ->
    InstanceId = string:concat("comm_port_", randoms:getRandomId()),
    CommPort =
        util:sup_worker_desc(comm_port, comm_port, start_link),
    CommAcceptor =
        util:sup_worker_desc(comm_acceptor, comm_acceptor, start_link,
                             [InstanceId]),
    CommLogger =
        util:sup_worker_desc(comm_logger, comm_logger, start_link),
    {ok, {{one_for_all, 10, 1},
          [
           CommPort,
           CommLogger,
           CommAcceptor
          ]}}.


