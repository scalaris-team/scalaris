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
%%% File    : cs_sup_and.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Supervisor for chord# nodes
%%%
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_sup_and).
-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).
-include("../include/scalaris.hrl").

-export([start_link/2, init/1]).

start_link(InstanceId, Options) ->
    supervisor:start_link(?MODULE, [InstanceId, Options]).

%% userdevguide-begin cs_sup_and:init
init([InstanceId, Options]) ->
    Node =
        util:sup_worker_desc(cs_node, cs_node, start_link,
                             [InstanceId, Options]),
    DB =
        util:sup_worker_desc(?DB, ?DB, start_link,
                             [InstanceId]),
    {ok, {{one_for_all, 10, 1},
          [
           DB,
           Node
          ]}}.
%% userdevguide-end cs_sup_and:init

