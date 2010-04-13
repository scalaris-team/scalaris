%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    sup_dht_node_core.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Supervisor for each DHT node that is responsible for keeping
%%%         processes running that are essential to the operation of the node.
%%%
%%%         If one of the supervised processes (dht_node, ?DB, msg_delay or
%%%         sup_dht_node_core_tx) fails, all will be re-started!
%%%         Note that the DB is needed by the dht_node (and not vice-versa) and
%%%         is thus started at first.
%%% @end
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(sup_dht_node_core).
-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/2, init/1]).

-spec start_link(instanceid(), [any()]) -> {ok, Pid::pid()} | ignore | {error, Error::{already_started, Pid::pid()} | term()}.
start_link(InstanceId, Options) ->
    supervisor:start_link(?MODULE, [InstanceId, Options]).

%% userdevguide-begin sup_dht_node_core:init
-spec init([instanceid() | [any()]]) -> {ok, {{one_for_all, MaxRetries::pos_integer(), PeriodInSeconds::pos_integer()}, [ProcessDescr::any()]}}.
init([InstanceId, Options]) ->
    process_dictionary:register_process(InstanceId, sup_dht_node_core, self()),
    Node =
        util:sup_worker_desc(dht_node, dht_node, start_link,
                             [InstanceId, Options]),
    DB =
        util:sup_worker_desc(?DB, ?DB, start_link,
                             [InstanceId]),
    Delayer =
        util:sup_worker_desc(msg_delay, msg_delay, start_link,
                             [InstanceId]),
    TX =
        util:sup_supervisor_desc(sup_dht_node_core_tx, sup_dht_node_core_tx, start_link,
                                 [InstanceId]),
    {ok, {{one_for_all, 10, 1},
          [
           DB,
           Node,
           Delayer,
           TX
          ]}}.
%% userdevguide-end sup_dht_node_core:init
