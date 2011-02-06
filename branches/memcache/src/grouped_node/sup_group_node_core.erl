%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @version $Id$
-module(sup_group_node_core).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/2, init/1]).

-spec start_link(pid_groups:groupname(), [any()]) -> {ok, Pid::pid()} | ignore |
                                           {error, Error::{already_started, Pid::pid()} |
                                                           shutdown | term()}.
start_link(InstanceId, Options) ->
    supervisor:start_link(?MODULE, [InstanceId, Options]).

%% userdevguide-begin sup_group_node_core:init
-spec init([pid_groups:groupname() | [any()]]) -> {ok, {{one_for_all, MaxRetries::pos_integer(),
                                               PeriodInSeconds::pos_integer()},
                                              [ProcessDescr::any()]}}.
init([InstanceId, Options]) ->
    pid_groups:join_as(InstanceId, sup_dht_node_core),
    Proposer =
        util:sup_worker_desc(proposer, proposer, start_link, [InstanceId]),
    Acceptor =
        util:sup_worker_desc(acceptor, acceptor, start_link, [InstanceId]),
    Learner =
        util:sup_worker_desc(learner, learner, start_link, [InstanceId]),
    Node =
        util:sup_worker_desc(group_node, group_node, start_link,
                             [InstanceId, Options]),
    Delayer =
        util:sup_worker_desc(msg_delay, msg_delay, start_link,
                             [InstanceId]),
    _TX =
        util:sup_supervisor_desc(sup_dht_node_core_tx, sup_dht_node_core_tx, start_link,
                                 [InstanceId]),
    {ok, {{one_for_all, 10, 1},
          [
           Proposer, Acceptor, Learner,
           Node,
           Delayer
           %TX
          ]}}.
%% userdevguide-end sup_group_node_core:init
