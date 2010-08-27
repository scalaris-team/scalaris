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
%% @doc    Supervisor for each DHT node that is responsible for keeping
%%         processes running that are essential to the operation of the node.
%%
%%         If one of the supervised processes (dht_node, msg_delay or
%%         sup_dht_node_core_tx) fails, all will be re-started!
%%         Note that the DB is needed by the dht_node (and not vice-versa) and
%%         is thus started at first.
%% @end
%% @version $Id$
-module(sup_dht_node_core).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).

-export([start_link/2, init/1]).

-spec start_link(pid_groups:groupname(), Options::[tuple()]) ->
                        {ok, Pid::pid()} | ignore |
                        {error, Error::{already_started, Pid::pid()} |
                                       shutdown | term()}.
start_link(DHTNodeGroup, Options) ->
    supervisor:start_link(?MODULE, {DHTNodeGroup, Options}).

%% userdevguide-begin sup_dht_node_core:init
-spec init({pid_groups:groupname(), Options::[tuple()]}) ->
                  {ok, {{one_for_all, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()},
                        [ProcessDescr::any()]}}.
init({DHTNodeGroup, Options}) ->
    pid_groups:join_as(DHTNodeGroup, ?MODULE),
    Proposer =
        util:sup_worker_desc(proposer, proposer, start_link, [DHTNodeGroup]),
    Acceptor =
        util:sup_worker_desc(acceptor, acceptor, start_link, [DHTNodeGroup]),
    Learner =
        util:sup_worker_desc(learner, learner, start_link, [DHTNodeGroup]),
    DHTNode =
        util:sup_worker_desc(dht_node, dht_node, start_link,
                             [DHTNodeGroup, Options]),
    Delayer =
        util:sup_worker_desc(msg_delay, msg_delay, start_link,
                             [DHTNodeGroup]),
    TX =
        util:sup_supervisor_desc(sup_dht_node_core_tx, sup_dht_node_core_tx, start_link,
                                 [DHTNodeGroup]),
    {ok, {{one_for_all, 10, 1},
          [
           Proposer, Acceptor, Learner,
           DHTNode,
           Delayer,
           TX
          ]}}.
%% userdevguide-end sup_dht_node_core:init
