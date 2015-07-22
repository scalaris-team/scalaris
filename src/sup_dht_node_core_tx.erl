%  @copyright 2009-2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc    Supervisor for each DHT node that is responsible for keeping
%%         its transaction processes running.
%%
%%         If one of the supervised processes fails, it will be re-started!
%% @end
%% @version $Id$
-module(sup_dht_node_core_tx).
-author('schintke@zib.de').
-vsn('$Id$').

-behaviour(supervisor).

-export([start_link/1, init/1]).
-export([supspec/1, childs/1]).

-spec start_link(pid_groups:groupname())
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(DHTNodeGroup) ->
    supervisor:start_link(?MODULE, [DHTNodeGroup]).

-spec init([pid_groups:groupname()]) ->
                  {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()},
                        [ProcessDescr::supervisor:child_spec()]}}.
init([DHTNodeGroup]) ->
    pid_groups:join_as(DHTNodeGroup, ?MODULE),
    supspec([DHTNodeGroup]).

-spec supspec(any()) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()}, []}}.
supspec(_) ->
    {ok, {{one_for_one, 10, 1}, []}}.

-spec childs([pid_groups:groupname()]) ->
                    [ProcessDescr::supervisor:child_spec()].
childs([DHTNodeGroup]) ->
    RDHT_tx_read = sup:worker_desc(rdht_tx_read, rdht_tx_read, start_link,
                                        [DHTNodeGroup]),
    RDHT_tx_write = sup:worker_desc(rdht_tx_write, rdht_tx_write,
                                         start_link, [DHTNodeGroup]),
    TX_TM = sup:worker_desc(tx_tm, tx_tm_rtm, start_link,
                                 [DHTNodeGroup, tx_tm]),
    TX_TM_Paxos = sup:supervisor_desc(
                    sup_paxos_tm, sup_paxos, start_link,
                    [{DHTNodeGroup, [{sup_paxos_parent, tx_tm}]}]),


    TX_RTMs = [sup:worker_desc(
                   {tx_rtm, Id}, tx_tm_rtm,
                   start_link,
                   [DHTNodeGroup, {tx_rtm, Id}])
               || Id <- lists:seq(1, config:read(replication_factor))],

    TX_RTM_Paxi = [sup:supervisor_desc(
                {sup_paxos_rtm, Id}, sup_paxos, start_link,
                [{DHTNodeGroup, [{sup_paxos_parent, {tx_rtm, Id}}]}])
                   || Id <- lists:seq(1, config:read(replication_factor))],

    TX_TM_New = sup:worker_desc(tx_tm_new, tx_tm, start_link,
                                 [DHTNodeGroup, tx_tm_new]),
    lists:flatten([
                   RDHT_tx_read, RDHT_tx_write,
                   %% start paxos supervisors before tx processes to create used atoms
                   TX_TM_Paxos, TX_TM,
                   TX_RTM_Paxi,
                   TX_RTMs,
                   TX_TM_New
    ]).
