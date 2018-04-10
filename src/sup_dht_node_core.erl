%  @copyright 2007-2015 Zuse Institute Berlin

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

-export([start_link/2, init/1, check_config/0]).
-export([supspec/1, childs/1]).

-spec start_link(pid_groups:groupname(), Options::[tuple()]) ->
                        {ok, Pid::pid()} | ignore |
                        {error, Error::{already_started, Pid::pid()} |
                                       shutdown | term()}.
start_link(DHTNodeGroup, Options) ->
    supervisor:start_link(?MODULE, [DHTNodeGroup, Options]).

%% userdevguide-begin sup_dht_node_core:init
-spec init([{pid_groups:groupname(), Options::[tuple()]}]) ->
                  {ok, {{one_for_all, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()},
                        [ProcessDescr::supervisor:child_spec()]}}.
init([DHTNodeGroup, _Options] = X) ->
    pid_groups:join_as(DHTNodeGroup, ?MODULE),
    supspec(X).
%% userdevguide-end sup_dht_node_core:init

-spec supspec(any()) -> {ok, {{one_for_all, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()}, []}}.
supspec(_) ->
    {ok, {{one_for_all, 10, 1}, []}}.

-spec childs([{pid_groups:groupname(), Options::[tuple()]}]) ->
                    [ProcessDescr::supervisor:child_spec()].
childs([DHTNodeGroup, Options]) ->
    PaxosProcesses = sup:supervisor_desc(sup_paxos, sup_paxos,
                                              start_link, [{DHTNodeGroup, []}]),
    DHTNodeModule = config:read(dht_node),
    DHTNode = sup:worker_desc(dht_node, DHTNodeModule, start_link,
                                   [DHTNodeGroup, Options]),
    %% rbrcseq process working on the kv DB

    KV_RBRcseq = sup:worker_desc(
                   kv_db, rbrcseq,
                   start_link,
                   [DHTNodeGroup,
                    _PidGroupsNameKV = kv_db,
                    _DBSelectorKV = kv_db]),
    %% rbrcseq process working on the lease_db1 DB
    Lease_RBRcseqs = [sup:worker_desc(
                   {lease_db, Id}, rbrcseq,
                   start_link,
                   [DHTNodeGroup,
                    _PidGroupsNameL1 = {lease_db, Id},
                    _DBSelectorL1 = {lease_db, Id}])
        || Id <- lists:seq(1, config:read(replication_factor))],

    Tx_RBRcseqs = [sup:worker_desc(
                   {tx_id, Id}, rbrcseq,
                   start_link,
                   [DHTNodeGroup,
                    _PidGroupsNameL1 = {tx_id, Id},
                    _DBSelectorL1 = {tx_id, Id}])
                   || Id <- lists:seq(1, config:read(replication_factor))],

    %% crdt process  working on the kv DB
    Crdt_KVcseq = sup:worker_desc(crdt_db, crdt_proposer,
                                start_link,
                                [DHTNodeGroup,
                                 _PidGroupsNameCrdt = crdt_db,
                                 _DBSelectorCrdt = crdt_db]),

    DHTNodeMonitor = sup:worker_desc(
                       dht_node_monitor, dht_node_monitor, start_link,
                       [DHTNodeGroup, Options]),
    TX =
        sup:supervisor_desc(sup_dht_node_core_tx, sup_dht_node_core_tx, start_link,
                                 [DHTNodeGroup]),
    lists:flatten([
     PaxosProcesses,
     KV_RBRcseq,
     Lease_RBRcseqs,
     Tx_RBRcseqs,
     Crdt_KVcseq,
     DHTNodeMonitor,
     DHTNode,
     TX
    ]).


%% @doc Checks whether config parameters for the sup_dht_node_core supervisor
%%      exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_module(dht_node).
