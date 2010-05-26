%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  and onScale solutions GmbH
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
%%% File    sup_dht_node_core_tx.erl
%%% @author Florian Schintke <schintke@zib.de>
%%% @doc    Supervisor for each DHT node that is responsible for keeping
%%%         its transaction processes running.
%%%
%%%         If one of the supervised processes fails, all will be re-started!
%%% @end
%%% Created : 1 Dec 2009 by Florian Schintke <schintke@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(sup_dht_node_core_tx).
-author('schintke@zib.de').
-vsn('$Id$').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/1, init/1]).

-spec start_link(instanceid()) -> {ok, Pid::pid()} | ignore | {error, Error::{already_started, Pid::pid()} | term()}.
start_link(InstanceId) ->
    supervisor:start_link(?MODULE, [InstanceId]).

-spec init([instanceid()]) -> {ok, {{one_for_all, MaxRetries::pos_integer(), PeriodInSeconds::pos_integer()}, [ProcessDescr::any()]}}.
init([InstanceId]) ->
    process_dictionary:register_process(InstanceId, sup_dht_node_core_tx, self()),
    RDHT_tx_read =
        util:sup_worker_desc(rdht_tx_read, rdht_tx_read, start_link, [InstanceId]),
    RDHT_tx_write =
        util:sup_worker_desc(rdht_tx_write, rdht_tx_write, start_link, [InstanceId]),
    TX_TM =
        util:sup_worker_desc(tx_tm, tx_tm_rtm, start_link, [InstanceId, tx_tm]),
    TX_RTM0 =
        util:sup_worker_desc(tx_rtm0, tx_tm_rtm, start_link, [InstanceId, tx_rtm0]),
    TX_RTM1 =
        util:sup_worker_desc(tx_rtm1, tx_tm_rtm, start_link, [InstanceId, tx_rtm1]),
    TX_RTM2 =
        util:sup_worker_desc(tx_rtm2, tx_tm_rtm, start_link, [InstanceId, tx_rtm2]),
    TX_RTM3 =
        util:sup_worker_desc(tx_rtm3, tx_tm_rtm, start_link, [InstanceId, tx_rtm3]),
    {ok, {{one_for_all, 10, 1},
          [
           RDHT_tx_read, RDHT_tx_write,
           TX_TM, TX_RTM0, TX_RTM1, TX_RTM2, TX_RTM3
          ]}}.
