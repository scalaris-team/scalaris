%% @copyright 2011 Zuse Institute Berlin

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
%% @doc API for monitoring individual nodes and the whole ring
%% @version $Id$
-module(api_monitoring).
-author('schuett@zib.de').
-vsn('$Id$').

-export([get_node_info/0, get_node_performance/0, get_service_info/0, get_service_performance/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec get_node_info() -> list().
get_node_info() ->
    %MyMonitor = pid_groups:pid_of("clients_group", monitor),
    %statistics:getMonitorStats(MyMonitor, Keys),
    [{scalaris_version, ?SCALARIS_VERSION},
     {erlang_version, erlang:system_info(otp_release)},
     {dht_nodes, length(pid_groups:find_all(dht_node))}].

-spec get_node_performance() -> list().
get_node_performance() ->
    Monitor = pid_groups:pid_of("clients_group", monitor),
    {_CountD, _CountPerSD, AvgMsD, _MinMsD, _MaxMsD, StddevMsD, _HistMsD} =
        case statistics:getMonitorStats(Monitor, [{api_tx, 'req_list'}]) of
            []                           -> {[], [], [], [], [], [], []};
            [{api_tx, 'req_list', Data}] -> Data
        end,
    [{latency_avg, AvgMsD},
     {latency_stddev, StddevMsD}].

-spec get_service_info() -> list().
get_service_info() ->
    Ring = statistics:get_ring_details(),
    [{total_load, statistics:get_total_load(Ring)},
     {nodes, length(Ring)}].

-spec get_service_performance() -> list().
get_service_performance() ->
    Monitor = pid_groups:find_a(monitor_perf),
    {_CountD, _CountPerSD, AvgMsD, _MinMsD, _MaxMsD, StddevMsD, _HistMsD} =
        case statistics:getMonitorStats(Monitor, [{api_tx, 'req_list'}]) of
            []                           -> {[], [], [], [], [], [], []};
            [{api_tx, 'req_list', Data}] -> Data
        end,
    [{latency_avg, AvgMsD},
     {latency_stddev, StddevMsD}].
