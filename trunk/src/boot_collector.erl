%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : boot_collector.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : collects statistics on the system
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @doc collects statistics on the system
%% @version $Id: boot_collector.erl 463 2008-05-05 11:14:22Z schuett $
-module(boot_collector).

-author('schuett@zib.de').
-vsn('$Id: boot_collector.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([start_link/1, start/1, collector/1]).

mytrunc(Float) ->
    io_lib:format("~f", [Float]).

collector(InstanceId) ->
    erlang:put(instance_id, InstanceId),
    Ring = lists:filter(fun (X) -> is_valid(X) end, statistics:get_ring_details()),
    RingSize = util:lengthX(Ring),
    if 
	RingSize == 0 ->
	    log:log("logger", "empty ring"),
	    ok;
	true ->
	    TotalNodes   = util:lengthX(Ring),
	    TotalLoad    = statistics:get_total_load(Ring),
	    AverageLoad  = statistics:get_average_load(Ring),
	    LoadStdDev   = statistics:get_load_std_deviation(Ring),
	    AvRTSize     = statistics:get_average_rt_size(Ring),
	    RTSizeStdDev = statistics:get_rt_size_std_deviation(Ring),
	    MemoryUsage  = statistics:get_memory_usage(Ring),
	    MaxMemoryUsage= statistics:get_max_memory_usage(Ring),
	    cs_message:dumpMessageStatistics(lists:map(fun ({ok, X}) -> node_details:message_log(X) end, Ring)),

	    %Intervals = [{"1h", 3600}, {"1d", 24*3600}, {"1w", 7*24*3600}, {"1m", 30*24*3600}],

	    rrdtool:update("total_nodes.rrd", io_lib:format("~p", [TotalNodes])),
	    rrdtool:update("total_load.rrd", io_lib:format("~p", [TotalLoad])),
	    rrdtool:update("average_load.rrd", mytrunc(AverageLoad)),
	    rrdtool:update("average_load_std_deviation.rrd", mytrunc(LoadStdDev)),
	    rrdtool:update("average_rt_size.rrd", mytrunc(AvRTSize)),
	    rrdtool:update("average_rt_size_std_deviation.rrd", mytrunc(RTSizeStdDev)),
	    rrdtool:update("average_memory_usage.rrd", mytrunc(MemoryUsage)),
	    rrdtool:update("max_memory_usage.rrd", io_lib:format("~p", [MaxMemoryUsage])),

	    rrdtool:graph("total_nodes_1h.png",                   3600, "total_nodes.rrd:total_nodes",                     "Total Nodes"),
	    rrdtool:graph("total_load_1h.png",                    3600, "total_load.rrd:total_load",                       "Total Load"),
	    rrdtool:graph("average_load_1h.png",                  3600, "average_load.rrd:average_load",                   "Average Load"),
	    rrdtool:graph("average_load_std_deviation_1h.png",    3600, "average_load_std_deviation.rrd:av_load_std_dev",  "Average Load (Std. Deviation)"),
	    rrdtool:graph("average_rt_size_1h.png",               3600, "average_rt_size.rrd:average_rt_size",             "Average RT Size"),
	    rrdtool:graph("average_rt_size_std_deviation_1h.png", 3600, "average_rt_size_std_deviation.rrd:av_rt_std_dev", "Average RT Size (Std. Deviation)"),
	    rrdtool:graph("average_memory_usage_1h.png",          3600, "average_memory_usage.rrd:av_mem",                 "Average Memory Usage"),
	    rrdtool:graph("max_memory_usage_1h.png",              3600, "max_memory_usage.rrd:max_mem",                    "Max Memory Usage"),

	    rrdtool:graph("total_nodes_1d.png",                   86400, "total_nodes.rrd:total_nodes",                     "Total Nodes"),
	    rrdtool:graph("total_load_1d.png",                    86400, "total_load.rrd:total_load",                       "Total Load"),
	    rrdtool:graph("average_load_1d.png",                  86400, "average_load.rrd:average_load",                   "Average Load"),
	    rrdtool:graph("average_load_std_deviation_1d.png",    86400, "average_load_std_deviation.rrd:av_load_std_dev",  "Average Load (Std. Deviation)"),
	    rrdtool:graph("average_rt_size_1d.png",               86400, "average_rt_size.rrd:average_rt_size",             "Average RT Size"),
	    rrdtool:graph("average_rt_size_std_deviation_1d.png", 86400, "average_rt_size_std_deviation.rrd:av_rt_std_dev", "Average RT Size (Std. Deviation)"),
	    rrdtool:graph("average_memory_usage_1d.png",          86400, "average_memory_usage.rrd:av_mem",                 "Average Memory Usage"),
	    rrdtool:graph("max_memory_usage_1d.png",              86400, "max_memory_usage.rrd:max_mem",                    "Max Memory Usage"),

	    rrdtool:graph("total_nodes_1w.png",                   7*86400, "total_nodes.rrd:total_nodes",                     "Total Nodes"),
	    rrdtool:graph("total_load_1w.png",                    7*86400, "total_load.rrd:total_load",                       "Total Load"),
	    rrdtool:graph("average_load_1w.png",                  7*86400, "average_load.rrd:average_load",                   "Average Load"),
	    rrdtool:graph("average_load_std_deviation_1w.png",    7*86400, "average_load_std_deviation.rrd:av_load_std_dev",  "Average Load (Std. Deviation)"),
	    rrdtool:graph("average_rt_size_1w.png",               7*86400, "average_rt_size.rrd:average_rt_size",             "Average RT Size"),
	    rrdtool:graph("average_rt_size_std_deviation_1w.png", 7*86400, "average_rt_size_std_deviation.rrd:av_rt_std_dev", "Average RT Size (Std. Deviation)"),
	    rrdtool:graph("average_memory_usage_1w.png",          7*86400, "average_memory_usage.rrd:av_mem",                 "Average Memory Usage"),
	    rrdtool:graph("max_memory_usage_1w.png",              7*86400, "max_memory_usage.rrd:max_mem",                    "Max Memory Usage"),

	    rrdtool:graph("total_nodes_1m.png",                   30*86400, "total_nodes.rrd:total_nodes",                     "Total Nodes"),
	    rrdtool:graph("total_load_1m.png",                    30*86400, "total_load.rrd:total_load",                       "Total Load"),
	    rrdtool:graph("average_load_1m.png",                  30*86400, "average_load.rrd:average_load",                   "Average Load"),
	    rrdtool:graph("average_load_std_deviation_1m.png",    30*86400, "average_load_std_deviation.rrd:av_load_std_dev",  "Average Load (Std. Deviation)"),
	    rrdtool:graph("average_rt_size_1m.png",               30*86400, "average_rt_size.rrd:average_rt_size",             "Average RT Size"),
	    rrdtool:graph("average_rt_size_std_deviation_1m.png", 30*86400, "average_rt_size_std_deviation.rrd:av_rt_std_dev", "Average RT Size (Std. Deviation)"),
	    rrdtool:graph("average_memory_usage_1m.png",          30*86400, "average_memory_usage.rrd:av_mem",                 "Average Memory Usage"),
	    rrdtool:graph("max_memory_usage_1m.png",              30*86400, "max_memory_usage.rrd:max_mem",                    "Max Memory Usage")
    end.

loop() ->
    receive
	{trigger} ->
	    %io:format("starting collector: ~p~n", [timer:tc(boot_collector, collector, [erlang:get(instance_id)])]),
	    collector(erlang:get(instance_id)),
	    timer:send_after(config:collectorInterval(), {trigger}),
	    loop()
    end.

is_valid({ok, _}) ->
    true;
is_valid({failed}) ->
    false.

start(InstanceId) ->
    process_dictionary:register_process(InstanceId, boot_collector, self()),
    register(boot_collector, self()),
    timer:send_after(config:collectorInterval(), {trigger}),
    loop().

start_link(InstanceId) ->
    {ok, spawn_link(?MODULE, start, [InstanceId])}.
