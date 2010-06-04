%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : ganglia.erl
%%% Author  : Marie Hoffmann <hoffmann@zib.de>
%%% Description : ganglia monitoring interface
%%%
%%% Created : 11. Nov 2008 by Marie Hoffmann <hoffmann@zib.de>
%%%-------------------------------------------------------------------
%% @author Marie Hoffmann <hoffmann@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$

-module(ganglia).
-author('hoffmann@zib.de').
-vsn('$Id$').

-export([start_link/0, start/0]).

start_link() ->
    case config:read(ganglia_enable) of
        true ->
            {ok, spawn_link(?MODULE, start, [])};
        false ->
            ignore
    end.

start() ->
    Last = erlang:now(),
    ganglia_loop(Last).

ganglia_loop(Last) ->
    % message statistics
    {Tree, _Time} = comm_logger:dump(),
    update(Tree),
    Now = erlang:now(),
    SinceLast = timer:now_diff(Now, Last),
    % transaction statistics
    Timers = monitor_timing:get_timers(),
    [update_timer(Timer, SinceLast / 1000000.0) || Timer <- Timers],
    % vivaldi statistics
    monitor_per_dht_node(fun monitor_vivaldi_errors/2, process_dictionary:find_all_groups(dht_node)),
    timer:sleep(config:read(ganglia_interval)),
    ganglia_loop(Now).

update(Tree) ->
  gmetric(both, "Erlang Processes", "int32", erlang:system_info(process_count), "Total Number"),
  gmetric(both, "Memory used by Erlang processes", "int32", erlang:memory(processes_used), "Bytes"),
  gmetric(both, "Memory used by ETS tables", "int32", erlang:memory(ets), "Bytes"),
  gmetric(both, "Memory used by atoms", "int32", erlang:memory(atom), "Bytes"),
  gmetric(both, "Memory used by binaries", "int32", erlang:memory(binary), "Bytes"),
  gmetric(both, "Memory used by system", "int32", erlang:memory(system), "Bytes"),
  DHTNodesMemoryUsage = lists:sum([element(2, erlang:process_info(P, memory))
                                  || P <- process_dictionary:find_all_dht_nodes()]),
  gmetric(both, "Memory used by dht_nodes", "int32", DHTNodesMemoryUsage, "Bytes"),
  traverse(gb_trees:iterator(Tree)).

update_timer({Timer, Count, Min, Avg, Max}, SinceLast) ->
  gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "min"])), "float", Min, "ms"),
  gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "avg"])), "float", Avg, "ms"),
  gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "max"])), "float", Max, "ms"),
  gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "tp"])), "float", Count / SinceLast, "1/s").

traverse(Iter1) ->
  case gb_trees:next(Iter1) of
    none ->
      ok;
    {Key, {Bytes, _Count}, Iter2} ->
      gmetric(positive, Key, "int32", Bytes, "Bytes"),
      traverse(Iter2)
  end.

gmetric(Slope, Metric, Type, Value, Unit) ->
    os:cmd(io_lib:format("gmetric --slope ~p --name ~p --type ~p --value ~p --units ~p~n",
                         [Slope, Metric, Type, Value, Unit])).

monitor_vivaldi_errors(Group, Idx) ->
    case process_dictionary:lookup_process(Group, vivaldi) of
        failed ->
            ok;
        Vivaldi ->
            comm:send_local(Vivaldi, {get_coordinate, comm:this()}),
            receive
                {vivaldi_get_coordinate_response, _, Error} ->
                    gmetric(both, lists:flatten(io_lib:format("vivaldi_error_~p", [Idx])), "float", Error, "error")
            end
    end.

monitor_per_dht_node(_, failed) ->
    ok;
monitor_per_dht_node(F, Nodes) ->
    DHTNodes = lists:sort(Nodes),
    lists:foldl(fun (Group, Idx) ->
                        F(Group, Idx),
                        Idx + 1
                end, 0, DHTNodes).

