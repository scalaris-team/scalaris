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
%%% File    : ganglia.erl
%%% Author  : Marie Hoffmann <hoffmann@zib.de>
%%% Description : ganglia monitoring interface
%%%
%%% Created : 11. Nov 2008 by Marie Hoffmann <hoffmann@zib.de>
%%%-------------------------------------------------------------------
%% @author Marie Hoffmann <hoffmann@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$

-module(ganglia).
-export([start_link/0, start/0]).


start_link() ->
    case config:read(ganglia_enable) of
        true ->
            {ok, spawn_link(?MODULE, start, [])};
        false ->
            ignore
    end.

start() ->
  {Tree, _Time} = comm_layer.comm_logger:dump(),
  update(Tree),
  timer:sleep(config:read(ganglia_interval)),
  start().

update(Tree) ->
  gmetric(both, "Erlang Processes", "int32", erlang:system_info(process_count), "Total Number"),
  gmetric(both, "Memory used by Erlang processes", "int32", erlang:memory(processes_used), "Bytes"),
  gmetric(both, "Memory used by ETS tables", "int32", erlang:memory(ets), "Bytes"),
  gmetric(both, "Memory used by atoms", "int32", erlang:memory(atom), "Bytes"),
  gmetric(both, "Memory used by binaries", "int32", erlang:memory(binary), "Bytes"),
  gmetric(both, "Memory used by system", "int32", erlang:memory(system), "Bytes"),
  CSNodesMemoryUsage = lists:sum([erlang:process_info(P, [memory]) || P <- process_dictionary:find_all_cs_nodes()]),
  gmetric(both, "Memory used by cs_nodes", "int32", CSNodesMemoryUsage, "Bytes"),
  traverse(gb_trees:iterator(Tree)).

traverse(Iter1) ->
  case gb_trees:next(Iter1) of
    none ->
      ok;
    {Key, {Bytes, _Count}, Iter2} ->
      gmetric(positive, Key, "int32", Bytes, "Bytes"),
      traverse(Iter2)
  end.

gmetric(Slope, Metric, Type, Value, Unit) ->
    os:cmd(io_lib:format("gmetric --slope ~p --name ~p --type ~p --value ~p --units ~p~n", [Slope, Metric, Type, Value, Unit])).
