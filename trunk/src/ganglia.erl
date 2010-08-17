% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Marie Hoffmann <hoffmann@zib.de>
%% @doc Ganglia monitoring interface.
%% @end
%% @version $Id$
-module(ganglia).
-author('hoffmann@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([start_link/0, start/0]).

-type time() :: {MegaSecs::non_neg_integer(),
                 Secs::non_neg_integer(),
                 MicroSecs::non_neg_integer()}.

-spec start_link() -> {ok, pid()} | ignore.
start_link() ->
    case config:read(ganglia_enable) of
        true ->
            {ok, spawn_link(?MODULE, start, [])};
        false ->
            ignore
    end.

-spec start() -> time().
start() ->
    Last = erlang:now(),
    ganglia_loop(Last).

-spec ganglia_loop(PreviousTime::time()) -> NewTime::time().
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
    monitor_per_dht_node(fun monitor_vivaldi_errors/2, pid_groups:groups_with(dht_node)),
    timer:sleep(config:read(ganglia_interval)),
    ganglia_loop(Now).

-spec update(Tree::gb_tree()) -> ok.
update(Tree) ->
    gmetric(both, "Erlang Processes", "int32", erlang:system_info(process_count), "Total Number"),
    gmetric(both, "Memory used by Erlang processes", "int32", erlang:memory(processes_used), "Bytes"),
    gmetric(both, "Memory used by ETS tables", "int32", erlang:memory(ets), "Bytes"),
    gmetric(both, "Memory used by atoms", "int32", erlang:memory(atom), "Bytes"),
    gmetric(both, "Memory used by binaries", "int32", erlang:memory(binary), "Bytes"),
    gmetric(both, "Memory used by system", "int32", erlang:memory(system), "Bytes"),
    DHTNodesMemoryUsage = lists:sum([element(2, erlang:process_info(P, memory))
                                    || P <- pid_groups:find_all(dht_node)]),
    gmetric(both, "Memory used by dht_nodes", "int32", DHTNodesMemoryUsage, "Bytes"),
    traverse(gb_trees:iterator(Tree)).

-spec update_timer(monitor_timing:timer(), SinceLast::number()) -> ok.
update_timer({Timer, Count, Min, Avg, Max}, SinceLast) ->
    gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "min"])), "float", Min, "ms"),
    gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "avg"])), "float", Avg, "ms"),
    gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "max"])), "float", Max, "ms"),
    gmetric(both, lists:flatten(io_lib:format("~p_~s", [Timer, "tp"])), "float", Count / SinceLast, "1/s"),
    ok.

-spec traverse(Iter1::term()) -> ok.
traverse(Iter1) ->
  case gb_trees:next(Iter1) of
    none -> ok;
    {Key, {Bytes, _Count}, Iter2} ->
      gmetric(positive, Key, "int32", Bytes, "Bytes"),
      traverse(Iter2)
  end.

-spec gmetric(Slope::both | positive, Metric::string(), Type::string(), Value::number(), Unit::string()) -> string().
gmetric(Slope, Metric, Type, Value, Unit) ->
    os:cmd(io_lib:format("gmetric --slope ~p --name ~p --type ~p --value ~p --units ~p~n",
                         [Slope, Metric, Type, Value, Unit])).

-spec monitor_vivaldi_errors(pid_groups:groupname(), non_neg_integer()) -> ok | string().
monitor_vivaldi_errors(Group, Idx) ->
    case pid_groups:pid_of(Group, vivaldi) of
        failed ->
            ok;
        Vivaldi ->
            comm:send_local(Vivaldi, {get_coordinate, comm:this()}),
            receive
                {vivaldi_get_coordinate_response, _, Error} ->
                    gmetric(both, lists:flatten(io_lib:format("vivaldi_error_~p", [Idx])), "float", Error, "error")
            end
    end.

-spec monitor_per_dht_node(fun((pid_groups:groupname(), Idx::non_neg_integer()) -> any()), [pid_groups:groupname(),...] | failed) -> ok | non_neg_integer().
monitor_per_dht_node(_, failed) ->
    ok;
monitor_per_dht_node(F, Nodes) ->
    DHTNodes = lists:sort(Nodes),
    lists:foldl(fun (Group, Idx) ->
                        F(Group, Idx),
                        Idx + 1
                end, 0, DHTNodes).

