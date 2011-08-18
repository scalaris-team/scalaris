%  @copyright 2011 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Administrative functions available for external programs.
%% @end
%% @version $Id$
-module(api_vm).
-author('kruber@zib.de').
-vsn('$Id$ ').

-export([get_version/0, get_info/0,
         add_nodes/1, number_of_nodes/0, get_nodes/0,
         shutdown_node/1, shutdown_nodes/1,
         kill_node/1, kill_nodes/1,
         shutdown_vm/0, kill_vm/0]).

-include("scalaris.hrl").

%% @doc Gets the version of Scalaris.
-spec get_version() -> string().
get_version() ->
    ?SCALARIS_VERSION.

%% @doc Gets the number of Scalaris nodes inside this VM.
-spec number_of_nodes() -> non_neg_integer().
number_of_nodes() ->
    length(pid_groups:find_all(dht_node)).

%% @doc Gets the names of all Scalaris nodes inside this VM.
-spec get_nodes() -> [pid_groups:groupname()].
get_nodes() ->
    pid_groups:groups_with(dht_node).

%% @doc Adds Number Scalaris nodes to this VM.
-spec add_nodes(non_neg_integer()) -> {[pid_groups:groupname()], [{error, term()}]}.
add_nodes(0) -> {[], []};
add_nodes(Number) -> admin:add_nodes(Number).

%% @doc Sends a graceful leave request to a given node.
-spec shutdown_node(pid_groups:groupname()) -> ok | not_found.
shutdown_node(Name) ->
    case element(1, shutdown_nodes([Name])) of
        [] -> not_found;
        _  -> ok
    end.
%% @doc Sends a graceful leave request to multiple nodes.
-spec shutdown_nodes
        ([pid_groups:groupname()]) -> {Ok::[pid_groups:groupname()], NotFound::[pid_groups:groupname()]};
        (non_neg_integer())        -> {Ok::[pid_groups:groupname()], NotFound::[]}.
shutdown_nodes(Names) ->
    del_nodes(Names, true).

%% @doc Kills a given node.
-spec kill_node(pid_groups:groupname()) -> ok | not_found.
kill_node(Name) ->
    case element(1, kill_nodes([Name])) of
        [] -> not_found;
        _  -> ok
    end.
%% @doc Kills multiple nodes.
-spec kill_nodes
        ([pid_groups:groupname()]) -> {Ok::[pid_groups:groupname()], NotFound::[pid_groups:groupname()]};
        (non_neg_integer())        -> {Ok::[pid_groups:groupname()], NotFound::[]}.
kill_nodes(Names) ->
    del_nodes(Names, false).

%% @doc Gets supervisor specs for all nodes with the given names and kills them
%%      (internal function).
-spec del_nodes([pid_groups:groupname()], Graceful::boolean())
        -> {Ok::[pid_groups:groupname()], NotFound::[pid_groups:groupname()]};
               (non_neg_integer(), Graceful::boolean())
        -> {Ok::[pid_groups:groupname()], NotFound::[]}.
del_nodes(0, _Graceful) -> {[], []};
del_nodes(1, Graceful) ->
    % delete one after another for a better chance of success during concurrent executions
    AllSpecs = [{Spec, pid_groups:group_of(Pid)}
               || {_Id, Pid, _Type, _} = Spec <- supervisor:which_children(main_sup),
                  is_pid(Pid)],
    case util:random_subset(1, AllSpecs) of
        [] -> {[], []};
        [{Spec, Name} | _] ->
            case admin:del_node(Spec, Graceful) of
                ok -> {[Name], []};
                {error, not_found} -> del_nodes(1, Graceful)
            end
    end;
del_nodes(Number, Graceful) when is_integer(Number) ->
    {RestOk, []} = del_nodes(Number - 1, Graceful),
    {MyOk, []} = del_nodes(1, Graceful),
    {lists:append(MyOk, RestOk), []};
del_nodes(Names, Graceful) ->
    Results =
        [begin
             Specs = [Spec || {_Id, Pid, _Type, _} = Spec <- supervisor:which_children(main_sup),
                              is_pid(Pid),
                              pid_groups:group_of(Pid) =:= Name],
             case Specs of
                 [Spec] -> case admin:del_node(Spec, Graceful) of
                               ok -> Name;
                               {error, not_found} -> {not_found, Name}
                           end;
                 _      -> {not_found, Name}
             end
         end || Name <- Names],
    {[Name || Name <- Results, not is_tuple(Name)],
     [Name || {not_found, Name} <- Results]}.

%% @doc Graceful shutdown of this VM.
-spec shutdown_vm() -> no_return().
shutdown_vm() ->
    _ = shutdown_nodes(get_nodes()),
    % TODO: need to wait for the actual leave to finish
    timer:sleep(5000),
    erlang:halt().

%% @doc Kills this VM.
-spec kill_vm() -> no_return().
kill_vm() ->
    erlang:halt().

%% @doc Gets some information about the VM and Scalaris.
-spec get_info() -> [{scalaris_version | erlang_version, string()} |
                     {mem_total, non_neg_integer()} | {uptime, Ms::non_neg_integer()}].
get_info() ->
    [{scalaris_version, ?SCALARIS_VERSION},
     {erlang_version, erlang:system_info(otp_release)},
     {mem_total, erlang:memory(total)},
     {uptime, erlang:element(1, erlang:statistics(wall_clock))}].
