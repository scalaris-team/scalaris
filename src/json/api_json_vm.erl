%% @copyright 2012 Zuse Institute Berlin

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
%% @doc JSON API for accessing an Erlang VM.
%% @version $Id$
-module(api_json_vm).
-author('kruber@zib.de').
-vsn('$Id$').

-export([handler/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(get_version, [])                 -> get_version();
handler(get_info, [])                    -> get_info();

handler(number_of_nodes, [])             -> number_of_nodes();
handler(get_nodes, [])                   -> get_nodes();
handler(add_nodes, [Number])             -> add_nodes(Number);

handler(shutdown_node, [Name])           -> shutdown_node(Name);
handler(shutdown_nodes, [Count])         -> shutdown_nodes(Count);
handler(shutdown_nodes_by_name, [Names]) -> shutdown_nodes_by_name(Names);

handler(kill_node, [Name])               -> kill_node(Name);
handler(kill_nodes, [Count])             -> kill_nodes(Count);
handler(kill_nodes_by_name, [Names])     -> kill_nodes_by_name(Names);

handler(get_other_vms, [MaxVMs])         -> get_other_vms(MaxVMs);

handler(shutdown_vm, [])                 -> shutdown_vm();
handler(kill_vm, [])                     -> kill_vm();

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

%% interface for vm calls
-spec get_version() -> {struct, [{Key::atom(), Value::term()}]}.
get_version() ->
    {struct, [{status, "ok"}, {value, api_vm:get_version()}]}.

-spec get_info() -> {struct, [{Key::atom(), Value::term()}]}.
get_info() ->
    % need to transform node name and IP address to strings:
    Info = [case I of
                {erlang_node, Node} ->
                    {erlang_node, lists:flatten(io_lib:format("~s", [Node]))};
                {ip, {IP1, IP2, IP3, IP4}} ->
                    {ip, lists:flatten(io_lib:format("~B.~B.~B.~B", [IP1, IP2, IP3, IP4]))};
                X -> X
            end || I <- api_vm:get_info()],
    {struct, [{status, "ok"}, {value, api_json:tuple_list_to_json(Info)}]}.

-spec number_of_nodes() -> {struct, [{Key::atom(), Value::term()}]}.
number_of_nodes() ->
    {struct, [{status, "ok"}, {value, api_vm:number_of_nodes()}]}.

-spec get_nodes() -> {struct, [{Key::atom(), Value::term()}]}.
get_nodes() ->
    {struct, [{status, "ok"}, {value, {array, [pid_groups:group_to_string(Group) || Group <- api_vm:get_nodes()]}}]}.

-spec add_nodes(Number::non_neg_integer()) -> {struct, [{Key::atom(), Value::term()}]}.
add_nodes(Number) ->
    {Ok, Failed1} = api_vm:add_nodes(Number),
    Failed = [lists:flatten(io_lib:format("~p", [Reason])) || {error, Reason} <- Failed1],
    {struct, [{status, "ok"},
              {ok, {array, [pid_groups:group_to_string(Group) || Group <- Ok]}},
              {failed, {array, Failed}}]}.

-spec shutdown_node(Name::nonempty_string()) -> {struct, [{Key::atom(), Value::term()}]}.
shutdown_node(Name) ->
    {struct, [{status, erlang:atom_to_list(api_vm:shutdown_node(pid_groups:string_to_group(Name)))}]}.

-spec shutdown_nodes(Count::non_neg_integer()) -> {struct, [{Key::atom(), Value::term()}]}.
shutdown_nodes(Count) ->
    {struct, [{status, "ok"}, {ok, {array, [pid_groups:group_to_string(Group) || Group <- api_vm:shutdown_nodes(Count)]}}]}.

-spec shutdown_nodes_by_name(Names::{array, [nonempty_string()]}) -> {struct, [{Key::atom(), Value::term()}]}.
shutdown_nodes_by_name({array, Names}) ->
    {Ok, NotFound} = api_vm:shutdown_nodes_by_name(
                       [pid_groups:string_to_group(Group) || Group <- Names]),
    Ok2 = [pid_groups:group_to_string(Group) || Group <- Ok],
    NotFound2 = [pid_groups:group_to_string(Group) || Group <- NotFound],
    {struct, [{status, "ok"}, {ok, {array, Ok2}}, {not_found, {array, NotFound2}}]}.

-spec kill_node(Name::nonempty_string()) -> {struct, [{Key::atom(), Value::term()}]}.
kill_node(Name) ->
    {struct, [{status, erlang:atom_to_list(api_vm:kill_node(pid_groups:string_to_group(Name)))}]}.

-spec kill_nodes(Count::non_neg_integer()) -> {struct, [{Key::atom(), Value::term()}]}.
kill_nodes(Count) ->
    Ok = [pid_groups:group_to_string(Group) || Group <- api_vm:kill_nodes(Count)],
    {struct, [{status, "ok"}, {ok, {array, Ok}}]}.

-spec kill_nodes_by_name(Names::{array, [nonempty_string()]}) -> {struct, [{Key::atom(), Value::term()}]}.
kill_nodes_by_name({array, Names}) ->
    {Ok, NotFound} = api_vm:kill_nodes_by_name(
                       [pid_groups:string_to_group(Group) || Group <- Names]),
    Ok2 = [pid_groups:group_to_string(Group) || Group <- Ok],
    NotFound2 = [pid_groups:group_to_string(Group) || Group <- NotFound],
    {struct, [{status, "ok"}, {ok, {array, Ok2}}, {not_found, {array, NotFound2}}]}.

-spec get_other_vms(MaxVMs::pos_integer()) -> {struct, [{Key::atom(), Value::term()}]}.
get_other_vms(MaxVMs) ->
    OtherVMs = [{struct, [{erlang_node, erlang:atom_to_list(ErlNode)},
                          {ip, lists:flatten(io_lib:format("~B.~B.~B.~B", [IP1, IP2, IP3, IP4]))},
                          {port, Port},
                          {yaws_port, YawsPort}]}
               || {ErlNode, {IP1, IP2, IP3, IP4}, Port, YawsPort} <- api_vm:get_other_vms(MaxVMs)],
    {struct, [{status, "ok"}, {value, {array, OtherVMs}}]}.

-spec shutdown_vm() -> {struct, [{Key::atom(), Value::term()}]}.
shutdown_vm() ->
    Result = api_vm:shutdown_vm(),
    {struct, [{status, erlang:atom_to_list(Result)}]}.

-spec kill_vm() -> {struct, [{Key::atom(), Value::term()}]}.
kill_vm() ->
    Result = api_vm:kill_vm(),
    {struct, [{status, erlang:atom_to_list(Result)}]}.
