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

%% @author Florian Schintke <schintke@zib.de>
%% @doc JSON API for accessing monitoring data.
%% @version $Id$
-module(api_json_monitor).
-author('schintke@zib.de').
-vsn('$Id$').

-export([handler/2]).

% for api_json:
-export([get_node_info/0, get_node_performance/0,
         get_service_info/0, get_service_performance/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(get_node_info, [])               -> get_node_info();
handler(get_node_performance, [])        -> get_node_performance();
handler(get_service_info, [])            -> get_service_info();
handler(get_service_performance, [])     -> get_service_performance();

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

%% interface for monitoring calls
-spec get_node_info() -> {struct, [{Key::atom(), Value::term()}]}.
get_node_info() ->
    {struct, [{status, "ok"}, {value, api_json:tuple_list_to_json(api_monitor:get_node_info())}]}.

-spec get_node_performance() -> {struct, [{Key::atom(), Value::term()}]}.
get_node_performance() ->
    {struct, [{status, "ok"}, {value, api_json:tuple_list_to_json(api_monitor:get_node_performance())}]}.

-spec get_service_info() -> {struct, [{Key::atom(), Value::term()}]}.
get_service_info() ->
    {struct, [{status, "ok"}, {value, api_json:tuple_list_to_json(api_monitor:get_service_info())}]}.

-spec get_service_performance() -> {struct, [{Key::atom(), Value::term()}]}.
get_service_performance() ->
    {struct, [{status, "ok"}, {value, api_json:tuple_list_to_json(api_monitor:get_service_performance())}]}.
