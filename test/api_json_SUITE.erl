%  @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc Unit tests for the JSON-API
%% @end
%% @version $Id$
-module(api_json_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').
-compile(export_all).

-include("unittest.hrl").

all()   -> [get_node_info, get_node_performance, get_service_info, get_service_performance].
suite() -> [ {timetrap, {seconds, 120}} ].

init_per_suite(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    Config.

end_per_suite(_Config) ->
    ok.

get_node_info(_Config) ->
     {struct, [{status, "ok"}, {value, {struct, Value}}]} = api_json:handler(get_node_info, []),
    ?assert(erlang:is_list(Value)),
    ok.

get_node_performance(_Config) ->
     {struct, [{status, "ok"}, {value, {struct, Value}}]} = api_json:handler(get_node_performance, []),
    ?assert(erlang:is_list(Value)),
    ok.

get_service_info(_Config) ->
     {struct, [{status, "ok"}, {value, {struct, Value}}]} = api_json:handler(get_service_info, []),
    ?assert(erlang:is_list(Value)),
    ok.

get_service_performance(_Config) ->
     {struct, [{status, "ok"}, {value, {struct, Value}}]} = api_json:handler(get_service_performance, []),
    ?assert(erlang:is_list(Value)),
    ok.
