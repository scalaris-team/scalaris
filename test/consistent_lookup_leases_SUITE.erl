%% @copyright 2012-2013 Zuse Institute Berlin

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
%% @doc    Unit tests for consistent lookups with leases
%% @end
%% @version $Id$
-module(consistent_lookup_leases_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{send_tests, [sequence], [
                              test_consistent_send
                              ]}
    ].

all() ->
    [
     {group, send_tests}
     ].

suite() -> [ {timetrap, {seconds, 120}} ].

group(send_tests) ->
    [{timetrap, {seconds, 400}}];
group(_) ->
    suite().

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir},
                                            {leases, true}]}]),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

test_consistent_send(_Config) ->
    Ev = dht_node_lookup:envelope(3, {unittest_consistent_send, self(), '_'}),
    api_dht_raw:unreliable_lookup(?RT:hash_key("0"),
                                  Ev),
    receive
        {unittest_consistent_send, _Self, true} = Msg ->
            ct:pal("message ~p", [Msg]);
        {unittest_consistent_send, _Self, false} = Msg ->
            ct:pal("message ~p", [Msg])
    end.
