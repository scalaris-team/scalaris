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
%% @doc    Unit tests for slide_leases
%% @end
%% @version $Id$
-module(rm_leases_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{tester_tests, [sequence], [
                                 tester_type_check_rm_leases
                              ]},
     {kill_tests, [sequence], [
                               test_single_kill
                               ]}
    ].

all() ->
    [
     {group, tester_tests},
     {group, kill_tests}
     ].

suite() -> [ {timetrap, {seconds, 400}} ].

group(tester_tests) ->
    [{timetrap, {seconds, 400}}];
group(kill_tests) ->
    [{timetrap, {seconds, 20}}].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir},
                                                    {leases, true},
                                                    {leases_gc, true}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

tester_type_check_rm_leases(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {rm_leases,
           [
            {start_link, 1},
            {on, 2}
           ],
           [
            {compare_and_fix_rm_with_leases, 1} %% cannot create dht_node_state (reference for bulkowner)
           ]}
        ],
    %% join a dht_node group to be able to call lease trigger functions
    pid_groups:join(pid_groups:group_with(dht_node)),
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% kill unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


test_single_kill(_Config) ->
%    log:log("join nodes", []),
    join_test(4, 5),
    log:log("kill nodes", []),
    synchronous_kill(5, 4),
    timer:sleep(5000),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% join helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

join_test(Current, TargetSize) ->
    lease_helper:wait_for_ring_size(Current),
    lease_helper:wait_for_correct_ring(),
    join_until(Current, TargetSize),
    true.

join_until(Current, TargetSize) ->
    joiner_helper(Current, TargetSize).

joiner_helper(Target, Target) ->
    ok;
joiner_helper(Current, Target) ->
    synchronous_join(Current+1),
    joiner_helper(Current+1, Target).

synchronous_join(TargetSize) ->
    api_vm:add_nodes(1),
    lease_helper:wait_for_ring_size(TargetSize),
    lease_helper:wait_for_correct_ring(),
    lease_helper:wait_for_correct_leases(TargetSize).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% kill helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

synchronous_kill(_Current, TargetSize) ->
    api_vm:kill_nodes(1),
    ct:pal("wait for ring size"),
    lease_helper:wait_for_ring_size(TargetSize),
    ct:pal("wait for correct ring"),
    lease_helper:wait_for_correct_ring(),
    ct:pal("wait for correct leases"),
    lease_helper:wait_for_correct_leases(TargetSize).

