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
-module(slide_leases_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{tester_tests, [sequence], [
                                 tester_type_check_slide_leases
                              ]},
     {join_tests, [sequence], [
                               test_single_join,
                               test_double_join,
                               test_triple_join,
                               test_quadruple_join
                               ]},
     {join_and_leave_tests, [sequence], [
                                         test_quadruple_join_single_leave,
                                         test_quadruple_join_double_leave,
                                         test_quadruple_join_triple_leave,
                                         test_quadruple_join_quadruple_leave
                                         ]}
    ].

all() ->
    [
     {group, tester_tests},
     {group, join_tests},
     {group, join_and_leave_tests}
     ].

suite() -> [ {timetrap, {seconds, 300}} ].

group(tester_tests) ->
    [{timetrap, {seconds, 400}}];
group(join_tests) ->
    [{timetrap, {seconds, 10}}];
group(join_and_leave_tests) ->
    [{timetrap, {seconds, 30}}].

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
            unittest_helper:make_ring(1, [{config, [{log_path, PrivDir},
                                                    {leases, true},
                                                    {leases_gc, true}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

tester_type_check_slide_leases(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %tester:register_value_creator({typedef, dht_node_state, state}, slide_leases, tester_create_dht_node_state, 0),
    %tester:register_value_creator({typedef, dht_node_state, state}, slide_leases, tester_create_slide_ops, 0),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {slide_leases,
           [
            {prepare_join_send, 2},
            {prepare_rcv_data, 2},
            {prepare_send_data1, 3},
            {prepare_send_data2, 3},
            {update_rcv_data1, 3},
            {update_rcv_data2, 3},
            {prepare_send_delta1, 3},
            {prepare_send_delta2, 3},
            {finish_delta1, 3},
            {finish_delta2, 3},
            {finish_delta_ack1, 3},
            {finish_delta_ack2, 4}
           ],
           [
            {send_continue_msg, 1},
            {find_lease, 3}
           ]}
        ],
    %% join a dht_node group to be able to call lease trigger functions
    pid_groups:join(pid_groups:group_with(dht_node)),
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    %tester:unregister_value_creator( TODO ),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% join unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


test_single_join(_Config) ->
    join_test(2).

test_double_join(_Config) ->
    join_test(3).

test_triple_join(_Config) ->
    join_test(4).

test_quadruple_join(_Config) ->
    join_test(5).

test_single_join_single_leave(_Config) ->
    join_leave_test(2, 1).

test_quadruple_join_single_leave(_Config) ->
    join_leave_test(5, 4).

test_quadruple_join_double_leave(_Config) ->
    join_leave_test(5, 3).

test_quadruple_join_triple_leave(_Config) ->
    join_leave_test(5, 2).

test_quadruple_join_quadruple_leave(_Config) ->
    join_leave_test(5, 1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% join helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

join_test(TargetSize) ->
    lease_helper:wait_for_ring_size(1),
    lease_helper:wait_for_correct_ring(),
    join_until(TargetSize),
    true.

join_until(TargetSize) ->
    joiner_helper(1, TargetSize).

joiner_helper(Target, Target) ->
    ok;
joiner_helper(Current, Target) ->
    synchronous_join(Current+1),
    joiner_helper(Current+1, Target).

synchronous_join(TargetSize) ->
    api_vm:add_nodes(1),
    log:log("wait for ring size ~w", [TargetSize]),
    lease_helper:wait_for_ring_size(TargetSize),
    log:log("wait for correct ring"),
    lease_helper:wait_for_correct_ring(),
    log:log("wait for correct leases ~w", [TargetSize]),
    lease_helper:wait_for_correct_leases(TargetSize).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% leave helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

join_leave_test(JoinTargetSize, LeaveTargetSize) ->
    lease_helper:wait_for_ring_size(1),
    lease_helper:wait_for_correct_ring(),
    join_until(JoinTargetSize),
    lease_helper:print_all_active_leases(),
    lease_helper:print_all_passive_leases(),
    leave_until(JoinTargetSize, LeaveTargetSize),
    true.

leave_until(TargetSize, TargetSize) ->
    ok;
leave_until(CurrentSize, TargetSize) ->
    Group = pid_groups:group_with(dht_node),
    Node = pid_groups:pid_of(Group, dht_node),
    ct:pal("shuting down node: ~w ~w", [Group, Node]),
    ok = api_vm:shutdown_node(Group),
    lease_helper:wait_for_ring_size(CurrentSize - 1),
    ct:pal("have correct ring size ~w", [Node]),
    lease_helper:wait_for_correct_ring(),
    ct:pal("have correct ring ~w", [Node]),
    lease_helper:wait_for_correct_leases(CurrentSize - 1),
    ct:pal("shuting down node: success"),
    leave_until(CurrentSize - 1, TargetSize).


