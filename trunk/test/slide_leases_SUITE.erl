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
                               test_double_join
%                               test_triple_join
                               ]}
    ].

all() ->
    [
     {group, tester_tests},
     {group, join_tests}
     ].

suite() -> [ {timetrap, {seconds, 120}} ].

group(tester_tests) ->
    [{timetrap, {seconds, 400}}];
group(join_tests) ->
    [{timetrap, {seconds, 4}}].

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
                                                    {leases, true}]}]),
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
            {locally_disable_lease, 2},
            {find_lease, 2}
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

synchronous_join(TargetSize) ->
    api_vm:add_nodes(1),
    wait_for_ring_size(TargetSize),
    wait_for_correct_ring().

test_single_join(_Config) ->
    wait_for_ring_size(1),
    wait_for_correct_ring(),
    ct:pal("leases ~p", [get_leases()]),
    synchronous_join(2),
    true.

test_double_join(_Config) ->
    wait_for_ring_size(1),
    wait_for_correct_ring(),
    ct:pal("leases ~p", [get_leases()]),
    synchronous_join(2),
    synchronous_join(3),
    true.

test_triple_join(_Config) ->
    wait_for_ring_size(1),
    wait_for_correct_ring(),
    ct:pal("leases ~p", [get_leases()]),
    synchronous_join(2),
    synchronous_join(3),
    synchronous_join(4),
    true.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% wait helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wait_for(F) ->
    case F() of
        true ->
            ok;
        false ->
            wait_for(F);
        X ->
            ct:pal("error in wait_for ~p", [X]),
            wait_for(F)
    end.

wait_for_ring_size(Size) ->
    wait_for(fun () -> api_vm:number_of_nodes() == Size end).

wait_for_correct_ring() ->
    wait_for(fun () -> admin:check_ring() == ok end).


get_dht_node_state(Pid, What) ->
    comm:send_local(Pid, {get_state, comm:this(), What}),
    receive
        {get_state_response, Data} ->
            Data
    end.

get_leases() ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {get_state, comm:this(), lease_list}),
    receive
        {get_state_response, Leases} ->
            Leases
    end.
