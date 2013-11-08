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
    wait_for_ring_size(1),
    wait_for_correct_ring(),
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
    wait_for_ring_size(TargetSize),
    log:log("wait for correct ring"),
    wait_for_correct_ring(),
    log:log("wait for correct leases ~w", [TargetSize]),
    wait_for_correct_leases(TargetSize).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% leave helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

join_leave_test(JoinTargetSize, LeaveTargetSize) ->
    wait_for_ring_size(1),
    wait_for_correct_ring(),
    join_until(JoinTargetSize),
    print_all_active_leases(),
    print_all_passive_leases(),
    leave_until(JoinTargetSize, LeaveTargetSize),
    true.

leave_until(TargetSize, TargetSize) ->
    ok;
leave_until(CurrentSize, TargetSize) ->
    Group = pid_groups:group_with(dht_node),
    Node = pid_groups:pid_of(Group, dht_node),
    ct:pal("shuting down node: ~w ~w", [Group, Node]),
    ok = api_vm:shutdown_node(Group),
    wait_for_ring_size(CurrentSize - 1),
    ct:pal("have correct ring size ~w", [Node]),
    wait_for_correct_ring(),
    ct:pal("have correct ring ~w", [Node]),
    wait_for_correct_leases(CurrentSize - 1),
    ct:pal("shuting down node: success"),
    leave_until(CurrentSize - 1, TargetSize).

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
    wait_for(fun () -> ct:pal("->admin:check_ring_deep()"),
                       Res = admin:check_ring_deep(),
                       ct:pal("<-admin:check_ring_deep()"),
                       Res == ok
             end).


get_dht_node_state(Pid, What) ->
    comm:send_local(Pid, {get_state, comm:this(), What}),
    receive
        {get_state_response, Data} ->
            Data
    end.

get_all_leases() ->
    [ get_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ].

get_leases(Pid) ->
    get_dht_node_state(Pid, lease_list).

wait_for_correct_leases(TargetSize) ->
    ct:pal("running lease_checker"),
    wait_for(lease_checker(TargetSize)),
    ct:pal("have correct lease_checker"),
    wait_for(fun check_leases_per_node/0),
    ct:pal("have correct leases_per_node").

is_disjoint([]) ->
    true;
is_disjoint([H | T]) ->
    is_disjoint(H, T) andalso
        is_disjoint(T).

is_disjoint(_I, []) ->
    true;
is_disjoint(I, [H|T]) ->
    intervals:is_empty(intervals:intersection([I],[H]))
        andalso is_disjoint(I, T).

lease_checker(TargetSize) ->
    fun () ->
            LeaseLists = get_all_leases(),
            ActiveLeases  = [lease_list:get_active_lease(LL)  || LL  <- LeaseLists],
            PassiveLeases = lists:flatten([lease_list:get_passive_leases(LL) || LL <- LeaseLists]),
            ActiveIntervals =   lists:flatten(
                                  [ l_on_cseq:get_range(Lease) || Lease <- ActiveLeases, Lease =/= empty]),
            NormalizedActiveIntervals = intervals:tester_create_interval(ActiveIntervals),
            %log:log("Lease-Checker: ~w ~w ~w", [ActiveLeases, ActiveIntervals, PassiveLeases]),
            %ct:pal("ActiveIntervals: ~p", [ActiveIntervals]),
            %ct:pal("PassiveLeases: ~p", [PassiveLeases]),
            IsAll = intervals:is_all(NormalizedActiveIntervals),
            IsDisjoint = is_disjoint(ActiveIntervals),
            HaveAllActiveLeases = length(ActiveLeases) == TargetSize,
            HaveNoPassiveLeases = length(PassiveLeases) == 0,
            %case IsAll of
            %    false -> log:log("not IsAll~n");
            %    true -> ok
            %end,
            %if
            %    not IsDisjoint -> log:log("not IsDisjoint~n");
            %    true -> ok
            %end,
            %if
            %    not HaveAllActiveLeases ->
            %        log:log("not HaveAllActiveLeases: ~w ~w~n", [length(ActiveLeases), TargetSize]),
            %        log:log("~p", [ActiveLeases]);
            %    true -> ok
            %end,
            %if
            %    not HaveNoPassiveLeases -> log:log("not HaveNoPassiveLeases~n");
            %    true -> ok
            %end,
            IsAll andalso
                IsDisjoint andalso
                HaveAllActiveLeases andalso % @todo enable after garbage collection is implemented
                HaveNoPassiveLeases
    end.

check_leases_per_node() ->
    lists:all(fun (B) -> B end, [ check_local_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ]).

check_local_leases(DHTNode) ->
    LeaseList = get_dht_node_state(DHTNode, lease_list),
    ActiveLease = lease_list:get_active_lease(LeaseList),
    PassiveLeases = lease_list:get_passive_leases(LeaseList),
    %log:log("active lease: ~w", [ActiveLease]),
    ActiveInterval = case ActiveLease of
                         empty ->
                             intervals:empty();
                         _ ->
                             l_on_cseq:get_range(ActiveLease)
                     end,
    MyRange = get_dht_node_state(DHTNode, my_range),
    LocalCorrect = are_equal(MyRange, ActiveInterval),
    length(PassiveLeases) == 0 andalso LocalCorrect.


%% @doc checks whether two interval lists cover the same range
-spec are_equal(intervals:interval(), list(intervals:interval())) -> boolean().
are_equal(A, B) ->
    %log:log("are equal: ~w ~w", [A, B]),
    A =:= B.
    %A =:= lists:foldl(fun intervals:union/2, intervals:empty(), B).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
print_all_active_leases() ->
    AllLeases = get_all_leases(),
    ActiveLeases = [ lease_list:get_active_lease(LL) || LL <- AllLeases ],
    log:log("active leases: ~w", [ActiveLeases]),
    ok.

print_all_passive_leases() ->
    AllLeases = get_all_leases(),
    PassiveLeases = [ lease_list:get_passive_leases(LL) || LL <- AllLeases ],
    log:log("passive leases: ~w", [PassiveLeases]),
    ok.
