%% @copyright 2012-2016 Zuse Institute Berlin

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
    [{tester_tests,   [sequence], [
                                  tester_type_check_rm_leases
                              ]},
     {kill_tests,     [sequence], [
                                  test_single_kill
                               ]},
     {add_tests,      [sequence], [
                                  test_single_add,
                                  test_double_add,
                                  test_triple_add
                               ]},
     {partition_tests,[sequence], [
                                   test_network_partition
                               ]},
     {rm_loop_tests,  [sequence], [
                                  propose_new_neighbor
                                 ]},

     {repeater, [{repeat, 30}], [{group, kill_tests}   , {group, add_tests},
                                 {group, rm_loop_tests}, {group, partition_tests}]}
    ].

all() ->
    [
     {group, tester_tests},
     {group, kill_tests},
     {group, add_tests},
     {group, rm_loop_tests},
     {group, partition_tests}
     ].

suite() -> [ {timetrap, {seconds, 40}} ].

group(tester_tests) ->
    [{timetrap, {seconds, 400}}];
group(partition_tests) ->
    [{timetrap, {seconds, 400}}];
group(_) ->
    suite().

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).


init_per_testcase(TestCase, Config) ->
    case TestCase of
        test_network_partition ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            %% this test case requires an even number of nodes
            unittest_helper:make_symmetric_ring([{scale_ring_size_by, 2}, {config,
                                                                           [{log_path, PrivDir},
                                                                            {leases, true}]}]),
            ok;
        _ ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir},
                                                           {leases, true}]}]),
            ok
    end,
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

tester_type_check_rm_leases(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {rm_leases,
           [
            {start_link, 1},
            {start_gen_component,5}, %% unsupported types
            {on, 2},
            {get_takeovers, 1} %% sends messages
           ],
           [
            {compare_and_fix_rm_with_leases, 5}, %% cannot create dht_node_state (reference for bulkowner)
            {propose_new_neighbors, 1}, %% sends messages
            {prepare_takeover, 3} %% cannot create dht_node_state (reference for bulkowner)
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
    case config:read(replication_factor) < 4 of
        true ->
            log:log("skipped: this test case is likely to fail for small replication factors"),
            ok;
        false ->
            NrOfNodes = api_vm:number_of_nodes(),
            log:log("kill nodes", []),
            synchronous_kill(NrOfNodes),
            %% timer:sleep(5000), % enable to see rest of protocol
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% add unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


test_single_add(_Config) ->
    %% log:log("join nodes", []),
    log:log("add nodes", []),
    synchronous_add(config:read(replication_factor), config:read(replication_factor)+1),
    %timer:sleep(5000), % enable to see rest of protocol
    ok.

test_double_add(_Config) ->
    %% log:log("join nodes", []),
    log:log("add nodes", []),
    synchronous_add(config:read(replication_factor), config:read(replication_factor)+2),
    %timer:sleep(5000), % enable to see rest of protocol
    ok.

test_triple_add(_Config) ->
    %% log:log("join nodes", []),
    log:log("add nodes", []),
    synchronous_add(config:read(replication_factor), config:read(replication_factor)+3),
    %timer:sleep(5000), % enable to see rest of protocol
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% rm_loop unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
propose_new_neighbor(_Config) ->
    lease_helper:wait_for_ring_size(config:read(replication_factor)),
    lease_helper:wait_for_correct_ring(),
    MainNode = pid_groups:group_with(dht_node),
    % main node
    RMLeasesPid = pid_groups:pid_of(MainNode, rm_leases),
    DHTNodePid = pid_groups:pid_of(MainNode, dht_node),

    % fake death
    {_Pred, PredRange, Lease} = get_pred_info(DHTNodePid),
    Result = {qread_done, fake_reqid, fake_round, fake_old_write_round, Lease},
    Msg = {read_after_rm_change, PredRange, Result},
    TakeoversBefore = rm_leases:get_takeovers(RMLeasesPid),
    ct:pal("+wait_for_messages_after ~w", [gb_trees:to_list(TakeoversBefore)]),
    wait_for_messages_after(RMLeasesPid, [merge_after_rm_change], %get_node_for_new_neighbor],
                            fun () ->
                                    comm:send_local(RMLeasesPid, Msg)
                            end),
    ct:pal("-wait_for_messages_after ~w", [gb_trees:to_list(TakeoversBefore)]),


    AllRMMsgs = [read_after_rm_change, takeover_after_rm_change,
                 merge_after_rm_change, merge_after_leave,
                 get_node_for_new_neighbor, get_takeovers],
    ct:pal("+test_quiescence"),
    test_quiescence(RMLeasesPid, AllRMMsgs, 100),
    ct:pal("-test_quiescence"),
    TakeoversBefore = rm_leases:get_takeovers(RMLeasesPid),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% network partition unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_network_partition(_Config) ->
    %% We create a ring with an even number of nodes. For the odd
    %% nodes, we stop lease renewal and wait for the leases to time
    %% out. After that, we propose to the first (even) node to
    %% takeover the last (odd) node.

    DHTNodes = pid_groups:find_all(dht_node),
    IdsAndNodes = lists:sort(
        [
          begin
              comm:send_local(Node, {get_state, comm:this(), [node_id]}),
              receive
                  {get_state_response, [{node_id, Id}]} ->
                      {Id, Node}
              end
          end
          || Node <- DHTNodes]),
    OddNodes = iterate_even_odd(IdsAndNodes,
                                fun(Val, Even) ->
                                        case Even of
                                            true ->
                                                ok;
                                            false ->
                                                {val, Val}
                                        end
                                end),
    DHTNodePid = hd(DHTNodes),

    %% stop odd nodes
    _ = [lease_helper:intercept_lease_renew(Node) || {_Id, Node} <- OddNodes],
    lease_helper:wait_for_number_of_valid_active_leases(length(DHTNodes) div 2),

    RMLeasesPid = pid_groups:pid_of(pid_groups:group_of(DHTNodePid), rm_leases),

    %% propose takeover
    {_Pred, PredRange, Lease} = get_pred_info(DHTNodePid),
    Result = {qread_done, fake_reqid, fake_round, fake_old_write_round, Lease},
    Msg = {read_after_rm_change, PredRange, Result},
    %comm:send_local(RMLeasesPid, Msg),

    %% what do we expect to happen? takeover and merge should succeed
    wait_for_messages_after(RMLeasesPid, [merge_after_rm_change],
                            fun () ->
                                    comm:send_local(RMLeasesPid, Msg)
                            end),
    ok.

iterate_even_odd(L, F) ->
    iterate_even_odd1(L, F, true, []).

iterate_even_odd1([], _F, _Flag, Acc) ->
    Acc;
iterate_even_odd1([Val|Rest], F, Even, Acc) ->
    case F(Val, Even) of
        {val, Value} ->
            iterate_even_odd1(Rest, F, not Even, [Value|Acc]);
        _ ->
            iterate_even_odd1(Rest, F, not Even, Acc)
    end.

get_pred_info(Pid) ->
    comm:send_local(Pid, {get_state, comm:this(), neighbors}),
    Neighbors = receive
        {get_state_response, Neighbors2} -> Neighbors2
    end,
    % @todo could add fake node??!
    PredNode = nodelist:pred(Neighbors),
    PredPid = node:pidX(PredNode),
    comm:send(PredPid, {get_state, comm:this(), my_range}),
    PredRange = receive
        {get_state_response, PredRange2} -> PredRange2
    end,
    LeaseId = l_on_cseq:id(PredRange),
    {ok, Lease} = l_on_cseq:read(LeaseId),
    {PredPid, PredRange, Lease}.

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
    _ = api_vm:add_nodes(1),
    check_ring_state(TargetSize).

check_ring_state(TargetSize) ->
    lease_helper:wait_for_ring_size(TargetSize),
    lease_helper:wait_for_correct_ring(),
    lease_helper:wait_for_correct_leases(TargetSize).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% kill helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

synchronous_kill(Current) ->
    _ = api_vm:kill_nodes(1),
    ct:pal("wait for ring size"),
    lease_helper:wait_for_ring_size(Current - 1),
    ct:pal("wait for correct ring"),
    lease_helper:wait_for_correct_ring(),
    ct:pal("wait for correct leases"),
    lease_helper:wait_for_correct_leases(Current - 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% add helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

synchronous_add(Current, Current) ->
    ok;
synchronous_add(Current, _TargetSize) ->
    ct:pal("================================== adding node ========================"),
    _ = api_vm:add_nodes(1),
    ct:pal("wait for ring size"),
    lease_helper:wait_for_ring_size(Current + 1),
    ct:pal("wait for correct ring"),
    lease_helper:wait_for_correct_ring(),
    ct:pal("wait for correct leases"),
    util:wait_for(fun () -> admin:check_leases(Current + 1) end, 10000),
    %lease_helper:wait_for_correct_leases(Current + 1),
    ct:pal("================================== adding node done ========================").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intercepting and blocking
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wait_for_messages_after(Pid, Msgs, F) ->
    gen_component:bp_set_cond(Pid, watch_for_msgs_filter(self(), Msgs), wait_for_message),
    F(),
    receive
        {saw_message, _Msg} ->
            gen_component:bp_del(Pid, wait_for_message)
    end,
    ok.

test_quiescence(Pid, Msgs, Timeout) ->
    gen_component:bp_set_cond(Pid, watch_for_msgs_filter(self(), Msgs), test_quiescence),
    receive
        {saw_message, Msg} ->
            gen_component:bp_del(Pid, test_quiescence),
            ?ct_fail("expected quiescence, but got ~w", [Msg])
        after Timeout ->
                gen_component:bp_del(Pid, test_quiescence),
                ok
    end.

watch_for_msgs_filter(Pid, Msgs) ->
    fun (Message, _State) ->
            %ct:pal("saw ~w~n~w~n~w~n", [Message, lists:member(Message, Msgs), Msgs]),
            case lists:member(element(1, Message), Msgs) of
                true ->
                    comm:send_local(Pid, {saw_message, Message}),
                    false;
                _ ->
                    false
            end
    end.
