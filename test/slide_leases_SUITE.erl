%% @copyright 2012-2017 Zuse Institute Berlin

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
                                         test_quadruple_join_single_leave%,
                                         %%  the following tests can
                                         %%  run into timeouts if the
                                         %%  majority of replicas of a
                                         %%  lease have left
                                         %%  %test_quadruple_join_double_leave,
                                         %%  %test_quadruple_join_triple_leave,
                                         %%  %test_quadruple_join_quadruple_leave
                                         ]},
     {repeater, [{repeat, 30}], [{group, join_tests}]}
    ].

all() ->
    [
     {group, tester_tests},
     {group, join_tests},
     {group, join_and_leave_tests}%,
     %% {group, repeater}
     ].

suite() -> [ {timetrap, {seconds, 300}} ].

group(tester_tests) ->
    [{timetrap, {seconds, 400}}];
group(join_tests) ->
    [{timetrap, {seconds, 10}}];
group(join_and_leave_tests) ->
    [{timetrap, {seconds, 30}}];
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

tester_type_check_slide_leases(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    tester:register_value_creator({typedef, prbr, write_filter, []},
                                  prbr, tester_create_write_filter, 1),
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

    tester:unregister_value_creator({typedef, prbr, write_filter, []}),
    %% tester:unregister_value_creator( TODO ),
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
    %% AllDHTNodes = pid_groups:find_all(dht_node),
    %% _ = add_bp_on_successful_split(),
    %% _ = add_bp_on_successful_handover(),
    _ = api_vm:add_nodes(1),
    %% log:log("wait for successful split"),
    %% wait_for_successful_split(),
    %% _ = delete_bp_on_successful_split(AllDHTNodes),
    %% log:log("wait for successful handover"),
    %% wait_for_successful_handover(),
    %% _ = delete_bp_on_successful_handover(AllDHTNodes),
    log:log("wait for ring size ~w", [TargetSize]),
    lease_helper:wait_for_ring_size(TargetSize),
    log:log("wait for ring to stabilize in sync. join"),
    lease_checker2:wait_for_clean_leases(200, [{ring_size, TargetSize}]).
    %% util:wait_for(fun admin:check_leases/0, 10000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% leave helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

join_leave_test(JoinTargetSize, LeaveTargetSize) ->
    lease_helper:wait_for_ring_size(1),
    log:log("wait for ring to stabilize"),
    util:wait_for(fun admin:check_leases/0, 10000),
    join_until(JoinTargetSize),
    log:log("wait for ring to stabilize after join until"),
    util:wait_for(fun admin:check_leases/0, 10000),
    lease_helper:print_all_active_leases(),
    lease_helper:print_all_passive_leases(),
    leave_until(JoinTargetSize, LeaveTargetSize),
    true.

leave_until(TargetSize, TargetSize) ->
    ok;
leave_until(CurrentSize, TargetSize) ->
    Node = lease_checker:get_random_save_node(),
    case Node of
        failed ->
            ok;
        _ ->
            Group = pid_groups:group_of(comm:make_local(Node)),
            ct:pal("shuting down node: ~p ~w", [Group, Node]),
            ok = api_vm:kill_node(Group),
            lease_helper:wait_for_ring_size(CurrentSize - 1),
            ct:pal("wait for ring to stabilize after shutdown"),
            util:wait_for(fun admin:check_leases/0, 10000),
            ct:pal("shuting down node: success"),
            leave_until(CurrentSize - 1, TargetSize)
    end.

%% listen_for_split(Pid) ->
%%     fun (Message, _State) ->
%%             case Message of
%%                 {l_on_cseq, split_reply_step4, _L1, _R1, _R2, _Keep, _ReplyTo, _PostAux,
%%                  {qwrite_done, _ReqId, _Round, _L2}} ->
%%                     comm:send_local(Pid, {successful_split}),
%%                     false;
%%                 {l_on_cseq, split_reply_step4, _L1, _R1, _R2, _Keep, _ReplyTo, _PostAux,
%%                  {qwrite_deny, _ReqId, _Round, _L2, {content_check_failed,
%%                                                     {Reason, _Current, _Next}}}} ->
%%                     log:log("split failed: ~w", [Reason]),
%%                     false;
%%                 _ ->
%%                     false
%%             end
%%     end.
%% 
%% listen_for_handover(Pid) ->
%%     fun (Message, _State) ->
%%             case Message of
%%                 {l_on_cseq, handover_reply, {qwrite_done, _ReqId, _Round, _Value},
%%                  _ReplyTo, _NewOwner, _New} ->
%%                     comm:send_local(Pid, {successful_handover}),
%%                     false;
%%                 {l_on_cseq, handover_reply,
%%                  {qwrite_deny, _ReqId, _Round, _Value,
%%                   {content_check_failed, {Reason, _Current, _Next}}},
%%                  _ReplyTo, _NewOwner, _New} ->
%%                     log:log("handover failed: ~w", [Reason]),
%%                     false;
%%                 _ ->
%%                     false
%%             end
%%     end.
%% 
%% add_bp_on_successful_split() ->
%%     [gen_component:bp_set_cond(Node, listen_for_split(self()), listen_split) ||
%%         Node <- pid_groups:find_all(dht_node)].
%% 
%% wait_for_successful_split() ->
%%     receive
%%         {successful_split} ->
%%             ok
%%     end.
%% 
%% delete_bp_on_successful_split(Nodes) ->
%%     [gen_component:bp_del(Node, listen_split) || Node <- Nodes].
%% 
%% add_bp_on_successful_handover() ->
%%     [gen_component:bp_set_cond(Node, listen_for_handover(self()), listen_handover) ||
%%         Node <- pid_groups:find_all(dht_node)].
%% 
%% wait_for_successful_handover() ->
%%     receive
%%         {successful_handover} ->
%%             ok
%%     end.

delete_bp_on_successful_handover(Nodes) ->
    [gen_component:bp_del(Node, listen_handover) || Node <- Nodes].
