% @copyright 2008-2015 Zuse Institute Berlin

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
%% @author Nico Kruber <kruber@zib.de>
%% @author Florian Schintke <schintke@zib.de>
%% @doc : Unit tests for transactions under churn
%% @end
%% @version $Id$
-module(churn_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

test_cases() ->
    [
     transactions_1_failure_4_nodes_read,
     transactions_2_failures_4_nodes_read,
     transactions_3_failures_4_nodes_read,
     transactions_1_failure_4_nodes_networksplit_write,
     transactions_2_failures_4_nodes_networksplit_write,
     transactions_3_failures_4_nodes_networksplit_write
    ].

all() ->
%%     unittest_helper:create_ct_all(test_cases()).
    test_cases().

groups() ->
%%     unittest_helper:create_ct_groups(test_cases(), [{transactions_1_failure_4_nodes_read, [sequence, {repeat_until_any_fail, forever}]}]).
    [].

suite() -> [ {timetrap, {seconds, 30}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(TestCase, Config) ->
    case TestCase of
        %% the ring maintenance fixes network split situations, which
        %% can lead to two separate rings and consistency violation.
        transactions_2_failures_4_nodes_networksplit_write ->
            {skip, "ring maint. cannot handle network split yet - see issue 59"};
        transactions_3_failures_4_nodes_networksplit_write ->
            {skip, "ring maint. cannot handle network split yet - see issue 59"};
        _ ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_symmetric_ring(
              [{config, [{log_path, PrivDir}, {rrepair_after_crash, false},
                         {replication_factor, 4}]}]),
            unittest_helper:check_ring_size_fully_joined(4),
            [{stop_ring, true} | Config]
%%             {skip, "temporarily"}
    end.

end_per_testcase(_TestCase, _Config) ->
    ok.

transactions_1_failure_4_nodes_read(_) ->
    transactions_X_failures_4_nodes_read(1, ok, ok).

transactions_2_failures_4_nodes_read(_) ->
    transactions_X_failures_4_nodes_read(2, ok_or_abort, ok).

transactions_3_failures_4_nodes_read(_) ->
    transactions_X_failures_4_nodes_read(3, abort, ok_or_abort).

-spec transactions_X_failures_4_nodes_read(
        FailedNodes::1..3, RAfterFail::ok | abort | ok_or_abort,
        WAfterFail::ok | abort | ok_or_abort) -> true.
transactions_X_failures_4_nodes_read(FailedNodes, RAfterFail, WAfterFail) ->
    ?equals_w_note(api_tx:write("0", 1), {ok}, "write_0_a"),
    ?equals_w_note(api_tx:read("0"), {ok, 1}, "read_0_a"),
    % wait for late write messages to arrive at the original nodes
    api_tx_SUITE:wait_for_dht_entries(4),
    _ = api_vm:kill_nodes(FailedNodes),
    unittest_helper:check_ring_size(4 - FailedNodes),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:wait_for_stable_ring_deep(),
    
    RAfterFailRes = api_tx:read("0"),
    case RAfterFail of
        ok    -> ?equals_w_note(RAfterFailRes, {ok, 1}, "read_0_b");
        abort -> ?equals_w_note(RAfterFailRes, {fail, not_found}, "read_0_b");
        ok_or_abort -> ok
    end,
    
    WAfterFailRes = api_tx:write("0", 2),
    case WAfterFail of
        ok    -> ?equals_w_note(WAfterFailRes, {ok}, "write_0_b");
        abort -> ?equals_w_note(WAfterFailRes, {fail, abort, ["0"]}, "write_0_b");
        ok_or_abort -> ok
    end,

    RAfterWAfterFailRes = api_tx:read("0"),
    case WAfterFailRes of
        {ok} -> ?equals_w_note(RAfterWAfterFailRes, {ok, 2}, "read_0_c");
        _    -> ?equals_w_note(RAfterWAfterFailRes, {fail, not_found}, "read_0_c")
    end.

transactions_1_failure_4_nodes_networksplit_write(_) ->
    % pause some dht_node:
    Node = pid_groups:find_a(sup_dht_node),
    PauseSpec = pause_node(Node),
    unittest_helper:check_ring_size(3),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:wait_for_stable_ring_deep(),

    ct:pal("attempting write_0_a~n"),
    ?equals_w_note(api_tx:write("0", 1), {ok}, "write_0_a"),
    ct:pal("attempting read_0_a~n"),
    ?equals_w_note(api_tx:read("0"), {ok, 1}, "read_0_a"),

    unpause_node(PauseSpec),

    ct:pal("attempting read_0_b~n"),
    ?equals_w_note(api_tx:read("0"), {ok, 1}, "read_0_b"),
    ok.

transactions_2_failures_4_nodes_networksplit_write(_) ->
    transactions_more_failures_4_nodes_networksplit_write(2).

transactions_3_failures_4_nodes_networksplit_write(_) ->
    transactions_more_failures_4_nodes_networksplit_write(3).

-spec transactions_more_failures_4_nodes_networksplit_write(FailedNodes::2 | 3) -> ok.
transactions_more_failures_4_nodes_networksplit_write(FailedNodes) ->
    PauseSpecs = [pause_node(DhtNodeSupPid) || DhtNodeSupPid <- util:random_subset(FailedNodes, pid_groups:find_all(sup_dht_node))],
    unittest_helper:check_ring_size(4 - FailedNodes),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:wait_for_stable_ring_deep(),

    ct:pal("attempting write_0_a~n"),
    ?equals_w_note(api_tx:write("0", 1), {fail, abort, ["0"]}, "write_0_a"),
    ct:pal("attempting read_0_a~n"),
    ?equals_w_note(api_tx:read("0"), {fail, abort, ["0"]}, "read_0_a"),

    _ = [unpause_node(PauseSpec) || PauseSpec <- PauseSpecs],

    ct:pal("attempting write_0_b~n"),
    ?equals_w_note(api_tx:write("0", 2), {ok}, "write_0_b"),
    ?equals_w_note(api_tx:read("0"), {ok, 2}, "read_0_b"),

    ok.

-type pause_spec() :: {pid_groups:groupname(), [pid()]}.

-spec pause_node(DhtNodeSupPid::pid()) -> pause_spec().
pause_node(DhtNodeSupPid) ->
    GroupName = pid_groups:group_of(DhtNodeSupPid),
    DhtNodePid = pid_groups:pid_of(GroupName, dht_node),
    ct:log("Pausing pid ~p~n", [DhtNodePid]),
    DhtNodeSupChilds = sup:sup_get_all_children(DhtNodeSupPid),
    _ = [case gen_component:is_gen_component(Pid) of
             true ->
                 gen_component:bp_set_cond(Pid, fun(_Msg, _State) -> true end, sleep),
                 gen_component:bp_barrier(Pid);
             false -> ok
         end || Pid <- DhtNodeSupChilds],

    fd:report(crash, DhtNodeSupChilds, 'DOWN'),

    pid_groups:hide(GroupName),
    {GroupName, DhtNodeSupChilds}.

-spec unpause_node(pause_spec()) -> ok.
unpause_node({GroupName, DhtNodeSupChilds}) ->
    pid_groups:unhide(GroupName),
    DhtNodePid = pid_groups:pid_of(GroupName, dht_node),
    ct:pal("Restarting pid ~p~n", [DhtNodePid]),
    % restart the node again:
    _ = [case gen_component:is_gen_component(Pid) of
             true ->
                 %% all have to have reached a breakpoint, otherwise this
                 %% blocks because of the bp_barrier...
                 gen_component:bp_del_async(Pid, sleep),
                 gen_component:bp_cont(Pid);
             false -> ok
         end || Pid <- DhtNodeSupChilds],
    ok.
