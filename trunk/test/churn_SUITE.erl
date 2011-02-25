% @copyright 2008-2011 Zuse Institute Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @author Nico Kruber <kruber@zib.de>
%%% @author Florian Schintke <schintke@zib.de>
%%% @doc : Unit tests for transactions under churn
%%% @end
-module(churn_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [transactions_1_failure_4_nodes_read,
     transactions_2_failures_4_nodes_read,
     transactions_3_failures_4_nodes_read,
     transactions_1_failure_4_nodes_networksplit_write,
     transactions_2_failures_4_nodes_networksplit_write,
     transactions_3_failures_4_nodes_networksplit_write].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

init_per_testcase(TestCase, Config) ->
    case TestCase of
        transactions_2_failures_4_nodes_networksplit_write ->
            {skip, "cannot handle network split yet - see issue 59"};
        transactions_3_failures_4_nodes_networksplit_write ->
            {skip, "cannot handle network split yet - see issue 59"};
        _ ->
            % stop ring from previous test case (it may have run into a timeout)
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring_with_ids(fun() -> ?RT:get_replica_keys(?RT:hash_key(0)) end, [{config, [{log_path, PrivDir}]}]),
            Config
%%             {skip, "temporarily"}
    end.
 
end_per_testcase(_TestCase, _Config) ->
    %error_logger:tty(false),
    unittest_helper:stop_ring(),
    ok.

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

transactions_1_failure_4_nodes_read(_) ->
    ?equals_w_note(cs_api_v2:write(0, 1), ok, "write_0_a"),
    ?equals_w_note(cs_api_v2:read(0), 1, "read_0_a"),
    % wait for late write messages to arrive at the original nodes
    % note: this is not safe - a message may be delayed even more!
    timer:sleep(100),
    _ = admin:del_nodes(1),
    unittest_helper:check_ring_size(3),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:wait_for_stable_ring_deep(),
    ?equals_w_note(cs_api_v2:read(0), 1, "read_0_b"),
    ?equals_w_note(cs_api_v2:write(0, 2), ok, "write_0_b"),
    ?equals_w_note(cs_api_v2:read(0), 2, "read_0_c"),
    ok.

transactions_2_failures_4_nodes_read(_) ->
    transactions_more_failures_4_nodes_read(2).

transactions_3_failures_4_nodes_read(_) ->
    transactions_more_failures_4_nodes_read(3).

-spec transactions_more_failures_4_nodes_read(FailedNodes::2 | 3) -> ok.
transactions_more_failures_4_nodes_read(FailedNodes) ->
    ?equals_w_note(cs_api_v2:write(0, 1), ok, "write_0_a"),
    ?equals_w_note(cs_api_v2:read(0), 1, "read_0_a"),
    % wait for late write messages to arrive at the original nodes
    % note: this is not safe - a message may be delayed even more!
    timer:sleep(100),
    _ = admin:del_nodes(FailedNodes),
    unittest_helper:check_ring_size(4 - FailedNodes),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:wait_for_stable_ring_deep(),
    ?equals_w_note(cs_api_v2:read(0), {fail, not_found}, "read_0_b"),
    ?equals_w_note(cs_api_v2:write(0, 2), {fail, abort}, "write_0_b"),
    ?equals_w_note(cs_api_v2:read(0), {fail, not_found}, "read_0_c"),
    ok.

transactions_1_failure_4_nodes_networksplit_write(_) ->
    % pause some dht_node:
    Node = pid_groups:find_a(sup_dht_node),
    PauseSpec = pause_node(Node),
    unittest_helper:check_ring_size(3),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:wait_for_stable_ring_deep(),
    
    ct:pal("attempting write_0_a~n"),
    ?equals_w_note(cs_api_v2:write(0, 1), ok, "write_0_a"),
    ct:pal("attempting read_0_a~n"),
    ?equals_w_note(cs_api_v2:read(0), 1, "read_0_a"),

    unpause_node(PauseSpec),

    ct:pal("attempting read_0_b~n"),
    ?equals_w_note(cs_api_v2:read(0), 1, "read_0_b"),
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
    ?equals_w_note(cs_api_v2:write(0, 1), {fail, abort}, "write_0_a"),
    ct:pal("attempting read_0_a~n"),
    ?equals_w_note(cs_api_v2:read(0), {fail, abort}, "read_0_a"),
    
    _ = [unpause_node(PauseSpec) || PauseSpec <- PauseSpecs],

    ct:pal("attempting write_0_b~n"),
    ?equals_w_note(cs_api_v2:write(0, 2), ok, "write_0_b"),
    ?equals_w_note(cs_api_v2:read(0), 2, "read_0_b"),
    
    ok.

-type pause_spec() :: {pid_groups:groupname(), [pid()]}.

-spec pause_node(DhtNodeSupPid::pid()) -> pause_spec().
pause_node(DhtNodeSupPid) ->
    GroupName = pid_groups:group_of(DhtNodeSupPid),
    DhtNodePid = pid_groups:pid_of(GroupName, dht_node),
    DhtNodeSupChilds = unittest_helper:get_all_children(DhtNodeSupPid),
    _ = [begin
             gen_component:bp_set_cond(Pid, fun(_Msg, _State) -> true end, sleep),
             gen_component:bp_barrier(Pid)
         end || Pid <- DhtNodeSupChilds],

    [ comm:send_local(fd, {unittest_report_down, comm:make_global(X)})
      || X <- unittest_helper:get_all_children(DhtNodeSupPid)],

    pid_groups:hide(GroupName),
    {GroupName, DhtNodeSupChilds}.

-spec unpause_node(pause_spec()) -> ok.
unpause_node({GroupName, DhtNodeSupChilds}) ->
    ct:pal("restarting node~n"),
    pid_groups:unhide(GroupName),
    % restart the node again:
    _ = [begin
             gen_component:bp_del(Pid, sleep),
             gen_component:bp_cont(Pid)
         end || Pid <- DhtNodeSupChilds],
    ok.
