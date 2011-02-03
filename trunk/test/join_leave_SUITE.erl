% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Unit tests for src/dht_node_join.erl in combination with
%%         src/dht_node_move.erl.
%% @end
%% @version $Id$
-module(join_leave_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [
     tester_join_at, tester_join_at_v2,
     add_9, rm_5, add_9_rm_5,
     add_2x3_load, add_2x3_load_v2,
     add_3_rm_2_load, add_3_rm_2_load_v2,
     tester_join_at_timeouts_v2
    ].

suite() ->
    [
     {timetrap, {seconds, 60}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        % note: the craceful leave tests only work if transactions are
        % transferred to new TMs if the TM dies or the bench_server restarts
        % the transactions
        add_3_rm_2_load ->
            {skip, "no graceful leave yet"};
        add_3_rm_2_load_v2 ->
            {skip, "no graceful leave yet"};
        _ ->
            % stop ring from previous test case (it may have run into a timeout)
            unittest_helper:stop_ring(),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

join_parameters_list() ->
    [{join_response_timeout, 100},
     {join_response_timeouts, 3},
     {join_request_timeout, 100},
     {join_request_timeouts, 3},
     {join_lookup_timeout, 300},
     {join_known_hosts_timeout, 100},
     {join_timeout, 3000},
     {join_get_number_of_samples_timeout, 100}].

add_9(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_9_test/0, "add_9").

add_9_test() ->
    admin:add_nodes(9),
    check_size(10).

rm_5(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    admin:add_nodes(9),
    check_size(10),
    stop_time(fun rm_5_test/0, "rm_5").

rm_5_test() ->
    admin:del_nodes(5),
    check_size(5).

add_9_rm_5(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_9_rm_5_test/0, "add_9_rm_5").

add_9_rm_5_test() ->
    admin:add_nodes(9),
    check_size(10),
    admin:del_nodes(5),
    check_size(5).

add_2x3_load(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_2x3_load_test/0, "add_2x3_load"),
    dht_node_move_SUITE:check_size2_v1(4).

add_2x3_load_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    admin:add_nodes(3),
    check_size(7),
    unittest_helper:wait_for_process_to_die(BenchPid).

add_2x3_load_v2(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_2x3_load_v2_test/0, "add_2x3_load_v2"),
    dht_node_move_SUITE:check_size2_v2(4).

add_2x3_load_v2_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment_v2(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    admin:add_nodes(3),
    check_size(7),
    unittest_helper:wait_for_process_to_die(BenchPid).

add_3_rm_2_load(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_3_rm_2_load_test/0, "add_2x3_load"),
    dht_node_move_SUITE:check_size2_v1(4).

add_3_rm_2_load_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    % let 2 nodes gracefully leave
    _ = [comm:send_local(Pid, {leave}) || Pid <- util:random_subset(2, pid_groups:find_all(dht_node))],
%%     admin:del_nodes(2),
    check_size(2),
    unittest_helper:wait_for_process_to_die(BenchPid).

add_3_rm_2_load_v2(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_3_rm_2_load_v2_test/0, "add_2x3_load_v2"),
    dht_node_move_SUITE:check_size2_v2(4).

add_3_rm_2_load_v2_test() ->
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment_v2(1, 5000) end),
    admin:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    % let 2 nodes gracefully leave
    _ = [comm:send_local(Pid, {leave}) || Pid <- util:random_subset(2, pid_groups:find_all(dht_node))],
%%     admin:del_nodes(2),
    check_size(2),
    unittest_helper:wait_for_process_to_die(BenchPid).

-spec prop_join_at(FirstId::?RT:key(), SecondId::?RT:key()) -> true.
prop_join_at(FirstId, SecondId) ->
    BenchSlaves = 2, BenchRuns = 50,
    unittest_helper:make_ring_with_ids([FirstId], [{config, [pdb:get(log_path, ?MODULE) | join_parameters_list()]}]),
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment(BenchSlaves, BenchRuns) end),
    admin:add_node_at_id(SecondId),
    check_size(2),
    unittest_helper:wait_for_process_to_die(BenchPid),
    dht_node_move_SUITE:check_size2_v1(BenchSlaves * 4),
    unittest_helper:stop_ring(),
    true.

-spec prop_join_at_v2(FirstId::?RT:key(), SecondId::?RT:key()) -> true.
prop_join_at_v2(FirstId, SecondId) ->
    BenchSlaves = 2, BenchRuns = 50,
    unittest_helper:make_ring_with_ids([FirstId], [{config, [pdb:get(log_path, ?MODULE) | join_parameters_list()]}]),
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment_v2(BenchSlaves, BenchRuns) end),
    admin:add_node_at_id(SecondId),
    check_size(2),
    unittest_helper:wait_for_process_to_die(BenchPid),
    dht_node_move_SUITE:check_size2_v2(BenchSlaves * 4),
    unittest_helper:stop_ring(),
    true.

tester_join_at(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    prop_join_at(0, 0),
    tester:test(?MODULE, prop_join_at, 2, 5).

tester_join_at_v2(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    prop_join_at_v2(0, 0),
    tester:test(?MODULE, prop_join_at_v2, 2, 5).

% TODO: simulate more message drops,
% TODO: simulate certain protocol runs, e.g. the other node replying with noop, etc.
% keep in sync with dht_node_join and the timeout config parameters of join_parameters_list/0
-type join_message() ::
%% messages at the joining node:
%{get_dht_nodes_response, Nodes::[node:node_type()]} |
%{join, get_number_of_samples, Samples::non_neg_integer(), Conn::connection()} |
%{join, get_candidate_response, OrigJoinId::?RT:key(), Candidate::lb_op:lb_op(), Conn::connection()} |
%{join, join_response, Succ::node:node_type(), Pred::node:node_type(), MoveFullId::slide_op:id(), CandId::lb_op:id()} |
    {{join, join_response, '_', '_', '_', '_'}, [], 1..2, drop_msg} |
%{join, join_response, not_responsible, CandId::lb_op:id()} |
%{join, lookup_timeout, Conn::connection()} |
%{join, known_hosts_timeout} |
%{join, get_number_of_samples_timeout, Conn::connection()} |
%{join, join_request_timeout, Timeouts::non_neg_integer(), CandId::lb_op:id()} |
%{join, timeout} |
%% messages at the existing node:
%{join, number_of_samples_request, SourcePid::comm:mypid(), LbPsv::module(), Conn::connection()} |
%{join, get_candidate, Source_PID::comm:mypid(), Key::?RT:key(), LbPsv::module(), Conn::connection()} |
%{join, join_request, NewPred::node:node_type(), CandId::lb_op:id()} |
    {{join, join_request, '_', '_'}, [], 1..2, drop_msg}.
%{join, join_response_timeout, NewPred::node:node_type(), MoveFullId::slide_op:id(), CandId::lb_op:id()} |
%{Msg::lb_psv_simple:custom_message() | lb_psv_split:custom_message() | 
%      lb_psv_gossip:custom_message(),
% {join, LbPsv::module(), LbPsvState::term()}}

%% @doc Makes IgnoredMessages unique, i.e. only one msg per msg type.
-spec fix_tester_ignored_msg_list(IgnoredMessages::[join_message(),...]) -> [join_message(),...].
fix_tester_ignored_msg_list(IgnoredMessages) ->
    lists:usort(fun(E1, E2) ->
                        erlang:element(2, erlang:element(1, E1)) =<
                            erlang:element(2, erlang:element(1, E2))
                end, IgnoredMessages).

-spec send_ignore_msg_list_to(NthNode::1..4, PredOrSuccOrNode::pred | succ | node, IgnoredMessages::[join_message(),...]) -> ok.
send_ignore_msg_list_to(NthNode, PredOrSuccOrNode, IgnoredMessages) ->
    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    
    FailMsg = lists:flatten(io_lib:format("~.0p (~B.*) ignoring messages: ~.0p",
                                          [PredOrSuccOrNode, NthNode, IgnoredMessages])),
    {Pred, Node, Succ} = dht_node_move_SUITE:get_pred_node_succ(NthNode, FailMsg),
    Other = case PredOrSuccOrNode of
                succ -> Succ;
                pred -> Pred;
                node -> Node
            end,
    comm:send(node:pidX(Other), {mockup_dht_node, add_match_specs, IgnoredMessages}).

-spec prop_join_at_timeouts_v2(FirstId::?RT:key(), SecondId::?RT:key(),
        IgnoredMessages::[join_message(),...]) -> true.
prop_join_at_timeouts_v2(FirstId, SecondId, IgnoredMessages_) ->
    OldProcesses = unittest_helper:get_processes(),
    BenchSlaves = 2, BenchRuns = 50,
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),
    
    unittest_helper:make_ring_with_ids(
      [FirstId],
      [{config, [pdb:get(log_path, ?MODULE) | join_parameters_list()]}]),
    send_ignore_msg_list_to(1, node, IgnoredMessages),
    BenchPid = erlang:spawn(fun() -> bench_server:run_increment_v2(BenchSlaves, BenchRuns) end),
    unittest_helper:wait_for_process_to_die(BenchPid),
    admin:add_node_at_id(SecondId),
    check_size(2),
    dht_node_move_SUITE:check_size2_v2(BenchSlaves * 4),
    unittest_helper:stop_ring(),
%%     randoms:stop(), % tester may need it
    inets:stop(),
    unittest_helper:kill_new_processes(OldProcesses),
    true.

tester_join_at_timeouts_v2(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    tester:test(?MODULE, prop_join_at_timeouts_v2, 3, 5).

-spec stop_time(F::fun(() -> any()), Tag::string()) -> ok.
stop_time(F, Tag) ->
    Start = erlang:now(),
    F(),
    Stop = erlang:now(),
    ElapsedTime = timer:now_diff(Stop, Start) / 1000000.0,
    Frequency = 1 / ElapsedTime,
    ct:pal("~p took ~ps: ~p1/s",
           [Tag, ElapsedTime, Frequency]),
    ok.

-spec check_size(Size::pos_integer()) -> ok.
check_size(Size) ->
    unittest_helper:check_ring_size(Size),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size(Size).
