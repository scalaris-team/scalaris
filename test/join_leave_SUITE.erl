% @copyright 2010-2013 Zuse Institute Berlin

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

test_cases() ->
    [
     tester_join_at,
     add_9, rm_5, add_9_rm_5,
     add_2x3_load,
     add_1_rm_1_load,
     add_3_rm_1_load,
     add_3_rm_2_load,
     add_3_rm_3_load,
     tester_join_at_timeouts
    ].

all() ->
%%     unittest_helper:create_ct_all(test_cases()).
    unittest_helper:create_ct_all([join_lookup]) ++
        unittest_helper:create_ct_all([add_3_rm_3_data]) ++
        test_cases().

groups() ->
%%     unittest_helper:create_ct_groups(test_cases(), [{add_9_rm_5, [sequence, {repeat_until_any_fail, forever}]}]).
    unittest_helper:create_ct_groups([join_lookup], [{join_lookup, [sequence, {repeat_until_any_fail, 30}]}]) ++
    unittest_helper:create_ct_groups([add_3_rm_3_data], [{add_3_rm_3_data, [sequence, {repeat_until_any_fail, 30}]}]).

suite() -> [ {timetrap, {seconds, 30}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(TestCase, Config) ->
    case TestCase of
        % note: the craceful leave tests only work if transactions are
        % transferred to new TMs if the TM dies or the bench_server restarts
        % the transactions
        add_1_rm_1_load ->
            {skip, "graceful leave not fully supported yet"};
        add_3_rm_1_load ->
            {skip, "graceful leave not fully supported yet"};
        add_3_rm_2_load ->
            {skip, "graceful leave not fully supported yet"};
        add_3_rm_3_load ->
            {skip, "graceful leave not fully supported yet"};
        _ ->
            % stop ring from previous test case (it may have run into a timeout)
            unittest_helper:stop_ring(),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

join_parameters_list() ->
    [{move_wait_for_reply_timeout, 300},
     {move_send_msg_retries, 3},
     {move_send_msg_retry_delay, 0},
     {join_request_timeout, 100},
     {join_request_timeouts, 3},
     {join_lookup_timeout, 300},
     {join_known_hosts_timeout, 100},
     {join_timeout, 3000},
     {join_get_number_of_samples_timeout, 100}].

%% check whether we may loose some lookup when join is finished and
%% slide operations are still going on.
join_lookup(Config) ->
    %% need config to get random node id
    Config2 = unittest_helper:start_minimal_procs(Config, [], false),
    Keys = ?RT:get_replica_keys(?RT:get_random_node_id()),
    unittest_helper:stop_minimal_procs(Config2),

    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    %% do as less as possible between make_ring and sending the lookups
    This = comm:this(),
    _ = [ comm:send_local(pid_groups:find_a(dht_node),
                          {?lookup_aux, X, 0, {ping, This}}) || X <- Keys ],

    %% got all 4 replies? ok
    [ receive {pong} -> ok end || _ <- Keys ].

add_9(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_9_test/0, "add_9").

add_9_test() ->
    _ = api_vm:add_nodes(9),
    check_size(10).

rm_5(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    _ = api_vm:add_nodes(9),
    check_size(10),
    stop_time(fun rm_5_test/0, "rm_5").

rm_5_test() ->
    _ = api_vm:kill_nodes(5),
    check_size(5).

add_9_rm_5(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    stop_time(fun add_9_rm_5_test/0, "add_9_rm_5").

add_9_rm_5_test() ->
    _ = api_vm:add_nodes(9),
    check_size(10),
    _ = api_vm:kill_nodes(5),
    check_size(5).

add_3_rm_3_data(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper: make_ring(1, [{config, [{log_path, PrivDir} | join_parameters_list()]}]),
    _ = api_vm:add_nodes(3),
    unittest_helper:check_ring_size_fully_joined(4),
    RandomKeys = [randoms:getRandomString() || _ <- lists:seq(1,25)],
    _ = util:map_with_nr(fun(Key, X) -> {ok} = api_tx:write(Key, X) end, RandomKeys, 10000001),
    % note: there may be hash collisions -> count the number of unique DB entries!
    RandomHashedKeys = lists:append([?RT:get_replica_keys(?RT:hash_key(K)) || K <- RandomKeys]),
    ExpLoad = length(lists:usort(RandomHashedKeys)),
    % wait for late write messages to arrive at the original nodes
    % if all writes have arrived, a range read should return 4 values
    util:wait_for(
      fun() ->
              {Status, Values} = api_dht_raw:range_read(0, 0),
              Status =:= ok andalso erlang:length(Values) =:= ExpLoad
      end),
    ct:pal("######## starting graceful leave ########"),
    _ = api_vm:shutdown_nodes(3),
    unittest_helper:check_ring_load(ExpLoad).

add_2x3_load(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}, {monitor_perf_interval, 0} | join_parameters_list()]}]),
    stop_time(fun add_2x3_load_test/0, "add_2x3_load"),
    unittest_helper:check_ring_load(4),
    unittest_helper:check_ring_data().

add_2x3_load_test() ->
    BenchPid = erlang:spawn(fun() -> bench:increment(1, 1000) end),
    _ = api_vm:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    _ = api_vm:add_nodes(3),
    check_size(7),
    util:wait_for_process_to_die(BenchPid).

add_1_rm_1_load(Config) ->
    add_x_rm_y_load(Config, 1, 1).

add_3_rm_1_load(Config) ->
    add_x_rm_y_load(Config, 3, 1).

add_3_rm_2_load(Config) ->
    add_x_rm_y_load(Config, 3, 2).

add_3_rm_3_load(Config) ->
    add_x_rm_y_load(Config, 3, 3).

-spec add_x_rm_y_load(Config::[tuple()], X::non_neg_integer(), Y::pos_integer()) -> boolean().
add_x_rm_y_load(Config, X, Y) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}, {monitor_perf_interval, 0} | join_parameters_list()]}]),
    stop_time(fun() -> add_x_rm_y_load_test(X, Y) end, lists:flatten(io_lib:format("add_~B_rm_~B_load", [X, Y]))),
    unittest_helper:check_ring_load(4),
    unittest_helper:check_ring_data().

-spec add_x_rm_y_load_test(X::non_neg_integer(), Y::pos_integer()) -> ok.
add_x_rm_y_load_test(X, Y) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(1, 1000) end),
    _ = api_vm:add_nodes(X),
    check_size(X + 1),
    timer:sleep(500),
    % let Y nodes gracefully leave
    _ = [comm:send_local(Pid, {leave, null}) || Pid <- util:random_subset(Y, pid_groups:find_all(dht_node))],
    check_size(X + 1 - Y),
    util:wait_for_process_to_die(BenchPid).

-spec prop_join_at(FirstId::?RT:key(), SecondId::?RT:key()) -> true.
prop_join_at(FirstId, SecondId) ->
    BenchSlaves = 2, BenchRuns = 50,
    unittest_helper:make_ring_with_ids([FirstId], [{config, [pdb:get(log_path, ?MODULE), {monitor_perf_interval, 0} | join_parameters_list()]}]),
    BenchPid = erlang:spawn(fun() -> bench:increment(BenchSlaves, BenchRuns) end),
    _ = admin:add_node_at_id(SecondId),
    check_size(2),
    util:wait_for_process_to_die(BenchPid),
    unittest_helper:check_ring_load(BenchSlaves * 4),
    unittest_helper:check_ring_data(),
    unittest_helper:stop_ring(),
    true.

tester_join_at(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    prop_join_at(rt_SUITE:number_to_key(0), rt_SUITE:number_to_key(0)),
    tester:test(?MODULE, prop_join_at, 2, 5).

-spec reply_to_join_response(Msg::comm:message(), State, Reason::abort | crash | send_error) -> State
        when is_subtype(State, dht_node_state:state() | dht_node_join:join_state()).
reply_to_join_response(Msg, State, Reason) when is_tuple(State) andalso element(1, State) =:= state ->
    case Msg of
        {join, join_response, Succ, Pred, MoveId, CandId, _TargetId, _NextOp} ->
            % joined nodes only reply with a reject! (crash and send_error are always possible)
            case Reason of
                send_error ->
                    comm:send(node:pidX(Succ), {move, {send_error, comm:this(), Msg, unittest}, MoveId});
                crash ->
                    comm:send(node:pidX(Succ), {crash, comm:this(), {move, MoveId}});
                abort ->
                    dht_node_join:reject_join_response(Succ, Pred, MoveId, CandId)
            end;
        {join, join_response, _MsgReason, _CandId} ->
            % joined nodes only ignore this message! (not sent with shepherd, no fd used)
            case Reason of
                _ ->
                    ok
            end
    end,
    State;
reply_to_join_response(Msg, State, Reason) when is_tuple(State) andalso element(1, State) =:= join ->
    case Msg of
        {join, join_response, Succ, Pred, MoveId, CandId, _TargetId, _NextOp} ->
            case Reason of
                send_error ->
                    comm:send(node:pidX(Succ), {move, {send_error, comm:this(), Msg, unittest}, MoveId});
                abort ->
                    dht_node_join:reject_join_response(Succ, Pred, MoveId, CandId);
                crash ->
                    comm:send(node:pidX(Succ), {crash, comm:this(), {move, MoveId}})
            end;
        _ -> ok
    end,
    State.

-spec reply_to_join_request(Msg::comm:message(), State, Reason::not_responsible | busy) -> State
        when is_subtype(State, dht_node_state:state() | dht_node_join:join_state()).
reply_to_join_request(Msg, State, Reason) when is_tuple(State) andalso element(1, State) =:= state ->
    case Msg of
        {join, join_request, NewPred, CandId, _MaxTransportEntries} ->
            comm:send(node:pidX(NewPred), {join, join_response, Reason, CandId});
        _ -> ok
    end,
    State;
reply_to_join_request(_Msg, State, _Reason) when is_tuple(State) andalso element(1, State) =:= join ->
    State.

% TODO: simulate more message drops,
% TODO: simulate certain protocol runs, e.g. the other node replying with noop, etc.
% keep in sync with dht_node_join and the timeout config parameters of join_parameters_list/0
-type join_message() ::
%% messages at the joining node:
%{get_dht_nodes_response, Nodes::[node:node_type()]} |
%{join, get_number_of_samples, Samples::non_neg_integer(), Conn::connection()} |
%{join, get_candidate_response, OrigJoinId::?RT:key(), Candidate::lb_op:lb_op(), Conn::connection()} |
%{join, join_response, Succ::node:node_type(), Pred::node:node_type(), MoveFullId::slide_op:id(),
% CandId::lb_op:id(), TargetId::?RT:key(), NextOp::slide_op:next_op()} |
    {{join, join_response, '_', '_', '_', '_', '_', '_'}, [], 1..2,
     {reply_to_join_response, abort | crash | send_error}} |
%{join, join_response, not_responsible, CandId::lb_op:id()} |
%{join, lookup_timeout, Conn::connection()} |
%{join, known_hosts_timeout} |
%{join, get_number_of_samples_timeout, Conn::connection()} |
%{join, join_request_timeout, Timeouts::non_neg_integer(), CandId::lb_op:id()} |
%{join, timeout} |
%% messages at the existing node:
%{join, number_of_samples_request, SourcePid::comm:mypid(), LbPsv::module(), Conn::connection()} |
%{join, get_candidate, Source_PID::comm:mypid(), Key::?RT:key(), LbPsv::module(), Conn::connection()} |
%{join, join_request, NewPred::node:node_type(), CandId::lb_op:id(), MaxTransportEntries::unknown | pos_integer()} |
    {{join, join_request, '_', '_', '_'}, [], 1..2,
     drop_msg | {reply_to_join_request, not_responsible | busy}}.
%{Msg::lb_psv_simple:custom_message() | lb_psv_split:custom_message() |
%      lb_psv_gossip:custom_message(),
% {join, LbPsv::module(), LbPsvState::term()}}

%% @doc Makes IgnoredMessages unique, i.e. only one msg per msg type.
-spec fix_tester_ignored_msg_list(IgnoredMessages::[join_message(),...]) -> [join_message(),...].
fix_tester_ignored_msg_list(IgnoredMessages) ->
    IMsg2 = lists:usort(fun(E1, E2) ->
                                erlang:element(2, erlang:element(1, E1)) =<
                                    erlang:element(2, erlang:element(1, E2))
                        end, IgnoredMessages),
    [begin
         NewAction =
             case Action of
                 {reply_to_join_response, Reason} ->
                     fun(MsgX, StateX) -> reply_to_join_response(MsgX, StateX, Reason) end;
                 {reply_to_join_request, Reason} ->
                     fun(MsgX, StateX) -> reply_to_join_request(MsgX, StateX, Reason) end;
                 X -> X
             end,
         {Msg, Conds, Count, NewAction}
     end || {Msg, Conds, Count, Action} <- IMsg2].

-spec send_ignore_msg_list_to(NthNode::1..4, PredOrSuccOrNode::pred | succ | node, IgnoredMessages::[join_message(),...]) -> ok.
send_ignore_msg_list_to(NthNode, PredOrSuccOrNode, IgnoredMessages) ->
    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],

    FailMsg = lists:flatten(io_lib:format("~.0p (~B.*) ignoring messages: ~.0p",
                                          [PredOrSuccOrNode, NthNode, IgnoredMessages])),
    {_PredsPred, Pred, Node, Succ} = dht_node_move_SUITE:get_pred_node_succ(NthNode, FailMsg),
    Other = case PredOrSuccOrNode of
                succ -> Succ;
                pred -> Pred;
                node -> Node
            end,
    comm:send(node:pidX(Other), {mockup_dht_node, add_match_specs, IgnoredMessages}).

-spec prop_join_at_timeouts(FirstId::?RT:key(), SecondId::?RT:key(),
        IgnoredMessages::[join_message(),...], IgnMsgAt1st::boolean(),
        IgnMsgAt2nd::boolean()) -> true.
prop_join_at_timeouts(FirstId, SecondId, IgnoredMessages_, IgnMsgAt1st, IgnMsgAt2nd) ->
    OldProcesses = unittest_helper:get_processes(),
    BenchSlaves = 2, BenchRuns = 50,
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    unittest_helper:make_ring_with_ids(
      [FirstId],
      [{config, [{dht_node, mockup_dht_node}, pdb:get(log_path, ?MODULE), {monitor_perf_interval, 0} | join_parameters_list()]}]),
    if IgnMsgAt1st -> send_ignore_msg_list_to(1, node, IgnoredMessages);
       true        -> ok
    end,
    BenchPid = erlang:spawn(fun() -> bench:increment(BenchSlaves, BenchRuns) end),
    util:wait_for_process_to_die(BenchPid),
    MoreOpts = if IgnMsgAt2nd -> [{match_specs, IgnoredMessages}];
                  true        -> []
               end,
    _ = admin:add_node([{{dht_node, id}, SecondId}, {skip_psv_lb}] ++ MoreOpts),
    check_size(2),
    unittest_helper:check_ring_load(BenchSlaves * 4),
    unittest_helper:check_ring_data(),
    unittest_helper:stop_ring(),
%%     randoms:stop(), % tester may need it
    _ = inets:stop(),
    unittest_helper:kill_new_processes(OldProcesses),
    true.

tester_join_at_timeouts(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    tester:test(?MODULE, prop_join_at_timeouts, 5, 10).

-spec stop_time(F::fun(() -> any()), Tag::string()) -> ok.
stop_time(F, Tag) ->
    Start = erlang:now(),
    F(),
    Stop = erlang:now(),
    ElapsedTime = timer:now_diff(Stop, Start) / 1000000.0,
    Frequency = 1 / ElapsedTime,
    ct:pal("~p took ~ps: ~p1/s~n",
           [Tag, ElapsedTime, Frequency]),
    ok.

-spec check_size(Size::pos_integer()) -> ok.
check_size(Size) ->
    unittest_helper:check_ring_size(Size),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size_fully_joined(Size).
