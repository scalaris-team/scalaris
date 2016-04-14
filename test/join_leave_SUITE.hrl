% @copyright 2010-2015 Zuse Institute Berlin

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

-include("unittest.hrl").
-include("scalaris.hrl").

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(graceful_leave_load, _Config) ->
    % TODO: occasionally the bench server (as a Scalaris client) hangs because
    %       it connected to the node being shut down
    %       -> in a real-life system where the client is on the same node as
    %          the Scalaris node and both are being shut down together this has
    %          no influence on the rest of the nodes (a stale transaction is
    %          cleaned up by the RTMs after some time)
    {skip, "graceful leave not fully supported yet"};
init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

join_parameters_list() ->
    [{move_wait_for_reply_timeout, 300},
     {move_send_msg_retries, 3},
     {move_send_msg_retry_delay, 0},
     {join_request_timeout, 1000},
     {join_request_timeouts, 3},
     {join_lookup_timeout, 1000},
     {join_known_hosts_timeout, 1000},
     {join_timeout, 3000},
     {join_get_number_of_samples_timeout, 1000}].

%% check whether we may loose some lookup when join is finished and
%% slide operations are still going on.
join_lookup(Config) ->
    %% need config to get random node id
    Config2 = unittest_helper:start_minimal_procs(Config, [], false),
    Config3 = unittest_helper:stop_minimal_procs(Config2),

    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config3),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir},
                                            {rrepair_after_crash, false}]
                                       ++ additional_ring_config()}]),
    Keys = ?RT:get_replica_keys(?RT:get_random_node_id()),
    ?proto_sched(start),
    %% do as less as possible between make_ring and sending the lookups
    This = comm:this(),
    _ = [ comm:send_local(pid_groups:find_a(dht_node),
                          {?lookup_aux, X, 0, {ping, This}}) || X <- Keys ],

    %% got all replies? ok
    [ begin
          trace_mpath:thread_yield(),
          receive
              ?SCALARIS_RECV({pong, dht_node}, ok) end
      end || _ <- Keys ],
    ?proto_sched(stop).

add_9(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir},
                                            {rrepair_after_crash, false}
                                           | join_parameters_list()]
                                       ++ additional_ring_config()}]),
    %% stop_time calls ?proto_sched(start)
    stop_time(fun add_9_test/0, "add_9").

add_9_test() ->
    _ = api_vm:add_nodes(9),
    check_size(10).

rm_5(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir},
                                            {rrepair_after_crash, false}
                                           | join_parameters_list()]
                                       ++ additional_ring_config()}]),
    _ = api_vm:add_nodes(9),
    check_size(10),
    %% stop_time calls ?proto_sched(start)
    stop_time(fun rm_5_test/0, "rm_5").

rm_5_test() ->
    _ = api_vm:kill_nodes(5),
    check_size(5).

add_9_rm_5(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir},
                                            {rrepair_after_crash, false}
                                           | join_parameters_list()]
                                       ++ additional_ring_config()}]),
    %% stop_time calls ?proto_sched(start)
    stop_time(fun add_9_rm_5_test/0, "add_9_rm_5").

add_9_rm_5_test() ->
    _ = api_vm:add_nodes(9),
    check_size(10),
    _ = api_vm:kill_nodes(5),
    check_size(5).

add_3_rm_3_data(Config) ->
    add_3_rm_3_data(Config, false).

add_3_rm_3_data_inc(Config) ->
    add_3_rm_3_data(Config, true).

add_3_rm_3_data(Config, Incremental) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{move_max_transport_entries, 25},
                                            {move_use_incremental_slides, Incremental},
                                            {log_path, PrivDir},
                                            {monitor_perf_interval, 0},
                                            {rrepair_after_crash, false}
                                           | join_parameters_list()]
                                       ++ additional_ring_config()}]),

    RandomKeys = [randoms:getRandomString() || _ <- lists:seq(1,100)],
    % note: there may be hash collisions -> count the number of unique DB entries!
    RandomHashedKeys = lists:flatmap(fun(K) -> ?RT:get_replica_keys(?RT:hash_key(K)) end, RandomKeys),
    ExpLoad = length(lists:usort(RandomHashedKeys)),

    _ = util:map_with_nr(fun(Key, X) -> {ok} = api_tx:write(Key, X) end, RandomKeys, 10000001),
    % wait for late write messages to arrive at the original nodes
    api_tx_SUITE:wait_for_dht_entries(ExpLoad),

    ?proto_sched(start),
    ct:pal("######## starting join ########"),
    _ = api_vm:add_nodes(3),
    ?proto_sched(stop),
    unittest_helper:check_ring_size_fully_joined(4),
    ?proto_sched(restart),
    ct:pal("######## starting graceful leave ########"),
    _ = api_vm:shutdown_nodes(3),
    ?proto_sched(stop),
    check_size(1),
    ct:pal("######## checking load ########"),
    unittest_helper:check_ring_load(ExpLoad),
    ct:pal("######## done ########"),
    ok.

add_2x3_load(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir},
                                            {rrepair_after_crash, false},
                                            {monitor_perf_interval, 0}
                                           | join_parameters_list()]
                                       ++ additional_ring_config()}]),
    %% stop_time calls ?proto_sched2(start/stop)
    stop_time(fun add_2x3_load_test/0,
               fun() -> ?bench(1, 100, 1, 5000) end,
               "add_2x3_load"),
    unittest_helper:check_ring_load(config:read(replication_factor)),
    unittest_helper:check_ring_data().

add_2x3_load_test() ->
    ct:pal("######## starting join 1 ########"),
    _ = api_vm:add_nodes(3),
    check_size(4),
    timer:sleep(500),
    ct:pal("######## starting join 2 ########"),
    _ = api_vm:add_nodes(3),
    check_size(7),
    ct:pal("######## waiting for bench finish ########").

make_4_add_1_rm_1_load(Config) ->
    make_4_add_x_rm_y_load(Config, 1, 1, true).

make_4_add_2_rm_2_load(Config) ->
    make_4_add_x_rm_y_load(Config, 2, 2, true).

make_4_add_3_rm_3_load(Config) ->
    make_4_add_x_rm_y_load(Config, 3, 3, true).

-spec make_4_add_x_rm_y_load(Config::[tuple()], X::non_neg_integer(), Y::pos_integer(),
                             StartOnlyAdded::boolean()) -> boolean().
make_4_add_x_rm_y_load(Config, X, Y, StartOnlyAdded) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring_with_ids(
      ?RT:get_replica_keys(?MINUS_INFINITY),
      [{config, [{log_path, PrivDir}, {rrepair_after_crash, false},
                 {monitor_perf_interval, 0} | join_parameters_list()]
            ++ additional_ring_config()}]),
    %% stop_time calls ?proto_sched(start)
    stop_time(fun() -> add_x_rm_y_load_test(X, Y, StartOnlyAdded) end,
               fun() -> ?bench(10, 100, 10, 1000) end,
               lists:flatten(io_lib:format("add_~B_rm_~B_load", [X, Y]))),
    unittest_helper:check_ring_load(config:read(replication_factor)*10),
    unittest_helper:check_ring_data().

-spec add_x_rm_y_load_test(X::non_neg_integer(), Y::pos_integer(), StartOnlyAdded::boolean()) -> ok.
add_x_rm_y_load_test(X, Y, StartOnlyAdded) ->
    %% BenchPid = erlang:spawn(fun() -> bench:increment(10, 1000) end),
    {Started, []} = api_vm:add_nodes(X),
    check_size(X + 4),
    timer:sleep(500),
    Ring = [begin
                Pred = hd(node_details:get(Details, predlist)),
                Node = node_details:get(Details, node),
                {comm:make_local(node:pidX(Node)), node:id(Node),
                 ?RT:get_range(node:id(Pred), node:id(Node)) / ?RT:n()}
            end || {ok, Details} <- statistics:get_ring_details()],
    ct:pal("RING: ~.2p", [Ring]),
    ct:pal("######## finish join ########"),
    % let Y nodes gracefully leave, one after another - not more than minority of tm_tms can die!
    ToShutdown =
        if StartOnlyAdded -> util:random_subset(Y, Started);
           true -> [pid_groups:group_of(Pid)|| Pid <- util:random_subset(Y, pid_groups:find_all(dht_node))]
        end,
    [begin
         Pid = pid_groups:pid_of(Group, dht_node),
         ct:pal("Killing #~p: ~p (~p)", [I, Pid, Group]),
         comm:send_local(Pid, {leave, null}),
         check_size(X + 4 - I)
     end || {I, Group} <- lists:zip(lists:seq(1, Y), ToShutdown)],
    check_size(X + 4 - Y),
    ct:pal("######## waiting for bench finish ########").

-spec prop_join_at(FirstId::?RT:key(), SecondId::?RT:key(), Incremental::boolean()) -> true.
prop_join_at(FirstId, SecondId, Incremental) ->
    OldProcesses = unittest_helper:get_processes(),
    BenchSlaves = 2,

    unittest_helper:make_ring_with_ids(
      [FirstId],
      [{config, [{move_max_transport_entries, 25},
                 {move_use_incremental_slides, Incremental},
                 pdb:get(log_path, ?MODULE), {rrepair_after_crash, false},
                 {monitor_perf_interval, 0} | join_parameters_list()]
            ++ additional_ring_config()}]),

    RandomKeys = [randoms:getRandomString() || _ <- lists:seq(1,100)],
    % note: there may be hash collisions -> count the number of unique DB entries!
    RandomHashedKeys = lists:flatmap(fun(K) -> ?RT:get_replica_keys(?RT:hash_key(K)) end, RandomKeys),
    ExpLoad = length(lists:usort(RandomHashedKeys)),

    _ = util:map_with_nr(fun(Key, X) -> {ok} = api_tx:write(Key, X) end, RandomKeys, 10000001),
    % wait for late write messages to arrive at the original nodes
    api_tx_SUITE:wait_for_dht_entries(ExpLoad),

    %% stop_time calls ?proto_sched2(start/stop)
    stop_time(fun() -> _ = admin:add_node_at_id(SecondId), check_size(2) end,
               fun() -> ?bench(BenchSlaves, 15, BenchSlaves, 50) end,
               "join_at"),

    unittest_helper:check_ring_load(ExpLoad + BenchSlaves * config:read(replication_factor)),
    unittest_helper:check_ring_data(),
    unittest_helper:stop_ring(),
    unittest_helper:kill_new_processes(OldProcesses),
    true.

tester_join_at(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    prop_join_at(rt_SUITE:number_to_key(0), rt_SUITE:number_to_key(0), false),
    tester:test(?MODULE, prop_join_at, 3, 5).

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
                    comm:send(node:pidX(Succ), {move, MoveId, {fd_notify, crash, comm:this(), 'DOWN'}});
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
                    comm:send(node:pidX(Succ), {move, MoveId, {fd_notify, crash, comm:this(), 'DOWN'}})
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
      [{config, [{dht_node, mockup_dht_node}, pdb:get(log_path, ?MODULE),
                 {rrepair_after_crash, false}, {monitor_perf_interval, 0}
                | join_parameters_list()]
            ++ additional_ring_config()}]),
    if IgnMsgAt1st -> send_ignore_msg_list_to(1, node, IgnoredMessages);
       true        -> ok
    end,
    BenchPid = erlang:spawn(fun() -> bench:increment(BenchSlaves, BenchRuns) end),
    wait_for_process_to_die(BenchPid),
    MoreOpts = if IgnMsgAt2nd -> [{match_specs, IgnoredMessages}];
                  true        -> []
               end,
    _ = admin:add_node([{{dht_node, id}, SecondId}, {skip_psv_lb}] ++ MoreOpts),
    check_size(2),
    unittest_helper:check_ring_load(BenchSlaves * config:read(replication_factor)),
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
    Start = os:timestamp(),
    ?proto_sched(start),
    F(),
    ?proto_sched(stop),
    Stop = os:timestamp(),
    ElapsedTime = erlang:max(1, timer:now_diff(Stop, Start)) / 1000000.0,
    Frequency = 1 / ElapsedTime,
    ct:pal("~p took ~ps: ~p1/s~n",
           [Tag, ElapsedTime, Frequency]),
    ok.

-spec stop_time(F1::fun(() -> any()), F2::fun(() -> any()), Tag::string()) -> ok.
stop_time(F1, F2, Tag) ->
    Start = os:timestamp(),
    Pid = ?proto_sched2(start, fun() -> F2() end),
    F1(),
    ?proto_sched2(stop, Pid),
    Stop = os:timestamp(),
    ElapsedTime = erlang:max(1, timer:now_diff(Stop, Start)) / 1000000.0,
    Frequency = 1 / ElapsedTime,
    ct:pal("~p took ~ps: ~p1/s~n",
           [Tag, ElapsedTime, Frequency]),
    ok.

-spec check_size(Size::pos_integer()) -> ok.
check_size(Size) ->
    Infected = trace_mpath:infected(),
    ?IIF(Infected, ?proto_sched(stop), ok),
    unittest_helper:check_ring_size(Size),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size_fully_joined(Size),
    ?IIF(Infected, ?proto_sched(restart), ok).

%% @doc Waits for the given process to die (without putting the wait messages
%%      into the proto_sched if enabled).
-spec wait_for_process_to_die(Pid::pid()) -> ok.
wait_for_process_to_die(Pid) ->
    Infected = trace_mpath:infected(),
    ?IIF(Infected, ?proto_sched(stop), ok),
    util:wait_for_process_to_die(Pid),
    ?IIF(Infected, ?proto_sched(restart), ok).
