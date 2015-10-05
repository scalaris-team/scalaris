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
%% @doc    Unit tests for src/dht_node_move.erl (slide and jump operations).
%%   dht_node_move_SUITE.erl:
%%       The regular execution of the suite.
%%   dht_node_move_proto_sched_SUITE.erl:
%%       Executes using the proto scheduler which serializes all messages to
%%       generate a random interleaving of messages on different channels.
%% @end
%% @version $Id$

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").


-type(node_type() :: predspred | pred | node | succ).

-type(node_tuple() :: {node:node_type(), node:node_type(), node:node_type(), node:node_type()}).

-type(targetid_fun() :: fun((node_tuple()) -> ?RT:key())).

-record(slideconf, {name :: atom(),
                    node :: node_type(),
                    slide_with :: node_type(),
                    targetid ::  targetid_fun()}).

-type(slide_config_record() :: #slideconf{}).


test_cases() -> [].

groups() ->
    MoveConfig = {move_config, move_config_parameters()},
    MoveConfigInc = {move_config, move_config_parameters_incremental()},
    MoveConfigInc2 = {move_config, move_config_parameters_incremental2()},
    GroupOptions = [sequence, {repeat, 1}],
    Config = [MoveConfig | GroupOptions],
    ConfigInc = [MoveConfigInc | GroupOptions],
    ConfigInc2 = [MoveConfigInc2 | GroupOptions],
    SendToPredTestCases =
        [
         symm4_slide_succ_rcv_load,
         symm4_slide_pred_send_load,
         tester_symm4_slide_succ_rcv_load_timeouts_succ,
         tester_symm4_slide_succ_rcv_load_timeouts_node,
         tester_symm4_slide_pred_send_load_timeouts_pred,
         tester_symm4_slide_pred_send_load_timeouts_node
        ],
    SendToSuccTestCases =
        [
         symm4_slide_succ_send_load,
         symm4_slide_pred_rcv_load,
         tester_symm4_slide_succ_send_load_timeouts_succ,
         tester_symm4_slide_succ_send_load_timeouts_node,
         tester_symm4_slide_pred_rcv_load_timeouts_pred,
         tester_symm4_slide_pred_rcv_load_timeouts_node
        ],
    SendToBothTestCases =
        [
         test_slide_adjacent,
         test_slide_conflict
        ],
    SlideIllegallyTestCases =
        [
         test_slide_illegally
        ],
    JumpSlideTestCases =
        [
         tester_test_jump
        ],
    [
      {send_to_pred, Config, SendToPredTestCases},
      {send_to_pred_incremental, ConfigInc, SendToPredTestCases},
      {send_to_succ, Config, SendToSuccTestCases},
      {send_to_succ_incremental, ConfigInc, SendToSuccTestCases},
      {send_to_both, Config, SendToBothTestCases},
      {send_to_both_incremental, ConfigInc2, SendToBothTestCases},
      {slide_illegally, Config, SlideIllegallyTestCases},
      {jump_slide, Config, JumpSlideTestCases},
      {jump_slide_incremental, ConfigInc, JumpSlideTestCases}
    ]
      ++
%%         unittest_helper:create_ct_groups(test_cases(), [{tester_symm4_slide_pred_send_load_timeouts_pred_incremental, [sequence, {repeat_until_any_fail, forever}]}]).
        [].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) ->
    unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    GroupConfig = proplists:get_value(tc_group_properties, Config, []),
    {move_config, MoveConf} = lists:keyfind(move_config, 1, GroupConfig),
    unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir},
                                                   {dht_node, mockup_dht_node},
                                                   {monitor_perf_interval, 0},
                                                   {join_lb_psv, lb_psv_simple},
                                                   {lb_psv_samples, 1},
                                                   {replication_factor, 4}]
                                          ++ MoveConf ++ additional_ring_config()}]),
    % wait for all nodes to finish their join before writing data
    unittest_helper:check_ring_size_fully_joined(4),
    %% write some data (use a function because left-over tx_timeout messages can disturb the tests):
    Pid = erlang:spawn(fun() ->
                               _ = [api_tx:write(erlang:integer_to_list(X), X) || X <- lists:seq(1, 100)]
                       end),
    util:wait_for_process_to_die(Pid),
    % wait a bit for the rm-processes to settle
    timer:sleep(500),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%% @doc Sets tighter timeouts for slides
-spec move_config_parameters() -> unittest_helper:kv_opts().
move_config_parameters() ->
    [{move_use_incremental_slides, false},
     {move_wait_for_reply_timeout, 1000},
     {move_send_msg_retries, 2},
     {move_send_msg_retry_delay, 0}].

%% @doc Sets tighter timeouts for slides
-spec move_config_parameters_incremental() -> unittest_helper:kv_opts().
move_config_parameters_incremental() ->
    % note: later parameters in the list override the first ones in config
    move_config_parameters() ++
        [{move_use_incremental_slides, true},
         {move_max_transport_entries, 2},
         {move_send_msg_retry_delay, 0}].

%% @doc Config like above but more entries are moved at once
-spec move_config_parameters_incremental2() -> unittest_helper:kv_opts().
move_config_parameters_incremental2() ->
    move_config_parameters_incremental() ++
        [{move_max_transport_entries, 10}].

%% @doc Test slide with successor, receiving data from it (using api_tx in the bench server).
symm4_slide_succ_rcv_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
                      api_tx_SUITE:wait_for_dht_entries(440),
                      symm4_slide_load_test(1, succ, "slide_succ_rcv", fun id_my_to_succ_1_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(2, succ, "slide_succ_rcv", fun id_my_to_succ_1_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(3, succ, "slide_succ_rcv", fun id_my_to_succ_1_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(4, succ, "slide_succ_rcv", fun id_my_to_succ_1_100/6, ?NUM_SLIDES),
                      erlang:exit(BenchPid, 'kill'),
                      util:wait_for_process_to_die(BenchPid)
              end, "symm4_slide_succ_rcv_load"),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data().

%% @doc Test slide with successor, sending data to it (using api_tx in the bench server).
symm4_slide_succ_send_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
                      api_tx_SUITE:wait_for_dht_entries(440),
                      symm4_slide_load_test(1, succ, "slide_succ_send", fun id_pred_to_my_99_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(2, succ, "slide_succ_send", fun id_pred_to_my_99_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(3, succ, "slide_succ_send", fun id_pred_to_my_99_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(4, succ, "slide_succ_send", fun id_pred_to_my_99_100/6, ?NUM_SLIDES),
                      erlang:exit(BenchPid, 'kill'),
                      util:wait_for_process_to_die(BenchPid)
              end, "symm4_slide_succ_send_load"),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data().

%% @doc Test slide with predecessor, sending data to it (using api_tx in the bench server).
symm4_slide_pred_send_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
                      api_tx_SUITE:wait_for_dht_entries(440),
                      symm4_slide_load_test(1, pred, "slide_pred_send", fun id_pred_to_my_1_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(2, pred, "slide_pred_send", fun id_pred_to_my_1_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(3, pred, "slide_pred_send", fun id_pred_to_my_1_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(4, pred, "slide_pred_send", fun id_pred_to_my_1_100/6, ?NUM_SLIDES),
                      erlang:exit(BenchPid, 'kill'),
                      util:wait_for_process_to_die(BenchPid)
              end, "symm4_slide_pred_send_load"),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data().

%% @doc Test slide with predecessor, receiving data from it (using api_tx in the bench server).
symm4_slide_pred_rcv_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
                      api_tx_SUITE:wait_for_dht_entries(440),
                      symm4_slide_load_test(1, pred, "slide_pred_rcv", fun id_predspred_to_pred_99_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(2, pred, "slide_pred_rcv", fun id_predspred_to_pred_99_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(3, pred, "slide_pred_rcv", fun id_predspred_to_pred_99_100/6, ?NUM_SLIDES),
                      symm4_slide_load_test(4, pred, "slide_pred_rcv", fun id_predspred_to_pred_99_100/6, ?NUM_SLIDES),
                      erlang:exit(BenchPid, 'kill'),
                      util:wait_for_process_to_die(BenchPid)
              end, "symm4_slide_pred_rcv_load"),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data().

%%%%%%%%%%%%%%%%%%%%

-spec reply_with_send_error(Msg::comm:message(), State::dht_node_state:state())
        -> State::dht_node_state:state().
reply_with_send_error(Msg, State) when element(1, Msg) =:= move ->
    % just in case, if there are two slides, then send two send errors
    SlidePred = dht_node_state:get(State, slide_pred),
    SlideSucc = dht_node_state:get(State, slide_succ),
    % note: only send a message once -> use lists:usort/1 to filter out duplicates
    FailMsgs =
        [begin
             case Slide of
                 null ->
                     case element(2, Msg) of
                         slide ->
                             {node:pidX(element(5, Msg)), element(4, Msg)};
                         _ ->
                             {null, ok}
                     end;
                 _    ->
                     Target = node:pidX(slide_op:get_node(Slide)),
                     FailMsgCookie =
                         case element(2, Msg) of
                             slide_abort   -> {timeouts, 0};
                             delta_ack     -> {timeouts, 0};
                             slide         -> element(4, Msg);
                             _             -> slide_op:get_id(Slide)
                         end,
                     {Target, FailMsgCookie}
             end
         end || Slide <- lists:usort([SlidePred, SlideSucc])],
    _ = [comm:send(Target, {move, {send_error, comm:this(), Msg, unittest}, FailMsgCookie})
        || X = {Target, FailMsgCookie} <- lists:usort(FailMsgs),
           X =/= {null, ok}],
    State;
reply_with_send_error(_Msg, State) ->
    State.

% keep in sync with dht_node_move and the timeout config parameters set in set_move_config_parameters/0
-type move_message() ::
%{move, slide, OtherType::slide_op:type(), MoveFullId::slide_op:id(), InitNode::node:node_type(), TargetNode::node:node_type(), TargetId::?RT:key(), Tag::any(), NextOp::slide_op:next_op(), MaxTransportEntries::pos_integer()} |
    {{move, slide, '_', '_', '_', '_', '_', '_', '_', '_'}, [], 1..2, reply_with_send_error} |
%{move, slide_abort, pred | succ, MoveFullId::slide_op:id(), Reason::abort_reason()} |
    {{move, slide_abort, '_', '_', '_'}, [], 1, reply_with_send_error} |
% note: do not loose local messages:
%{move, node_update, Tag::{move, slide_op:id()}} | % message from RM that it has changed the node's id to TargetId
%{move, rm_new_pred, Tag::{move, slide_op:id()}} | % message from RM that it knows the pred we expect
%{move, data, MovingData::db_dht:db_as_list(), MoveFullId::slide_op:id(), TargetId::?RT:key(), NextOp::slide_op:next_op()} |
    {{move, data, '_', '_', '_', '_'}, [], 1..5, reply_with_send_error} |
%{move, data_ack, MoveFullId::slide_op:id()} |
    {{move, data_ack, '_'}, [], 1..5, reply_with_send_error} |
%{move, delta, ChangedData::db_dht:db_as_list(), DeletedKeys::[?RT:key()], MoveFullId::slide_op:id()} |
    {{move, delta, '_', '_', '_'}, [], 1..5, reply_with_send_error} |
%%{move, delta_ack, MoveFullId::slide_op:id(), {none}} |
    {{move, delta_ack, '_', {none}}, [], 1..2, reply_with_send_error} |
%{move, delta_ack, MoveFullId::slide_op:id(), {continue, NewSlideId::slide_op:id()}} |
    {{move, delta_ack, '_', {continue, '_'}}, [], 1..2, reply_with_send_error}.
% note: do not lose local messages:
%{move, rm_db_range, MoveFullId::slide_op:id()} |
% note: there's no timeout for this message
%{move, done, MoveFullId::slide_op:id()} |

%% @doc Makes IgnoredMessages unique, i.e. only one msg per msg type.
-spec fix_tester_ignored_msg_list(IgnoredMessages::[move_message(),...]) -> [move_message(),...].
fix_tester_ignored_msg_list(IgnoredMessages) ->
    IMsg2 = lists:usort(fun(E1, E2) ->
                                erlang:element(2, erlang:element(1, E1)) =<
                                    erlang:element(2, erlang:element(1, E2))
                        end, IgnoredMessages),
    [begin
         NewAction =
             case Action of
                 reply_with_send_error -> fun reply_with_send_error/2;
                 X -> X
             end,
         {Msg, Conds, Count, NewAction}
     end || {Msg, Conds, Count, Action} <- IMsg2].

-spec send_ignore_msg_list_to(NthNode::1..4, PredOrSuccOrNode::pred | succ | node, IgnoredMessages::[move_message(),...]) -> ok.
send_ignore_msg_list_to(NthNode, PredOrSuccOrNode, IgnoredMessages) ->
    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],

    FailMsg = lists:flatten(io_lib:format("~.0p (~B.*) ignoring messages: ~.0p",
                                          [PredOrSuccOrNode, NthNode, IgnoredMessages])),
    {_PredsPred, Pred, Node, Succ} = get_pred_node_succ(NthNode, FailMsg),
    Other = case PredOrSuccOrNode of
                succ -> Succ;
                pred -> Pred;
                node -> Node
            end,
    comm:send(node:pidX(Other), {mockup_dht_node, add_match_specs, IgnoredMessages}).

%% @doc Test slide with successor, receiving data from it (using api_tx in the bench server), ignore (some) messages on succ.
-spec prop_symm4_slide_succ_rcv_load_timeouts_succ(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_succ_rcv_load_timeouts_succ(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, succ, IgnoredMessages),
    symm4_slide_load_test(1, succ, "slide_succ_rcv_timeouts_succ", fun id_my_to_succ_1_100/6, 1),
    send_ignore_msg_list_to(2, succ, IgnoredMessages),
    symm4_slide_load_test(2, succ, "slide_succ_rcv_timeouts_succ", fun id_my_to_succ_1_100/6, 1),
    send_ignore_msg_list_to(3, succ, IgnoredMessages),
    symm4_slide_load_test(3, succ, "slide_succ_rcv_timeouts_succ", fun id_my_to_succ_1_100/6, 1),
    send_ignore_msg_list_to(4, succ, IgnoredMessages),
    symm4_slide_load_test(4, succ, "slide_succ_rcv_timeouts_succ", fun id_my_to_succ_1_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with successor, receiving data from it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_succ_rcv_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_succ_rcv_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide_load_test(1, succ, "slide_succ_rcv_timeouts_node", fun id_my_to_succ_1_100/6, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide_load_test(2, succ, "slide_succ_rcv_timeouts_node", fun id_my_to_succ_1_100/6, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide_load_test(3, succ, "slide_succ_rcv_timeouts_node", fun id_my_to_succ_1_100/6, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide_load_test(4, succ, "slide_succ_rcv_timeouts_node", fun id_my_to_succ_1_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

tester_symm4_slide_succ_rcv_load_timeouts_succ(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    api_tx_SUITE:wait_for_dht_entries(440),
    tester:test(?MODULE, prop_symm4_slide_succ_rcv_load_timeouts_succ, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

tester_symm4_slide_succ_rcv_load_timeouts_node(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    api_tx_SUITE:wait_for_dht_entries(440),
    tester:test(?MODULE, prop_symm4_slide_succ_rcv_load_timeouts_node, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

%% @doc Test slide with successor, sending data to it (using api_tx in the bench server), ignore (some) messages on succ.
-spec prop_symm4_slide_succ_send_load_timeouts_succ(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_succ_send_load_timeouts_succ(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, succ, IgnoredMessages),
    symm4_slide_load_test(1, succ, "slide_succ_send_timeouts_succ", fun id_pred_to_my_99_100/6, 1),
    send_ignore_msg_list_to(2, succ, IgnoredMessages),
    symm4_slide_load_test(2, succ, "slide_succ_send_timeouts_succ", fun id_pred_to_my_99_100/6, 1),
    send_ignore_msg_list_to(3, succ, IgnoredMessages),
    symm4_slide_load_test(3, succ, "slide_succ_send_timeouts_succ", fun id_pred_to_my_99_100/6, 1),
    send_ignore_msg_list_to(4, succ, IgnoredMessages),
    symm4_slide_load_test(4, succ, "slide_succ_send_timeouts_succ", fun id_pred_to_my_99_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with successor, sending data to it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_succ_send_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_succ_send_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide_load_test(1, succ, "slide_succ_send_timeouts_node", fun id_pred_to_my_99_100/6, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide_load_test(2, succ, "slide_succ_send_timeouts_node", fun id_pred_to_my_99_100/6, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide_load_test(3, succ, "slide_succ_send_timeouts_node", fun id_pred_to_my_99_100/6, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide_load_test(4, succ, "slide_succ_send_timeouts_node", fun id_pred_to_my_99_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

tester_symm4_slide_succ_send_load_timeouts_succ(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    api_tx_SUITE:wait_for_dht_entries(440),
    tester:test(?MODULE, prop_symm4_slide_succ_send_load_timeouts_succ, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

tester_symm4_slide_succ_send_load_timeouts_node(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    api_tx_SUITE:wait_for_dht_entries(440),
    tester:test(?MODULE, prop_symm4_slide_succ_send_load_timeouts_node, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

%% @doc Test slide with predecessor, sending data to it (using api_tx in the bench server), ignore (some) messages on pred.
-spec prop_symm4_slide_pred_send_load_timeouts_pred(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_pred_send_load_timeouts_pred(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, pred, IgnoredMessages),
    symm4_slide_load_test(1, pred, "slide_pred_send_timeouts_pred", fun id_pred_to_my_1_100/6, 1),
    send_ignore_msg_list_to(2, pred, IgnoredMessages),
    symm4_slide_load_test(2, pred, "slide_pred_send_timeouts_pred", fun id_pred_to_my_1_100/6, 1),
    send_ignore_msg_list_to(3, pred, IgnoredMessages),
    symm4_slide_load_test(3, pred, "slide_pred_send_timeouts_pred", fun id_pred_to_my_1_100/6, 1),
    send_ignore_msg_list_to(4, pred, IgnoredMessages),
    symm4_slide_load_test(4, pred, "slide_pred_send_timeouts_pred", fun id_pred_to_my_1_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with predecessor, sending data to it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_pred_send_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_pred_send_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide_load_test(1, pred, "slide_pred_send_timeouts_node", fun id_pred_to_my_1_100/6, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide_load_test(2, pred, "slide_pred_send_timeouts_node", fun id_pred_to_my_1_100/6, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide_load_test(3, pred, "slide_pred_send_timeouts_node", fun id_pred_to_my_1_100/6, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide_load_test(4, pred, "slide_pred_send_timeouts_node", fun id_pred_to_my_1_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

tester_symm4_slide_pred_send_load_timeouts_pred(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    tester:test(?MODULE, prop_symm4_slide_pred_send_load_timeouts_pred, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

tester_symm4_slide_pred_send_load_timeouts_node(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    api_tx_SUITE:wait_for_dht_entries(440),
    tester:test(?MODULE, prop_symm4_slide_pred_send_load_timeouts_node, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

%% @doc Test slide with successor, receiving data from it (using api_tx in the bench server), ignore (some) messages on pred.
-spec prop_symm4_slide_pred_rcv_load_timeouts_pred(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_pred_rcv_load_timeouts_pred(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, pred, IgnoredMessages),
    symm4_slide_load_test(1, pred, "slide_pred_rcv_timeouts_pred", fun id_predspred_to_pred_99_100/6, 1),
    send_ignore_msg_list_to(2, pred, IgnoredMessages),
    symm4_slide_load_test(2, pred, "slide_pred_rcv_timeouts_pred", fun id_predspred_to_pred_99_100/6, 1),
    send_ignore_msg_list_to(3, pred, IgnoredMessages),
    symm4_slide_load_test(3, pred, "slide_pred_rcv_timeouts_pred", fun id_predspred_to_pred_99_100/6, 1),
    send_ignore_msg_list_to(4, pred, IgnoredMessages),
    symm4_slide_load_test(4, pred, "slide_pred_rcv_timeouts_pred", fun id_predspred_to_pred_99_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with predecessor, receiving data from it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_pred_rcv_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_pred_rcv_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide_load_test(1, pred, "slide_pred_rcv_timeouts_node", fun id_predspred_to_pred_99_100/6, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide_load_test(2, pred, "slide_pred_rcv_timeouts_node", fun id_predspred_to_pred_99_100/6, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide_load_test(3, pred, "slide_pred_rcv_timeouts_node", fun id_predspred_to_pred_99_100/6, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide_load_test(4, pred, "slide_pred_rcv_timeouts_node", fun id_predspred_to_pred_99_100/6, 1),

    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

tester_symm4_slide_pred_rcv_load_timeouts_pred(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    api_tx_SUITE:wait_for_dht_entries(440),
    tester:test(?MODULE, prop_symm4_slide_pred_rcv_load_timeouts_pred, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

tester_symm4_slide_pred_rcv_load_timeouts_node(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
    api_tx_SUITE:wait_for_dht_entries(440),
    tester:test(?MODULE, prop_symm4_slide_pred_rcv_load_timeouts_node, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

%%%%%%%%%%%%%%%%%%%%

-spec id_my_to_succ_1_100(PredsPredId::?RT:key(), PredId::?RT:key(), MyId::?RT:key(),
                          SuccId::?RT:key(), N::pos_integer(), Count::pos_integer())
        -> NewKey::?RT:key().
id_my_to_succ_1_100(_PredsPredId, _PredId, MyId, SuccId, _N, _Count) ->
    ?RT:get_split_key(MyId, SuccId, {1, 100}).

-spec id_pred_to_my_99_100(PredsPredId::?RT:key(), PredId::?RT:key(), MyId::?RT:key(),
                           SuccId::?RT:key(), N::pos_integer(), Count::pos_integer())
        -> NewKey::?RT:key().
id_pred_to_my_99_100(_PredsPredId, PredId, MyId, _SuccId, _N, _Count) ->
    ?RT:get_split_key(PredId, MyId, {99, 100}).

-spec id_pred_to_my_1_100(PredsPredId::?RT:key(), PredId::?RT:key(), MyId::?RT:key(),
                          SuccId::?RT:key(), N::pos_integer(), Count::pos_integer())
        -> NewKey::?RT:key().
id_pred_to_my_1_100(_PredsPredId, PredId, MyId, _SuccId, _N, _Count) ->
    ?RT:get_split_key(PredId, MyId, {1, 100}).

-spec id_predspred_to_pred_99_100(PredsPredId::?RT:key(), PredId::?RT:key(), MyId::?RT:key(),
                                  SuccId::?RT:key(), N::pos_integer(), Count::pos_integer())
        -> NewKey::?RT:key().
id_predspred_to_pred_99_100(PredsPredId, PredId, _MyId, _SuccId, _N, _Count) ->
    ?RT:get_split_key(PredsPredId, PredId, {99, 100}).

-spec symm4_slide_load_test(
        NthNode::1..4, PredOrSucc::pred | succ, Tag::string(),
        TargetIdFun::fun((PredsPredId::?RT:key(), PredId::?RT:key(), MyId::?RT:key(),
                          SuccId::?RT:key(), N::pos_integer(), Count::pos_integer()) -> TargetId::?RT:key()),
        Count::pos_integer()) -> ok.
symm4_slide_load_test(NthNode, PredOrSucc, Tag, TargetIdFun, Count) ->
    % get a random DHT node and let it slide with its successor/predecessor (Count times)
    DhtNode = lists:nth(NthNode, pid_groups:find_all(dht_node)),
    _ = [begin
             FailMsg = lists:flatten(
                         io_lib:format("slide_~.0p(~B.~B, ~.0p)",
                                       [PredOrSucc, NthNode, N, DhtNode])),
             {PredsPred, Pred, Node, Succ} = get_pred_node_succ(NthNode, FailMsg),
             TargetId = TargetIdFun(node:id(PredsPred), node:id(Pred),
                                    node:id(Node), node:id(Succ), N, Count),
             NewTag = lists:flatten(io_lib:format("~s-~B.~B", [Tag, NthNode, N])),
             Other = case PredOrSucc of
                         succ -> Succ;
                         pred -> Pred
                     end,
             ?proto_sched(start),
             symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, NewTag, NthNode, N, Node, Other),
             ?proto_sched(stop),
             % note: do not yield trace_mpath thread with "after 0"!
             receive
                 ?SCALARIS_RECV(Z,
                                ?ct_fail("slide_~.0p(~B.~B, ~.0p) unexpected message: ~.0p",
                                         [PredOrSucc, NthNode, N, DhtNode, Z]))
             after 0 -> ok
             end
         end || N <- lists:seq(1, Count)],
    ok.


-spec get_pred_node_succ(NthNode::1..4, FailMsg::string())
        -> {PredsPred::node:node_type(), Pred::node:node_type(),
            Node::node:node_type(), Succ::node:node_type()}.
get_pred_node_succ(NthNode, FailMsg) ->
    DhtNodePid = lists:nth(NthNode, pid_groups:find_all(dht_node)),
    {Pred1, Node1, Succ} = get_pred_node_succ2(DhtNodePid, FailMsg),
    {PredsPred, Pred2, Node2} = get_pred_node_succ2(node:pidX(Pred1), FailMsg),
    {PredsPred, node:newer(Pred1, Pred2), node:newer(Node1, Node2), Succ}.

-spec get_pred_node_succ2(NodePid::pid() | comm:mypid(), FailMsg::string())
        -> {Pred::node:node_type(), Node::node:node_type(), Succ::node:node_type()}.
get_pred_node_succ2(NodePid, FailMsg) when is_pid(NodePid) ->
    get_pred_node_succ2(comm:make_global(NodePid), FailMsg);
get_pred_node_succ2(NodePid, FailMsg) ->
    comm:send(NodePid, {get_node_details, comm:this(), [node, pred, succ]}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({get_node_details_response, NodeDetails},
                       begin
                           Node = node_details:get(NodeDetails, node),
                           Pred = node_details:get(NodeDetails, pred),
                           Succ = node_details:get(NodeDetails, succ),
                           {Pred, Node, Succ}
                       end);
        ?SCALARIS_RECV(Y, ?ct_fail("~s: unexpected message while "
                                   "waiting for get_node_details_response: ~.0p",
                                   [FailMsg, Y]))
    end.

-spec symm4_slide_load_test_slide(DhtNode::pid(), PredOrSucc::pred | succ,
        TargetId::?RT:key(), Tag::any(), NthNode::1..4, N::pos_integer(),
        Node::node:node_type(), Other::node:node_type()) -> ok.
symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, Tag, NthNode, N, Node, Other) ->
    comm:send_local(DhtNode, {move, start_slide, PredOrSucc, TargetId, Tag, comm:this()}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
        {move, result, Tag, ok}, begin
                                     %% ct:pal("~p.~p ~.0p -> ~.0p~n", [NthNode, N, node:id(Node), TargetId]),
                                     ok
                                 end);
        ?SCALARIS_RECV({move, result, Tag, Result},
                       begin
                           case lists:member(Result, [ongoing_slide, wrong_pred_succ_node]) of
                               true ->
                                   ct:pal("slide_~.0p(~B.~B, ~.0p, ~.0p, ~.0p) result: ~.0p~nretrying...~n",
                                          [PredOrSucc, NthNode, N, Node, Other, TargetId, Result]),
                                   timer:sleep(25), % wait a bit before trying again
                                   symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, Tag, NthNode, N, Node, Other);
                               _ ->
                                   ?ct_fail("slide_~.0p(~B.~B, ~.0p, ~.0p, ~.0p) result: ~.0p",
                                            [PredOrSucc, NthNode, N, Node, Other, TargetId, Result])
                           end
                       end);
        ?SCALARIS_RECV(X,
                       ?ct_fail("slide_~.0p(~B.~B, ~.0p, ~.0p, ~.0p) unexpected message: ~.0p",
                                [PredOrSucc, NthNode, N, Node, Other, TargetId, X]))
        end.

-spec stop_time(F::fun(() -> any()), Tag::string()) -> ok.
stop_time(F, Tag) ->
    Start = os:timestamp(),
    F(),
    Stop = os:timestamp(),
    ElapsedTime = erlang:max(1, timer:now_diff(Stop, Start)) / 1000000.0,
    Frequency = 1 / ElapsedTime,
    ct:pal("~p took ~ps: ~p1/s~n",
           [Tag, ElapsedTime, Frequency]),
    ok.

-spec check_size(Size::pos_integer()) -> ok.
check_size(Size) ->
    unittest_helper:check_ring_size(Size),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size(Size).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper functions simultaneous slides %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec gen_split_key_fun(node_type(), {integer(), integer()}) -> targetid_fun().
gen_split_key_fun(Selector, Fraction) ->
    fun(Nodes) ->
            From = select_from_nodes(Selector, Nodes),
            To = select_from_nodes(succ(Selector), Nodes),
            ?RT:get_split_key(node:id(From), node:id(To), Fraction)
    end.

-spec pred(node_type()) -> node_type().
pred(pred) -> predspred;
pred(node) -> pred;
pred(succ) -> node.

-spec succ(node_type()) -> node_type().
succ(predspred) -> pred;
succ(pred)      -> node;
succ(node)      -> succ.

-spec generate_slide_variation(slide_config_record()) -> [slide_config_record(),...].
generate_slide_variation(SlideConf) ->
    SlideWith = ?IIF(SlideConf#slideconf.slide_with =:= succ, pred, succ),
    Node =
        case SlideWith of
            pred -> succ(SlideConf#slideconf.node);
            succ -> pred(SlideConf#slideconf.node)
        end,
    Name = list_to_atom(string:concat(atom_to_list(SlideConf#slideconf.name), "var")),
    Variation = #slideconf{name = Name,
                           node = Node,
                           slide_with = SlideWith,
                           targetid = SlideConf#slideconf.targetid},
    [SlideConf, Variation].

-spec select_from_nodes(Selector::node_type(), Nodes::node_tuple()) -> node:node_type().
select_from_nodes(Selector, Nodes) ->
    N = selector_to_idx(Selector),
    element(N, Nodes).

-spec select_from_nodes(Node::node_type(), Direction::pred | succ, Nodes::node_tuple()) -> node:node_type().
select_from_nodes(Node, Direction, Nodes) ->
    NIdx = selector_to_idx(Node),
    Idx = case Direction of
              pred -> NIdx - 1;
              succ -> NIdx + 1
          end,
    element(Idx, Nodes).

-spec selector_to_idx(node_type()) -> 1..4.
selector_to_idx(predspred) -> 1;
selector_to_idx(pred) -> 2;
selector_to_idx(node) -> 3;
selector_to_idx(succ) -> 4.

-spec get_node_details(DhtNode::pid() | comm:mypid()) -> {node:node_type(), node:node_type(), node:node_type()}.
get_node_details(DhtNode) ->
    erlang:list_to_tuple(get_node_details(DhtNode, [pred, node, succ])).

-spec get_node_details(DhtNode::pid() | comm:mypid(), Which::[node | pred | succ,...])
        -> [node:node_type(),...].
get_node_details(DhtNode, Which) ->
    comm:send(comm:make_global(DhtNode), {get_node_details, comm:this(), Which}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({get_node_details_response, NodeDetails},
                       [node_details:get(NodeDetails, X) || X <- Which])
    end.

-spec get_predspred_pred_node_succ(DhtNode::pid()) -> node_tuple().
get_predspred_pred_node_succ(DhtNode) ->
    {Pred, Node, Succ} = get_node_details(DhtNode),
    {PredsPred, Pred2, Node2} = get_node_details(node:pidX(Pred)),
    % the nodes' RM info must be correct after each slide and thus before the
    % next one (which is when this function is called)
    ?equals_w_note(Pred, Pred2, wrong_pred_info_in_node),
    ?equals_w_note(Node2, Node, wrong_succ_info_in_pred),

    % make sure, all pred/succ info is correct:
    ?equals(admin:check_ring(), ok),

    {PredsPred, Pred, Node, Succ}.

-spec set_breakpoint(Pid::pid(), gen_component:bp_name()) -> ok.
set_breakpoint(Pid, Tag) ->
    gen_component:bp_set(Pid, move, Tag),
    gen_component:bp_barrier(Pid).

wait(Pid, Tag) ->
    gen_component:bp_del(Pid, Tag).

continue(Pid) ->
    gen_component:bp_cont(Pid).

-spec proto_sched_callback_on_send() -> proto_sched:callback_on_msg().
proto_sched_callback_on_send() ->
    This = self(),
    fun(_From, _To, {move, result, Tag, _Result}) ->
            comm:send_local(This, {end_of_slide,   Tag});
       (_From, _To, _Msg) ->
            ok
    end.

-spec proto_sched_callback_on_deliver() -> proto_sched:callback_on_msg().
proto_sched_callback_on_deliver() ->
    This = self(),
    fun(_From, _To, {move, start_slide, _, _, Tag, _}) ->
            comm:send_local(This, {begin_of_slide, Tag});
       (_From, _To, _Msg) ->
            ok
    end.

receive_result() ->
    % do not use SCALARIS_RECV since we receive messages from proto_sched
    % callbacks and we must not interfere with proto_sched here!
    receive
        X -> X
    end.

%% check for interleaving of slides using
%% FIFO properties of erlang message channels
-spec slide_interleaving() -> boolean().
slide_interleaving() ->
    case proto_sched:infected() of
        false -> true;
        true  ->
            %% first message
            {begin_of_slide, Tag} = receive_result(),
            %% next message
            Result =
                case receive_result() of
                    {end_of_slide, Tag}     -> false;
                    {begin_of_slide, _Tag2} -> true
                end,
            %% clear pending messages
            _ = receive_result(),
            _ = receive_result(),
            Result
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Execute simultaneous slides %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Execute simultaneous slides between two adjacent nodes,
%%      i.e. send data to the pred and receive from the succ
-spec slide_simultaneously(DhtNode::pid(), {SlideConf1::slide_config_record(), SlideConf2::slide_config_record()}, VerifyFun::fun()) -> ok.
slide_simultaneously(DhtNode, {SlideConf1, SlideConf2} = _Action, VerifyFun) ->
    SlideVariations1 = generate_slide_variation(SlideConf1),
    SlideVariations2 = generate_slide_variation(SlideConf2),
    _ = [ begin
              Nodes = get_predspred_pred_node_succ(DhtNode),
              Node1 = select_from_nodes(Slide1#slideconf.node, Nodes),
              Node2 = select_from_nodes(Slide2#slideconf.node, Nodes),
              Direction1 = Slide1#slideconf.slide_with,
              Direction2 = Slide2#slideconf.slide_with,
              TargetId1 = (Slide1#slideconf.targetid)(Nodes),
              TargetId2 = (Slide2#slideconf.targetid)(Nodes),
              Tag1 = Slide1#slideconf.name,
              Tag2 = Slide2#slideconf.name,
              PidLocal1 = comm:make_local(node:pidX(Node1)),
              PidLocal2 = comm:make_local(node:pidX(Node2)),
              ?proto_sched(start),
              ct:pal("Beginning~n"
                    " ~p~n  ~s (~p) with~n  ~s (~p)~n  to ~p,~n"
                    " ~p~n  ~s (~p) with~n  ~s (~p)~n  to ~p",
                     [Tag1, Slide1#slideconf.node, Node1, Direction1,
                      select_from_nodes(Slide1#slideconf.node, Direction1, Nodes), TargetId1,
                      Tag2, Slide2#slideconf.node, Node2, Direction2,
                      select_from_nodes(Slide2#slideconf.node, Direction2, Nodes), TargetId2]),
              %% We use breakpoints to assure simultaneous slides.
              %% As these don't work with the proto scheduler, we test the timing of the slides using
              %% a callback function instead. The function sends out messages begin_of_slide and
              %% end_of_slide messages for each slide.
              case proto_sched:infected() of
                  false ->
                      set_breakpoint(PidLocal1, slide1),
                      ?IIF(PidLocal1 =/= PidLocal2, set_breakpoint(PidLocal2, slide2), ok);
                  true  ->
                      proto_sched:register_callback(
                        proto_sched_callback_on_send(), on_send),
                      proto_sched:register_callback(
                        proto_sched_callback_on_deliver(), on_deliver)
              end,
              %% send out slides
              comm:send(node:pidX(Node1), {move, start_slide, Direction1, TargetId1, {slide1, Direction1, Tag1}, comm:this()}),
              comm:send(node:pidX(Node2), {move, start_slide, Direction2, TargetId2, {slide2, Direction2, Tag2}, comm:this()}),
              %% Continue once both slides have reached the gen_component
              case proto_sched:infected() of
                  false ->
                      wait(PidLocal1, slide1),
                      ?IIF(PidLocal1 =/= PidLocal2, wait(PidLocal2, slide2), ok),
                      continue(PidLocal1),
                      ?IIF(PidLocal1 =/= PidLocal2, continue(PidLocal2), ok);
                  true  ->
                      ok
              end,
              %% receive results
              % we need to process any slide result message and cannot filter
              % for a tag, otherwise we might hang if running with proto_sched
              % -> Result1 may be the result of Tag2, similar for Result2
              ReceiveResultFun =
                  fun() ->
                          trace_mpath:thread_yield(),
                          receive
                              ?SCALARIS_RECV({move, result, _Tag, Result},% ->
                                             Result)
                          end
                  end,
              Result1 = ReceiveResultFun(),
              Result2 = ReceiveResultFun(),
              % NOTE: must check interleaving inside proto_sched for
              %       slide_interleaving() to be able to distinguish between
              %       proto_sched and non-proto_sched runs
              Interleaving = slide_interleaving(),
              ct:pal("Result1: ~p,~nResult2: ~p~nInterleaving: ~p",
                     [Result1, Result2, Interleaving]),
              ?proto_sched(stop),
              _ = get_predspred_pred_node_succ(DhtNode),
              ct:pal("checked pred/succ info"),
              VerifyFun(Result1, Result2, Interleaving),
              timer:sleep(10)
          end || Slide1 <- SlideVariations1, Slide2 <- SlideVariations2],
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Test cases for simultaneous slides %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Test slides for two adjacent nodes
test_slide_adjacent(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 100000) end),
    % make sure all keys have been created...
    api_tx_SUITE:wait_for_dht_entries(440),
    VerifyFun =
        fun(Result1, Result2, _Interleaved) ->
                unittest_helper:check_ring_load(440),
                unittest_helper:check_ring_data(),
                %% both slides should be successfull
                ?assert_w_note(
                    Result1 =:= ok andalso Result2 =:= ok
                        orelse
                        %% rarely, one slide fails because the neighbour
                        %% information hasn't been updated yet
                          ((Result1 =:= wrong_pred_succ_node) xor
                               (Result2 =:= wrong_pred_succ_node)),
                    [{result1, Result1}, {result2, Result2}])
        end,
    Actions =
        [{
           #slideconf{name = action1slide1,
                      node = pred,
                      slide_with = succ,
                      targetid = gen_split_key_fun(pred, {1, 2})},
           #slideconf{name = action1slide2,
                      node = node,
                      slide_with = succ,
                      targetid = gen_split_key_fun(node, {1, 2})}
         },
         {
           #slideconf{name = action2slide1,
                      node = pred,
                      slide_with = succ,
                      targetid = gen_split_key_fun(pred, {1, 3})},
           #slideconf{name = action2slide2,
                      node = node,
                      slide_with = succ,
                      targetid = gen_split_key_fun(pred, {2, 3})}
         },
         {
           #slideconf{name = action3slide1,
                      node = pred,
                      slide_with = succ,
                      targetid = gen_split_key_fun(predspred, {1, 2})},
           #slideconf{name = action3slide2,
                      node = node,
                      slide_with = succ,
                      targetid = gen_split_key_fun(pred, {1, 2})}
         },
         {
           #slideconf{name = action4slide1,
                      node = pred,
                      slide_with = succ,
                      targetid = gen_split_key_fun(predspred, {1, 2})},
           #slideconf{name = action4slide2,
                      node = node,
                      slide_with = succ,
                      targetid = gen_split_key_fun(node, {1, 2})}
         }],
    DhtNodes = pid_groups:find_all(dht_node),
    _ = [begin
             slide_simultaneously(DhtNode, Action, VerifyFun)
         end || DhtNode <- DhtNodes, Action <- Actions],
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).


%% @doc Test for two slides in conflict with each other
test_slide_conflict(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 100000) end),
    % make sure all keys have been created...
    api_tx_SUITE:wait_for_dht_entries(440),
    DhtNodes = pid_groups:find_all(dht_node),
    VerifyFun =
        fun (Result1, Result2, Interleaved) ->
                unittest_helper:check_ring_load(440),
                unittest_helper:check_ring_data(),
                %% Outcome: at most one slide may succeed.
                %% We ensure slide interleaving using breakpoints. When using the proto scheduler
                %% we test for interleaving as we cannot use breakpoints.
                case Interleaved of
                    true ->
                        ?assert_w_note(Result1 =/= ok orelse Result2 =/= ok,
                                       [{result1, Result1}, {result2, Result2}]);
                    false ->
                        Vals = [ok, ongoing_slide, target_id_not_in_range, wrong_pred_succ_node],
                        ?assert_w_note(lists:member(Result1, Vals),
                                       {result1, Result1, 'not', Vals}),
                        ?assert_w_note(lists:member(Result2, Vals),
                                       {result2, Result2, 'not', Vals})
                end
        end,
    Actions = [
               {
                 #slideconf{name = action1slide1,
                            node = pred,
                            slide_with = succ,
                            targetid = gen_split_key_fun(pred, {3, 4})},
                 #slideconf{name = action1slide2,
                            node = node,
                            slide_with = succ,
                            targetid = gen_split_key_fun(pred, {1, 4})}
               },
               {
                 #slideconf{name = action2slide1,
                            node = node,
                            slide_with = succ,
                            targetid = gen_split_key_fun(pred, {1, 2})},
                 #slideconf{name = action2slide2,
                            node = node,
                            slide_with = succ,
                            targetid = gen_split_key_fun(node, {1, 2})}
               }],
    _ = [ begin
              slide_simultaneously(DhtNode, Action, VerifyFun)
          end || DhtNode <- DhtNodes, Action <- Actions],
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

%% @doc slide illegally, i.e. provide a target id impossible to slide to
test_slide_illegally(_Config) ->
    DhtNodes = pid_groups:find_all(dht_node),
    %% Two slides that can't be performed
    Slide1 = #slideconf{name = illegalslide1,
                        node = node,
                        slide_with = succ,
                        targetid = gen_split_key_fun(predspred, {1, 2})},
    Slide2 = #slideconf{name = illegalslide2,
                        node = pred,
                        slide_with = succ,
                        targetid = gen_split_key_fun(node, {1, 2})},
    %% Variations (different startup messages)
    Slides = generate_slide_variation(Slide1) ++ generate_slide_variation(Slide2),
    _ = [ begin
              Nodes = get_predspred_pred_node_succ(DhtNode),
              Node = select_from_nodes(Slide#slideconf.node, Nodes),
              Direction = Slide#slideconf.slide_with,
              TargetId = (Slide#slideconf.targetid)(Nodes),
              Tag = Slide#slideconf.name,
              ct:pal("Beginning ~p", [Tag]),
              ?proto_sched(start),
              comm:send(node:pidX(Node), {move, start_slide, Direction, TargetId, {slide, Direction, Tag}, comm:this()}),
              trace_mpath:thread_yield(),
              receive
                  ?SCALARIS_RECV({move, result, {slide, Direction, Tag}, target_id_not_in_range}, ok);
                  ?SCALARIS_RECV(X, ?ct_fail("Illegal Slide ~p", [X]))
              end,
              ?proto_sched(stop)
          end || DhtNode <- DhtNodes, Slide <- Slides].

%% @doc lets the tester run prop_jump_slide with different keys
tester_test_jump(_Config) ->
    tester:test(?MODULE, prop_jump_slide, 1, _Iterations=100).

%% @doc tests the jump operation.
-spec prop_jump_slide(TargetKey::?RT:key()) -> true.
prop_jump_slide(TargetKey) ->
    DhtNodes = pid_groups:find_all(dht_node),
    %% get a random node
    JumpingNode = util:randomelem(DhtNodes),
    %% check if we chose a valid key
    InvalidTarget = key_taken_as_node_id(TargetKey, JumpingNode),
    perform_jump(JumpingNode, TargetKey, InvalidTarget),
    true.

-spec perform_jump(JumpingNode::pid(), ?RT:key(), InvalidTarget::boolean()) -> true.
perform_jump(JumpingNode, TargetKey, InvalidTarget) ->
    %% get neighborhood to check if jump will be a slide
    comm:send_local(JumpingNode, {get_state, comm:this(), neighbors}),
    Neighbors = fun() -> receive ?SCALARIS_RECV({get_state_response, Neighb}, Neighb) end end(),
    ConvertedToSlide =
        case intervals:in(TargetKey, nodelist:node_range(Neighbors)) orelse
             intervals:in(TargetKey, nodelist:succ_range(Neighbors)) of
            true -> ct:pal("Jump will be converted to slide."),
                    true;
            _ -> false
        end,
    %% debug output in case of pending slide/jump operations
    comm:send_local(JumpingNode, {get_node_details, comm:this(), [slide_pred, slide_succ]}),
    SlideInfo = fun() -> receive ?SCALARIS_RECV({get_node_details_response, Details}, Details) end end(),
    [{slide_pred, SlidePred}, {slide_succ, SlideSucc}] = SlideInfo,
    ct:pal("SlidePred: ~p SlideSucc: ~p", [SlidePred, SlideSucc]),

    %% start jump
    Ring = lists:sort(fun(A, B) -> node:id(A) =< node:id(B) end,
                      [hd(get_node_details(DhtNode, [node])) || DhtNode <- pid_groups:find_all(dht_node)]),
    ct:pal("Ring: ~.7p~nNode ~p jumping to~n             ~p", [Ring, JumpingNode, TargetKey]),
    ?proto_sched(start),
    comm:send_local(JumpingNode, {move, start_jump, TargetKey, prop_jump_slide, comm:this()}),
    trace_mpath:thread_yield(),
    Result = fun() ->
                receive
                    ?SCALARIS_RECV({move, result, prop_jump_slide, Res}, Res)
                end
             end(),
    ct:pal("Result: ~p~n", [Result]),
    ?proto_sched(stop),
    % when the protocol finishes, the pred/succ infos in the ring should be correct!
    ?proto_sched(fun admin:check_ring/0), % only safe with proto_sched!
    %% check result
    if Result =:= wrong_pred_succ_node ->
            % should not happen with proto_sched!
            ?proto_sched(fun() -> ?ct_fail("Result should not be wrong_pred_succ_node", []) end),
            ct:pal("Retrying because of wrong_pred_succ_node"),
            timer:sleep(10),
            perform_jump(JumpingNode, TargetKey, InvalidTarget);
       InvalidTarget ->
            ?assert_w_note((ConvertedToSlide andalso Result =:= target_id_not_in_range)
                               orelse Result =:= ok, {ConvertedToSlide, Result}),
            %% check if the invalid target is really taken by another node
            ?assert(key_taken_as_node_id(TargetKey, JumpingNode)),
            %% and that it is different from ours
            comm:send_local(JumpingNode, {get_state, comm:this(), node_id}),
            MyId = fun() -> receive ?SCALARIS_RECV({get_state_response, Id}, Id) end end(),
            ?compare(fun erlang:'=/='/2, MyId, TargetKey);
       true ->
            ?equals(Result, ok)
    end.

-spec key_taken_as_node_id(?RT:key(), JumpingNode::comm:erl_local_pid()) -> boolean().
key_taken_as_node_id(Key, JumpingNode) ->
    DhtNodes = pid_groups:find_all(dht_node),
    lists:any(fun(Node) when Node =:= JumpingNode -> false;
                 (Node) ->
                %% get node information
                comm:send_local(Node, {get_state, comm:this(), node_id}),
                NodeId = fun() -> receive ?SCALARIS_RECV({get_state_response, Id}, Id) end end(),
                NodeId =:= Key
              end, DhtNodes).
