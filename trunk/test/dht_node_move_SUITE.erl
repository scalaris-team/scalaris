% @copyright 2010-2012 Zuse Institute Berlin

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
%%   First executes all tests using the erlang scheduler. Then executes using
%%   the proto scheduler which produces a random interleaving between the
%%   different channels.
%% @end
%% @version $Id$
-module(dht_node_move_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

-define(NUM_SLIDES, 50).

test_cases() -> [].

all() ->
    [
     {group, send_to_pred},
     {group, send_to_pred_incremental},
     {group, send_to_succ},
     {group, send_to_succ_incremental}]
      ++
    [
     {group, send_to_pred_proto_sched},
     {group, send_to_pred_incremental_proto_sched},
     {group, send_to_succ_proto_sched},
     {group, send_to_succ_incremental_proto_sched}
    ].

groups() ->
    MoveConfig = {move_config, move_config_parameters()},
    MoveConfigInc = {move_config, move_config_parameters_incremental()},
    GroupOptions = [sequence, {repeat, 1}],
    Config = [MoveConfig | GroupOptions],
    ConfigInc = [MoveConfigInc | GroupOptions],
    ConfigProtoSched = [proto_sched | Config],
    ConfigIncProtoSched = [proto_sched | ConfigInc],
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
    [ %% Groups started with normal configuration
      {send_to_pred, Config, SendToPredTestCases},
      {send_to_pred_incremental, ConfigInc, SendToPredTestCases},
      {send_to_succ, Config, SendToSuccTestCases},
      {send_to_succ_incremental, ConfigInc, SendToSuccTestCases},
      {send_to_pred, Config, SendToPredTestCases}
    ]
      ++
    [ %% Groups started with proto scheduler
     {send_to_pred_proto_sched, ConfigProtoSched, SendToPredTestCases},
     {send_to_pred_incremental_proto_sched, ConfigIncProtoSched, SendToPredTestCases},
     {send_to_succ_proto_sched, ConfigProtoSched, SendToSuccTestCases},
     {send_to_succ_incremental_proto_sched, ConfigIncProtoSched, SendToSuccTestCases}
    ]
      ++
%%         unittest_helper:create_ct_groups(test_cases(), [{tester_symm4_slide_pred_send_load_timeouts_pred_incremental, [sequence, {repeat_until_any_fail, forever}]}]).
        [].

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) ->
    unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    GroupConfig = proplists:get_value(tc_group_properties, Config, []),
    {move_config, MoveConf} = lists:keyfind(move_config, 1, GroupConfig),
    unittest_helper:make_ring_with_ids(
      fun() -> ?RT:get_replica_keys(?RT:hash_key("0")) end,
      [{config, [{log_path, PrivDir}, {dht_node, mockup_dht_node}, {monitor_perf_interval, 0},
                 {join_lb_psv, lb_psv_simple}, {lb_psv_samples, 1}]
            ++ MoveConf}]),
    % wait for all nodes to finish their join before writing data
    unittest_helper:check_ring_size_fully_joined(4),
    %% write some data (use a function because left-over tx_timeout messages can disturb the tests):
    Pid = erlang:spawn(fun() ->
                               _ = [api_tx:write(erlang:integer_to_list(X), X) || X <- lists:seq(1, 100)]
                       end),
    util:wait_for_process_to_die(Pid),
    timer:sleep(500), % wait a bit for the rm-processes to settle
    %% start proto scheduler if applicable
    GroupConfig = proplists:get_value(tc_group_properties, Config, []),
    case proplists:is_defined(proto_sched, GroupConfig) of
        true -> proto_sched:start(),
                proto_sched:start_deliver();
        _ -> ok
    end,
    Config.

end_per_testcase(_TestCase, Config) ->
    %% stop proto scheduler if applicable
    GroupConfig = proplists:get_value(tc_group_properties, Config, []),
    case proplists:is_defined(proto_sched, GroupConfig) of
        true ->
            proto_sched:stop(),
            ct:pal("Proto schedluer infos: ~.2p", proto_sched:get_infos()),
            proto_sched:cleanup();
        _ -> ok
    end,
    unittest_helper:stop_ring(),
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

%% @doc Test slide with successor, receiving data from it (using api_tx in the bench server).
symm4_slide_succ_rcv_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
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
%{move, data, MovingData::?DB:db_as_list(), MoveFullId::slide_op:id(), TargetId::?RT:key(), NextOp::slide_op:next_op()} |
    {{move, data, '_', '_', '_', '_'}, [], 1..5, reply_with_send_error} |
%{move, data_ack, MoveFullId::slide_op:id()} |
    {{move, data_ack, '_'}, [], 1..5, reply_with_send_error} |
%{move, delta, ChangedData::?DB:db_as_list(), DeletedKeys::[?RT:key()], MoveFullId::slide_op:id()} |
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
    tester:test(?MODULE, prop_symm4_slide_succ_rcv_load_timeouts_succ, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

tester_symm4_slide_succ_rcv_load_timeouts_node(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
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
    tester:test(?MODULE, prop_symm4_slide_succ_send_load_timeouts_succ, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

tester_symm4_slide_succ_send_load_timeouts_node(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
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
    tester:test(?MODULE, prop_symm4_slide_pred_rcv_load_timeouts_pred, 1, 25),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data(),
    erlang:exit(BenchPid, 'kill'),
    util:wait_for_process_to_die(BenchPid).

tester_symm4_slide_pred_rcv_load_timeouts_node(_Config) ->
    BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
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
             symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, NewTag, NthNode, N, Node, Other),
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
    comm:send_local(DhtNode, {move, start_slide, PredOrSucc, TargetId, Tag, self()}),
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
                                   timer:sleep(100), % wait a bit before trying again
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
    unittest_helper:check_ring_size(Size).
