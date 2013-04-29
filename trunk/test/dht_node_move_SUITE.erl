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
%% @end
%% @version $Id$
-module(dht_node_move_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

test_cases() -> [].

all() ->
    [
     {group, send_to_pred},
     {group, send_to_pred_incremental},
     {group, send_to_succ},
     {group, send_to_succ_incremental}
    ] ++
%%     unittest_helper:create_ct_all(test_cases()).
        test_cases().

groups() ->
    GroupOptions = [sequence, {repeat, 1}],
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
    [
     {send_to_pred, GroupOptions, SendToPredTestCases},
     {send_to_pred_incremental, GroupOptions, SendToPredTestCases},
     {send_to_succ, GroupOptions, SendToSuccTestCases},
     {send_to_succ_incremental, GroupOptions, SendToSuccTestCases}
    ] ++
%%         unittest_helper:create_ct_groups(test_cases(), [{tester_symm4_slide_pred_send_load_timeouts_pred_incremental, [sequence, {repeat_until_any_fail, forever}]}]).
        [].

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) ->
    Config1 = unittest_helper:init_per_group(Group, Config),
    MoveConf = case Group of
                   send_to_pred ->
                       move_config_parameters();
                   send_to_pred_incremental ->
                       move_config_parameters_incremental();
                   send_to_succ ->
                       move_config_parameters();
                   send_to_succ_incremental ->
                       move_config_parameters_incremental()
               end,
    [{move_config, MoveConf} | Config1].

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    {move_config, MoveConf} = lists:keyfind(move_config, 1, Config),
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
    Config.

end_per_testcase(_TestCase, _Config) ->
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
                      symm4_slide1_load_test(1, succ, "slide_succ_rcv", fun succ_id_fun1/2, 50),
                      symm4_slide1_load_test(2, succ, "slide_succ_rcv", fun succ_id_fun1/2, 50),
                      symm4_slide1_load_test(3, succ, "slide_succ_rcv", fun succ_id_fun1/2, 50),
                      symm4_slide1_load_test(4, succ, "slide_succ_rcv", fun succ_id_fun1/2, 50),
                      erlang:exit(BenchPid, 'kill'),
                      util:wait_for_process_to_die(BenchPid)
              end, "symm4_slide_succ_rcv_load"),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data().

%% @doc Test slide with successor, sending data to it (using api_tx in the bench server).
symm4_slide_succ_send_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
                      symm4_slide2_load_test(1, succ, "slide_succ_send", fun succ_id_fun2/2, 50),
                      symm4_slide2_load_test(2, succ, "slide_succ_send", fun succ_id_fun2/2, 50),
                      symm4_slide2_load_test(3, succ, "slide_succ_send", fun succ_id_fun2/2, 50),
                      symm4_slide2_load_test(4, succ, "slide_succ_send", fun succ_id_fun2/2, 50),
                      erlang:exit(BenchPid, 'kill'),
                      util:wait_for_process_to_die(BenchPid)
              end, "symm4_slide_succ_send_load"),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data().

%% @doc Test slide with predecessor, sending data to it (using api_tx in the bench server).
symm4_slide_pred_send_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
                      symm4_slide1_load_test(1, pred, "slide_pred_send", fun pred_id_fun1/2, 50),
                      symm4_slide1_load_test(2, pred, "slide_pred_send", fun pred_id_fun1/2, 50),
                      symm4_slide1_load_test(3, pred, "slide_pred_send", fun pred_id_fun1/2, 50),
                      symm4_slide1_load_test(4, pred, "slide_pred_send", fun pred_id_fun1/2, 50),
                      erlang:exit(BenchPid, 'kill'),
                      util:wait_for_process_to_die(BenchPid)
              end, "symm4_slide_pred_send_load"),
    unittest_helper:check_ring_load(440),
    unittest_helper:check_ring_data().

%% @doc Test slide with predecessor, receiving data from it (using api_tx in the bench server).
symm4_slide_pred_rcv_load(_Config) ->
    stop_time(fun() ->
                      BenchPid = erlang:spawn(fun() -> bench:increment(10, 10000) end),
                      symm4_slide2_load_test(1, pred, "slide_pred_rcv", fun pred_id_fun2/2, 50),
                      symm4_slide2_load_test(2, pred, "slide_pred_rcv", fun pred_id_fun2/2, 50),
                      symm4_slide2_load_test(3, pred, "slide_pred_rcv", fun pred_id_fun2/2, 50),
                      symm4_slide2_load_test(4, pred, "slide_pred_rcv", fun pred_id_fun2/2, 50),
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
%{move, change_op, MoveFullId::slide_op:id(), TargetId::?RT:key(), NextOp::slide_op:next_op()} | % message from pred to succ that it has created a new (incremental) slide if succ has already set up the slide
    {{move, change_op, '_', '_', '_'}, [], 1..2, reply_with_send_error} |
%{move, change_id, MoveFullId::slide_op:id()} | % message from succ to pred if pred has already set up the slide
    {{move, change_id, '_'}, [], 1..2, reply_with_send_error} |
%{move, change_id, MoveFullId::slide_op:id(), TargetId::?RT:key(), NextOp::slide_op:next_op()} | % message from succ to pred if pred has already set up the slide but succ made it an incremental slide
    {{move, change_id, '_', '_', '_'}, [], 1..2, reply_with_send_error} |
%{move, slide_abort, pred | succ, MoveFullId::slide_op:id(), Reason::abort_reason()} |
    {{move, slide_abort, '_', '_', '_'}, [], 1, reply_with_send_error} |
% note: do not loose local messages:
%{move, node_update, Tag::{move, slide_op:id()}} | % message from RM that it has changed the node's id to TargetId
%{move, rm_new_pred, Tag::{move, slide_op:id()}} | % message from RM that it knows the pred we expect
%{move, req_data, MoveFullId::slide_op:id()} |
    {{move, req_data, '_'}, [], 1..2, reply_with_send_error} |
%{move, data, MovingData::?DB:db_as_list(), MoveFullId::slide_op:id()} |
    {{move, data, '_', '_'}, [], 1..5, reply_with_send_error} |
%{move, data_ack, MoveFullId::slide_op:id()} |
    {{move, data_ack, '_'}, [], 1..5, reply_with_send_error} |
%{move, delta, ChangedData::?DB:db_as_list(), DeletedKeys::[?RT:key()], MoveFullId::slide_op:id()} |
    {{move, delta, '_', '_', '_'}, [], 1..5, reply_with_send_error} |
%{move, delta_ack, MoveFullId::slide_op:id()} |
% note: this would result in the slide op being aborted with send_delta_timeout
%       since only send_delta_timeout will handle this but at this point, the
%       other node will not have this slide op anymore
%    {{move, delta_ack, '_'}, [], 1..2, reply_with_send_error} |
%{move, delta_ack, MoveFullId::slide_op:id(), continue, NewSlideId::slide_op:id()} |
    {{move, delta_ack, '_', continue, '_'}, [], 1..2, reply_with_send_error} |
%{move, delta_ack, MoveFullId::slide_op:id(), OtherType::slide_op:type(), NewSlideId::slide_op:id(), InitNode::node:node_type(), TargetNode::node:node_type(), TargetId::?RT:key(), Tag::any(), MaxTransportEntries::pos_integer()} |
    {{move, delta_ack, '_', '_', '_', '_', '_', '_', '_', '_'}, [], 1..2, reply_with_send_error}.
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
    {Pred, Node, Succ} = get_pred_node_succ(NthNode, FailMsg),
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
    symm4_slide1_load_test(1, succ, "slide_succ_rcv_timeouts_succ", fun succ_id_fun1/2, 1),
    send_ignore_msg_list_to(2, succ, IgnoredMessages),
    symm4_slide1_load_test(2, succ, "slide_succ_rcv_timeouts_succ", fun succ_id_fun1/2, 1),
    send_ignore_msg_list_to(3, succ, IgnoredMessages),
    symm4_slide1_load_test(3, succ, "slide_succ_rcv_timeouts_succ", fun succ_id_fun1/2, 1),
    send_ignore_msg_list_to(4, succ, IgnoredMessages),
    symm4_slide1_load_test(4, succ, "slide_succ_rcv_timeouts_succ", fun succ_id_fun1/2, 1),
    
    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with successor, receiving data from it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_succ_rcv_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_succ_rcv_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),
    
    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide1_load_test(1, succ, "slide_succ_rcv_timeouts_node", fun succ_id_fun1/2, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide1_load_test(2, succ, "slide_succ_rcv_timeouts_node", fun succ_id_fun1/2, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide1_load_test(3, succ, "slide_succ_rcv_timeouts_node", fun succ_id_fun1/2, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide1_load_test(4, succ, "slide_succ_rcv_timeouts_node", fun succ_id_fun1/2, 1),
    
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
    
    send_ignore_msg_list_to(1, succ, IgnoredMessages),    symm4_slide2_load_test(1, succ, "slide_succ_send_timeouts_succ", fun succ_id_fun2/2, 1),
    send_ignore_msg_list_to(2, succ, IgnoredMessages),
    symm4_slide2_load_test(2, succ, "slide_succ_send_timeouts_succ", fun succ_id_fun2/2, 1),
    send_ignore_msg_list_to(3, succ, IgnoredMessages),
    symm4_slide2_load_test(3, succ, "slide_succ_send_timeouts_succ", fun succ_id_fun2/2, 1),
    send_ignore_msg_list_to(4, succ, IgnoredMessages),
    symm4_slide2_load_test(4, succ, "slide_succ_send_timeouts_succ", fun succ_id_fun2/2, 1),
    
    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with successor, sending data to it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_succ_send_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_succ_send_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),
    
    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide2_load_test(1, succ, "slide_succ_send_timeouts_node", fun succ_id_fun2/2, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide2_load_test(2, succ, "slide_succ_send_timeouts_node", fun succ_id_fun2/2, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide2_load_test(3, succ, "slide_succ_send_timeouts_node", fun succ_id_fun2/2, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide2_load_test(4, succ, "slide_succ_send_timeouts_node", fun succ_id_fun2/2, 1),
    
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
    symm4_slide1_load_test(1, pred, "slide_pred_send_timeouts_pred", fun pred_id_fun1/2, 1),
    send_ignore_msg_list_to(2, pred, IgnoredMessages),
    symm4_slide1_load_test(2, pred, "slide_pred_send_timeouts_pred", fun pred_id_fun1/2, 1),
    send_ignore_msg_list_to(3, pred, IgnoredMessages),
    symm4_slide1_load_test(3, pred, "slide_pred_send_timeouts_pred", fun pred_id_fun1/2, 1),
    send_ignore_msg_list_to(4, pred, IgnoredMessages),
    symm4_slide1_load_test(4, pred, "slide_pred_send_timeouts_pred", fun pred_id_fun1/2, 1),
    
    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with predecessor, sending data to it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_pred_send_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_pred_send_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),
    
    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide1_load_test(1, pred, "slide_pred_send_timeouts_node", fun pred_id_fun1/2, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide1_load_test(2, pred, "slide_pred_send_timeouts_node", fun pred_id_fun1/2, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide1_load_test(3, pred, "slide_pred_send_timeouts_node", fun pred_id_fun1/2, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide1_load_test(4, pred, "slide_pred_send_timeouts_node", fun pred_id_fun1/2, 1),
    
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
    symm4_slide2_load_test(1, pred, "slide_pred_rcv_timeouts_pred", fun pred_id_fun2/2, 1),
    send_ignore_msg_list_to(2, pred, IgnoredMessages),
    symm4_slide2_load_test(2, pred, "slide_pred_rcv_timeouts_pred", fun pred_id_fun2/2, 1),
    send_ignore_msg_list_to(3, pred, IgnoredMessages),
    symm4_slide2_load_test(3, pred, "slide_pred_rcv_timeouts_pred", fun pred_id_fun2/2, 1),
    send_ignore_msg_list_to(4, pred, IgnoredMessages),
    symm4_slide2_load_test(4, pred, "slide_pred_rcv_timeouts_pred", fun pred_id_fun2/2, 1),
    
    % cleanup, just in case:
    _ = [comm:send_local(DhtNodePid, {mockup_dht_node, clear_match_specs})
           || DhtNodePid <- pid_groups:find_all(dht_node)],
    true.

%% @doc Test slide with predecessor, receiving data from it (using api_tx in the bench server), ignore (some) messages on node.
-spec prop_symm4_slide_pred_rcv_load_timeouts_node(IgnoredMessages::[move_message(),...]) -> true.
prop_symm4_slide_pred_rcv_load_timeouts_node(IgnoredMessages_) ->
    IgnoredMessages = fix_tester_ignored_msg_list(IgnoredMessages_),

    send_ignore_msg_list_to(1, node, IgnoredMessages),
    symm4_slide2_load_test(1, pred, "slide_pred_rcv_timeouts_node", fun pred_id_fun2/2, 1),
    send_ignore_msg_list_to(2, node, IgnoredMessages),
    symm4_slide2_load_test(2, pred, "slide_pred_rcv_timeouts_node", fun pred_id_fun2/2, 1),
    send_ignore_msg_list_to(3, node, IgnoredMessages),
    symm4_slide2_load_test(3, pred, "slide_pred_rcv_timeouts_node", fun pred_id_fun2/2, 1),
    send_ignore_msg_list_to(4, node, IgnoredMessages),
    symm4_slide2_load_test(4, pred, "slide_pred_rcv_timeouts_node", fun pred_id_fun2/2, 1),

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

succ_id_fun1(MyId, SuccId) ->
    ?RT:get_split_key(MyId, SuccId, {1, 100}).

succ_id_fun2(MyId, PredId) ->
    ?RT:get_split_key(PredId, MyId, {99, 100}).

pred_id_fun1(MyId, PredId) ->
    ?RT:get_split_key(PredId, MyId, {1, 100}).

pred_id_fun2(PredId, PredsPredId) ->
    ?RT:get_split_key(PredsPredId, PredId, {99, 100}).

-spec symm4_slide1_load_test(NthNode::1..4, PredOrSucc::pred | succ, Tag::string(),
        TargetIdFun::fun((MyId::?RT:key(), OtherId::?RT:key()) -> TargetId::?RT:key()),
        Count::pos_integer()) -> ok.
symm4_slide1_load_test(NthNode, PredOrSucc, Tag, TargetIdFun, Count) ->
    % get a random DHT node and let it slide with its successor/predecessor (Count times)
    DhtNode = lists:nth(NthNode, pid_groups:find_all(dht_node)),
    _ = [begin
             FailMsg = lists:flatten(
                         io_lib:format("slide_~.0p(~B.~B, ~.0p)",
                                       [PredOrSucc, NthNode, N, DhtNode])),
             {Pred, Node, Succ} = get_pred_node_succ(NthNode, FailMsg),
             Other = case PredOrSucc of
                         succ -> Succ;
                         pred -> Pred
                     end,
             TargetId = TargetIdFun(node:id(Node), node:id(Other)),
             NewTag = lists:flatten(io_lib:format("~s-~B.~B", [Tag, NthNode, N])),
             symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, NewTag, NthNode, N, Node, Other),
             receive Z ->
                         ?ct_fail("slide_~.0p(~B.~B, ~.0p) unexpected message: ~.0p",
                                  [PredOrSucc, NthNode, N, DhtNode, Z])
             after 0 -> ok
             end
         end || N <- lists:seq(1, Count)],
    ok.

% note: calls TargetIdFun(MyId, PredId) for PredOrSucc =:= succ and
%       TargetIdFun(PredId, PredsPredId) for PredOrSucc =:= pred
-spec symm4_slide2_load_test(NthNode::1..4, PredOrSucc::pred | succ, Tag::string(),
        TargetIdFun::fun((MyId_or_PredId::?RT:key(), PredId_or_PredsPredId::?RT:key()) -> TargetId::?RT:key()),
        Count::pos_integer()) -> ok.
symm4_slide2_load_test(NthNode, PredOrSucc, Tag, TargetIdFun, Count) ->
    % get a random DHT node and let it slide with its successor/predecessor (Count times)
    DhtNode = lists:nth(NthNode, pid_groups:find_all(dht_node)),
    _ = [begin
             FailMsg = lists:flatten(
                         io_lib:format("slide_~.0p(~B.~B, ~.0p)",
                                       [PredOrSucc, NthNode, N, DhtNode])),
             {Pred, Node, Succ} = get_pred_node_succ(NthNode, FailMsg),
             {TargetId, Other} =
                 case PredOrSucc of
                     succ -> {TargetIdFun(node:id(Node), node:id(Pred)), Succ};
                     pred ->
                         {PredsPred, _, _} = get_pred_node_succ2(node:pidX(Pred), FailMsg),
                         {TargetIdFun(node:id(Pred), node:id(PredsPred)), Pred}
                 end,
             NewTag = lists:flatten(io_lib:format("~s-~B.~B", [Tag, NthNode, N])),
             symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, NewTag, NthNode, N, Node, Other),
             receive Z ->
                         ?ct_fail("slide_~.0p(~B.~B, ~.0p) unexpected message: ~.0p",
                                  [PredOrSucc, NthNode, N, DhtNode, Z])
             after 0 -> ok
             end
         end || N <- lists:seq(1, Count)],
    ok.


-spec get_pred_node_succ(NthNode::1..4, FailMsg::string()) -> {Pred::node:node_type(), Node::node:node_type(), Succ::node:node_type()}.
get_pred_node_succ(NthNode, FailMsg) ->
    DhtNodePid = lists:nth(NthNode, pid_groups:find_all(dht_node)),
    get_pred_node_succ2(DhtNodePid, FailMsg).

-spec get_pred_node_succ2(NodePid::pid() | comm:mypid(), FailMsg::string()) -> {Pred::node:node_type(), Node::node:node_type(), Succ::node:node_type()}.
get_pred_node_succ2(NodePid, FailMsg) when is_pid(NodePid) ->
    get_pred_node_succ2(comm:make_global(NodePid), FailMsg);
get_pred_node_succ2(NodePid, FailMsg) ->
    comm:send(NodePid, {get_node_details, comm:this(), [node, pred, succ]}),
    receive
        {get_node_details_response, NodeDetails} ->
            Node = node_details:get(NodeDetails, node),
            Pred = node_details:get(NodeDetails, pred),
            Succ = node_details:get(NodeDetails, succ),
            {Pred, Node, Succ};
        Y ->
            ?ct_fail("~s: unexpected message while "
                         "waiting for get_node_details_response: ~.0p",
                         [FailMsg, Y])
    end.

-spec symm4_slide_load_test_slide(DhtNode::pid(), PredOrSucc::pred | succ,
        TargetId::?RT:key(), Tag::any(), NthNode::1..4, N::pos_integer(),
        Node::node:node_type(), Other::node:node_type()) -> ok.
symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, Tag, NthNode, N, Node, Other) ->
    comm:send_local(DhtNode, {move, start_slide, PredOrSucc, TargetId, Tag, self()}),
    receive
        {move, result, Tag, ok} ->
%%             ct:pal("~p.~p ~.0p -> ~.0p~n", [NthNode, N, node:id(Node), TargetId]),
            ok;
        {move, result, Tag, Result} ->
            case lists:member(Result, [ongoing_slide, wrong_pred_succ_node]) of
                true ->
                    ct:pal("slide_~.0p(~B.~B, ~.0p, ~.0p, ~.0p) result: ~.0p~nretrying...~n",
                           [PredOrSucc, NthNode, N, Node, Other, TargetId, Result]),
                    timer:sleep(100), % wait a bit before trying again
                    symm4_slide_load_test_slide(DhtNode, PredOrSucc, TargetId, Tag, NthNode, N, Node, Other);
                _ ->
                    ?ct_fail("slide_~.0p(~B.~B, ~.0p, ~.0p, ~.0p) result: ~.0p",
                             [PredOrSucc, NthNode, N, Node, Other, TargetId, Result])
            end;
        X ->
            ?ct_fail("slide_~.0p(~B.~B, ~.0p, ~.0p, ~.0p) unexpected message: ~.0p",
                     [PredOrSucc, NthNode, N, Node, Other, TargetId, X])
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
