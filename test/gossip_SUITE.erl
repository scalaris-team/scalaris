%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    gossip_SUITE.erl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Tests for the gossip module.
%%% @end
%%% Created : 29 Mar 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(gossip_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include_lib("unittest.hrl").

all() ->
    [test_init,
     test_on_trigger1,
     test_on_trigger2,
     test_on_trigger3,
     test_on_trigger4,
     test_on_trigger5,
     test_on_trigger6,
     test_on_trigger7,
     test_on_get_node_details_response_local_info1,
     test_on_get_node_details_response_local_info2,
     test_on_get_node_details_response_local_info3,
     test_on_get_node_details_response_local_info4,
     test_on_get_node_details_response_local_info5,
     test_on_get_node_details_response_local_info6,
     test_on_get_node_details_response_local_info7,
     test_on_get_node_details_response_leader_start_new_round1,
     test_on_get_node_details_response_leader_start_new_round2,
     test_on_get_node_details_response_leader_start_new_round3,
     test_on_get_node_details_response_leader_start_new_round4,
     test_on_get_node_details_response_leader_start_new_round5,
     test_on_get_node_details_response_leader_start_new_round6,
     test_on_get_node_details_response_leader_start_new_round7,
     test_on_get_state,
     test_on_get_state_response,
     test_on_cy_cache1,
     test_on_cy_cache2,
     test_on_cy_cache3,
     test_on_get_values_best1,
     test_on_get_values_best2,
     test_on_get_values_best3,
     test_on_get_values_best4,
     test_on_get_values_best5,
     test_on_get_values_best6,
     test_on_get_values_all1,
     test_on_get_values_all2,
     test_on_get_values_all3,
     test_on_get_values_all4,
     test_on_get_values_all5,
     test_on_get_values_all6,
     test_get_values_best0,
     test_get_values_best1,
     test_get_values_all0,
     test_get_values_all1].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    error_logger:tty(true),
    Owner = self(),
    Pid = spawn(fun () ->
                        crypto:start(),
                        process_dictionary:start_link(),
                        config:start_link(["scalaris.cfg", "scalaris.local.cfg"]),
                        comm_port:start_link(),
                        timer:sleep(1000),
                        comm_port:set_local_address({127,0,0,1},14195),
                        application:start(log4erl),
                        Owner ! {continue},
                        receive
                            {done} ->
                                ok
                        end
                end),
    receive
        {continue} ->
            ok
    end,
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    reset_config(),
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    gen_component:kill(process_dictionary),
    error_logger:tty(false),
    exit(Pid, kill),
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    reset_config(),
    Config.

test_init(Config) ->
    config:write(gossip_interval, 100),
    InitialState1 = gossip:init('trigger_periodic'),
    
    GossipNewState = gossip_state:new_state(),
    ?equals_pattern(InitialState1, {GossipNewState, GossipNewState, [], {'trigger_periodic', _TriggerState}}),
    ?expect_message({trigger}),
    ?expect_no_message(),
    Config.

test_get_values_best0(Config) ->
    process_dictionary:register_process("gossip_group", gossip, self()),

    gossip:get_values_best(),
    ?expect_message({get_values_best, _Pid}),
    ?expect_no_message(),
    Config.

test_get_values_best1(Config) ->
    process_dictionary:register_process("gossip_group", gossip, self()),

    SelfPid = self(),
    gossip:get_values_best(self()),
    ?expect_message({get_values_best, SelfPid}),
    ?expect_no_message(),
    Config.

test_get_values_all0(Config) ->
    process_dictionary:register_process("gossip_group", gossip, self()),

    gossip:get_values_all(),
    ?expect_message({get_values_all, _Pid}),
    ?expect_no_message(),
    Config.

test_get_values_all1(Config) ->
    process_dictionary:register_process("gossip_group", gossip, self()),

    SelfPid = self(),
    gossip:get_values_all(self()),
    ?expect_message({get_values_all, SelfPid}),
    ?expect_no_message(),
    Config.

test_on_trigger1(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    process_dictionary:register_process("gossip_group", cyclon, self()),
    
    GossipNewValues = gossip_state:new_internal(),
%%     Self = self(),
    
    config:write(gossip_min_triggers_per_round, 2),
    config:write(gossip_max_triggers_per_round, 4),
    config:write(gossip_converge_avg_count_start_new_round, 1),
    
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({trigger}, {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(gossip_state:get(NewState, values), GossipNewValues),
    ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
    ?equals(gossip_state:get(NewState, triggered), 1),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({trigger}),
    % round = 0 -> request for new round will be send
    ThisWithCookie = comm:this_with_cookie(leader_start_new_round),
    ?expect_message({get_node_details, ThisWithCookie, [my_range]}),
    % neither initialized nor round > 0 -> no request for random node
%%     ?expect_message({get_subset_rand, 1, Self}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_trigger2(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    process_dictionary:register_process("gossip_group", cyclon, self()),
    
    GossipNewValues = gossip_state:new_internal(),
%%     Self = self(),
    
    config:write(gossip_min_triggers_per_round, 2),
    config:write(gossip_max_triggers_per_round, 4),
    config:write(gossip_converge_avg_count_start_new_round, 1),
    
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, true, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({trigger}, {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(gossip_state:get(NewState, values), GossipNewValues),
    ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
    ?equals(gossip_state:get(NewState, triggered), 1),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({trigger}),
    % round = 0 -> request for new round will be send
    ThisWithCookie = comm:this_with_cookie(leader_start_new_round),
    ?expect_message({get_node_details, ThisWithCookie, [my_range]}),
    % initialized but not round > 0 -> no request for random node
%%     ?expect_message({get_subset_rand, 1, Self}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_trigger3(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    process_dictionary:register_process("gossip_group", cyclon, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    Self = self(),
    
    config:write(gossip_min_triggers_per_round, 2),
    config:write(gossip_max_triggers_per_round, 4),
    config:write(gossip_converge_avg_count_start_new_round, 1),
    
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({trigger}, {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(gossip_state:get(NewState, values), Values),
    ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
    ?equals(gossip_state:get(NewState, triggered), 1),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({trigger}),
    % round != 0 and triggers (1) <= min_tpr (2) -> request for new round will not be send
%%     ThisWithCookie = comm:this_with_cookie(leader_start_new_round),
%%     ?expect_message({get_node_details, ThisWithCookie, [my_range]}),
    % initialized and round > 0 -> request for random node will be send
    ?expect_message({get_subset_rand, 1, Self}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_trigger4(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    process_dictionary:register_process("gossip_group", cyclon, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    Self = self(),
    
    config:write(gossip_min_triggers_per_round, 2),
    config:write(gossip_max_triggers_per_round, 4),
    config:write(gossip_converge_avg_count_start_new_round, 1),
    
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 1, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 1, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({trigger}, {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(gossip_state:get(NewState, values), Values),
    ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
    ?equals(gossip_state:get(NewState, triggered), 2),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({trigger}),
    % round != 0 and triggers (2) <= min_tpr (2) -> request for new round will not be send
%%     ThisWithCookie = comm:this_with_cookie(leader_start_new_round),
%%     ?expect_message({get_node_details, ThisWithCookie, [my_range]}),
    % initialized and round > 0 -> request for random node will be send
    ?expect_message({get_subset_rand, 1, Self}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_trigger5(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    process_dictionary:register_process("gossip_group", cyclon, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    Self = self(),
    
    config:write(gossip_min_triggers_per_round, 2),
    config:write(gossip_max_triggers_per_round, 4),
    config:write(gossip_converge_avg_count_start_new_round, 1),

    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 2, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 2, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({trigger}, {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(gossip_state:get(NewState, values), Values),
    ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
    ?equals(gossip_state:get(NewState, triggered), 3),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({trigger}),
    % round != 0 and triggers (3) > min_tpr (2) and conv_avg_count (0) < conv_avg_count_newround (1) -> request for new round will not be send
%%     ThisWithCookie = comm:this_with_cookie(leader_start_new_round),
%%     ?expect_message({get_node_details, ThisWithCookie, [my_range]}),
    % initialized and round > 0 -> request for random node will be send
    ?expect_message({get_subset_rand, 1, Self}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_trigger6(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    process_dictionary:register_process("gossip_group", cyclon, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    Self = self(),
    
    config:write(gossip_min_triggers_per_round, 2),
    config:write(gossip_max_triggers_per_round, 4),
    config:write(gossip_converge_avg_count_start_new_round, 1),

    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 3, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 3, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({trigger}, {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(gossip_state:get(NewState, values), Values),
    ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
    ?equals(gossip_state:get(NewState, triggered), 4),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({trigger}),
    % round != 0 and triggers (4) > min_tpr (2) and conv_avg_count (0) < conv_avg_count_newround (1) -> request for new round will not be send
%%     ThisWithCookie = comm:this_with_cookie(leader_start_new_round),
%%     ?expect_message({get_node_details, ThisWithCookie, [my_range]}),
    % initialized and round > 0 -> request for random node will be send
    ?expect_message({get_subset_rand, 1, Self}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_trigger7(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    process_dictionary:register_process("gossip_group", cyclon, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    Self = self(),
    
    config:write(gossip_min_triggers_per_round, 2),
    config:write(gossip_max_triggers_per_round, 4),
    config:write(gossip_converge_avg_count_start_new_round, 1),

    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 4, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 4, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({trigger}, {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(gossip_state:get(NewState, values), Values),
    ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
    ?equals(gossip_state:get(NewState, triggered), 5),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({trigger}),
    % round != 0 and triggers (5) > max_tpr (4) -> request for new round will be send
    ThisWithCookie = comm:this_with_cookie(leader_start_new_round),
    ?expect_message({get_node_details, ThisWithCookie, [my_range]}),
    % initialized and round > 0 -> request for random node will be send
    ?expect_message({get_subset_rand, 1, Self}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_local_info1(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    NodeDetails = node_details:set(
                    node_details:set(
                      node_details:set(
                        node_details:new(), node, node:new(pid, 1, 0)),
                      pred, node:new(pid, 0, 0)),
                    load, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, true, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, local_info},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages should be send if already initialized
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_local_info2(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    NodeDetails = node_details:set(
                    node_details:set(
                      node_details:set(
                        node_details:new(), node, node:new(pid, 1, 0)),
                      pred, node:new(pid, 0, 0)),
                    load, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, true, 0, 0, 0),
    % non-empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, local_info},
                  {PreviousState, State, [{msg1}, {msg2}], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages should be send if already initialized
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_local_info3(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    NodeDetails = node_details:set(
                    node_details:set(
                      node_details:set(
                        node_details:new(), node, node:new(pid, 1, 0)),
                      pred, node:new(pid, 0, 0)),
                    load, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    % empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, local_info},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    NewValues_exp = gossip_state:new_internal(0, 0, unknown, 1, 0, 0, 0),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), true),
    ?equals(gossip_state:get(NewState, triggered), gossip_state:get(State, triggered)),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % message queue empty -> no messages should be send
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_local_info4(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    NodeDetails = node_details:set(
                    node_details:set(
                      node_details:set(
                        node_details:new(), node, node:new(pid, 1, 0)),
                      pred, node:new(pid, 0, 0)),
                    load, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    % non-empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, local_info},
                  {PreviousState, State, [{msg1}, {msg2}], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    NewValues_exp = gossip_state:new_internal(0, 0, unknown, 1, 0, 0, 0),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), true),
    ?equals(gossip_state:get(NewState, triggered), gossip_state:get(State, triggered)),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % message queue non-empty -> the following messages should have been send
    ?expect_message({msg1}),
    ?expect_message({msg2}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_local_info5(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    NodeDetails = node_details:set(
                    node_details:set(
                      node_details:set(
                        node_details:new(), node, node:new(pid, 4, 0)),
                      pred, node:new(pid, 2, 0)),
                    load, 4),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    % non-empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, local_info},
                  {PreviousState, State, [{msg1}, {msg2}], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    NewValues_exp = gossip_state:new_internal(4, 4*4, unknown, 2, 4, 4, 0),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), true),
    ?equals(gossip_state:get(NewState, triggered), gossip_state:get(State, triggered)),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % message queue non-empty -> the following messages should have been send
    ?expect_message({msg1}),
    ?expect_message({msg2}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_local_info6(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    Values = gossip_state:new_internal(2.0, 2.0*2.0, unknown, 6.0, 2, 2, 0),
    NodeDetails = node_details:set(
                    node_details:set(
                      node_details:set(
                        node_details:new(), node, node:new(pid, 4, 0)),
                      pred, node:new(pid, 2, 0)),
                    load, 4),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, false, 0, 0, 0),
    % non-empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, local_info},
                  {PreviousState, State, [{msg1}, {msg2}], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    NewValues_exp = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 4, 0),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), true),
    ?equals(gossip_state:get(NewState, triggered), gossip_state:get(State, triggered)),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % message queue non-empty -> the following messages should have been send
    ?expect_message({msg1}),
    ?expect_message({msg2}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_local_info7(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    Values = gossip_state:new_internal(2.0, 2.0*2.0, unknown, 6.0, 6, 6, 0),
    NodeDetails = node_details:set(
                    node_details:set(
                      node_details:set(
                        node_details:new(), node, node:new(pid, 4, 0)),
                      pred, node:new(pid, 2, 0)),
                    load, 4),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, false, 0, 0, 0),
    % non-empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, local_info},
                  {PreviousState, State, [{msg1}, {msg2}], get_ptrigger_nodelay()}),
    
    ?equals(NewPreviousState, PreviousState),
    NewValues_exp = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 4, 6, 0),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), true),
    ?equals(gossip_state:get(NewState, triggered), gossip_state:get(State, triggered)),
    ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
    ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
    
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % message queue non-empty -> the following messages should have been send
    ?expect_message({msg1}),
    ?expect_message({msg2}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_leader_start_new_round1(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    
    % not the leader
    NodeDetails = node_details:set(node_details:new(), my_range, intervals:new('(', 0, 1, ']')),
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, leader_start_new_round},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages should be send if not the leader
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_leader_start_new_round2(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    
    % not the leader
    NodeDetails = node_details:set(node_details:new(), my_range, intervals:new('(', 0, 1, ']')),
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    % non-empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, leader_start_new_round},
                  {PreviousState, State, [{msg1}, {msg2}], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, [{msg1}, {msg2}]),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages should be send if not the leader
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_leader_start_new_round3(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    
    % not the leader
    NodeDetails = node_details:set(node_details:new(), my_range, intervals:new('(', 1, 10, ']')),
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    % non-empty queued messages list
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, leader_start_new_round},
                  {PreviousState, State, [{msg1}, {msg2}], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, [{msg1}, {msg2}]),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages should be send if not the leader
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_leader_start_new_round4(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    
    % the node is the leader
    NodeDetails = node_details:set(node_details:new(), my_range, intervals:new('(', 10, 1, ']')),
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, leader_start_new_round},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, State),
    NewValues_exp = gossip_state:new_internal(unknown, unknown, 1.0, unknown, unknown, unknown, 1),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), false),
    ?equals(gossip_state:get(NewState, triggered), 0),
    ?equals(gossip_state:get(NewState, msg_exch), 0),
    ?equals(gossip_state:get(NewState, converge_avg_count), 0),

    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % if a new round is started, the leader asks for its node's information
    ThisWithCookie = comm:this_with_cookie(local_info),
    ?expect_message({get_node_details, ThisWithCookie, [pred, node, load]}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_leader_start_new_round5(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    
    % the node is the leader
    NodeDetails = node_details:set(node_details:new(), my_range, intervals:new('(', 10, 1, ']')),
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, false, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, leader_start_new_round},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, State),
    NewValues_exp = gossip_state:new_internal(unknown, unknown, 1.0, unknown, unknown, unknown, 1),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), false),
    ?equals(gossip_state:get(NewState, triggered), 0),
    ?equals(gossip_state:get(NewState, msg_exch), 0),
    ?equals(gossip_state:get(NewState, converge_avg_count), 0),

    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % if a new round is started, the leader asks for its node's information
    ThisWithCookie = comm:this_with_cookie(local_info),
    ?expect_message({get_node_details, ThisWithCookie, [pred, node, load]}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_leader_start_new_round6(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    
    % the node is the leader
    NodeDetails = node_details:set(node_details:new(), my_range, intervals:new('(', 10, 0, ']')),
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, false, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, leader_start_new_round},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, State),
    NewValues_exp = gossip_state:new_internal(unknown, unknown, 1.0, unknown, unknown, unknown, 1),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), false),
    ?equals(gossip_state:get(NewState, triggered), 0),
    ?equals(gossip_state:get(NewState, msg_exch), 0),
    ?equals(gossip_state:get(NewState, converge_avg_count), 0),

    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % if a new round is started, the leader asks for its node's information
    ThisWithCookie = comm:this_with_cookie(local_info),
    ?expect_message({get_node_details, ThisWithCookie, [pred, node, load]}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_node_details_response_leader_start_new_round7(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    
    GossipNewValues = gossip_state:new_internal(),
    
    % the node is the leader
    NodeDetails = node_details:set(node_details:new(), my_range, intervals:new('(', 0, 0, ']')),
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({{get_node_details_response, NodeDetails}, leader_start_new_round},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, State),
    NewValues_exp = gossip_state:new_internal(unknown, unknown, 1.0, unknown, unknown, unknown, 1),
    ?equals(gossip_state:get(NewState, values), NewValues_exp),
    ?equals(gossip_state:get(NewState, initialized), false),
    ?equals(gossip_state:get(NewState, triggered), 0),
    ?equals(gossip_state:get(NewState, msg_exch), 0),
    ?equals(gossip_state:get(NewState, converge_avg_count), 0),

    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % if a new round is started, the leader asks for its node's information
    ThisWithCookie = comm:this_with_cookie(local_info),
    ?expect_message({get_node_details, ThisWithCookie, [pred, node, load]}),
    % no further messages
    ?expect_no_message(),
    Config.

test_on_get_state(Config) ->
    % TODO: implement unit test
    Config.

test_on_get_state_response(Config) ->
    % TODO: implement unit test
    Config.

test_on_cy_cache1(Config) ->
    GossipNewValues = gossip_state:new_internal(),
    
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    % empty node cache
    Cache = [],
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({cy_cache, Cache},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages should be send if no node given
    ?expect_no_message(),
    Config.

test_on_cy_cache2(Config) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),

    GossipNewValues = gossip_state:new_internal(),
    
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    % non-empty node cache
    Cache = [node:new(comm:make_global(self()), 10, 0)],
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({cy_cache, Cache},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages sent to itself
    ?expect_no_message(),
    Config.

test_on_cy_cache3(Config) ->
    erlang:put(instance_id, "gossip_group"),
    % register some other process as the dht_node
    DHT_Node = fake_dht_node(),
%%     ?equals(process_dictionary:get_group_member(dht_node), DHT_Node),

    GossipNewValues = gossip_state:new_internal(),
    
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    % non-empty node cache
    Cache = [node:new(comm:this(), 10, 0)],
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({cy_cache, Cache},
                  {PreviousState, State, [], get_ptrigger_nodelay()}),

    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % if pids don't match, a get_state is send to the cached node's dht_node
    This = comm:this(),
    ?expect_message({send_to_group_member, gossip, {get_state, This, Values}}),
    % no further messages
    ?expect_no_message(),
    
    exit(DHT_Node, kill),
    Config.

test_on_get_values_best1(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, true, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    BestVal = gossip_state:conv_state_to_extval(State),
    ?expect_message({gossip_get_values_best_response, BestVal}),

    Config.

test_on_get_values_best2(Config) ->
    config:write(gossip_converge_avg_count, 20),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, true, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    BestVal = gossip_state:conv_state_to_extval(State),
    ?expect_message({gossip_get_values_best_response, BestVal}),

    Config.

test_on_get_values_best3(Config) ->
    config:write(gossip_converge_avg_count, 21),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, true, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    BestVal = gossip_state:conv_state_to_extval(PreviousState),
    ?expect_message({gossip_get_values_best_response, BestVal}),

    Config.

test_on_get_values_best4(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % not initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, false, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    BestVal = gossip_state:conv_state_to_extval(PreviousState),
    ?expect_message({gossip_get_values_best_response, BestVal}),

    Config.

test_on_get_values_best5(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % not initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 2
    State = create_gossip_state(Values, false, 20, 20, 2),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    BestVal = gossip_state:conv_state_to_extval(PreviousState),
    ?expect_message({gossip_get_values_best_response, BestVal}),

    Config.

test_on_get_values_best6(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 2
    State = create_gossip_state(Values, true, 20, 20, 2),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    BestVal = gossip_state:conv_state_to_extval(PreviousState),
    ?expect_message({gossip_get_values_best_response, BestVal}),

    Config.

test_on_get_values_all1(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, true, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    PreviousVal = gossip_state:conv_state_to_extval(PreviousState),
    CurrentVal = gossip_state:conv_state_to_extval(State),
    BestVal = CurrentVal,
    ?expect_message({gossip_get_values_all_response, PreviousVal, CurrentVal, BestVal}),

    Config.

test_on_get_values_all2(Config) ->
    config:write(gossip_converge_avg_count, 20),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, true, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    PreviousVal = gossip_state:conv_state_to_extval(PreviousState),
    CurrentVal = gossip_state:conv_state_to_extval(State),
    BestVal = CurrentVal,
    ?expect_message({gossip_get_values_all_response, PreviousVal, CurrentVal, BestVal}),

    Config.

test_on_get_values_all3(Config) ->
    config:write(gossip_converge_avg_count, 21),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, true, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    PreviousVal = gossip_state:conv_state_to_extval(PreviousState),
    CurrentVal = gossip_state:conv_state_to_extval(State),
    BestVal = PreviousVal,
    ?expect_message({gossip_get_values_all_response, PreviousVal, CurrentVal, BestVal}),

    Config.

test_on_get_values_all4(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % not initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 20
    State = create_gossip_state(Values, false, 20, 20, 20),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    PreviousVal = gossip_state:conv_state_to_extval(PreviousState),
    CurrentVal = gossip_state:conv_state_to_extval(State),
    BestVal = PreviousVal,
    ?expect_message({gossip_get_values_all_response, PreviousVal, CurrentVal, BestVal}),

    Config.

test_on_get_values_all5(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % not initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 2
    State = create_gossip_state(Values, false, 20, 20, 2),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    PreviousVal = gossip_state:conv_state_to_extval(PreviousState),
    CurrentVal = gossip_state:conv_state_to_extval(State),
    BestVal = PreviousVal,
    ?expect_message({gossip_get_values_all_response, PreviousVal, CurrentVal, BestVal}),

    Config.

test_on_get_values_all6(Config) ->
    config:write(gossip_converge_avg_count, 10),
    TriggerNoDelay = get_ptrigger_nodelay(),
    
    PreviousValues = gossip_state:new_internal(1.0, 1.0, 1.0, 1.0, 1, 1, 1),
    % initialized, triggers = 10, msg_exchg = 10, conv_avg_count = 10
    PreviousState = create_gossip_state(PreviousValues, true, 10, 10, 10),
    Values = gossip_state:new_internal(2.0, 2.0, 2.0, 2.0, 2, 2, 2),
    % initialized, triggers = 20, msg_exchg = 20, conv_avg_count = 2
    State = create_gossip_state(Values, true, 20, 20, 2),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState} =
        gossip:on({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay}),

    ?equals(NewState, State),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals(NewTriggerState, TriggerNoDelay),
    PreviousVal = gossip_state:conv_state_to_extval(PreviousState),
    CurrentVal = gossip_state:conv_state_to_extval(State),
    BestVal = PreviousVal,
    ?expect_message({gossip_get_values_all_response, PreviousVal, CurrentVal, BestVal}),

    Config.

% helper functions:
create_gossip_state(Values, Initialized, Triggered, Msg_exch, Converge_avg_count) ->
    S1 = gossip_state:new_state(Values),
    S2 = case Initialized of
             true -> gossip_state:set_initialized(S1);
             false -> S1
         end,
    S3 = case Triggered of
             0 -> S2;
             _ -> lists:foldl(fun(_, LastState) -> gossip_state:inc_triggered(LastState) end, S2, lists:seq(1, Triggered))
         end,
    S4 = case Msg_exch of
             0 -> S3;
             _ -> lists:foldl(fun(_, LastState) -> gossip_state:inc_msg_exch(LastState) end, S3, lists:seq(1, Msg_exch))
         end,
    _S5 = case Converge_avg_count of
             0 -> S4;
             _ -> lists:foldl(fun(_, LastState) -> gossip_state:inc_converge_avg_count(LastState) end, S4, lists:seq(1, Converge_avg_count))
         end.

reset_config() ->
    config:write(gossip_interval, 1000),
    config:write(gossip_min_triggers_per_round, 10),
    config:write(gossip_max_triggers_per_round, 1000),
    config:write(gossip_converge_avg_epsilon, 5.0),
    config:write(gossip_converge_avg_count, 10),
    config:write(gossip_converge_avg_count_start_new_round, 20),
    config:write(gossip_trigger, trigger_periodic).

get_ptrigger_nodelay() ->
    get_ptrigger_delay(0).

get_ptrigger_delay(Delay) ->
    trigger:init('trigger_periodic', fun () -> Delay end, 'trigger').

fake_dht_node() ->
    DHT_Node = spawn(?MODULE, fake_dht_node_start, [self()]),
    receive
        {started, DHT_Node} -> DHT_Node
    end.

fake_dht_node_start(Supervisor) ->
    process_dictionary:register_process("gossip_group", dht_node, self()),
    Supervisor ! {started, self()},
    fake_process().

fake_process() ->
    ?consume_message({ok}, 1000),
    fake_process().
