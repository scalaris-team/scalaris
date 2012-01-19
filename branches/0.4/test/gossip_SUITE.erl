%  @copyright 2010-2011 Zuse Institute Berlin
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

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [test_init,
     test_on_trigger,
     test_on_get_node_details_response_local_info,
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
     test_get_values_all0].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    unittest_helper:start_minimal_procs(Config2, [], true).

end_per_suite(Config) ->
    unittest_helper:stop_minimal_procs(Config),
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    reset_config(),
    Config.

test_init(Config) ->
    pid_groups:join_as("gossip_group", gossip),
    DHT_Node = fake_node("gossip_group", dht_node),
    
    config:write(gossip_interval, 100),
    EmptyMsgQueue = msg_queue:new(),
    GossipNewState = gossip_state:new_state(),
    InitialState1 = gossip:init('trigger_periodic'),
    ?equals_pattern(InitialState1,
                    {uninit, EmptyMsgQueue, {'trigger_periodic', _TriggerState}, GossipNewState}),

    ?expect_no_message(),
    
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    FullState2 = gossip:on_inactive({activate_gossip, MyRange}, InitialState1),
    OnActiveHandler = fun gossip:on_active/2,
    ?equals_pattern(FullState2,
                    {'$gen_component', [{on_handler, OnActiveHandler}],
                     {GossipNewState, GossipNewState, EmptyMsgQueue, {'trigger_periodic', _TriggerState}, MyRange}}),
    ?expect_message({gossip_trigger}),
    ?expect_no_message(),
    exit(DHT_Node, kill),
    Config.

test_get_values_best0(Config) ->
    pid_groups:join_as("gossip_group", gossip),

    comm:send_local(pid_groups:get_my(gossip), {get_values_best, self()}),
    ?expect_message({get_values_best, _Pid}),
    ?expect_no_message(),
    Config.

test_get_values_all0(Config) ->
    pid_groups:join_as("gossip_group", gossip),

    comm:send_local(pid_groups:get_my(gossip), {get_values_all, self()}),
    ?expect_message({get_values_all, _Pid}),
    ?expect_no_message(),
    Config.

-spec prop_on_trigger(PredId::?RT:key(), NodeId::?RT:key(),
        MinTpR::pos_integer(), MinToMaxTpR::pos_integer(),
        ConvAvgCntSNR::pos_integer(),
        PreviousState::gossip_state:state(), State::gossip_state:state(),
        MsgQueue::msg_queue:msg_queue()) -> true.
prop_on_trigger(PredId, NodeId, MinTpR, MinToMaxTpR, ConvAvgCntSNR, PreviousState, State, MsgQueue) ->
    MaxTpR = MinTpR + MinToMaxTpR,
    
    Self = self(),
    This = comm:this(),
    
    pid_groups:join_as("gossip_group", dht_node),
    pid_groups:join_as("gossip_group", cyclon),
    MyRange = node:mk_interval_between_ids(PredId, NodeId),
    
    config:write(gossip_min_triggers_per_round, MinTpR),
    config:write(gossip_max_triggers_per_round, MaxTpR),
    config:write(gossip_converge_avg_count_start_new_round, ConvAvgCntSNR),
    
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({gossip_trigger}, {PreviousState, State, MsgQueue, get_ptrigger_nodelay(), MyRange}),
    
    ?equals(NewMyRange, MyRange),

    IsLeader = intervals:in(?RT:hash_key("0"), MyRange),
    StartNewRound = IsLeader andalso
                        (gossip_state:get(State, round) =:= 0 orelse
                             (gossip_state:get(State, triggered) + 1 > MinTpR andalso
                                  (gossip_state:get(State, triggered) + 1 > MaxTpR orelse
                                       gossip_state:get(State, converge_avg_count) >= ConvAvgCntSNR))),
    case StartNewRound of
        true ->
            % see gossip:calc_initial_avg_kr/2:
            KrExp = try ?RT:get_range(PredId, NodeId)
                    catch % ?RT:get_range() may throw
                        throw:not_supported -> unknown
                    end,
            NewValuesExp1 = gossip_state:set(gossip_state:new_internal(), size_inv, 1.0),
            NewValuesExp2 = gossip_state:set(NewValuesExp1, avg_kr, KrExp),
            NewValuesExp3 = gossip_state:set(NewValuesExp2, round, gossip_state:get(State, round) + 1),
            NewStateExp = gossip_state:new_state(NewValuesExp3),

            ?equals(gossip_state:get(NewState, values), gossip_state:get(NewStateExp, values)),
            ?equals(gossip_state:get(NewState, initialized), gossip_state:get(NewStateExp, initialized)),
            ?equals(gossip_state:get(NewState, triggered), gossip_state:get(NewStateExp, triggered)),
            ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(NewStateExp, msg_exch)),
            ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(NewStateExp, converge_avg_count)),
            ?equals(NewPreviousState, gossip_state:inc_triggered(State)),
            ?equals(NewMsgQueue, MsgQueue),
            ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
            ?expect_message({gossip_trigger}),
            
            ?expect_message({get_node_details, This, [load]});
        false ->
            ?equals(gossip_state:get(NewState, values), gossip_state:get(State, values)),
            ?equals(gossip_state:get(NewState, initialized), gossip_state:get(State, initialized)),
            ?equals(gossip_state:get(NewState, triggered), gossip_state:get(State, triggered) + 1),
            ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
            ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
            ?equals(NewPreviousState, PreviousState),
            ?equals(NewMsgQueue, MsgQueue),
            ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
            ?expect_message({gossip_trigger})
    end,
    % request for random node?
    case gossip_state:get(NewState, round) > 0 andalso
             gossip_state:get(NewState, initialized) of
        true -> ?expect_message({get_subset_rand, 1, Self});
        false -> ok
    end,
    % no further messages
    ?expect_no_message().

test_on_trigger1() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    prop_on_trigger(?RT:hash_key("1"), ?RT:hash_key("10"), 2, 4, 1, PreviousState, State, []),
    prop_on_trigger(?RT:hash_key("10"), ?RT:hash_key("1"), 2, 4, 1, PreviousState, State, []).

test_on_trigger2() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, true, 0, 0, 0),
    prop_on_trigger(?RT:hash_key("1"), ?RT:hash_key("10"), 2, 4, 1, PreviousState, State, []),
    prop_on_trigger(?RT:hash_key("10"), ?RT:hash_key("1"), 2, 4, 1, PreviousState, State, []).

test_on_trigger3() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 0, 0, 0),
    prop_on_trigger(?RT:hash_key("1"), ?RT:hash_key("10"), 2, 4, 1, PreviousState, State, []),
    prop_on_trigger(?RT:hash_key("10"), ?RT:hash_key("1"), 2, 4, 1, PreviousState, State, []).

test_on_trigger4() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 1, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 1, 0, 0),
    prop_on_trigger(?RT:hash_key("1"), ?RT:hash_key("10"), 2, 4, 1, PreviousState, State, []),
    prop_on_trigger(?RT:hash_key("10"), ?RT:hash_key("1"), 2, 4, 1, PreviousState, State, []).

test_on_trigger5() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 2, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 2, 0, 0),
    prop_on_trigger(?RT:hash_key("1"), ?RT:hash_key("10"), 2, 4, 1, PreviousState, State, []),
    prop_on_trigger(?RT:hash_key("10"), ?RT:hash_key("1"), 2, 4, 1, PreviousState, State, []).

test_on_trigger6() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 3, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 3, 0, 0),
    prop_on_trigger(?RT:hash_key("1"), ?RT:hash_key("10"), 2, 4, 1, PreviousState, State, []),
    prop_on_trigger(?RT:hash_key("10"), ?RT:hash_key("1"), 2, 4, 1, PreviousState, State, []).

test_on_trigger7() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values but round = 1, initialized, triggers = 4, msg_exchg = 0, conv_avg_count = 0
    Values = gossip_state:set(GossipNewValues, round, 1),
    State = create_gossip_state(Values, true, 4, 0, 0),
    prop_on_trigger(?RT:hash_key("1"), ?RT:hash_key("10"), 2, 4, 1, PreviousState, State, []),
    prop_on_trigger(?RT:hash_key("10"), ?RT:hash_key("1"), 2, 4, 1, PreviousState, State, []).

test_on_trigger(_Config) ->
    test_on_trigger1(),
    test_on_trigger2(),
    test_on_trigger3(),
    test_on_trigger4(),
    test_on_trigger5(),
    test_on_trigger6(),
    test_on_trigger7(),
    tester:test(?MODULE, prop_on_trigger, 8, 100).

-spec prop_on_get_node_details_response_local_info(Load::integer(),
        PreviousState::gossip_state:state(), State::gossip_state:state(),
        MsgQueue::msg_queue:msg_queue()) -> true.
prop_on_get_node_details_response_local_info(Load, PreviousState, State, MsgQueue) ->
    NodeDetails = node_details:set(node_details:new(), load, Load),
    TriggerState = get_ptrigger_nodelay(),
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_node_details_response, NodeDetails},
                  {PreviousState, State, MsgQueue, TriggerState, MyRange}),
    
    ?equals(NewMyRange, MyRange),
    
    case gossip_state:get(State, initialized) of
        true -> 
            ?equals(NewState, State);
        false ->
            OldAvgLoad = gossip_state:get(State, avgLoad),
            case OldAvgLoad of
                unknown -> ?equals(float(gossip_state:get(NewState, avgLoad)), float(Load));
                _       -> ?equals(float(gossip_state:get(NewState, avgLoad)), (Load + OldAvgLoad) / 2.0)
            end,
            OldAvg2Load = gossip_state:get(State, avgLoad2),
            case OldAvg2Load of
                unknown -> ?equals(float(gossip_state:get(NewState, avgLoad2)), float(Load * Load));
                _       -> ?equals(float(gossip_state:get(NewState, avgLoad2)), (Load * Load + OldAvg2Load) / 2.0)
            end,
            ?equals(gossip_state:get(NewState, size_inv), gossip_state:get(State, size_inv)),
            ?equals(gossip_state:get(NewState, avg_kr), gossip_state:get(State, avg_kr)),
            OldMinLoad = gossip_state:get(State, minLoad),
            case OldMinLoad of
                unknown -> ?equals(gossip_state:get(NewState, minLoad), Load);
                _       -> ?equals(gossip_state:get(NewState, minLoad), erlang:min(Load, OldMinLoad))
            end,
            OldMaxLoad = gossip_state:get(State, maxLoad),
            case OldMaxLoad of
                unknown -> ?equals(gossip_state:get(NewState, maxLoad), Load);
                _       -> ?equals(gossip_state:get(NewState, maxLoad), erlang:max(Load, OldMaxLoad))
            end,
            ?equals(gossip_state:get(NewState, initialized), true),
            ?equals(gossip_state:get(NewState, triggered), gossip_state:get(State, triggered)),
            ?equals(gossip_state:get(NewState, msg_exch), gossip_state:get(State, msg_exch)),
            ?equals(gossip_state:get(NewState, converge_avg_count), gossip_state:get(State, converge_avg_count)),
            _ = [?expect_message(Msg) || Msg <- MsgQueue],
            ok
    end,
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, TriggerState),
    ?expect_no_message().

test_on_get_node_details_response_local_info1() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, true, 0, 0, 0),
    prop_on_get_node_details_response_local_info(0, PreviousState, State, []).

test_on_get_node_details_response_local_info2() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, true, 0, 0, 0),
    prop_on_get_node_details_response_local_info(0, PreviousState, State, [{msg1}, {msg2}]).

test_on_get_node_details_response_local_info3() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    prop_on_get_node_details_response_local_info(0, PreviousState, State, []).

test_on_get_node_details_response_local_info4() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    prop_on_get_node_details_response_local_info(0, PreviousState, State, [{msg1}, {msg2}]).

test_on_get_node_details_response_local_info5() ->
    GossipNewValues = gossip_state:new_internal(),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % empty values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(GossipNewValues, false, 0, 0, 0),
    prop_on_get_node_details_response_local_info(4, PreviousState, State, [{msg1}, {msg2}]).

test_on_get_node_details_response_local_info6() ->
    GossipNewValues = gossip_state:new_internal(),
    Values = gossip_state:new_internal(2.0, 2.0*2.0, unknown, 6.0, 2, 2, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, false, 0, 0, 0),
    prop_on_get_node_details_response_local_info(4, PreviousState, State, [{msg1}, {msg2}]).

test_on_get_node_details_response_local_info7() ->
    GossipNewValues = gossip_state:new_internal(),
    Values = gossip_state:new_internal(2.0, 2.0*2.0, unknown, 6.0, 6, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, not initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, false, 0, 0, 0),
    prop_on_get_node_details_response_local_info(4, PreviousState, State, [{msg1}, {msg2}]).

test_on_get_node_details_response_local_info(_Config) ->
    test_on_get_node_details_response_local_info1(),
    test_on_get_node_details_response_local_info2(),
    test_on_get_node_details_response_local_info3(),
    test_on_get_node_details_response_local_info4(),
    test_on_get_node_details_response_local_info5(),
    test_on_get_node_details_response_local_info6(),
    test_on_get_node_details_response_local_info7(),
    tester:test(?MODULE, prop_on_get_node_details_response_local_info, 4, 100).

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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    % empty node cache
    Cache = [],
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({cy_cache, Cache},
                  {PreviousState, State, [], get_ptrigger_nodelay(), MyRange}),

    ?equals(NewMyRange, MyRange),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages should be send if no node given
    ?expect_no_message(),
    Config.

test_on_cy_cache2(Config) ->
    pid_groups:join_as("gossip_group", dht_node),

    GossipNewValues = gossip_state:new_internal(),
    
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    MyRange = node:mk_interval_between_ids(?RT:hash_key("10"), ?RT:hash_key("0")),
    % non-empty node cache
    Cache = [node:new(comm:make_global(self()), 10, 0)],
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({cy_cache, Cache},
                  {PreviousState, State, [], get_ptrigger_nodelay(), MyRange}),

    ?equals(NewMyRange, MyRange),
    ?equals(NewPreviousState, PreviousState),
    ?equals(NewState, State),
    ?equals(NewMsgQueue, []),
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    % no messages sent to itself
    ?expect_no_message(),
    Config.

test_on_cy_cache3(Config) ->
    % register some other process as the dht_node
    DHT_Node = fake_node("gossip_group", dht_node),
%%     ?equals(pid_groups:get_my(dht_node), DHT_Node),

    GossipNewValues = gossip_state:new_internal(),
    
    Values = gossip_state:new_internal(3.0, 10.0, unknown, 4.0, 2, 6, 0),
    PreviousState = create_gossip_state(GossipNewValues, true, 10, 2, 0),
    % given values, initialized, triggers = 0, msg_exchg = 0, conv_avg_count = 0
    State = create_gossip_state(Values, true, 0, 0, 0),
    MyRange = node:mk_interval_between_ids(?RT:hash_key("10"), ?RT:hash_key("0")),
    % non-empty node cache
    Cache = [node:new(comm:this(), 10, 0)],
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({cy_cache, Cache},
                  {PreviousState, State, [], get_ptrigger_nodelay(), MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_best, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
    MyRange = node:mk_interval_between_ids(?RT:hash_key("0"), ?RT:hash_key("0")),
    {NewPreviousState, NewState, NewMsgQueue, NewTriggerState, NewMyRange} =
        gossip:on_active({get_values_all, self()},
                  {PreviousState, State, [], TriggerNoDelay, MyRange}),

    ?equals(NewMyRange, MyRange),
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
% note: use manageable values for Triggered, Msg_exch, Converge_avg_count!
-spec create_gossip_state(Values::gossip_state:values_internal(),
                          Initialized::boolean(),
                          Triggered::0..1000,
                          Msg_exch::0..1000,
                          Converge_avg_count::0..1000)
        -> gossip_state:state().
create_gossip_state(Values, Initialized, Triggered, Msg_exch, Converge_avg_count) ->
    S1 = gossip_state:new_state(Values),
    S2 = case Initialized of
             true -> gossip_state:set_initialized(S1);
             false -> S1
         end,
    S3 = inc_triggered(S2, Triggered),
    S4 = inc_msg_exch(S3, Msg_exch),
    _S5 = inc_converge_avg_count(S4, Converge_avg_count).

-spec inc_triggered(State::gossip_state:state(), Count::non_neg_integer()) -> gossip_state:state().
inc_triggered(State, 0) -> State;
inc_triggered(State, Count) -> inc_triggered(gossip_state:inc_triggered(State), Count - 1).

-spec inc_msg_exch(State::gossip_state:state(), Count::non_neg_integer()) -> gossip_state:state().
inc_msg_exch(State, 0) -> State;
inc_msg_exch(State, Count) -> inc_msg_exch(gossip_state:inc_msg_exch(State), Count - 1).

-spec inc_converge_avg_count(State::gossip_state:state(), Count::non_neg_integer()) -> gossip_state:state().
inc_converge_avg_count(State, 0) -> State;
inc_converge_avg_count(State, Count) -> inc_converge_avg_count(gossip_state:inc_converge_avg_count(State), Count - 1).

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
    trigger:init('trigger_periodic', fun () -> Delay end, 'gossip_trigger').

-spec fake_node(RegisterGroup::pid_groups:groupname(), RegisterName::pid_groups:pidname()) -> pid().
fake_node(RegisterGroup, RegisterName) ->
    unittest_helper:start_subprocess(
      fun() -> pid_groups:join_as(RegisterGroup, RegisterName) end).
