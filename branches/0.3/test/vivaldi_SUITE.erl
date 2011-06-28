%%%-------------------------------------------------------------------
%%% File    : vivaldi_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/vivaldi.erl
%%%
%%% Created :  18 Feb 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(vivaldi_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

all() ->
    [test_init,
     test_on_trigger,
     test_on_vivaldi_shuffle,
     test_on_cy_cache1,
     test_on_cy_cache2,
     test_on_cy_cache3].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    ok = unittest_helper:fix_cwd(),
    error_logger:tty(true),
    Pid = unittest_helper:start_process(
            fun() ->
                    {ok, _GroupsPid} = pid_groups:start_link(),
                    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
                    ConfigOptions = unittest_helper:prepare_config([{config, [{log_path, PrivDir}]}]),
                    {ok, _ConfigPid} = config:start_link2(ConfigOptions),
                    {ok, _LogPid} = log:start_link(),
                    {ok, _CommPid} = sup_comm_layer:start_link(),
                    comm_server:set_local_address({127,0,0,1}, unittest_helper:get_scalaris_port())
            end),
    [{wrapper_pid, Pid} | Config2].

end_per_suite(Config) ->
    {wrapper_pid, Pid} = lists:keyfind(wrapper_pid, 1, Config),
    reset_config(),
    error_logger:tty(false),
    log:set_log_level(none),
    exit(Pid, kill),
    unittest_helper:stop_pid_groups(),
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    reset_config(),
    Config.

test_init(Config) ->
    config:write(vivaldi_interval, 100),
    EmptyMsgQueue = msg_queue:new(),
    FullState1 = vivaldi:init('trigger_periodic'),
    ?equals_pattern(FullState1,
                    {'$gen_component', [{on_handler, on_inactive}],
                     {inactive, EmptyMsgQueue, {'trigger_periodic', _TriggerState}}}),
    {'$gen_component', [{on_handler, on_inactive}], InitialState1} = FullState1,
    ?expect_no_message(),
    
    FullState2 = vivaldi:on_inactive({activate_vivaldi}, InitialState1),
    ?equals_pattern(FullState2,
                    {'$gen_component', [{on_handler, on_active}],
                     {[_X, _Y], 1.0, {'trigger_periodic', _TriggerState}}}),
    ?expect_message({vivaldi_trigger}),
    ?expect_no_message(),
    Config.

test_on_trigger(Config) ->
    pid_groups:join_as(atom_to_list(?MODULE), cyclon),
    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    Trigger1 = get_ptrigger_nodelay(),
    InitialState = {Coordinate, Confidence, Trigger1},
    {NewCoordinate, NewConfidence, NewTriggerState} =
        vivaldi:on_active({vivaldi_trigger}, InitialState),

    Self = self(),
    ?equals(Coordinate, NewCoordinate),
    ?equals(Confidence, NewConfidence),
    % note: cannot compare with an opaque trigger state from trigger:next/1
    % since the timer ref will be different
    ?equals_pattern(NewTriggerState, {'trigger_periodic', _}),
    ?expect_message({get_subset_rand, 1, Self}),
    ?expect_message({vivaldi_trigger}),
    ?expect_no_message(),
    Config.

test_on_vivaldi_shuffle(Config) ->
    config:write(vivaldi_count_measurements, 1),
    config:write(vivaldi_measurements_delay, 0),
    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    InitialState = {Coordinate, Confidence, get_ptrigger_nodelay()},
    _NewState = vivaldi:on_active({vivaldi_shuffle, comm:this(), [0.0, 0.0], 1.0},
                                 InitialState),
    receive
        {ping, SourcePid} -> comm:send(SourcePid, {pong})
    end,

    ?expect_message({update_vivaldi_coordinate, _Latency, {[0.0, 0.0], 1.0}}),
    ?expect_no_message(),
    % TODO: check the node's state
    Config.

test_on_cy_cache1(Config) ->
    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    InitialState = {Coordinate, Confidence, get_ptrigger_nodelay()},
    % empty node cache
    Cache = [],
    NewState =
        vivaldi:on_active({cy_cache, Cache}, InitialState),

    ?equals(NewState, InitialState),
    % no messages should be send if no node given
    ?expect_no_message(),
    Config.

test_on_cy_cache2(Config) ->
    pid_groups:join_as(atom_to_list(?MODULE), dht_node),

    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    InitialState = {Coordinate, Confidence, get_ptrigger_nodelay()},
    % non-empty node cache
    Cache = [node:new(comm:make_global(self()), ?RT:hash_key("10"), 0)],
    NewState =
        vivaldi:on_active({cy_cache, Cache}, InitialState),

    ?equals(NewState, InitialState),
    % no messages sent to itself
    ?expect_no_message(),
    Config.

test_on_cy_cache3(Config) ->
    % register some other process as the dht_node
    DHT_Node = fake_dht_node(),
%%     ?equals(pid_groups:get_my(dht_node), DHT_Node),

    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    InitialState = {Coordinate, Confidence, get_ptrigger_nodelay()},
    % non-empty node cache
    Cache = [node:new(comm:make_global(self()), ?RT:hash_key("10"), 0)],
    NewState =
        vivaldi:on_active({cy_cache, Cache}, InitialState),

    ?equals(NewState, InitialState),
    % if pids don't match, a get_state is send to the cached node's dht_node
    This = comm:this(),
    ?expect_message({send_to_group_member, vivaldi,
                     {vivaldi_shuffle, This, Coordinate, Confidence}}),
    % no further messages
    ?expect_no_message(),
    
    exit(DHT_Node, kill),
    Config.

reset_config() ->
    config:write(vivaldi_interval, 10000),
    config:write(vivaldi_dimensions, 2),
    config:write(vivaldi_count_measurements, 10),
    config:write(vivaldi_measurements_delay, 1000),
    config:write(vivaldi_latency_timeout, 60000),
    config:write(vivaldi_trigger, trigger_periodic).

get_ptrigger_nodelay() ->
    get_ptrigger_delay(0).

get_ptrigger_delay(Delay) ->
    trigger:init('trigger_periodic', fun () -> Delay end, 'vivaldi_trigger').

fake_dht_node() ->
    unittest_helper:start_subprocess(
      fun() -> pid_groups:join_as("vivaldi_SUITE_group", dht_node) end).
