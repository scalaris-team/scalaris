% @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Unit tests for src/vivaldi.erl
%% @end
%% @version $Id$
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
    config:write(vivaldi_interval, 100),
    EmptyMsgQueue = msg_queue:new(),
    InitialState1 = vivaldi:init([]),
    ?equals_pattern(InitialState1,
                    {inactive, EmptyMsgQueue}),
    ?expect_no_message(),

    FullState2 = vivaldi:on_inactive({activate_vivaldi}, InitialState1),
    OnActiveHandler = fun vivaldi:on_active/2,
    ?equals_pattern(FullState2,
                    {'$gen_component', [{on_handler, OnActiveHandler}],
                     {[_X, _Y], 1.0}}),
    ?expect_message({vivaldi_trigger}),
    ?expect_message({trigger_once}),
    ?expect_no_message(),
    Config.

test_on_trigger(Config) ->
    pid_groups:join_as(atom_to_list(?MODULE), cyclon),
    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    InitialState = {Coordinate, Confidence},
    {NewCoordinate, NewConfidence} =
        vivaldi:on_active({trigger_once}, InitialState),

    Self = self(),
    ?equals(Coordinate, NewCoordinate),
    ?equals(Confidence, NewConfidence),
    ?expect_message({get_subset_rand, 1, Self}),
    ?expect_no_message(),
    Config.

test_on_vivaldi_shuffle(Config) ->
    config:write(vivaldi_count_measurements, 1),
    config:write(vivaldi_measurements_delay, 0),
    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    InitialState = {Coordinate, Confidence},
    _NewState = vivaldi:on_active({vivaldi_shuffle, comm:this(), [0.0, 0.0], 1.0},
                                 InitialState),
    receive
        {ping, SourcePid} -> comm:send(SourcePid, {pong, vivaldi})
    end,

    ?expect_message({update_vivaldi_coordinate, _Latency, {[0.0, 0.0], 1.0}}),
    ?expect_no_message(),
    % TODO: check the node's state
    Config.

test_on_cy_cache1(Config) ->
    Coordinate = [1.0, 1.0],
    Confidence = 1.0,
    InitialState = {Coordinate, Confidence},
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
    InitialState = {Coordinate, Confidence},
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
    InitialState = {Coordinate, Confidence},
    % non-empty node cache
    Cache = [node:new(comm:make_global(self()), ?RT:hash_key("10"), 0)],
    NewState =
        vivaldi:on_active({cy_cache, Cache}, InitialState),

    ?equals(NewState, InitialState),
    % if pids don't match, a get_state is send to the cached node's dht_node
    This = comm:this(),
    ?expect_message({?send_to_group_member, vivaldi,
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
    config:write(vivaldi_latency_timeout, 60000).

fake_dht_node() ->
    element(1, unittest_helper:start_subprocess(
              fun() -> pid_groups:join_as("vivaldi_SUITE_group", dht_node) end)).