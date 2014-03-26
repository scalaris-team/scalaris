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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Unit tests for src/dht_node_join.erl in combination with
%%         src/dht_node_move.erl.
%% @end
%% @version $Id$
-module(join_leave_proto_sched_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

%% start proto scheduler for this suite
%% start proto scheduler for this suite
-define(proto_sched(Action),
        fun() -> %% use fun to have fresh, locally scoped variables
                case Action of
                    start ->
                        %% ct:pal("Starting proto scheduler"),
                        proto_sched:thread_num(1),
                        proto_sched:thread_begin();
                    stop ->
                        %% is a ring running?
                        case erlang:whereis(pid_groups) =:= undefined
                            orelse pid_groups:find_a(proto_sched) =:= failed of
                            true -> ok;
                            false ->
                                %% then finalize proto_sched run:
                                %% try to call thread_end(): if this
                                %% process was running the proto_sched
                                %% thats fine, otherwise thread_end()
                                %% will raise an exception
                                proto_sched:thread_end(),
                                proto_sched:wait_for_end(),
                                ct:pal("Proto scheduler stats: ~.2p",
                                       [proto_sched:info_shorten_messages(proto_sched:get_infos(), 200)]),
                                proto_sched:cleanup()
                        end
                end
        end()).

suite() -> [ {timetrap, {seconds, 60}} ].

test_cases() ->
    [
     tester_join_at,
     %add_9, % TODO
     rm_5,
     %add_9_rm_5, % TODO
     %add_2x3_load, % TODO
     tester_join_at_timeouts
    ].

groups() ->
%%     unittest_helper:create_ct_groups(test_cases(), [{add_9_rm_5, [sequence, {repeat_until_any_fail, forever}]}]).
    unittest_helper:create_ct_groups([join_lookup], [{join_lookup, [sequence, {repeat_until_any_fail, 20}]}]) ++
    unittest_helper:create_ct_groups([add_3_rm_3_data], [{add_3_rm_3_data, [sequence, {repeat_until_any_fail, 20}]}]) ++
    unittest_helper:create_ct_groups([add_3_rm_3_data_inc], [{add_3_rm_3_data_inc, [sequence, {repeat_until_any_fail, 20}]}]) ++
    [{graceful_leave_load, [sequence, {repeat_until_any_fail, 5}], [make_4_add_1_rm_1_load, make_4_add_2_rm_2_load, make_4_add_3_rm_3_load]}].

all() ->
%%     unittest_helper:create_ct_all(test_cases()).
    unittest_helper:create_ct_all([join_lookup]) ++
%        unittest_helper:create_ct_all([add_3_rm_3_data]) ++ % TODO
%        unittest_helper:create_ct_all([add_3_rm_3_data_inc]) ++ % TODO
        [{group, graceful_leave_load}] ++
        test_cases().

-include("join_leave_SUITE.hrl").
