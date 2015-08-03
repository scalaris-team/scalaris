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

%% no proto scheduler for this suite
-define(proto_sched(Action), ok).

%% spawn a process running the given function
-define(proto_sched2(Action, Arg), proto_sched2_fun(Action, Arg)).

%% Use different bench parameters when using proto sched
%%  bench:increment(Threads1, Iterations1) for proto sched
%%  bench:increment(Threads2, Iterations2) otherwise
-define(bench(Threads1, Iterations1, Threads2, Iterations2),
        ct:pal("Threads: ~w. Its: ~w", [Threads2, Iterations2]),
        bench:increment(Threads2, Iterations2)).

suite() -> [ {timetrap, {seconds, 60}} ].

test_cases() ->
    [
     tester_join_at,
     add_9, rm_5, add_9_rm_5,
     add_2x3_load,
     tester_join_at_timeouts
    ].

groups() ->
%%     unittest_helper:create_ct_groups(test_cases(), [{add_9_rm_5, [sequence, {repeat_until_any_fail, forever}]}]).
    unittest_helper:create_ct_groups([join_lookup], [{join_lookup, [sequence, {repeat_until_any_fail, 5}]}]) ++
    unittest_helper:create_ct_groups([add_3_rm_3_data], [{add_3_rm_3_data, [sequence, {repeat_until_any_fail, 5}]}]) ++
    unittest_helper:create_ct_groups([add_3_rm_3_data_inc], [{add_3_rm_3_data_inc, [sequence, {repeat_until_any_fail, 5}]}]) ++
    [{graceful_leave_load, [sequence, {repeat_until_any_fail, 5}], [make_4_add_1_rm_1_load, make_4_add_2_rm_2_load, make_4_add_3_rm_3_load]}].

all() ->
%%     unittest_helper:create_ct_all(test_cases()).
    unittest_helper:create_ct_all([join_lookup]) ++
        unittest_helper:create_ct_all([add_3_rm_3_data]) ++
        unittest_helper:create_ct_all([add_3_rm_3_data_inc]) ++
        [{group, graceful_leave_load}] ++
        test_cases().

-spec additional_ring_config() -> [].
additional_ring_config() ->
    [].

-include("join_leave_SUITE.hrl").

-spec proto_sched2_fun(start, Fun::fun(() -> term())) -> pid();
                      (stop, Pid::pid()) -> ok.
proto_sched2_fun(start, Fun) ->
    erlang:spawn(Fun);
proto_sched2_fun(stop, Pid) ->
    wait_for_process_to_die(Pid).
