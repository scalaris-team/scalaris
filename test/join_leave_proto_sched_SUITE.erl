% @copyright 2010-2016 Zuse Institute Berlin

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
-define(proto_sched(Action), proto_sched_fun(Action)).

%% start proto scheduler with a second thread running function given as Arg
%% Action :: start | restart | stop.  Arg :: fun()
-define(proto_sched2(Action, Arg), proto_sched2_fun(Action, Arg)).

%% Use different bench parameters when using proto sched
%%  bench:increment(Threads1, Iterations1) for proto sched
%%  bench:increment(Threads2, Iterations2) otherwise
-define(bench(Threads1, Iterations1, Threads2, Iterations2),
        bench:increment(Threads1, Iterations1)).

suite() -> [ {timetrap, {seconds, 60}} ].

test_cases() ->
    ReplicationFactor = config:read(replication_factor),
    if
        ReplicationFactor > 16 -> [];
        true                   -> [ tester_join_at ]
    end.

groups() ->
    ReplicationFactor = config:read(replication_factor),
    AddRMGroup =
        if
            ReplicationFactor > 16 -> [ add_9, rm_5, add_9_rm_5               ];
            true                   -> [ add_9, rm_5, add_9_rm_5, add_2x3_load ]
        end,
    unittest_helper:create_ct_groups([join_lookup], [{join_lookup, [sequence, {repeat_until_any_fail, 20}]}]) ++
    unittest_helper:create_ct_groups([add_3_rm_3_data], [{add_3_rm_3_data, [sequence, {repeat_until_any_fail, 20}]}]) ++
    unittest_helper:create_ct_groups([add_3_rm_3_data_inc], [{add_3_rm_3_data_inc, [sequence, {repeat_until_any_fail, 20}]}]) ++
    [{add_rm, [sequence, {repeat_until_any_fail, 5}], AddRMGroup}] ++
    [{graceful_leave_load, [sequence, {repeat_until_any_fail, 5}], [make_4_add_1_rm_1_load, make_4_add_2_rm_2_load, make_4_add_3_rm_3_load]}].

all() ->
    unittest_helper:create_ct_all([join_lookup]) ++
        unittest_helper:create_ct_all([add_3_rm_3_data]) ++
%        unittest_helper:create_ct_all([add_3_rm_3_data_inc]) ++ % TODO: too heavy for proto_sched to finish within 120s
        [{group, add_rm},
         {group, graceful_leave_load}] ++
        test_cases().

-spec additional_ring_config() -> [{stabilization_interval_base, 100000},...].
additional_ring_config() ->
    % increase ring stabilisation interval since proto_sched infections get
    % lost if rm subscriptions are triggered instead of continuing due to our
    % direct (and infected) messages!
    [{stabilization_interval_base, 100000}, % ms
     {tman_cyclon_interval, 100} % s
    ].

-include("join_leave_SUITE.hrl").

-spec proto_sched_fun(start | restart | stop) -> ok.
proto_sched_fun(start) ->
    %% ct:pal("Starting proto scheduler"),
    unittest_helper:print_proto_sched_stats(at_end_if_failed), % clear previous stats
    proto_sched:thread_num(1),
    proto_sched:thread_begin();
proto_sched_fun(restart) ->
    %% ct:pal("Starting proto scheduler"),
    proto_sched:thread_num(1),
    proto_sched:thread_begin();
proto_sched_fun(stop) ->
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
            unittest_helper:print_proto_sched_stats(at_end_if_failed_append),
            proto_sched:cleanup()
    end.

-spec proto_sched2_fun(start, Fun::fun(() -> term())) -> pid();
                      (stop, Pid::pid()) -> ok.
proto_sched2_fun(start, Fun) ->
    unittest_helper:print_proto_sched_stats(at_end_if_failed), % clear previous stats
    proto_sched:thread_num(2),
    Pid = erlang:spawn(fun() ->
                               proto_sched:thread_begin(),
                               Fun(),
                               proto_sched:thread_end()
                       end),
    proto_sched:thread_begin(),
    Pid;
proto_sched2_fun(stop, _Pid) ->
    proto_sched_fun(stop).
