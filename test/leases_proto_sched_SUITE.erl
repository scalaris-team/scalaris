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

%% @author Thorsten Schütt <schuett@zib.de>
%% @doc    Unit tests for leases.
%% @end
%% @version $Id$

-module(leases_proto_sched_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{join_tests, [sequence], [
                                 test_single_join
                              ]}
    ].

all() ->
    [
     {group, join_tests}
     ].

suite() -> [ {timetrap, {seconds, 300}} ].

group(join_tests) ->
    [{timetrap, {seconds, 10}}];
group(_) ->
    suite().

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir},
                                            {leases, true}]}]),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

-spec proto_sched_fun(start | stop) -> ok.
proto_sched_fun(start) ->
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
            proto_sched:wait_for_end()
    end.

-spec proto_sched2_fun(setup, ThreadNum::pos_integer()) -> ok;
                      (cleanup, PIDs::[pid() | atom()]) -> ok.
proto_sched2_fun(setup, Arg) ->
    proto_sched:thread_num(Arg);
proto_sched2_fun(cleanup, _Arg) ->
    proto_sched:wait_for_end(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    proto_sched:cleanup().

test_single_join(_Config) ->
    proto_sched2_fun(setup, 1),
    proto_sched_fun(start),
    {[_], []} = api_vm:add_nodes(1),
    lease_helper:wait_for_ring_size(5),
    proto_sched_fun(stop),
    proto_sched2_fun(cleanup, []),
    ?assert(admin:check_leases()),
    ok.
