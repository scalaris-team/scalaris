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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    Unit tests for the map reduce protocol.
%% @end
%% @version $Id$
-module(mr_proto_sched_SUITE).
-author('fajerski@zib.de').
-vsn('$Id$').

-compile([export_all]).

-define(proto_sched(Action),
        fun() ->
                case Action of
                    start ->
                        ct:pal("Starting proto scheduler"),
                        proto_sched:start(),
                        proto_sched:start_deliver();
                    stop ->
                        proto_sched:stop(),
                        case erlang:whereis(pid_groups) =:= undefined orelse pid_groups:find_a(proto_sched) =:= failed of
                            true -> ok;
                            false -> ct:pal("Proto scheduler stats: ~.2p", proto_sched:get_infos()),
                                     proto_sched:cleanup()
                        end
                end
        end()).

-include("mr_SUITE.hrl").

all() ->
    tests_avail() ++ [test_join, test_leave].

suite() -> [ {timetrap, {seconds, 9}} ].

test_join(_Config) ->
    ct:pal("starting job that triggers breakpoint"),
    MrPid = spawn_link(fun() ->
                               ?proto_sched(start),
                               ct:pal("starting mr job"),
                               Res = api_mr:start_job(get_wc_job_erl()),
                               ct:pal("mr job finished"),
                               check_results(Res)
                       end),
    ct:pal("adding node to provoke slide"),
    _AddPid = spawn_link(fun() ->
                                 ?proto_sched(start),
                                 api_vm:add_nodes(2)
                         end),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size_fully_joined(4),
    ct:pal("ring fully joined (4)"),
    util:wait_for_process_to_die(MrPid),
    ok.

test_leave(_Config) ->
    api_vm:shutdown_nodes(1),
    {[AddedNode], _} = api_vm:add_nodes(1),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size_fully_joined(2),
    MrPid = spawn_link(fun() ->
                               ?proto_sched(start),
                               ct:pal("starting mr job"),
                               Res = api_mr:start_job(get_wc_job_erl()),
                               ct:pal("mr job finished"),
                               check_results(Res)
                       end),
    ct:pal("shutting down node ~p to provoke slide", [AddedNode]),
    _VMPid = spawn_link(fun() ->
                                ?proto_sched(start),
                                api_vm:shutdown_node(AddedNode)
                        end),
    util:wait_for_process_to_die(MrPid),
    ok.

