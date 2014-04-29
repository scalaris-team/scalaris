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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    Unit tests for the map reduce protocol.
%% @end
%% @version $Id$
-module(mr_proto_sched_SUITE).
-author('fajerski@zib.de').
-vsn('$Id$').

-compile([export_all]).

%% start proto scheduler for this suite
-define(proto_sched(Action),
        fun() -> %% use fun to have fresh, locally scoped variables
                case Action of
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
                                proto_sched:thread_end()
                        end;
                    start ->
                        proto_sched:thread_begin()
                end
        end()).
-define(proto_sched2(Action, Arg),
        fun() ->
                case Action of
                    setup ->
                        proto_sched:thread_num(Arg);
                    cleanup ->
                        proto_sched:wait_for_end(),
                        ct:pal("Proto scheduler stats: ~.2p",
                               [proto_sched:info_shorten_messages(proto_sched:get_infos(), 200)]),
                        proto_sched:cleanup()
                end
        end()).

-include("mr_SUITE.hrl").

all() ->
tests_avail() ++ [test_join, test_leave].

suite() -> [ {timetrap, {seconds, 15}} ].

test_join(_Config) ->
    ?proto_sched2(setup, 2),
    spawn(fun() ->
                       ?proto_sched(start),
                       ct:pal("starting mr job"),
                       Res = api_mr:start_job(get_wc_job_erl()),
                       ct:pal("mr job finished"),
                       check_results(Res),
                       ?proto_sched(stop)
               end),
    ct:pal("adding node to provoke slide"),
    spawn(fun() ->
                       ?proto_sched(start),
                       api_vm:add_nodes(2),
                       ?proto_sched(stop)
               end),
    ?proto_sched2(cleanup, []),
    ct:pal("ring fully joined (4)"),
    ok.

test_leave(_Config) ->
    api_vm:shutdown_nodes(1),
    {[AddedNode], _} = api_vm:add_nodes(1),
    unittest_helper:check_ring_size_fully_joined(2),
    unittest_helper:wait_for_stable_ring_deep(),
    ?proto_sched2(setup, 2),
    spawn_link(fun() ->
                       ?proto_sched(start),
                       ct:pal("starting mr job"),
                       Res = api_mr:start_job(get_wc_job_erl()),
                       ct:pal("mr job finished"),
                       check_results(Res),
                       ?proto_sched(stop)
               end),
    ct:pal("shutting down node ~p to provoke slide", [AddedNode]),
    spawn_link(fun() ->
                       ?proto_sched(start),
                       api_vm:shutdown_node(AddedNode),
                       ?proto_sched(stop)
               end),
    ?proto_sched2(cleanup, []),
    ok.
