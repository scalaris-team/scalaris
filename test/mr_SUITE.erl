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
-module(mr_SUITE).
-author('fajerski@zib.de').
-vsn('$Id$').

-compile([export_all]).

%% no proto scheduler for this suite
-define(proto_sched(_Action), ok).
-define(proto_sched2(Action, Arg), proto_sched2_fun(Action, Arg)).

-include("mr_SUITE.hrl").

all() ->
    tests_avail() ++ [test_join, test_leave].

suite() -> [ {timetrap, {seconds, 15}} ].

-spec proto_sched2_fun(setup, ThreadNum::pos_integer()) -> ok;
                      (cleanup, PIDs::[pid() | atom()]) -> ok.
proto_sched2_fun(setup, _Arg) ->
    ok;
proto_sched2_fun(cleanup, Arg) ->
    _ = [util:wait_for_process_to_die(Pid) || Pid <- Arg],
    ok.

test_join(_Config) ->
    Pids = pid_groups:find_all(dht_node),
    ct:pal("setting breakpoint before starting reduce phase"),
    NextPhase = fun(Msg, _State) ->
            case Msg of
                {bulkowner, start, _Id, _I, {mr, next_phase, _JobId, 1, _Interval}} ->
                    comm:send_local(self(), Msg),
                    drop_single;
                _ ->
                    false
            end
    end,
    [gen_component:bp_set_cond(Pid, NextPhase, mr_bp) || Pid <- Pids],
    ct:pal("starting job that triggers breakpoint"),
    MrPid = spawn_link(fun() ->
                    ct:pal("starting mr job"),
                    Res = api_mr:start_job(get_wc_job_erl()),
                    ct:pal("mr job finished"),
                    check_results(Res)
            end),
    timer:sleep(1000),
    ct:pal("adding node to provoke slide"),
    _ = api_vm:add_nodes(2),
    unittest_helper:check_ring_size_fully_joined(4),
    unittest_helper:wait_for_stable_ring_deep(),
    ct:pal("ring fully joined (4)"),
    ct:pal("removing breakpoints"),
    [gen_component:bp_del(Pid, mr_bp) || Pid <- Pids],
    util:wait_for_process_to_die(MrPid),
    ok.

test_leave(_Config) ->
    [_] = api_vm:shutdown_nodes(1),
    {[AddedNode], _} = api_vm:add_nodes(1),
    unittest_helper:check_ring_size_fully_joined(2),
    unittest_helper:wait_for_stable_ring_deep(),
    Pids = pid_groups:find_all(dht_node),
    ct:pal("setting breakpoint before starting reduce phase on ~p", [Pids]),
    NextPhase = fun(Msg, _State) ->
            case Msg of
                {bulkowner, start, _Id, _I, {mr, next_phase, _JobId, 1, _Interval}} ->
                    comm:send_local(self(), Msg),
                    drop_single;
                _ ->
                    false
            end
    end,
    [gen_component:bp_set_cond(Pid, NextPhase, mr_bp) || Pid <- Pids],
    ct:pal("starting job that triggers breakpoint"),
    MrPid = spawn_link(fun() ->
                    ct:pal("starting mr job"),
                    Res = api_mr:start_job(get_wc_job_erl()),
                    ct:pal("mr job finished"),
                    check_results(Res)
            end),
    timer:sleep(1000),
    ct:pal("shutting down node to provoke slide"),
    ok = api_vm:shutdown_node(AddedNode),
    [ct:pal("removing breakpoints on ~p...~p", [Pid, gen_component:bp_del_async(Pid, mr_bp)]) || Pid <- Pids],
    util:wait_for_process_to_die(MrPid),
    ok.
