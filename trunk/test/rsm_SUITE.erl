% @copyright 2008-2011 Zuse Institute Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Unit tests for src/grouped_node/rsm/*.erl.
%%% @end
%% @version $Id$
-module(rsm_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

-behavior(rsm_beh).

-export([init_state/0, on/2]).

all() ->
    [deliver_1, deliver_1_add_1_deliver1].
    %[deliver_1_add_1_deliver1].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = unittest_helper:fix_cwd(),
    Config.

end_per_testcase(_TestCase, Config) ->
    _ = unittest_helper:stop_pid_groups(),
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
deliver_1(Config) ->
    start_env(Config),
    {ok, _SupPid} = rsm_api:start_link("rsm_SUITE_deliver", first, rsm_SUITE),
    Pid = comm:make_global(pid_groups:pid_of("rsm_SUITE_deliver", rsm_node)),

    % first deliver
    Msg = {msg1},
    rsm_api:deliver(Msg, Pid),
    unittest_helper:wait_for((rsm_app_state([Pid], [Msg])), 500),
    unittest_helper:wait_for((rsm_version([Pid], 1)), 500),

    ok.

deliver_1_add_1_deliver1(Config) ->
    start_env(Config),
    {ok, _SupPid} = rsm_api:start_link("rsm_SUITE_node1", first, rsm_SUITE),
    Pid = comm:make_global(pid_groups:pid_of("rsm_SUITE_node1", rsm_node)),

    % first deliver
    Msg = {msg1},
    rsm_api:deliver(Msg, Pid),
    unittest_helper:wait_for((rsm_app_state([Pid], [Msg])), 500),

    %add node
    {ok, _SupPid2} = rsm_api:start_link("rsm_SUITE_node2", {join, [Pid]}, rsm_SUITE),
    Pid2 = comm:make_global(pid_groups:pid_of("rsm_SUITE_node2", rsm_node)),
    unittest_helper:wait_for((rsm_size(Pid, 2)), 500),
    unittest_helper:wait_for((rsm_version([Pid, Pid2], 2)), 500),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rsm behavior
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init_state() ->
    [].

on(Msg, State) ->
    [Msg | State].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% utility
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_env(Config) ->
    {ok, _GroupsPid} = pid_groups:start_link(),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    ConfigOptions = unittest_helper:prepare_config([{config, [{log_path, PrivDir}]}]),
    {ok, _ConfigPid} = config:start_link2(ConfigOptions),
    {ok, _LogPid} = log:start_link(),
    {ok, _CommPid} = sup_comm_layer:start_link(),
    {ok, _FDPid} = fd:start_link("fd_group"),
    comm:send({{127,0,0,1}, unittest_helper:get_scalaris_port(), self()}, {foo}), %argh
    unittest_helper:wait_for((fun () -> comm:is_valid(comm:this()) end), 500).

rsm_size(Pid, Size) ->
    fun () ->
            {View, _AppState} = rsm_api:get_single_state(Pid),
            length(rsm_view:get_members(View)) == Size
    end.

rsm_version(Pids, Version) ->
    fun () ->
            ct:log("~w", [Pids]),
            Versions = lists:usort(lists:map(fun (Pid) ->
                                                     {View, _AppState} = rsm_api:get_single_state(Pid),
                                                     rsm_view:get_version(View)
                                             end, Pids)),
            ct:log("~w", [Versions]),
            Versions == [Version]
    end.

rsm_app_state(Pids, ExpectedAppState) ->
    fun () ->
            lists:all(
              fun (Pid) ->
                      {_View, AppState} = (catch rsm_api:get_single_state(Pid)),
                      AppState  == ExpectedAppState
              end, Pids)
    end.
