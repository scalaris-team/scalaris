%% @copyright 2014 Zuse Institute Berlin

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
%% @doc    Unit tests for lease_watcher.erl
%% @end
%% @version $Id$
-module(lease_watcher_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{tester_tests, [sequence], [
                                 tester_type_check_lease_watcher
                              ]},
     {crash_recovery_tests, [sequence], [
                               test_simple_crash_recovery
                               ]}
    ].

all() ->
    [
     {group, tester_tests},
     {group, crash_recovery_tests}
     ].

suite() -> [ {timetrap, {seconds, 400}} ].

group(tester_tests) ->
    [{timetrap, {seconds, 400}}];
group(crash_recovery_tests) ->
    [{timetrap, {seconds, 40}}];
group(_) ->
    suite().

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout)
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Ids = ?RT:get_replica_keys(rt_SUITE:number_to_key(0)),
            unittest_helper:make_ring_with_ids(Ids, [{config, [{log_path, PrivDir},
                                                               {leases, true}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

tester_type_check_lease_watcher(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {lease_watcher,
           [
            {start_link, 1},
            {on, 2},
            {init, 1}
           ],
           [
            {process_lease, 1}
           ]}
        ],
    %% join a dht_node group to be able to call lease trigger functions
    pid_groups:join(pid_groups:group_with(dht_node)),
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% simple unit test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_simple_crash_recovery(_Config) ->
    % 1. deactivate leases
    deactivate_leases(),
    % there are no valid leases
    % 2. trigger all lease-watchers
    [comm:send_local(DhtNodePid, {trigger})
           || DhtNodePid <- pid_groups:find_all(lease_watcher)],
    timer:sleep((l_on_cseq:unittest_get_delta() + 1) * 1000),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

deactivate_leases() ->
    DHTNodes = pid_groups:find_all(dht_node),
    [l_on_cseq:unittest_clear_lease_list(Pid) || Pid <- DHTNodes],
    ok.
