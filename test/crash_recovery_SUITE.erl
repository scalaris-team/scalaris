%% @copyright 2012-2014 Zuse Institute Berlin

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
%% @doc    Unit tests for crash recovery with leases
%% @end
%% @version $Id$
-module(crash_recovery_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{crash_recovery_tests,[sequence], [
                                        test_crash_recovery
                                       ]}
    ].

all() ->
    [
     {group, crash_recovery_tests}
     ].

suite() -> [ {timetrap, {seconds, 40}} ].

group(crash_recovery_tests) ->
    [{timetrap, {seconds, 400}}];
group(_) ->
    suite().

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    unittest_helper:end_per_suite(Config).

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).


init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout)
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Ids = unittest_helper:get_evenly_spaced_keys(4),
            unittest_helper:make_ring_with_ids(Ids, [{config, [{log_path, PrivDir},
                                                               {leases, true}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% crash recovery test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_crash_recovery(_Config) ->
    % we create a ring with four nodes. We stop lease renewal on all
    % nodes and wait for the leases to timeout.
    DHTNodes = pid_groups:find_all(dht_node),

    % stop all nodes
    ct:pal("cr: stop all nodes"),
    [lease_helper:intercept_lease_renew(Node) || Node <- DHTNodes],
    lease_helper:wait_for_number_of_valid_active_leases(0),
    [gen_component:bp_del(Node, block_trigger) || Node <- DHTNodes],


    % trigger renewal on all nodes
    ct:pal("cr: renew all nodes ~p", [DHTNodes]),
    [ comm:send_local(Node, {l_on_cseq, renew_leases}) || Node <- DHTNodes],
    
    % wait for leases to reappear
    ct:pal("cr: wait for leases to reappear"),
    lease_helper:wait_for_number_of_valid_active_leases(4).
