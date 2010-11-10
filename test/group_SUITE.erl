% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%%% @doc    Unit tests for src/grouped_node/*.erl.
%%% @end
%% @version $Id$
-module(group_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [add_9, add_9_remove_4, db_repair, group_split, group_split_with_data].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    scalaris2:stop(),
    unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    unittest_helper:fix_cwd(),
    scalaris2:start(),
    config:write(dht_node_sup, sup_group_node),
    config:write(dht_node, group_node),
    config:write(group_node_trigger, trigger_periodic),
    config:write(group_node_base_interval, 30000),
    Config.

end_per_testcase(_TestCase, Config) ->
    scalaris2:stop(),
    Config.

add_9(_Config) ->
    admin:add_nodes(9),
    wait_for(check_versions([{1, 11}], 10)),
    ok.

add_9_remove_4(_Config) ->
    admin:add_nodes(9),
    wait_for(check_versions([{1, 11}], 10)),
    admin:del_nodes(4),
    timer:sleep(3000),
    wait_for(check_versions([{1, 15}], 6)),
    ok.

db_repair(_Config) ->
    % add one node
    admin:add_nodes(1),
    % check group_state
    wait_for(check_versions([{1, 3}], 2)),
    % check db
    wait_for(check_dbs([{is_current, 0}], 2)),
    % write one kv-pair
    group_api:paxos_write(1,2),
    % add one node
    admin:add_nodes(1),
    % check group_state
    wait_for(check_versions([{1, 5}], 3)),
    % check db
    wait_for(check_dbs([{is_current, 1}], 3)),
    ok.

group_split(_Config) ->
    config:write(group_node_base_interval, 60000),
    config:write(group_max_size, 9),
    admin:add_nodes(9),
    % check group_state
    wait_for(check_versions([{1, 11}], 10)),
    % check db
    wait_for(check_dbs([{is_current, 0}], 10)),
    pid_groups:find_a(group_node) ! {trigger},
    timer:sleep(1000),
    wait_for(check_versions([{2, 2}, {3, 2}], 10)),
    ok.

group_split_with_data(_Config) ->
    config:write(group_node_base_interval, 60000),
    config:write(group_max_size, 9),
    admin:add_nodes(9),
    % check group_state
    wait_for(check_versions([{1, 11}], 10)),
    % check db
    wait_for(check_dbs([{is_current, 0}], 10)),
    group_api:paxos_write(1                      , 2),
    group_api:paxos_write(1 + rt_simple:n() div 2, 2),
    group_api:paxos_write(2                      , 2),
    group_api:paxos_write(2 + rt_simple:n() div 2, 2),
    wait_for(check_dbs([{is_current, 4}], 10)),
    pid_groups:find_a(group_node) ! {trigger},
    timer:sleep(1000),
    wait_for(check_versions([{2, 2}, {3, 2}], 10)),
    wait_for(check_dbs([{is_current, 1}, {is_current, 3}], 10)),
    ok.

wait_for(F) ->
    case F() of
        true ->
            ok;
        false ->
            timer:sleep(500),
            wait_for(F)
    end.

check_versions(ExpectedVersions, Length) ->
    fun () ->
            Versions = [V || {_, V} <- group_debug:dbg_version()],
            ct:pal("~p ~p", [lists:usort(Versions), group_debug:dbg_version()]),
            ExpectedVersions ==
                lists:usort(Versions) andalso length(Versions) == Length
    end.

check_dbs(ExpectedVersions, Length) ->
    fun () ->
            Versions = group_debug:dbg_db_without_pid(),
            ct:pal("db: ~p ~p", [lists:usort(Versions), length(Versions)]),
            lists:usort(Versions) == ExpectedVersions andalso
                length(Versions) == Length
    end.
