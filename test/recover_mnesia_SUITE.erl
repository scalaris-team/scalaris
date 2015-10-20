%% @copyright 2015 Zuse Institute Berlin

%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.

%% @author Tanguy Racinet <tanracinet@gmail.com>
%% @doc    Unit tests for the mnesia recovery.
%% @end
%% @version $Id$
-module(recover_mnesia_SUITE).

-include("scalaris.hrl").
-include("unittest.hrl").

-author('tanracinet@gmail.com').
-vsn('$Id$').

-compile(export_all).

-define(CLOSE, close).

num_executions() ->
    5.

repeater_num_executions() ->
    10.

ring_size() ->
    4.

all() -> [
          {group, make_ring_group},
          {group, remove_node_group}
         ].
groups() ->
    [
     {make_ring_group, [sequence], [test_make_ring, write, {group, recover_data_group}]},
     {recover_data_group, [sequence, {repeat, num_executions()}], [read]},
     {remove_node_group, [sequence], [write, {group, remove_node}]},
     {remove_node, [sequence, {repeat, num_executions()}], [remove_node]},

     {make_ring_group_repeater, [sequence], [test_make_ring, write,
                                             {group, recover_data_group_repeater}]},
     {recover_data_group_repeater, [sequence, {repeat, repeater_num_executions()}], [read]},
     {remove_node_group_repeater, [sequence], [write, {group, remove_node_repeater}]},
     {remove_node_repeater, [sequence, {repeat, repeater_num_executions()}], [remove_node]},

     {repeater, [{repeat, 30}], [{group, make_ring_group_repeater},
                                 {group, remove_node_group_repeater}]}

    ].

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(recover_data_group = Group, Config) ->
    unittest_helper:init_per_group(Group, Config);
init_per_group(recover_data_group_repeater = Group, Config) ->
    unittest_helper:init_per_group(Group, Config);
init_per_group(remove_node = Group, Config) ->
    unittest_helper:init_per_group(Group, Config);
init_per_group(remove_node_repeater = Group, Config) ->
    unittest_helper:init_per_group(Group, Config);
init_per_group(Group, Config) ->
    ct:pal("stop ring and clean repository from previous test case (it may have run into a timeout)"),
    %% stop ring and clean repository from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    _ = application:stop(mnesia),
    %% need config to get db path
    Config2 = unittest_helper:start_minimal_procs(Config, [], false),
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
        "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
    _ = file:delete(WorkingDir ++ "schema.DAT"),
    unittest_helper:stop_minimal_procs(Config2),

    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(ring_size(), [{config, [{log_path, PrivDir},
                                                      {leases, true},
                                                      {db_backend, db_mnesia}]}]),
    unittest_helper:check_ring_size_fully_joined(ring_size()),
    unittest_helper:init_per_group(Group, Config).

end_per_group(recover_data_group = Group, Config) ->
    unittest_helper:end_per_group(Group, Config);
end_per_group(recover_data_group_repeater = Group, Config) ->
    unittest_helper:end_per_group(Group, Config);
end_per_group(remove_node = Group, Config) ->
    unittest_helper:end_per_group(Group, Config);
end_per_group(remove_node_repeater = Group, Config) ->
    unittest_helper:end_per_group(Group, Config);
end_per_group(repeater = Group, Config) ->
    unittest_helper:end_per_group(Group, Config);
end_per_group(Group, Config) ->
    ct:pal("stop ring, stop mnesia and clean repository"),
    %% stop ring, stop mnesia and clean repository
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
        "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
    Tabs = lists:delete(schema, mnesia:system_info(tables)),
    unittest_helper:stop_ring(),
    _ = application:stop(mnesia),
    [ok = file:delete(WorkingDir ++ atom_to_list(X)++".DCD")||X<-Tabs],
    ok = file:delete(WorkingDir ++ "schema.DAT"),
    unittest_helper:end_per_group(Group, Config).

rw_suite_runs(N) ->
    erlang:min(N, 200).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test create_ring/1 of mnesia recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_make_ring(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:stop_ring(),
    %% wait for leases to expire
    timer:sleep(11000),
    unittest_helper:make_ring_recover([{config, [{log_path, PrivDir},
                                                 {leases, true},
                                                 {db_backend, db_mnesia},
                                                 {start_type, recover}]}]),
    lease_checker2:wait_for_clean_leases(500, 4),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test write/1 write data to KV DBs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
write(_Config) ->
    %% write data to KV
    lease_checker2:get_kv_db(),
    _ = [kv_on_cseq:write(integer_to_list(X),X) || X <- lists:seq(1, 100)],
    lease_checker2:get_kv_db(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test read/1 ensure data integrity after recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    lease_checker2:get_kv_db(),
    unittest_helper:stop_ring(),
    %% wait for leases to expire
    timer:sleep(11000),
    unittest_helper:make_ring_recover( [{config, [{log_path, PrivDir},
                                                  {leases, true},
                                                  {db_backend, db_mnesia},
                                                  {start_type, recover}]}]),
    lease_checker2:wait_for_clean_leases(500, 4),
    %% ring restored -> checking KV data integrity
    _ = check_data_integrity(),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test remove_node/1 remove a node and ensure data integrity after recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
remove_node(_Config) ->
    ct:pal("wait for check_leases"),
    lease_checker2:wait_for_clean_leases(500, 4),
    %% delete random node from ring
    RandomNode = comm:make_local(lease_checker:get_random_save_node()),
    PidGroup = pid_groups:group_of(RandomNode),
    PidGroupTabs = [Table || Table <- db_mnesia:get_persisted_tables(),
                             element(2, db_util:parse_table_name(Table)) =:= PidGroup],
    ct:pal("kill node"),
    {[PidGroup], _Not_found} = admin:del_nodes_by_name([PidGroup], false),
    %% wait for leases to expire
    ct:pal("wait for leases to expire"),
    timer:sleep(11000),
    _ = [?ASSERT(db_mnesia:close_and_delete(db_mnesia:open(X))) || X <- PidGroupTabs],
    ct:pal("wait for check_leases"),
    lease_checker2:wait_for_clean_leases(500, 3),
    %% check data integrity
    ct:pal("check data integrity"),
    _ = check_data_integrity(),
    %% add node to reform ring_size() node ring
    ct:pal("add node"),
    _ = admin:add_nodes(1),
    ct:pal("sleep"),
    timer:sleep(3000),
    ct:pal("check_ring_size_fully_joined"),
    unittest_helper:check_ring_size_fully_joined(ring_size()),
    ct:pal("wait for check_leases"),
    lease_checker2:wait_for_clean_leases(500, 4),
    true.

check_data_integrity() ->
    lease_checker2:get_kv_db(),
    Pred = fun (Id) ->
                   case kv_on_cseq:read(integer_to_list(Id)) of
                       {ok, Id} -> true;
                       _        -> false
                   end
           end,
    Elements = lists:filter(Pred, lists:seq(1, 100)),
    case length(Elements) of
        100 ->
            true;
        X ->
            ct:pal("found ~p of 100 elements", [X]),
            100 = X
    end.
