%% @copyright 2015-2017 Zuse Institute Berlin

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
-define(LEASES_DELTA, 1).

num_executions() ->
    5.

repeater_num_executions() ->
    1000.

ring_size() ->
    config:read(replication_factor).

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

     {remove_node_group_repeater, [sequence], [write, {group, remove_node_repeater}]},
     {remove_node_repeater, [sequence, {repeat, repeater_num_executions()}], [remove_node]},

     {repeater, [{group, remove_node_group_repeater}]}

    ].

suite() -> [ {timetrap, {seconds, 1200}} ].

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
    RingSize = ring_size(),
    Config3 = unittest_helper:stop_minimal_procs(Config2),

    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config3),
    unittest_helper:make_ring(RingSize, [{config, [{log_path, PrivDir},
                                                      {leases, true},
                                                      {leases_delta, ?LEASES_DELTA},
                                                      {db_backend, db_mnesia}]}]),
    unittest_helper:check_ring_size_fully_joined(ring_size()),
    LeasesTimeout = config:read(leases_delta) * 1000 + 1000,
    Config4 = [{leases_timeout, LeasesTimeout} | Config3],
    unittest_helper:init_per_group(Group, Config4).

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

init_per_testcase(_TestCase = remove_node, Config) ->
    case config:read(replication_factor) of
        3 ->
           {skip, "single node failure might cause data loss in aysmmetric rings for R=3."};
        _ ->
            Config
    end;
init_per_testcase(_TestCase, Config) ->
    Config.

remove_node() ->
     [
      {timetrap,{seconds,180}}
     ].

rw_suite_runs(N) ->
    erlang:min(N, 200).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test create_ring/1 of mnesia recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_make_ring(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:stop_ring(),
    wait_for_expired_leases(Config),
    unittest_helper:make_ring_recover([{config, [{log_path, PrivDir},
                                                 {leases, true},
                                                 {leases_delta, ?LEASES_DELTA},
                                                 {db_backend, db_mnesia},
                                                 {start_type, recover}]}]),
    lease_checker2:wait_for_clean_leases(500, [{ring_size, ring_size()}]),
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
    wait_for_expired_leases(Config),
    unittest_helper:make_ring_recover( [{config, [{log_path, PrivDir},
                                                  {leases, true},
                                                  {leases_delta, ?LEASES_DELTA},
                                                  {db_backend, db_mnesia},
                                                  {start_type, recover}]}]),
    lease_checker2:wait_for_clean_leases(500, [{ring_size, ring_size()}]),
    %% ring restored -> checking KV data integrity
    _ = check_data_integrity(),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test remove_node/1 remove a node and ensure data integrity after recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
remove_node(Config) ->
    ct:pal("wait for check_leases"),
    lease_checker2:wait_for_clean_leases(500, [{ring_size, ring_size()}]),
    SaveNode = lease_checker:get_random_save_node(),
    case SaveNode of
        failed ->
            true;
        _ ->
            RandomNode = comm:make_local(SaveNode),
            io:format("show prbr statistics for the ring~n"),
            lease_checker2:get_kv_db(),

            io:format("show prbr statistics for node to be killed~n"),
            lease_checker2:get_kv_db(RandomNode),

            ct:pal("PRBR state before node is removed"),
            print_prbr_data(kv_db),
            _ = print_leases_data(),

            %% get relative range of node to remove and check if it is not to large
            {true, LL} = lease_checker:get_dht_node_state_unittest(comm:make_global(RandomNode), lease_list),
            NodeRange = l_on_cseq:get_range(lease_list:get_active_lease(LL)),
            RelativeRange = lease_checker:get_relative_range_unittest(NodeRange),
            ct:pal("Statistics of removed node ~p~n"
                   "Interval of node to be removed:~nNode Interval ~p~n"
                   "Relative Range ~p", [RandomNode, NodeRange, RelativeRange]),

            R = config:read(replication_factor),
            SaveFraction = quorum:minority(R) / R,
            ct:pal("Safe relative range to remove ~p", [SaveFraction]),
            ?assert_w_note(RelativeRange =< SaveFraction, "Removing a safe node means that only"
                           " one replica should be affected"),

            %% prbr data of node for diagnostic purpose...
            comm:send_local(RandomNode, {prbr, tab2list_raw, kv_db, self()}),
            receive
                {_, NodeData} -> NodeData
            end,
            Values = [prbr:entry_val(E) || E <- NodeData],
            ct:pal("Number of values: ~p~nNumber of unique values: ~p~nValue list:~p",
                   [length(Values), length(lists:usort(Values)), lists:sort(Values)]),

            %% The tests starts here...
            PidGroup = pid_groups:group_of(RandomNode),
            PidGroupTabs = [Table || Table <- db_mnesia:get_persisted_tables(),
                                     element(2, db_util:parse_table_name(Table)) =:= PidGroup],
            ct:pal("kill node"),
            {[PidGroup], _Not_found} = admin:del_nodes_by_name([PidGroup], false),
            %% wait for leases to expire
            ct:pal("wait for leases to expire"),
            wait_for_expired_leases(Config),
            _ = [?ASSERT(db_mnesia:close_and_delete(db_mnesia:open(X))) || X <- PidGroupTabs],
            ct:pal("wait for check_leases"),
            lease_checker2:wait_for_clean_leases(500, [{ring_size, ring_size()-1}]),

            ct:pal("PRBR state after leases expired"),
            print_prbr_data(kv_db),
            _ = print_leases_data(),

            %% check data integrity
            ct:pal("check data integrity"),
            _ = check_data_integrity(),
            %% "repair" replicas
            ct:pal("repair replicas"),
            _ = repair_replicas(),

            ct:pal("PRBR state after calling repair_replicas"),
            print_prbr_data(kv_db),
            _ = print_leases_data(),

            %% add node to reform ring_size() node ring
            ct:pal("add node"),
            _ = admin:add_nodes(1),
            ct:pal("sleep"),
            timer:sleep(3000),
            ct:pal("check_ring_size_fully_joined"),
            unittest_helper:check_ring_size_fully_joined(ring_size()),
            ct:pal("wait for check_leases"),
            lease_checker2:wait_for_clean_leases(500, [{ring_size, ring_size()}]),

            ct:pal("PRBR state after node was inserted"),
            print_prbr_data(kv_db),
            _ = print_leases_data(),

            true
    end.

check_data_integrity() ->
    io:format("show prbr statistics for the ring~n"),
    lease_checker2:get_kv_db(),
    Pred = fun (Id) ->
                   case kv_on_cseq:read(integer_to_list(Id)) of
                       {ok, Id} -> true;
                       {fail, not_found} -> false
                   end
           end,
    Elements = lists:filter(Pred, lists:seq(1, 100)),
    case length(Elements) of
        100 ->
            true;
        X ->
            ct:pal("found ~p of 100 elements", [X]),
            Missing = lists:subtract(lists:seq(1, 100), Elements),
            ct:pal("Missing elements are:~n~w", [Missing]),
            ct:pal("Printing missing element data..."),
            [print_element_data(E, kv_db) || E <- Missing],
            print_prbr_data(kv_db),

            100 = X
    end.

repair_replicas() ->
    case config:read(replication_factor) rem 2 =:= 1 of
        true -> %% only repair for odd replication factors
            io:format("show prbr statistics for the ring~n"),
            lease_checker2:get_kv_db(),
            _ = [kv_on_cseq:write(integer_to_list(X),X) || X <- lists:seq(1, 100)],
            io:format("show prbr statistics for the ring~n"),
            lease_checker2:get_kv_db();
        false ->
            ok
    end.

wait_for_expired_leases(Config) ->
    {leases_timeout, LeasesTimeout} = lists:keyfind(leases_timeout, 1, Config),
    timer:sleep(LeasesTimeout).

%%@doc Prints a list of tuples showing which value is stored in which dht node
%%     Format : [{Value, [list_of_dht_nodes_value_is_stored_in]}]
print_prbr_data(DB) ->
    PrbrData = get_prbr_data(fun(NodePid, E) ->
                                {prbr:entry_val(E), NodePid}
                             end, DB),
    GroupedByValueDict = lists:foldl(fun({K, V}, D) -> dict:append(K, V, D) end,
                                             dict:new(), PrbrData),
    GroupedValues = lists:sort(dict:to_list(GroupedByValueDict)),

    ct:pal("PRBR state ~w:~nFormat [{Value, [list_of_dht_nodes_value_is_stored_in]}]~n"
           "~100p", [DB, GroupedValues]),
    ok.

print_leases_data() ->
    _ = [print_prbr_data({lease_db, I}) || I <- lists:seq(1, config:read(replication_factor))].

print_element_data(Id, DB) ->
    HashedKey = ?RT:hash_key(integer_to_list(Id)),
    ReplicaKeyList = replication:get_keys(HashedKey),
    PrbrData = get_prbr_data(fun(NodePid, E) ->
                                {prbr:entry_key(E),
                                 prbr:entry_val(E),
                                 NodePid}
                             end, DB),
    IdData = lists:filter(fun(E) ->
                            lists:member(element(1, E), ReplicaKeyList)
                          end, PrbrData),

    ct:pal("Printing data for ID=~p~nReplica key list:~n~p~n"
           "Entries found in prbr:~n~100p",
           [Id, lists:sort(ReplicaKeyList), lists:sort(IdData)]),
    ok.

%% get all elements stored in prbr as flattened list.
%% applies DataExtractFun(DhtNodePidEFoundOn, E) for every entry E.
get_prbr_data(DataExtractFun, DB) ->
    DhtNodes = pid_groups:find_all(dht_node),
    lists:flatten(
        [begin
            comm:send_local(ThisNode, {prbr, tab2list_raw, DB, self()}),
            receive
                {_, List} -> [DataExtractFun(ThisNode, E) || E <- List]
            after 1000 ->
                ct:pal("DHT node ~p does not reply...", [ThisNode]),
                []
            end
         end || ThisNode <- DhtNodes]).
