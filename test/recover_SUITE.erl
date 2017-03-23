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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Unit tests for recovery.
%% @end
-module(recover_SUITE).

-include("scalaris.hrl").
-include("unittest.hrl").

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(CLOSE, close).

groups() ->
    [{slide_tests, [sequence], [
                                half_join_and_recover_after_step2,
                                half_join_and_recover_after_step3,
                                half_join_and_recover_after_step4
                               ]},
     {leave_tests, [sequence], [
                                %% half_leave_and_recover_after_step1,
                                %% half_leave_and_recover_after_step2,
                                half_leave_and_recover_after_step3,
                                half_leave_and_recover_after_step4
                               ]},
     {repeater, [{repeat, 10}], [{group, slide_tests}, {group, leave_tests}]}
    ].

all() -> [].

suite() -> [ {timetrap, {seconds, 60}} ].

group(_) ->
    suite().

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir},
                                                   {leases, true},
                                                   {db_backend, db_mnesia}]}]),
    Config.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    _ = application:stop(mnesia),
    %% need config to get db path
    Config2 = unittest_helper:start_minimal_procs(Config, [], false),
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
        "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
    _ = file:delete(WorkingDir ++ "schema.DAT"),
    _ = unittest_helper:stop_minimal_procs(Config2),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test interrupted split before recover
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

half_join_and_recover_after_step2(Config) ->
    half_join_and_recover(Config, split_reply_step2).

half_join_and_recover_after_step3(Config) ->
    half_join_and_recover(Config, split_reply_step3).

half_join_and_recover_after_step4(Config) ->
    half_join_and_recover(Config, split_reply_step4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test interrupted merge before recover
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

half_leave_and_recover_after_step1(Config) ->
    half_leave_and_recover(Config, merge_reply_step1).

half_leave_and_recover_after_step2(Config) ->
    half_leave_and_recover(Config, merge_reply_step2).

half_leave_and_recover_after_step3(Config) ->
    half_leave_and_recover(Config, merge_reply_step3).

half_leave_and_recover_after_step4(Config) ->
    half_leave_and_recover(Config, merge_reply_step4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generic test for interrupted split before recover
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

half_join_and_recover(Config, MsgTag) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    %% write data
    _ = [kv_on_cseq:write(integer_to_list(X),X) || X <- lists:seq(1, 100)],
    %% hook into split-protocol
    [gen_component:bp_set_cond(Pid, block(self(), MsgTag),
                               block)
     || Pid <- pid_groups:find_all(dht_node)],
    %% add node
    _ = api_vm:add_nodes(1),
    %% wait for break point
    receive
        {dropped, MsgTag} -> ok
    end,
    %% stop ring
    unittest_helper:stop_ring(),
    %% wait for leases to expire
    timer:sleep(11000),
    %% %% recover
    unittest_helper:make_ring_recover( [{config, [{log_path, PrivDir},
                                                  {leases, true},
                                                  {db_backend, db_mnesia},
                                                  {start_type, recover}]}]),
    lease_checker2:wait_for_clean_leases(500, [{ring_size, config:read(replication_factor)}]),
    io:format("admin:check_ring(): ~p~n", [admin:check_ring()]),
    io:format("admin:check_ring_deep(): ~p~n", [admin:check_ring_deep()]),
    lease_checker2:get_kv_db(),
    io:format("api_vm:number_of_nodes: ~p~n", [api_vm:number_of_nodes()]),
    io:format("pid_groups:find_all(dht_node): ~p~n", [pid_groups:find_all(dht_node)]),
    io:format("pid_groups:find_all(routing_table): ~p~n", [pid_groups:find_all(routing_table)]),
    %% ring restored -> checking KV data integrity
    _ = check_data_integrity(),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generic test for interrupted merge before recover
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

half_leave_and_recover(Config, MsgTag) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    %% write data
    _ = [kv_on_cseq:write(integer_to_list(X),X) || X <- lists:seq(1, 100)],
    %% check ring
    lease_checker2:wait_for_clean_leases(500, [{ring_size, config:read(replication_factor)}]),
    %% hook into merge-protocol
    [gen_component:bp_set_cond(Pid, block(self(), MsgTag),
                               block)
     || Pid <- pid_groups:find_all(dht_node)],
    %% kill node
    SaveNode = lease_checker:get_random_save_node(),
    case SaveNode of
        failed ->
            true;
        _ ->
            RandomNode = comm:make_local(SaveNode),
            PidGroup = pid_groups:group_of(RandomNode),
            PidGroupTabs = [Table || Table <- db_mnesia:get_persisted_tables(),
                                     element(2, db_util:parse_table_name(Table)) =:= PidGroup],
            ct:pal("kill node"),
            {[PidGroup], _Not_found} = admin:del_nodes_by_name([PidGroup], false),
            %% wait for break point
            receive
                {dropped, MsgTag} -> ok
            end,
            %% stop ring
            unittest_helper:stop_ring(),
            %% wait for leases to expire
            timer:sleep(11000),
            %% remove database files
            _ = [?ASSERT(db_mnesia:close_and_delete(db_mnesia:open(X))) || X <- PidGroupTabs],
            %% recover
            unittest_helper:make_ring_recover( [{config, [{log_path, PrivDir},
                                                          {leases, true},
                                                          {db_backend, db_mnesia},
                                                          {start_type, recover}]}]),
            lease_checker2:wait_for_clean_leases(500, [{ring_size, config:read(replication_factor)-1}]),
            io:format("admin:check_ring(): ~p~n", [admin:check_ring()]),
            io:format("admin:check_ring_deep(): ~p~n", [admin:check_ring_deep()]),
            lease_checker2:get_kv_db(),
            io:format("api_vm:number_of_nodes: ~p~n", [api_vm:number_of_nodes()]),
            io:format("pid_groups:find_all(dht_node): ~p~n", [pid_groups:find_all(dht_node)]),
            io:format("pid_groups:find_all(routing_table): ~p~n", [pid_groups:find_all(routing_table)]),
            %% ring restored -> checking KV data integrity
            _ = check_data_integrity(),
            true
    end.

block(Owner, MsgTag) ->
    %% blocking qwrite_done and qwrite_deny
    fun (Message, _State) ->
            case {element(1, Message), element(2, Message)} of
                {l_on_cseq, MsgTag} ->
                    comm:send_local(Owner, {dropped, MsgTag}),
                    drop_single;
                _ ->
                    false
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_data_integrity() ->
    %% lease_checker2:get_kv_db(),
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
