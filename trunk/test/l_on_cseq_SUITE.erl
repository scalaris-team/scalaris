%% @copyright 2012-2013 Zuse Institute Berlin

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
%% @doc    Unit tests for l_on_cseq
%% @end
%% @version $Id$
-module(l_on_cseq_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{tester_tests, [sequence], [
                              tester_type_check_l_on_cseq
                              ]},
     {renew_tests, [sequence], [
                             test_renew_with_concurrent_renew,
                             test_renew_with_concurrent_owner_change,
                             test_renew_with_concurrent_range_change,
                             test_renew_with_concurrent_aux_change_invalid_split,
                             test_renew_with_concurrent_aux_change_valid_split,
                             test_renew_with_concurrent_aux_change_invalid_merge,
                             test_renew_with_concurrent_aux_change_invalid_merge_stopped,
                             test_renew_with_concurrent_aux_change_valid_merge
                             ]},
     {split_tests, [sequence], [
                             test_split,
                             test_split_with_concurrent_renew,
                             test_split_but_lease_already_exists,
                             test_split_with_owner_change_in_step1,
                             test_split_with_owner_change_in_step2,
                             test_split_with_owner_change_in_step3,
                             test_split_with_aux_change_in_step1
                            ]},
     {merge_tests, [sequence], [
                               ]}, % @todo
     {takeover_tests, [sequence], [
                                   test_takeover
                               ]},
     {handover_tests, [sequence], [
                                test_handover,
                                test_handover_with_concurrent_renew,
                                test_handover_with_concurrent_aux_change,
                                test_handover_with_concurrent_owner_change
                                ]}
    ].

all() ->
    [
     {group, tester_tests},
     {group, renew_tests},
     {group, split_tests},
     {group, handover_tests},
     {group, takeover_tests}
     ].

suite() -> [ {timetrap, {seconds, 60}} ].

group(tester_tests) ->
    [{timetrap, {seconds, 400}}];
group(renew_tests) ->
    [{timetrap, {seconds, 10}}];
group(split_tests) ->
    [{timetrap, {seconds, 20}}];
group(takeover_tests) ->
    [{timetrap, {seconds, 30}}];
group(handover_tests) ->
    [{timetrap, {seconds, 10}}].


init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(TestCase, Config) ->
    case TestCase of
        test_garbage_collector ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir},
                                                    {leases, true}]}]),
            unittest_helper:check_ring_size_fully_joined(4),
            Config;
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir},
                                                    {leases, true}]}]),
            unittest_helper:check_ring_size_fully_joined(4),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

tester_type_check_l_on_cseq(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {l_on_cseq,
           [ {add_first_lease_to_db, 2}, %% cannot create DB refs for State
             {lease_renew, 2}, %% sends messages
             {lease_handover, 3}, %% sends messages
             {lease_takeover, 2}, %% sends messages
             {lease_split, 5}, %% sends messages
             {lease_merge, 3}, %% sends messages
             {lease_send_lease_to_node, 2}, %% sends messages
             {lease_split_and_change_owner, 6}, %% sends messages
             {id, 1}, %% todo
             {split_range, 1}, %% todo
             {unittest_lease_update, 3}, %% only for unittests
             {disable_lease, 2}, %% requires dht_node_state
             {on, 2}, %% cannot create dht_node_state (reference for bulkowner)
             {get_pretty_timeout, 1}, %% cannot create valid timestamps
             {read, 2} %% cannot create pids
           ],
           [
             {get_active_lease, 1}, %% cannot create reference (bulkowner uses one in dht_node_state
             {format_utc_timestamp, 1} %% cannot create valid timestamps
           ]},
          {lease_list,
           [
             {update_lease_in_dht_node_state, 4}, %% cannot create dht_node_state (reference for bulkowner)
             {remove_lease_from_dht_node_state, 3} %% cannot create dht_node_state (reference for bulkowner)
           ],
           [
            {update_lease_in_dht_node_state, 3}, %% cannot create dht_node_state (reference for bulkowner)
            {update_active_lease, 2}, %% assert fails for random input
            {remove_passive_lease_from_dht_node_state, 2}, %% cannot create dht_node_state (reference for bulkowner)
            {remove_active_lease_from_dht_node_state, 2} %% cannot create dht_node_state (reference for bulkowner)
           ]},
          {leases,
           [
            {is_responsible, 2} %% cannot create dht_node_state (reference for bulkowner)
           ],
           [
           ]
          }
        ],
    %% join a dht_node group to be able to call lease trigger functions
    pid_groups:join(pid_groups:group_with(dht_node)),
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% renew unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_renew_with_concurrent_renew(_Config) ->
    ModifyF =
        fun(Old) ->
                l_on_cseq:set_timeout(
                  l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1))
        end,
    WaitF = fun wait_for_simple_update/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_owner_change(_Config) ->
    ModifyF =
        fun(Old) ->
                l_on_cseq:set_owner(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(
                      l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                      0)),
                  comm:this())
        end,
    WaitF = fun wait_for_delete/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_range_change(_Config) ->
    ModifyF =
        fun(Old) ->
                l_on_cseq:set_range(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(
                      l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                      0)),
                  obfuscated_intervals_all())
        end,
    WaitF = fun wait_for_epoch_update/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_invalid_split(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {invalid, split, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(
                      l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                    0)),
                  Aux)
        end,
    WaitF = fun wait_for_epoch_update/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_valid_split(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {valid, split, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(
                      l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                    0)),
                  Aux)
        end,
    WaitF = fun wait_for_epoch_update/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_invalid_merge(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {invalid, merge, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(
                      l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                    0)),
                  Aux)
        end,
    WaitF = fun wait_for_epoch_update/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_invalid_merge_stopped(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {invalid, merge, stopped},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(
                      l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                    0)),
                  Aux)
        end,
    WaitF = fun wait_for_delete/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_valid_merge(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {valid, merge, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(
                      l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                    0)),
                  Aux)
        end,
    WaitF = fun wait_for_epoch_update/2,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% split unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_split(_Config) ->
    NullF = fun (_Id, _Lease) -> ok end,
    WaitRightLeaseF = fun (Id, Lease) ->
                             OldEpoch   = l_on_cseq:get_epoch(Lease),
                             wait_for_lease_version(Id, OldEpoch+2, 0)
                     end,
    WaitLeftLeaseF = fun (Id) ->
                             wait_for_lease_version(Id, 2, 0)
                      end,
    FinalWaitF = fun wait_for_split_success_msg/0,
    test_split_helper_for_4_steps(_Config,
                                  NullF, NullF,NullF, NullF,
                                  WaitLeftLeaseF, WaitRightLeaseF, FinalWaitF),
    true.

test_split_with_concurrent_renew(_Config) ->
    NullF = fun (_Id, _Lease) -> ok end,
    RenewLeaseLeftF = fun (_Id, Lease) ->
                              log:log("left renew lease with ~w ~w", [_Id, Lease]),
                              l_on_cseq:lease_renew(Lease, passive),
                              wait_for_lease_version(l_on_cseq:get_id(Lease),
                                                     l_on_cseq:get_epoch(Lease),
                                                     l_on_cseq:get_version(Lease)+1)
                  end,
    RenewLeaseRightF = fun (_Id, Lease) ->
                               log:log("right renew lease with ~w ~w", [_Id, Lease]),
                               l_on_cseq:lease_renew(Lease, active),
                               wait_for_lease_version(l_on_cseq:get_id(Lease),
                                                      l_on_cseq:get_epoch(Lease),
                                                      l_on_cseq:get_version(Lease)+1)
                  end,
    WaitRightLeaseF = fun (Id, Lease) ->
                              OldEpoch   = l_on_cseq:get_epoch(Lease),
                              wait_for_lease_version(Id, OldEpoch+2, 0)
                     end,
    WaitLeftLeaseF = fun (Id) ->
                             wait_for_lease_version(Id, 2, 0)
                      end,
    FinalWaitF = fun wait_for_split_success_msg/0,
    test_split_helper_for_4_steps(_Config,
                                  NullF, NullF, RenewLeaseLeftF, RenewLeaseRightF,
                                  WaitLeftLeaseF, WaitRightLeaseF, FinalWaitF),
    true.

test_split_but_lease_already_exists(_Config) ->
    ContentCheck =
        fun (Current, _WriteFilter, _Next) ->
                case Current == prbr_bottom of
                    true ->
                        {true, null};
                    false ->
                        {false, lease_already_exists}
                end
        end,
    CreateLeaseF =
        fun(LeftId) ->
                New = l_on_cseq:set_version(
                        l_on_cseq:set_epoch(
                          l_on_cseq:unittest_create_lease(LeftId),
                          47),
                        11),
                DB = l_on_cseq:get_db_for_id(LeftId),
                rbrcseq:qwrite(DB, self(), LeftId,
                               ContentCheck,
                               New),
                receive
                    {qwrite_done, _ReqId, _Round, _} -> ok;
                    X -> ct:pal("wrong message ~p", [X]),
                          timer:sleep(4000)
                end
        end,
    WaitRightLeaseF = fun (Id, Lease) ->
                             OldEpoch = l_on_cseq:get_epoch(Lease),
                             OldVersion = l_on_cseq:get_version(Lease),
                             wait_for_lease_version(Id, OldEpoch, OldVersion)
                     end,
    WaitLeftLeaseF = fun (Id) ->
                             wait_for_lease_version(Id, 47, 11)
                      end,
    FinalWaitF = fun wait_for_split_fail_msg/0,
    test_split_helper_for_1_step(_Config,
                                 CreateLeaseF,
                                 WaitLeftLeaseF, WaitRightLeaseF, FinalWaitF),
    true.

test_split_with_owner_change_in_step1(_Config) ->
    ChangeOwnerF =
        fun (Id, Lease) ->
                ct:pal("changing owner: ~p ~p", [Id, Lease]),
                New = l_on_cseq:set_owner(
                        l_on_cseq:set_timeout(
                          l_on_cseq:set_version(
                            l_on_cseq:set_epoch(Lease, l_on_cseq:get_epoch(Lease)+1),
                            0)),
                        comm:this()),
                l_on_cseq:unittest_lease_update(Lease, New, active)
        end,
    NullF = fun (_Id, _Lease) -> ok end,
    WaitRightLeaseF = fun wait_for_delete/2,
    WaitLeftLeaseF = fun (_Id) -> ok end,
                            % we cannot read the left lease anymore, because
                            % consistent routing will prevent the delivery of
                            % messages

    %fun (Id) ->
    %        wait_for_lease_version(Id, 1, 0)
    %end,
    FinalWaitF = fun wait_for_split_fail_msg/0,
    test_split_helper_for_2_steps(_Config,
                                  NullF, ChangeOwnerF,
                                  WaitLeftLeaseF, WaitRightLeaseF, FinalWaitF),
    true.

test_split_with_owner_change_in_step2(Config) ->
    ChangeOwnerF =
        fun (Id, Lease) ->
                ct:pal("changing owner: ~p ~p", [Id, Lease]),
                New = l_on_cseq:set_owner(
                        l_on_cseq:set_timeout(
                          l_on_cseq:set_version(
                            l_on_cseq:set_epoch(Lease, l_on_cseq:get_epoch(Lease)+1),
                            0)),
                        comm:this()),
                l_on_cseq:unittest_lease_update(Lease, New, passive)
        end,
    NullF = fun (_Id, _Lease) -> ok end,
    WaitRightLeaseF = fun (_Id, _Lease) -> ok end,
                            % we cannot read the left lease anymore, because
                            % consistent routing will prevent the delivery of
                            % messages
    %fun (Id, Lease) ->
    %        OldEpoch = l_on_cseq:get_epoch(Lease),
    %        wait_for_lease_version(Id, OldEpoch + 1, 0)
    %end,
    WaitLeftLeaseF = fun wait_for_delete/1,
    FinalWaitF = fun wait_for_split_fail_msg/0,
    test_split_helper_for_3_steps(Config,
                                  NullF, NullF, ChangeOwnerF,
                                  WaitLeftLeaseF, WaitRightLeaseF, FinalWaitF),
    true.

test_split_with_owner_change_in_step3(Config) ->
    ChangeOwnerF =
        fun (Id, Lease) ->
                ct:pal("changing owner: ~p ~p", [Id, Lease]),
                New = l_on_cseq:set_owner(
                        l_on_cseq:set_timeout(
                          l_on_cseq:set_version(
                            l_on_cseq:set_epoch(Lease, l_on_cseq:get_epoch(Lease)+1),
                            0)),
                        comm:this()),
                l_on_cseq:unittest_lease_update(Lease, New, active)
        end,
    NullF = fun (_Id, _Lease) -> ok end,
    WaitLeftLeaseF = fun (_Id) -> ok end,
                            % we cannot read the left lease anymore, because
                            % consistent routing will prevent the delivery of
                            % messages
    %fun (Id) ->
    %        wait_for_lease_version(Id, 2, 0)
    %end,
    WaitRightLeaseF = fun wait_for_delete/2,
    FinalWaitF = fun wait_for_split_fail_msg/0,
    test_split_helper_for_4_steps(Config,
                                  NullF, NullF, NullF, ChangeOwnerF,
                                  WaitLeftLeaseF, WaitRightLeaseF, FinalWaitF),
    true.

test_split_with_aux_change_in_step1(_Config) ->
    ChangeOwnerF =
        fun (Id, Lease) ->
                ct:pal("changing aux: ~p ~p", [Id, Lease]),
                New = l_on_cseq:set_aux(
                        l_on_cseq:set_timeout(
                          l_on_cseq:set_version(
                            l_on_cseq:set_epoch(Lease, l_on_cseq:get_epoch(Lease)+1),
                            0)),
                        {invalid, merge, stopped}),
                l_on_cseq:unittest_lease_update(Lease, New, passive)
        end,
    NullF = fun (_Id, _Lease) -> ok end,
    WaitRightLeaseF = fun (Id, Lease) ->
                             wait_for_lease_version(Id, l_on_cseq:get_epoch(Lease) + 1, 0)
                      end,
    WaitLeftLeaseF = fun (Id) ->
                             wait_for_lease_version(Id, 1, 0)
                      end,
    FinalWaitF = fun wait_for_split_fail_msg/0,
    test_split_helper_for_2_steps(_Config,
                                  NullF, ChangeOwnerF,
                                  WaitLeftLeaseF, WaitRightLeaseF, FinalWaitF),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% handover unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_handover(_Config) ->
    ModifyF = fun(Old) -> Old end,
    WaitF = fun (Id, _Lease) ->
                    wait_for_lease_owner(Id, comm:this()),
                    receive
                        {handover, success, _} -> ok
                    end
            end,
    test_handover_helper(_Config, ModifyF, WaitF),
    true.

test_handover_with_concurrent_renew(_Config) ->
    ModifyF = fun(Old) ->
                      l_on_cseq:set_version(
                        l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                        0)
                      end,
    WaitF = fun (Id, _Lease) ->
                    wait_for_lease_owner(Id, comm:this()),
                    receive
                        {handover, success, _} -> ok
                    end
            end,
    test_handover_helper(_Config, ModifyF, WaitF),
    true.

test_handover_with_concurrent_aux_change(_Config) ->
    ModifyF = fun(Old) ->
                      l_on_cseq:set_aux(
                        l_on_cseq:set_version(
                          l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                          0),
                        {valid, merge, foo, bar})
                      end,
    WaitF = fun (_Id, _Lease) ->
                    receive
                        {handover, failed, _} -> ok
                    end
            end,
    test_handover_helper(_Config, ModifyF, WaitF),
    true.

test_handover_with_concurrent_owner_change(_Config) ->
    ModifyF = fun(Old) ->
                      l_on_cseq:set_owner(
                        l_on_cseq:set_version(
                          l_on_cseq:set_epoch(Old, l_on_cseq:get_epoch(Old)+1),
                          0),
                        comm:this())
                      end,
    WaitF = fun (_Id, _Lease) ->
                    receive
                        {handover, failed, _} -> ok
                    end
            end,
    test_handover_helper(_Config, ModifyF, WaitF),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% takeover unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_takeover(_Config) ->
    log:log("start test_takeover"),
    ModifyF = fun(Old) -> Old end,
    WaitF = fun (Id, _Lease, OriginalOwner) ->
                    ct:pal("takeover: wait_for_lease_owner ~p", [OriginalOwner]),
                    wait_for_lease_owner(Id, OriginalOwner),
                    ct:pal("takeover: wait_for_lease_owner done")
            end,
    test_takeover_helper(_Config, ModifyF, WaitF),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% takeover helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_takeover_helper(_Config, ModifyF, WaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_of(DHTNode)),

    % intercept lease renew
    {l_on_cseq, renew, Old, _Mode} = intercept_lease_renew(DHTNode),
    %OriginalOwner = l_on_cseq:get_owner(Old),
    ct:pal("takeover: old lease ~p", [Old]),
    Id = l_on_cseq:get_id(Old),
    % now we change the owner of the lease
    l_on_cseq:lease_handover(Old, comm:this(), self()),
    ct:pal("new owner ~p", [comm:this()]),
    HandoverWaitF = fun (_Id, _Lease) ->
                            wait_for_lease_owner(_Id, comm:this()),
                            receive
                                {handover, success, _} -> ok
                            end
                    end,
    HandoverWaitF(Id, Old),
    ct:pal("takeover: now we update the lease"),
    % now we update the lease
    {ok, Current} = l_on_cseq:read(Id),
    ct:pal("takeover: current lease: ~p", [Current]),
    New = ModifyF(Current),
    case New =/= Current of
        true ->
            Res = l_on_cseq:unittest_lease_update(Current, New, active),
            ct:pal("takeover: lease_update: ~p (~p -> ~p)", [Res, Current, New]),
            wait_for_lease(New);
        false ->
            ok
    end,
    ct:pal("takeover: takeover"),
    % now the error handling of lease_takeover is going to be tested
    takeover_loop(Current),
    ct:pal("takeover: wait_for_lease2"),
    WaitF(Id, Current, comm:make_global(DHTNode)),
    ct:pal("takeover: done"),
    true.

takeover_loop(L) ->
    l_on_cseq:lease_takeover(L, self()),
    M = receive
            {takeover, _ , _} = _M -> _M;
            {takeover, _ , _, _} = _M -> _M
        end,
    case M of
        {takeover, success, L2} ->
            ct:pal("takeover succeed ~w", [L2]),
            ok;
        {takeover, failed, L2, Result} ->
            ct:pal("retrying takeover ~p ~p", [L2, l_on_cseq:get_pretty_timeout(L2)]),
            %% we repeat until the lease expired and then hopefully succeed
            timer:sleep(500),
            takeover_loop(L2)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% handover helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_handover_helper(_Config, ModifyF, WaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_of(DHTNode)),

    % intercept lease renew
    {l_on_cseq, renew, Old, _Mode} = intercept_lease_renew(DHTNode),
    Id = l_on_cseq:get_id(Old),
    % now we update the lease
    New = ModifyF(Old),
    l_on_cseq:unittest_lease_update(Old, New, active),
    wait_for_lease(New),
    % now the error handling of lease_handover is going to be tested
    l_on_cseq:lease_handover(Old, comm:this(), self()),
    WaitF(Id, Old),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% split helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_split_prepare(DHTNode) ->
    % intercept lease renew
    {l_on_cseq, renew, _Old, _Mode} = intercept_lease_renew(DHTNode),
    % prepeare split
    comm:send_local(DHTNode, {get_state, comm:this(), lease_list}),
    L = receive
            {get_state_response, LeaseList} ->
                lease_list:get_active_lease(LeaseList)
             end,
    {ok, R1, R2} = l_on_cseq:split_range(l_on_cseq:get_range(L)),
    log:log("split under test:~n~w~n~w~n~w~n", [l_on_cseq:get_range(L), R1, R2]),
    %[R1, R2] = intervals:split(l_on_cseq:get_range(L), 2),
    LeftId = l_on_cseq:id(R1),
    RightId = l_on_cseq:id(R2),
    intercept_split_request(DHTNode),                                   % install intercepts
    intercept_split_reply(DHTNode, split_reply_step1),                    %
    intercept_split_reply(DHTNode, split_reply_step2),                    %
    intercept_split_reply(DHTNode, split_reply_step3),                    %
    intercept_split_reply(DHTNode, split_reply_step4),                    %

    % step1
    log:log("starting the split under test"),
    l_on_cseq:lease_split(L, R1, R2, second, self()),            % trigger step
    ct:pal("intercepting msg"),
    StartMsg = receive                                           % intercept msg
                   M = {l_on_cseq, split, _Lease, _R1, _R2, _Keep,
                        _ReplyTo, _PostAux} ->
                       M
               end,
    ct:pal("intercepted msg"),
    {l_on_cseq, split, Lease, _R1, _R2, _Keep, _ReplyTo, _PostAux} = StartMsg,
    {Lease, LeftId, RightId, StartMsg}.


test_split_helper_for_1_step(_Config,
                             ModifyBeforeStep1,
                             WaitLeftLease, WaitRightLease, FinalWaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_of(DHTNode)),
    {Lease, LeftId, RightId, StartMsg} = test_split_prepare(DHTNode),
    ModifyBeforeStep1(LeftId),                           % modify world
    gen_component:bp_del(DHTNode, block_split_request),
    gen_component:bp_del(DHTNode, split_reply_step1),
    comm:send_local(DHTNode, StartMsg),                          % release msg
    % wait for result
    ct:pal("wait left"),
    WaitLeftLease(LeftId),
    ct:pal("wait right"),
    WaitRightLease(RightId, Lease),
    FinalWaitF().

test_split_helper_for_2_steps(_Config,
                              ModifyBeforeStep1,
                              ModifyBeforeStep2,
                              WaitLeftLease, WaitRightLease, FinalWaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_of(DHTNode)),
    {Lease, LeftId, RightId, StartMsg} = test_split_prepare(DHTNode),
    ct:pal("0"),
    ModifyBeforeStep1(LeftId, Lease),                           % modify world
    gen_component:bp_del(DHTNode, block_split_request),
    comm:send_local(DHTNode, StartMsg),                          % release msg
    % step 2
    split_helper_do_step(DHTNode, split_reply_step1, ModifyBeforeStep2, RightId),
    wait_for_split_message(DHTNode, split_reply_step2),
    % wait for result
    ct:pal("wait left"),
    WaitLeftLease(LeftId),
    ct:pal("wait right"),
    WaitRightLease(RightId, Lease),
    FinalWaitF().

test_split_helper_for_3_steps(_Config,
                              ModifyBeforeStep1,
                              ModifyBeforeStep2,
                              ModifyBeforeStep3,
                              WaitLeftLease, WaitRightLease, FinalWaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_of(DHTNode)),
    {Lease, LeftId, RightId, StartMsg} = test_split_prepare(DHTNode),
    ModifyBeforeStep1(RightId, Lease),                           % modify world
    gen_component:bp_del(DHTNode, block_split_request),
    comm:send_local(DHTNode, StartMsg),                          % release msg
    % step 2
    split_helper_do_step(DHTNode, split_reply_step1, ModifyBeforeStep2, LeftId),
    % step 3
    split_helper_do_step(DHTNode, split_reply_step2, ModifyBeforeStep3, LeftId),
    wait_for_split_message(DHTNode, split_reply_step3),
    % wait for result
    log:pal("wait left"),
    WaitLeftLease(LeftId),
    log:pal("wait right"),
    WaitRightLease(RightId, Lease),
    FinalWaitF().

test_split_helper_for_4_steps(_Config,
                              ModifyBeforeStep1,
                              ModifyBeforeStep2,
                              ModifyBeforeStep3,
                              ModifyBeforeStep4,
                              WaitLeftLease, WaitRightLease, FinalWaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_of(DHTNode)),
    {Lease, LeftId, RightId, StartMsg} = test_split_prepare(DHTNode),
    log:log("left and right-id:~w~n~w~n", [LeftId, RightId]),
    ModifyBeforeStep1(RightId, Lease),                           % modify world
    gen_component:bp_del(DHTNode, block_split_request),
    comm:send_local(DHTNode, StartMsg),                          % release msg
    % step2
    split_helper_do_step(DHTNode, split_reply_step1, ModifyBeforeStep2, LeftId),
    log:log("finished step2"),
    % step3
    split_helper_do_step(DHTNode, split_reply_step2, ModifyBeforeStep3, RightId),
    log:log("finished step3"),
    % step4
    split_helper_do_step(DHTNode, split_reply_step3, ModifyBeforeStep4, LeftId),
    log:log("finished step4"),
    wait_for_split_message(DHTNode, split_reply_step4),
    log:log("got split message"),
    % wait for result
    ct:pal("wait left"),
    WaitLeftLease(LeftId),
    ct:pal("wait right"),
    WaitRightLease(RightId, Lease),
    FinalWaitF().

split_helper_do_step(DHTNode, StepTag, ModifyBeforeStep, Id) ->
    log:pal("doing ~p", [StepTag]),
    ReplyMsg = receive
                   M = {l_on_cseq, StepTag, Lease, _R1, _R2, _Keep, _ReplyTo, _PostAux, _Resp} ->
                       M
               end,
    ModifyBeforeStep(Id, Lease),
    gen_component:bp_del(DHTNode, StepTag),
    watch_message(DHTNode, ReplyMsg).

wait_for_split_message(DHTNode, StepTag) ->
    log:pal("waiting for ~p", [StepTag]),
    receive
        M = {l_on_cseq, StepTag, _Lease, _R1, _R2, _Keep, _ReplyTo, _PostAux, _Resp} ->
            %log:pal("got ~p", [M]),
            gen_component:bp_del(DHTNode, StepTag),
            watch_message(DHTNode, M)
    end.

wait_for_split_success_msg() ->
    log:pal("wait_for_split_success_msg() ~p", [self()]),
    receive
        {split, success, _, _} ->
            ok
    end.

wait_for_split_fail_msg() ->
    receive
        {split, fail, _} ->
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% renew helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_renew_helper(_Config, ModifyF, WaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_with(dht_node)),

    % intercept lease renew
    M = {l_on_cseq, renew, Old, _Mode} = intercept_lease_renew(DHTNode),
    Id = l_on_cseq:get_id(Old),
    % now we update the lease
    New = ModifyF(Old),
    l_on_cseq:unittest_lease_update(Old, New, active),
    wait_for_lease(New),
    % now the error handling of lease_renew is going to be tested
    comm:send_local(DHTNode, M),
    WaitF(Id, Old),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% wait helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wait_for(F) ->
    case F() of
        true ->
            ok;
        false ->
            wait_for(F)
    end.

wait_for_lease(Lease) ->
    Id = l_on_cseq:get_id(Lease),
    wait_for_lease_helper(Id, fun (L) -> L == Lease end).

wait_for_lease_version(Id, Epoch, Version) ->
    ct:pal("wait_for_lease_version ~p", [Id]),
    wait_for_lease_helper(Id,
                          fun (Lease) ->
                                  ct:pal("want ~p:~p; have ~p:~p", [Epoch, Version, l_on_cseq:get_epoch(Lease), l_on_cseq:get_version(Lease)]),
                                  Epoch   == l_on_cseq:get_epoch(Lease)
                          andalso Version == l_on_cseq:get_version(Lease)
                          end).

wait_for_lease_owner(Id, NewOwner) ->
    wait_for_lease_helper(Id,
                          fun (Lease) ->
                                  NewOwner   == l_on_cseq:get_owner(Lease)
                          end).

wait_for_lease_helper(Id, F) ->
    wait_for(fun () ->
                     DHTNode = pid_groups:find_a(dht_node),
                     %comm:send_local(DHTNode, {get_state, comm:this(), lease_list}),
                     %{A, P} = receive
                     %        {get_state_response, {ActiveList, PassiveList}} ->
                     %            {ActiveList, PassiveList}
                     %    end,
                     %ct:pal("~p ~p", [A, P]),
                     case l_on_cseq:read(Id) of
                         {ok, Lease} ->
                             F(Lease);
                         _ ->
                             false
                     end
             end).

get_dht_node_state(Pid, What) ->
    comm:send_local(Pid, {get_state, comm:this(), What}),
    receive
        {get_state_response, Data} ->
            Data
    end.

get_all_active_leases() ->
    [ get_active_lease(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ].

get_active_lease(Pid) ->
    LeaseList = get_dht_node_state(Pid, lease_list),
    lease_list:get_active_lease(LeaseList).

wait_for_simple_update(Id, Old) ->
    OldVersion = l_on_cseq:get_version(Old),
    OldEpoch   = l_on_cseq:get_epoch(Old),
    wait_for_lease_version(Id, OldEpoch, OldVersion+2).

wait_for_epoch_update(Id, Old) ->
    OldEpoch   = l_on_cseq:get_epoch(Old),
    wait_for_lease_version(Id, OldEpoch+1, 1).

wait_for_delete(Id, _Old) ->
    DHTNode = pid_groups:find_a(dht_node),
    ct:pal("wait_for_delete ~p", [Id]),
    wait_for(fun () ->
                     LeaseList = get_dht_node_state(DHTNode, lease_list),
                     L = lease_list:get_active_lease(LeaseList),
                     case L of
                         empty ->
                             true;
                         _ ->
                             l_on_cseq:get_id(L) =/= Id
                     end
             end).

wait_for_delete(Id) ->
    ct:pal("wait_for_delete ~p", [Id]),
    DHTNode = pid_groups:find_a(dht_node),
    wait_for(fun () ->
                     LeaseList = get_dht_node_state(DHTNode, lease_list),
                     L = lease_list:get_active_lease(LeaseList),
                     case L of
                         empty ->
                             true;
                         _ ->
                             l_on_cseq:get_id(L) =/= Id
                     end
             end).

wait_for_number_of_leases(Nr) ->
    wait_for(fun() ->
                     length(get_all_active_leases()) == Nr
             end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intercepting and blocking
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

watch_message(Pid, Message) ->
    gen_component:bp_set_cond(Pid, block_message(self(), Message), watch_message),
    comm:send_local(Pid, Message),
    receive
        {saw_message} ->
            _ = gen_component:bp_step(Pid),
            gen_component:bp_del(Pid, watch_message),
            gen_component:bp_cont(Pid)
    end.

intercept_split_request(DHTNode) ->
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_split_request(self()), block_split_request).

intercept_split_reply(DHTNode, StepTag) ->
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_split_reply(self(), StepTag), StepTag).

intercept_lease_renew(DHTNode) ->
    %DHTNode = pid_groups:find_a(dht_node),
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_renew(self()), block_renew),
    Msg = receive
              M = {l_on_cseq, renew, _Lease, _Mode} ->
                  M
          end,
    gen_component:bp_set_cond(DHTNode, block_trigger(self()), block_trigger),
    gen_component:bp_del(DHTNode, block_renew),
    Msg.

block_message(Pid, WatchedMessage) ->
    fun (Message, _State) ->
            case Message of
                WatchedMessage ->
                    comm:send_local(Pid, {saw_message}),
                    true;
                _ ->
                    false
            end
    end.

block_split_request(Pid) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, split, _Lease, _R1, _R2, _Keep, _ReplyTo, _PostAux} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

block_split_reply(Pid, StepTag) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, StepTag, _Lease, _R1, _R2, _Keep, _ReplyTo, _PostAux, _Resp} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

block_renew(Pid) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, renew, _Lease, _Mode} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

block_trigger(Pid) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, renew_leases} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

obfuscated_intervals_all() ->
    [{interval,'(',0,5,']'},
     {element,0},
     {interval,'(',5,340282366920938463463374607431768211456,')'}
    ].
