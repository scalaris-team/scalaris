%% @copyright 2012 Zuse Institute Berlin

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
%% @version $Id$
-module(l_on_cseq_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [
            %test_renew_with_concurrent_renew,
            %test_renew_with_concurrent_owner_change,
            %test_renew_with_concurrent_range_change,
            %test_renew_with_concurrent_aux_change_invalid_split,
            %test_renew_with_concurrent_aux_change_valid_split,
            %test_renew_with_concurrent_aux_change_invalid_merge,
            %test_renew_with_concurrent_aux_change_invalid_merge_stopped,
            %test_renew_with_concurrent_aux_change_valid_merge,
            test_split
           ].
suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(1, [{config, [{log_path, PrivDir},
                                                    {leases, true}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

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
    WaitF = fun wait_for_simple_update/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_owner_change(_Config) ->
    ModifyF =
        fun(Old) ->
                l_on_cseq:set_owner(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
                  comm:this())
        end,
    WaitF = fun wait_for_delete/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_range_change(_Config) ->
    ModifyF =
        fun(Old) ->
                l_on_cseq:set_range(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
                  other_range)
        end,
    WaitF = fun wait_for_simple_update/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_invalid_split(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {invalid, split, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
                  Aux)
        end,
    WaitF = fun wait_for_simple_update/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_valid_split(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {valid, split, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
                  Aux)
        end,
    WaitF = fun wait_for_simple_update/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_invalid_merge(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {invalid, merge, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
                  Aux)
        end,
    WaitF = fun wait_for_simple_update/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_invalid_merge_stopped(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {invalid, merge, stopped},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
                  Aux)
        end,
    WaitF = fun wait_for_delete/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

test_renew_with_concurrent_aux_change_valid_merge(_Config) ->
    ModifyF =
        fun(Old) ->
                Aux = {valid, merge, r1, r2},
                l_on_cseq:set_aux(
                  l_on_cseq:set_timeout(
                    l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
                  Aux)
        end,
    WaitF = fun wait_for_simple_update/3,
    test_renew_helper(_Config, ModifyF, WaitF),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% split unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_split(_Config) ->
    NullF = fun (_Id, _Lease) -> ok end,
    WaitLeftLeaseF = fun (Id, Lease) ->
                              OldEpoch   = l_on_cseq:get_epoch(Lease),
                              wait_for_lease_version(Id, OldEpoch+2, 0)
                     end,
    WaitRightLeaseF = fun (Id) ->
                             wait_for_lease_version(Id, 2, 0)
                      end,
    test_split_helper_for_4_steps(_Config,
                                  NullF, NullF,NullF, NullF,
                                  WaitLeftLeaseF, WaitRightLeaseF),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% split helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_split_helper_for_4_steps(_Config,
                              ModifyBeforeStep1, ModifyBeforeStep2,
                              ModifyBeforeStep3, ModifyBeforeStep4,
                              WaitLeftLease, WaitRightLease) ->
    % intercept lease renew
    %{l_on_cseq, renew, Old} = intercept_lease_renew(),
    %Id = l_on_cseq:get_id(Old),
    % prepeare split
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_with(dht_node)),
    comm:send_local(DHTNode, {get_state, comm:this(), lease_list}),
    L = receive
                 {get_state_response, List} ->
                     hd(List)
             end,
    Range = l_on_cseq:get_range(L),
    [R1, R2] = intervals:split(Range, 2),
    LeftId = id(R1),
    RightId = id(R2),
    %ct:pal("~p", [[R1, R2]]),
    %ct:pal("~p", [[LeftId, RightId, Id]]),
    intercept_split_request(),                                   % install intercepts
    intercept_split_reply(split_reply_step1),                    %
    intercept_split_reply(split_reply_step2),                    %
    intercept_split_reply(split_reply_step3),                    %

    %ct:pal("start with step1"),
    %
    % step1
    %
    l_on_cseq:lease_split(L, R1, R2),                            % trigger step
    StartMsg = receive                                           % intercept msg
                   M = {l_on_cseq, split, _, _, _} ->
                       M
               end,
    {l_on_cseq, split, Lease, _R1, _R2} = StartMsg,
    ModifyBeforeStep1(RightId, Lease),                           % modify world
    gen_component:bp_del(DHTNode, block_split_request),
    comm:send_local(DHTNode, StartMsg),                          % release msg
    % step2
    split_helper_do_step(split_reply_step1, ModifyBeforeStep2, LeftId),
    % step3
    split_helper_do_step(split_reply_step2, ModifyBeforeStep3, RightId),
    % step4
    split_helper_do_step(split_reply_step3, ModifyBeforeStep4, LeftId),
    % wait for result
    %ct:pal("wait left"),
    WaitLeftLease(LeftId, L),
    %ct:pal("wait right"),
    WaitRightLease(RightId).

split_helper_do_step(StepTag, ModifyBeforeStep, Id) ->
    %ct:pal("doing ~p", [StepTag]),
    DHTNode = pid_groups:find_a(dht_node),
    ReplyMsg = receive
                   M = {l_on_cseq, StepTag, Lease, _R1, _R2, _Resp} ->
                       M
               end,
    ModifyBeforeStep(Id, Lease),
    gen_component:bp_del(DHTNode, StepTag),
    comm:send_local(DHTNode, ReplyMsg).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% renew helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_renew_helper(_Config, ModifyF, WaitF) ->
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_with(dht_node)),

    % intercept lease renew
    M = {l_on_cseq, renew, Old} = intercept_lease_renew(),
    Id = l_on_cseq:get_id(Old),
    % now we update the lease
    New = ModifyF(Old),
    l_on_cseq:lease_update(Old, New),
    wait_for_lease(New),
    % now the error handling of lease_renew is going to be tested
    ct:pal("sending message ~p~n", [M]),
    comm:send_local(DHTNode, M),
    WaitF(Id, Old, New),
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
    wait_for_lease_helper(Id,
                          fun (Lease) ->
                                  %ct:pal("want ~p:~p; have ~p:~p", [Epoch, Version, l_on_cseq:get_epoch(Lease), l_on_cseq:get_version(Lease)]),
                                  Epoch   == l_on_cseq:get_epoch(Lease)
                          andalso Version == l_on_cseq:get_version(Lease)
                          end).

wait_for_lease_helper(Id, F) ->
    wait_for(fun () ->
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

wait_for_simple_update(Id, Old, _New) ->
    OldVersion = l_on_cseq:get_version(Old),
    OldEpoch   = l_on_cseq:get_epoch(Old),
    wait_for_lease_version(Id, OldEpoch, OldVersion+2).

wait_for_delete(_Id, _Old, _New) ->
    DHTNode = pid_groups:find_a(dht_node),
    wait_for(fun () ->
                     L = get_dht_node_state(DHTNode, lease_list),
                     L == []
             end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intercepting and blocking
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

intercept_split_request() ->
    DHTNode = pid_groups:find_a(dht_node),
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_split_request(self()), block_split_request).

intercept_split_reply(StepTag) ->
    DHTNode = pid_groups:find_a(dht_node),
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_split_reply(self(), StepTag), StepTag).

intercept_lease_renew() ->
    DHTNode = pid_groups:find_a(dht_node),
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_renew(self()), block_renew),
    Msg = receive
              M = {l_on_cseq, renew, _Lease} ->
                  M
          end,
    ct:pal("intercepted renew request ~p~n", [Msg]),
    gen_component:bp_set_cond(DHTNode, block_trigger(self()), block_trigger),
    gen_component:bp_del(DHTNode, block_renew),
    Msg.

block_split_request(Pid) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, split, _Lease, _R1, _R2} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

block_split_reply(Pid, StepTag) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, StepTag, _Lease, _R1, _R2, _Resp} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

block_renew(Pid) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, renew, _Lease} ->
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

- spec id(intervals:interval()) -> non_neg_integer().
id([]) -> 0;
id([[]]) -> 0;
id(X) ->
    case lists:member(all, X) of
        true -> 0;
        _ ->
            {_, Id, _, _} = intervals:get_bounds(X),
            Id
    end.
