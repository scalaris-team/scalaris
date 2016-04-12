%% @copyright 2012-2013, 2016 Zuse Institute Berlin

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
%% @doc    Unit tests for l_on_cseq based on a mockup dht_node
%% @end
%% @version $Id$
-module(mockup_l_on_cseq_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{merge_tests, [sequence], [
                                test_merge,
                                test_merge_with_renewal_before_step1,
                                test_merge_with_renewal_after_step1,
                                test_merge_with_renewal_after_step2,
                                test_merge_with_renewal_after_step3
                               ]},
     {split_tests, [sequence], [
                                test_split
                               ]}
    ].

all() ->
    [
     {group, merge_tests},
     {group, split_tests}
     ].

suite() -> [ {timetrap, {seconds, 90}} ].

group(merge_tests) ->
    [{timetrap, {seconds, 400}}];
group(split_tests) ->
    [{timetrap, {seconds, 400}}];
group(_) ->
    suite().

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) ->
    unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir},
                                                   {leases, true}]}]),
    {ok, _} = mockup_l_on_cseq:start_link(),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% merge unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_merge(_Config) ->
    % join group
    Pid = pid_groups:find_a(mockup_l_on_cseq),
    pid_groups:join(pid_groups:group_of(Pid)),
    % create leases
    {L1, L2} = mockup_l_on_cseq:create_two_adjacent_leases(comm:make_global(Pid)),
    % do merge
    % evil, but l_on_cseq:lease_merge sends to a real dht_node
    comm:send_local(Pid, {l_on_cseq, merge, L1, L2, self()}),
    receive
        {merge, success, _L2, _L1} -> ok
    end,
    % no renews during merge!
    ?equals(mockup_l_on_cseq:get_renewal_counter(), 0),
    % check L1
    ?equals(l_on_cseq:read(l_on_cseq:get_id(L1)), {ok, prbr_bottom}),
    % check L2
    case l_on_cseq:read(l_on_cseq:get_id(L2)) of
        {ok, L2_} ->
            ?equals(l_on_cseq:get_range(L2_),
                    intervals:union(l_on_cseq:get_range(L1),
                                    l_on_cseq:get_range(L2)));
        {fail, not_found} -> ?ct_fail("{fail, not_found}", [])
    end,
    true.

test_merge_with_renewal_before_step1(_Config) ->
    test_merge_with_renewal_at(_Config, merge, first).

test_merge_with_renewal_after_step1(_Config) ->
    test_merge_with_renewal_at(_Config, merge_reply_step1, second).

test_merge_with_renewal_after_step2(_Config) ->
    test_merge_with_renewal_at(_Config, merge_reply_step2, first).

test_merge_with_renewal_after_step3(_Config) ->
    test_merge_with_renewal_at(_Config, merge_reply_step3, second).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% split unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_split(_Config) ->
    % join group
    Pid = pid_groups:find_a(mockup_l_on_cseq),
    pid_groups:join(pid_groups:group_of(Pid)),
    % create lease
    L = mockup_l_on_cseq:create_lease(rt_SUITE:number_to_key(0),
                                      rt_SUITE:number_to_key(16), comm:make_global(Pid)),
    % do split
    Range = l_on_cseq:get_range(L),
    [R1, R2] = intervals:split(Range, 2),
    % evil, but l_on_cseq:lease_merge sends to a real dht_node
    comm:send_local(Pid, {l_on_cseq, split, L, R1, R2, self(), empty}),
    {L1, L2} = receive
                   {split, success, NewL1, NewL2} ->
                       {NewL1, NewL2}
               end,
    % check L1
    case l_on_cseq:read(l_on_cseq:get_id(L1)) of
        {ok, L1_} ->
            ?equals(l_on_cseq:get_range(L1_), R1),
            ?equals(l_on_cseq:get_aux(L1_), empty),
            true;
        {fail, not_found} ->
            ?ct_fail("{fail, not_found}", [])
    end,
    % check L2
    case l_on_cseq:read(l_on_cseq:get_id(L2)) of
        {ok, L2_} ->
            ?equals(l_on_cseq:get_range(L2_), R2),
            ?equals(l_on_cseq:get_aux(L2_), empty),
            true;
        {fail, not_found} ->
            ?ct_fail("{fail, not_found}", [])
    end,
    LeaseList = mockup_l_on_cseq:get_lease_list(),
    ActiveLease = lease_list:get_active_lease(LeaseList),
    [PassiveLease] = lease_list:get_passive_leases(LeaseList),
    ?equals(l_on_cseq:get_id(ActiveLease), l_on_cseq:get_id(L2)),
    ?equals(l_on_cseq:get_id(PassiveLease), l_on_cseq:get_id(L1)),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% merge unittest helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_merge_with_renewal_at(_Config, Step, FirstOrSecond) ->
    % join group
    Pid = pid_groups:find_a(mockup_l_on_cseq),
    pid_groups:join(pid_groups:group_of(Pid)),
    % create lease
    {L1, L2} = mockup_l_on_cseq:create_two_adjacent_leases(comm:make_global(Pid)),
    % prepare message filter
    mockup_l_on_cseq:set_message_filter(fun (Msg) ->
                                            element(2, Msg) =:= Step
                                        end,
                                        self()),
    % do merge
    % evil, but l_on_cseq:lease_merge sends to a real dht_node
    comm:send_local(Pid, {l_on_cseq, merge, L1, L2, self()}),
    % wait for message_filter
    %ct:pal("waiting for message filter", []),
    receive
        {intercepted_message, Msg} ->
            mockup_l_on_cseq:reset_message_filter(),
            case FirstOrSecond of
                first ->
                    synchronous_renew(L1, passive);
                second ->
                    synchronous_renew(L2, active)
            end,
            comm:send_local(Pid, Msg);
        X -> ct:pal("unknown message ~w", [X])
    end,
    % wait for finish
    ct:pal("waiting for merge success ~w", [self()]),
    receive
        {merge, success, _L2, _L1} -> ok
    end,
    % no renews during merge!
    ?equals(mockup_l_on_cseq:get_renewal_counter(), 1),
    % check L1
    case l_on_cseq:read(l_on_cseq:get_id(L1)) of
        {ok, L1_} -> ?equals(L1_, prbr_bottom);
        {fail, not_found} -> ?ct_fail("{fail, not_found}", [])
    end,
    % check L2
    case l_on_cseq:read(l_on_cseq:get_id(L2)) of
        {ok, L2_} ->
            ?equals(l_on_cseq:get_range(L2_),
                    intervals:union(l_on_cseq:get_range(L1),
                                    l_on_cseq:get_range(L2)));
        {fail, not_found} -> ?ct_fail("{fail, not_found}", [])
    end,
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% trigger lease renewal and wait until the renew happened
synchronous_renew(Lease, Mode) ->
    Pid = pid_groups:find_a(mockup_l_on_cseq),
    {ok, Current} = l_on_cseq:read(l_on_cseq:get_id(Lease)),
    ct:pal("sync. renew ~w ~w~n~w", [l_on_cseq:get_id(Lease), Mode, Current]),
    mockup_l_on_cseq:set_message_filter(fun (Msg) ->
                                            element(2, Msg) =:= renew_reply
                                        end,
                                        self()),
    %comm:send_local(Pid,
    %                {l_on_cseq, renew, Lease, Mode}),

    l_on_cseq:lease_renew(Pid, Current, Mode),
    receive
        {intercepted_message, Msg} ->
            mockup_l_on_cseq:reset_message_filter(),
            comm:send_local(Pid, Msg)
    end,
    ct:pal("sync. renew after~n~w", [l_on_cseq:read(l_on_cseq:get_id(Lease))]).

