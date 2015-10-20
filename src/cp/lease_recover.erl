% @copyright 2007-2014 Zuse Institute Berlin

%  Licensed under the Apache License, Version 2.0 (the "License");
%  you may not use this file except in compliance with the License.
%  You may obtain a copy of the License at
%
%      http://www.apache.org/licenses/LICENSE-2.0
%
%  Unless required by applicable law or agreed to in writing, software
%  distributed under the License is distributed on an "AS IS" BASIS,
%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%  See the License for the specific language governing permissions and
%  limitations under the License.

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Recover leases.
%% @end
%% @version $$
-module(lease_recover).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([recover/1]).

-spec recover(list(prbr:state())) -> lease_list:lease_list().
recover(LeaseDBs) ->
    AllLeases = lists:flatmap(fun prbr:tab2list/1, LeaseDBs),
    LocalLeases = [L || {Id, L} <- AllLeases,
                        L =/= prbr_bottom, %% ??
                        Id =:= l_on_cseq:get_id(L) %% is this the first replica?,
                  ],
    case length(LocalLeases) of
        0 ->
            log:log("recovered with zero leases (~p)~n", [comm:this()]),
            restart_node(),
            lease_list:empty();
        1 ->
            wait_for_leases_to_timeout(LocalLeases),
            Lease = hd(LocalLeases),
            %% one potentially active lease: set active lease
            lease_list:make_lease_list(Lease, [], []);
        2 ->
            %% could be an ongoing split: finish operation
            wait_for_leases_to_timeout(LocalLeases),
            log:log("leases: ~p~n", [LocalLeases]),
            {Active, Passive} = get_active_passive(LocalLeases),
            Me = comm:reply_as(self(), 3, {l_on_cseq, post_recover_takeover, '_'}),
            l_on_cseq:lease_takeover(Passive, Me),
            lease_list:make_lease_list(Active, [Passive], []);
        _ ->
            %% could be an ongoing split or an ongoing merge: finish operation
            wait_for_leases_to_timeout(LocalLeases),
            log:log("leases: ~p~n", [LocalLeases]),
            ts = nyi, % ts: not yet implemented
            lease_list:empty()
    end.

-spec wait_for_leases_to_timeout([l_on_cseq:lease_t()]) -> ok.
wait_for_leases_to_timeout(LocalLeases) ->
    MaxTimeout = lists:max([l_on_cseq:get_timeout(L) || L <- LocalLeases]),
    WaitTime = timer:now_diff(MaxTimeout, os:timestamp()) * 1000,
    if
        WaitTime >= 0 ->
            timer:sleep(WaitTime);
        true ->
            ok
    end,
    ?DBG_ASSERT(lists:all(fun l_on_cseq:has_timed_out/1, LocalLeases)),
    ok.

% @doc take the left one
-spec get_active_passive([l_on_cseq:lease_t()]) ->
                                {l_on_cseq:lease_t(), l_on_cseq:lease_t()}.
get_active_passive(LocalLeases) ->
    [First, Second] = LocalLeases,
    case intervals:is_all(intervals:union(l_on_cseq:get_range(First),
                                          l_on_cseq:get_range(Second))) of
        true ->
            {Second, First};
        false ->
            case intervals:is_left_of(l_on_cseq:get_range(First),
                                      l_on_cseq:get_range(Second)) of
                true ->
                    {Second, First};
                false ->
                    {First, Second}
            end
    end.

-spec restart_node() -> no_return().
restart_node() ->
    NewNode = admin:add_node([]),
    log:log("we are restarting ~p -> ~p~n", [comm:this(), NewNode]),
    %% async. call!
    service_per_vm:kill_nodes_by_name([pid_groups:my_groupname()]),
    util:sleep_for_ever().
