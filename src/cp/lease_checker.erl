% @copyright 2012-2014 Zuse Institute Berlin,

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
%% @doc check ring.
%% @end
%% @version $Id$
-module(lease_checker).
-author('schuett@zib.de').
-vsn('$Id:$ ').

-include("scalaris.hrl").


-export([check_leases_for_all_nodes/0]).
-export([check_leases_for_the_ring/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% public api
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec check_leases_for_all_nodes() -> boolean().
check_leases_for_all_nodes() ->
    lists:all(fun (B) -> B end, [ check_local_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ]).

-spec check_leases_for_the_ring() -> boolean().
check_leases_for_the_ring() ->
    lease_checker(admin:number_of_nodes()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% check leases
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_local_leases(DHTNode) ->
    LeaseList = get_dht_node_state(DHTNode, lease_list),
    ActiveLease = lease_list:get_active_lease(LeaseList),
    PassiveLeases = lease_list:get_passive_leases(LeaseList),
    MyRange = get_dht_node_state(DHTNode, my_range),
    log:log("active lease=~p; my_range=~p", [ActiveLease, MyRange]),
    ActiveInterval = case ActiveLease of
                         empty ->
                             intervals:empty();
                         _ ->
                             l_on_cseq:get_range(ActiveLease)
                     end,
    LocalCorrect = MyRange =:= ActiveInterval,
    length(PassiveLeases) == 0 andalso LocalCorrect.

lease_checker(TargetSize) ->
    LeaseLists = get_all_leases(),
    ActiveLeases  = [lease_list:get_active_lease(LL)  || LL  <- LeaseLists],
    PassiveLeases = lists:flatten([lease_list:get_passive_leases(LL) || LL <- LeaseLists]),
    ActiveIntervals = [l_on_cseq:get_range(Lease) || Lease <- ActiveLeases, Lease =/= empty],
    NormalizedActiveIntervals = intervals:union(ActiveIntervals),
    %log:log("Lease-Checker: ~w ~w ~w", [ActiveLeases, ActiveIntervals, PassiveLeases]),
    %ct:pal("ActiveIntervals: ~p", [ActiveIntervals]),
    %ct:pal("PassiveLeases: ~p", [PassiveLeases]),
    IsAll = intervals:is_all(NormalizedActiveIntervals),
    IsDisjoint = is_disjoint(ActiveIntervals),
    HaveAllActiveLeases = length(ActiveLeases) == TargetSize,
    HaveNoPassiveLeases = length(PassiveLeases) == 0,
    HaveAllAuxEmpty = lists:all(fun(L) ->
                                        L =/= empty andalso l_on_cseq:get_aux(L) =:= empty
                                end, ActiveLeases),
    % ct:pal("lease checker: ~w ~w ~w ~w~n~w~n~w~n", [IsAll, IsDisjoint, HaveAllActiveLeases, HaveNoPassiveLeases,PassiveLeases, NormalizedActiveIntervals]),
    case IsAll of
        false ->
            %print_all_active_leases(),
            ok;
        true ->
            ok
    end,
    IsAll andalso
        HaveAllAuxEmpty andalso
        IsDisjoint andalso
        HaveAllActiveLeases andalso % @todo enable after garbage collection is implemented
        HaveNoPassiveLeases.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec is_disjoint([intervals:interval()]) -> boolean().
is_disjoint([]) ->
    true;
is_disjoint([H | T]) ->
    is_disjoint(H, T) andalso
        is_disjoint(T).

is_disjoint(_I, []) ->
    true;
is_disjoint(I, [H|T]) ->
    intervals:is_empty(intervals:intersection(I,H))
        andalso is_disjoint(I, T).

get_dht_node_state(Pid, What) ->
    comm:send_local(Pid, {get_state, comm:this(), What}),
    receive
        {get_state_response, Data} ->
            Data
    end.

get_all_leases() ->
    [ get_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ].

get_leases(Pid) ->
    get_dht_node_state(Pid, lease_list).
