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
%% @doc    Unit tests for slide_leases
%% @end
%% @version $Id$
-module(lease_helper).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

-export([wait_for_correct_leases/1,
         wait_for_ring_size/1,
         wait_for_correct_ring/0,
         print_all_active_leases/0,
         print_all_passive_leases/0
        ]).

-spec wait_for_correct_leases(pos_integer()) -> ok.
wait_for_correct_leases(TargetSize) ->
    wait_for(lease_checker(TargetSize)),
    ct:pal("have correct lease_checker"),
    wait_for(fun check_leases_per_node/0),
    ct:pal("have correct leases_per_node"),
    ok.

-spec wait_for_ring_size(pos_integer()) -> ok.
wait_for_ring_size(Size) ->
    wait_for(fun () -> 
                     Nodes = api_vm:number_of_nodes(),
                     log:log("expected ~w, found ~w", [Size, Nodes]),
                     Nodes == Size end).

-spec wait_for_correct_ring() -> ok.
wait_for_correct_ring() ->
    wait_for(fun () -> ct:pal("->admin:check_ring_deep()"),
                       Res = admin:check_ring_deep(),
                       ct:pal("<-admin:check_ring_deep()"),
                       Res == ok
             end).

-spec print_all_active_leases() -> ok.
print_all_active_leases() ->
    AllLeases = get_all_leases(),
    ActiveLeases = [ lease_list:get_active_lease(LL) || LL <- AllLeases ],
    log:log("active leases: ~w", [ActiveLeases]),
    ok.

-spec print_all_passive_leases() -> ok.
print_all_passive_leases() ->
    AllLeases = get_all_leases(),
    PassiveLeases = [ lease_list:get_passive_leases(LL) || LL <- AllLeases ],
    log:log("passive leases: ~w", [PassiveLeases]),
    ok.

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
            wait_for(F);
        X ->
            ct:pal("error in wait_for ~p", [X]),
            wait_for(F)
    end,
    ok.

get_dht_node_state(Pid, What) ->
    comm:send_local(Pid, {get_state, comm:this(), What}),
    receive
        {get_state_response, Data} ->
            Data
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_all_leases() ->
    [ get_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ].

get_leases(Pid) ->
    get_dht_node_state(Pid, lease_list).

check_leases_per_node() ->
    lists:all(fun (B) -> B end, [ check_local_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ]).

is_disjoint([]) ->
    true;
is_disjoint([H | T]) ->
    is_disjoint(H, T) andalso
        is_disjoint(T).

is_disjoint(_I, []) ->
    true;
is_disjoint(I, [H|T]) ->
    intervals:is_empty(intervals:intersection([I],[H]))
        andalso is_disjoint(I, T).


check_local_leases(DHTNode) ->
    LeaseList = get_dht_node_state(DHTNode, lease_list),
    ActiveLease = lease_list:get_active_lease(LeaseList),
    PassiveLeases = lease_list:get_passive_leases(LeaseList),
    %log:log("active lease: ~w", [ActiveLease]),
    ActiveInterval = case ActiveLease of
                         empty ->
                             intervals:empty();
                         _ ->
                             l_on_cseq:get_range(ActiveLease)
                     end,
    MyRange = get_dht_node_state(DHTNode, my_range),
    LocalCorrect = MyRange =:= ActiveInterval,
    length(PassiveLeases) == 0 andalso LocalCorrect.

lease_checker(TargetSize) ->
    fun () ->
            LeaseLists = get_all_leases(),
            ActiveLeases  = [lease_list:get_active_lease(LL)  || LL  <- LeaseLists],
            PassiveLeases = lists:flatten([lease_list:get_passive_leases(LL) || LL <- LeaseLists]),
            ActiveIntervals =   lists:flatten(
                                  [ l_on_cseq:get_range(Lease) || Lease <- ActiveLeases, Lease =/= empty]),
            NormalizedActiveIntervals = intervals:tester_create_interval(ActiveIntervals),
            %log:log("Lease-Checker: ~w ~w ~w", [ActiveLeases, ActiveIntervals, PassiveLeases]),
            %ct:pal("ActiveIntervals: ~p", [ActiveIntervals]),
            %ct:pal("PassiveLeases: ~p", [PassiveLeases]),
            IsAll = intervals:is_all(NormalizedActiveIntervals),
            IsDisjoint = is_disjoint(ActiveIntervals),
            HaveAllActiveLeases = length(ActiveLeases) == TargetSize,
            HaveNoPassiveLeases = length(PassiveLeases) == 0,
            %ct:pal("lease checker: ~w ~w ~w ~w", [IsAll, IsDisjoint, HaveAllActiveLeases, HaveNoPassiveLeases]),
            case IsAll of
                false ->
                    %print_all_active_leases(),
                    ok;
                true ->
                    ok
            end,
            IsAll andalso
                IsDisjoint andalso
                HaveAllActiveLeases andalso % @todo enable after garbage collection is implemented
                HaveNoPassiveLeases
    end.
