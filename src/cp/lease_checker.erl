% @copyright 2012-2017 Zuse Institute Berlin,

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
-export([check_leases_for_the_ring/1]).
-export([get_random_save_node/0]).

-export([get_relative_range_unittest/1]).
-export([get_dht_node_state_unittest/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% public api
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec check_leases_for_all_nodes() -> boolean().
check_leases_for_all_nodes() ->
    io:format("======= node local test ==========~n"),
    lists:all(fun (B) -> B end, [ check_local_leases(DHTNode) || DHTNode <- all_dht_nodes() ]).

-spec check_leases_for_the_ring() -> boolean().
check_leases_for_the_ring() ->
    io:format("======= global test ==========~n"),
    lease_checker(admin:number_of_nodes()).

-spec check_leases_for_the_ring(pos_integer()) -> boolean().
check_leases_for_the_ring(TargetSize) ->
    io:format("======= global test ==========~n"),
    lease_checker(TargetSize).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% check leases
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_local_leases(DHTNode) ->
    case get_dht_node_state(DHTNode, [lease_list, my_range]) of
        false ->
            false;
        {true, [{lease_list, LeaseList}, {my_range, MyRange}]} ->
            ActiveLease = lease_list:get_active_lease(LeaseList),
            PassiveLeases = lease_list:get_passive_leases(LeaseList),
            ActiveInterval = case ActiveLease of
                                 empty ->
                                     intervals:empty();
                                 _ ->
                                     l_on_cseq:get_range(ActiveLease)
                             end,
            LocalCorrect = MyRange =:= ActiveInterval,
            RelRange = get_relative_range(ActiveInterval),
            io:format("rm =:= leases:~w~n active lease=~p~n my_range    =~p~n rel_range     =~p~n",
                      [LocalCorrect, ActiveInterval, MyRange, RelRange]),
            length(PassiveLeases) == 0 andalso LocalCorrect
    end.

lease_checker(TargetSize) ->
    LeaseLists = get_all_leases(),
    ActiveLeases  = [lease_list:get_active_lease(LL)  || LL  <- LeaseLists],
    PassiveLeases = lists:flatmap(fun lease_list:get_passive_leases/1, LeaseLists),
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
    io:format("complete ring covered by leases: ~w~n", [IsAll]),
    io:format("all aux-fields are empty       : ~w~n", [HaveAllAuxEmpty]),
    io:format("no leases overlap              : ~w~n", [IsDisjoint]),
    io:format("each node has one active lease : ~w~n", [HaveAllActiveLeases]),
    io:format("no passive leases              : ~w~n", [HaveNoPassiveLeases]),
    case HaveAllAuxEmpty of
        false ->
            io:format("aux fields: ~w~n", [[ l_on_cseq:get_aux(L) || L <- ActiveLeases,
                                                                     L =/= empty ]]);
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


%@doc returns a random node which covers a minority of the key-space
-spec get_random_save_node() -> comm:mypid() | failed.
get_random_save_node() ->
    R = config:read(replication_factor),
    SaveFraction = quorum:minority(R) / R,
    LeaseNodes = [{ActiveLease, Node} || Node <- all_dht_nodes(),
                                         {true, LL} <- [get_dht_node_state(Node, lease_list)],
                                         ActiveLease <- [lease_list:get_active_lease(LL)],
                                            ActiveLease =/= empty],

    SaveNodes = [{Range, Node} || {Lease, Node} <- LeaseNodes,
                                   Range <- [get_relative_range(l_on_cseq:get_range(Lease))],
                                    Range =< SaveFraction],

    case SaveNodes of
        [] ->
            failed;
        _ ->
            Rand = randoms:uniform(),
            ReturnNode = if Rand < 0.5 ->
                                 _UnsafestSafeNode = lists:max(SaveNodes);
                            true ->
                                 _RandomSafeNode = util:randomelem(SaveNodes)
                         end,
            element(2, ReturnNode)
    end.

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

-spec get_relative_range_unittest(intervals:interval()) -> float().
get_relative_range_unittest(ActiveInterval) ->
    ?ASSERT(util:is_unittest()),
    get_relative_range(ActiveInterval).

-spec get_relative_range(intervals:interval()) -> float().
get_relative_range(ActiveInterval) ->
    case intervals:empty() of
        ActiveInterval ->
            0.0 / ?RT:n();
        _ ->
            {_, Begin, End, _} = intervals:get_bounds(ActiveInterval),
            ?RT:get_range(Begin, End) / ?RT:n()
    end.

-spec get_dht_node_state_unittest(comm:mypid(), atom() | list(atom())) -> term() | list(term()).
get_dht_node_state_unittest(Pid, What) ->
    ?ASSERT(util:is_unittest()),
    get_dht_node_state(Pid, What).

-spec get_dht_node_state(comm:mypid(), atom() | list(atom())) -> term() | list(term()).
get_dht_node_state(Pid, What) ->
    false = trace_mpath:infected(),
    Cookie = {os:timestamp(), randoms:getRandomInt()},
    This = comm:reply_as(comm:this(), 2, {get_dht_node_state_response, '_', Cookie}),
    comm:send(Pid, {get_state, This, What}),
    trace_mpath:thread_yield(),
    Result =
        receive
            ?SCALARIS_RECV({get_dht_node_state_response, {get_state_response, Data}, Cookie},% ->
                {true, Data})
        after 50 ->
                false
        end,
    % drain message queue
    drain_message_queue(),
    Result.

drain_message_queue() ->
    false = trace_mpath:infected(),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({get_dht_state_response, _Data, _Cookie},% ->
                       ok)
    after 0 ->
            ok
    end.

-spec get_all_leases() -> list(lease_list:lease_list()).
get_all_leases() ->
    % short for lists:filtermap/2
    [ element(2, L) || Node <- all_dht_nodes(),
                       (L = get_dht_node_state(Node, lease_list)) =/= false ].

-spec all_dht_nodes() -> list(comm:mypid()).
all_dht_nodes() ->
    mgmt_server:node_list(),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({get_list_response, Nodes},% ->
            Nodes)
    end.
