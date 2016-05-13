% @copyright 2012-2016 Zuse Institute Berlin,

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
-module(lease_checker2).
-author('schuett@zib.de').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-record(leases_state_t, {
          last_check  = ?required(lease_state_t, last_check) :: erlang:timestamp(),
          node_infos  = ?required(lease_state_t, node_infos) :: node_list(),
          last_failed = ?required(lease_state_t, last_faile) :: boolean()
         }).

-record(node_info_t, {
          lease_list  = ?required(lease_state_t, lease_list) :: lease_list:lease_list(),
          my_range    = ?required(lease_state_t, my_range)   :: intervals:interval()
         }).

-type node_list() :: gb_trees:tree(comm:mypid(), node_info() | empty).
-type leases_state() :: #leases_state_t{}.
-type node_info() :: #node_info_t{}.

-type option() :: {ring_size, pos_integer()} | {ring_size_range, pos_integer(), pos_integer()}.
-type options() :: list(option()).

-export_type([leases_state/0]).

-export([wait_for_clean_leases/2]).

-export([get_kv_db/0, get_kv_db/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% public api
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec wait_for_clean_leases(WaitTimeInMs::pos_integer(), Options::options()) -> ok.
wait_for_clean_leases(WaitTimeInMs, Options) ->
    ?ASSERT(not gen_component:is_gen_component(self())),
    wait_for_clean_leases(WaitTimeInMs, Options, true, create_new_state()).

-spec get_kv_db() -> ok.
get_kv_db() ->
    KVDBs = [ get_dht_node_state(Pid, kv_db) || Pid <- all_dht_nodes()],
    Data = [prbr:tab2list(DB) || {true, DB} <- KVDBs, DB =/= false],
    FlattenedData = lists:flatten(Data),
    io:format("kv-pairs: ~p~n", [length(FlattenedData)]),
    Empties = [ DB || DB <- KVDBs, DB =:= false],
    Bottoms = [Value || {_Key, Value} <- FlattenedData, Value =:= prbr_bottom],
    io:format("falses: ~p~n", [length(Empties)]),
    io:format("prbr_bottoms: ~p~n", [length(Bottoms)]),
    %% io:format("data: ~p~n", [FlattenedData]),
    ok.

-spec get_kv_db(term()) -> ok.
get_kv_db(Pid) ->
    io:format("~p~n", [Pid]),
    {true, DB} = get_dht_node_state(comm:make_global(Pid), kv_db),
    io:format("kv-pairs: ~p~n", [length(prbr:tab2list(DB))]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% internal api
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec wait_for_clean_leases(WaitTimeInMs::pos_integer(), Options::options(),
                            First::boolean(), State::leases_state()) -> ok.
wait_for_clean_leases(WaitTimeInMs, Options, First, State) ->
    case check_leases(State, Options, First) of
        {true, _}  -> ok;
        {false, NewState} ->
            WaitID = uid:get_pids_uid(),
            comm:send_local_after(WaitTimeInMs, self(), {continue_wait, WaitID}),
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV({continue_wait, WaitID},% ->
                               wait_for_clean_leases(WaitTimeInMs, Options, false, NewState))
            end
    end.

-spec check_leases(OldState::leases_state(), Options::options(), First::boolean()) ->
                          {boolean(), leases_state()}.
check_leases(OldState, Options, First) ->
    LastFailed = OldState#leases_state_t.last_failed,
    NewState = create_new_state(),
    TargetSize = renderTargetSize(Options),
    case {gb_trees:size(OldState#leases_state_t.node_infos),
          gb_trees:size(NewState#leases_state_t.node_infos)} of
        {Old, Old} ->
            io:format("================= check leases (~p of ~p) ====================~n",
                      [Old, TargetSize]);
        {Old, New} ->
          io:format("================= check leases ((~p -> ~p) of ~p) ====================~n",
                   [Old, New, TargetSize])
     end,
    case First of
        true -> io:format("begin existing nodes~n"),
                describe_nodes(OldState#leases_state_t.node_infos),
                io:format("end existing nodes~n");
        false -> ok
    end,
    Changed =
        case compare_node_lists(OldState#leases_state_t.node_infos,
                                NewState#leases_state_t.node_infos) of
            true -> false;
            false ->
                io:format("begin diff~n"),
                describe_lease_states_diff(OldState, NewState),
                io:format("end diff~n"),
                true
        end,
    Verbose = First orelse not LastFailed orelse Changed,
    Res = check_state(NewState, Verbose, Options),
    io:format("check_state returned(verbose=~p) ~p~n", [Verbose, Res]),
    {Res, NewState#leases_state_t{last_failed=not Res}}.

-spec check_state(State::leases_state(), Verbose::boolean(),
                  Options::options()) -> boolean().
check_state(State, Verbose, Options) ->
    case check_leases_locally(State, Verbose) of
        true ->
            case check_leases_globally(State, Verbose, Options) of
                true ->
                    true;
                false -> io:format("check_leases_globally failed~n"),
                         false
            end;
        false -> io:format("check_leases_locally failed~n"),
                 false
    end.
    %% check_leases_locally(State, Verbose) andalso
    %%     check_leases_globally(State, Verbose, TargetSize).

-spec check_leases_locally(leases_state(), boolean()) -> boolean().
check_leases_locally(#leases_state_t{node_infos=Nodes}, Verbose) ->
    lists:foldl(fun ({Pid, Node}, Acc) ->
                        Acc and check_local_leases(Pid, Node, Verbose)
                end, true, gb_trees:to_list(Nodes)).

-spec check_leases_globally(leases_state(), boolean(), options()) -> boolean().
check_leases_globally(State, Verbose, Options) ->
    lease_checker(State, Verbose, Options).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% compare functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns true iff the lists are equal
-spec compare_node_lists(node_list(), node_list()) -> boolean().
compare_node_lists(Old, New) ->
    OldPids = ordsets:from_list(gb_trees:keys(Old)),
    NewPids = ordsets:from_list(gb_trees:keys(New)),

    case OldPids =:= NewPids of
        false -> false;
        true ->
            lists:foldl(fun (Pid, Acc) ->
                                Acc andalso compare_node_infos(gb_trees:get(Pid, Old),
                                                               gb_trees:get(Pid, New))
                        end, true, ordsets:to_list(NewPids))
    end.

-spec compare_node_infos(node_info() | empty, node_info() | empty) -> boolean().
compare_node_infos(Old, New) ->
    case {Old, New} of
        {empty, empty} -> true;
        {empty, New} -> false;
        {Old, empty} -> false;
        {Old, New}   -> Old#node_info_t.my_range =:= New#node_info_t.my_range
                            andalso compare_lease_lists(Old#node_info_t.lease_list,
                                                        New#node_info_t.lease_list)
    end.

-spec compare_lease_lists(L1::lease_list:lease_list(), L2::lease_list:lease_list()) -> boolean().
compare_lease_lists(L1, L2) ->
    compare_leases(lease_list:get_active_lease(L1), lease_list:get_active_lease(L2))
        andalso compare_passive_leases(lease_list:get_passive_leases(L1),
                                       lease_list:get_passive_leases(L2)).

-spec compare_passive_leases(L1::[l_on_cseq:lease_t()], L2::[l_on_cseq:lease_t()]) -> boolean().
compare_passive_leases(L1, L2) ->
    Ids1 = [l_on_cseq:get_id(L) || L <- L1],
    Ids2 = [l_on_cseq:get_id(L) || L <- L2],
    SetOfIds1 = ordsets:from_list(Ids1),
    SetOfIds2 = ordsets:from_list(Ids2),
    case SetOfIds1 =:= SetOfIds2 of
        true ->
            %% @todo use lists:foldl
            lists:all(fun (Bool) -> Bool end,
                      lists:zipwith(fun (Lease1, Lease2) -> compare_leases(Lease1, Lease2) end,
                                    L1, L2));
        false ->
            false
    end.

-spec compare_leases(L1::l_on_cseq:lease_t() | empty, L2::l_on_cseq:lease_t() | empty) -> boolean().
compare_leases(L1, L2) ->
    case {L1, L2} of
        {empty, empty} -> true;
        {empty, L2} -> false;
        {L1, empty} -> false;
        _ ->
            l_on_cseq:get_id(L1) =:= l_on_cseq:get_id(L2)
                andalso l_on_cseq:get_owner(L1) =:= l_on_cseq:get_owner(L2)
                andalso l_on_cseq:get_range(L1) =:= l_on_cseq:get_range(L2)
                andalso l_on_cseq:get_aux(L1) =:= l_on_cseq:get_aux(L2)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% describe things functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec describe_nodes(node_list()) -> ok.
describe_nodes(Nodes) ->
    _ = [ describe_node(Pid, Node) || {Pid, Node} <- gb_trees:to_list(Nodes) ],
    ok.

-spec describe_list_of_leases(Leases::[l_on_cseq:lease_t()], active | passive) -> ok.
describe_list_of_leases(Leases, Type) ->
    _ = [ describe_lease(L, Type) || L <- Leases ],
    ok.

-spec describe_lease(L1::l_on_cseq:lease_t(), active | passive) -> ok.
describe_lease(Lease, _Type) ->
    %% @todo use type parameter
    Interval = l_on_cseq:get_range(Lease),
    RelRange = get_relative_range(Interval),
    Owner    = l_on_cseq:get_owner(Lease),
    Aux      = l_on_cseq:get_aux(Lease),

    io:format("    range:~p~n   rel_range:~p~n   owner:~p~n   aux:~p~n", [Interval, RelRange, Owner, Aux]),
    ok.

%% @todo change to /1 with | empty
-spec describe_node(Node::comm:mypid(), node_info() | empty) -> ok.
describe_node(Pid, NodeInfo) ->
    case NodeInfo of
        empty -> ok;
        _ ->
            LeaseList = NodeInfo#node_info_t.lease_list,
            MyRange   = NodeInfo#node_info_t.my_range,
            ActiveLease = lease_list:get_active_lease(LeaseList),
            PassiveLeases = lease_list:get_passive_leases(LeaseList),
            ActiveInterval = case ActiveLease of
                                 empty ->
                                     intervals:empty();
                                 _ ->
                                     l_on_cseq:get_range(ActiveLease)
                             end,
            RelRange = get_relative_range(ActiveInterval),
            Aux = case ActiveLease of
                      empty -> no_lease;
                      _ -> l_on_cseq:get_aux(ActiveLease)
                  end,
            LocalCorrect = MyRange =:= ActiveInterval,
            io:format("  ~p~n", [Pid]),
            io:format("    rm =:= leases -> ~w~n", [LocalCorrect]),
            io:format("      active lease=~p~n", [ActiveInterval]),
            io:format("        my_range  =~p~n", [MyRange]),
            io:format("        rel_range =~p~n", [RelRange]),
            io:format("        aux       =~p~n", [Aux]),
            io:format("      passive     =~p~n", [PassiveLeases]),
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% describe differences functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec describe_lease_states_diff(leases_state(), leases_state()) -> ok.
describe_lease_states_diff(Old, New) ->
    %% PRE they differ
    describe_nodes_diff(Old#leases_state_t.node_infos, New#leases_state_t.node_infos).

-spec describe_nodes_diff(node_list(), node_list()) -> ok.
describe_nodes_diff(OldNodeInfos, NewNodeInfos) ->
    %% PRE they differ
    OldPids = ordsets:from_list(gb_trees:keys(OldNodeInfos)),
    NewPids = ordsets:from_list(gb_trees:keys(NewNodeInfos)),

    Unchanged = ordsets:intersection(OldPids, NewPids),
    FoundPids = ordsets:subtract(NewPids, OldPids),
    LostPids  = ordsets:subtract(OldPids, NewPids),
    %% lost
    _ = case ordsets:size(LostPids) of
        0 -> ok;
        N -> Old = ordsets:to_list(LostPids),
             io:format("lost ~p nodes: ~p~n", [N, Old]),
             describe_nodes(gb_trees_filter(fun (Pid, _Node) ->
                                                    ordsets:is_element(Pid, LostPids) end,
                                            OldNodeInfos))
        end,
    %% found
    _ = case ordsets:size(FoundPids) of
        0 -> ok;
        N2 -> New = ordsets:to_list(FoundPids),
             io:format("found ~p new nodes: ~p~n", [N2, New]),
             describe_nodes(gb_trees_filter(fun (Pid, _Node) ->
                                                    ordsets:is_element(Pid, FoundPids) end,
                                            NewNodeInfos)),
             io:format("end found~n", [])
    end,
    _ = case ordsets:size(Unchanged) of
        0 -> ok;
        _ ->
            [ describe_node_diff(Pid, gb_trees:get(Pid, OldNodeInfos),
                                 gb_trees:get(Pid, NewNodeInfos))
              || Pid <- ordsets:to_list(Unchanged)]
    end,
    ok.

-spec describe_node_diff(Node::comm:mypid(), OldNodeInfo::node_info() | empty,
                         NewNodeInfo::node_info() | empty) -> ok.
describe_node_diff(Node, OldNodeInfo, NewNodeInfo) ->
    case {OldNodeInfo, NewNodeInfo} of
        {empty, empty}       -> ok;
        {empty, NewNodeInfo} ->
            io:format("the node ~p has changed~n", [Node]),
            io:format("node info changed from empty to~n"),
            describe_node(Node, NewNodeInfo);
        {OldNodeInfo, empty} ->
            io:format("the node ~p has changed~n", [Node]),
            io:format("node info changed to empty from~n"),
            describe_node(Node, OldNodeInfo);
        {OldNodeInfo, NewNodeInfo} ->
            OldLeaseList = OldNodeInfo#node_info_t.lease_list,
            NewLeaseList = NewNodeInfo#node_info_t.lease_list,
            LeasesDiffer = not compare_lease_lists(OldLeaseList, NewLeaseList),
            OldRange = OldNodeInfo#node_info_t.my_range,
            NewRange = NewNodeInfo#node_info_t.my_range,
            RangesDiffer = OldRange =/= NewRange,

            case LeasesDiffer orelse RangesDiffer of
                true ->
                    io:format("the node ~p has changed~n", [Node]),
                    _ = case LeasesDiffer of
                            true ->
                                describe_lease_list_diff(OldLeaseList, NewLeaseList);
                            false ->
                                ok
                        end,
                    _ = case RangesDiffer of
                            true -> io:format("    the nodes' range changed~n      ~p~n      ~p~n",
                                              [OldRange, NewRange]);
                            false -> ok
                        end,
                    ok;
                false ->
                    ok
            end
    end.

-spec describe_lease_list_diff(lease_list:lease_list(), lease_list:lease_list()) -> ok.
describe_lease_list_diff(OldLeaseList, NewLeaseList) ->
    describe_lease_diff(lease_list:get_active_lease(OldLeaseList),
                        lease_list:get_active_lease(NewLeaseList), active),
    describe_list_of_leases_diff(lease_list:get_passive_leases(OldLeaseList),
                                 lease_list:get_passive_leases(NewLeaseList), passive),
    ok.

-spec describe_lease_diff(l_on_cseq:lease_t() | empty, l_on_cseq:lease_t() | empty,
                          active | passive) -> ok.
describe_lease_diff(OldLease, NewLease, Type) ->
    case {OldLease, NewLease} of
        {empty, empty} -> ok;
        {empty, NewLease} -> io:format("nyi3~n");
        {OldLease, empty} -> io:format("nyi4~n");
        {_, _} ->
            case compare_leases(OldLease, NewLease) of
                true -> ok;
                false ->
                    io:format("  an ~p lease has changed~n", [Type]),
                    case l_on_cseq:get_id(OldLease) =:= l_on_cseq:get_id(NewLease) of
                        true -> ok;
                        false ->
                            io:format("    the id has changed~n      ~p~n      ~p~n",
                                      [l_on_cseq:get_id(OldLease),
                                       l_on_cseq:get_id(NewLease)])
                    end,
                    case l_on_cseq:get_owner(OldLease) =:= l_on_cseq:get_owner(NewLease) of
                        true -> ok;
                        false ->
                            io:format("    the owner has changed~n      ~p~n      ~p~n",
                                      [l_on_cseq:get_owner(OldLease),
                                       l_on_cseq:get_owner(NewLease)])
                    end,
                    case l_on_cseq:get_range(OldLease) =:= l_on_cseq:get_range(NewLease) of
                        true -> ok;
                        false ->
                            io:format("    the range has changed from~n    ~p~n    ->~n    ~p~n",
                                      [l_on_cseq:get_range(OldLease),
                                       l_on_cseq:get_range(NewLease)])
                    end,
                    case l_on_cseq:get_aux(OldLease) =:= l_on_cseq:get_aux(NewLease) of
                        true -> ok;
                        false ->
                            io:format("    the aux has changed~n    ~p~n    ->~n    ~p~n",
                                      [l_on_cseq:get_aux(OldLease),
                                       l_on_cseq:get_aux(NewLease)])
                    end
            end
    end,
    ok.

-spec describe_list_of_leases_diff([l_on_cseq:lease_t()], [l_on_cseq:lease_t()],
                                   active | passive) -> ok.
describe_list_of_leases_diff(OldLeases, NewLeases, Type) ->
    OldIds = ordsets:from_list([l_on_cseq:get_id(L) || L <- OldLeases]),
    NewIds = ordsets:from_list([l_on_cseq:get_id(L) || L <- NewLeases]),

    Unchanged = ordsets:intersection(OldIds, NewIds),
    FoundIds = ordsets:subtract(NewIds, OldIds),
    LostIds  = ordsets:subtract(OldIds, NewIds),

    %% lost
    _ = case ordsets:size(LostIds) of
        0 -> ok;
        N -> io:format("  lost ~p passive leases: ~p~n", [N, ordsets:to_list(LostIds)]),
             describe_list_of_leases(lists:filter(fun (L) ->
                                                          ordsets:is_element(l_on_cseq:get_id(L),
                                                                             LostIds)
                                                  end, OldLeases), Type)
    end,
    %% found
    _ = case ordsets:size(FoundIds) of
        0 -> ok;
        N2 -> io:format("  found ~p new passive leases: ~p~n", [N2, ordsets:to_list(FoundIds)]),
              describe_list_of_leases(lists:filter(fun (L) ->
                                                           ordsets:is_element(l_on_cseq:get_id(L),
                                                                              FoundIds)
                                                   end, NewLeases), Type)
    end,
    _ = case ordsets:size(Unchanged) of
        0 -> ok;
        _ -> OL = lists:sort(fun (L1, L2) -> l_on_cseq:get_id(L1) < l_on_cseq:get_id(L2) end,
                             OldLeases),
             NL = lists:sort(fun (L1, L2) -> l_on_cseq:get_id(L1) < l_on_cseq:get_id(L2) end,
                             NewLeases),
             [ describe_lease_diff(L1,L2, Type) || L1 <- OL, L2 <- NL,
                                                   ordsets:is_element(l_on_cseq:get_id(L1),
                                                                      Unchanged)]
    end,
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% state handling
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec create_new_state() -> leases_state().
create_new_state() ->
    Nodes = lists:foldl(fun (DHTNode, Tree) ->
                                Info = create_node_info(DHTNode),
                                gb_trees:insert(DHTNode, Info, Tree)
                        end, gb_trees:empty(), all_dht_nodes()),
    #leases_state_t{last_check = os:timestamp(), node_infos=Nodes, last_failed=false}.

-spec create_node_info(comm:mypid()) -> node_info() | empty.
create_node_info(DHTNode) ->
    case get_dht_node_state(DHTNode, [lease_list, my_range]) of
        false ->
            empty;
        {true, [{lease_list, LeaseList}, {my_range, MyRange}]} ->
            #node_info_t{lease_list = LeaseList, my_range = MyRange}
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% check leases
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec check_local_leases(comm:mypid(), node_info(), boolean()) -> boolean().
check_local_leases(Pid, NodeInfo, Verbose) ->
    case NodeInfo of
        empty ->
            false;
        #node_info_t{lease_list=LeaseList, my_range=MyRange} ->
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
            case length(PassiveLeases) == 0 andalso LocalCorrect of
                true -> true;
                false ->
                    case Verbose of
                        true ->
                            case ActiveLease of
                                empty ->
                                    io:format("the active lease is empty~n");
                                _ ->
                                    Aux = l_on_cseq:get_aux(ActiveLease),
                                    io:format("  ~p~n", [Pid]),
                                    io:format("    rm =:= leases -> ~w~n", [LocalCorrect]),
                                    io:format("      active lease=~p~n", [ActiveInterval]),
                                    io:format("        my_range  =~p~n", [MyRange]),
                                    io:format("        rel_range =~p~n", [RelRange]),
                                    io:format("        aux       =~p~n", [Aux]),
                                    io:format("      passive     =~p~n", [PassiveLeases])
                            end;
                        false ->
                            ok
                    end,
                    false
            end
    end.

-spec lease_checker(State::leases_state(), Verbose::boolean(),
                    Options::options()) -> boolean().
lease_checker(#leases_state_t{node_infos=NodeInfos}, Verbose, Options) ->
    LeaseLists = [Node#node_info_t.lease_list || Node <- gb_trees:values(NodeInfos)],
    ActiveLeases  = [lease_list:get_active_lease(LL)  || LL  <- LeaseLists],
    PassiveLeases = lists:flatmap(fun lease_list:get_passive_leases/1, LeaseLists),
    ActiveIntervals = [l_on_cseq:get_range(Lease) || Lease <- ActiveLeases, Lease =/= empty],
    NormalizedActiveIntervals = intervals:union(ActiveIntervals),
    %io:format("Lease-Checker: ~w ~w ~w", [ActiveLeases, ActiveIntervals, PassiveLeases]),
    %ct:pal("ActiveIntervals: ~p", [ActiveIntervals]),
    %ct:pal("PassiveLeases: ~p", [PassiveLeases]),
    IsAll = intervals:is_all(NormalizedActiveIntervals),
    IsDisjoint = is_disjoint(ActiveIntervals),
    HaveAllActiveLeases = checkActiveLeases(length(ActiveLeases), Options),
    %% HaveAllActiveLeases = length(ActiveLeases) == TargetSize,
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
    case IsAll andalso HaveAllAuxEmpty andalso IsDisjoint andalso HaveAllActiveLeases andalso HaveNoPassiveLeases of
        true -> ok;
        false ->
            case Verbose of
                true ->
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
                    end;
                false -> ok
            end
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

-spec get_relative_range(intervals:interval()) -> float().
get_relative_range(ActiveInterval) ->
    case intervals:empty() of
        ActiveInterval ->
            0.0 / ?RT:n();
        _ ->
            {_, Begin, End, _} = intervals:get_bounds(ActiveInterval),
            ?RT:get_range(Begin, End) / ?RT:n()
    end.

-spec get_dht_node_state(comm:mypid(), atom() | list(atom())) -> term() | list(term()).
get_dht_node_state(Pid, What) ->
    case proto_sched:infected() of
        true ->
            Cookie = {os:timestamp(), randoms:getRandomInt()},
            This = comm:reply_as(comm:this(), 2, {get_dht_node_state_response, '_', Cookie}),
            comm:send(Pid, {get_state, This, What}),
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV({get_dht_node_state_response,
                                {get_state_response, Data}, Cookie},% ->
                               {true, Data})
                end;
        false ->
            false = trace_mpath:infected(),
            Cookie = {os:timestamp(), randoms:getRandomInt()},
            This = comm:reply_as(comm:this(), 2, {get_dht_node_state_response, '_', Cookie}),
            comm:send(Pid, {get_state, This, What}),
            Result =
                receive
                    ?SCALARIS_RECV({get_dht_node_state_response, {get_state_response, Data}, Cookie},% ->
                                   {true, Data})
                after 50 ->
                        false
                end,
            %% drain message queue
            drain_message_queue(),
            Result
    end.

drain_message_queue() ->
    false = trace_mpath:infected(),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({get_dht_state_response, _Data, _Cookie},% ->
                       ok)
    after 0 ->
            ok
    end.

-spec all_dht_nodes() -> list(comm:mypid()).
all_dht_nodes() ->
    mgmt_server:node_list(),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({get_list_response, Nodes},% ->
            Nodes)
    end.

%% @doc keep alle elements of Tree for which F(K,V) is true
-spec gb_trees_filter(F::fun((K, V) -> boolean()), Tree::gb_trees:tree(K,V)) -> gb_trees:tree(K,V).
gb_trees_filter(F, Tree) ->
    gb_trees_filter(F, gb_trees:empty(), gb_trees:iterator(Tree)).

-spec gb_trees_filter(F::fun((K, V) -> boolean()), Acc::gb_trees:tree(K, V),
                      Iter::gb_trees:iter(K,V)) -> gb_trees:tree(K, V).
gb_trees_filter(F, Acc, Iter) ->
    case gb_trees:next(Iter) of
        none -> Acc;
        {Key, Value, Iter2} ->
            case F(Key,Value) of
                true ->
                    gb_trees_filter(F, gb_trees:enter(Key, Value, Acc), Iter2);
                false ->
                    gb_trees_filter(F, Acc, Iter2)
            end
    end.

-spec renderTargetSize(Options::options()) -> term().
renderTargetSize(Options) ->
    case lists:keyfind(ring_size, 1, Options) of
        false ->
            case lists:keyfind(ring_size_range, 1, Options) of
                false ->
                    unknown;
                {ring_size_range, From, To} ->
                    [From, To]
            end;
        {ring_size, TargetSize} ->
            TargetSize
    end.

-spec checkActiveLeases(NumberOfActiveLeases::pos_integer(), Options::options()) -> boolean().
checkActiveLeases(NumberOfActiveLeases, Options) ->
    case lists:keyfind(ring_size, 1, Options) of
        false ->
            case lists:keyfind(ring_size_range, 1, Options) of
                false ->
                    true; %% unknown
                {ring_size_range, From, To} ->
                    From =< NumberOfActiveLeases andalso  NumberOfActiveLeases =< To
            end;
        {ring_size, TargetSize} ->
            NumberOfActiveLeases =:= TargetSize
    end.
