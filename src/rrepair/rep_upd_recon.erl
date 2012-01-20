% @copyright 2011 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    replica reconcilication protocol
%% @end
%% @version $Id$

-module(rep_upd_recon).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/2, check_config/0]).

%export for testing
-export([encodeBlob/2, decodeBlob/1,
         minKeyInInterval/2,
         mapInterval/2, map_key_to_quadrant/2, 
         get_key_quadrant/1, get_interval_quadrant/1]).

-ifdef(with_export_type_support).
-export_type([method/0, recon_struct/0, recon_stage/0]).
-endif.

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%DETAIL DEBUG MESSAGES
%-define(TRACE2(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE2(X,Y), ok).

-define(IIF(C, A, B), case C of
                          true -> A;
                          _ -> B
                      end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type method()         :: bloom | merkle_tree | art | undefined.
-type recon_stage()    :: req_shared_interval | res_shared_interval | build_struct | reconciliation.
-type exit_reason()    :: empty_interval | {ok, atom()}.

-type db_entry_enc()   :: binary().
-type db_as_list_enc() :: [db_entry_enc()].
-type db_chunk_enc()   :: {intervals:interval(), db_as_list_enc()}.

-record(bloom_recon_struct, 
        {
         interval = intervals:empty()                     :: intervals:interval(), 
         keyBF    = ?required(bloom_recon_struct, keyBF)  :: ?REP_BLOOM:bloom_filter(),
         versBF   = ?required(bloom_recon_struct, versBF) :: ?REP_BLOOM:bloom_filter()         
        }).
-type bloom_recon_struct() :: #bloom_recon_struct{}.

-type internal_params() :: {interval, intervals:interval()} |
                           {senderPid, comm:mypid()} |
                           {art, art:art()}.

-type recon_struct() :: bloom_recon_struct() |
                        merkle_tree:merkle_tree() |
                        art:art() |                        
                        [internal_params()].

-record(ru_recon_state,
        {
         ownerLocalPid      = ?required(ru_recon_state, ownerLocalPid)  :: comm:erl_local_pid(),
         ownerRemotePid     = ?required(ru_recon_state, ownerRemotePid) :: comm:mypid(),
         dhtNodePid         = ?required(ru_recon_state, dhtNodePid)     :: comm:erl_local_pid(),
         dest_ru_pid        = undefined                                 :: comm:mypid() | undefined,
         recon_method       = undefined                                 :: method(),   %determines the build sync struct
         recon_struct       = {}                                        :: recon_struct() | {},
         recon_stage        = reconciliation                            :: recon_stage(),
         sync_pid           = undefined                                 :: comm:mypid() | undefined,%sync dest process
         sync_master        = false                                     :: boolean(),               %true if process is sync leader
         sync_round         = 0                                         :: rep_upd:round(),
         stats              = ru_recon_stats:new()                      :: ru_recon_stats:stats(),
         sync_start_time    = {0, 0, 0}                                 :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
         }).
-type state() :: #ru_recon_state{}.

-type build_args() :: {} | {Fpr::float()}.
-type tree_cmp_response() :: ok_inner | ok_leaf |
                             fail_leaf | fail_node |
                             not_found.

-type message() ::
    {get_state_response, intervals:interval()} |
    {get_chunk_response, db_chunk_enc()} |
    {check_node, MasterPid::comm:mypid(), intervals:interval(), merkle_tree:mt_node_key()} |
    {check_node_response, tree_cmp_response(), intervals:interval(), [merkle_tree:mt_node_key()]} |
    {start_recon, method(), recon_stage(), recon_struct(), Master::boolean()} |
    {shutdown, exit_reason()} | 
    {crash, DeadPid::comm:mypid()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, MyInterval}, State = 
       #ru_recon_state{ recon_stage = build_struct,
                        recon_method = bloom,
                        dhtNodePid = DhtNodePid }) ->
    send_chunk_req(DhtNodePid, self(), MyInterval, mapInterval(MyInterval, 1), get_max_items(bloom)),
    State#ru_recon_state{ recon_struct = [{interval, MyInterval}] };

on({get_state_response, MyInterval}, State = 
       #ru_recon_state{ recon_stage = req_shared_interval,
                        sync_master = true,
                        sync_round = Round,
                        recon_method = RMethod,
                        dhtNodePid = DhtNodePid,
                        ownerRemotePid = OwnerPid}) ->
    comm:send_local(DhtNodePid, 
                    {lookup_aux, select_sync_node(MyInterval), 0, 
                     {send_to_group_member, rep_upd,
                      {request_recon, OwnerPid, Round, false, 
                       req_shared_interval, RMethod, [{interval, MyInterval}]}}}),
    comm:send_local(self(), {shutdown, negotiate_interval_master}),
    State;

on({get_state_response, MyInterval}, State = 
       #ru_recon_state{ recon_stage = req_shared_interval,
                        sync_master = false,
                        recon_method = RMethod,
                        recon_struct = Params,                        
                        dhtNodePid = DhtNodePid,
                        dest_ru_pid = DestRUPid }) ->
    RMethod =:= merkle_tree andalso fd:subscribe(DestRUPid),
    SrcInterval = proplists:get_value(interval, Params),
    MyIMapped = mapInterval(MyInterval, 1),
    SrcIMapped = mapInterval(SrcInterval, 1),
    Intersection = intervals:intersection(MyIMapped, SrcIMapped),
    case intervals:is_empty(Intersection) of
        true ->
            ?TRACE("SYNC CLIENT - INTERSECTION EMPTY", []),
            comm:send_local(self(), {shutdown, intersection_empty});
        false ->
            MyIntersec = mapInterval_to_range(Intersection, MyInterval, 2),
            send_chunk_req(DhtNodePid, self(), MyIntersec, Intersection, get_max_items(RMethod))
    end,
    State#ru_recon_state{ recon_stage = build_struct,
                          recon_struct = [{interval, Intersection}] };

on({get_state_response, MyI}, State = 
       #ru_recon_state{ recon_stage = res_shared_interval,
                        recon_method = RMethod,
                        recon_struct = Params,
                        dhtNodePid = DhtNodePid,
                        dest_ru_pid = ClientRU_Pid}) ->
    Interval = proplists:get_value(interval, Params),
    ClientPid = proplists:get_value(senderPid, Params),
    MyIntersec = mapInterval(Interval, get_interval_quadrant(MyI)),
    case intervals:is_subset(MyIntersec, MyI) of
        false ->
            ?TRACE("SYNC MASTER - INTERSEC NOT IN MY I", []),
            comm:send_local(self(), {shutdown, negotiate_interval_master}),            
            comm:send(ClientPid, {shutdown, no_interval_intersection});
        true ->
            RMethod =:= merkle_tree andalso fd:subscribe(ClientRU_Pid),
            send_chunk_req(DhtNodePid, self(), MyIntersec, Interval, get_max_items(RMethod))
    end,    
    State#ru_recon_state{ recon_stage = build_struct, 
                          sync_pid = ClientPid };

on({get_state_response, MyI}, State = 
       #ru_recon_state{ recon_stage = reconciliation,
                        recon_method = bloom,
                        dhtNodePid = DhtNodePid,
                        recon_struct = #bloom_recon_struct{ interval = BloomI}
                       }) ->
    MyBloomI = mapInterval(BloomI, get_interval_quadrant(MyI)),
    MySyncI = intervals:intersection(MyI, MyBloomI),
    case intervals:is_empty(MySyncI) of
        true ->
            comm:send_local(self(), {shutdown, empty_interval});
        false ->
            send_chunk_req(DhtNodePid, self(), MySyncI, 
                           mapInterval(MySyncI, 1), get_max_items(bloom))
    end,
    State;

on({get_chunk_response, {_, []}}, State) ->
    comm:send_local(self(), {shutdown, req_chunk_is_empty}),
    State;

on({get_chunk_response, {RestI, DBList}}, State =
       #ru_recon_state{ recon_stage = build_struct,
                        recon_method = bloom,        
                        recon_struct = Params,                    
                        sync_round = Round,
                        dhtNodePid = DhtNodePid,
                        ownerRemotePid = MyRU_Pid,
                        sync_master = SyncMaster,
                        stats = Stats }) ->
    MyI = proplists:get_value(interval, Params),
    %Get Interval of DBList/Chunk
    %TODO: IMPROVEMENT getChunk should return ChunkInterval (db is traversed twice! - 1st getChunk, 2nd here)    
    ChunkI = mapInterval(MyI, 1),
    case intervals:is_empty(RestI) of
        false ->
            ?TRACE2("FORK RECON", []),
            {ok, Pid} = fork_recon(State, Round),
            send_chunk_req(DhtNodePid, Pid, RestI, 
                           mapInterval(RestI, get_interval_quadrant(ChunkI)), 
                           get_max_items(bloom));
        _ -> ok
    end,
    {BuildTime, SyncStruct} = 
        util:tc(fun() -> build_recon_struct(bloom, {ChunkI, DBList}, {get_fpr()}) end),
    case SyncMaster of
        true ->
            DestKey = select_sync_node(MyI),
            comm:send_local(DhtNodePid, 
                            {lookup_aux, DestKey, 0, 
                             {send_to_group_member, rep_upd, 
                              {request_recon, MyRU_Pid, Round, false, reconciliation, bloom, SyncStruct}}}),
            ?TRACE("BLOOM SEND RECON REQUEST TO Key: ~p", [DestKey]),
            comm:send_local(self(), {shutdown, {ok, build_struct}});
        _ -> ok
    end,
    State#ru_recon_state{ recon_struct = SyncStruct, 
                          stats = ru_recon_stats:set([{build_time, BuildTime}], Stats) };

on({get_chunk_response, {RestI, DBList}}, State = 
       #ru_recon_state{ recon_stage = build_struct,
                        recon_method = merkle_tree,
                        sync_pid = SrcPid,
                        recon_struct = Params,
                        sync_master = IsMaster,
                        dest_ru_pid = DestRU_Pid,
                        ownerRemotePid = OwnerPid,
                        dhtNodePid = DhtNodePid,
                        sync_round = Round,
                        stats = Stats })  ->
    {BuildTime, MerkleTree} = 
        case merkle_tree:is_merkle_tree(Params) of
            true -> %extend tree
                {BTime, NTree} = util:tc(fun() -> add_to_tree(DBList, Params) end),
                {ru_recon_stats:get(build_time, Stats) + BTime, merkle_tree:gen_hash(NTree) };
            false -> %build new tree
                Interval = proplists:get_value(interval, Params),
                util:tc(fun() -> build_recon_struct(merkle_tree, 
                                                    {Interval, DBList}, 
                                                    {}) 
                        end)
        end,
    case intervals:is_empty(RestI) of
        false ->
            ?TRACE("TREE EXISTS - ADD REST - RestI=~p", [RestI]),
            send_chunk_req(DhtNodePid, self(), RestI, RestI, get_max_items(merkle_tree)),%MappedI is wrong
            State#ru_recon_state{ recon_struct = MerkleTree, 
                                  stats = 
                                      ru_recon_stats:set([{build_time, BuildTime}], 
                                                         Stats) };        
        true ->
            case IsMaster of
                true ->
                    ?TRACE("MASTER SENDS ROOT CHECK", []),
                    comm:send(SrcPid, 
                              {check_node, comm:this(), 
                               merkle_tree:get_interval(MerkleTree), 
                               merkle_tree:get_hash(MerkleTree)});
                false -> 
                    comm:send(DestRU_Pid, 
                              {request_recon, OwnerPid, Round, true, 
                               res_shared_interval, merkle_tree,
                               [{interval, merkle_tree:get_interval(MerkleTree)},
                                {senderPid, comm:this()}] 
                              })
            end,
            NewStats = ru_recon_stats:set(
                         [{tree_compareLeft, ?IIF(IsMaster, 1, 0)},
                          {tree_size, merkle_tree:size_detail(MerkleTree)},
                          {build_time, BuildTime}], Stats), 
            State#ru_recon_state{ recon_stage = reconciliation, 
                                  recon_struct = MerkleTree, 
                                  stats = NewStats
                                }          
    end;

on({get_chunk_response, {[], DBList}}, State = 
       #ru_recon_state{ recon_stage = build_struct, 
                        recon_method = art,
                        recon_struct = Params,
                        sync_master = IsMaster,
                        ownerRemotePid = OwnerPid,
                        dest_ru_pid = DestRU_Pid,
                        sync_round = Round,
                        stats = Stats })  ->
    ToBuild = ?IIF(IsMaster, merkle_tree, art),
    Interval = proplists:get_value(interval, Params),
    {BuildTime, NewStruct} =
        util:tc(fun() -> build_recon_struct(ToBuild, {Interval, DBList}, {}) end),    
    NewStats = 
        case IsMaster of
            true -> 
                %TODO measure sync time
                {ok, ARStats} = 
                    art_recon(NewStruct, proplists:get_value(art, Params), State),
                comm:send_local(self(), {shutdown, sync_finished}),
                ru_recon_stats:set([{build_time, BuildTime},
                                    {tree_size, merkle_tree:size_detail(NewStruct)}], 
                                   ARStats);
            false ->
                comm:send(DestRU_Pid, 
                          {request_recon, OwnerPid, Round, true, 
                           res_shared_interval, art,
                           [{interval, art:get_interval(NewStruct)},
                            {senderPid, comm:this()},
                            {art, NewStruct}]
                          }),
                comm:send_local(self(), {shutdown, client_art_send}),
                ru_recon_stats:set([{build_time, BuildTime}], Stats)
        end,    
    State#ru_recon_state{ recon_stage = reconciliation, 
                          recon_struct = NewStruct, 
                          stats = NewStats };
    
on({get_chunk_response, {RestI, DBList}}, State = 
       #ru_recon_state{ recon_stage = reconciliation,
                        recon_method = bloom,
                        dhtNodePid = DhtNodePid,
                        ownerLocalPid = Owner,
                        dest_ru_pid = DestRU_Pid,
                        recon_struct = #bloom_recon_struct{ interval = BloomI, 
                                                            keyBF = KeyBF,
                                                            versBF = VersBF},
                        sync_round = Round }) ->
    %if rest interval is non empty start another sync    
    SyncFinished = intervals:is_empty(RestI),
    not SyncFinished andalso
        send_chunk_req(DhtNodePid, self(), RestI, 
                       mapInterval(RestI, get_interval_quadrant(BloomI)), 
                       get_max_items(bloom)),
    {Obsolete, Missing} = 
        filterPartitionMap(fun(Filter) -> 
                                   not ?REP_BLOOM:is_element(VersBF, Filter) 
                           end,
                           fun(Partition) -> 
                                   {MinKey, _} = decodeBlob(Partition),
                                   ?REP_BLOOM:is_element(KeyBF, MinKey)
                           end,
                           fun(Map) -> 
                                   {K, _} = decodeBlob(Map), 
                                   K 
                           end,
                           DBList),
    ?TRACE("Reconcile Bloom Round=~p ; Obsolete=~p ; Missing=~p", [Round, length(Obsolete), length(Missing)]),
    length(Obsolete) > 0 andalso
        comm:send_local(Owner, {request_resolve, Round, {key_sync, DestRU_Pid, Obsolete}, []}),
    length(Missing) > 0 andalso
        comm:send_local(Owner, {request_resolve, Round, {key_sync, DestRU_Pid, Missing}, []}),
    SyncFinished andalso        
        comm:send_local(self(), {shutdown, sync_finished}),
    State;
    
on({start_recon, ReconMethod, ReconStage, ReconStruct, Master}, State) ->
    comm:send_local(State#ru_recon_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#ru_recon_state{ recon_stage = ReconStage, 
                          recon_struct = ReconStruct,
                          recon_method = ReconMethod,
                          sync_master = Master orelse ReconStage =:= res_shared_interval };

on({crash, Pid}, State) ->
    comm:send_local(self(), {shutdown, {fail, crash_of_recon_node, Pid}}),
    State;

on({shutdown, Reason}, #ru_recon_state{ ownerLocalPid = Owner, 
                                        sync_round = Round,
                                        stats = Stats,
                                        sync_master = Master,
                                        sync_start_time = SyncStartTime }) ->
    ?TRACE("SHUTDOWN Round=~p Reason=~p", [Round, Reason]),
    NewStats = ru_recon_stats:set([{recon_time, timer:now_diff(erlang:now(), SyncStartTime)},
                                   {finish, Reason =:= sync_finished}], Stats),
    comm:send_local(Owner, 
                    {recon_progress_report, self(), Round, Master, NewStats}),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({check_node, SrcPid, Interval, Hash}, State =
       #ru_recon_state{ recon_struct = Tree }) ->
    Node = merkle_tree:lookup(Interval, Tree),
    case Node of
        not_found ->
            comm:send(SrcPid, {check_node_response, not_found, Interval, []});
        _ ->
            IsLeaf = merkle_tree:is_leaf(Node),
            {Result, ChildHashs} = case merkle_tree:get_hash(Node) =:= Hash of
                                       true when IsLeaf -> {ok_leaf, []};
                                       true when not IsLeaf -> {ok_inner, []};
                                       false when IsLeaf -> {fail_leaf, []};
                                       false when not IsLeaf ->
                                           {fail_inner, 
                                            [merkle_tree:get_hash(N) || N <- merkle_tree:get_childs(Node)]}
                                   end,
            comm:send(SrcPid, {check_node_response, Result, Interval, ChildHashs})
    end,
    State#ru_recon_state{ sync_pid = SrcPid };

on({check_node_response, Result, I, ChildHashs}, State = 
       #ru_recon_state{ sync_pid = SyncDest,
                        dest_ru_pid = SrcNode,
                        stats = Stats, 
                        ownerLocalPid = OwnerPid,
                        recon_struct = Tree,
                        sync_round = Round }) ->
    Node = merkle_tree:lookup(I, Tree),
    IncOps = 
        case Result of
            not_found -> [{error_count, 1}]; 
            ok_leaf -> [{tree_compareSkipped, 1}];
            ok_inner -> [{tree_compareSkipped, merkle_tree:size(Node)}];
            fail_leaf ->
                Leafs = reconcileNode(Node, {SrcNode, Round, OwnerPid}),
                [{tree_leafsSynced, Leafs}];               
            fail_inner ->
                MyChilds = merkle_tree:get_childs(Node),
                {Matched, NotMatched} = compareNodes(MyChilds, ChildHashs, {[], []}),
                SkippedSubNodes = 
                    lists:foldl(fun(MNode, Acc) -> 
                                        Acc + case merkle_tree:is_leaf(MNode) of
                                                  true -> 0;
                                                  false -> merkle_tree:size(MNode) - 1
                                              end
                                end, 0, Matched),
                lists:foreach(fun(X) -> 
                                      comm:send(SyncDest, 
                                                {check_node,
                                                 comm:this(), 
                                                 merkle_tree:get_interval(X), 
                                                 merkle_tree:get_hash(X)}) 
                              end, NotMatched),
                [{tree_compareLeft, length(NotMatched)},
                 {tree_nodesCompared, length(Matched)},
                 {tree_compareSkipped, SkippedSubNodes}]    
        end,
    FinalStats = ru_recon_stats:inc(IncOps ++ [{tree_compareLeft, -1}, 
                                               {tree_nodesCompared, 1}], Stats),
    CompLeft = ru_recon_stats:get(tree_compareLeft, FinalStats),
    if CompLeft =< 1 ->
           comm:send(SyncDest, {shutdown, sync_finished_remote_shutdown}),
           comm:send_local(self(), {shutdown, sync_finished});
       true -> ok
    end,
    State#ru_recon_state{ stats = FinalStats }.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec add_to_tree(?DB:db_as_list(), merkle_tree:merkle_tree()) -> merkle_tree:merkle_tree().
add_to_tree(DBItems, MTree) ->
    lists:foldl(fun(DBEntryEnc, Tree) -> merkle_tree:insert(DBEntryEnc, Tree) end, 
                MTree, DBItems).

-spec compareNodes([merkle_tree:mt_node()], 
                   [merkle_tree:mt_node_key()], 
                   {Matched::[merkle_tree:mt_node()], NotMatched::[merkle_tree:mt_node()]}) -> 
          {Matched::[merkle_tree:mt_node()], NotMatched::[merkle_tree:mt_node()]}.
compareNodes([], [], Acc) -> Acc;
compareNodes([N | Nodes], [H | NodeHashs], {Matched, NotMatched}) ->
    Hash = merkle_tree:get_hash(N),
    case Hash =:= H of
        true -> compareNodes(Nodes, NodeHashs, {[N|Matched], NotMatched});
        false -> compareNodes(Nodes, NodeHashs, {Matched, [N|NotMatched]})
    end.

% @doc Starts simple sync for a given node, returns number of leaf sync requests.
-spec reconcileNode(merkle_tree:mt_node() | not_found, 
                    {comm:mypid() | undefined, rep_upd:round(), comm:mypid() | undefined}) -> non_neg_integer().
reconcileNode(not_found, _) -> 0;
reconcileNode(Node, Conf) ->
    case merkle_tree:is_leaf(Node) of
        true -> reconcileLeaf(Node, Conf);
        false -> lists:foldl(fun(X, Acc) -> Acc + reconcileNode(X, Conf) end, 
                             0, merkle_tree:get_childs(Node))
    end.

-spec reconcileLeaf(merkle_tree:mt_node(), 
                    {comm:mypid() | undefined, rep_upd:round(), comm:mypid() | undefined}) -> 1.
reconcileLeaf(_, {undefined, _, _}) -> erlang:error("Recon Destination PID undefined");
reconcileLeaf(_, {_, _, undefined}) -> erlang:error("Recon Owner PID undefined");
reconcileLeaf(Node, {Dest, Round, Owner}) ->
    ToSync = lists:map(fun(KeyVer) -> 
                           case decodeBlob(KeyVer) of
                               {K, _} -> K;
                               _ -> KeyVer
                            end
                       end, 
                       merkle_tree:get_bucket(Node)),
    comm:send_local(Owner, {request_resolve, Round, {key_sync, Dest, ToSync}, []}),
    1.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% art recon
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec art_recon(MyTree, Art, State) -> {ok, Stats} when
    is_subtype(MyTree, merkle_tree:merkle_tree()),
    is_subtype(Art,    art:art()),
    is_subtype(State,  state()),
    is_subtype(Stats,  ru_recon_stats:stats()).
art_recon(Tree, Art, #ru_recon_state{ sync_round = Round, 
                                      dest_ru_pid = DestPid,
                                      ownerLocalPid = OwnerPid,
                                      stats = Stats }) ->
    case merkle_tree:get_interval(Tree) =:= art:get_interval(Art) of
        true -> 
            {NodesToSync, NStats} = 
                art_get_sync_leafs([merkle_tree:get_root(Tree)], Art, Stats, []),
            _ = [reconcileLeaf(X, {DestPid, Round, OwnerPid}) || X <- NodesToSync],
            {ok, NStats};
        false -> {ok, Stats}
    end.

-spec art_get_sync_leafs(Nodes, Art, Stats, Acc) -> {ToSync, Stats} when
    is_subtype(Nodes,  [merkle_tree:mt_node()]),
    is_subtype(Art,    art:art()),
    is_subtype(Stats,  ru_recon_stats:stats()),
    is_subtype(Acc,    [merkle_tree:mt_node()]),
    is_subtype(ToSync, [merkle_tree:mt_node()]),
    is_subtype(Stats,  ru_recon_stats:stats()).
art_get_sync_leafs([], _Art, Stats, ToSyncAcc) ->
    {ToSyncAcc, Stats};
art_get_sync_leafs([Node | ToCheck], Art, OStats, ToSyncAcc) ->
    Stats = ru_recon_stats:inc([{tree_nodesCompared, 1}], OStats),
    IsLeaf = merkle_tree:is_leaf(Node),
    case art:lookup(Node, Art) of
        true ->
            NStats = ru_recon_stats:inc([{tree_compareSkipped, ?IIF(IsLeaf, 0, merkle_tree:size(Node))}], 
                                         Stats),
            art_get_sync_leafs(ToCheck, Art, NStats, ToSyncAcc);
        false ->
            case IsLeaf of
                true ->
                    NStats = ru_recon_stats:inc([{tree_leafsSynced, 1}], Stats),
                    art_get_sync_leafs(ToCheck, Art, NStats, [Node | ToSyncAcc]);
                false ->
                    art_get_sync_leafs(
                           lists:append(merkle_tree:get_childs(Node), ToCheck), 
                           Art, Stats, ToSyncAcc)
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_recon_struct(Method, DB_Chunk, Args) -> Recon_Struct when
      is_subtype(Method,       method()),
      is_subtype(DB_Chunk,     {intervals:interval(), db_as_list_enc()}),
      is_subtype(Args,         build_args()),
      is_subtype(Recon_Struct, bloom_recon_struct() | merkle_tree:merkle_tree() | art:art()).

build_recon_struct(bloom, {I, DBItems}, {Fpr}) ->
    ElementNum = length(DBItems),
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?REP_HFS:new(HFCount),    
    {KeyBF, VerBF} = fill_bloom(DBItems, 
                                ?REP_BLOOM:new(ElementNum, Fpr, Hfs), 
                                ?REP_BLOOM:new(ElementNum, Fpr, Hfs)),
    #bloom_recon_struct{ interval = I, keyBF = KeyBF, versBF = VerBF };

build_recon_struct(merkle_tree, {I, DBItems}, _) ->
    Tree = merkle_tree:bulk_build(I, DBItems),
    merkle_tree:gen_hash(Tree);

build_recon_struct(art, Chunk, _) ->
    Tree = build_recon_struct(merkle_tree, Chunk, {}),
    art:new(Tree).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% bloom_filter specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Create two bloom filter of a given database chunk.
%%      One over all keys and one over all keys concatenated with their version.
-spec fill_bloom(DB, Key::Bloom, Version::Bloom) -> {Key2::Bloom, Version2::Bloom} when
      is_subtype(DB,    db_as_list_enc()),
      is_subtype(Bloom, ?REP_BLOOM:bloom_filter()).
fill_bloom([], KeyBF, VerBF) ->
    {KeyBF, VerBF};
fill_bloom([DB_Entry_Enc | T], KeyBF, VerBF) ->
    {KeyOnly, _} = decodeBlob(DB_Entry_Enc),
    fill_bloom(T, 
               ?REP_BLOOM:add(KeyBF, KeyOnly), 
               ?REP_BLOOM:add(VerBF, DB_Entry_Enc)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Sends get_chunk request to local DHT_node process.
%      Request responses a list of encoded key-version pairs in ChunkI, 
%      where key is mapped to its assosiated key in MappedI.
-spec send_chunk_req(DhtPid, AnswerPid, ChunkI, DestI, MaxItems) -> ok when
    is_subtype(DhtPid,    comm:erl_local_pid()),
    is_subtype(AnswerPid, comm:erl_local_pid()),
    is_subtype(ChunkI,    intervals:interval()),
    is_subtype(DestI,     intervals:interval()),
    is_subtype(MaxItems,  pos_integer()).
send_chunk_req(DhtPid, SrcPid, I, DestI, MaxItems) ->
    comm:send_local(
      DhtPid, 
      {get_chunk, SrcPid, I, 
       fun(Item) -> db_entry:get_version(Item) =/= -1 end,
       fun(Item) -> 
               encodeBlob(minKeyInInterval(db_entry:get_key(Item), DestI), 
                          db_entry:get_version(Item)) 
       end,
       MaxItems}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @ doc filter, partition and map items of a list in one run
-spec filterPartitionMap(Filter::Fun, Partition::Fun, Map::Fun, List) -> {True::List, False::List} when
     is_subtype(Fun,  fun((A) -> boolean())),
     is_subtype(List, [A]).                                                                                                     
filterPartitionMap(Filter, Pred, Map, List) ->
    filterPartitionMap(Filter, Pred, Map, List, [], []).

filterPartitionMap(_, _, _, [], TrueL, FalseL) ->
    {TrueL, FalseL};
filterPartitionMap(Filter, Pred, Map, [H | T], TrueL, FalseL) ->
    {Satis, NonSatis} = 
        case Filter(H) of
            true -> case Pred(H) of
                        true -> {[Map(H) | TrueL], FalseL};
                        false -> {TrueL, [Map(H) | FalseL]}
                    end;        
            false -> {TrueL, FalseL}
        end,
    filterPartitionMap(Filter, Pred, Map, T, Satis, NonSatis).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc selects a random associated key of an interval ending
-spec select_sync_node(intervals:interval()) -> ?RT:key().
select_sync_node(Interval) ->
    {_, _LKey, RKey, _RBr} = intervals:get_bounds(Interval),
    %Key = ?RT:get_split_key(_LKey, RKey, {1, randoms:rand_uniform(1, 10)}), %TODO    
    Key = case _RBr of
              ')' -> RKey - 1;
              ']' -> RKey
          end,
    Keys = lists:delete(Key, ?RT:get_replica_keys(Key)),
    util:randomelem(Keys).

-spec minKeyInInterval(?RT:key(), intervals:interval()) -> ?RT:key().
minKeyInInterval(Key, I) ->
    erlang:hd([K || K <- lists:sort(?RT:get_replica_keys(Key)), intervals:in(K, I)]).

-spec map_key_to_quadrant(?RT:key(), pos_integer()) -> ?RT:key().
map_key_to_quadrant(Key, N) ->
    lists:nth(N, lists:sort(?RT:get_replica_keys(Key))).

-spec get_key_quadrant(?RT:key()) -> pos_integer().
get_key_quadrant(Key) ->
    Keys = lists:sort(?RT:get_replica_keys(Key)),
    {_, Q} = lists:foldl(fun(X, {Status, Nr} = Acc) ->
                                     case X =:= Key of
                                         true when Status =:= no -> {yes, Nr};
                                         false when Status =/= yes -> {no, Nr + 1};
                                         _ -> Acc
                                     end
                             end, {no, 1}, Keys),
    Q.

% @doc Returns the quadrant in which a given interval begins.
-spec get_interval_quadrant(intervals:interval()) -> pos_integer().
get_interval_quadrant(I) ->
    {_, LKey, _, _} = intervals:get_bounds(I),
    get_key_quadrant(LKey).        

-spec add_quadrants_to_key(?RT:key(), non_neg_integer(), pos_integer()) -> ?RT:key().
add_quadrants_to_key(Key, Add, RepFactor) ->
    Dest = get_key_quadrant(Key) + Add,
    Rep = RepFactor + 1,
    case Dest div Rep of
        1 -> map_key_to_quadrant(Key, (Dest rem Rep) + 1);
        0 -> map_key_to_quadrant(Key, Dest)
    end.            

% @doc Maps an arbitrary Interval to an Interval laying or starting in 
%      the given RepQuadrant. The replication degree of X divides the keyspace into X replication qudrants.
%      Interval has to be continuous!
-spec mapInterval(intervals:interval(), RepQuadrant::pos_integer()) -> intervals:interval().
mapInterval(I, Q) ->
    {LBr, LKey, RKey, RBr} = intervals:get_bounds(I),
    LQ = get_key_quadrant(LKey),
    RepFactor = length(?RT:get_replica_keys(LKey)),
    QDiff = (RepFactor - LQ + Q) rem RepFactor,
    intervals:new(LBr, 
                  add_quadrants_to_key(LKey, QDiff, RepFactor), 
                  add_quadrants_to_key(RKey, QDiff, RepFactor), 
                  RBr).

% @doc Tries to map SrcI into a destination Interval (DestRange)
-spec mapInterval_to_range(intervals:interval(), intervals:interval(), pos_integer()) -> intervals:interval() | error.
mapInterval_to_range(I, I, _) ->
    I;
mapInterval_to_range(SrcI, DestRange, Q) ->
    {_, LKey, _, _} = intervals:get_bounds(SrcI),
    RepFactor = length(?RT:get_replica_keys(LKey)),
    case Q > RepFactor of
        true -> error;
        false -> mapInterval_to_range(mapInterval(SrcI, Q), DestRange, Q + 1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec encodeBlob(?DB:version() | ?RT:key(), ?DB:value() | ?DB:version()) -> db_entry_enc().
encodeBlob(A, B) -> 
    term_to_binary([A, "#", B]).

-spec decodeBlob(db_entry_enc()) -> {?DB:version() | ?RT:key(), ?DB:value() | ?DB:version()} | fail.
decodeBlob(Blob) when is_binary(Blob) ->
    L = binary_to_term(Blob),
    case length(L) of
        3 -> {hd(L), lists:last(L)};
        _ -> fail
    end;
decodeBlob(_) -> fail.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc init module
-spec init(state()) -> state().
init(State) ->
    State#ru_recon_state{ sync_start_time = erlang:now() }.

-spec start(Round, Sender_RU_Pid) -> {ok, MyPid} when
      is_subtype(Round,         rep_upd:round()),
      is_subtype(Sender_RU_Pid, comm:mypid() | undefined),
      is_subtype(MyPid,         pid()).

start(Round, SenderRUPid) ->
    State = #ru_recon_state{ ownerLocalPid = self(), 
                             ownerRemotePid = comm:this(), 
                             dhtNodePid = pid_groups:get_my(dht_node),
                             dest_ru_pid = SenderRUPid,
                             sync_round = Round },
    gen_component:start(?MODULE, fun ?MODULE:on/2, State).

-spec fork_recon(state(), rep_upd:round()) -> {ok, pid()}.
fork_recon(Conf, {ReconRound, Fork}) ->
    State = Conf#ru_recon_state{ sync_round = {ReconRound, Fork} },
    comm:send_local(Conf#ru_recon_state.ownerLocalPid, {recon_forked}),
    gen_component:start(?MODULE, fun ?MODULE:on/2, State).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rep_update_activate) of
        true ->                
            config:cfg_is_float(rep_update_recon_fpr) andalso
            config:cfg_is_greater_than(rep_update_recon_fpr, 0) andalso
            config:cfg_is_less_than(rep_update_recon_fpr, 1) andalso
            config:cfg_is_integer(rep_update_max_items) andalso
            config:cfg_is_greater_than(rep_update_max_items, 0);
        _ -> true
    end.

-spec get_fpr() -> float().
get_fpr() ->
    config:read(rep_update_recon_fpr).

-spec get_max_items(method()) -> pos_integer().
get_max_items(ReconMethod) ->
    case ReconMethod of
        merkle_tree -> all;
        art -> all;
        _ -> config:read(rep_update_max_items)
    end.
