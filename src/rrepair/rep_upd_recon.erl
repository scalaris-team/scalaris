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
-export([print_recon_stats/1]).

%export for testing
-export([encodeBlob/2, decodeBlob/1, 
         mapInterval/2, map_key_to_quadrant/2, 
         get_key_quadrant/1, get_interval_quadrant/1]).

-ifdef(with_export_type_support).
-export_type([recon_method/0, recon_struct/0, recon_stage/0, keyValVers/0]).
-export_type([ru_recon_stats/0]).
-endif.


-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
%-define(TRACE(X,Y), ok).

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

-type recon_method() :: bloom | merkle_tree | art | undefined.
-type recon_stage()  :: req_shared_interval | res_shared_interval | build_struct | reconciliation.
-type keyValVers()   :: {?RT:key(), ?DB:value(), ?DB:version()}.
-type exit_reason()  :: empty_interval | {ok, atom()}.

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

-record(ru_recon_stats,
        {
         tree_size          = {0, 0} :: merkle_tree:mt_size(),
         tree_compareLeft   = 0      :: non_neg_integer(),
         tree_nodesCompared = 0      :: non_neg_integer(),
         tree_leafsSynced   = 0      :: non_neg_integer(),
         tree_compareSkipped= 0      :: non_neg_integer(),
         error_count        = 0      :: non_neg_integer(),
         build_time         = 0      :: non_neg_integer(),   %in us
         recon_time         = 0      :: non_neg_integer()    %in us
         }). 
-type ru_recon_stats() :: #ru_recon_stats{}.

-record(ru_recon_state,
        {
         ownerLocalPid      = ?required(ru_recon_state, ownerLocalPid)  :: comm:erl_local_pid(),
         ownerRemotePid     = ?required(ru_recon_state, ownerRemotePid) :: comm:mypid(),
         dhtNodePid         = ?required(ru_recon_state, dhtNodePid)     :: comm:erl_local_pid(),
         dest_ru_pid        = undefined                                 :: comm:mypid() | undefined,
         recon_method       = undefined                                 :: recon_method(),   %determines the build sync struct
         recon_struct       = {}                                        :: recon_struct() | {},
         recon_stage        = reconciliation                            :: recon_stage(),
         sync_pid           = undefined                                 :: comm:mypid() | undefined,%sync dest process
         sync_master        = false                                     :: boolean(),               %true if process is sync leader
         sync_round         = 0                                         :: float(),
         recon_stats        = #ru_recon_stats{}                         :: ru_recon_stats(),
         sync_start_time    = {0, 0, 0}                                 :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
         }).
-type state() :: #ru_recon_state{}.

-type build_args() :: {} | {Fpr::float()}.

-type message() ::
    {get_state_response, intervals:interval()} |
    {get_chunk_response, rep_upd:db_chunk()} |
    {check_node, MasterPid::comm:mypid(), intervals:interval(), merkle_tree:mt_node_key()} |
    {check_node_response, ok | not_found | fail_is_leaf | fail_node, intervals:interval(), [merkle_tree:mt_node_key()]} |
    {start_recon, recon_method(), recon_stage(), recon_struct(), Master::boolean()} |
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
    comm:send_local(DhtNodePid, {get_chunk, self(), MyInterval, get_max_items(bloom)}),
    State;

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
    SrcInterval = util:proplist_get_value(interval, Params),
    MyIMapped = mapInterval(MyInterval, 1),
    SrcIMapped = mapInterval(SrcInterval, 1),
    Intersection = intervals:intersection(MyIMapped, SrcIMapped),
    ?TRACE2("REQ SHARED I FROM~nMyI=[~p] -~nMyIMapped=[~p]~nSrcI=[~p]~nSrcIMapped=~p~nIntersec=~p", 
           [MyInterval, MyIMapped, SrcInterval, SrcIMapped, Intersection]),
    case intervals:is_empty(Intersection) of
        true ->
            ?TRACE("SYNC CLIENT - INTERSECTION EMPTY", []),
            comm:send_local(self(), {shutdown, intersection_empty});
        false ->
            %comm:send_local(DhtNodePid, 
            %                {get_chunk, self(), Intersection, get_max_items(RMethod)}) %only one merkle sync if active
            comm:send_local(DhtNodePid, 
                            {get_chunk, self(), 
                             mapInterval_to_range(Intersection, MyInterval, 2), 
                             get_max_items(RMethod)})
    end,
    State#ru_recon_state{ recon_stage = build_struct,
                          recon_struct = [{interval, Intersection}] };

on({get_state_response, MyInterval}, State = 
       #ru_recon_state{ recon_stage = res_shared_interval,
                        recon_method = RMethod,
                        recon_struct = Params,
                        dhtNodePid = DhtNodePid,
                        dest_ru_pid = ClientRU_Pid}) ->
    Interval = util:proplist_get_value(interval, Params),
    ClientPid = util:proplist_get_value(senderPid, Params),
    MappedIntersec = mapInterval(Interval, get_interval_quadrant(MyInterval)),
    case intervals:is_subset(MappedIntersec, MyInterval) of
        false ->
            ?TRACE("SYNC MASTER - INTERSEC NOT IN MY I", []),
            comm:send_local(self(), {shutdown, negotiate_interval_master}),            
            comm:send(ClientPid, {shutdown, no_interval_intersection});
        true ->
            RMethod =:= merkle_tree andalso fd:subscribe(ClientRU_Pid),
            comm:send_local(DhtNodePid, 
                            {get_chunk, self(), MappedIntersec, get_max_items(RMethod)})
    end,    
    State#ru_recon_state{ recon_stage = build_struct, 
                          sync_pid = ClientPid };

on({get_state_response, NodeDBInterval}, State = 
       #ru_recon_state{ recon_stage = reconciliation,
                        recon_method = bloom,
                        dhtNodePid = DhtNodePid,
                        recon_struct = #bloom_recon_struct{ interval = BloomI}
                       }) ->
    SyncInterval = intervals:intersection(NodeDBInterval, BloomI),
    case intervals:is_empty(SyncInterval) of
        true ->
            comm:send_local(self(), {shutdown, empty_interval});
        false ->
            comm:send_local(DhtNodePid, {get_chunk, self(), SyncInterval, get_max_items(bloom)})
    end,
    State;

on({get_chunk_response, {_, []}}, State) ->
    comm:send_local(self(), {shutdown, req_chunk_is_empty}),
    State;

on({get_chunk_response, {RestI, [First | T] = DBList}}, State =
       #ru_recon_state{ recon_stage = build_struct,
                        recon_method = bloom,                            
                        sync_round = Round,
                        dhtNodePid = DhtNodePid,
                        ownerRemotePid = MyRU_Pid,
                        sync_master = SyncMaster,
                        recon_stats = Stats }) ->
    case intervals:is_empty(RestI) of
        false ->
            ?TRACE2("FORK RECON", []),
            {ok, Pid} = fork_recon(State, Round + 0.1),
            comm:send_local(DhtNodePid, {get_chunk, Pid, RestI, get_max_items(bloom)});
        _ -> ok
    end,
    %Get Interval of DBList
    %TODO: IMPROVEMENT getChunk should return ChunkInterval (db is traversed twice! - 1st getChunk, 2nd here)
    ChunkI = intervals:new('[', db_entry:get_key(First), db_entry:get_key(lists:last(T)), ']'),
    ?TRACE2("RECV CHUNK interval= ~p  - RestInterval= ~p - DBLength=~p", [ChunkI, RestI, length(DBList)]),
    {BuildTime, SyncStruct} = 
        util:tc(fun() -> build_recon_struct(bloom, {ChunkI, DBList}, {get_fpr()}) end),
    case SyncMaster of
        true ->
            ?TRACE("SEND BLOOM - time=~p", [now()]),
            DestKey = select_sync_node(ChunkI),
            comm:send_local(DhtNodePid, 
                            {lookup_aux, DestKey, 0, 
                             {send_to_group_member, rep_upd, 
                              {request_recon, MyRU_Pid, Round, false, reconciliation, bloom, SyncStruct}}}),
            comm:send_local(self(), {shutdown, {ok, build_struct}});
        _ -> ok
    end,
    State#ru_recon_state{ recon_struct = SyncStruct, 
                          recon_stats = Stats#ru_recon_stats{ build_time = BuildTime } };

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
                        recon_stats = Stats })  ->    
    {BuildTime, MerkleTree} = 
        case merkle_tree:is_merkle_tree(Params) of
            true -> %extend tree
                {BTime, NTree} = 
                    util:tc(fun() -> add_to_tree(DBList, Params) end),
                {Stats#ru_recon_stats.build_time + BTime, merkle_tree:gen_hash(NTree) };
            false -> %build new tree
                Interval = util:proplist_get_value(interval, Params),
                util:tc(fun() -> build_recon_struct(merkle_tree, 
                                                    {Interval, DBList}, 
                                                    {}) 
                        end)
        end,
    case intervals:is_empty(RestI) of
        false ->
            ?TRACE("TREE EXISTS - ADD REST - RestI=~p", [RestI]),
            comm:send_local(DhtNodePid, {get_chunk, self(), RestI, get_max_items(merkle_tree)}),
            State#ru_recon_state{ recon_struct = MerkleTree, 
                                  recon_stats = Stats#ru_recon_stats{ build_time = BuildTime } };        
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
            NewStats = Stats#ru_recon_stats{ tree_compareLeft = ?IIF(IsMaster, 1, 0),
                                             tree_size = merkle_tree:size_detail(MerkleTree),
                                             build_time = BuildTime
                                           }, 
            State#ru_recon_state{ recon_stage = reconciliation, 
                                  recon_struct = MerkleTree, 
                                  recon_stats = NewStats
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
                        recon_stats = Stats })  ->
    ToBuild = ?IIF(IsMaster, merkle_tree, art),
    Interval = util:proplist_get_value(interval, Params),
    {BuildTime, NewStruct} =
        util:tc(fun() -> build_recon_struct(ToBuild, {Interval, DBList}, {}) end),    
    NewStats = 
        case IsMaster of
            true -> 
                %TODO measure sync time
                {ok, ReconStats} = 
                    art_recon(NewStruct, util:proplist_get_value(art, Params), State),
                comm:send_local(self(), {shutdown, art_sync_finished}),
                Stats#ru_recon_stats { build_time = BuildTime,
                                       tree_size = merkle_tree:size_detail(NewStruct),
                                       tree_leafsSynced = ReconStats#ru_recon_stats.tree_leafsSynced };                
            false ->
                comm:send(DestRU_Pid, 
                          {request_recon, OwnerPid, Round, true, 
                           res_shared_interval, art,
                           [{interval, art:get_interval(NewStruct)},
                            {senderPid, comm:this()},
                            {art, NewStruct}]
                          }),
                comm:send_local(self(), {shutdown, client_art_send}),
                Stats#ru_recon_stats{ build_time = BuildTime }
        end,    
    State#ru_recon_state{ recon_stage = reconciliation, 
                          recon_struct = NewStruct, 
                          recon_stats = NewStats };
    
on({get_chunk_response, {RestI, DBList}}, State = 
       #ru_recon_state{ recon_stage = reconciliation,
                        recon_method = bloom,
                        dhtNodePid = DhtNodePid,                        
                        ownerRemotePid = OwnerPid,
                        dest_ru_pid = DestRU_Pid,
                        recon_struct = #bloom_recon_struct{ keyBF = KeyBF,
                                                            versBF = VersBF},
                        sync_master = Master,
                        sync_round = Round }) ->
    ?TRACE2("GetChunk Res - Recon Bloom Round=~p", [Round]),
    %if rest interval is non empty start another sync    
    SyncFinished = intervals:is_empty(RestI),
    not SyncFinished andalso
        comm:send_local(DhtNodePid, {get_chunk, self(), RestI, get_max_items(bloom)}),
    %set reconciliation
    {Obsolete, _Missing} = 
        filterPartitionMap(fun(A) -> 
                                   db_entry:get_version(A) > -1 andalso
                                       not ?REP_BLOOM:is_element(VersBF, concatKeyVer(A)) 
                           end,
                           fun(B) -> 
                                   ?REP_BLOOM:is_element(KeyBF, minKey(db_entry:get_key(B)))
                           end,
                           fun(C) ->
                                   { minKey(db_entry:get_key(C)), 
                                     db_entry:get_value(C), 
                                     db_entry:get_version(C) }
                           end,
                           DBList),
    %TODO: Possibility to resolve missing replicas
    length(Obsolete) > 0 andalso
        comm:send(DestRU_Pid, {request_resolve, Round, simple, {simple_detail_sync, Obsolete},
                               {Master andalso get_do_feedback(), OwnerPid}, 
                               {false, OwnerPid}}),
    SyncFinished andalso
        comm:send_local(self(), {shutdown, {ok, reconciliation}}),
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
                                        recon_stats = Stats,
                                        sync_master = Master,
                                        sync_start_time = SyncStartTime }) ->
    ?TRACE("SHUTDOWN Round=~p Reason=~p", [Round, Reason]),
    NewStats = Stats#ru_recon_stats{ recon_time = timer:now_diff(erlang:now(), SyncStartTime) },
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
            %?TRACE2("COMPARE RECV{I=[~p] Hash=[~p]} with MY {I=[~p] Hash=[~p]}", 
            %       [Interval, Hash, merkle_tree:get_interval(Node), merkle_tree:get_hash(Node)]),
            case merkle_tree:get_hash(Node) =:= Hash of
                true ->
                    comm:send(SrcPid, {check_node_response, ok, Interval, []});
                false when IsLeaf -> 
                    comm:send(SrcPid, {check_node_response, fail_is_leaf, Interval, []});
                false when not IsLeaf ->
                    ChildHashs = lists:map(fun(N) ->
                                                   merkle_tree:get_hash(N)
                                           end, 
                                           merkle_tree:get_childs(Node)),
                    comm:send(SrcPid, {check_node_response, fail_node, Interval, ChildHashs})
            end
    end,
    State#ru_recon_state{ sync_pid = SrcPid };

on({check_node_response, Result, I, ChildHashs}, State = 
       #ru_recon_state{ sync_pid = SyncDest,
                        dest_ru_pid = SrcNode,
                        recon_stats = 
                            #ru_recon_stats{ error_count = Errors,
                                             tree_leafsSynced = LeafCount,
                                             tree_compareSkipped = Skipped
                                           } = Stats, 
                        ownerRemotePid = OwnerPid,
                        recon_struct = Tree,
                        sync_round = Round }) ->
    Node = merkle_tree:lookup(I, Tree),
    NewStats = Stats#ru_recon_stats{ 
                    tree_compareLeft = Stats#ru_recon_stats.tree_compareLeft - 1,
                    tree_nodesCompared = Stats#ru_recon_stats.tree_nodesCompared + 1 },
    FinalStats = 
        case Result of
            not_found -> NewStats#ru_recon_stats{ error_count = Errors + 1 };
            ok -> NewStats#ru_recon_stats{ tree_compareSkipped = Skipped + merkle_tree:size(Node) };
            fail_is_leaf ->
                Leafs = reconcileNode(Node, {SrcNode, Round, OwnerPid}),
                NewStats#ru_recon_stats{ tree_leafsSynced = LeafCount + Leafs };               
            fail_node ->
                MyChilds = merkle_tree:get_childs(Node),
                {Matched, NotMatched} = compareNodes(MyChilds, ChildHashs, {[], []}),
                SkippedSubNodes = lists:foldl(fun(MNode, Acc) -> 
                                                      Acc + case merkle_tree:is_leaf(MNode) of
                                                                true -> 0;
                                                                false -> merkle_tree:size(MNode)
                                                            end
                                              end, 0, Matched),
                lists:foreach(fun(X) -> 
                                      comm:send(SyncDest, 
                                                {check_node,
                                                 comm:this(), 
                                                 merkle_tree:get_interval(X), 
                                                 merkle_tree:get_hash(X)}) 
                              end, NotMatched),
                NewStats#ru_recon_stats{ 
                    tree_compareLeft = NewStats#ru_recon_stats.tree_compareLeft + length(NotMatched),
                    tree_nodesCompared = NewStats#ru_recon_stats.tree_nodesCompared + length(Matched),
                    tree_compareSkipped = Skipped + SkippedSubNodes }        
        end,
    if FinalStats#ru_recon_stats.tree_compareLeft =< 1 ->
           comm:send(SyncDest, {shutdown, sync_finished_remote_shutdown}),
           comm:send_local(self(), {shutdown, sync_finished});
       true -> ok
    end,
    State#ru_recon_state{ recon_stats = FinalStats }.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec add_to_tree(?DB:db_as_list(), merkle_tree:merkle_tree()) -> merkle_tree:merkle_tree().
add_to_tree(DBItems, MTree) ->
    TreeI = merkle_tree:get_interval(MTree),
    lists:foldl(fun({Key, Val, _, _, Ver}, Tree) ->
                        MinKey = minKeyInInterval(Key, TreeI),
                        merkle_tree:insert(MinKey, encodeBlob(Ver, Val), Tree)
                end, 
                MTree, 
                DBItems).

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
-spec reconcileNode(merkle_tree:mt_node(), {comm:mypid(), float(), comm:mypid()}) -> non_neg_integer().
reconcileNode(Node, Conf) ->
    Count = case merkle_tree:is_leaf(Node) of
                true -> reconcileLeaf(Node, Conf);
                false -> lists:foldl(fun(X, Acc) -> 
                                             Acc + reconcileNode(X, Conf) 
                                     end, 
                                     0, 
                                     merkle_tree:get_childs(Node))
            end,
    Count.
-spec reconcileLeaf(merkle_tree:mt_node(), {comm:mypid(), float(), comm:mypid()}) -> 1.
reconcileLeaf(Node, {DestPid, Round, OwnerPid}) ->
    ToSync = lists:map(fun({Key, Value}) -> 
                               {Ver, Val} = decodeBlob(Value),
                               {Key, Val, Ver}
                       end, 
                       merkle_tree:get_bucket(Node)),
    comm:send(DestPid, 
              {request_resolve, Round, simple, {simple_detail_sync, ToSync},
               {true, OwnerPid}, {false, OwnerPid}}),
    1.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% art recon
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec art_recon(MyTree, Art, State) -> {ok, Stats} when
    is_subtype(MyTree, merkle_tree:merkle_tree()),
    is_subtype(Art,    art:art()),
    is_subtype(State,  state()),
    is_subtype(Stats,  ru_recon_stats()).
art_recon(Tree, Art, #ru_recon_state{ sync_round = Round, 
                                      dest_ru_pid = DestPid,
                                      ownerRemotePid = OwnerPid }) ->
    case merkle_tree:get_interval(Tree) =:= art:get_interval(Art) of
        true -> 
            NodesToSync = 
                art_get_sync_leafs([merkle_tree:get_root(Tree)], Art, []),
            ?TRACE("OK GET SYNC LEAFS",[]),
            _ = [reconcileLeaf(X, {DestPid, Round, OwnerPid}) || X <- NodesToSync],
            {ok, #ru_recon_stats{ tree_leafsSynced = erlang:length(NodesToSync) }};
        false -> {ok, #ru_recon_stats{}}
    end.

-spec art_get_sync_leafs(Nodes, Art, Acc) -> Result when
    is_subtype(Nodes,  [merkle_tree:mt_node()]),
    is_subtype(Art,    art:art()),
    is_subtype(Acc,    [merkle_tree:mt_node()]),
    is_subtype(Result, [merkle_tree:mt_node()]).
art_get_sync_leafs([], Art, ToSyncAcc) ->
    ToSyncAcc;
art_get_sync_leafs([Node | ToCheck], Art, ToSyncAcc) ->
    case art:lookup(Node, Art) of
        true ->
            art_get_sync_leafs(ToCheck, Art, ToSyncAcc);
        false ->
            case merkle_tree:is_leaf(Node) of
                true ->
                    art_get_sync_leafs(ToCheck, Art, [Node | ToSyncAcc]);
                false ->
                    art_get_sync_leafs(
                           lists:append(merkle_tree:get_childs(Node), ToCheck), 
                           Art, 
                           ToSyncAcc)
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_recon_struct(Method, DB_Chunk, Args) -> Recon_Struct when
      is_subtype(Method,       recon_method()),
      is_subtype(DB_Chunk,     rep_upd:db_chunk()),
      is_subtype(Args,         build_args()),
      is_subtype(Recon_Struct, bloom_recon_struct() | merkle_tree:merkle_tree() | art:art()).

build_recon_struct(bloom, {I, DBItems}, {Fpr}) ->
    ElementNum = length(DBItems),
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?REP_HFS:new(HFCount),    
    {KeyBF, VerBF} = fill_bloom(DBItems, 
                                ?REP_BLOOM:new(ElementNum, Fpr, Hfs), 
                                ?REP_BLOOM:new(ElementNum, Fpr, Hfs)),
    #bloom_recon_struct{ interval = I,
                         keyBF = KeyBF,
                         versBF = VerBF
                       };

build_recon_struct(merkle_tree, {I, DBItems}, _) ->
    Tree = add_to_tree(DBItems, merkle_tree:new(I)),
    merkle_tree:gen_hash(Tree);

build_recon_struct(art, Chunk, _) ->
    Tree = build_recon_struct(merkle_tree, Chunk, {}),
    art:new(Tree).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% bloom_filter specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Create two bloom filter of a given database chunk.
%%      One over all keys and one over all keys concatenated with their version.
-spec fill_bloom(DB, KeyBloom, VersionBloom) -> {KeyBloom2, VersionBloom2} when
      is_subtype(DB,            ?DB:db_as_list()),
      is_subtype(KeyBloom,      ?REP_BLOOM:bloom_filter()),
      is_subtype(VersionBloom,  ?REP_BLOOM:bloom_filter()),
      is_subtype(KeyBloom2,     ?REP_BLOOM:bloom_filter()),
      is_subtype(VersionBloom2, ?REP_BLOOM:bloom_filter()).

fill_bloom([], KeyBF, VerBF) ->
    {KeyBF, VerBF};
fill_bloom([{_, _, _, _, -1} | T], KeyBF, VerBF) ->
    fill_bloom(T, KeyBF, VerBF);
fill_bloom([{Key, _, _, _, Ver} | T], KeyBF, VerBF) ->
    AddKey = minKey(Key),
    NewKeyBF = ?REP_BLOOM:add(KeyBF, AddKey),
    NewVerBF = ?REP_BLOOM:add(VerBF, encodeBlob(AddKey, Ver)),
    fill_bloom(T, NewKeyBF, NewVerBF).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec print_recon_stats(ru_recon_stats()) -> [any()].
print_recon_stats(Stats) ->
    FieldNames = record_info(fields, ru_recon_stats),
    Res = util:for_to_ex(1, length(FieldNames), 
                         fun(I) ->
                                 {lists:nth(I, FieldNames), erlang:element(I + 1, Stats)}
                         end),
    [erlang:element(1, Stats), lists:flatten(lists:reverse(Res))].

% @ doc filter, partition and map items of a list in one run
-spec filterPartitionMap(fun((A) -> boolean()), fun((A) -> boolean()), fun((A) -> any()), [A]) -> {Satisfying::[any()], NonSatisfying::[any()]}.
filterPartitionMap(Filter, Pred, Map, List) ->
    filterPartitionMap(Filter, Pred, Map, List, [], []).

filterPartitionMap(_, _, _, [], Satis, NonSatis) ->
    {Satis, NonSatis};
filterPartitionMap(Filter, Pred, Map, [H | T], Satis, NonSatis) ->
    case Filter(H) of
        true -> case Pred(H) of
                     true -> filterPartitionMap(Filter, Pred, Map, T, [Map(H) | Satis], NonSatis);
                     false -> filterPartitionMap(Filter, Pred, Map, T, Satis, [Map(H) | NonSatis])
                 end;        
        false -> filterPartitionMap(Filter, Pred, Map, T, Satis, NonSatis)
    end.

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
    lists:nth(random:uniform(erlang:length(Keys)), Keys).

% @doc transforms a key to its smallest associated key
-spec minKey(?RT:key()) -> ?RT:key().
minKey(Key) ->
    lists:min(?RT:get_replica_keys(Key)).

-spec minKeyInInterval(?RT:key(), intervals:interval()) -> ?RT:key().
minKeyInInterval(Key, I) ->
    minKeyInI(I, lists:sort(?RT:get_replica_keys(Key))).

minKeyInI(I, [H|T]) ->
    case intervals:in(H, I) of
        true -> H;
        false -> minKeyInI(I, T)
    end.

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
    Q = get_key_quadrant(Key),
    Dest = Q + Add,
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

-spec concatKeyVer(db_entry:entry()) -> binary().
concatKeyVer(DBEntry) ->
    encodeBlob(minKey(db_entry:get_key(DBEntry)), db_entry:get_version(DBEntry)).

-spec encodeBlob(?DB:version() | ?RT:key(), ?DB:value() | ?DB:version()) -> binary().
encodeBlob(A, B) -> 
    term_to_binary([A, "#", B]).

-spec decodeBlob(binary()) -> {?DB:version() | ?RT:key(), ?DB:value() | ?DB:version()} | fail.
decodeBlob(Blob) ->
    L = binary_to_term(Blob),
    case length(L) of
        3 -> {hd(L), lists:last(L)};
       _ -> fail
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc init module
-spec init(state()) -> state().
init(State) ->
    State#ru_recon_state{ sync_start_time = erlang:now() }.

-spec start(Round, Sender_RU_Pid) -> {ok, MyPid} when
      is_subtype(Round,         float()),
      is_subtype(Sender_RU_Pid, comm:mypid() | undefined),
      is_subtype(MyPid,         pid()).

start(Round, SenderRUPid) ->
    State = #ru_recon_state{ ownerLocalPid = self(), 
                             ownerRemotePid = comm:this(), 
                             dhtNodePid = pid_groups:get_my(dht_node),
                             dest_ru_pid = SenderRUPid,
                             sync_round = Round },
    gen_component:start(?MODULE, State, []).

-spec fork_recon(state(), float()) -> {ok, pid()}.
fork_recon(Conf, Round) ->
    State = Conf#ru_recon_state{ sync_round = Round },
    comm:send_local(Conf#ru_recon_state.ownerLocalPid, {recon_forked}),
    gen_component:start(?MODULE, State, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rep_update_activate) of
        true ->
            config:cfg_is_bool(rep_update_sync_feedback) andalso                
            config:cfg_is_float(rep_update_recon_fpr) andalso
            config:cfg_is_greater_than(rep_update_recon_fpr, 0) andalso
            config:cfg_is_less_than(rep_update_recon_fpr, 1) andalso
            config:cfg_is_integer(rep_update_max_items) andalso
            config:cfg_is_greater_than(rep_update_max_items, 0);
        _ -> true
    end.

-spec get_do_feedback() -> boolean().
get_do_feedback() ->
    config:read(rep_update_sync_feedback).

-spec get_fpr() -> float().
get_fpr() ->
    config:read(rep_update_recon_fpr).

-spec get_max_items(recon_method()) -> pos_integer().
get_max_items(ReconMethod) ->
    case ReconMethod of
        merkle_tree -> all;
        art -> all;
        _ -> config:read(rep_update_max_items)
    end.