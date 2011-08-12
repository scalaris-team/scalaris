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
%% @doc    replica update synchronization protocol
%%         2 phases: I) reconciliation - find differences 
%%                  II) resolution - resolve differences
%% @end
%% @version $Id$

-module(rep_upd_sync).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start_sync/2, check_config/0]).

%exports for testing
-export([encodeBlob/2, decodeBlob/1, 
         mapInterval/2, map_key_to_quadrant/2, 
         get_key_quadrant/1, get_interval_quadrant/1]).

-ifdef(with_export_type_support).
-export_type([sync_struct/0, sync_stage/0]).
-endif.

-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
%-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type keyValVers() :: {?RT:key(), ?DB:value(), ?DB:version()}.

-type sync_stage()  :: req_shared_interval | res_shared_interval | build_struct | reconciliation | resolution.

-type exit_reason() :: empty_interval | {ok, ItemsUpdated::non_neg_integer()}.

-record(bloom_sync_struct, 
        {
         interval = intervals:empty()                       :: intervals:interval(), 
         srcNode  = ?required(bloom_sync_struct, srcNode)   :: comm:mypid(),
         keyBF    = ?required(bloom_sync_struct, keyBF)     :: ?REP_BLOOM:bloomFilter(),
         versBF   = ?required(bloom_sync_struct, versBF)    :: ?REP_BLOOM:bloomFilter()         
        }).
-type bloom_sync_struct() :: #bloom_sync_struct{}.

-record(merkle_sync_struct,
        {
         dest_repUpd_pid = ?required(merkle_sync_struct, dest_repUpd_pid) :: comm:mypid(),
         tree            = merkle_tree:empty()                            :: merkle_tree:merkle_tree()
         }).
-type merkle_sync_struct() :: #merkle_sync_struct{}.

-type simple_detail_sync() :: {simple_detail_sync, SrcNode::comm:mypid(), [keyValVers()]}.

-type internal_buffer_struct() :: intervals:interval() |  
                                  {comm:mypid(), comm:mypid(), intervals:interval()} |
                                  {comm:mypid(), intervals:interval()}.

-type sync_struct() :: bloom_sync_struct() |
                       merkle_sync_struct() |
                       simple_detail_sync() |
                       internal_buffer_struct().

-record(rep_upd_sync_stats,
        {
         diffCount          = 0 :: non_neg_integer(),
         updatedCount       = 0 :: non_neg_integer(),
         notUpdatedCount    = 0 :: non_neg_integer(),
         tree_compareLeft   = 0 :: non_neg_integer(),
         tree_nodesCompared = 0 :: non_neg_integer(),
         tree_leafsSynced   = 0 :: non_neg_integer(),
         errorCount         = 0 :: non_neg_integer(),
         buildTime          = 0 :: non_neg_integer(),
         syncTime           = 0 :: non_neg_integer()
         }).
-type rep_upd_sync_stats() :: #rep_upd_sync_stats{}.

-record(rep_upd_sync_state,
        {
         ownerLocalPid      = ?required(rep_upd_sync_state, ownerLocalPid)  :: comm:erl_local_pid(),
         ownerRemotePid     = ?required(rep_upd_sync_state, ownerRemotePid) :: comm:mypid(),
         dhtNodePid         = ?required(rep_upd_sync_state, ownerDhtPid)    :: comm:erl_local_pid(),
         sync_method        = undefined                                     :: rep_upd:sync_method(),   %determines the build sync struct
         sync_struct        = {}                                            :: sync_struct() | {},
         sync_stage         = reconciliation                                :: sync_stage(),
         sync_pid           = undefined                                     :: comm:mypid() | undefined,%sync dest process
         sync_master        = ?required(rep_upd_sync_state, sync_master)    :: boolean(),               %true if process is sync leader
         sync_round         = 0                                             :: float(),
         sync_stats         = #rep_upd_sync_stats{}                         :: rep_upd_sync_stats(),    
         feedback           = []                                            :: [keyValVers()]
         }).
-type state() :: #rep_upd_sync_state{}.

-type build_args() :: {} | 
                      {DestRepUpdPid::comm:mypid()} |           %merkle tree build args
                      {Fpr::float(), SrcNodePid::comm:mypid()}. %bloom build args

-type message() ::
    {get_state_response, intervals:interval()} |
    {get_chunk_response, rep_upd:db_chunk()} |
    {update_key_entry_ack, db_entry:entry(), Exists::boolean(), Done::boolean()} |
    {check_node, MasterPid::comm:mypid(), intervals:interval(), merkle_tree:mt_node_key()} |
    {check_node_response, ok | not_found} |
    {check_node_response, fail, intervals:interval(), [merkle_tree:mt_node_key()] | is_leaf} |
    {start_sync, rep_upd:sync_method(), sync_stage()} |
    {start_sync, rep_upd:sync_method(), sync_stage(), sync_struct()} |
    {shutdown, exit_reason()} |
    {crash, DeadPid::comm:mypid()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, MyInterval}, State = 
       #rep_upd_sync_state{ sync_stage = build_struct,
                            sync_method = bloom,
                            dhtNodePid = DhtNodePid }) ->
    comm:send_local(DhtNodePid, {get_chunk, self(), MyInterval, get_max_items()}),
    State;
on({get_state_response, MyInterval}, State = 
       #rep_upd_sync_state{ sync_stage = req_shared_interval,
                            sync_master = true,
                            sync_round = Round,
                            dhtNodePid = DhtNodePid,
                            ownerRemotePid = OwnerPid}) ->
    comm:send_local(DhtNodePid, 
                    {lookup_aux, select_sync_node(MyInterval), 0, 
                     {send_to_group_member, rep_upd,
                      {request_sync, Round, req_shared_interval, merkleTree, {OwnerPid, MyInterval}}}}),
    comm:send_local(self(), {shutdown, negotiate_interval_master}),
    State;

on({get_state_response, MyInterval}, State = 
       #rep_upd_sync_state{ sync_stage = req_shared_interval,
                            sync_master = false,
                            sync_struct = {MasterOwnerPid, SrcInterval},                        
                            dhtNodePid = DhtNodePid }) ->
    fd:subscribe(MasterOwnerPid),    
    MyIMapped = mapInterval(MyInterval, 1),
    SrcIMapped = mapInterval(SrcInterval, 1),
    Intersection = intervals:intersection(MyIMapped, SrcIMapped),
    ?TRACE("REQ SHARED I FROM~nMyI=[~p] -~nMyIMapped=[~p]~nSrcI=[~p]~nSrcIMapped=~p~nIntersec=~p", 
           [MyInterval, MyIMapped, SrcInterval, SrcIMapped, Intersection]),
    case intervals:is_empty(Intersection) of
        true ->
            comm:send_local(self(), {shutdown, intersection_empty});
        false ->
            comm:send_local(DhtNodePid, 
                            {get_chunk, self(), Intersection, get_max_items()})
    end,
    State#rep_upd_sync_state{ sync_stage = build_struct,
                              sync_struct = {MasterOwnerPid, Intersection} };

on({get_state_response, MyInterval}, State = 
       #rep_upd_sync_state{ sync_stage = res_shared_interval,
                            sync_struct = {ClientOwnerPid, ClientPid, Interval},
                            dhtNodePid = DhtNodePid }) ->
    MappedIntersec = mapInterval(Interval, get_interval_quadrant(MyInterval)),
    case intervals:is_subset(MappedIntersec, MyInterval) of
        false ->
            comm:send_local(self(), {shutdown, negotiate_interval_master}),
            comm:send(ClientPid, {shutdown, no_interval_intersection});
        true ->
            fd:subscribe(ClientOwnerPid),
            comm:send_local(DhtNodePid, {get_chunk, self(), MappedIntersec, get_max_items()})
    end,    
    State#rep_upd_sync_state{ sync_stage = build_struct, 
                              sync_struct = {ClientOwnerPid, Interval}, 
                              sync_pid = ClientPid };

on({get_state_response, NodeDBInterval}, State = 
       #rep_upd_sync_state{ sync_stage = reconciliation,
                            sync_method = bloom,
                            dhtNodePid = DhtNodePid,
                            sync_struct = #bloom_sync_struct{ interval = BloomI}
                          }) ->
    SyncInterval = intervals:intersection(NodeDBInterval, BloomI),
    case intervals:is_empty(SyncInterval) of
        true ->
            comm:send_local(self(), {shutdown, empty_interval});
        false ->
            comm:send_local(DhtNodePid, {get_chunk, self(), SyncInterval, get_max_items()})
    end,
    State;

on({get_state_response, NodeDBInterval}, State = 
       #rep_upd_sync_state{ sync_stage = resolution,
                            sync_struct = {simple_detail_sync, _, DiffList},
                            dhtNodePid = DhtNodePid
                          }) ->
    %simple detail sync case
    MyPid = comm:this(),
    erlang:spawn(lists, 
                 foreach, 
                 [fun({MinKey, Val, Vers}) ->
                          PosKeys = ?RT:get_replica_keys(MinKey),
                          UpdKeys = lists:filter(fun(X) -> 
                                                         intervals:in(X, NodeDBInterval)
                                                 end, PosKeys),
                          lists:foreach(fun(Key) ->
                                                comm:send_local(DhtNodePid, 
                                                                {update_key_entry, MyPid, Key, Val, Vers})
                                        end, UpdKeys)
                  end, 
                  DiffList]),
    %kill is done by update_key_entry_ack
    State;

on({get_chunk_response, {_, []}}, State) ->
    State;

on({get_chunk_response, {RestI, [First | T] = DBList}}, State =
       #rep_upd_sync_state{ sync_stage = build_struct,
                            sync_method = bloom,                            
                            sync_round = Round,
                            dhtNodePid = DhtNodePid,
                            sync_master = SyncMaster,
                            sync_stats = SyncStats }) ->
    ?TRACE("Get_Chunk Res - Build Bloom", []),
    case intervals:is_empty(RestI) of
        false ->
            %{ok, Pid} = start_sync(true, Round + 0.1),
            {ok, Pid} = fork_sync(State, Round + 0.1),
            comm:send_local(DhtNodePid, {get_chunk, Pid, RestI, get_max_items()});
        _ -> ok
    end,
    %Get Interval of DBList
    %TODO: IMPROVEMENT getChunk should return ChunkInterval (db is traversed twice! - 1st getChunk, 2nd here)
    ChunkI = intervals:new('[', db_entry:get_key(First), db_entry:get_key(lists:last(T)), ']'),
    %?TRACE("RECV CHUNK interval= ~p  - RestInterval= ~p - DBLength=~p", [ChunkI, RestI, length(DBList)]),
    {BuildTime, SyncStruct} = util:tc(fun() -> 
                                              build_sync_struct(
                                                bloom, 
                                                {ChunkI, DBList}, 
                                                {get_sync_fpr(), 
                                                 State#rep_upd_sync_state.ownerRemotePid}) 
                                      end),
    case SyncMaster of
        true ->
            DestKey = select_sync_node(ChunkI),
            comm:send_local(DhtNodePid, 
                            {lookup_aux, DestKey, 0, 
                             {send_to_group_member, rep_upd, 
                              {request_sync, Round, reconciliation, bloom, SyncStruct}}}),
            comm:send_local(self(), {shutdown, {ok, build_struct}});
        _ -> ok
    end,
    State#rep_upd_sync_state{ sync_struct = SyncStruct, 
                              sync_stats = 
                                  SyncStats#rep_upd_sync_stats{ buildTime = BuildTime } 
                            };

on({get_chunk_response, {RestI, DBList}}, State = 
       #rep_upd_sync_state{ sync_stage = build_struct,
                            sync_method = merkleTree,
                            sync_pid = SrcPid,
                            sync_struct = SyncStruct,
                            sync_master = IsMaster,
                            ownerRemotePid = OwnerPid,
                            dhtNodePid = DhtNodePid,
                            sync_round = Round,
                            sync_stats = Stats }) ->
    %build or extend tree
    {BuildTime, TreeSync} = 
        case is_record(SyncStruct, merkle_sync_struct) of
            true ->
                {BTime, NewTree} = util:tc(
                                     fun() -> add_to_tree(DBList, 
                                                          SyncStruct#merkle_sync_struct.tree) 
                                     end),
                {Stats#rep_upd_sync_stats.buildTime + BTime, 
                 SyncStruct#merkle_sync_struct{ tree = NewTree } };
            false ->
                {DestOwnerPid, Interval} = SyncStruct,
                util:tc(
                  fun() -> build_sync_struct(merkleTree, 
                                             {Interval, DBList}, 
                                             {DestOwnerPid}) 
                  end)
        end,
    %start sync if possibile
    case intervals:is_empty(RestI) of
        false ->
            ?TRACE("TREE EXISTS - ADD REST - RestI=~p ~n TreeI= ~p", 
                   [RestI, merkle_tree:get_interval(TreeSync#merkle_sync_struct.tree)]),
            comm:send_local(DhtNodePid, {get_chunk, self(), RestI, get_max_items()}),
            State#rep_upd_sync_state{sync_struct = TreeSync, 
                                     sync_stats = Stats#rep_upd_sync_stats{ buildTime = BuildTime } };        
        true ->
            FinalTree = merkle_tree:gen_hashes(TreeSync#merkle_sync_struct.tree),
            FinalTreeSync = TreeSync#merkle_sync_struct{ tree = FinalTree },
            ToCompare = 
                case IsMaster of
                    true ->
                        comm:send(SrcPid, {check_node, 
                                           comm:this(), 
                                           merkle_tree:get_interval(FinalTree), 
                                           merkle_tree:get_hash(FinalTree)}),
                        1;
                    false -> 
                        comm:send(FinalTreeSync#merkle_sync_struct.dest_repUpd_pid, 
                                  {request_sync, 
                                   Round, 
                                   res_shared_interval, 
                                   merkleTree, 
                                   {OwnerPid, comm:this(), merkle_tree:get_interval(FinalTree)}}),
                        0
                end,
            State#rep_upd_sync_state{ sync_stage = reconciliation, 
                                      sync_struct = FinalTreeSync, 
                                      sync_stats = 
                                          Stats#rep_upd_sync_stats{ tree_compareLeft = ToCompare,
                                                                    buildTime = BuildTime
                                                                  } }
    end;

on({get_chunk_response, {RestI, DBList}}, State = 
       #rep_upd_sync_state{ sync_stage = reconciliation,
                            sync_method = bloom,
                            ownerRemotePid = OwnerPid,
                            sync_struct = #bloom_sync_struct{ srcNode = SrcNode,
                                                              keyBF = KeyBF,
                                                              versBF = VersBF},
                            dhtNodePid = DhtNodePid,
                            sync_round = Round }) ->
    ?TRACE("GetChunk Res - Recon Bloom Round=~p", [Round]),
    %if rest interval is non empty start another sync    
    SyncFinished = intervals:is_empty(RestI),
    not SyncFinished andalso
        comm:send_local(DhtNodePid, {get_chunk, self(), RestI, get_max_items()}),
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
    %TODO possibility of DETAIL SYNC IMPL - NOW SEND COMPLETE obsolete Entries (key-val-vers)
    length(Obsolete) > 0 andalso
        comm:send(SrcNode, {request_sync, Round, resolution, bloom, {simple_detail_sync, OwnerPid, Obsolete}}),
    SyncFinished andalso
        comm:send_local(self(), {shutdown, {ok, reconciliation}}),
    State;

on({update_key_entry_ack, Entry, Exists, Done}, State =
       #rep_upd_sync_state{ sync_struct = {simple_detail_sync, Sender, _},
                            ownerRemotePid = Owner,
                            sync_stats = 
                                #rep_upd_sync_stats{ diffCount = DiffCount,
                                                     updatedCount = OkCount, 
                                                     notUpdatedCount = FailedCount                                                         
                                                   } = Stats,
                            feedback = Feedback,                            
                            sync_round = Round
                          }) ->
    NewStats = case Done of 
                   true  -> Stats#rep_upd_sync_stats{ updatedCount = OkCount +1 };
                   false -> Stats#rep_upd_sync_stats{ notUpdatedCount = FailedCount + 1 } 
               end,    
    NewState = case not Done andalso Exists andalso get_do_feedback() of
                   true -> State#rep_upd_sync_state{ sync_stats = NewStats,
                                                     feedback = [{db_entry:get_key(Entry),
                                                                  db_entry:get_value(Entry),
                                                                  db_entry:get_version(Entry)} | Feedback]};
                   false -> State#rep_upd_sync_state{ sync_stats = NewStats }
               end,
    _ = case DiffCount - 1 =:= OkCount + FailedCount of
            true ->
                get_do_feedback() andalso
                    comm:send(Sender, {request_sync, Round, resolution, bloom, {simple_detail_sync, Owner, NewState#rep_upd_sync_state.feedback}}),
                comm:send_local(self(), {shutdown, {ok, NewState#rep_upd_sync_state.sync_stats}});
            _ ->
                ok
        end,
    NewState;

on({start_sync, SyncMethod, SyncStage}, State) ->
    comm:send_local(State#rep_upd_sync_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#rep_upd_sync_state{ sync_method = SyncMethod, 
                              sync_stage = SyncStage };    
on({start_sync, SyncMethod, SyncStage, SyncStruct}, State =
       #rep_upd_sync_state{ sync_master = Master }) ->
    comm:send_local(State#rep_upd_sync_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#rep_upd_sync_state{ sync_stage = SyncStage, 
                              sync_struct = SyncStruct,
                              sync_method = SyncMethod,
                              sync_master = Master orelse SyncStage =:= res_shared_interval };

on({crash, Pid}, State) ->
    comm:send_local(self(), {shutdown, {fail, crash_of_sync_node, Pid}}),
    State;

on({shutdown, Reason}, State = #rep_upd_sync_state{ sync_round = Round }) ->
    comm:send_local(State#rep_upd_sync_state.ownerLocalPid, 
                    {sync_progress_report, self(), shutdown, io_lib:format("[R~p] ~p", [Round, Reason])}),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({check_node, SrcPid, Interval, Hash}, State =
       #rep_upd_sync_state{ sync_struct = #merkle_sync_struct{ tree = Tree } }) ->
    Node = merkle_tree:lookup(Interval, Tree),
    case Node of
        not_found ->
            comm:send(SrcPid, 
                      {check_node_response, not_found});
        _ ->
            IsLeaf = merkle_tree:is_leaf(Node),
            case merkle_tree:get_hash(Node) =:= Hash of
                true ->
                    comm:send(SrcPid, 
                              {check_node_response, ok});
                false when IsLeaf -> 
                    comm:send(SrcPid, 
                              {check_node_response, fail, Interval, is_leaf});
                false when not IsLeaf ->
                    ChildHashs = lists:map(fun(N) ->
                                                   merkle_tree:get_hash(N)
                                           end, 
                                           merkle_tree:get_childs(Node)),
                    comm:send(SrcPid, 
                              {check_node_response, fail, Interval, ChildHashs})
            end
    end,
    State#rep_upd_sync_state{ sync_pid = SrcPid };

on({check_node_response, Result}, State = 
       #rep_upd_sync_state{
                           sync_pid = SyncDest, 
                           sync_stats = 
                                #rep_upd_sync_stats{ tree_compareLeft = CLeft,
                                                     errorCount = Errors
                                                   } = Stats 
                          }) ->
    NewErrorCount = Errors + case Result of
                                 ok -> 0;
                                 not_found -> 1
                             end,
    NewStats = Stats#rep_upd_sync_stats{ tree_compareLeft = CLeft - 1, 
                                         errorCount = NewErrorCount },
    if NewStats#rep_upd_sync_stats.tree_compareLeft =< 1 ->
           comm:send(SyncDest, {shutdown, sync_finished_remote_shutdown}),
           comm:send_local(self(), {shutdown, sync_finished})
    end,                                                        
    State#rep_upd_sync_state{ sync_stats = NewStats };

on({check_node_response, fail, I, is_leaf}, State = 
       #rep_upd_sync_state{ sync_stats = 
                                #rep_upd_sync_stats{
                                                    errorCount = Errors,
                                                    tree_compareLeft = ToCompare,
                                                    tree_leafsSynced = LeafCount
                                                    } = Stats, 
                            ownerRemotePid = OwnerPid,
                            sync_struct = #merkle_sync_struct{ dest_repUpd_pid = SrcNode,
                                                               tree = Tree },
                            sync_round = Round }) ->
    Node = merkle_tree:lookup(I, Tree),
    NewStats = case Node of
                   not_found -> 
                       Stats#rep_upd_sync_stats{ errorCount = Errors + 1 };
                   _ -> 
                       IsLeaf = merkle_tree:is_leaf(Node),
                       case IsLeaf of
                           true ->
                               reconcileLeaf(Node, {SrcNode, Round, OwnerPid}),
                               Stats#rep_upd_sync_stats{ tree_leafsSynced = LeafCount + 1 };
                           false ->
                               reconcileNode(Node, {SrcNode, Round, OwnerPid})
                                %TODO stats updaten
                       end
               end,
    State#rep_upd_sync_state{ sync_stats = NewStats#rep_upd_sync_stats{ tree_compareLeft = ToCompare - 1} };

on({check_node_response, fail, I, ChildHashs}, State = 
       #rep_upd_sync_state{ sync_stats = #rep_upd_sync_stats{
                                                             tree_compareLeft = ToCompare,
                                                             tree_nodesCompared = Compared
                                                             } = Stats, 
                            sync_pid = SrcPid,
                            sync_struct = #merkle_sync_struct{ tree = Tree } }) ->
    Node = merkle_tree:lookup(I, Tree),
    NewStats = case Node of 
                   not_found -> Stats;
                   _ -> MyChilds = merkle_tree:get_childs(Node),
                        NotMatched = compareNodes(MyChilds, ChildHashs, []),
                        lists:foreach(
                          fun(X) -> 
                                  comm:send(SrcPid, 
                                            {check_node,
                                             comm:this(), 
                                             merkle_tree:get_interval(X), 
                                             merkle_tree:get_hash(X)}) 
                          end, 
                          NotMatched),
                        Stats#rep_upd_sync_stats{ tree_compareLeft = ToCompare + length(NotMatched) - 1,
                                                  tree_nodesCompared = Compared + 1 }                
               end,
    State#rep_upd_sync_state{ sync_stats = NewStats }.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% build sync struct
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_sync_struct(rep_upd:sync_method(), rep_upd:db_chunk(), Args::build_args()) -> bloom_sync_struct() | merkle_sync_struct().
build_sync_struct(bloom, {I, DBItems}, {Fpr, SrcNodePid}) ->
    ElementNum = length(DBItems),
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?REP_HFS:new(HFCount),
    BF1 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),
    BF2 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),    
    {KeyBF, VerBF} = fill_bloom(DBItems, BF1, BF2),
    #bloom_sync_struct{ interval = I,
                        srcNode = SrcNodePid,
                        keyBF = KeyBF,
                        versBF = VerBF
                      };
build_sync_struct(merkleTree, {I, DBItems}, {DestRepUpdPid}) ->
    #merkle_sync_struct{ dest_repUpd_pid = DestRepUpdPid, 
                         tree = add_to_tree(DBItems, merkle_tree:new(I))}.

-spec add_to_tree(?DB:db_as_list(), merkle_tree:merkle_tree()) -> merkle_tree:merkle_tree().
add_to_tree(DBItems, MTree) ->
    TreeI = merkle_tree:get_interval(MTree),
    lists:foldl(fun({Key, Val, _, _, Ver}, Tree) ->
                        MinKey = minKeyInInterval(Key, TreeI),
                        merkle_tree:insert(MinKey, encodeBlob(Ver, Val), Tree)
                end, 
                MTree, 
                DBItems).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% BloomFilter specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Create two bloom filter of a given database chunk.
%%      One over all keys and one over all keys concatenated with their version.
-spec fill_bloom(?DB:db_as_list(), KeyBF::?REP_BLOOM:bloomFilter(), VerBF::?REP_BLOOM:bloomFilter()) -> 
          {?REP_BLOOM:bloomFilter(), ?REP_BLOOM:bloomFilter()}.

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
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec compareNodes([merkle_tree:mt_node()], [merkle_tree:mt_node_key()], [merkle_tree:mt_node()]) -> [merkle_tree:mt_node()].
compareNodes([], [], Acc) -> Acc;
compareNodes([N | Nodes], [H | NodeHashs], Acc) ->
    Hash = merkle_tree:get_hash(N),
    case Hash =:= H of
        true -> compareNodes(Nodes, NodeHashs, Acc);
        false -> compareNodes(Nodes, NodeHashs, [N|Acc])
    end.

% @doc Starts simple sync for a given node
-spec reconcileNode(merkle_tree:mt_node(), {comm:mypid(), float(), comm:mypid()}) -> ok.
reconcileNode(Node, Conf) ->
    _ = case merkle_tree:is_leaf(Node) of
            true -> reconcileLeaf(Node, Conf);
            false -> [ reconcileNode(X, Conf) || X <- merkle_tree:get_childs(Node)]
        end,
    ok.
-spec reconcileLeaf(merkle_tree:mt_node(), {comm:mypid(), float(), comm:mypid()}) -> ok.
reconcileLeaf(Node, {DestPid, Round, OwnerPid}) ->
    ToSync = lists:map(fun({Key, Value}) -> 
                               {Ver, Val} = decodeBlob(Value),
                               {Key, Val, Ver}
                       end, 
                       merkle_tree:get_bucket(Node)),
    comm:send(DestPid, 
              {request_sync, Round, resolution, merkleTree, {simple_detail_sync, OwnerPid, ToSync}}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
-spec init({comm:erl_local_pid(), sync_struct()}) -> state().
init(State) ->
    State.

-spec start_sync(boolean(), float()) -> {ok, pid()}.
start_sync(SyncMaster, Round) ->
    State = #rep_upd_sync_state{ ownerLocalPid = self(), 
                                 ownerRemotePid = comm:this(), 
                                 dhtNodePid = pid_groups:get_my(dht_node), 
                                 sync_master = SyncMaster,
                                 sync_round = Round },
    gen_component:start(?MODULE, State, []).

-spec fork_sync(state(), float()) -> {ok, pid()}.
fork_sync(Sync_Conf, Round) ->
    State = Sync_Conf#rep_upd_sync_state{ sync_round = Round },
    gen_component:start(?MODULE, State, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rep_update_activate) of
        true ->
            config:is_bool(rep_update_sync_feedback) andalso
            config:is_float(rep_update_fpr) andalso
            config:is_greater_than(rep_update_fpr, 0) andalso
            config:is_less_than(rep_update_fpr, 1) andalso
            config:is_integer(rep_update_max_items) andalso
            config:is_greater_than(rep_update_max_items, 0);
        _ -> true
    end.

-spec get_do_feedback() -> boolean().
get_do_feedback() ->
    config:read(rep_update_sync_feedback).

-spec get_sync_fpr() -> float().
get_sync_fpr() ->
    config:read(rep_update_fpr).

-spec get_max_items() -> pos_integer().
get_max_items() ->
    config:read(rep_update_max_items).