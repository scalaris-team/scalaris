% @copyright 2011, 2012 Zuse Institute Berlin

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
%% @doc    replica reconcilication module
%% @end
%% @version $Id$

-module(rr_recon).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/1, start/2, check_config/0]).

%export for testing
-export([encodeBlob/2, decodeBlob/1,
         minKeyInInterval/2,
         mapInterval/2, map_key_to_quadrant/2, 
         get_key_quadrant/1, get_interval_quadrant/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%DETAIL DEBUG MESSAGES
%-define(TRACE2(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE2(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([method/0, request/0]).
-endif.

-type method()         :: bloom | merkle_tree | art | iblt | undefined.
-type stage()          :: req_shared_interval | res_shared_interval | build_struct | reconciliation.

-type exit_reason()    :: empty_interval |          %interval intersection between initator and client is empty
                          negotiate_interval |      %rc initiator has send its interval and exits
                          chunk_is_empty |          %db chunk is empty  
                          recon_node_crash |        %sync partner node crashed  
                          sync_finished |           %initiator finish recon
                          sync_finished_remote.     %client-side shutdown by merkle-tree recon initiator

-type db_entry_enc()   :: binary().
-type db_as_list_enc() :: [db_entry_enc()].
-type db_chunk_enc()   :: {intervals:interval(), db_as_list_enc()}.

-record(bloom_recon_struct, 
        {
         interval = intervals:empty()                       :: intervals:interval(), 
         bloom    = ?required(bloom_recon_struct, bloom)    :: ?REP_BLOOM:bloom_filter()         
        }).
-type bloom_recon_struct() :: #bloom_recon_struct{}.

-type internal_params() :: {interval, intervals:interval()} |
                           {reconPid, comm:mypid()} |
                           {art, art:art()}.

-type struct() :: bloom_recon_struct() |
                  merkle_tree:merkle_tree() |
                  art:art() |                        
                  [internal_params()].
-type recon_dest() :: ?RT:key() | random.

-record(rr_recon_state,
        {
         ownerLocalPid      = ?required(rr_recon_state, ownerLocalPid)  :: comm:erl_local_pid(),
         ownerRemotePid     = ?required(rr_recon_state, ownerRemotePid) :: comm:mypid(),
         ownerMonitor       = null                                      :: null | reference(),
         dhtNodePid         = ?required(rr_recon_state, dhtNodePid)     :: comm:erl_local_pid(),
         dest_key           = random                                    :: recon_dest(),
         dest_rr_pid        = undefined                                 :: comm:mypid() | undefined, %dest rrepair pid
         dest_recon_pid     = undefined                                 :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                                 :: method(),
         struct             = {}                                        :: struct() | {},
         stage              = req_shared_interval                       :: stage(),
         initiator          = false                                     :: boolean(),
         stats              = rr_recon_stats:new()                      :: rr_recon_stats:stats()
         }).
-type state() :: #rr_recon_state{}.

-type tree_cmp_response() :: ok_inner | ok_leaf |
                             fail_leaf | fail_node |
                             not_found.

-type request() :: 
    {start, method(), DestKey::recon_dest()} |
    {continue, method(), stage(), struct(), Initiator::boolean()}.          

-type message() ::          
    %API
    request() |
    %tree sync msgs
    {check_node, InitiatorPid::comm:mypid(), intervals:interval(), merkle_tree:mt_node_key()} |
    {check_node_response, tree_cmp_response(), intervals:interval(), [merkle_tree:mt_node_key()]} |
    %dht node response
    {get_state_response, intervals:interval()} |
    {get_chunk_response, db_chunk_enc()} |          
    %internal
    {shutdown, exit_reason()} | 
    {crash, DeadPid::comm:mypid()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = req_shared_interval,
                        initiator = true,
                        stats = Stats,
                        method = Method,
                        dest_key = DestKey,
                        dhtNodePid = DhtPid,                        
                        ownerRemotePid = OwnerPid }) ->    
    Msg = {send_to_group_member, rrepair, 
           {continue_recon, OwnerPid, rr_recon_stats:get(session_id, Stats), 
            {continue, Method, req_shared_interval, [{interval, MyI}], false}}},    
    DKey = case DestKey of
               random -> select_sync_node(MyI);        
               _ -> DestKey
           end,
    ?TRACE("START_TO_DEST ~p", [DKey]),
    comm:send_local(DhtPid, {?lookup_aux, DKey, 0, Msg}),    
    comm:send_local(self(), {shutdown, negotiate_interval}),
    State;

on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = req_shared_interval,
                        initiator = false,
                        method = Method,
                        struct = Params,                        
                        dhtNodePid = DhtPid,
                        dest_rr_pid = DestRRPid }) ->
    Method =:= merkle_tree andalso fd:subscribe(DestRRPid),
    SrcI = proplists:get_value(interval, Params),
    Intersec = find_intersection(MyI, SrcI),
    case intervals:is_empty(Intersec) of
        false when Method =:= bloom -> 
            send_chunk_req(DhtPid, self(), Intersec, mapInterval(Intersec, 1), get_max_items(Method));
        false -> send_chunk_req(DhtPid, self(), Intersec, Intersec, get_max_items(Method));
        true -> comm:send_local(self(), {shutdown, empty_interval})
    end,
    State#rr_recon_state{ stage = build_struct,
                          struct = [{interval, Intersec}] };

on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = res_shared_interval,
                        method = RMethod,
                        struct = Params,
                        dhtNodePid = DhtPid,
                        dest_rr_pid = DestRRPid}) ->
    DestI = proplists:get_value(interval, Params),
    DestReconPid = proplists:get_value(reconPid, Params, undefined),
    MyIntersec = find_intersection(MyI, DestI),
    case intervals:is_subset(MyIntersec, MyI) and not intervals:is_empty(MyIntersec) of
        false ->
            comm:send_local(self(), {shutdown, negotiate_interval}),
            DestReconPid =/= undefined andalso
                comm:send(DestReconPid, {shutdown, empty_interval});
        true ->
            RMethod =:= merkle_tree andalso fd:subscribe(DestRRPid),
            send_chunk_req(DhtPid, self(), MyIntersec, DestI, get_max_items(RMethod))
    end,    
    State#rr_recon_state{ stage = build_struct, dest_recon_pid = DestReconPid };

on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = reconciliation,
                        method = bloom,
                        dhtNodePid = DhtPid,
                        struct = #bloom_recon_struct{ interval = BloomI}
                       }) ->
    MySyncI = find_intersection(MyI, BloomI),
    ?TRACE("GET STATE - MyI=~p ~n BloomI=~p ~n SynI=~p", [MyI, BloomI, MySyncI]),
    case intervals:is_empty(MySyncI) of
        true -> comm:send_local(self(), {shutdown, empty_interval});
        false -> send_chunk_req(DhtPid, self(), MySyncI, mapInterval(MySyncI, 1), get_max_items(bloom))
    end,
    State;

on({get_chunk_response, {_, []}}, State) ->
    comm:send_local(self(), {shutdown, chunk_is_empty}),
    State;

on({get_chunk_response, {RestI, DBList}}, State =
       #rr_recon_state{ stage = build_struct,
                        method = RMethod,        
                        struct = Params,                    
                        dhtNodePid = DhtNodePid,
                        initiator = Initiator,
                        stats = Stats }) ->
    SyncI = proplists:get_value(interval, Params),    
    ToBuild = ?IIF(RMethod =:= art, ?IIF(Initiator, merkle_tree, art), RMethod),
    {BuildTime, SyncStruct} =
        case merkle_tree:is_merkle_tree(Params) of
            true ->
                {BTime, NTree} = util:tc(merkle_tree, insert_list, [DBList, Params]),
                {rr_recon_stats:get(build_time, Stats) + BTime, merkle_tree:gen_hash(NTree) };
            false -> util:tc(fun() -> build_recon_struct(ToBuild, {SyncI, DBList}) end)
        end,
    EmptyRest = intervals:is_empty(RestI),
    if not EmptyRest ->
           Pid = if RMethod =:= bloom -> 
                        erlang:element(2, fork_recon(State));
                    true -> self()
                 end,
            send_chunk_req(DhtNodePid, Pid, RestI, 
                           mapInterval(RestI, get_interval_quadrant(SyncI)), 
                           get_max_items(RMethod));
        true -> ok
    end,
    {NStage, NStats} = if EmptyRest orelse RMethod =:= bloom -> {reconciliation, begin_sync(SyncStruct, State)};
                          not EmptyRest andalso RMethod =:= merkle_tree -> {build_struct, Stats};
                          true -> {reconciliation, Stats}
                       end,
    State#rr_recon_state{ stage = NStage, 
                          struct = SyncStruct, 
                          stats = rr_recon_stats:set([{build_time, BuildTime}], NStats) };    
    
on({get_chunk_response, {RestI, DBList}}, State = 
       #rr_recon_state{ stage = reconciliation,
                        method = bloom,
                        dhtNodePid = DhtNodePid,
                        ownerLocalPid = Owner,
                        ownerRemotePid = OwnerR,
                        dest_rr_pid = DestRU_Pid,
                        struct = #bloom_recon_struct{ bloom = BF},
                        stats = Stats }) ->
    %if rest interval is non empty start another sync    
    SID = rr_recon_stats:get(session_id, Stats),
    SyncFinished = intervals:is_empty(RestI),
    not SyncFinished andalso
        send_chunk_req(DhtNodePid, self(), RestI, mapInterval(RestI, 1), get_max_items(bloom)),
    Diff = [erlang:element(1, decodeBlob(KV)) || KV <- DBList,
                                                 not ?REP_BLOOM:is_element(BF, KV)],
    ?TRACE("Reconcile Bloom Session=~p ; Diff=~p", [SID, length(Diff)]),
    NewStats = if
                   length(Diff) > 0 ->
                       comm:send_local(Owner, {request_resolve, SID, {key_upd_send, DestRU_Pid, Diff}, [{feedback, OwnerR}]}),
                       rr_recon_stats:inc([{resolve_started, 2}], Stats); %feedback causes 2 resolve runs
                   true -> Stats
               end,
    SyncFinished andalso        
        comm:send_local(self(), {shutdown, sync_finished}),
    State#rr_recon_state{ stats = NewStats };

on({start, Method, DestKey}, State) ->
    comm:send_local(State#rr_recon_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#rr_recon_state{ struct = {},
                          method = Method,
                          dest_key = DestKey,
                          initiator = true };

on({continue, Method, Stage, Struct, Initiator}, State) ->
    comm:send_local(State#rr_recon_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#rr_recon_state{ stage = Stage, 
                          struct = Struct,
                          method = Method,
                          initiator = Initiator orelse Stage =:= res_shared_interval };

on({crash, _Pid}, State) ->
    comm:send_local(self(), {shutdown, recon_node_crash}),
    State;

on({shutdown, Reason}, #rr_recon_state{ ownerLocalPid = Owner,
                                        ownerMonitor = OwnerMon,
                                        stats = Stats,
                                        initiator = Initiator }) ->
    ?TRACE("SHUTDOWN Session=~p Reason=~p", [rr_recon_stats:get(session_id, Stats), Reason]),
    
    Status = exit_reason_to_rc_status(Reason),
    NewStats = rr_recon_stats:set([{status, Status}], Stats),
    if OwnerMon =/= null -> erlang:demonitor(OwnerMon);
       true -> ok
    end,
    comm:send_local(Owner, {recon_progress_report, self(), Initiator, NewStats}),
    kill;

on({'DOWN', _MonitorRef, process, Owner, _Info}, {Owner, _RemotePid, _Token, _Start, _Count, _Latencies}) ->
    log:log(info, "shutdown rr_recon due to rrepair shut down", []),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({check_node, SenderPid, Interval, Hash}, State = #rr_recon_state{ struct = Tree }) ->
    Node = merkle_tree:lookup(Interval, Tree),
    {Result, ChildHashs} = 
        case Node of
            not_found -> {not_found, []};
            _ ->
                IsLeaf = merkle_tree:is_leaf(Node),
                case merkle_tree:get_hash(Node) =:= Hash of
                    true when IsLeaf -> {ok_leaf, []};
                    true when not IsLeaf -> {ok_inner, []};
                    false when IsLeaf -> {fail_leaf, []};
                    false when not IsLeaf ->
                        {fail_inner, 
                         [merkle_tree:get_hash(N) || N <- merkle_tree:get_childs(Node)]}
                end
        end,
    comm:send(SenderPid, {check_node_response, Result, Interval, ChildHashs}),
    State#rr_recon_state{ dest_recon_pid = SenderPid };

on({check_node_response, Result, I, ChildHashs}, State = 
       #rr_recon_state{ dest_recon_pid = DestReconPid,
                        dest_rr_pid = SrcNode,
                        stats = Stats, 
                        ownerLocalPid = OwnerL,
                        ownerRemotePid = OwnerR,
                        struct = Tree }) ->
    SID = rr_recon_stats:get(session_id, Stats),
    Node = merkle_tree:lookup(I, Tree),
    NodeIsLeaf = merkle_tree:is_leaf(Node),
    IncOps = 
        case Result of
            not_found -> [{error_count, 1}]; 
            ok_leaf -> [{tree_compareSkipped, 1}];
            ok_inner -> [{tree_compareSkipped, merkle_tree:size(Node)}];
            fail_inner when not NodeIsLeaf ->
                MyChilds = merkle_tree:get_childs(Node),
                {Matched, NotMatched} = compareNodes(MyChilds, ChildHashs, {[], []}),
                SkippedSubNodes = 
                    lists:foldl(fun(MNode, Acc) -> 
                                        Acc + case merkle_tree:is_leaf(MNode) of
                                                  true -> 0;
                                                  false -> merkle_tree:size(MNode) - 1
                                              end
                                end, 0, Matched),
                _ = [comm:send(DestReconPid, 
                               {check_node,
                                comm:this(), 
                                merkle_tree:get_interval(X), 
                                merkle_tree:get_hash(X)}) || X <- NotMatched],
                [{tree_compareLeft, length(NotMatched)},
                 {tree_nodesCompared, length(Matched)},
                 {tree_compareSkipped, SkippedSubNodes}];
            _ -> %case fail_leaf OR fail_inner when NodeIsLeaf
                Leafs = reconcileNode(Node, {SrcNode, SID, OwnerL, OwnerR}),
                [{tree_leafsSynced, Leafs}, {resolve_started, Leafs * 2}]            
        end,
    FinalStats = rr_recon_stats:inc(IncOps ++ [{tree_compareLeft, -1}, 
                                               {tree_nodesCompared, 1}], Stats),
    CompLeft = rr_recon_stats:get(tree_compareLeft, FinalStats),
    if CompLeft =< 1 ->
           comm:send(DestReconPid, {shutdown, sync_finished_remote}),
           comm:send_local(self(), {shutdown, sync_finished});
       true -> ok
    end,
    State#rr_recon_state{ stats = FinalStats }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec begin_sync(struct(), state()) -> rr_recon_stats:stats().
begin_sync(SyncStruct, State = #rr_recon_state{ method = Method,
                                                struct = Params,
                                                ownerRemotePid = OwnerPid,
                                                dest_recon_pid = DestReconPid,
                                                dest_rr_pid = DestRRPid,                                       
                                                initiator = Initiator, 
                                                stats = Stats }) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    case Method of
        merkle_tree -> 
            case Initiator of
                true -> comm:send(DestReconPid, 
                                  {check_node, comm:this(), 
                                   merkle_tree:get_interval(SyncStruct), 
                                   merkle_tree:get_hash(SyncStruct)});            
                false ->
                    IntParams = [{interval, merkle_tree:get_interval(SyncStruct)}, {reconPid, comm:this()}],
                    comm:send(DestRRPid, 
                              {continue_recon, OwnerPid, SID, 
                               {continue, merkle_tree, res_shared_interval, IntParams, true}})
            end,
            rr_recon_stats:set(
              [{tree_compareLeft, ?IIF(Initiator, 1, 0)},
               {tree_size, merkle_tree:size_detail(SyncStruct)}], Stats);
        bloom ->
            comm:send(DestRRPid, {continue_recon, OwnerPid, SID,
                                  {continue, bloom, reconciliation, SyncStruct, true}}),
            comm:send_local(self(), {shutdown, {ok, build_struct}}),
            Stats;
        art ->
            {AOk, ARStats} = 
                case Initiator of
                    true -> art_recon(SyncStruct, proplists:get_value(art, Params), State);                    
                    false ->
                        ArtParams = [{interval, art:get_interval(SyncStruct)}, {art, SyncStruct}],
                        comm:send(DestRRPid, 
                                  {continue_recon, OwnerPid, SID, 
                                   {continue, art, res_shared_interval, ArtParams, true}}),
                        {no, Stats}
                end,            
            comm:send_local(self(), {shutdown, ?IIF(AOk =:= ok, sync_finished, client_art_send)}),
            ARStats
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec compareNodes(CompareNodes::NodeL, CmpKeyList, Acc::Result) -> Result when
    is_subtype(CmpKeyList,  [merkle_tree:mt_node_key()]),
    is_subtype(NodeL,       [merkle_tree:mt_node()]),
    is_subtype(Result,      {Matched::NodeL, NotMatched::NodeL}).
compareNodes([], [], Acc) -> Acc;
compareNodes([N | Nodes], [H | NodeHashs], {Matched, NotMatched}) ->
    case merkle_tree:get_hash(N) =:= H of
        true -> compareNodes(Nodes, NodeHashs, {[N|Matched], NotMatched});
        false -> compareNodes(Nodes, NodeHashs, {Matched, [N|NotMatched]})
    end.

% @doc Starts simple sync for a given node, returns number of leaf sync requests.
-spec reconcileNode(Node | not_found, {Dest::RPid, SID, OwnerLocal::LPid, OwnerRemote::RPid}) -> non_neg_integer() when
    is_subtype(LPid,    comm:erl_local_pid()),
    is_subtype(RPid,    comm:mypid() | undefined),
    is_subtype(Node,    merkle_tree:mt_node()),
    is_subtype(SID,     rrepair:session_id()).
reconcileNode(not_found, _) -> 0;
reconcileNode(Node, Conf) ->
    case merkle_tree:is_leaf(Node) of
        true -> reconcileLeaf(Node, Conf);
        false -> lists:foldl(fun(X, Acc) -> Acc + reconcileNode(X, Conf) end, 
                             0, merkle_tree:get_childs(Node))
    end.

-spec reconcileLeaf(Node, {Dest::RPid, SID, OwnerLocal::LPid, OwnerRemote::RPid}) -> 1 when
    is_subtype(LPid,    comm:erl_local_pid()),
    is_subtype(RPid,    comm:mypid() | undefined),
    is_subtype(Node,    merkle_tree:mt_node()),
    is_subtype(SID,     rrepair:session_id()).
reconcileLeaf(_, {undefined, _, _, _}) -> erlang:error("Recon Destination PID undefined");
reconcileLeaf(Node, {Dest, SID, OwnerL, OwnerR}) ->
    ToSync = lists:map(fun(KeyVer) -> 
                           case decodeBlob(KeyVer) of
                               {K, _} -> K;
                               _ -> KeyVer
                            end
                       end, 
                       merkle_tree:get_bucket(Node)),
    comm:send_local(OwnerL, {request_resolve, SID, {key_upd_send, Dest, ToSync}, [{feedback, OwnerR}]}),
    1.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% art recon
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec art_recon(MyTree, Art, State) -> {ok, Stats} when
    is_subtype(MyTree, merkle_tree:merkle_tree()),
    is_subtype(Art,    art:art()),
    is_subtype(State,  state()),
    is_subtype(Stats,  rr_recon_stats:stats()).
art_recon(Tree, Art, #rr_recon_state{ dest_rr_pid = DestPid,
                                      ownerLocalPid = OwnerL,
                                      ownerRemotePid = OwnerR,
                                      stats = Stats }) ->
    SID = rr_recon_stats:get(session_id, Stats),
    case merkle_tree:get_interval(Tree) =:= art:get_interval(Art) of
        true -> 
            {ASyncLeafs, NStats} = 
                art_get_sync_leafs([merkle_tree:get_root(Tree)], Art, Stats, []),
            _ = [reconcileLeaf(X, {DestPid, SID, OwnerL, OwnerR}) || X <- ASyncLeafs],
            {ok, NStats};
        false -> {ok, Stats}
    end.

-spec art_get_sync_leafs(Nodes::NodeL, Art, Stats, Acc::NodeL) -> {ToSync::NodeL, Stats} when
    is_subtype(NodeL,   [merkle_tree:mt_node()]),
    is_subtype(Art,    art:art()),
    is_subtype(Stats,  rr_recon_stats:stats()).
art_get_sync_leafs([], _Art, Stats, ToSyncAcc) ->
    {ToSyncAcc, rr_recon_stats:inc([{resolve_started, length(ToSyncAcc) * 2}], Stats)};
art_get_sync_leafs([Node | ToCheck], Art, OStats, ToSyncAcc) ->
    Stats = rr_recon_stats:inc([{tree_nodesCompared, 1}], OStats),
    IsLeaf = merkle_tree:is_leaf(Node),
    case art:lookup(Node, Art) of
        true ->
            NStats = rr_recon_stats:inc([{tree_compareSkipped, ?IIF(IsLeaf, 0, merkle_tree:size(Node))}], 
                                         Stats),
            art_get_sync_leafs(ToCheck, Art, NStats, ToSyncAcc);
        false ->
            case IsLeaf of
                true ->
                    NStats = rr_recon_stats:inc([{tree_leafsSynced, 1}], Stats),
                    art_get_sync_leafs(ToCheck, Art, NStats, [Node | ToSyncAcc]);
                false ->
                    art_get_sync_leafs(
                           lists:append(merkle_tree:get_childs(Node), ToCheck), 
                           Art, Stats, ToSyncAcc)
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_recon_struct(Method, DB_Chunk) -> Recon_Struct when
      is_subtype(Method,       method()),
      is_subtype(DB_Chunk,     {intervals:interval(), db_as_list_enc()}),
      is_subtype(Recon_Struct, bloom_recon_struct() | merkle_tree:merkle_tree() | art:art()).
build_recon_struct(bloom, {I, DBItems}) ->    
    Fpr = get_fpr(),
    ElementNum = length(DBItems),
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    BF = ?REP_BLOOM:new(ElementNum, Fpr, ?REP_HFS:new(HFCount), DBItems),
    #bloom_recon_struct{ interval = I, bloom = BF };
build_recon_struct(merkle_tree, {I, DBItems}) ->
    merkle_tree:new(I, DBItems, [{branch_factor, get_merkle_branch_factor()}, 
                                 {bucket_size, get_merkle_bucket_size()}]);
build_recon_struct(art, Chunk) ->
    art:new(build_recon_struct(merkle_tree, Chunk), get_art_config()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Sends get_chunk request to local DHT_node process.
%      Request responses a list of encoded key-version pairs in ChunkI, 
%      where key is mapped to its assosiated key in MappedI.
-spec send_chunk_req(DhtPid::LPid, AnswerPid::LPid, ChunkI::I, DestI::I, MaxItems) -> ok when
    is_subtype(LPid,        comm:erl_local_pid()),
    is_subtype(I,           intervals:interval()),
    is_subtype(MaxItems,    pos_integer()).
send_chunk_req(DhtPid, SrcPid, I, DestI, MaxItems) ->
    comm:send_local(
      DhtPid, 
      {get_chunk, SrcPid, I, 
       fun(Item) -> db_entry:get_version(Item) =/= -1 end,
       fun(Item) -> encodeBlob(minKeyInInterval(db_entry:get_key(Item), DestI), 
                               db_entry:get_version(Item)) 
       end,
       MaxItems}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec exit_reason_to_rc_status(exit_reason()) -> rr_recon_stats:status().
exit_reason_to_rc_status(negotiate_interval) -> wait;
exit_reason_to_rc_status(sync_finished) -> finish;
exit_reason_to_rc_status(sync_finished_remote) -> finish;
exit_reason_to_rc_status(_) -> abort.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc selects a random associated key of an interval ending
-spec select_sync_node(intervals:interval()) -> ?RT:key().
select_sync_node(Interval) ->
    {_, LKey, RKey, _} = intervals:get_bounds(Interval),
    Key = ?RT:get_split_key(LKey, RKey, {1, randoms:rand_uniform(1, 50)}),
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
    erlang:element(2, lists:foldl(fun(X, {Status, Nr} = Acc) ->
                                          case X =:= Key of
                                              true when Status =:= no -> {yes, Nr};
                                              false when Status =:= no -> {no, Nr + 1};
                                              _ -> Acc
                                          end
                                  end, {no, 1}, Keys)).

% @doc Returns the quadrant in which a given interval begins.
-spec get_interval_quadrant(intervals:interval()) -> pos_integer().
get_interval_quadrant(I) ->
    {LBr, LKey, RKey, _} = intervals:get_bounds(I),
    case LBr of
        '[' -> get_key_quadrant(LKey);
        '(' -> get_key_quadrant(?RT:get_split_key(LKey, RKey, {1, 2}))               
    end.

-spec add_quadrants_to_key(?RT:key(), non_neg_integer(), pos_integer()) -> ?RT:key().
add_quadrants_to_key(Key, Add, RepFactor) ->
    Dest = get_key_quadrant(Key) + Add,
    Rep = RepFactor + 1,
    case Dest div Rep of
        1 -> map_key_to_quadrant(Key, (Dest rem Rep) + 1);
        0 -> map_key_to_quadrant(Key, Dest)
    end.            

% @doc Maps an arbitrary Interval to an Interval laying or starting in 
%      the given RepQuadrant. The replication degree X divides the keyspace into X replication qudrants.
%      Interval has to be continuous!
-spec mapInterval(intervals:interval(), RepQuadrant::pos_integer()) -> intervals:interval().
mapInterval(I, Q) ->
    {LBr, LKey, RKey, RBr} = intervals:get_bounds(I),
    LQ = get_key_quadrant(LKey),
    RepFactor = rep_factor(),
    QDiff = (RepFactor - LQ + Q) rem RepFactor,
    intervals:new(LBr,
                  add_quadrants_to_key(LKey, QDiff, RepFactor),
                  add_quadrants_to_key(RKey, QDiff, RepFactor),
                  RBr).

% @doc Gets intersection of two associated intervals as sub interval of A.
-spec find_intersection(intervals:interval(), intervals:interval()) -> intervals:interval().
find_intersection(A, B) ->
    lists:foldl(fun(Q, Acc) ->
                        Sec = intervals:intersection(A, mapInterval(B, Q)),
                        case intervals:is_empty(Acc) 
                                 andalso not intervals:is_empty(Sec) of
                            true -> Sec;
                            false -> Acc
                        end
                    end, intervals:empty(), lists:seq(1, rep_factor())).

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

-spec rep_factor() -> pos_integer().
rep_factor() ->
    4.    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc init module
-spec init(state()) -> state().
init(State) ->
    Mon = erlang:monitor(process, State#rr_recon_state.ownerLocalPid),
    State#rr_recon_state{ ownerMonitor = Mon }.

-spec start(rrepair:session_id() | null) -> {ok, pid()}.
start(SessionId) -> start(SessionId, undefined).

-spec start(SessionId, SenderRRPid) -> {ok, pid()} when
      is_subtype(SessionId,     rrepair:session_id() | null),
      is_subtype(SenderRRPid,   comm:mypid() | undefined).
start(SessionId, SenderRRPid) ->
    State = #rr_recon_state{ ownerLocalPid = self(), 
                             ownerRemotePid = comm:this(), 
                             dhtNodePid = pid_groups:get_my(dht_node),
                             dest_rr_pid = SenderRRPid,
                             stats = rr_recon_stats:new([{session_id, SessionId}]) },
    gen_component:start(?MODULE, fun ?MODULE:on/2, State, []).

-spec fork_recon(state()) -> {ok, pid()}.
fork_recon(Conf) ->
    NStats = rr_recon_stats:set([{session_id, null}], Conf#rr_recon_state.stats),    
    State = Conf#rr_recon_state{ stats = NStats },
    comm:send_local(Conf#rr_recon_state.ownerLocalPid, {recon_forked}),
    gen_component:start(?MODULE, fun ?MODULE:on/2, State, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config parameter handling
%
% rr_bloom_fpr              - bloom filter false positive rate (fpr)
% rr_max_items              - max. number of items per bloom filter, if node data count 
%                             exeeds rr_max_items two or more bloom filter will be used
% rr_art_inner_fpr          - 
% rr_art_leaf_fpr           -  
% rr_art_correction_factor  - 
% rr_merkle_branch_factor   - merkle tree branching factor thus number of childs per node
% rr_merkle_bucket_size     - size of merkle tree leaf buckets
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks wheter config parameter is float and in [0,1].
-spec check_percent(atom()) -> boolean().
check_percent(Atom) ->
    config:cfg_is_float(Atom) andalso
        config:cfg_is_greater_than(Atom, 0) andalso
        config:cfg_is_less_than(Atom, 1).

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rrepair_enabled) of
        true ->                
            check_percent(rr_bloom_fpr) andalso
                check_percent(rr_art_inner_fpr) andalso
                check_percent(rr_art_leaf_fpr) andalso
                config:cfg_is_integer(rr_art_correction_factor) andalso
                config:cfg_is_greater_than(rr_art_correction_factor, 0) andalso
                config:cfg_is_integer(rr_merkle_branch_factor) andalso
                config:cfg_is_greater_than(rr_merkle_branch_factor, 1) andalso
                config:cfg_is_integer(rr_merkle_bucket_size) andalso
                config:cfg_is_greater_than(merkle_bucket_size, 0) andalso
                config:cfg_is_integer(rr_max_items) andalso
                config:cfg_is_greater_than(rr_max_items, 0);
        _ -> true
    end.

-spec get_fpr() -> float().
get_fpr() ->
    config:read(rr_bloom_fpr).

-spec get_max_items(method()) -> pos_integer() | all.
get_max_items(Method) ->
    case Method of
        merkle_tree -> all;
        art -> all;
        _ -> config:read(rr_max_items)
    end.

-spec get_merkle_branch_factor() -> pos_integer().
get_merkle_branch_factor() ->
    config:read(rr_merkle_branch_factor).

-spec get_merkle_bucket_size() -> pos_integer().
get_merkle_bucket_size() ->
    config:read(rr_merkle_bucket_size).

-spec get_art_config() -> art:config().
get_art_config() ->
    [{correction_factor, config:read(rr_art_correction_factor)},
     {inner_bf_fpr, config:read(rr_art_inner_fpr)},
     {leaf_bf_fpr, config:read(rr_art_leaf_fpr)}].    
