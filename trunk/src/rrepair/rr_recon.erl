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
%% @doc    replica reconcilication protocol
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
         dhtNodePid         = ?required(rr_recon_state, dhtNodePid)     :: comm:erl_local_pid(),
         dest_key           = random                                    :: recon_dest(),             %dest dht node pid
         dest_rr_pid        = undefined                                 :: comm:mypid() | undefined, %dest rrepair pid
         dest_recon_pid     = undefined                                 :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                                 :: method(),                 %reconciliation method
         struct             = {}                                        :: struct() | {},
         stage              = reconciliation                            :: stage(),
         master             = false                                     :: boolean(),               %true if process is recon leader/initiator
         round              = 0                                         :: rrepair:round(),
         stats              = rr_recon_stats:new()                      :: rr_recon_stats:stats()
         }).
-type state() :: #rr_recon_state{}.

-type tree_cmp_response() :: ok_inner | ok_leaf |
                             fail_leaf | fail_node |
                             not_found.

-type request() :: 
    {start, method(), DestKey::recon_dest()} |
    {continue, method(), stage(), struct(), Master::boolean()}.          

-type message() ::          
    %API
    request() |
    %tree sync msgs
    {check_node, MasterPid::comm:mypid(), intervals:interval(), merkle_tree:mt_node_key()} |
    {check_node_response, tree_cmp_response(), intervals:interval(), [merkle_tree:mt_node_key()]} |
    %dht node response
    {get_state_response, intervals:interval()} |
    {get_chunk_response, db_chunk_enc()} |          
    %misc (internal)
    {shutdown, exit_reason()} | 
    {crash, DeadPid::comm:mypid()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = build_struct,
                        method = bloom,
                        dhtNodePid = DhtNodePid }) ->
    send_chunk_req(DhtNodePid, self(), MyI, mapInterval(MyI, 1), get_max_items(bloom)),
    State#rr_recon_state{ struct = [{interval, MyI}] };

on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = req_shared_interval, 
                        master = true,
                        method = RMethod }) ->
    ReconReq = {continue, RMethod, req_shared_interval, [{interval, MyI}], false},
    start_to_dest(MyI, ReconReq, State),
    comm:send_local(self(), {shutdown, negotiate_interval_master}),
    State;

on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = req_shared_interval,
                        master = false,
                        method = Method,
                        struct = Params,                        
                        dhtNodePid = DhtPid,
                        dest_rr_pid = DestRRPid }) ->
    Method =:= merkle_tree andalso fd:subscribe(DestRRPid),
    SrcI = proplists:get_value(interval, Params),
    Intersec = find_intersection(MyI, SrcI),
    case intervals:is_empty(Intersec) of
        true -> comm:send_local(self(), {shutdown, intersection_empty});
        false -> send_chunk_req(DhtPid, self(), Intersec, Intersec, get_max_items(Method))
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
    DestReconPid = proplists:get_value(reconPid, Params),
    MyIntersec = find_intersection(MyI, DestI),
    case intervals:is_subset(MyIntersec, MyI) and not intervals:is_empty(MyIntersec) of
        false ->
            comm:send_local(self(), {shutdown, negotiate_interval_master}),            
            comm:send(DestReconPid, {shutdown, no_interval_intersection});
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
    MyBloomI = mapInterval(BloomI, get_interval_quadrant(MyI)),
    MySyncI = intervals:intersection(MyI, MyBloomI),
    ?TRACE("GET STATE - MyI=~p ~n BloomI=~p ~n BloomIMapped=~p ~n SynI=~p", [MyI, BloomI, MyBloomI, MySyncI]),
    case intervals:is_empty(MySyncI) of
        true -> comm:send_local(self(), {shutdown, empty_interval});
        false -> send_chunk_req(DhtPid, self(), MySyncI, 
                                mapInterval(MySyncI, 1), get_max_items(bloom))
    end,
    State;

on({get_chunk_response, {_, []}}, State) ->
    comm:send_local(self(), {shutdown, req_chunk_is_empty}),
    State;

on({get_chunk_response, {RestI, DBList}}, State =
       #rr_recon_state{ stage = build_struct,
                        method = RMethod,        
                        struct = Params,                    
                        round = Round,
                        dhtNodePid = DhtNodePid,
                        master = SyncMaster,
                        stats = Stats }) ->
    SyncI = proplists:get_value(interval, Params),    
    ToBuild = ?IIF(RMethod =:= art, ?IIF(SyncMaster, merkle_tree, art), RMethod),
    {BuildTime, SyncStruct} =
        case merkle_tree:is_merkle_tree(Params) of
            true ->
                {BTime, NTree} = util:tc(fun() -> merkle_tree:insert_list(DBList, Params) end),
                {rr_recon_stats:get(build_time, Stats) + BTime, merkle_tree:gen_hash(NTree) };
            false -> util:tc(fun() -> build_recon_struct(ToBuild, {SyncI, DBList}) end)
        end,
    EmptyRest = intervals:is_empty(RestI),
    if not EmptyRest ->
            Pid = if RMethod =:= bloom -> {ok, P} = fork_recon(State, Round), P;
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
                        dest_rr_pid = DestRU_Pid,
                        struct = #bloom_recon_struct{ interval = BloomI, 
                                                            keyBF = KeyBF,
                                                            versBF = VersBF},
                        round = Round }) ->
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
    
on({start, Method, DestKey}, State) ->
    comm:send_local(State#rr_recon_state.dhtNodePid, {get_state, comm:this(), my_range}),
    Stage = case Method of
                bloom -> build_struct;
                merkle_tree -> req_shared_interval;        
                art -> req_shared_interval
            end,
    State#rr_recon_state{ stage = Stage, 
                          struct = {},
                          method = Method,
                          dest_key = DestKey,
                          master = true };

on({continue, Method, Stage, Struct, Master}, State) ->
    comm:send_local(State#rr_recon_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#rr_recon_state{ stage = Stage, 
                          struct = Struct,
                          method = Method,
                          master = Master orelse Stage =:= res_shared_interval };

on({crash, Pid}, State) ->
    comm:send_local(self(), {shutdown, {fail, crash_of_recon_node, Pid}}),
    State;

on({shutdown, Reason}, #rr_recon_state{ ownerLocalPid = Owner, 
                                        round = Round,
                                        stats = Stats,
                                        master = Master }) ->
    ?TRACE("SHUTDOWN Round=~p Reason=~p", [Round, Reason]),
    NewStats = rr_recon_stats:set([{finish, Reason =:= sync_finished}], Stats),
    comm:send_local(Owner, {recon_progress_report, self(), Round, Master, NewStats}),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({check_node, SenderPid, Interval, Hash}, State =
       #rr_recon_state{ struct = Tree }) ->
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
                        ownerLocalPid = OwnerPid,
                        struct = Tree,
                        round = Round }) ->
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
                                      comm:send(DestReconPid, 
                                                {check_node,
                                                 comm:this(), 
                                                 merkle_tree:get_interval(X), 
                                                 merkle_tree:get_hash(X)}) 
                              end, NotMatched),
                [{tree_compareLeft, length(NotMatched)},
                 {tree_nodesCompared, length(Matched)},
                 {tree_compareSkipped, SkippedSubNodes}]    
        end,
    FinalStats = rr_recon_stats:inc(IncOps ++ [{tree_compareLeft, -1}, 
                                               {tree_nodesCompared, 1}], Stats),
    CompLeft = rr_recon_stats:get(tree_compareLeft, FinalStats),
    if CompLeft =< 1 ->
           comm:send(DestReconPid, {shutdown, sync_finished_remote_shutdown}),
           comm:send_local(self(), {shutdown, sync_finished});
       true -> ok
    end,
    State#rr_recon_state{ stats = FinalStats }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec begin_sync(struct(), state()) -> rr_recon_stats:stats().
begin_sync(SyncStruct, State = #rr_recon_state{ method = Method,
                                                struct = Params,
                                                round = Round,
                                                ownerRemotePid = OwnerPid,
                                                dest_recon_pid = DestReconPid,
                                                dest_rr_pid = DestRRPid,                                       
                                                master = IsMaster, 
                                                stats = Stats }) ->
    ?TRACE("BEGIN SYNC", []),
    case Method of
        merkle_tree -> 
            case IsMaster of
                true -> comm:send(DestReconPid, 
                                  {check_node, comm:this(), 
                                   merkle_tree:get_interval(SyncStruct), 
                                   merkle_tree:get_hash(SyncStruct)});            
                false ->
                    IntParams = [{interval, merkle_tree:get_interval(SyncStruct)}, {reconPid, comm:this()}],
                    comm:send(DestRRPid, 
                              {continue_recon, OwnerPid, Round, 
                               {continue, merkle_tree, res_shared_interval, IntParams, true}})
            end,
            rr_recon_stats:set(
              [{tree_compareLeft, ?IIF(IsMaster, 1, 0)},
               {tree_size, merkle_tree:size_detail(SyncStruct)}], Stats);
        bloom when IsMaster ->
            start_to_dest(proplists:get_value(interval, Params), 
                          {continue, bloom, reconciliation, SyncStruct, false}, 
                          State),
            comm:send_local(self(), {shutdown, {ok, build_struct}}),
            Stats;
        art ->
            {AOk, ARStats} = 
                case IsMaster of
                    true -> art_recon(SyncStruct, proplists:get_value(art, Params), State);                    
                    false ->
                        ArtParams = [{interval, art:get_interval(SyncStruct)},
                                     {reconPid, comm:this()},
                                     {art, SyncStruct}],
                        comm:send(DestRRPid, 
                                  {continue_recon, OwnerPid, Round, 
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
    Hash = merkle_tree:get_hash(N),
    case Hash =:= H of
        true -> compareNodes(Nodes, NodeHashs, {[N|Matched], NotMatched});
        false -> compareNodes(Nodes, NodeHashs, {Matched, [N|NotMatched]})
    end.

% @doc Starts simple sync for a given node, returns number of leaf sync requests.
-spec reconcileNode(Node | not_found, {Dest::Pid, Round, Owner::Pid}) -> non_neg_integer() when
    is_subtype(Pid,     comm:mypid() | undefined),
    is_subtype(Node,    merkle_tree:mt_node()),
    is_subtype(Round,   rrepair:round()).
reconcileNode(not_found, _) -> 0;
reconcileNode(Node, Conf) ->
    case merkle_tree:is_leaf(Node) of
        true -> reconcileLeaf(Node, Conf);
        false -> lists:foldl(fun(X, Acc) -> Acc + reconcileNode(X, Conf) end, 
                             0, merkle_tree:get_childs(Node))
    end.

-spec reconcileLeaf(Node, {Dest::Pid, Round, OwnRRepair::Pid}) -> 1 when
    is_subtype(Pid,     comm:mypid() | undefined),
    is_subtype(Node,    merkle_tree:mt_node()),
    is_subtype(Round,   rrepair:round()).
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
    is_subtype(Stats,  rr_recon_stats:stats()).
art_recon(Tree, Art, #rr_recon_state{ round = Round, 
                                      dest_rr_pid = DestPid,
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

-spec art_get_sync_leafs(Nodes::NodeL, Art, Stats, Acc::NodeL) -> {ToSync::NodeL, Stats} when
    is_subtype(NodeL,   [merkle_tree:mt_node()]),
    is_subtype(Art,    art:art()),
    is_subtype(Stats,  rr_recon_stats:stats()).
art_get_sync_leafs([], _Art, Stats, ToSyncAcc) ->
    {ToSyncAcc, Stats};
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
    Hfs = ?REP_HFS:new(HFCount),    
    {KeyBF, VerBF} = fill_bloom(DBItems, 
                                ?REP_BLOOM:new(ElementNum, Fpr, Hfs), 
                                ?REP_BLOOM:new(ElementNum, Fpr, Hfs)),
    #bloom_recon_struct{ interval = mapInterval(I, 1), keyBF = KeyBF, versBF = VerBF };

build_recon_struct(merkle_tree, {I, DBItems}) ->
    ?TRACE("START BUILD MERKLE", []),
    M = merkle_tree:new(I, DBItems, []),
    ?TRACE("END BUILD MERKLE", []),
    M;

build_recon_struct(art, Chunk) ->
    Tree = build_recon_struct(merkle_tree, Chunk),
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
-spec send_chunk_req(DhtPid::LPid, AnswerPid::LPid, ChunkI::I, DestI::I, MaxItems) -> ok when
    is_subtype(LPid,        comm:erl_local_pid()),
    is_subtype(I,           intervals:interval()),
    is_subtype(MaxItems,    pos_integer()).
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

filterPartitionMap(_, _, _, [], TrueL, FalseL) -> {TrueL, FalseL};
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

-spec start_to_dest(MyI, Req, State) -> ok 
when
  is_subtype(MyI,     intervals:interval()),
  is_subtype(Req,     request()),
  is_subtype(State,   state()).
start_to_dest(MyI, Req, #rr_recon_state{ dest_key = DestKey,
                                         dhtNodePid = DhtPid,
                                         round = Round,
                                         ownerRemotePid = OwnerPid }) ->
    Msg = {send_to_group_member, rrepair, {continue_recon, OwnerPid, Round, Req}},    
    DKey = case DestKey of
               random -> select_sync_node(MyI);        
               _ -> DestKey
           end,
    ?TRACE("START_TO_DEST ~p", [DestKey]),
    comm:send_local(DhtPid, {lookup_aux, DKey, 0, Msg}).

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
    {_, Q} = lists:foldl(fun(X, {Status, Nr} = Acc) ->
                                 case X =:= Key of
                                     true when Status =:= no -> {yes, Nr};
                                     false when Status =:= no -> {no, Nr + 1};
                                     _ -> Acc
                                 end
                         end, {no, 1}, Keys),
    Q.

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
    I = lists:foldl(fun(Q, Acc) ->
                            Sec = intervals:intersection(A, mapInterval(B, Q)),
                            case intervals:is_empty(Acc) 
                                     andalso not intervals:is_empty(Sec) of
                                true -> Sec;
                                false -> Acc
                            end
                    end, intervals:empty(), lists:seq(1, rep_factor())),
    I.
    %{_, RI} = intervals:get_elements(I),
    %?TRACE("--->FIND INTERSEC<------~nI=~p~nR=~p", [I, RI]),
    %RI.

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
    State.

-spec start(rrepair:round()) -> {ok, pid()}.
start(Round) -> start(Round, undefined).

-spec start(Round, SenderRRPid) -> {ok, pid()} when
      is_subtype(Round,         rrepair:round()),
      is_subtype(SenderRRPid,   comm:mypid() | undefined).
start(Round, SenderRRPid) ->
    State = #rr_recon_state{ ownerLocalPid = self(), 
                             ownerRemotePid = comm:this(), 
                             dhtNodePid = pid_groups:get_my(dht_node),
                             dest_rr_pid = SenderRRPid,
                             round = Round },
    gen_component:start(?MODULE, fun ?MODULE:on/2, State, []).

-spec fork_recon(state(), rrepair:round()) -> {ok, pid()}.
fork_recon(Conf, {ReconRound, Fork}) ->
    State = Conf#rr_recon_state{ round = {ReconRound, Fork + 1} },
    comm:send_local(Conf#rr_recon_state.ownerLocalPid, {recon_forked}),
    gen_component:start(?MODULE, fun ?MODULE:on/2, State, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rrepair_enabled) of
        true ->                
            config:cfg_is_float(rr_bloom_fpr) andalso
            config:cfg_is_greater_than(rr_bloom_fpr, 0) andalso
            config:cfg_is_less_than(rr_bloom_fpr, 1) andalso
            config:cfg_is_integer(rr_max_items) andalso
            config:cfg_is_greater_than(rr_max_items, 0);
        _ -> true
    end.

-spec get_fpr() -> float().
get_fpr() ->
    config:read(rr_bloom_fpr).

-spec get_max_items(method()) -> pos_integer().
get_max_items(Method) ->
    case Method of
        merkle_tree -> all;
        art -> all;
        _ -> config:read(rr_max_items)
    end.
