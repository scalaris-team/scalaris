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
-export([map_key_to_interval/2, map_key_to_quadrant/2]).

%export for testing
-export([encodeBlob/2, decodeBlob/1,
         map_interval/2, 
         get_key_quadrant/1, get_interval_quadrant/1,
         find_intersection/2,
         get_interval_size/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

%DETAIL DEBUG MESSAGES
-define(TRACE2(X,Y), ok).
%-define(TRACE2(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

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
                          build_struct |            %client send its struct (bloom/art) to initiator and exits
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

-type merkle_cmp_request() :: {intervals:interval(), merkle_tree:mt_node_key()}.
-type merkle_cmp_result()  ::
          {intervals:interval(), ok_inner | ok_leaf | not_found | fail_leaf} |
          {intervals:interval(), fail_inner, [merkle_tree:mt_node_key()]}.

-type request() :: 
    {start, method(), DestKey::recon_dest()} |
    {continue, method(), stage(), struct(), Initiator::boolean()}.          

-type message() ::          
    %API
    request() |
    %tree sync msgs
    {check_nodes, InitiatorPid::comm:mypid(), [merkle_cmp_request()]} |
    {check_nodes_response, [merkle_cmp_result()]} |
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
        true ->
            RMethod =:= merkle_tree andalso fd:subscribe(DestRRPid),
            send_chunk_req(DhtPid, self(), MyIntersec, DestI, get_max_items(RMethod));
        false ->
            comm:send_local(self(), {shutdown, negotiate_interval}),
            DestReconPid =/= undefined andalso
                comm:send(DestReconPid, {shutdown, empty_interval})
    end,    
    State#rr_recon_state{ stage = build_struct, dest_recon_pid = DestReconPid };

on({get_state_response, MyI}, State = 
       #rr_recon_state{ stage = reconciliation,
                        method = bloom,
                        dhtNodePid = DhtPid,
                        struct = #bloom_recon_struct{ interval = BloomI}
                       }) ->
    MySyncI = find_intersection(MyI, BloomI),
    case intervals:is_empty(MySyncI) of
        false -> send_chunk_req(DhtPid, self(), MySyncI, BloomI, get_max_items(bloom));
        true -> comm:send_local(self(), {shutdown, empty_interval})
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
           SubSyncI = find_intersection(SyncI, RestI),
           case intervals:is_empty(SubSyncI) of
               false ->            
                   Pid = if RMethod =:= bloom -> 
                                ForkParams = lists:keyreplace(interval, 1, Params, {interval, SubSyncI}),
                                erlang:element(2, fork_recon(State#rr_recon_state{ struct = ForkParams }));
                            true -> self()
                         end,
                   send_chunk_req(DhtNodePid, Pid, find_intersection(RestI, SyncI), SubSyncI, 
                                  get_max_items(RMethod));
               true -> ok
           end;
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
                        struct = #bloom_recon_struct{ bloom = BF },
                        stats = Stats }) ->
    %if rest interval is non empty start another sync    
    SID = rr_recon_stats:get(session_id, Stats),
    SyncFinished = intervals:is_empty(RestI),
    not SyncFinished andalso
        send_chunk_req(DhtNodePid, self(), RestI, RestI, get_max_items(bloom)),
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

on({'DOWN', _MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({check_nodes, SenderPid, ToCheck}, State = #rr_recon_state{ struct = Tree }) ->    
    Result = [check_node(X, Tree) || X <- ToCheck], %afraid of parallel (util:par_map) which causes lots tree copies
    comm:send(SenderPid, {check_nodes_response, Result}),
    State#rr_recon_state{ dest_recon_pid = SenderPid };

on({check_nodes_response, CmpResults}, State =
       #rr_recon_state{ dest_recon_pid = DestReconPid,
                        dest_rr_pid = SrcNode,
                        stats = Stats, 
                        ownerLocalPid = OwnerL,
                        ownerRemotePid = OwnerR,
                        struct = Tree }) ->
    SID = rr_recon_stats:get(session_id, Stats),
    {Req, Res, NStats} = process_tree_cmp_result(CmpResults, Tree, Stats),
    Req =/= [] andalso
        comm:send(DestReconPid, {check_nodes, comm:this(), Req}),    
    {Leafs, Resolves} = lists:foldl(fun(Node, {AccL, AccR}) -> 
                                            {LCount, RCount} = resolve_node(Node, {SrcNode, SID, OwnerL, OwnerR}),
                                            {AccL + LCount, AccR + RCount}
                                    end, {0, 0}, Res),
    FStats = rr_recon_stats:inc([{tree_leafsSynced, Leafs}, {resolve_started, Resolves}], NStats),
    CompLeft = rr_recon_stats:get(tree_compareLeft, FStats),
    if CompLeft =:= 0 ->
           comm:send(DestReconPid, {shutdown, sync_finished_remote}),
           comm:send_local(self(), {shutdown, sync_finished});
       true -> ok
    end,
    State#rr_recon_state{ stats = FStats }.

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
                                  {check_nodes, comm:this(), 
                                   [{merkle_tree:get_interval(SyncStruct), 
                                     merkle_tree:get_hash(SyncStruct)}]});
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
            comm:send_local(self(), {shutdown, ?IIF(AOk =:= ok, sync_finished, build_struct)}),
            ARStats
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec check_node({intervals:interval(), merkle_tree:mt_node_key()}, merkle_tree:merkle_tree()) -> merkle_cmp_result().
check_node({I, Hash}, Tree) ->
    Node = merkle_tree:lookup(I, Tree),
    case Node of
        not_found -> {I, not_found};
        _ ->
            IsLeaf = merkle_tree:is_leaf(Node),
            case merkle_tree:get_hash(Node) =:= Hash of
                true when IsLeaf -> {I, ok_leaf};
                true when not IsLeaf -> {I, ok_inner};
                false when IsLeaf -> {I, fail_leaf};
                false when not IsLeaf ->
                    {I, fail_inner, [merkle_tree:get_hash(N) || N <- merkle_tree:get_childs(Node)]} 
            end
    end.

-spec process_tree_cmp_result([merkle_cmp_result()], merkle_tree:merkle_tree(), Stats) ->
          {Requests, Resolve, New::Stats}
    when
      is_subtype(Stats,     rr_recon_stats:stats()),
      is_subtype(Requests,  [merkle_cmp_request()]),
      is_subtype(Resolve,   [merkle_tree:mt_node()]).
process_tree_cmp_result(CmpResult, Tree, Stats) ->
    Compared = length(CmpResult),
    NStats = rr_recon_stats:inc([{tree_compareLeft, -Compared}, 
                                 {tree_nodesCompared, Compared}], Stats),
    process_tree_cmp_result(CmpResult, Tree, [], [], NStats).

-spec process_tree_cmp_result([merkle_cmp_result()], Tree, AccR::Requests, AccRes::Resolve, Old::Stats) -> {NewR::Requests, NewRes::Resolve, New::Stats}
    when
        is_subtype(Tree,        merkle_tree:merkle_tree()),
        is_subtype(Resolve,     [merkle_tree:mt_node()]),
        is_subtype(Stats,       rr_recon_stats:stats()),
        is_subtype(Requests,    [merkle_cmp_request()]).
process_tree_cmp_result([], _Tree, AccReq, AccRes, Stats) ->
    {AccReq, AccRes, Stats};
process_tree_cmp_result([{_, not_found} | T], Tree, AccReq, AccRes, Stats) ->
    NStats = rr_recon_stats:inc([{error_count, 1}], Stats),
    process_tree_cmp_result(T, Tree, AccReq, AccRes, NStats);
process_tree_cmp_result([{_, ok_leaf} | T], Tree, AccReq, AccRes, Stats) ->
    NStats = rr_recon_stats:inc([{tree_compareSkipped, 1}], Stats),
    process_tree_cmp_result(T, Tree, AccReq, AccRes, NStats);
process_tree_cmp_result([{I, ok_inner} | T], Tree, AccReq, AccRes, Stats) ->
    Node = merkle_tree:lookup(I, Tree),
    NStats = rr_recon_stats:inc([{tree_compareSkipped, merkle_tree:size(Node)}], Stats),
    process_tree_cmp_result(T, Tree, AccReq, AccRes, NStats);
process_tree_cmp_result([{I, fail_leaf} | T], Tree, AccReq, AccRes, Stats) ->
    Node = merkle_tree:lookup(I, Tree),
    process_tree_cmp_result(T, Tree, AccReq, [Node | AccRes], Stats);
process_tree_cmp_result([{I, fail_inner, ChildHashs} | T], Tree, AccReq, AccRes, Stats) ->
    Node = merkle_tree:lookup(I, Tree),
    case merkle_tree:is_leaf(Node) of
        false -> 
            MyChilds = merkle_tree:get_childs(Node),
            {Matched, NotMatched} = compareNodes(MyChilds, ChildHashs, {[], []}),
            Skipped = lists:foldl(fun(MNode, Acc) -> 
                                          Acc + case merkle_tree:is_leaf(MNode) of
                                                    true -> 0;
                                                    false -> merkle_tree:size(MNode) - 1
                                                end
                                  end, 0, Matched),
            NewReq = [{merkle_tree:get_interval(X), merkle_tree:get_hash(X)} || X <- NotMatched],
            NStats = rr_recon_stats:inc([{tree_compareLeft, length(NotMatched)},
                                          {tree_nodesCompared, length(Matched)},
                                          {tree_compareSkipped, Skipped}], Stats),
            process_tree_cmp_result(T, Tree, lists:append(NewReq, AccReq), AccRes, NStats);
        true -> 
            process_tree_cmp_result(T, Tree, AccReq, [Node | AccRes], Stats)
    end.

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

% @doc Starts one resolve process per leaf node in a given node
%      Returns: number of visited leaf nodes and number of leaf resovle requests.
-spec resolve_node(Node | not_found, {Dest::RPid, SID, OwnerLocal::LPid, OwnerRemote::RPid}) -> {Leafs::non_neg_integer(), ResolveReq::non_neg_integer()} when
    is_subtype(LPid,    comm:erl_local_pid()),
    is_subtype(RPid,    comm:mypid() | undefined),
    is_subtype(Node,    merkle_tree:mt_node()),
    is_subtype(SID,     rrepair:session_id()).
resolve_node(not_found, _) -> {0, 0};
resolve_node(Node, Conf) ->
    case merkle_tree:is_leaf(Node) of
        true -> {1, resolve_leaf(Node, Conf)};
        false -> lists:foldl(fun(X, {AccL, AccR}) ->  
                                     {LCount, RCount} = resolve_node(X, Conf),
                                     {AccL + LCount, AccR + RCount} 
                             end, 
                             {0, 0}, merkle_tree:get_childs(Node))
    end.

% @doc Returns number ob caused resolve requests (requests with feedback count 2)
-spec resolve_leaf(Node, {Dest::RPid, SID, OwnerLocal::LPid, OwnerRemote::RPid}) -> 1 | 2 when
    is_subtype(LPid,    comm:erl_local_pid()),
    is_subtype(RPid,    comm:mypid() | undefined),
    is_subtype(Node,    merkle_tree:mt_node()),
    is_subtype(SID,     rrepair:session_id()).
resolve_leaf(_, {undefined, _, _, _}) -> erlang:error("Recon Destination PID undefined");
resolve_leaf(Node, {Dest, SID, OwnerL, OwnerR}) ->
    case merkle_tree:get_item_count(Node) of
        0 ->
           comm:send(Dest, {request_resolve, SID, {interval_upd_send, merkle_tree:get_interval(Node), OwnerR}, []}),
           1;
       _ ->
           comm:send_local(OwnerL, {request_resolve, SID, {interval_upd_send, merkle_tree:get_interval(Node), Dest}, [{feedback, OwnerR}]}),
           2
    end.

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
            {ASyncLeafs, Stats2} = art_get_sync_leafs([merkle_tree:get_root(Tree)], Art, Stats, []),
            ResolveCalled = lists:foldl(fun(X, Acc) ->
                                                Acc + resolve_leaf(X, {DestPid, SID, OwnerL, OwnerR})
                                        end, 0, ASyncLeafs),
            Stats3 = rr_recon_stats:inc([{resolve_started, ResolveCalled}], Stats2),
            {ok, Stats3};
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
            NStats = rr_recon_stats:inc([{tree_compareSkipped, ?IIF(IsLeaf, 0, merkle_tree:size(Node))}], Stats),
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
       fun(Item) ->
               case map_key_to_interval(db_entry:get_key(Item), DestI) of
                   none -> encodeBlob(?MINUS_INFINITY, 0); %TODO should be filtered
                   Key -> encodeBlob(Key, db_entry:get_version(Item))
               end
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

% @doc Maps any key (K) into a given interval (I). If K is already in I, K is returned.
%      If K has more than one associated keys in I, the closest one is returned.
%      If all associated keys of K are not in I, none is returned.
-spec map_key_to_interval(?RT:key(), intervals:interval()) -> ?RT:key() | none.
map_key_to_interval(Key, I) ->
    RGrp = [K || K <- ?RT:get_replica_keys(Key), intervals:in(K, I)],
    case RGrp of
        [] -> none;
        [R] -> R;
        [_|_] -> RGrpDis = [case X of
                                Key -> {X, 0};
                                _ -> {X, erlang:min(?RT:get_range(Key, X), ?RT:get_range(X, Key))}
                            end || X <- RGrp],
                 element(1, erlang:hd(lists:keysort(2, RGrpDis)))
    end.

% @doc Maps an abitrary key to its associated key in replication quadrant N.
-spec map_key_to_quadrant(?RT:key(), pos_integer()) -> ?RT:key().
map_key_to_quadrant(Key, N) ->
    lists:nth(N, lists:sort(?RT:get_replica_keys(Key))).

% @doc Returns the replication quadrant number (not null-based) in which key is located.
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

% @doc Maps an arbitrary Interval into the given replication quadrant. 
%      The replication degree X divides the keyspace into X replication quadrants.
%      Precondition: Interval (I) is continuous!
%      Result: Continuous left-open interval starting or laying in given RepQuadrant.
-spec map_interval(intervals:interval(), RepQuadrant::pos_integer()) -> intervals:interval().
map_interval(I, Q) ->    
    case intervals:is_all(I) of
        false ->
            {LBr, LKey, RKey, RBr} = intervals:get_bounds(I),
            LQ = get_key_quadrant(LKey),
            RepFactor = rep_factor(),
            QDiff = (RepFactor - LQ + Q) rem RepFactor,
            NewLKey = add_quadrants_to_key(LKey, QDiff, RepFactor),
            NewRKey = add_quadrants_to_key(RKey, QDiff, RepFactor),
            intervals:new(LBr, NewLKey, NewRKey, RBr);
        true -> I
    end.

% @doc Gets intersection of two associated intervals as sub interval of A.
-spec find_intersection(intervals:interval(), intervals:interval()) -> intervals:interval().
find_intersection(A, B) ->
    SecI = lists:foldl(fun(Q, Acc) ->
                               Sec = intervals:intersection(A, map_interval(B, Q)),
                               case intervals:is_empty(Sec) orelse 
                                         not intervals:is_continuous(Sec) of
                                   false -> [Sec | Acc];
                                   true -> Acc
                               end
                       end, [], lists:seq(1, rep_factor())),
    case SecI of
        [] -> intervals:empty();
        [I] -> I;
        [H|T] ->
            element(2, lists:foldl(fun(X, {ASize, _AI} = XAcc) ->
                                           XSize = get_interval_size(X),
                                           if XSize >= ASize -> {XSize, X};
                                              true -> XAcc
                                           end
                                   end, {get_interval_size(H), H}, T))
    end.

% @doc Note: works only on continuous intervals
-spec get_interval_size(intervals:interval()) -> number().
get_interval_size(I) ->
    case intervals:is_all(I) of        
        false ->
            case intervals:is_empty(I) of
                false ->
                    {'(', LKey, RKey, ']'} = intervals:get_bounds(I),
                    ?RT:get_range(LKey, RKey);
                true -> 0
            end;
        true -> ?RT:n()
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec encodeBlob(?RT:key(), ?DB:value() | ?DB:version()) -> db_entry_enc().
encodeBlob(A, B) -> 
    term_to_binary([A, "#", B]).

-spec decodeBlob(db_entry_enc()) -> {?RT:key(), ?DB:value() | ?DB:version()} | fail.
decodeBlob(Blob) when is_binary(Blob) ->
    case binary_to_term(Blob) of
        [Key, "#", X] ->  {Key, X};
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
