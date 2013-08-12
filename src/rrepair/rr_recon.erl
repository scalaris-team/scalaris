% @copyright 2011, 2012, 2013 Zuse Institute Berlin

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
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/2, check_config/0]).
-export([map_key_to_interval/2, map_key_to_quadrant/2, map_interval/2]).

%export for testing
-export([encodeBlob/2, decodeBlob/1,
         quadrant_intervals/0, find_sync_interval/2, quadrant_subints_/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), log:pal("~w: [ ~.0p ] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE_SEND(Pid, Msg), ?TRACE("to ~.0p: ~.0p~n", [Pid, Msg])).
-define(TRACE1(Msg, State),
        ?TRACE("~n  Msg: ~.0p~n"
               "  State: method: ~.0p;  stage: ~.0p;  initiator: ~.0p~n"
               "          destI: ~.0p~n"
               "         params: ~.0p~n",
               [Msg, State#rr_recon_state.method, State#rr_recon_state.stage,
                State#rr_recon_state.initiator, State#rr_recon_state.dest_interval,
                ?IIF(is_list(State#rr_recon_state.struct), State#rr_recon_state.struct, [])])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([method/0, request/0, db_entry_enc/0]).
-endif.

-type quadrant()       :: 1..4. % 1..rep_factor()
-type method()         :: bloom | merkle_tree | art.% | iblt.
-type stage()          :: req_shared_interval | build_struct | reconciliation.

-type exit_reason()    :: empty_interval |          %interval intersection between initator and client is empty
                          recon_node_crash |        %sync partner node crashed  
                          sync_finished |           %finish recon on local node
                          sync_finished_remote.     %client-side shutdown by merkle-tree recon initiator

-type db_entry_enc()   :: binary().
-type db_chunk_enc()   :: [db_entry_enc()].
-type db_chunk()       :: [{?RT:key(), db_dht:version()}].

-record(bloom_recon_struct,
        {
         interval = intervals:empty()                       :: intervals:interval(),
         bloom    = ?required(bloom_recon_struct, bloom)    :: bloom:bloom_filter()
        }).

-record(merkle_params,
        {
         interval = intervals:empty()                       :: intervals:interval(),
         reconPid = ?required(merkle_param, reconPid)       :: comm:mypid(),
         branch_factor = get_merkle_branch_factor()         :: pos_integer(),
         bucket_size   = get_merkle_bucket_size()           :: pos_integer()
        }).

-record(art_recon_struct,
        {
         art           = ?required(art_recon_struct, art)           :: art:art(),
         branch_factor = ?required(art_recon_struct, branch_factor) :: pos_integer(),
         bucket_size   = ?required(art_recon_struct, bucket_size)   :: pos_integer()
        }).

-type sync_struct() :: #bloom_recon_struct{} |
                       merkle_tree:merkle_tree() |
                       [merkle_tree:mt_node()] |
                       #art_recon_struct{}.
-type parameters() :: #bloom_recon_struct{} |
                      #art_recon_struct{} |
                      #merkle_params{}.
-type recon_dest() :: ?RT:key() | random.

-record(rr_recon_state,
        {
         ownerPid           = ?required(rr_recon_state, ownerPid)       :: comm:erl_local_pid(),
         dhtNodePid         = ?required(rr_recon_state, dhtNodePid)     :: comm:erl_local_pid(),
         dest_rr_pid        = ?required(rr_recon_state, dest_rr_pid)    :: comm:mypid(), %dest rrepair pid
         dest_recon_pid     = undefined                                 :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                                 :: method() | undefined,
         dest_interval      = intervals:empty()                         :: intervals:interval(),
         params             = {}                                        :: parameters() | {}, % parameters from the other node
         struct             = {}                                        :: sync_struct() | {}, % my recon structure
         stage              = req_shared_interval                       :: stage(),
         initiator          = false                                     :: boolean(),
         stats              = rr_recon_stats:new()                      :: rr_recon_stats:stats()
         }).
-type state() :: #rr_recon_state{}.

-define(ok_inner,    1).
-define(ok_leaf,     2).
-define(fail_leaf,   3).
-define(fail_inner,  4).
-define(omit,        9).

-type merkle_cmp_result()  :: ?ok_inner | ?ok_leaf | ?fail_leaf | ?fail_inner.
-type merkle_cmp_request() :: merkle_tree:mt_node_key() | ?omit.

-type request() :: 
    {start, method(), DestKey::recon_dest()} |
    {create_struct, method(), SenderI::intervals:interval()} | % from initiator
    {start_recon, bloom, #bloom_recon_struct{}} | % to initiator
    {start_recon, merkle_tree, #merkle_params{}} | % to initiator
    {start_recon, art, #art_recon_struct{}}. % to initiator

-type message() ::
    % API
    request() |
    % tree sync msgs
    {check_nodes, InitiatorPid::comm:mypid(), [merkle_cmp_request()]} |
    {check_nodes_response, [merkle_cmp_result()]} |
    % dht node response
    {create_struct2, {get_state_response, MyI::intervals:interval()}} |
    {create_struct2, DestI::intervals:interval(), {get_chunk_response, {intervals:interval(), db_chunk()}}} |
    {reconcile, {get_chunk_response, {intervals:interval(), db_chunk()}}} |
    % internal
    {shutdown, exit_reason()} | 
    {crash, DeadPid::comm:mypid()} |
    {'DOWN', MonitorRef::reference(), process, Owner::comm:erl_local_pid(), Info::any()} |
    % merkle tree sync messages
    {check_nodes, SenderPid::comm:mypid(), ToCheck::[merkle_cmp_request()]} |
    {check_nodes_response, CmpResults::[merkle_cmp_result()]}
    .

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state() | kill.

on({create_struct, RMethod, SenderI} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % (first) request from initiator to create a sync struct
    This = comm:reply_as(comm:this(), 2, {create_struct2, '_'}),
    comm:send_local(State#rr_recon_state.dhtNodePid, {get_state, This, my_range}),
    State#rr_recon_state{method = RMethod, initiator = false, dest_interval = SenderI};

on({create_struct2, {get_state_response, MyI}} = _Msg,
   State = #rr_recon_state{stage = req_shared_interval, initiator = false,
                           method = RMethod,            dhtNodePid = DhtPid,
                           dest_rr_pid = DestRRPid,     dest_interval = SenderI}) ->
    ?TRACE1(_Msg, State),
    % target node got sync request, asked for its interval
    % dest_interval contains the interval of the initiator
    % -> client creates recon structure based on common interval, sends it to initiator
    RMethod =:= merkle_tree andalso fd:subscribe(DestRRPid),
    SyncI = find_sync_interval(MyI, SenderI),
    NewState = State#rr_recon_state{stage = build_struct},
    case intervals:is_empty(SyncI) of
        false ->
            % reduce SenderI to the sub-interval matching SyncI, i.e. a mapped SyncI
            SenderSyncI = map_interval(SenderI, SyncI),
            send_chunk_req(DhtPid, self(), SyncI, SenderSyncI, get_max_items(), false),
            NewState;
        true ->
            shutdown(empty_interval, NewState)
    end;

on({create_struct2, DestI, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = build_struct,       initiator = false}) ->
    ?TRACE1(_Msg, State),
    % create recon structure based on all elements in sync interval
    DBList = [encodeBlob(Key, VersionX) || {KeyX, VersionX} <- DBList0,
                                           none =/= (Key = map_key_to_interval(KeyX, DestI))],
    build_struct(DBList, DestI, RestI, State);

on({start_recon, RMethod, Params} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % initiator got other node's sync struct or parameters over sync interval
    % (mapped to the initiator's range)
    % -> create own recon structure based on sync interval and reconcile
    % note: sync interval may be an outdated sub-interval of this node's range
    %       -> pay attention when saving values to DB!
    %       (it could be outdated then even if we retrieved the current range now!)
    case RMethod of
        bloom ->
            MySyncI = Params#bloom_recon_struct.interval,
            DestReconPid = undefined;
        merkle_tree ->
            #merkle_params{interval = MySyncI, reconPid = DestReconPid} = Params,
            fd:subscribe(DestReconPid);
        art ->
            MySyncI = art:get_interval(Params#art_recon_struct.art),
            DestReconPid = undefined
    end,
    % client only sends non-empty sync intervals or exits
    ?ASSERT(not intervals:is_empty(MySyncI)),
    
    DhtNodePid = State#rr_recon_state.dhtNodePid,
    send_chunk_req(DhtNodePid, self(), MySyncI, MySyncI, get_max_items(), true),
    State#rr_recon_state{stage = reconciliation, params = Params,
                         method = RMethod, initiator = true,
                         dest_recon_pid = DestReconPid};

on({reconcile, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     initiator = true,
                           method = bloom,             dhtNodePid = DhtNodePid,
                           params = #bloom_recon_struct{bloom = BF},
                           dest_rr_pid = DestRU_Pid,   stats = Stats,
                           ownerPid = OwnerL}) ->
    ?TRACE1(_Msg, State),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    Diff = case bloom:item_count(BF) of
               0 -> [KeyX || {KeyX, _VersionX} <- DBList0];
               _ -> [KeyX || {KeyX, VersionX} <- DBList0,
                             not bloom:is_element(BF, encodeBlob(KeyX, VersionX))]
           end,
    %if rest interval is non empty start another sync    
    SID = rr_recon_stats:get(session_id, Stats),
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(DhtNodePid, self(), RestI, RestI, get_max_items(), true);
       true -> ok
    end,
    ?TRACE("Reconcile Bloom Session=~p ; Diff=~p", [SID, length(Diff)]),
    NewStats =
        case Diff of
            [_|_] ->
                send_local(OwnerL, {request_resolve, SID, {key_upd_send, DestRU_Pid, Diff},
                                    [{feedback_request, comm:make_global(OwnerL)}]}),
                rr_recon_stats:inc([{resolve_started, 2}], Stats); %feedback causes 2 resolve runs
            [] -> Stats
        end,
    NewState = State#rr_recon_state{stats = NewStats},
    if SyncFinished -> shutdown(sync_finished, NewState);
       true         -> NewState
    end;

on({reconcile, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     initiator = true,
                           method = RMethod,           params = Params})
  when RMethod =:= merkle_tree orelse RMethod =:= art->
    ?TRACE1(_Msg, State),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    DBList = [encodeBlob(KeyX, VersionX) || {KeyX, VersionX} <- DBList0],
    MySyncI = case RMethod of
                  merkle_tree -> Params#merkle_params.interval;
                  art         -> art:get_interval(Params#art_recon_struct.art)
              end,
    build_struct(DBList, MySyncI, RestI, State);

on({crash, _Pid} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    shutdown(recon_node_crash, State);

on({shutdown, Reason}, State) ->
    shutdown(Reason, State);

on({'DOWN', _MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({check_nodes, SenderPid, ToCheck}, State = #rr_recon_state{ struct = Tree }) ->
    {Result, RestTree} = check_node(ToCheck, Tree),
    send(SenderPid, {check_nodes_response, Result}),
    State#rr_recon_state{ dest_recon_pid = SenderPid, struct = RestTree };

on({check_nodes_response, CmpResults}, State =
       #rr_recon_state{ dest_recon_pid = DestReconPid,
                        dest_rr_pid = SrcNode,
                        stats = Stats,
                        ownerPid = OwnerL,
                        struct = Tree }) ->
    SID = rr_recon_stats:get(session_id, Stats),
    {Req, Res, NStats, RTree} = process_tree_cmp_result(CmpResults, Tree, get_merkle_branch_factor(), Stats),
    Req =/= [] andalso
        send(DestReconPid, {check_nodes, comm:this(), Req}),
    {Leafs, Resolves} = resolve_nodes(Res, SrcNode, SID, OwnerL, comm:make_global(OwnerL), 0, 0),
    FStats = rr_recon_stats:inc([{tree_leafsSynced, Leafs},
                                 {resolve_started, Resolves}], NStats),
    CompLeft = rr_recon_stats:get(tree_compareLeft, FStats),
    NewState = State#rr_recon_state{stats = FStats, struct = RTree},
    if CompLeft =:= 0 ->
           send(DestReconPid, {shutdown, sync_finished_remote}),
           shutdown(sync_finished, NewState);
       true -> NewState
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_struct(DBList::db_chunk_enc(), DestI::intervals:non_empty_interval(),
                   RestI::intervals:interval(), state()) -> state() | kill.
build_struct(DBList, DestI, RestI,
             State = #rr_recon_state{method = RMethod, params = Params,
                                     struct = OldSyncStruct,
                                     initiator = Initiator, stats = Stats,
                                     dhtNodePid = DhtNodePid, stage = Stage}) ->
    ?ASSERT(not intervals:is_empty(DestI)),
    % bloom may fork more recon processes if un-synced elements remain
    FinishRecon =
        case intervals:is_empty(RestI) of
            false ->
                SubSyncI = map_interval(DestI, RestI),
                case intervals:is_empty(SubSyncI) of
                    false ->
                        MySubSyncI = map_interval(RestI, DestI), % mapped to my range
                        Reconcile = Initiator andalso (Stage =:= reconciliation),
                        if RMethod =:= bloom -> 
                               ForkState = State#rr_recon_state{dest_interval = SubSyncI,
                                                                params = Params},
                               {ok, Pid} = fork_recon(ForkState),
                               send_chunk_req(DhtNodePid, Pid, MySubSyncI, SubSyncI,
                                              get_max_items(), Reconcile),
                               true;
                           true ->
                               send_chunk_req(DhtNodePid, self(), MySubSyncI, SubSyncI,
                                              get_max_items(), Reconcile),
                               false
                        end;
                    true -> true
                end;
            true -> true
        end,
    ToBuild = if Initiator andalso RMethod =:= art -> merkle_tree;
                 true -> RMethod
              end,
    {BuildTime, SyncStruct} =
        util:tc(fun() -> build_recon_struct(ToBuild, OldSyncStruct, DestI, DBList, Params, FinishRecon) end),
    Stats1 = rr_recon_stats:inc([{build_time, BuildTime}], Stats),
    NewState = State#rr_recon_state{struct = SyncStruct, stats = Stats1},
    if FinishRecon ->
           begin_sync(SyncStruct, Params, NewState#rr_recon_state{stage = reconciliation});
       true ->
           NewState % keep stage (at initiator: reconciliation, at other: build_struct)
    end.

-spec begin_sync(MySyncStruct::sync_struct(), OtherSyncStruct::parameters(),
                 state()) -> state() | kill.
begin_sync(MySyncStruct, _OtherSyncStruct,
           State = #rr_recon_state{method = bloom, initiator = false,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, bloom, MySyncStruct}}),
    shutdown(sync_finished, State);
begin_sync(MySyncStruct, _OtherSyncStruct,
           State = #rr_recon_state{method = merkle_tree, initiator = Initiator,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_recon_pid = DestReconPid,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    case Initiator of
        true ->
            send(DestReconPid,
                 {check_nodes, comm:this(), [merkle_tree:get_hash(MySyncStruct)]});
        false ->
            SID = rr_recon_stats:get(session_id, Stats),
            SyncParams = #merkle_params{interval = merkle_tree:get_interval(MySyncStruct),
                                        reconPid = comm:this()},
            send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                             {start_recon, merkle_tree, SyncParams}})
    end,
    Stats1 =
        rr_recon_stats:set(
          [{tree_compareLeft, ?IIF(Initiator, 1, 0)},
           {tree_size, merkle_tree:size_detail(MySyncStruct)}], Stats),
    State#rr_recon_state{stats = Stats1};
begin_sync(MySyncStruct, OtherSyncStruct,
           State = #rr_recon_state{method = art, initiator = Initiator,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    case Initiator of
        true ->
            Stats1 = art_recon(MySyncStruct, OtherSyncStruct#art_recon_struct.art, State),
            shutdown(sync_finished, State#rr_recon_state{stats = Stats1});
        false ->
            SID = rr_recon_stats:get(session_id, Stats), 
            send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                             {start_recon, art, MySyncStruct}}),
            shutdown(sync_finished, State)
    end.

-spec shutdown(exit_reason(), state()) -> kill.
shutdown(Reason, #rr_recon_state{ownerPid = OwnerL, stats = Stats,
                                 initiator = Initiator, dest_rr_pid = DestRR,
                                 dest_recon_pid = DestRC}) ->
    ?TRACE("SHUTDOWN Session=~p Reason=~p", [rr_recon_stats:get(session_id, Stats), Reason]),
    Status = exit_reason_to_rc_status(Reason),
    NewStats = rr_recon_stats:set([{status, Status}], Stats),
    send_local(OwnerL, {recon_progress_report, comm:this(), Initiator, DestRR, DestRC, NewStats}),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec check_node([merkle_cmp_request()], merkle_tree:merkle_tree() | RestTree) 
        -> {[merkle_cmp_result()], RestTree}
    when is_subtype(RestTree, [merkle_tree:mt_node()]).
check_node(L, Tree) ->
    case merkle_tree:is_merkle_tree(Tree) of
        false -> p_check_node(L, Tree, [], []);
        true -> p_check_node(L, [merkle_tree:get_root(Tree)], [], [])
    end.

%% @doc Compares the given Hashes from the other node with my merkle_tree nodes.
%%      Returns the comparison results and the rest nodes to check in a next
%%      step.
-spec p_check_node(Hashes::[merkle_cmp_request()], MyNodes::[merkle_tree:mt_node()],
                   [merkle_cmp_result()], RestTreeIn::[[merkle_tree:mt_node()]])
        -> {[merkle_cmp_result()], RestTreeOut::[merkle_tree:mt_node()]}.
p_check_node([], [], AccR, AccN) ->
    {lists:reverse(AccR), lists:append(lists:reverse(AccN))};
p_check_node([?omit | TK], [_Node | TN], AccR, AccN) ->
    p_check_node(TK, TN, AccR, AccN);
p_check_node([Hash | TK], [Node | TN], AccR, AccN) ->
    IsLeaf = merkle_tree:is_leaf(Node),
    case merkle_tree:get_hash(Node) =:= Hash of
        true when IsLeaf -> p_check_node(TK, TN, [?ok_leaf | AccR], AccN);
        true when not IsLeaf -> p_check_node(TK, TN, [?ok_inner | AccR], AccN);
        false when IsLeaf -> p_check_node(TK, TN, [?fail_leaf | AccR], AccN);
        false when not IsLeaf ->
            Childs = merkle_tree:get_childs(Node),
            p_check_node(TK, TN, [?fail_inner | AccR], [Childs | AccN])
    end.

-spec process_tree_cmp_result([merkle_cmp_result()], merkle_tree:merkle_tree() | RestTree, pos_integer(), Stats) ->
          {Requests, Resolve, New::Stats, New::RestTree}
    when
      is_subtype(RestTree,  [merkle_tree:mt_node()]),
      is_subtype(Stats,     rr_recon_stats:stats()),
      is_subtype(Requests,  [merkle_cmp_request()]),
      is_subtype(Resolve,   [merkle_tree:mt_node()]).
process_tree_cmp_result(CmpResult, Tree, BranchSize, Stats) ->
    Compared = length(CmpResult),
    NStats = rr_recon_stats:inc([{tree_compareLeft, -Compared},
                                 {tree_nodesCompared, Compared}], Stats),
    case merkle_tree:is_merkle_tree(Tree) of
        false -> p_process_tree_cmp_result(CmpResult, Tree, BranchSize, NStats, [], [], []);
        true -> p_process_tree_cmp_result(CmpResult, [merkle_tree:get_root(Tree)], BranchSize, NStats, [], [], [])
    end.

-spec p_process_tree_cmp_result([merkle_cmp_result()], RestTree, BranchSize, Stats, Acc::Req, Acc::Res, AccRTree::Res) 
        -> {Req, Res, Stats, RestTree::Res}           
    when
      is_subtype(BranchSize,pos_integer()),
      is_subtype(Stats,     rr_recon_stats:stats()),
      is_subtype(Req,       [merkle_tree:mt_node_key()]),
      is_subtype(Res,       [merkle_tree:mt_node()]),
      is_subtype(RestTree,  [merkle_tree:mt_node()]).
p_process_tree_cmp_result([], [], _, Stats, Req, Res, RTree) ->
    {lists:reverse(Req), Res, Stats, lists:reverse(RTree)};
p_process_tree_cmp_result([?ok_inner | TR], [Node | TN], BS, Stats, Req, Res, RTree) ->
    NStats = rr_recon_stats:inc([{tree_compareSkipped, merkle_tree:size(Node)}], Stats),
    p_process_tree_cmp_result(TR, TN, BS, NStats, Req, Res, RTree);
p_process_tree_cmp_result([?ok_leaf | TR], [_Node | TN], BS, Stats, Req, Res, RTree) ->
    NStats = rr_recon_stats:inc([{tree_compareSkipped, 1}], Stats),
    p_process_tree_cmp_result(TR, TN, BS, NStats, Req, Res, RTree);
p_process_tree_cmp_result([?fail_leaf | TR], [Node | TN], BS, Stats, Req, Res, RTree) ->
    p_process_tree_cmp_result(TR, TN, BS, Stats, Req, [Node | Res], RTree);
p_process_tree_cmp_result([?fail_inner | TR], [Node | TN], BS, Stats, Req, Res, RTree) ->
    case merkle_tree:is_leaf(Node) of
        false -> 
            Childs = merkle_tree:get_childs(Node),
            NewReq = [merkle_tree:get_hash(X) || X <- Childs],
            NStats = rr_recon_stats:inc([{tree_compareLeft, length(Childs)}], Stats),
            p_process_tree_cmp_result(TR, TN, BS, NStats,
                                      lists:reverse(NewReq, Req),
                                      Res,
                                      lists:reverse(Childs, RTree));
        true -> 
            NewReq = lists:duplicate(BS, ?omit),
            p_process_tree_cmp_result(TR, TN, BS, Stats,
                                      lists:append(NewReq, Req), [Node | Res], RTree)
    end.

% @doc Starts one resolve process per leaf node in a given node
%      Returns: number of visited leaf nodes and number of leaf resolve requests.
-spec resolve_nodes([merkle_tree:mt_node()], Dest::comm:mypid(), rrepair:session_id(),
                    OwnerLocal::comm:erl_local_pid(), OwnerRemote::comm:mypid(),
                    LeafCountIn::non_neg_integer(), ResolveReqCountIn::non_neg_integer())
        -> {LeafCountOut::non_neg_integer(), ResolveReqCountOut::non_neg_integer()}.
resolve_nodes([], _Dest, _SID, _OwnerL, _OwnerR, Leafs, ResolveReqs) -> {Leafs, ResolveReqs};
resolve_nodes([Node | Rest], Dest, SID, OwnerL, OwnerR, Leafs, ResolveReqs) ->
    {LCount, RCount} =
        case merkle_tree:is_leaf(Node) of
            true -> {1, resolve_leaf(Node, Dest, SID, OwnerL, OwnerR)};
            false -> resolve_nodes(merkle_tree:get_childs(Node), Dest, SID, OwnerL,
                                   OwnerR, 0, 0)
        end,
    resolve_nodes(Rest, Dest, SID, OwnerL, OwnerR, Leafs + LCount, ResolveReqs + RCount).

% @doc Returns number of caused resolve requests (requests with feedback count 2)
-spec resolve_leaf(merkle_tree:mt_node(), Dest::comm:mypid(), rrepair:session_id(),
                   OwnerLocal::comm:erl_local_pid(), OwnerRemote::comm:mypid())
        -> 1 | 2.
resolve_leaf(Node, Dest, SID, OwnerL, OwnerR) ->
    Options = [{feedback_request, OwnerR}],
    LeafInterval = merkle_tree:get_interval(Node),
    case merkle_tree:get_item_count(Node) of
        0 ->
            send(Dest, {request_resolve, SID, {interval_upd, LeafInterval, []},
                        [{session_id, SID} | Options]}),
            1;
        _ ->
            send_local(OwnerL, {request_resolve, SID,
                                {interval_upd_send, LeafInterval, Dest},
                                Options}),
            2
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% art recon
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec art_recon(MyTree::merkle_tree:merkle_tree(), art:art(), state())
        -> rr_recon_stats:stats().
art_recon(Tree, Art, #rr_recon_state{ dest_rr_pid = DestPid,
                                      ownerPid = OwnerL,
                                      stats = Stats }) ->
    SID = rr_recon_stats:get(session_id, Stats),
    NStats = case merkle_tree:get_interval(Tree) =:= art:get_interval(Art) of
                 true -> 
                     {ASyncLeafs, Stats2} = art_get_sync_leafs([merkle_tree:get_root(Tree)], Art, Stats, []),
                     OwnerR = comm:make_global(OwnerL),
                     ResolveCalled = lists:foldl(fun(X, Acc) ->
                                                         Acc + resolve_leaf(X, DestPid, SID, OwnerL, OwnerR)
                                                 end, 0, ASyncLeafs),
                     rr_recon_stats:inc([{resolve_started, ResolveCalled}], Stats2);
                 false -> Stats
             end,
    rr_recon_stats:set([{tree_size, merkle_tree:size_detail(Tree)}], NStats).

-spec art_get_sync_leafs(Nodes::NodeL, art:art(), Stats, Acc::NodeL) -> {ToSync::NodeL, Stats} when
    is_subtype(NodeL,   [merkle_tree:mt_node()]),
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

-spec build_recon_struct(method(), OldSyncStruct::sync_struct() | {},
                         DestI::intervals:non_empty_interval(), db_chunk_enc(),
                         Params::parameters() | {}, FinishRecon::boolean())
        -> sync_struct().
build_recon_struct(bloom, _OldSyncStruct = {}, I, DBItems, _Params, true) ->
    % note: for bloom, parameters don't need to match - use our own
    ?ASSERT(not intervals:is_empty(I)),
    Fpr = get_bloom_fpr(),
    ElementNum = length(DBItems),
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    BF = bloom:new(ElementNum, Fpr, ?REP_HFS:new(HFCount), DBItems),
    #bloom_recon_struct{ interval = I, bloom = BF };
build_recon_struct(merkle_tree, _OldSyncStruct = {}, I, DBItems, Params, FinishRecon) ->
    ?ASSERT(not intervals:is_empty(I)),
    case Params of
        {} ->
            BranchFactor = get_merkle_branch_factor(),
            BucketSize = get_merkle_bucket_size();
        #merkle_params{branch_factor = BranchFactor,
                       bucket_size = BucketSize} ->
            ok;
        #art_recon_struct{branch_factor = BranchFactor,
                          bucket_size = BucketSize} ->
            ok
    end,
    merkle_tree:new(I, DBItems, [{branch_factor, BranchFactor},
                                 {bucket_size, BucketSize},
                                 {keep_bucket, not FinishRecon}]);
build_recon_struct(merkle_tree, OldSyncStruct, _I, DBItems, _Params, FinishRecon) ->
    ?ASSERT(not intervals:is_empty(_I)),
    ?ASSERT(merkle_tree:is_merkle_tree(OldSyncStruct)),
    NTree = merkle_tree:insert_list(DBItems, OldSyncStruct),
    if FinishRecon ->
           % no more DB items -> finish tree, remove buckets
           merkle_tree:gen_hash(NTree, true);
       true ->
           % don't hash now - there will be new items
           NTree
    end;
build_recon_struct(art, _OldSyncStruct = {}, I, DBItems, _Params = {}, FinishRecon) ->
    ?ASSERT(not intervals:is_empty(I)),
    Branch = get_merkle_branch_factor(),
    BucketSize = merkle_tree:get_opt_bucket_size(length(DBItems), Branch, 1),
    Tree = merkle_tree:new(I, DBItems, [{branch_factor, Branch},
                                        {bucket_size, BucketSize},
                                        {keep_bucket, not FinishRecon}]),
    if FinishRecon ->
           % no more DB items -> create art struct:
           #art_recon_struct{art = art:new(Tree, get_art_config()),
                             branch_factor = Branch, bucket_size = BucketSize};
       true ->
           % more DB items to come... stay with merkle tree
           Tree
    end;
build_recon_struct(art, OldSyncStruct, _I, DBItems, _Params = {}, FinishRecon) ->
    % similar to continued merkle build
    ?ASSERT(not intervals:is_empty(_I)),
    ?ASSERT(merkle_tree:is_merkle_tree(OldSyncStruct)),
    Tree1 = merkle_tree:insert_list(DBItems, OldSyncStruct),
    if FinishRecon ->
           % no more DB items -> finish tree, remove buckets, create art struct:
           NTree = merkle_tree:gen_hash(Tree1, true),
           #art_recon_struct{art = art:new(NTree, get_art_config()),
                             branch_factor = merkle_tree:get_branch_factor(NTree),
                             bucket_size = merkle_tree:get_bucket_size(NTree)};
       true ->
           % don't hash now - there will be new items
           Tree1
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec send(Pid::comm:mypid(), Msg::comm:message() | comm:group_message()) -> ok.
send(Pid, Msg) ->
    ?TRACE_SEND(Pid, Msg),
    comm:send(Pid, Msg).

-spec send_local(Pid::comm:erl_local_pid(), Msg::comm:message() | comm:group_message()) -> ok.
send_local(Pid, Msg) ->
    ?TRACE_SEND(Pid, Msg),
    comm:send_local(Pid, Msg).

%% @doc Sends a get_chunk request to the local DHT_node process.
%%      Request responds with a list of {Key, Value} tuples.
%%      The mapping to DestI is not done here!
-spec send_chunk_req(DhtPid::LPid, AnswerPid::LPid, ChunkI::I, DestI::I,
                     MaxItems::pos_integer() | all, Reconcile::boolean()) -> ok when
    is_subtype(LPid,        comm:erl_local_pid()),
    is_subtype(I,           intervals:interval()).
send_chunk_req(DhtPid, SrcPid, I, _DestI, MaxItems, true) ->
    ?ASSERT(I =:= _DestI),
    SrcPidReply = comm:reply_as(SrcPid, 2, {reconcile, '_'}),
    send_local(DhtPid,
               {get_chunk, SrcPidReply, I, fun get_chunk_filter/1,
                fun get_chunk_value/1, MaxItems});
send_chunk_req(DhtPid, SrcPid, I, DestI, MaxItems, false) ->
    SrcPidReply = comm:reply_as(SrcPid, 3, {create_struct2, DestI, '_'}),
    send_local(DhtPid,
               {get_chunk, SrcPidReply, I, fun get_chunk_filter/1,
                fun get_chunk_value/1, MaxItems}).

-spec get_chunk_filter(db_entry:entry()) -> boolean().
get_chunk_filter(DBEntry) -> db_entry:get_version(DBEntry) =/= -1.
-spec get_chunk_value(db_entry:entry()) -> {?RT:key(), db_dht:version() | -1}.
get_chunk_value(DBEntry) -> {db_entry:get_key(DBEntry), db_entry:get_version(DBEntry)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec exit_reason_to_rc_status(exit_reason()) -> rr_recon_stats:status().
exit_reason_to_rc_status(sync_finished) -> finish;
exit_reason_to_rc_status(sync_finished_remote) -> finish;
exit_reason_to_rc_status(_) -> abort.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Maps any key (K) into a given interval (I). If K is already in I, K is returned.
%%      If K has more than one associated keys in I, the closest one is returned.
%%      If all associated keys of K are not in I, none is returned.
-spec map_key_to_interval(?RT:key(), intervals:interval()) -> ?RT:key() | none.
map_key_to_interval(Key, I) ->
    RGrp = [K || K <- ?RT:get_replica_keys(Key), intervals:in(K, I)],
    case RGrp of
        [] -> none;
        [R] -> R;
        [H|T] ->
            element(1, lists:foldl(fun(X, {_KeyIn, DistIn} = AccIn) ->
                                           DistX = key_dist(X, Key),
                                           if DistX < DistIn -> {X, DistX};
                                              true -> AccIn
                                           end
                                   end, {H, key_dist(H, Key)}, T))
    end.

-compile({inline, [key_dist/2]}).

-spec key_dist(Key1::?RT:key(), Key2::?RT:key()) -> number().
key_dist(Key, Key) -> 0;
key_dist(Key1, Key2) ->
    Dist1 = ?RT:get_range(Key1, Key2),
    Dist2 = ?RT:get_range(Key2, Key1),
    erlang:min(Dist1, Dist2).

%% @doc Maps an abitrary key to its associated key in replication quadrant N.
-spec map_key_to_quadrant(?RT:key(), quadrant()) -> ?RT:key().
map_key_to_quadrant(Key, N) ->
    case lists:sort(?RT:get_replica_keys(Key)) of
        [?MINUS_INFINITY|TL] -> lists:nth(N, lists:append(TL, [?MINUS_INFINITY]));
        [_|_] = X            -> lists:nth(N, X)
    end.

%% @doc Gets the quadrant intervals.
-spec quadrant_intervals() -> [intervals:non_empty_interval(),...].
quadrant_intervals() ->
    case ?RT:get_replica_keys(?MINUS_INFINITY) of
        [_]               -> [intervals:all()];
        [HB,_|_] = Borders -> quadrant_intervals_(Borders, [], HB)
    end.

%% @doc Internal helper for quadrant_intervals/0 - keep in sync with
%%      map_key_to_quadrant/2!
%% TODO: use intervals:new('[', A, B, ')') instead so ?MINUS_INFINITY is in quadrant 1?
%%       -> does not fit ranges that well as they are normally defined as (A,B]
-spec quadrant_intervals_(Borders::[?RT:key(),...], ResultIn::[intervals:non_empty_interval()],
                          HeadB::?RT:key()) -> [intervals:non_empty_interval(),...].
quadrant_intervals_([K], Res, HB) ->
    lists:reverse(Res, [intervals:new('(', K, HB, ']')]);
quadrant_intervals_([A | [B | _] = TL], Res, HB) ->
    quadrant_intervals_(TL, [intervals:new('(', A, B, ']') | Res], HB).

%% @doc Gets all sub intervals of the given interval which lay only in
%%      a single quadrant.
-spec quadrant_subints_(A::intervals:interval(), Quadrants::[intervals:interval()],
                        AccIn::[intervals:interval()]) -> AccOut::[intervals:interval()].
quadrant_subints_(_A, [], Acc) -> Acc;
quadrant_subints_(A, [Q | QT], Acc) ->
    Sec = intervals:intersection(A, Q),
    case intervals:is_empty(Sec) of
        false when Sec =:= Q ->
            % if a quadrant is completely covered, only return this
            % -> this would reconcile all the other keys, too!
            % it also excludes non-continuous intervals
            [Q];
        false -> quadrant_subints_(A, QT, [Sec | Acc]);
        true  -> quadrant_subints_(A, QT, Acc)
    end.

%% @doc Gets all replicated intervals of I.
%%      PreCond: interval (I) is continuous and is inside a single quadrant!
-spec replicated_intervals(intervals:continuous_interval())
        -> [intervals:continuous_interval()].
replicated_intervals(I) ->
    ?ASSERT(intervals:is_continuous(I)),
    ?ASSERT(1 =:= length([ok || Q <- quadrant_intervals(),
                                not intervals:is_empty(
                                  intervals:intersection(I, Q))])),
    case intervals:is_all(I) of
        false ->
            case intervals:get_bounds(I) of
                {_LBr, ?MINUS_INFINITY, ?PLUS_INFINITY, _RBr} ->
                    [I]; % this is the only interval possible!
                {'[', Key, Key, ']'} ->
                    [intervals:new(X) || X <- ?RT:get_replica_keys(Key)];
                {LBr, LKey, RKey0, RBr} ->
                    LKeys = lists:sort(?RT:get_replica_keys(LKey)),
                    % note: get_bounds may also return ?PLUS_INFINITY but this is not a valid key in ?RT!
                    RKey = ?IIF(RKey0 =:= ?PLUS_INFINITY, ?MINUS_INFINITY, RKey0),
                    RKeys = case lists:sort(?RT:get_replica_keys(RKey)) of
                                [?MINUS_INFINITY | RKeysTL] ->
                                    lists:append(RKeysTL, [?MINUS_INFINITY]);
                                X -> X
                            end,
                    % since I is in a single quadrant, RKey >= LKey
                    % -> we can zip the sorted keys to get the replicated intervals
                    lists:zipwith(
                      fun(LKeyX, RKeyX) ->
                              ?ASSERT(?RT:get_range(LKeyX, ?IIF(RKeyX =:= ?MINUS_INFINITY, ?PLUS_INFINITY, RKeyX)) =:=
                                          ?RT:get_range(LKey, RKey0)),
                              intervals:new(LBr, LKeyX, RKeyX, RBr)
                      end, LKeys, RKeys)
            end;
        true -> [I]
    end.

%% @doc Gets a randomly selected sync interval as an intersection of the two
%%      given intervals as a sub interval of A inside a single quadrant.
%%      Result may be empty, otherwise it is also continuous!
-spec find_sync_interval(intervals:continuous_interval(), intervals:continuous_interval())
        -> intervals:interval().
find_sync_interval(A, B) ->
    ?ASSERT(intervals:is_continuous(A)),
    ?ASSERT(intervals:is_continuous(B)),
    Quadrants = quadrant_intervals(),
    InterSecs = [I || AQ <- quadrant_subints_(A, Quadrants, []),
                      BQ <- quadrant_subints_(B, Quadrants, []),
                      RBQ <- replicated_intervals(BQ),
                      not intervals:is_empty(
                        I = intervals:intersection(AQ, RBQ))],
    case InterSecs of
        [] -> intervals:empty();
        [_|_] -> util:randomelem(InterSecs)
    end.

%% @doc Maps interval B into interval A.
%%      PreCond: the second (continuous) interval must be in a single quadrant!
%%      The result is thus also only in a single quadrant.
%%      Result may be empty, otherwise it is also continuous!
-spec map_interval(intervals:continuous_interval(), intervals:continuous_interval())
        -> intervals:interval().
map_interval(A, B) ->
    ?ASSERT(intervals:is_continuous(A)),
    ?ASSERT(intervals:is_continuous(B)),
    ?ASSERT(1 =:= length([ok || Q <- quadrant_intervals(),
                                not intervals:is_empty(
                                  intervals:intersection(B, Q))])),
    
    % note: The intersection may only be non-continuous if A is covering more
    %       than a quadrant. In this case, another intersection will be larger
    %       and we can safely ignore this one!
    InterSecs = [I || RB <- replicated_intervals(B),
                      not intervals:is_empty(
                        I = intervals:intersection(A, RB)),
                      intervals:is_continuous(I)],
    case InterSecs of
        [] -> intervals:empty();
        [_|_] -> util:randomelem(InterSecs)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec encodeBlob(?RT:key(), db_dht:value() | db_dht:version()) -> db_entry_enc().
encodeBlob(A, B) -> 
    term_to_binary([A, "#", B]).

-spec decodeBlob(term()) -> {?RT:key(), db_dht:value() | db_dht:version()} | fail.
decodeBlob(Blob) when is_binary(Blob) ->
    case binary_to_term(Blob) of
        [Key, "#", X] -> {Key, X};
        _ -> fail
    end;
decodeBlob(_) -> fail.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc init module
-spec init(state()) -> state().
init(State) ->
    _ = erlang:monitor(process, State#rr_recon_state.ownerPid),
    State.

-spec start(SessionId::rrepair:session_id() | null, SenderRRPid::comm:mypid())
        -> {ok, pid()}.
start(SessionId, SenderRRPid) ->
    State = #rr_recon_state{ ownerPid = self(),
                             dhtNodePid = pid_groups:get_my(dht_node),
                             dest_rr_pid = SenderRRPid,
                             stats = rr_recon_stats:new([{session_id, SessionId}]) },
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, State, []).

-spec fork_recon(state()) -> {ok, pid()}.
fork_recon(Conf) ->
    NStats = rr_recon_stats:set([{session_id, null}], Conf#rr_recon_state.stats),
    State = Conf#rr_recon_state{ stats = NStats },
    send_local(Conf#rr_recon_state.ownerPid, {recon_forked}),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, State, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config parameter handling
%
% rr_bloom_fpr              - bloom filter false positive rate (fpr)
% rr_max_items              - max. number of items to retrieve from the
%                             dht_node at once
%                             if syncing with bloom filters and node data count
%                             exceeds rr_max_items, new rr_recon processes will
%                             be spawned (each creating a new bloom filter)
% rr_art_inner_fpr          - 
% rr_art_leaf_fpr           -  
% rr_art_correction_factor  - 
% rr_merkle_branch_factor   - merkle tree branching factor thus number of childs per node
% rr_merkle_bucket_size     - size of merkle tree leaf buckets
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether a config parameter is float and in [0,1].
-spec check_percent(atom()) -> boolean().
check_percent(Atom) ->
    config:cfg_is_float(Atom) andalso
        config:cfg_is_greater_than(Atom, 0) andalso
        config:cfg_is_less_than(Atom, 1).

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(rr_recon_method, [bloom, merkle_tree, art]) andalso
        check_percent(rr_bloom_fpr) andalso
        ?IIF(config:read(rr_max_items) =:= all,
             true,
             config:cfg_is_integer(rr_max_items) andalso
                 config:cfg_is_greater_than(rr_max_items, 0)) andalso
        config:cfg_is_integer(rr_merkle_branch_factor) andalso
        config:cfg_is_greater_than(rr_merkle_branch_factor, 1) andalso
        config:cfg_is_integer(rr_merkle_bucket_size) andalso
        config:cfg_is_greater_than(rr_merkle_bucket_size, 0) andalso
        check_percent(rr_art_inner_fpr) andalso
        check_percent(rr_art_leaf_fpr) andalso
        config:cfg_is_integer(rr_art_correction_factor) andalso
        config:cfg_is_greater_than(rr_art_correction_factor, 0).

-spec get_bloom_fpr() -> float().
get_bloom_fpr() ->
    config:read(rr_bloom_fpr).

-spec get_max_items() -> pos_integer() | all.
get_max_items() ->
    config:read(rr_max_items).

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
