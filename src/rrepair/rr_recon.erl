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
-export([map_key_to_interval/2, map_key_to_quadrant/2]).

%export for testing
-export([encodeBlob/2, decodeBlob/1,
         map_interval/2,
         get_key_quadrant/1,
         find_intersection/2,
         get_interval_size/1]).

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
-export_type([method/0, request/0]).
-endif.

-type quadrant()       :: 1..4. % 1..rep_factor()
-type method()         :: bloom | merkle_tree | art | iblt | undefined.
-type stage()          :: req_shared_interval | build_struct | reconciliation.

-type exit_reason()    :: empty_interval |          %interval intersection between initator and client is empty
                          negotiate_interval |      %rc initiator has send its interval and exits  
                          build_struct |            %client send its struct (bloom/art) to initiator and exits
                          recon_node_crash |        %sync partner node crashed  
                          sync_finished |           %initiator finish recon
                          sync_finished_remote.     %client-side shutdown by merkle-tree recon initiator

-type db_entry_enc()   :: binary().
-type db_chunk_enc()   :: [db_entry_enc()].
-type db_entry()       :: {?RT:key(), db_dht:version()}.
-type db_chunk()       :: [db_entry()].

-record(bloom_recon_struct,
        {
         interval = intervals:empty()                       :: intervals:interval(),
         bloom    = ?required(bloom_recon_struct, bloom)    :: ?REP_BLOOM:bloom_filter()         
        }).

-type internal_param() :: {reconPid, comm:mypid()}.

-type sync_struct() :: #bloom_recon_struct{} |
                       merkle_tree:merkle_tree() |
                       art:art().
-type parameters() :: sync_struct() |
                      [internal_param()] |
                      [merkle_tree:mt_node()].
-type recon_dest() :: ?RT:key() | random.

-record(rr_recon_state,
        {
         ownerPid           = ?required(rr_recon_state, ownerPid)       :: comm:erl_local_pid(),
         dhtNodePid         = ?required(rr_recon_state, dhtNodePid)     :: comm:erl_local_pid(),
         dest_rr_pid        = ?required(rr_recon_state, dest_rr_pid)    :: comm:mypid(), %dest rrepair pid
         dest_recon_pid     = undefined                                 :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                                 :: method(),
         dest_interval      = intervals:empty()                         :: intervals:interval(),
         struct             = {}                                        :: parameters() | {},
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
    {create_struct, method(), SenderI::intervals:interval(), parameters(), RcvInitiator::boolean()} |
    {start_recon, bloom, #bloom_recon_struct{}, RcvInitiator::boolean()} |
    {start_recon, art, art:art(), RcvInitiator::boolean()} |
    {continue, method(), stage(), parameters(), Initiator::boolean()}.

-type message() ::          
    %API
    request() |
    %tree sync msgs
    {check_nodes, InitiatorPid::comm:mypid(), [merkle_cmp_request()]} |
    {check_nodes_response, [merkle_cmp_result()]} |
    %dht node response
    {create_struct2, {get_state_response, MyI::intervals:interval()}} |
    {get_state_response, MyI::intervals:interval()} |
    {rr_recon, data, DestI::intervals:interval(), {get_chunk_response, {intervals:interval(), db_chunk()}}} |
    %internal
    {shutdown, exit_reason()} | 
    {crash, DeadPid::comm:mypid()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state() | kill.

on({create_struct, RMethod, SenderI, Params, RcvInitiator} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    This = comm:reply_as(comm:this(), 2, {create_struct2, '_'}),
    comm:send_local(State#rr_recon_state.dhtNodePid, {get_state, This, my_range}),
    State#rr_recon_state{method = RMethod, initiator = RcvInitiator, dest_interval = SenderI, struct = Params};

on({create_struct2, {get_state_response, MyI}} = _Msg,
   State = #rr_recon_state{stage = req_shared_interval, initiator = false,
                           method = RMethod,            dhtNodePid = DhtPid,
                           dest_rr_pid = DestRRPid,     dest_interval = SenderI}) ->
    ?TRACE1(_Msg, State),
    % target node got sync request, asked for its interval
    % struct contains the interval of the initiator
    % -> client creates recon structure based on common interval, sends it to initiator
    RMethod =:= merkle_tree andalso fd:subscribe(DestRRPid),
    SyncI = find_intersection(MyI, SenderI),
    % reduce SenderI to the sub-interval matching SyncI, i.e. a mapped SyncI
    SenderSyncI = find_intersection(SenderI, SyncI),
    NewState = State#rr_recon_state{stage = build_struct},
    case intervals:is_empty(SyncI) of
        false ->
            send_chunk_req(DhtPid, self(), SyncI, SenderSyncI, get_max_items(RMethod)),
            NewState;
        true ->
            shutdown(empty_interval, NewState)
    end;

on({create_struct2, {get_state_response, MyI}} = _Msg,
   State = #rr_recon_state{stage = req_shared_interval, initiator = true,
                           method = RMethod,            dhtNodePid = DhtPid,
                           dest_rr_pid = DestRRPid,     struct = Params,
                           dest_interval = SenderI})
  when (RMethod =:= merkle_tree orelse RMethod =:= art) ->
    ?TRACE1(_Msg, State),
    % initiator got interval of client to create his own recon structure (merkle/art)
    % based on common interval
    DestReconPid = proplists:get_value(reconPid, Params, undefined),
    SyncI = find_intersection(MyI, SenderI),
    NewState = State#rr_recon_state{stage = build_struct,
                                    dest_recon_pid = DestReconPid},
    case intervals:is_empty(SyncI) of
        false ->
            RMethod =:= merkle_tree andalso fd:subscribe(DestRRPid),
            send_chunk_req(DhtPid, self(), SyncI, SyncI, get_max_items(RMethod)),
            NewState;
        true when DestReconPid =/= undefined ->
            send(DestReconPid, {shutdown, empty_interval}),
            shutdown(negotiate_interval, NewState);
        true ->
            shutdown(negotiate_interval, NewState)
    end;

on({rr_recon, data, DestI, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = build_struct}) ->
    ?TRACE1(_Msg, State),
    % create recon structure based on all elements in sync interval
    DBList = [encodeBlob(Key, VersionX) || {KeyX, VersionX} <- DBList0,
                                           none =/= (Key = map_key_to_interval(KeyX, DestI))],
    build_struct(DBList, DestI, RestI, State);

on({start_recon, RMethod, SyncStruct, true = Initiator} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % initiator got other node's sync struct over sync interval (mapped to own range)
    % -> create own recon structure based on sync interval and reconcile
    % note: sync interval may be an outdated sub-interval of this node's range
    %       -> pay attention when saving values to DB!
    %       (it could be outdated then even if we retrieved the current range now!)
    case RMethod of
        bloom ->
            MySyncI = SyncStruct#bloom_recon_struct.interval,
            DestReconPid = undefined,
            ok;
%%         merkle_tree -> % TODO
%%             MySyncI = merkle_tree:get_interval(SyncStruct),
%%             DestReconPid = proplists:get_value(reconPid, Params, undefined),
%%             fd:subscribe(DestReconPid),
%%             ok;
        art ->
            MySyncI = art:get_interval(SyncStruct),
            DestReconPid = undefined,
            ok
    end,
    % client only sends non-empty sync intervals or exits
    ?ASSERT(not intervals:is_empty(MySyncI)),
    
    DhtNodePid = State#rr_recon_state.dhtNodePid,
    Self = comm:reply_as(self(), 2, {reconcile, '_'}),
    send_local(DhtNodePid,
               {get_chunk, Self, MySyncI, fun get_chunk_filter/1,
                fun get_chunk_value/1, get_max_items(RMethod)}),
    State#rr_recon_state{stage = reconciliation, struct = SyncStruct,
                         method = RMethod, initiator = Initiator,
                         dest_recon_pid = DestReconPid};

on({reconcile, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     ownerPid = OwnerL,
                           method = bloom,             dhtNodePid = DhtNodePid,
                           dest_rr_pid = DestRU_Pid,   stats = Stats,
                           struct = #bloom_recon_struct{bloom = BF},
                           initiator = true}) ->
    ?TRACE1(_Msg, State),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    Diff = [KeyX || {KeyX, VersionX} <- DBList0,
                    not ?REP_BLOOM:is_element(BF, encodeBlob(KeyX, VersionX))],
    %if rest interval is non empty start another sync    
    SID = rr_recon_stats:get(session_id, Stats),
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
            Self = comm:reply_as(self(), 2, {reconcile, '_'}),
            send_local(DhtNodePid,
                       {get_chunk, Self, RestI, fun get_chunk_filter/1,
                        fun get_chunk_value/1, get_max_items(bloom)});
       true -> ok
    end,
    ?TRACE("Reconcile Bloom Session=~p ; Diff=~p", [SID, length(Diff)]),
    NewStats =
        case Diff of
            [_|_] ->
                send_local(OwnerL, {request_resolve, SID, {key_upd_send, DestRU_Pid, Diff},
                                    [{feedback, comm:make_global(OwnerL)}]}),
                rr_recon_stats:inc([{resolve_started, 2}], Stats); %feedback causes 2 resolve runs
            [] -> Stats
        end,
    NewState = State#rr_recon_state{stats = NewStats},
    if SyncFinished -> shutdown(sync_finished, NewState);
       true         -> NewState
    end;

on({reconcile, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     initiator = true,
                           method = art,               struct = SyncStruct}) ->
    ?TRACE1(_Msg, State),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    DBList = [encodeBlob(KeyX, VersionX) || {KeyX, VersionX} <- DBList0],
    MySyncI = art:get_interval(SyncStruct),
    build_struct(DBList, MySyncI, RestI, State);

on({continue, Method, Stage, Struct, Initiator} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    send_local(State#rr_recon_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#rr_recon_state{ stage = Stage,
                          struct = Struct,
                          method = Method,
                          initiator = Initiator };

on({crash, _Pid} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    shutdown(recon_node_crash, State);

on({shutdown, Reason}, State) ->
    shutdown(Reason, State);

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
    {Leafs, Resolves} = lists:foldl(fun(Node, {AccL, AccR}) -> 
                                            {LCount, RCount} = resolve_node(Node, {SrcNode, SID, OwnerL}),
                                            {AccL + LCount, AccR + RCount}
                                    end, {0, 0}, Res),
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

-spec build_struct(DBList::db_chunk_enc(), DestI::I, RestI::I, state()) -> state()
          when is_subtype(I, intervals:interval()).
build_struct(DBList, DestI, RestI,
             State = #rr_recon_state{method = RMethod, struct = Params,
                                     initiator = Initiator, stats = Stats,
                                     dhtNodePid = DhtNodePid}) ->
    {BuildTime, SyncStruct} =
        case merkle_tree:is_merkle_tree(Params) of
            true ->
                {BTime, NTree} = util:tc(merkle_tree, insert_list, [DBList, Params]),
                {rr_recon_stats:get(build_time, Stats) + BTime, merkle_tree:gen_hash(NTree) };
            false ->
                ToBuild = ?IIF(RMethod =:= art, ?IIF(Initiator, merkle_tree, art), RMethod),
                util:tc(fun() -> build_recon_struct(ToBuild, {DestI, DBList}) end)
        end,
    EmptyRest = intervals:is_empty(RestI),
    % bloom may fork more recon processes if un-synced elements remain
    if not EmptyRest ->
           SubSyncI = find_intersection(DestI, RestI),
           case intervals:is_empty(SubSyncI) of
               false ->
                   Pid = if RMethod =:= bloom -> 
                                erlang:element(2, fork_recon(State#rr_recon_state{dest_interval = SubSyncI,
                                                                                  struct = Params}));
                            true -> self()
                         end,
                   send_chunk_req(DhtNodePid, Pid, find_intersection(RestI, DestI), SubSyncI,
                                  get_max_items(RMethod));
               true -> ok
           end;
        true -> ok
    end,
    {NStage, NStats} =
        if EmptyRest orelse RMethod =:= bloom -> {reconciliation, begin_sync(SyncStruct, State)};
           not EmptyRest andalso RMethod =:= merkle_tree -> {build_struct, Stats};
           true -> {reconciliation, Stats}
        end,
    State#rr_recon_state{ stage = NStage,
                          struct = SyncStruct,
                          stats = rr_recon_stats:set([{build_time, BuildTime}], NStats) }.

-spec begin_sync(sync_struct(), state()) -> rr_recon_stats:stats().
begin_sync(MySyncStruct, State = #rr_recon_state{method = Method,
                                                 struct = Params,
                                                 ownerPid = OwnerL,
                                                 dest_recon_pid = DestReconPid,
                                                 dest_rr_pid = DestRRPid,
                                                 initiator = Initiator,
                                                 stats = Stats }) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    case Method of
        merkle_tree -> 
            case Initiator of
                true -> send(DestReconPid,
                             {check_nodes, comm:this(), [merkle_tree:get_hash(MySyncStruct)]});
                false ->
                    SyncI = merkle_tree:get_interval(MySyncStruct),
                    SyncParams = [{reconPid, comm:this()}],
                    send(DestRRPid,
                         {continue_recon, comm:make_global(OwnerL), SID,
                          {create_struct, Method, SyncI, SyncParams, true}})
            end,
            rr_recon_stats:set(
              [{tree_compareLeft, ?IIF(Initiator, 1, 0)},
               {tree_size, merkle_tree:size_detail(MySyncStruct)}], Stats);
        bloom when Initiator =:= false ->
            send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                             {start_recon, Method, MySyncStruct, true}}),
            send_local(self(), {shutdown, {ok, build_struct}}),
            Stats;
        art ->
            {AOk, ARStats} = 
                case Initiator of
                    true -> art_recon(MySyncStruct, Params, State);
                    false ->
                        send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                                         {start_recon, Method, MySyncStruct, true}}),
                        {no, Stats}
                end,
            send_local(self(), {shutdown, ?IIF(AOk =:= ok, sync_finished, build_struct)}),
            ARStats
    end.

-spec shutdown(exit_reason(), state()) -> kill.
shutdown(Reason, #rr_recon_state{ownerPid = OwnerL, stats = Stats,
                                 initiator = Initiator}) ->
    ?TRACE("SHUTDOWN Session=~p Reason=~p", [rr_recon_stats:get(session_id, Stats), Reason]),
    Status = exit_reason_to_rc_status(Reason),
    NewStats = rr_recon_stats:set([{status, Status}], Stats),
    send_local(OwnerL, {recon_progress_report, self(), Initiator, NewStats}),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec check_node([merkle_cmp_request()], merkle_tree:merkle_tree() | RestTree) 
        -> {[merkle_cmp_result()], RestTree}
    when
        is_subtype(RestTree, [merkle_tree:mt_node()]).
check_node(L, Tree) ->
    case merkle_tree:is_merkle_tree(Tree) of
        false -> p_check_node(L, Tree, {[], []});
        true -> p_check_node(L, [merkle_tree:get_root(Tree)], {[], []})
    end.

-spec p_check_node([merkle_cmp_request()], [merkle_tree:mt_node()], Acc::Res) -> Res
    when
        is_subtype(Res, {[merkle_cmp_result()], [merkle_tree:mt_node()]}).
p_check_node([], [], {AccR, AccN}) ->
    {lists:reverse(AccR), lists:reverse(AccN)};
p_check_node([?omit | TK], [_Node | TN], Acc) ->
    p_check_node(TK, TN, Acc);
p_check_node([Hash | TK], [Node | TN], {AccR, AccN}) ->
    IsLeaf = merkle_tree:is_leaf(Node),
    case merkle_tree:get_hash(Node) =:= Hash of
        true when IsLeaf -> p_check_node(TK, TN, {[?ok_leaf | AccR], AccN});
        true when not IsLeaf -> p_check_node(TK, TN, {[?ok_inner | AccR], AccN});
        false when IsLeaf -> p_check_node(TK, TN, {[?fail_leaf | AccR], AccN});
        false when not IsLeaf ->
            Childs = merkle_tree:get_childs(Node),
            p_check_node(TK, TN, {[?fail_inner | AccR],
                                  lists:append(lists:reverse(Childs), AccN)}) 
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
        false -> p_process_tree_cmp_result(CmpResult, Tree, BranchSize, NStats, {[], [], []});
        true -> p_process_tree_cmp_result(CmpResult, [merkle_tree:get_root(Tree)], BranchSize, NStats, {[], [], []})
    end.

-spec p_process_tree_cmp_result([merkle_cmp_result()], RestTree, BranchSize, Stats, {Acc::Req, Acc::Res, AccRTree::Res}) 
        -> {Req, Res, Stats, RestTree::Res}           
    when
      is_subtype(BranchSize,pos_integer()),
      is_subtype(Stats,     rr_recon_stats:stats()),
      is_subtype(Req,       [merkle_tree:mt_node_key()]),
      is_subtype(Res,       [merkle_tree:mt_node()]),
      is_subtype(RestTree,  [merkle_tree:mt_node()]).
p_process_tree_cmp_result([], [], _, Stats, {Req, Res, RTree}) ->
    {lists:reverse(Req), Res, Stats, lists:reverse(RTree)};
p_process_tree_cmp_result([?ok_inner | TR], [Node | TN], BS, Stats, Acc) ->
    NStats = rr_recon_stats:inc([{tree_compareSkipped, merkle_tree:size(Node)}], Stats),
    p_process_tree_cmp_result(TR, TN, BS, NStats, Acc);
p_process_tree_cmp_result([?ok_leaf | TR], [_Node | TN], BS, Stats, Acc) ->
    NStats = rr_recon_stats:inc([{tree_compareSkipped, 1}], Stats),
    p_process_tree_cmp_result(TR, TN, BS, NStats, Acc);
p_process_tree_cmp_result([?fail_leaf | TR], [Node | TN], BS, Stats, {Req, Res, RTree}) ->
    p_process_tree_cmp_result(TR, TN, BS, Stats, {Req, [Node | Res], RTree});
p_process_tree_cmp_result([?fail_inner | TR], [Node | TN], BS, Stats, {Req, Res, RTree}) ->
    case merkle_tree:is_leaf(Node) of
        false -> 
            Childs = merkle_tree:get_childs(Node),
            NewReq = [merkle_tree:get_hash(X) || X <- Childs],
            NStats = rr_recon_stats:inc([{tree_compareLeft, length(Childs)}], Stats),
            p_process_tree_cmp_result(TR, TN, BS, NStats,
                                      {lists:append(lists:reverse(NewReq), Req),
                                       Res,
                                       lists:append(lists:reverse(Childs), RTree)});
        true -> 
            NewReq = [?omit || _ <- lists:seq(1, BS)],
            p_process_tree_cmp_result(TR, TN, BS, Stats,
                                      {lists:append(NewReq, Req), [Node | Res], RTree})
    end.

% @doc Starts one resolve process per leaf node in a given node
%      Returns: number of visited leaf nodes and number of leaf resovle requests.
-spec resolve_node(Node | not_found, {Dest::RPid, SID, OwnerRemote::comm:erl_local_pid()})
        -> {Leafs::non_neg_integer(), ResolveReq::non_neg_integer()} when
    is_subtype(RPid,    comm:mypid()),
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
-spec resolve_leaf(Node, {Dest::RPid, SID, OwnerRemote::comm:erl_local_pid()}) -> 1 | 2 when
    is_subtype(RPid,    comm:mypid()),
    is_subtype(Node,    merkle_tree:mt_node()),
    is_subtype(SID,     rrepair:session_id()).
resolve_leaf(Node, {Dest, SID, OwnerL}) ->
    OwnerR = comm:make_global(OwnerL),
    case merkle_tree:get_item_count(Node) of
        0 ->
           send(Dest, {request_resolve, SID, {interval_upd_send, merkle_tree:get_interval(Node), OwnerR}, []}),
           1;
       _ ->
           send_local(OwnerL, {request_resolve, SID, {interval_upd_send, merkle_tree:get_interval(Node), Dest}, [{feedback, OwnerR}]}),
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
                                      ownerPid = OwnerL,
                                      stats = Stats }) ->
    SID = rr_recon_stats:get(session_id, Stats),
    NStats = case merkle_tree:get_interval(Tree) =:= art:get_interval(Art) of
                 true -> 
                     {ASyncLeafs, Stats2} = art_get_sync_leafs([merkle_tree:get_root(Tree)], Art, Stats, []),
                     ResolveCalled = lists:foldl(fun(X, Acc) ->
                                                         Acc + resolve_leaf(X, {DestPid, SID, OwnerL})
                                                 end, 0, ASyncLeafs),
                     rr_recon_stats:inc([{resolve_started, ResolveCalled}], Stats2);
                 false -> Stats
             end,
    {ok, rr_recon_stats:set([{tree_size, merkle_tree:size_detail(Tree)}], NStats)}.

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
      is_subtype(DB_Chunk,     {intervals:interval(), db_chunk_enc()}),
      is_subtype(Recon_Struct, sync_struct()).
build_recon_struct(bloom, {I, DBItems}) ->
    Fpr = get_bloom_fpr(),
    ElementNum = length(DBItems),
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    BF = ?REP_BLOOM:new(ElementNum, Fpr, ?REP_HFS:new(HFCount), DBItems),
    #bloom_recon_struct{ interval = I, bloom = BF };
build_recon_struct(merkle_tree, {I, DBItems}) ->
    merkle_tree:new(I, DBItems, [{branch_factor, get_merkle_branch_factor()},
                                 {bucket_size, get_merkle_bucket_size()}]);
build_recon_struct(art, {I, DBItems}) ->
    Branch = get_merkle_branch_factor(),
    BucketSize = merkle_tree:get_opt_bucket_size(length(DBItems), Branch, 1),
    Tree = merkle_tree:new(I, DBItems, [{branch_factor, Branch},
                                        {bucket_size, BucketSize}]),
    art:new(Tree, get_art_config()).

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
-spec send_chunk_req(DhtPid::LPid, AnswerPid::LPid, ChunkI::I, DestI::I, MaxItems) -> ok when
    is_subtype(LPid,        comm:erl_local_pid()),
    is_subtype(I,           intervals:interval()),
    is_subtype(MaxItems,    pos_integer() | all).
send_chunk_req(DhtPid, SrcPid, I, DestI, MaxItems) ->
    SrcPidReply = comm:reply_as(SrcPid, 4, {rr_recon, data, DestI, '_'}),
    send_local(
      DhtPid,
      {get_chunk, SrcPidReply, I, fun get_chunk_filter/1, fun get_chunk_value/1,
       MaxItems}).

-spec get_chunk_filter(db_entry:entry()) -> boolean().
get_chunk_filter(DBEntry) -> db_entry:get_version(DBEntry) =/= -1.
-spec get_chunk_value(db_entry:entry()) -> db_entry().
get_chunk_value(DBEntry) -> {db_entry:get_key(DBEntry), db_entry:get_version(DBEntry)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec exit_reason_to_rc_status(exit_reason()) -> rr_recon_stats:status().
exit_reason_to_rc_status(negotiate_interval) -> wait;
exit_reason_to_rc_status(sync_finished) -> finish;
exit_reason_to_rc_status(sync_finished_remote) -> finish;
exit_reason_to_rc_status(_) -> abort.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
-spec map_key_to_quadrant(?RT:key(), quadrant()) -> ?RT:key().
map_key_to_quadrant(Key, N) ->
    map_key_to_quadrant_(lists:sort(?RT:get_replica_keys(Key)), N).
-spec map_key_to_quadrant_(RKeys::[?RT:key(),...], quadrant()) -> ?RT:key().
map_key_to_quadrant_(RKeys, N) ->
    lists:nth(N, RKeys).

% @doc Returns the replication quadrant number (starting at 1) in which Key is located.
-spec get_key_quadrant(?RT:key()) -> quadrant().
get_key_quadrant(Key) ->
    get_key_quadrant_(Key, lists:sort(?RT:get_replica_keys(Key))).
-spec get_key_quadrant_(?RT:key(), RKeys::[?RT:key(),...]) -> quadrant().
get_key_quadrant_(Key, RKeys) ->
    util:lists_index_of(Key, RKeys).

-spec add_quadrants_to_key(KeyQ::quadrant(), RKeys::[?RT:key(),...],
                           Add::non_neg_integer(), RepFactor::4) -> ?RT:key().
add_quadrants_to_key(KeyQ, RKeys, Add, RepFactor) when Add =< RepFactor ->
    DestQ0 = KeyQ + Add,
    DestQ = if DestQ0 > RepFactor -> DestQ0 - RepFactor;
               true               -> DestQ0
            end,
    map_key_to_quadrant_(RKeys, DestQ).

% @doc Maps an arbitrary Interval into the given replication quadrant. 
%      The replication degree X divides the keyspace into X replication quadrants.
%      Precondition: Interval (I) is continuous!
%      Result: Continuous left-open interval starting or laying in given RepQuadrant,
%      i.e. the left key of the interval's bounds is in the first quadrant
%      (independent of the left bracket).
-spec map_interval(intervals:continuous_interval(), RepQuadrant::quadrant())
        -> intervals:continuous_interval().
map_interval(I, Q) ->
    ?ASSERT(intervals:is_continuous(I)),
    case intervals:is_all(I) of
        false ->
            {LBr, LKey, RKey, RBr} = intervals:get_bounds(I),
            LRKeys = lists:sort(?RT:get_replica_keys(LKey)),
            RRKeys = lists:sort(?RT:get_replica_keys(RKey)),
            LQ = get_key_quadrant_(LKey, LRKeys),
            RQ = get_key_quadrant_(RKey, RRKeys),
            RepFactor = rep_factor(),
            % same as: (Q - LQ + RepFactor) rem RepFactor
            QDiff = ?IIF(Q > LQ, Q - LQ, Q - LQ + RepFactor),
            NewLKey = add_quadrants_to_key(LQ, LRKeys, QDiff, RepFactor),
            NewRKey = add_quadrants_to_key(RQ, RRKeys, QDiff, RepFactor),
            intervals:new(LBr, NewLKey, NewRKey, RBr);
        true -> I
    end.

% @doc Gets intersection of two associated intervals as sub interval of A.
-spec find_intersection(intervals:continuous_interval(), intervals:continuous_interval())
        -> intervals:continuous_interval().
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
-spec get_interval_size(intervals:continuous_interval()) -> number().
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

-spec encodeBlob(?RT:key(), db_dht:value() | db_dht:version()) -> db_entry_enc().
encodeBlob(A, B) -> 
    term_to_binary([A, "#", B]).

-spec decodeBlob(db_entry_enc()) -> {?RT:key(), db_dht:value() | db_dht:version()} | fail.
decodeBlob(Blob) when is_binary(Blob) ->
    case binary_to_term(Blob) of
        [Key, "#", X] -> {Key, X};
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
% rr_max_items              - max. number of items per bloom filter, if node data count 
%                             exceeds rr_max_items two or more bloom filter will be used
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
            (config:read(rr_recon_method) =:= bloom andalso
                 check_percent(rr_bloom_fpr) andalso
                 config:cfg_is_integer(rr_max_items) andalso
                 config:cfg_is_greater_than(rr_max_items, 0)) orelse
            (config:read(rr_recon_method) =:= merkle_tree andalso
                 config:cfg_is_integer(rr_merkle_branch_factor) andalso
                 config:cfg_is_greater_than(rr_merkle_branch_factor, 1) andalso
                 config:cfg_is_integer(rr_merkle_bucket_size) andalso
                 config:cfg_is_greater_than(rr_merkle_bucket_size, 0)) orelse
            (config:read(rr_recon_method) =:= art andalso
                 config:cfg_is_integer(rr_merkle_branch_factor) andalso
                 config:cfg_is_greater_than(rr_merkle_branch_factor, 1) andalso
                 % NOTE: merkle_tree:get_opt_bucket_size/3 is used to calculate the optimal bucket size
%%                  config:cfg_is_integer(rr_merkle_bucket_size) andalso
%%                  config:cfg_is_greater_than(rr_merkle_bucket_size, 0) andalso
                 check_percent(rr_art_inner_fpr) andalso
                 check_percent(rr_art_leaf_fpr) andalso
                 config:cfg_is_integer(rr_art_correction_factor) andalso
                 config:cfg_is_greater_than(rr_art_correction_factor, 0));
        _ -> true
    end.

-spec get_bloom_fpr() -> float().
get_bloom_fpr() ->
    config:read(rr_bloom_fpr).

-spec get_max_items(method()) -> pos_integer() | all.
get_max_items(Method) ->
    case Method of
        merkle_tree -> all;
        art -> all;
        _ -> config:read(rr_max_items) % bloom, iblt
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
