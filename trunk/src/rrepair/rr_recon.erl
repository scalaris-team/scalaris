% @copyright 2011-2014 Zuse Institute Berlin

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
-export([map_key_to_interval/2, map_key_to_quadrant/2, map_interval/2,
         quadrant_intervals/0]).
-export([get_chunk_kv/1, get_chunk_kvv/1, get_chunk_filter/1]).
%-export([compress_kv_list/4, calc_signature_size_1_to_n/3, calc_signature_size_nm_pair/4]).

%export for testing
-export([find_sync_interval/2, quadrant_subints_/3, key_dist/2]).
-export([merkle_compress_hashlist/4, merkle_decompress_hashlist/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), log:pal("~w: [ ~s:~.0p ] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).
-define(TRACE_SEND(Pid, Msg), ?TRACE("to ~s:~.0p: ~.0p~n", [pid_groups:group_of(comm:make_local(comm:get_plain_pid(Pid))), Pid, Msg])).
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
-export_type([method/0, request/0,
              db_chunk_kv/0, db_chunk_kvv/0]).
-endif.

-type quadrant()       :: 1..4. % 1..rep_factor()
-type method()         :: trivial | bloom | merkle_tree | art.% | iblt.
-type stage()          :: req_shared_interval | build_struct | reconciliation | resolve.

-type exit_reason()    :: empty_interval |      %interval intersection between initator and client is empty
                          recon_node_crash |    %sync partner node crashed
                          sync_finished |       %finish recon on local node
                          sync_finished_remote. %client-side shutdown by merkle-tree recon initiator

-type db_chunk_kv()    :: [{?RT:key(), db_dht:version()}].
-type 'db_chunk_kv+'() :: [{?RT:key(), db_dht:version()},...].
-type db_chunk_kvv()   :: [{?RT:key(), db_dht:version(), db_dht:value()}].
-type 'db_chunk_kvv+'():: [{?RT:key(), db_dht:version(), db_dht:value()},...].

-type signature_size() :: 1..160. % upper bound of 160 (SHA-1) to also limit testing
-type kv_tree()        :: gb_trees:tree(KeyBin::bitstring(), VersionShort::non_neg_integer()).

-record(trivial_recon_struct,
        {
         interval = intervals:empty()                         :: intervals:interval(),
         reconPid = undefined                                 :: comm:mypid() | undefined,
         db_chunk = ?required(trivial_recon_struct, db_chunk) :: bitstring() | kv_tree(),
         sig_size = 128                                       :: signature_size(),
         ver_size = 8                                         :: signature_size()
        }).

-record(bloom_recon_struct,
        {
         interval = intervals:empty()                       :: intervals:interval(),
         reconPid = undefined                               :: comm:mypid() | undefined,
         bloom    = ?required(bloom_recon_struct, bloom)    :: bloom:bloom_filter()
        }).

-record(merkle_params,
        {
         interval       = ?required(merkle_param, interval)       :: intervals:interval(),
         reconPid       = undefined                               :: comm:mypid() | undefined,
         branch_factor  = ?required(merkle_param, branch_factor)  :: pos_integer(),
         bucket_size    = ?required(merkle_param, bucket_size)    :: pos_integer(),
         p1e            = ?required(merkle_param, p1e)            :: float(),
         ni_leaf_count  = ?required(merkle_param, ni_leaf_count)  :: non_neg_integer(),
         ni_lic_diff    = ?required(merkle_param, ni_lic_diff)    :: non_neg_integer() % = MaxLeafCount * BranchFactor - MaxItemsCount
        }).

-record(art_recon_struct,
        {
         art            = ?required(art_recon_struct, art)            :: art:art(),
         branch_factor  = ?required(art_recon_struct, branch_factor)  :: pos_integer(),
         bucket_size    = ?required(art_recon_struct, bucket_size)    :: pos_integer()
        }).

-type sync_struct() :: #trivial_recon_struct{} |
                       #bloom_recon_struct{} |
                       merkle_tree:merkle_tree() |
                       [merkle_tree:mt_node()] |
                       #art_recon_struct{}.
-type parameters() :: #trivial_recon_struct{} |
                      #bloom_recon_struct{} |
                      #merkle_params{} |
                      #art_recon_struct{}.
-type recon_dest() :: ?RT:key() | random.

-type merkle_sync() ::
          {My::inner, Other::leaf,  SigSize::signature_size(), LeafNodes::[merkle_tree:mt_node()],
           FoundHash::boolean()} |
          {My::leaf,  Other::leaf,  SigSize::signature_size(), LeafNode::merkle_tree:mt_node()} |
          {My::leaf,  Other::inner, SigSize::signature_size(), LeafNode::merkle_tree:mt_node(),
           FoundHash::boolean()}.

-record(rr_recon_state,
        {
         ownerPid           = ?required(rr_recon_state, ownerPid)    :: pid(),
         dhtNodePid         = ?required(rr_recon_state, dhtNodePid)  :: pid(),
         dest_rr_pid        = ?required(rr_recon_state, dest_rr_pid) :: comm:mypid(), %dest rrepair pid
         dest_recon_pid     = undefined                              :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                              :: method() | undefined,
         dest_interval      = intervals:empty()                      :: intervals:interval(),
         my_sync_interval   = intervals:empty()                      :: intervals:interval(),
         params             = {}                                     :: parameters() | {}, % parameters from the other node
         struct             = {}                                     :: sync_struct() | {}, % my recon structure
         stage              = req_shared_interval                    :: stage(),
         initiator          = false                                  :: boolean(),
         merkle_sync        = []                                     :: [merkle_sync()],
         misc               = []                                     :: [{atom(), term()}], % any optional parameters an algorithm wants to keep
         kv_list            = []                                     :: db_chunk_kv(),
         stats              = rr_recon_stats:new()                   :: rr_recon_stats:stats(),
         to_resolve         = {[], []}                               :: {ToSend::rr_resolve:kvv_list(), ToReq::[?RT:key()]}
         }).
-type state() :: #rr_recon_state{}.

% Optimum for reducing P1E for the two parts: key and version comparison (factor B for the latter)
-define(TRIVIAL_B, 0.5).
% Optimum for reducing P1E for the two parts: bloom and trivial RC (factor B for the latter)
-define(BLOOM_B, 0.5809402158035948). % 2*math:log(2) / (1+2*math:log(2))

% keep in sync with check_node/5
-define(recon_ok,                       1). % match
-define(recon_fail_stop_leaf,           2). % mismatch, sending node has leaf node
-define(recon_fail_stop_inner_found,    3). % mismatch, sending node has inner node, found match
-define(recon_fail_stop_inner_notfound, 4). % mismatch, sending node has inner node, no match found
-define(recon_fail_cont_inner,          0). % mismatch, both inner nodes (continue!)

-type merkle_cmp_request() :: {Hash::merkle_tree:mt_node_key(), IsLeaf::boolean()}.

-type request() ::
    {start, method(), DestKey::recon_dest()} |
    {create_struct, method(), SenderI::intervals:interval()} | % from initiator
    {start_recon, bloom, #bloom_recon_struct{}} | % to initiator
    {start_recon, merkle_tree, #merkle_params{}} | % to initiator
    {start_recon, art, #art_recon_struct{}}. % to initiator

-type message() ::
    % API
    request() |
    % trivial sync messages
    {resolve_req, BinKeys::bitstring(), SigSize::signature_size()} |
    {resolve_req, DBChunk::bitstring(), SigSize::signature_size(),
     VSize::signature_size(), SenderPid::comm:mypid()} |
    % merkle tree sync messages
    {?check_nodes, SenderPid::comm:mypid(), ToCheck::bitstring(), SigSize::signature_size()} |
    {?check_nodes, ToCheck::bitstring(), SigSizeI::signature_size(),
     SigSizeILeafDiff::0 | signature_size() % = SigSizeL - SigSizeI
    } |
    {?check_nodes_response, FlagsBin::bitstring(), MaxLeafCount::non_neg_integer(),
     MaxLeafItemsCountDiff::non_neg_integer() % = MaxLeafCount * BranchFactor - MaxItemsCount
    } |
    {resolve_req, Hashes::bitstring()} |
    {resolve_req, Hashes::bitstring(), BinKeyList::[[bitstring()]]} |
    {resolve_req, BinKeyList::[[bitstring()]]} |
    % dht node response
    {create_struct2, {get_state_response, MyI::intervals:interval()}} |
    {create_struct2, DestI::intervals:interval(),
     {get_chunk_response, {intervals:interval(), db_chunk_kv()}}} |
    {reconcile, {get_chunk_response, {intervals:interval(), db_chunk_kv()}}} |
    {resolve, {get_chunk_response, {intervals:interval(), db_chunk_kvv()}}} |
    % internal
    {shutdown, exit_reason()} |
    {crash, DeadPid::comm:mypid(), Reason::fd:reason()} |
    {'DOWN', MonitorRef::reference(), process, Owner::pid(), Info::any()}
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
    SyncI = find_sync_interval(MyI, SenderI),
    NewState = State#rr_recon_state{stage = build_struct,
                                    my_sync_interval = SyncI},
    case intervals:is_empty(SyncI) of
        false ->
            case RMethod of
                trivial -> fd:subscribe(DestRRPid);
                bloom   -> fd:subscribe(DestRRPid);
                merkle_tree -> fd:subscribe(DestRRPid);
                _ -> ok
            end,
            % reduce SenderI to the sub-interval matching SyncI, i.e. a mapped SyncI
            SenderSyncI = map_interval(SenderI, SyncI),
            send_chunk_req(DhtPid, self(), SyncI, SenderSyncI, get_max_items(), create_struct),
            NewState;
        true ->
            shutdown(empty_interval, NewState)
    end;

on({create_struct2, DestI, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = build_struct,       initiator = false}) ->
    ?TRACE1(_Msg, State),
    % create recon structure based on all elements in sync interval
    DBList = [{Key, VersionX} || {KeyX, VersionX} <- DBList0,
                                 none =/= (Key = map_key_to_interval(KeyX, DestI))],
    build_struct(DBList, DestI, RestI, State);

on({start_recon, RMethod, Params} = _Msg,
   State = #rr_recon_state{dest_rr_pid = DestRRPid}) ->
    ?TRACE1(_Msg, State),
    % initiator got other node's sync struct or parameters over sync interval
    % (mapped to the initiator's range)
    % -> create own recon structure based on sync interval and reconcile
    % note: sync interval may be an outdated sub-interval of this node's range
    %       -> pay attention when saving values to DB!
    %       (it could be outdated then even if we retrieved the current range now!)
    case RMethod of
        trivial ->
            #trivial_recon_struct{interval = MySyncI, reconPid = DestReconPid,
                                  db_chunk = DBChunk,
                                  sig_size = SigSize, ver_size = VSize} = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(DestRRPid),
            % convert db_chunk to a gb_tree for faster access checks
            DBChunkTree =
                decompress_kv_list(DBChunk, gb_trees:empty(), SigSize, VSize),
            Params1 = Params#trivial_recon_struct{db_chunk = DBChunkTree},
            Reconcile = resolve,
            Stage = resolve;
        bloom ->
            #bloom_recon_struct{interval = MySyncI,
                                reconPid = DestReconPid} = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(DestRRPid),
            Params1 = Params,
            Reconcile = reconcile,
            Stage = reconciliation;
        merkle_tree ->
            #merkle_params{interval = MySyncI, reconPid = DestReconPid} = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(DestRRPid),
            Params1 = Params,
            Reconcile = reconcile,
            Stage = reconciliation;
        art ->
            MySyncI = art:get_interval(Params#art_recon_struct.art),
            DestReconPid = undefined,
            Params1 = Params,
            Reconcile = reconcile,
            Stage = reconciliation
    end,
    % client only sends non-empty sync intervals or exits
    ?DBG_ASSERT(not intervals:is_empty(MySyncI)),
    
    DhtNodePid = State#rr_recon_state.dhtNodePid,
    send_chunk_req(DhtNodePid, self(), MySyncI, MySyncI, get_max_items(), Reconcile),
    State#rr_recon_state{stage = Stage, params = Params1,
                         method = RMethod, initiator = true,
                         my_sync_interval = MySyncI,
                         dest_recon_pid = DestReconPid};

on({resolve, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = resolve,            initiator = true,
                           method = trivial,           dhtNodePid = DhtNodePid,
                           params = #trivial_recon_struct{db_chunk = OtherDBChunk,
                                                          sig_size = SigSize,
                                                          ver_size = VSize} = Params,
                           dest_rr_pid = DestRR_Pid,   stats = Stats,
                           ownerPid = OwnerL, to_resolve = {ToSend, ToReq},
                           dest_recon_pid = DestReconPid}) ->
    ?TRACE1(_Msg, State),

    {ToSend1, ToReq1, OtherDBChunk1} =
        get_full_diff(DBList, OtherDBChunk, ToSend, ToReq, SigSize, VSize),
    ?DBG_ASSERT2(length(ToSend1) =:= length(lists:ukeysort(1, ToSend1)),
                 {non_unique_send_list, ToSend, ToSend1}),
    ?DBG_ASSERT2(length(ToReq1) =:= length(lists:usort(ToReq1)),
                 {non_unique_req_list, ToReq1}),

    %if rest interval is non empty get another chunk
    SyncFinished = intervals:is_empty(RestI),
    Params1 = Params#trivial_recon_struct{db_chunk = OtherDBChunk1},
    if SyncFinished ->
           SID = rr_recon_stats:get(session_id, Stats),
           ?TRACE("Reconcile Trivial Session=~p ; ToSend=~p ; ToReq=~p",
                  [SID, length(ToSend1), length(ToReq1)]),
           NewStats =
               if ToSend1 =/= [] orelse ToReq1 =/= [] ->
                      send(DestRR_Pid, {request_resolve, SID,
                                        {?key_upd, ToSend1, ToReq1},
                                        [{from_my_node, 0},
                                         {feedback_request, comm:make_global(OwnerL)}]}),
                      % we will get one or two replies from a subsequent feedback response
                      FBCount = if ToReq1 =/= [] -> 2;
                                   true -> 1
                                end,
                      rr_recon_stats:inc([{resolve_started, FBCount},
                                          {await_rs_fb, FBCount}], Stats);
                  true ->
                      Stats
               end,

           % let the non-initiator's rr_recon process identify the remaining keys
           Req2Count = gb_trees:size(OtherDBChunk1),
           ToReq2 = util:gb_trees_foldl(
                      fun(KeyBin, _VersionShort, Acc) ->
                              <<Acc/bitstring, KeyBin/bitstring>>
                      end, <<>>, OtherDBChunk1),
           % the non-initiator will use key_upd_send and we must thus increase
           % the number of resolve processes here!
           NewStats2 =
               if Req2Count > 0 ->
                      rr_recon_stats:inc([{resolve_started, 1}], NewStats);
                  true -> NewStats
               end,

           ?TRACE("resolve_req Trivial Session=~p ; ToReq=~p (~p bits)",
                  [SID, Req2Count, erlang:bit_size(ToReq2)]),
           comm:send(DestReconPid, {resolve_req, ToReq2, SigSize}),
           
           Params2 = Params1#trivial_recon_struct{db_chunk = gb_trees:empty()},
           shutdown(sync_finished,
                    State#rr_recon_state{stats = NewStats2, params = Params2,
                                         to_resolve = {[], []}});
       true ->
           send_chunk_req(DhtNodePid, self(), RestI, RestI, get_max_items(), resolve),
           State#rr_recon_state{params = Params1,
                                to_resolve = {ToSend1, ToReq1}}
    end;

on({reconcile, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     initiator = true,
                           method = bloom,             dhtNodePid = DhtNodePid,
                           params = #bloom_recon_struct{bloom = BF},
                           stats = Stats,              kv_list = KVList,
                           dest_recon_pid = DestReconPid}) ->
    ?TRACE1(_Msg, State),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    Diff = case bloom:item_count(BF) of
               0 -> DBList0;
               _ -> [X || X <- DBList0, not bloom:is_element(BF, X)]
           end,
    NewKVList = lists:append(KVList, Diff),

    %if rest interval is non empty start another sync
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(DhtNodePid, self(), RestI, RestI, get_max_items(), reconcile),
           State#rr_recon_state{kv_list = NewKVList};
       true ->
           FullDiffSize = length(NewKVList),
           ?TRACE("Reconcile Bloom Session=~p ; Diff=~p",
                  [rr_recon_stats:get(session_id, Stats), FullDiffSize]),
           if FullDiffSize > 0 ->
                  % start resolve similar to a trivial recon but using the full diff!
                  % (as if non-initiator in trivial recon)
                  % NOTE: reduce P1E for the two parts here (bloom and trivial RC)
                  {BuildTime, {DBChunk, SigSize, VSize}} =
                      util:tc(fun() ->
                                      compress_kv_list_p1e(
                                        NewKVList, FullDiffSize,
                                        bloom:item_count(BF), ?BLOOM_B * get_p1e())
                              end),
                  
                  send(DestReconPid,
                       {resolve_req, DBChunk, SigSize, VSize, comm:this()}),
                  % we will get one reply from a subsequent ?key_upd resolve
                  NewStats = rr_recon_stats:inc([{resolve_started, 1},
                                                 {build_time, BuildTime}], Stats),
                  State#rr_recon_state{stats = NewStats, stage = resolve,
                                       kv_list = NewKVList};
              true ->
                  % must send resolve_req message for the non-initiator to shut down
                  send(DestReconPid, {resolve_req, <<>>, 1, 1, comm:this()}),
                  % note: kv_list has not changed, we can thus use the old State here:
                  shutdown(sync_finished, State)
           end
    end;

on({reconcile, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     initiator = true,
                           method = RMethod,           params = Params})
  when RMethod =:= merkle_tree orelse RMethod =:= art->
    ?TRACE1(_Msg, State),
    % no need to map keys since the other node's sync struct was created with
    % keys mapped to our interval
    MySyncI = case RMethod of
                  merkle_tree -> Params#merkle_params.interval;
                  art         -> art:get_interval(Params#art_recon_struct.art)
              end,
    build_struct(DBList, MySyncI, RestI, State);

on({crash, _Pid, _Reason} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    shutdown(recon_node_crash, State);

on({shutdown, Reason}, State) ->
    shutdown(Reason, State);

on({'DOWN', _MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% trivial/bloom reconciliation sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({resolve_req, BinKeys, SigSize} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = Initiator,
                           method = RMethod,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           kv_list = KVList,          stats = Stats})
  when (RMethod =:= trivial andalso not Initiator) orelse
           (RMethod =:= bloom andalso Initiator) ->
    ?TRACE1(_Msg, State),
    ReqSet = decompress_k_list(BinKeys, gb_sets:empty(), SigSize),
    NewStats =
        case gb_sets:is_empty(ReqSet) of
            true -> Stats;
            _ ->
                ReqKeys = [Key || {Key, _Version} <- KVList,
                                  gb_sets:is_member(compress_key(Key, SigSize),
                                                    ReqSet)],
                
                SID = rr_recon_stats:get(session_id, Stats),
                ?TRACE("Resolve Trivial Session=~p ; ToSend=~p",
                       [SID, length(ReqKeys)]),
                ?DBG_ASSERT2(length(ReqKeys) =:= length(lists:usort(ReqKeys)),
                             {non_unique_send_list, ReqKeys}),
                % note: the resolve request is counted at the initiator and
                %       thus from_my_node must be set accordingly on this node!
                send_local(OwnerL, {request_resolve, SID,
                                    {key_upd_send, DestRRPid, ReqKeys, []},
                                    [{feedback_request, comm:make_global(OwnerL)},
                                     {from_my_node, ?IIF(Initiator, 1, 0)}]}),
                % we will get one reply from a subsequent ?key_upd resolve
                rr_recon_stats:inc([{resolve_started, 1}], Stats)
        end,
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats});

on({resolve_req, DBChunk, SigSize, VSize, DestReconPid} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = bloom,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           kv_list = KVList,          stats = Stats}) ->
    ?TRACE1(_Msg, State),
    
    DBChunkTree =
        decompress_kv_list(DBChunk, gb_trees:empty(), SigSize, VSize),

    NewStats2 =
        case gb_trees:is_empty(DBChunkTree) of
            true ->
                % nothing to do if the chunk is empty:
                Stats;
            _ ->
                SID = rr_recon_stats:get(session_id, Stats),
                {ToSendKeys1, ToReq1, DBChunkTree1} =
                    get_part_diff(KVList, DBChunkTree, [], [], SigSize, VSize),

                ?TRACE("Resolve Bloom Session=~p ; ToSend=~p ; ToReq=~p",
                       [SID, length(ToSendKeys1), length(ToReq1)]),
                ?DBG_ASSERT2(length(ToSendKeys1) =:= length(lists:usort(ToSendKeys1)),
                             {non_unique_send_list, ToSendKeys1}),
                ?DBG_ASSERT2(length(ToReq1) =:= length(lists:usort(ToReq1)),
                             {non_unique_req_list, ToReq1}),
                
                % note: the resolve request was counted at the initiator and
                %       thus from_my_node must be 0 on this node!
                send_local(OwnerL, {request_resolve, SID,
                                    {key_upd_send, DestRRPid, ToSendKeys1, ToReq1},
                                    [{from_my_node, 0},
                                     {feedback_request, comm:make_global(OwnerL)}]}),
                % we will get one reply from a subsequent feedback response
                NewStats1 = rr_recon_stats:inc([{resolve_started, 1}], Stats),
                
                % let the initiator's rr_recon process identify the remaining keys
                Req2Count = gb_trees:size(DBChunkTree1),
                ToReq2 = util:gb_trees_foldl(
                           fun(KeyBin, _VersionShort, Acc) ->
                                   <<Acc/bitstring, KeyBin/bitstring>>
                           end, <<>>, DBChunkTree1),
                ?TRACE("resolve_req Bloom Session=~p ; ToReq=~p (~p bits)",
                       [SID, Req2Count, erlang:bit_size(ToReq2)]),
                comm:send(DestReconPid, {resolve_req, ToReq2, SigSize}),
                if Req2Count > 0 ->
                       rr_recon_stats:inc([{resolve_started, 1}], NewStats1);
                   true -> NewStats1
                end
        end,

    shutdown(sync_finished, State#rr_recon_state{stats = NewStats2});

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({?check_nodes, SenderPid, ToCheck, SigSize}, State)
  when is_bitstring(ToCheck) andalso is_integer(SigSize) ->
    NewState = State#rr_recon_state{dest_recon_pid = SenderPid},
    on({?check_nodes, ToCheck, SigSize, 0}, NewState);

on({?check_nodes, ToCheck0, SigSizeI, SigSizeILeafDiff},
   State = #rr_recon_state{stage = reconciliation,    initiator = false,
                           method = merkle_tree,      merkle_sync = MerkleSync,
                           params = #merkle_params{bucket_size = BucketSize} = Params,
                           struct = Tree,             ownerPid = OwnerL,
                           dest_rr_pid = DestNodePid, stats = Stats,
                           dest_recon_pid = DestReconPid})
  when is_bitstring(ToCheck0) andalso is_integer(SigSizeI) andalso is_integer(SigSizeILeafDiff) ->
    ?DBG_ASSERT(comm:is_valid(DestReconPid)),
    SigSizeL = SigSizeI + SigSizeILeafDiff,
    ToCheck = merkle_decompress_hashlist(ToCheck0, [], SigSizeI, SigSizeL),
    {FlagsBin, RTree, MerkleSyncNew, MaxLeafCount, MaxItemsCount} =
        check_node(ToCheck, Tree, SigSizeI, SigSizeL, MerkleSync),
    ?DBG_ASSERT(MaxLeafCount * BucketSize >= MaxItemsCount),
    MaxLeafItemsCountDiff = MaxLeafCount * BucketSize - MaxItemsCount,
    send(DestReconPid, {?check_nodes_response, FlagsBin, MaxLeafCount, MaxLeafItemsCountDiff}),
    NewState = State#rr_recon_state{struct = RTree, merkle_sync = MerkleSyncNew},
    if RTree =:= [] andalso MerkleSyncNew =:= [] ->
           shutdown(sync_finished, NewState);
       RTree =:= [] ->
            ?TRACE("Sync (NI): ~.2p", [MerkleSyncNew]),
            {Hashes, NStats} =
                merkle_resolve_leaves_noninit(MerkleSyncNew, DestNodePid, Stats, OwnerL, Params),
            send(DestReconPid, {resolve_req, Hashes}),
            NewState#rr_recon_state{stage = resolve, stats = NStats};
       true -> NewState
    end;

on({?check_nodes_response, FlagsBin, OtherMaxLeafCount, MaxLeafItemsCountDiff},
   State = #rr_recon_state{stage = reconciliation,        initiator = true,
                           method = merkle_tree,          merkle_sync = MerkleSync,
                           params = #merkle_params{bucket_size = BucketSize} = Params,
                           struct = Tree,                 stats = Stats,
                           dest_recon_pid = DestReconPid,
                           misc = [{signature_size, {SigSizeI, SigSizeL}},
                                   {p1e, {P1ETotal_I, _P1ETotal_L}}]}) ->
    case process_tree_cmp_result(FlagsBin, Tree, SigSizeI, SigSizeL, MerkleSync, Stats) of
        {[] = RTree, [] = MerkleSyncNew, NStats, _MyMaxLeafCount, _MyMaxItemsCount} ->
            NStage = reconciliation,
            P1E_I = P1E_L = P1ETotal_I,
            NextSigSizeI = SigSizeI,
            NextSigSizeL = SigSizeL,
            ok;
        {[] = RTree, [_|_] = MerkleSyncNew, NStats, _MyMaxLeafCount, _MyMaxItemsCount} ->
            NStage = resolve,
            P1E_I = P1E_L = P1ETotal_I,
            NextSigSizeI = SigSizeI,
            NextSigSizeL = SigSizeL,
            % note: we will get a resolve_req message from the other node
            ?TRACE("Sync (I): ~.2p", [MerkleSyncNew]),
            ok;
        {[_|_] = RTree, MerkleSyncNew, NStats, MyMaxLeafCount, MyMaxItemsCount} ->
            NStage = reconciliation,
            OtherMaxItemsCount = OtherMaxLeafCount * BucketSize - MaxLeafItemsCountDiff,
            {P1E_I, P1E_L, NextSigSizeI, NextSigSizeL} =
                merkle_next_signature_sizes(Params, P1ETotal_I, MyMaxLeafCount, MyMaxItemsCount,
                                            OtherMaxLeafCount, OtherMaxItemsCount),
            Req = merkle_compress_hashlist(RTree, <<>>, NextSigSizeI, NextSigSizeL),
            send(DestReconPid, {?check_nodes, Req, NextSigSizeI, NextSigSizeL - NextSigSizeI})
    end,
    NewState = State#rr_recon_state{stats = NStats, struct = RTree,
                                    misc = [{signature_size, {NextSigSizeI, NextSigSizeL}},
                                            {p1e, {P1E_I, P1E_L}}],
                                    merkle_sync = MerkleSyncNew, stage = NStage},
    if RTree =:= [] andalso MerkleSyncNew =:= [] ->
           %send(DestReconPid, {shutdown, sync_finished_remote}),
           shutdown(sync_finished, NewState);
       true -> NewState
    end;

on({resolve_req, Hashes} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = true,
                           method = merkle_tree,      merkle_sync = MerkleSync,
                           params = Params,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           dest_recon_pid = DestRCPid,
                           stats = Stats}) when is_bitstring(Hashes) ->
    ?TRACE1(_Msg, State),
    {HashesReply, BinKeyList, NStats} =
        merkle_resolve_leaves_init(MerkleSync, Hashes, DestRRPid, Stats, OwnerL, Params),
    comm:send(DestRCPid, {resolve_req, HashesReply, BinKeyList}),
    NewState = State#rr_recon_state{stats = NStats},
    % do not shutdown if HashesReply is non-empty!
    case HashesReply of
        <<>> ->
            %send(DestReconPid, {shutdown, sync_finished_remote}),
            shutdown(sync_finished, NewState);
        _ -> NewState
    end;

on({resolve_req, Hashes, BinKeyList} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = merkle_tree,      merkle_sync = MerkleSync,
                           params = Params,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           dest_recon_pid = DestRCPid,
                           stats = Stats})
  when is_bitstring(Hashes) andalso is_list(BinKeyList) ->
    ?TRACE1(_Msg, State),
    BucketSizeBits = merkle_max_bucket_size_bits(Params),
    {BinKeyListReq, NStats} =
        merkle_resolve_req_keys_noninit(
          MerkleSync, Hashes, BinKeyList, DestRRPid, Stats, OwnerL, BucketSizeBits, [],
          [], [], false, BinKeyList =/= []),
    case Hashes of
        <<>> -> ?DBG_ASSERT(BinKeyListReq =:= []),
                ok;
        _    -> comm:send(DestRCPid, {resolve_req, BinKeyListReq})
    end,
    shutdown(sync_finished, State#rr_recon_state{stats = NStats});

on({resolve_req, BinKeyList} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = true,
                           method = merkle_tree,      merkle_sync = MerkleSync,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           stats = Stats}) when is_list(BinKeyList) ->
    ?TRACE1(_Msg, State),
    NStats =
        merkle_resolve_req_keys_init(
          MerkleSync, BinKeyList, DestRRPid, Stats, OwnerL, [],
          BinKeyList =/= []),
    %send(DestReconPid, {shutdown, sync_finished_remote}),
    shutdown(sync_finished, State#rr_recon_state{stats = NStats}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_struct(DBList::db_chunk_kv(), DestI::intervals:non_empty_interval(),
                   RestI::intervals:interval(), state()) -> state() | kill.
build_struct(DBList, SyncI, RestI,
             State = #rr_recon_state{method = RMethod, params = Params,
                                     struct = OldSyncStruct,
                                     initiator = Initiator, stats = Stats,
                                     dhtNodePid = DhtNodePid, stage = Stage,
                                     kv_list = KVList}) ->
    ?DBG_ASSERT(not intervals:is_empty(SyncI)),
    % note: RestI already is a sub-interval of the sync interval
    BeginSync =
        case intervals:is_empty(RestI) of
            false ->
                Reconcile =
                    if Initiator andalso (Stage =:= reconciliation) -> reconcile;
                       true -> create_struct
                    end,
                send_chunk_req(DhtNodePid, self(), RestI, SyncI,
                               get_max_items(), Reconcile),
                false;
            true -> true
        end,
    NewKVList = lists:append(KVList, DBList),
    if BeginSync ->
           ToBuild = if Initiator andalso RMethod =:= art -> merkle_tree;
                        true -> RMethod
                     end,
           {BuildTime, SyncStruct} =
               util:tc(fun() -> build_recon_struct(ToBuild, OldSyncStruct, SyncI,
                                                   NewKVList, Params, BeginSync)
                       end),
           Stats1 = rr_recon_stats:inc([{build_time, BuildTime}], Stats),
           NewState = State#rr_recon_state{struct = SyncStruct, stats = Stats1,
                                           kv_list = NewKVList},
           begin_sync(SyncStruct, Params, NewState#rr_recon_state{stage = reconciliation});
       true ->
           % keep stage (at initiator: reconciliation, at other: build_struct)
           State#rr_recon_state{kv_list = NewKVList}
    end.

-spec begin_sync(MySyncStruct::sync_struct(), OtherSyncStruct::parameters() | {},
                 state()) -> state() | kill.
begin_sync(MySyncStruct, _OtherSyncStruct = {},
           State = #rr_recon_state{method = trivial, initiator = false,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    send(DestRRPid, {?IIF(SID =:= null, start_recon, continue_recon),
                     comm:make_global(OwnerL), SID,
                     {start_recon, trivial, MySyncStruct}}),
    State#rr_recon_state{struct = {}, stage = resolve};
begin_sync(MySyncStruct, _OtherSyncStruct = {},
           State = #rr_recon_state{method = bloom, initiator = false,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    send(DestRRPid, {?IIF(SID =:= null, start_recon, continue_recon),
                     comm:make_global(OwnerL), SID,
                     {start_recon, bloom, MySyncStruct}}),
    State#rr_recon_state{struct = {}, stage = resolve};
begin_sync(MySyncStruct, _OtherSyncStruct,
           State = #rr_recon_state{method = merkle_tree, initiator = Initiator,
                                   ownerPid = OwnerL, stats = Stats,
                                   params = Params,
                                   dest_recon_pid = DestReconPid,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    Stats1 =
        rr_recon_stats:set(
          [{tree_size, merkle_tree:size_detail(MySyncStruct)}], Stats),
    case Initiator of
        true ->
            #merkle_params{bucket_size = BucketSize,
                           p1e = P1ETotal,
                           ni_leaf_count = OtherLeafCount,
                           ni_lic_diff = LeafItemsCountDiff} = Params,
            MyLeafCount = merkle_tree:get_leaf_count(MySyncStruct),
            MyItemCount = merkle_tree:get_item_count(MySyncStruct),

            OtherItemsCount = OtherLeafCount * BucketSize - LeafItemsCountDiff,
            {P1E_I, P1E_L, NextSigSizeI, NextSigSizeL} =
                merkle_next_signature_sizes(Params, P1ETotal, MyLeafCount, MyItemCount,
                                            OtherLeafCount, OtherItemsCount),

            RootNode = merkle_tree:get_root(MySyncStruct),
            SigSize = case merkle_tree:is_leaf(RootNode) of
                          true  -> NextSigSizeL;
                          false -> NextSigSizeI
                      end,

            Req = merkle_compress_hashlist([RootNode], <<>>, SigSize, SigSize),
            send(DestReconPid, {?check_nodes, comm:this(), Req, SigSize}),
            State#rr_recon_state{stats = Stats1,
                                 misc = [{signature_size, {SigSize, SigSize}},
                                         {p1e, {P1E_I, P1E_L}}],
                                 kv_list = []};
        false ->
            MerkleI = merkle_tree:get_interval(MySyncStruct),
            MerkleV = merkle_tree:get_branch_factor(MySyncStruct),
            MerkleB = merkle_tree:get_bucket_size(MySyncStruct),
            P1ETotal = get_p1e(),

            LeafCount = merkle_tree:get_leaf_count(MySyncStruct),
            ItemCount = merkle_tree:get_item_count(MySyncStruct),
            ?DBG_ASSERT(LeafCount * MerkleB >= ItemCount),
            LeafItemsCountDiff = LeafCount * MerkleB - ItemCount,

            MySyncParams = #merkle_params{interval = MerkleI,
                                          branch_factor = MerkleV,
                                          bucket_size = MerkleB,
                                          p1e = P1ETotal,
                                          ni_leaf_count = LeafCount,
                                          ni_lic_diff = LeafItemsCountDiff},
            SyncParams = MySyncParams#merkle_params{reconPid = comm:this()},
            SID = rr_recon_stats:get(session_id, Stats),
            send(DestRRPid, {?IIF(SID =:= null, start_recon, continue_recon),
                             comm:make_global(OwnerL), SID,
                             {start_recon, merkle_tree, SyncParams}}),
            State#rr_recon_state{stats = Stats1, params = MySyncParams,
                                 kv_list = []}
    end;
begin_sync(MySyncStruct, OtherSyncStruct,
           State = #rr_recon_state{method = art, initiator = Initiator,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    case Initiator of
        true ->
            Stats1 = art_recon(MySyncStruct, OtherSyncStruct#art_recon_struct.art, State),
            shutdown(sync_finished, State#rr_recon_state{stats = Stats1,
                                                         kv_list = []});
        false ->
            SID = rr_recon_stats:get(session_id, Stats),
            send(DestRRPid, {?IIF(SID =:= null, start_recon, continue_recon),
                             comm:make_global(OwnerL), SID,
                             {start_recon, art, MySyncStruct}}),
            shutdown(sync_finished, State#rr_recon_state{kv_list = []})
    end.

-spec shutdown(exit_reason(), state()) -> kill.
shutdown(Reason, #rr_recon_state{ownerPid = OwnerL, stats = Stats,
                                 initiator = Initiator, dest_rr_pid = DestRR,
                                 dest_recon_pid = DestRC, method = RMethod,
                                 my_sync_interval = SyncI}) ->
    ?TRACE("SHUTDOWN Session=~p Reason=~p",
           [rr_recon_stats:get(session_id, Stats), Reason]),

    % unsubscribe from fd if a subscription was made:
    case Initiator orelse (not intervals:is_empty(SyncI)) of
        true ->
            case RMethod of
                trivial -> fd:unsubscribe(DestRR);
                bloom   -> fd:unsubscribe(DestRR);
                merkle_tree -> fd:unsubscribe(DestRR);
                _ -> ok
            end;
        false -> ok
    end,

    Status = exit_reason_to_rc_status(Reason),
    NewStats = rr_recon_stats:set([{status, Status}], Stats),
    send_local(OwnerL, {recon_progress_report, comm:this(), Initiator, DestRR,
                        DestRC, NewStats}),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% KV-List compression
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculates the minimum number of bits needed to have a hash collision
%%      probability of P1E, given we compare N hashes with M other hashes
%%      pairwise with each other (assuming the worst case, i.e. we have M+N
%%      hashes).
-spec calc_signature_size_nm_pair(N::non_neg_integer(), M::non_neg_integer(),
                                  P1E::float(), MaxSize::signature_size())
        -> SigSize::signature_size().
calc_signature_size_nm_pair(0, 0, P1E, _MaxSize) when P1E > 0 ->
    1;
calc_signature_size_nm_pair(0, 1, P1E, _MaxSize) when P1E > 0 ->
    1;
calc_signature_size_nm_pair(1, 0, P1E, _MaxSize) when P1E > 0 ->
    1;
calc_signature_size_nm_pair(N, M, P1E, MaxSize) when P1E > 0 ->
    min_max(util:ceil(util:log2((M+N)*(M+N-1) / P1E) - 1), 1, MaxSize).

%% @doc Transforms a list of key and version tuples (with unique keys), into a
%%      compact binary representation for transfer.
-spec compress_kv_list(KVList::db_chunk_kv(), Bin,
                       SigSize::signature_size(), VSize::signature_size())
        -> Bin when is_subtype(Bin, bitstring()).
compress_kv_list([], Bin, _SigSize, _VSize) ->
    Bin;
compress_kv_list([{K0, V} | TL], Bin, SigSize, VSize) ->
    KBin = compress_key(K0, SigSize),
    compress_kv_list(TL, <<Bin/bitstring, KBin/bitstring, V:VSize>>, SigSize, VSize).

%% @doc De-compresses the binary from compress_kv_list/4 into a gb_tree with a
%%      binary key representation and the integer of the (shortened) version.
-spec decompress_kv_list(CompressedBin::bitstring(), AccTree::kv_tree(),
                         SigSize::signature_size(), VSize::signature_size())
        -> ResTree::kv_tree().
decompress_kv_list(<<>>, Tree, _SigSize, _VSize) ->
    Tree;
decompress_kv_list(Bin, Tree, SigSize, VSize) ->
    <<KeyBin:SigSize/bitstring, Version:VSize, T/bitstring>> = Bin,
    Tree1 = gb_trees:enter(KeyBin, Version, Tree),
    decompress_kv_list(T, Tree1, SigSize, VSize).

%% @doc Gets all entries from MyEntries which are not encoded in MyIOtherKvTree
%%      or the entry in MyEntries has a newer version than the one in the tree
%%      and returns them as FBItems. ReqItems contains items in the tree but
%%      where the version in MyEntries is older than the one in the tree.
-spec get_full_diff
        (MyEntries::[], MyIOtherKvTree::kv_tree(),
         AccFBItems::FBItems, AccReqItems::[?RT:key()],
         SigSize::signature_size(), VSize::signature_size())
        -> {FBItems::FBItems, ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()}
            when is_subtype(FBItems, rr_resolve:kvv_list() | [?RT:key()]);
        (MyEntries::'db_chunk_kvv+'(), MyIOtherKvTree::kv_tree(),
         AccFBItems::rr_resolve:kvv_list(), AccReqItems::[?RT:key()],
         SigSize::signature_size(), VSize::signature_size())
        -> {FBItems::rr_resolve:kvv_list(), ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()};
        (MyEntries::'db_chunk_kv+'(), MyIOtherKvTree::kv_tree(),
         AccFBItems::[?RT:key()], AccReqItems::[?RT:key()],
         SigSize::signature_size(), VSize::signature_size())
        -> {FBItems::[?RT:key()], ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()}.
get_full_diff(MyEntries, MyIOtKvTree, FBItems, ReqItems, SigSize, VSize) ->
    get_full_diff_(MyEntries, MyIOtKvTree, FBItems, ReqItems, SigSize,
                  util:pow(2, VSize)).

%% @doc Helper for get_full_diff/6.
-spec get_full_diff_
        (MyEntries::[], MyIOtherKvTree::kv_tree(),
         AccFBItems::FBItems, AccReqItems::[?RT:key()],
         SigSize::signature_size(), VMod::pos_integer())
        -> {FBItems::FBItems, ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()}
            when is_subtype(FBItems, rr_resolve:kvv_list() | [?RT:key()]);
        (MyEntries::'db_chunk_kvv+'(), MyIOtherKvTree::kv_tree(),
         AccFBItems::rr_resolve:kvv_list(), AccReqItems::[?RT:key()],
         SigSize::signature_size(), VMod::pos_integer())
        -> {FBItems::rr_resolve:kvv_list(), ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()};
        (MyEntries::'db_chunk_kv+'(), MyIOtherKvTree::kv_tree(),
         AccFBItems::[?RT:key()], AccReqItems::[?RT:key()],
         SigSize::signature_size(), VMod::pos_integer())
        -> {FBItems::[?RT:key()], ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()}.
get_full_diff_([], MyIOtKvTree, FBItems, ReqItems, _SigSize, _VMod) ->
    {FBItems, ReqItems, MyIOtKvTree};
get_full_diff_([Tpl | Rest], MyIOtKvTree, FBItems, ReqItems, SigSize, VMod) ->
    Key = element(1, Tpl),
    Version = element(2, Tpl),
    TplSize = tuple_size(Tpl),
    {KeyBin, VersionShort} = compress_kv_pair(Key, Version, SigSize, VMod),
    case gb_trees:lookup(KeyBin, MyIOtKvTree) of
        none when TplSize =:= 3 ->
            Value = element(3, Tpl),
            get_full_diff_(Rest, MyIOtKvTree, [{Key, Value, Version} | FBItems],
                           ReqItems, SigSize, VMod);
        none when TplSize =:= 2 ->
            get_full_diff_(Rest, MyIOtKvTree, [Key | FBItems],
                           ReqItems, SigSize, VMod);
        {value, OtherVersionShort} ->
            MyIOtKvTree2 = gb_trees:delete(KeyBin, MyIOtKvTree),
            if VersionShort > OtherVersionShort andalso TplSize =:= 3 ->
                   Value = element(3, Tpl),
                   get_full_diff_(Rest, MyIOtKvTree2, [{Key, Value, Version} | FBItems],
                                  ReqItems, SigSize, VMod);
               VersionShort > OtherVersionShort andalso TplSize =:= 2 ->
                   get_full_diff_(Rest, MyIOtKvTree2, [Key | FBItems],
                                  ReqItems, SigSize, VMod);
               VersionShort =:= OtherVersionShort ->
                   get_full_diff_(Rest, MyIOtKvTree2, FBItems,
                                  ReqItems, SigSize, VMod);
               true ->
                   get_full_diff_(Rest, MyIOtKvTree2, FBItems,
                                  [Key | ReqItems], SigSize, VMod)
            end
    end.

%% @doc Gets all entries from MyEntries which are in MyIOtherKvTree
%%      and the entry in MyEntries has a newer version than the one in the tree
%%      and returns them as FBItems. ReqItems contains items in the tree but
%%      where the version in MyEntries is older than the one in the tree.
-spec get_part_diff(MyEntries::db_chunk_kv(), MyIOtherKvTree::kv_tree(),
                    AccFBItems::[?RT:key()], AccReqItems::[?RT:key()],
                    SigSize::signature_size(), VSize::signature_size())
        -> {FBItems::[?RT:key()], ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()}.
get_part_diff(MyEntries, MyIOtKvTree, FBItems, ReqItems, SigSize, VSize) ->
    get_part_diff_(MyEntries, MyIOtKvTree, FBItems, ReqItems, SigSize,
                   util:pow(2, VSize)).

%% @doc Helper for get_part_diff/6.
-spec get_part_diff_(MyEntries::db_chunk_kv(), MyIOtherKvTree::kv_tree(),
                     AccFBItems::[?RT:key()], AccReqItems::[?RT:key()],
                     SigSize::signature_size(), VMod::pos_integer())
        -> {FBItems::[?RT:key()], ReqItems::[?RT:key()], MyIOtherKvTree::kv_tree()}.
get_part_diff_([], MyIOtKvTree, FBItems, ReqItems, _SigSize, _VMod) ->
    {FBItems, ReqItems, MyIOtKvTree};
get_part_diff_([{Key, Version} | Rest], MyIOtKvTree, FBItems, ReqItems, SigSize, VMod) ->
    {KeyBin, VersionShort} = compress_kv_pair(Key, Version, SigSize, VMod),
    case gb_trees:lookup(KeyBin, MyIOtKvTree) of
        none ->
            get_part_diff_(Rest, MyIOtKvTree, FBItems, ReqItems,
                           SigSize, VMod);
        {value, OtherVersionShort} ->
            MyIOtKvTree2 = gb_trees:delete(KeyBin, MyIOtKvTree),
            if VersionShort > OtherVersionShort ->
                   get_part_diff_(Rest, MyIOtKvTree2, [Key | FBItems], ReqItems,
                                  SigSize, VMod);
               VersionShort =:= OtherVersionShort ->
                   get_part_diff_(Rest, MyIOtKvTree2, FBItems, ReqItems,
                                  SigSize, VMod);
               true ->
                   get_part_diff_(Rest, MyIOtKvTree2, FBItems, [Key | ReqItems],
                                  SigSize, VMod)
            end
    end.

%% @doc Transforms a single key and version tuple into a compact binary
%%      representation.
%%      Similar to compress_kv_list/4.
-spec compress_kv_pair(Key::?RT:key(), Version::db_dht:version(),
                        SigSize::signature_size(), VMod::pos_integer())
        -> {BinKey::bitstring(), VersionShort::integer()}.
compress_kv_pair(Key, Version, SigSize, VMod) ->
    KeyBin = compress_key(Key, SigSize),
    VersionShort = Version rem VMod,
    {<<KeyBin/bitstring>>, VersionShort}.

%% @doc Transforms a single key into a compact binary representation.
%%      Similar to compress_kv_pair/4.
-spec compress_key(Key::?RT:key(), SigSize::signature_size())
        -> BinKey::bitstring().
compress_key(Key, SigSize) ->
    KBin = erlang:term_to_binary(Key),
    RestSize = erlang:bit_size(KBin) - SigSize,
    if RestSize >= 0  ->
           <<_:RestSize/bitstring, KBinCompressed:SigSize/bitstring>> = KBin,
           KBinCompressed;
       true ->
           FillSize = erlang:abs(RestSize),
           <<0:FillSize, KBin/binary>>
    end.

%% @doc De-compresses a bitstring with hashes of SigSize number of bits
%%      into a gb_set with a binary key representation.
-spec decompress_k_list(CompressedBin::bitstring(), AccSet::gb_sets:set(bitstring()),
                         SigSize::signature_size()) -> ResSet::gb_sets:set(bitstring()).
decompress_k_list(<<>>, Set, _SigSize) ->
    Set;
decompress_k_list(Bin, Set, SigSize) ->
    <<KeyBin:SigSize/bitstring, T/bitstring>> = Bin,
    Set1 = gb_sets:add(KeyBin, Set),
    decompress_k_list(T, Set1, SigSize).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculates the minimum number of bits needed to have a hash collision
%%      probability of P1E, given we compare one hash with N other hashes.
-spec calc_signature_size_1_to_n(N::non_neg_integer(), P1E::float(),
                                 MaxSize::signature_size())
        -> SigSize::signature_size().
calc_signature_size_1_to_n(0, P1E, _MaxSize) when P1E > 0 ->
    1;
calc_signature_size_1_to_n(N, P1E, MaxSize) when P1E > 0 ->
    min_max(util:ceil(util:log2(N / P1E)), 1, MaxSize).

%% @doc Calculates from a total P1E the (next) P1E to use for signature and
%%      sub-tree/trivial reconciliations.
-spec merkle_next_p1e(BranchFactor::pos_integer(), P1ETotal::float())
    -> {P1E_I::float(), P1E_L::float()}.
merkle_next_p1e(BranchFactor, P1ETotal) ->
    % mistakes caused by:
    % inner node: current node or any of its BranchFactor children (B=BranchFactor+1)
    % leaf node: current node or the following trivial reconciliation (B=2)
    % => current node's probability of 0 errors = P0E(child)^B
    % however, we cannot use P0E=(1-P1E) since it is near 1 and its floating
    % point representation is sub-optimal!
    % => use Taylor expansion of P1E_next = 1 - (1-P1E)^(1/B)
    BI = BranchFactor + 1,
    P1E_I = P1ETotal / BI +
              (BI - 1) * P1ETotal * P1ETotal / (2 * BI * BI), % +O[p^3]
    P1E_L = P1ETotal / 2 + P1ETotal * P1ETotal / 8, % +O[p^3]
    {P1E_I, P1E_L}.

%% @doc Calculates the new signature sizes based on the next P1E as in
%%      merkle_next_p1e/2
-spec merkle_next_signature_sizes(
        Params::#merkle_params{}, P1ETotal::float(),
        MyMaxLeafCount::non_neg_integer(), MyMaxItemsCount::non_neg_integer(),
        OtherMaxLeafCount::non_neg_integer(), OtherMaxItemsCount::non_neg_integer())
    -> {P1E_I::float(), P1E_L::float(),
        NextSigSizeI::signature_size(), NextSigSizeL::signature_size()}.
merkle_next_signature_sizes(
  #merkle_params{bucket_size = BucketSize, branch_factor = BranchFactor}, P1ETotal,
  MyMaxLeafCount, MyMaxItemsCount, OtherMaxLeafCount, OtherMaxItemsCount) ->
    {P1E_I, P1E_L} = merkle_next_p1e(BranchFactor, P1ETotal),

    % note: we need to use the same P1E for this level's signature
    %       comparison as a children's tree has in total!
    NextSigSizeI =
        min_max(
          util:ceil(
            util:log2(erlang:max(1, MyMaxItemsCount + OtherMaxItemsCount)
                          / P1E_I)), 1, 160),

    L = erlang:max(MyMaxLeafCount, OtherMaxLeafCount),
    % exact, but due to problems with precision near 1 not feasible:
%%     NextSigSizeL =
%%         min_max(
%%           util:ceil(
%%             util:log2(
%%               % note: see TODO above for P0E precision!
%%               1 / (1 - math:pow(1 - P1E_L / (2 * BucketSize), 1 / L))
%%                      )), 1, 160),
    % approx (Puiseux Series of the above):
    NextSigSizeL =
        min_max(
          util:ceil(
            (math:log(2 * BucketSize * L) - math:log(P1E_L)) / math:log(2) +
                (1 - L) * P1E_L / (BucketSize * L * math:log(16))
                   ), 1, 160),

%%     log:pal("P1E_I: ~p, \tP1E_L: ~p, \tSigSizeI: ~B, \tSigSizeL: ~B~n"
%%             "Buckets: ~B, \tMyMI: ~B, \tOtMI: ~B, \tMyML: ~B, \tOtML: ~B",
%%             [P1E_I, P1E_L, NextSigSizeI, NextSigSizeL,
%%              BucketSize, MyMaxItemsCount, OtherMaxItemsCount, MyMaxLeafCount, OtherMaxLeafCount]),
    {P1E_I, P1E_L, NextSigSizeI, NextSigSizeL}.

-compile({nowarn_unused_function, {min_max_feeder, 3}}).
-spec min_max_feeder(X::number(), Min::number(), Max::number())
        -> {X::number(), Min::number(), Max::number()}.
min_max_feeder(X, Min, Max) when Min > Max -> {X, Max, Min};
min_max_feeder(X, Min, Max) -> {X, Min, Max}.

%% @doc Sets min and max boundaries for X and returns either Min, Max or X.
-spec min_max(X::number(), Min::number(), Max::number()) -> number().
min_max(X, Min, _Max) when X =< Min ->
    ?DBG_ASSERT(Min =< _Max),
    Min;
min_max(X, _Min, Max) when X >= Max ->
    ?DBG_ASSERT(_Min =< Max),
    Max;
min_max(X, _Min, _Max) ->
    ?DBG_ASSERT(_Min =< _Max),
    X.

%% @doc Transforms a list of merkle keys, i.e. hashes, into a compact binary
%%      representation for transfer.
-spec merkle_compress_hashlist(Nodes::[merkle_tree:mt_node()], Bin,
                               SigSizeI::signature_size(),
                               SigSizeL::signature_size()) -> Bin
    when is_subtype(Bin, bitstring()).
merkle_compress_hashlist([], Bin, _SigSizeI, _SigSizeL) ->
    Bin;
merkle_compress_hashlist([N1 | TL], Bin, SigSizeI, SigSizeL) ->
    H1 = merkle_tree:get_hash(N1),
    case merkle_tree:is_leaf(N1) of
        true ->
            merkle_compress_hashlist(TL, <<Bin/bitstring, 1:1, H1:SigSizeL>>,
                                     SigSizeI, SigSizeL);
        false ->
            merkle_compress_hashlist(TL, <<Bin/bitstring, 0:1, H1:SigSizeI>>,
                                     SigSizeI, SigSizeL)
    end.

%% @doc Transforms the compact binary representation of merkle hash lists from
%%      merkle_compress_hashlist/2 back into the original form.
-spec merkle_decompress_hashlist(bitstring(), Hashes,
                                 SigSizeI::signature_size(),
                                 SigSizeL::signature_size()) -> Hashes
    when is_subtype(Hashes, [merkle_cmp_request()]).
merkle_decompress_hashlist(<<>>, HashListR, _SigSizeI, _SigSizeL) ->
    lists:reverse(HashListR);
merkle_decompress_hashlist(Bin, HashListR, SigSizeI, SigSizeL) ->
    <<IsLeaf0:1, T0/bitstring>> = Bin,
    IsLeaf = if IsLeaf0 =:= 1 ->
                    <<Hash:SigSizeL/integer-unit:1, T1/bitstring>> = T0,
                    true;
                true ->
                    <<Hash:SigSizeI/integer-unit:1, T1/bitstring>> = T0,
                    false
    end,
    merkle_decompress_hashlist(T1, [{Hash, IsLeaf} | HashListR], SigSizeI, SigSizeL).

%% @doc Compares the given Hashes from the other node with my merkle_tree nodes
%%      (executed on non-initiator).
%%      Returns the comparison results and the rest nodes to check in a next
%%      step.
-spec check_node(Hashes::[merkle_cmp_request()],
                 merkle_tree:merkle_tree() | NodeList, SigSizeI::signature_size(),
                 SigSizeL::signature_size(), MerkleSyncIn::[merkle_sync()])
        -> {Flags::bitstring(), RestTree::NodeList,
            MerkleSyncOut::[merkle_sync()], MaxLeafCount::non_neg_integer(),
            MaxItemsCount::non_neg_integer()}
    when is_subtype(NodeList, [merkle_tree:mt_node()]).
check_node(Hashes, Tree, SigSizeI, SigSizeL, MerkleSyncIn) ->
    TreeNodes = case merkle_tree:is_merkle_tree(Tree) of
                    false -> Tree;
                    true -> [merkle_tree:get_root(Tree)]
                end,
    p_check_node(Hashes, TreeNodes, SigSizeI, SigSizeL, <<>>, [], [], MerkleSyncIn, 0, 0).

%% @doc Helper for check_node/5.
-spec p_check_node(Hashes::[merkle_cmp_request()], MyNodes::NodeList,
                   SigSizeI::signature_size(), SigSizeL::signature_size(), FlagsAcc::bitstring(),
                   RestTreeAcc::[NodeList], MerkleSyncAcc::[merkle_sync()],
                   MerkleSyncIn::[merkle_sync()], AccMLC::Count, AccMIC::Count)
        -> {FlagsOUT::bitstring(), RestTreeOut::NodeList,
            MerkleSyncOUT::[merkle_sync()], MaxLeafCount::Count, MaxItemsCount::Count}
    when
      is_subtype(NodeList, [merkle_tree:mt_node()]),
      is_subtype(Count, non_neg_integer()).
p_check_node([], [], _SigSizeI, _SigSizeL, FlagsAcc, AccN,
             MerkleSyncAcc, MerkleSyncIn, AccMLC, AccMIC) ->
    {FlagsAcc, lists:append(lists:reverse(AccN)),
     lists:reverse(MerkleSyncAcc, MerkleSyncIn), AccMLC, AccMIC};
p_check_node([{Hash, IsLeafHash} | TK], [Node | TN], SigSizeI, SigSizeL, FlagsAcc,
             AccN, MerkleSynAcc, MerkleSyncIN, AccMLC, AccMIC) ->
    IsLeafNode = merkle_tree:is_leaf(Node),
    NodeHash0 = merkle_tree:get_hash(Node),
    if IsLeafHash ->
           <<NodeHash:SigSizeL/integer-unit:1>> = <<NodeHash0:SigSizeL>>,
           ok;
       true ->
           <<NodeHash:SigSizeI/integer-unit:1>> = <<NodeHash0:SigSizeI>>,
           ok
    end,
    if Hash =:= NodeHash andalso IsLeafHash =:= IsLeafNode ->
           p_check_node(TK, TN, SigSizeI, SigSizeL, <<FlagsAcc/bitstring, ?recon_ok:3>>,
                        AccN, MerkleSynAcc,
                        MerkleSyncIN, AccMLC, AccMIC);
       (not IsLeafNode) andalso (not IsLeafHash) ->
           NewAccMLC = erlang:max(AccMLC, merkle_tree:get_leaf_count(Node)),
           NewAccMIC = erlang:max(AccMIC, merkle_tree:get_item_count(Node)),
           Childs = merkle_tree:get_childs(Node),
           p_check_node(TK, TN, SigSizeI, SigSizeL, <<FlagsAcc/bitstring, ?recon_fail_cont_inner:3>>,
                        [Childs | AccN], MerkleSynAcc,
                        MerkleSyncIN, NewAccMLC, NewAccMIC);
       (not IsLeafNode) andalso IsLeafHash ->
           {LeafNodes, FoundHash} =
               merkle_get_sync_leaves([Node], Hash, SigSizeL, [], false),
           Flag = ?IIF(FoundHash, ?recon_fail_stop_inner_found, ?recon_fail_stop_inner_notfound),
           p_check_node(TK, TN, SigSizeI, SigSizeL, <<FlagsAcc/bitstring, Flag:3>>,
                        AccN, [{inner, leaf, SigSizeL, LeafNodes, FoundHash} | MerkleSynAcc],
                        MerkleSyncIN, AccMLC, AccMIC);
       IsLeafNode andalso IsLeafHash ->
           p_check_node(TK, TN, SigSizeI, SigSizeL, <<FlagsAcc/bitstring, ?recon_fail_stop_leaf:3>>,
                        AccN, [{leaf, leaf, SigSizeL, Node} | MerkleSynAcc],
                        MerkleSyncIN, AccMLC, AccMIC);
       IsLeafNode andalso (not IsLeafHash) ->
           p_check_node(TK, TN, SigSizeI, SigSizeL, <<FlagsAcc/bitstring, ?recon_fail_stop_leaf:3>>,
                        AccN, [{leaf, inner, SigSizeI, Node, false} | MerkleSynAcc],
                        MerkleSyncIN, AccMLC, AccMIC)
    end.

%% @doc Processes compare results from check_node/3 on the initiator.
-spec process_tree_cmp_result(
        bitstring(), merkle_tree:merkle_tree() | NodeList,
        SigSizeI::signature_size(), SigSizeL::signature_size(),
        MerkleSyncIn::[merkle_sync()], Stats)
        -> {RestTree::NodeList, MerkleSyncOut::[merkle_sync()], NewStats::Stats,
            MaxLeafCount::non_neg_integer(), MaxItemsCount::non_neg_integer()}
    when
      is_subtype(NodeList,     [merkle_tree:mt_node()]),
      is_subtype(Stats,        rr_recon_stats:stats()).
process_tree_cmp_result(CmpResult, Tree, SigSizeI, SigSizeL, MerkleSyncIn, Stats) ->
    TreeNodes = case merkle_tree:is_merkle_tree(Tree) of
                    false -> Tree;
                    true -> [merkle_tree:get_root(Tree)]
                end,
    p_process_tree_cmp_result(CmpResult, TreeNodes, SigSizeI, SigSizeL,
                              MerkleSyncIn, Stats, [], [], 0, 0, 0).

%% @doc Helper for process_tree_cmp_result/6.
-spec p_process_tree_cmp_result(
        bitstring(), RestTree::NodeList, SigSizeI::signature_size(),
        SigSizeL::signature_size(), MerkleSyncIn::[merkle_sync()], Stats,
        RestTreeAcc::NodeList, MerkleSyncAcc::[merkle_sync()], AccMLC::Count,
        AccMIC::Count, AccCmp::Count)
        -> {RestTreeOut::NodeList, MerkleSyncOut::[merkle_sync()],
            NewStats::Stats, MaxLeafCount::Count, MaxItemsCount::Count}
    when
      is_subtype(NodeList, [merkle_tree:mt_node()]),
      is_subtype(Stats,    rr_recon_stats:stats()),
      is_subtype(Count,    non_neg_integer()).
p_process_tree_cmp_result(<<>>, [], _SigSizeI, _SigSizeL, MerkleSyncIn, Stats,
                          RestTreeAcc, MerkleSyncAcc, AccMLC, AccMIC, AccCmp) ->
    NStats = rr_recon_stats:inc([{tree_nodesCompared, AccCmp}], Stats),
     {lists:reverse(RestTreeAcc), lists:reverse(MerkleSyncAcc, MerkleSyncIn),
      NStats, AccMLC, AccMIC};
p_process_tree_cmp_result(<<?recon_ok:3, TR/bitstring>>, [Node | TN],
                          SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                          MerkleSyncAcc, AccMLC, AccMIC, AccCmp) ->
    Skipped = merkle_tree:size(Node) - 1,
    NStats = rr_recon_stats:inc([{tree_compareSkipped, Skipped}], Stats),
    p_process_tree_cmp_result(TR, TN, SigSizeI, SigSizeL, MerkleSyncIn, NStats,
                              RestTreeAcc, MerkleSyncAcc, AccMLC, AccMIC, AccCmp + 1);
p_process_tree_cmp_result(<<?recon_fail_stop_leaf:3, TR/bitstring>>, [Node | TN],
                          SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                          MerkleSyncAcc, AccMLC, AccMIC, AccCmp) ->
    Sync = case merkle_tree:is_leaf(Node) of
               true  -> {leaf, leaf, SigSizeL, Node};
               false -> LeafNodes = merkle_tree:get_leaves([Node]),
                        {inner, leaf, SigSizeI, LeafNodes, false}
           end,
    p_process_tree_cmp_result(TR, TN, SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                              [Sync | MerkleSyncAcc], AccMLC, AccMIC, AccCmp + 1);
p_process_tree_cmp_result(<<?recon_fail_stop_inner_found:3, TR/bitstring>>, [Node | TN],
                          SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                          MerkleSyncAcc, AccMLC, AccMIC, AccCmp) ->
    ?DBG_ASSERT(merkle_tree:is_leaf(Node)),
    p_process_tree_cmp_result(TR, TN, SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                              [{leaf, inner, SigSizeL, Node, _FoundSkipHash = true} | MerkleSyncAcc],
                              AccMLC, AccMIC, AccCmp + 1);
p_process_tree_cmp_result(<<?recon_fail_stop_inner_notfound:3, TR/bitstring>>, [Node | TN],
                          SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                          MerkleSyncAcc, AccMLC, AccMIC, AccCmp) ->
    ?DBG_ASSERT(merkle_tree:is_leaf(Node)),
    p_process_tree_cmp_result(TR, TN, SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                              [{leaf, inner, SigSizeL, Node, _FoundSkipHash = false} | MerkleSyncAcc],
                              AccMLC, AccMIC, AccCmp + 1);
p_process_tree_cmp_result(<<?recon_fail_cont_inner:3, TR/bitstring>>, [Node | TN],
                          SigSizeI, SigSizeL, MerkleSyncIn, Stats, RestTreeAcc,
                          MerkleSyncAcc, AccMLC, AccMIC, AccCmp) ->
    ?DBG_ASSERT(not merkle_tree:is_leaf(Node)),
    NewAccMLC = erlang:max(AccMLC, merkle_tree:get_leaf_count(Node)),
    NewAccMIC = erlang:max(AccMIC, merkle_tree:get_item_count(Node)),
    Childs = merkle_tree:get_childs(Node),
    p_process_tree_cmp_result(TR, TN, SigSizeI, SigSizeL, MerkleSyncIn, Stats,
                              lists:reverse(Childs, RestTreeAcc), MerkleSyncAcc,
                              NewAccMLC, NewAccMIC, AccCmp + 1).

%% @doc Gets all leaves in the given merkle node list whose hash =/= skipHash.
-spec merkle_get_sync_leaves(Nodes::NodeL, Skip::Hash, SigSize::signature_size(),
                             LeafAcc::NodeL, FoundSkipHash::boolean())
        -> {ToSync::NodeL, FoundSkipHash::boolean()}
    when
      is_subtype(Hash,  merkle_tree:mt_node_key()),
      is_subtype(NodeL, [merkle_tree:mt_node()]).
merkle_get_sync_leaves([], _Skip, _SigSize, ToSyncAcc, FoundSkipHash) ->
    {ToSyncAcc, FoundSkipHash};
merkle_get_sync_leaves([Node | Rest], Skip, SigSize, ToSyncAcc, FoundSkipHash) ->
    case merkle_tree:is_leaf(Node) of
        true  ->
            NodeHash0 = merkle_tree:get_hash(Node),
            <<NodeHash:SigSize/integer-unit:1>> = <<NodeHash0:SigSize>>,
            if NodeHash =:= Skip ->
                   merkle_get_sync_leaves(Rest, Skip, SigSize, ToSyncAcc, true);
               true ->
                   merkle_get_sync_leaves(Rest, Skip, SigSize, [Node | ToSyncAcc],
                                          FoundSkipHash)
            end;
        false ->
            merkle_get_sync_leaves(
              lists:append(merkle_tree:get_childs(Node), Rest), Skip, SigSize,
              ToSyncAcc, FoundSkipHash)
    end.

%% @doc Gets the number of bits needed to encode all possible bucket sizes with
%%      the given merkle params.
-spec merkle_max_bucket_size_bits(Params::#merkle_params{}) -> pos_integer().
merkle_max_bucket_size_bits(Params) ->
    BucketSizeBits0 = erlang:max(1, util:ceil(util:log2(Params#merkle_params.bucket_size + 1))),
    ?IIF(config:read(rr_align_to_bytes),
         bloom:resize(BucketSizeBits0, 8),
         BucketSizeBits0).

%% @doc Helper for adding a leaf node's KV-List to a compressed binary
%%      during merkle sync.
-spec merkle_resolve_add_leaf_hash(
        LeafNode::merkle_tree:mt_node(), SigSize0::signature_size(),
        BucketSizeBits::pos_integer(), HashesReplyIn::bitstring())
        -> HashesReplyOut::bitstring().
merkle_resolve_add_leaf_hash(LeafNode, SigSize0, BucketSizeBits, HashesReply) ->
    % TODO: make extra version binary for better compression (?)
    % TODO: use formulae of compress_kv_list_p1e/4 for the version size (?) - have to have the same info on the other node!
    Bucket = merkle_tree:get_bucket(LeafNode),
    BucketSize = merkle_tree:get_item_count(LeafNode),
    ?DBG_ASSERT(BucketSize < util:pow(2, BucketSizeBits)),
    ?DBG_ASSERT(BucketSize =:= length(Bucket)),
    HashesReply1 = <<HashesReply/bitstring, BucketSize:BucketSizeBits>>,

    % note: we can reach the best compression if values and versions align to
    %       byte-boundaries
    {SigSize, VSize} = align_bitsize(SigSize0, get_min_version_bits()),

    compress_kv_list(Bucket, HashesReply1, SigSize, VSize).

%% @doc Helper for retrieving a leaf node's KV-List from the compressed binary
%%      returned by merkle_resolve_add_leaf_hash/4 during merkle sync.
-spec merkle_resolve_retrieve_leaf_hashes(
        Hashes::bitstring(), SigSize0::signature_size(),
        BucketSizeBits::pos_integer())
        -> {NHashes::bitstring(), OtherBucketTree::kv_tree(),
            SigSize::signature_size(), VSize::signature_size()}.
merkle_resolve_retrieve_leaf_hashes(Hashes, SigSize0, BucketSizeBits) ->
    <<BSize:BucketSizeBits/integer-unit:1, HashesT/bitstring>> = Hashes,
    {SigSize, VSize} = align_bitsize(SigSize0, get_min_version_bits()),
    OBucketBinSize = BSize * (SigSize + VSize),
    <<OBucketBin:OBucketBinSize/bitstring, NHashes/bitstring>> = HashesT,
    OBucketTree = decompress_kv_list(OBucketBin, gb_trees:empty(), SigSize, VSize),
    {NHashes, OBucketTree, SigSize, VSize}.

%% @doc Gets a compact binary merkle hash list from all leaf-inner node
%%      comparisons so that the other node can filter its leaf nodes and
%%      resolves all other leaf nodes (called on non-initiator).
-spec merkle_resolve_leaves_noninit(
        Sync::[merkle_sync()], DestRRPid::comm:mypid(), Stats,
        OwnerL::comm:erl_local_pid(), Params::#merkle_params{})
        -> {Hashes::bitstring(), NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_noninit(Sync, DestRRPid, Stats, OwnerL, Params) ->
    BucketSizeBits = merkle_max_bucket_size_bits(Params),
    merkle_resolve_leaves_noninit(Sync, <<>>, DestRRPid, Stats, OwnerL,
                                  BucketSizeBits, [], 0).

%% @doc Helper for merkle_resolve_leaves_noninit/5.
-spec merkle_resolve_leaves_noninit(
        Sync::[merkle_sync()], HashesAcc::bitstring(), DestRRPid::comm:mypid(),
        Stats, OwnerL::comm:erl_local_pid(), BucketSizeBits::pos_integer(),
        ToSend::[?RT:key()], LeafNodesAcc::non_neg_integer())
        -> {Hashes::bitstring(), NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_noninit([{inner, leaf, _SigSize, LeafNodes, true} | TL], HashesReply,
                              DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, LeafNAcc)
  when is_list(LeafNodes) ->
    {ToSend1, NLeafNAcc} =
        lists:foldl(fun(N, {AccToSend, LeafNAccX}) ->
                            ?DBG_ASSERT(merkle_tree:is_leaf(N)),
                            {lists:append([Key || {Key, _Version} <- merkle_tree:get_bucket(N)],
                                          AccToSend),
                             LeafNAccX + 1}
                    end, {ToSend, LeafNAcc}, LeafNodes),
    merkle_resolve_leaves_noninit(TL, HashesReply, DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend1,
                                  NLeafNAcc);
merkle_resolve_leaves_noninit([{inner, leaf, _SigSize, _LeafNodes, false} | TL], HashesReply,
                              DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, LeafNAcc) ->
    % get KV-List from initiator and resolve in second step
    merkle_resolve_leaves_noninit(TL, HashesReply, DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend,
                                  LeafNAcc);
merkle_resolve_leaves_noninit([{leaf, leaf, SigSize0, LeafNode} | TL], HashesReply,
                              DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, LeafNAcc) ->
    ?DBG_ASSERT(merkle_tree:is_leaf(LeafNode)),
    HashesReply1 = merkle_resolve_add_leaf_hash(LeafNode, SigSize0, BucketSizeBits, HashesReply),
    merkle_resolve_leaves_noninit(TL, HashesReply1, DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend,
                                  LeafNAcc + 1);
merkle_resolve_leaves_noninit([{leaf, inner, SigSize0, LeafNode, false = _FoundSkipHash} | TL], HashesReply,
                              DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, LeafNAcc) ->
    ?DBG_ASSERT(merkle_tree:is_leaf(LeafNode)),
    HashesReply1 = merkle_resolve_add_leaf_hash(LeafNode, SigSize0, BucketSizeBits, HashesReply),
    merkle_resolve_leaves_noninit(TL, HashesReply1, DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend,
                                  LeafNAcc + 1);
merkle_resolve_leaves_noninit([], HashesReply, DestRRPid, Stats, OwnerL, _BucketSizeBits, ToSend, LeafNAcc) ->
    SID = rr_recon_stats:get(session_id, Stats),
    % resolve items from inner-leaf comparisons with leaf-hash matches as key_upd:
    KeyUpdResReqs =
        if ToSend =/= [] ->
               ?DBG_ASSERT2(length(ToSend) =:= length(lists:usort(ToSend)),
                            {non_unique_send_list, ToSend}),
               send_local(OwnerL, {request_resolve, SID,
                                   {key_upd_send, DestRRPid, ToSend, []},
                                   [{from_my_node, 1},
                                    {feedback_request, comm:make_global(OwnerL)}]}),
               1;
           true ->
               0
        end,

    NStats = rr_recon_stats:inc([{tree_leavesSynced, LeafNAcc},
                                 {resolve_started, KeyUpdResReqs}],
                                Stats),
    {HashesReply, NStats}.

%% @doc Helper for resolving an inner-leaf mismatch.
merkle_resolve_compare_inner_leaf(SigSize0, BucketSizeBits, LeafNodes, Hashes, ToSend, ToReq,
                                  ToResolve, ResolveNonEmptyAcc) ->
    {NHashes, OBucketTree, SigSize, VSize} =
        merkle_resolve_retrieve_leaf_hashes(Hashes, SigSize0, BucketSizeBits),
    {MyBuckets, NLeafNAcc} =
        lists:foldl(fun(N, {AccBuckets, LeafNAccX}) ->
                            ?DBG_ASSERT(merkle_tree:is_leaf(N)),
                            {lists:append(merkle_tree:get_bucket(N), AccBuckets),
                             LeafNAccX + 1}
                    end, {[], 0}, LeafNodes),
    {ToSend1, ToReq1, OBucketTree1} =
        get_full_diff(MyBuckets, OBucketTree, ToSend, ToReq, SigSize, VSize),
    ToResolve1 = [gb_trees:keys(OBucketTree1) | ToResolve],
    ResolveNonEmptyAcc1 =
        ?IIF(gb_trees:size(OBucketTree1) =/= 0, true, ResolveNonEmptyAcc),
    {NHashes, ToSend1, ToReq1, ToResolve1, ResolveNonEmptyAcc1, NLeafNAcc}.

%% @doc Helper for the final resolve step during merkle sync.
-spec merkle_resolve(
        DestRRPid::comm:mypid(), Stats, OwnerL::comm:erl_local_pid(),
        ToSend::[?RT:key()], ToReq::[?RT:key()], ToResolve::[[bitstring()]],
        ResolveNonEmpty::boolean(), LeafNodesAcc::non_neg_integer(),
        Initiator::boolean())
        -> {HashesReq::[[bitstring()]], NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve(DestRRPid, Stats, OwnerL, ToSend, ToReq, ToResolve,
               ResolveNonEmpty, LeafNAcc, Initiator) ->
    SID = rr_recon_stats:get(session_id, Stats),
    % resolve the leaf-leaf comparison's items as key_upd:
    % note: the resolve request is counted at the initiator and
    %       thus from_my_node must be set accordingly on this node!
    KeyUpdResReqs =
        if ToSend =/= [] orelse ToReq =/= [] ->
               ?DBG_ASSERT2(length(ToSend) =:= length(lists:usort(ToSend)),
                            {non_unique_send_list, ToSend}),
               ?DBG_ASSERT2(length(ToReq) =:= length(lists:usort(ToReq)),
                            {non_unique_req_list, ToReq}),
               send_local(OwnerL, {request_resolve, SID,
                                   {key_upd_send, DestRRPid, ToSend, ToReq},
                                   [{from_my_node, ?IIF(Initiator, 1, 0)},
                                    {feedback_request, comm:make_global(OwnerL)}]}),
               1;
           true ->
               0
        end,

    % let the other node's rr_recon process identify the remaining keys;
    % it will use key_upd_send (if non-empty) and we must thus increase
    % the number of resolve processes here!
    if ResolveNonEmpty -> 
           ToResolve1 = lists:reverse(ToResolve),
           MerkleResReqs = 1;
       true ->
           ToResolve1 = [],
           MerkleResReqs = 0
    end,
    
    ?TRACE("resolve_req Merkle Session=~p ; key_upd_send=~p ; merkle_resolve=~p",
           [SID, KeyUpdResReqs, MerkleResReqs]),
    NStats = rr_recon_stats:inc([{tree_leavesSynced, LeafNAcc},
                                 {resolve_started, KeyUpdResReqs + MerkleResReqs}],
                                Stats),
    {ToResolve1, NStats}.

%% @doc Filters leaf nodes of inner(I)-leaf(NI) or leaf-leaf comparisons
%%      by the compact binary representation of a merkle hash list from
%%      merkle_sync_compress_hashes/2 and resolves them (called on initiator).
%%      Leaf(I)-inner(NI) mismatches will result in a (compressed) KV-List being
%%      send back to the non-initiator for further syncronisation.
-spec merkle_resolve_leaves_init(
        Sync::[merkle_sync()], Hashes::bitstring(), DestRRPid::comm:mypid(),
        Stats, OwnerL::comm:erl_local_pid(), Params::#merkle_params{})
        -> {HashesReply::bitstring(), HashesReq::[[bitstring()]], NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_init(Sync, Hashes, DestRRPid, SID, OwnerL, Params) ->
    BucketSizeBits = merkle_max_bucket_size_bits(Params),
    merkle_resolve_leaves_init(Sync, Hashes, DestRRPid, SID, OwnerL, BucketSizeBits,
                               [], [], [], false, 0, <<>>).

%% @doc Helper for merkle_resolve_leaves_init/6.
-spec merkle_resolve_leaves_init(
        Sync::[merkle_sync()], Hashes::bitstring(), DestRRPid::comm:mypid(),
        Stats, OwnerL::comm:erl_local_pid(), BucketSizeBits::pos_integer(),
        ToSend::[?RT:key()], ToReq::[?RT:key()], ToResolve::[[bitstring()]],
        ResolveNonEmpty::boolean(), LeafNodesAcc::non_neg_integer(),
        HashesReplyAcc::bitstring())
        -> {HashesReply::bitstring(), HashesReq::[[bitstring()]], NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_init([{leaf, leaf, SigSize, LeafNode} | TL], Hashes,
                           DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                           ResolveNonEmpty, LeafNAcc, HashesReply) ->
    % same handling as below inner-leaf -> simply convert:
    merkle_resolve_leaves_init(
      [{inner, leaf, SigSize, [LeafNode], false} | TL], Hashes,
      DestRRPid, Stats, OwnerL, BucketSizeBits,
      ToSend, ToReq, ToResolve, ResolveNonEmpty, LeafNAcc, HashesReply);
merkle_resolve_leaves_init([{inner, leaf, SigSize0, LeafNodes, _FoundHash} | TL],
                           Hashes,
                           DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                           ResolveNonEmpty, LeafNAcc, HashesReply) ->
    {NHashes, ToSend1, ToReq1, ToResolve1, ResolveNonEmpty1, LeafCount} =
        merkle_resolve_compare_inner_leaf(SigSize0, BucketSizeBits, LeafNodes, Hashes, ToSend,
                                          ToReq, ToResolve, ResolveNonEmpty),
    merkle_resolve_leaves_init(TL, NHashes, DestRRPid, Stats, OwnerL, BucketSizeBits,
                               ToSend1, ToReq1, ToResolve1, ResolveNonEmpty1,
                               LeafNAcc + LeafCount, HashesReply);
merkle_resolve_leaves_init([{leaf, inner, _SigSize0, _LeafNode, true = _FoundSkipHash} | TL],
                           Hashes,
                           DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                           ResolveNonEmpty, LeafNAcc, HashesReply) ->
    % already resolved by non-initiator
    merkle_resolve_leaves_init(TL, Hashes, DestRRPid, Stats, OwnerL, BucketSizeBits,
                               ToSend, ToReq, ToResolve, ResolveNonEmpty,
                               LeafNAcc + 1, HashesReply);
merkle_resolve_leaves_init([{leaf, inner, SigSize0, LeafNode, false = _FoundSkipHash} | TL],
                           Hashes,
                           DestRRPid, Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                           ResolveNonEmpty, LeafNAcc, HashesReply) ->
    ?DBG_ASSERT(merkle_tree:is_leaf(LeafNode)),
    HashesReply1 = merkle_resolve_add_leaf_hash(LeafNode, SigSize0, BucketSizeBits, HashesReply),
    merkle_resolve_leaves_init(TL, Hashes, DestRRPid, Stats, OwnerL, BucketSizeBits,
                               ToSend, ToReq, ToResolve, ResolveNonEmpty,
                               LeafNAcc + 1, HashesReply1);
merkle_resolve_leaves_init([], <<>>, DestRRPid, Stats, OwnerL, _BucketSizeBits, ToSend, ToReq,
                           ToResolve, ResolveNonEmpty, LeafNAcc, HashesReply) ->
    {ToResolve1, NStats} =
        merkle_resolve(DestRRPid, Stats, OwnerL, ToSend, ToReq, ToResolve,
                       ResolveNonEmpty, LeafNAcc, true),
    {HashesReply, ToResolve1, NStats}.

%% @doc For each leaf-leaf or leaf(NI)-inner(I) comparison, gets the
%%      requested keys and resolves them (if non-empty) with a key_upd_send
%%      (called on non-initiator).
%%      Inner(NI)-leaf(I) mismatches will work with the compressed KV-List from
%%      the initiator.
-spec merkle_resolve_req_keys_noninit(
        Sync::[merkle_sync()], Hashes::bitstring(), BinKeyList::[[bitstring()]],
        DestRRPid::comm:mypid(), Stats, OwnerL::comm:erl_local_pid(), BucketSizeBits::pos_integer(),
        ToSend::[?RT:key()], ToReq::[?RT:key()], ToResolve::[[bitstring()]],
        ResolveNonEmpty::boolean(), BinKeyListNonEmpty::boolean())
        -> {HashesReq::[[bitstring()]], NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_req_keys_noninit([{leaf, leaf, SigSize0, LeafNode} | TL], Hashes,
                                [_ReqKeys | _] = BinKeyList, DestRRPid,
                                Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                                ResolveNonEmpty, true) ->
    % same handling as below leaf-inner -> simply convert:
    merkle_resolve_req_keys_noninit([{leaf, inner, SigSize0, LeafNode, false} | TL], Hashes,
                                    BinKeyList, DestRRPid,
                                    Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                                    ResolveNonEmpty, true);
merkle_resolve_req_keys_noninit([{leaf, inner, SigSize0, LeafNode, false = _FoundSkipHash} | TL], Hashes,
                                [ReqKeys | BinKeyList], DestRRPid,
                                Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                                ResolveNonEmpty, true) ->
    ToSend1 =
        case ReqKeys of
            [] -> ToSend;
            [_|_] ->
                ReqSet = gb_sets:from_list(ReqKeys),
                {SigSize, _VSize} = align_bitsize(SigSize0, get_min_version_bits()),
                [Key || {Key, _Version} <- merkle_tree:get_bucket(LeafNode),
                        gb_sets:is_member(compress_key(Key, SigSize),
                                          ReqSet)] ++ ToSend
        end,
    merkle_resolve_req_keys_noninit(TL, Hashes, BinKeyList, DestRRPid, Stats,
                                    OwnerL, BucketSizeBits, ToSend1, ToReq, ToResolve,
                                    ResolveNonEmpty, true);
merkle_resolve_req_keys_noninit([{inner, leaf, SigSize0, LeafNodes, false} | TL],
                                Hashes, BinKeyList, DestRRPid,
                                Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                                ResolveNonEmpty, BKLNonEmpty) ->
    {NHashes, ToSend1, ToReq1, ToResolve1, ResolveNonEmpty1, LeafCount} =
        merkle_resolve_compare_inner_leaf(SigSize0, BucketSizeBits, LeafNodes, Hashes, ToSend,
                                          ToReq, ToResolve, ResolveNonEmpty),
    NStats = rr_recon_stats:inc([{tree_leavesSynced, LeafCount}], Stats),
    merkle_resolve_req_keys_noninit(TL, NHashes, BinKeyList, DestRRPid, NStats,
                                    OwnerL, BucketSizeBits, ToSend1, ToReq1, ToResolve1,
                                    ResolveNonEmpty1, BKLNonEmpty);
merkle_resolve_req_keys_noninit([_ | TL], Hashes, BinKeyList, DestRRPid,
                                Stats, OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                                ResolveNonEmpty, BKLNonEmpty) ->
    % already resolved by initiator / in first step or optimised empty BinKeyList
    merkle_resolve_req_keys_noninit(TL, Hashes, BinKeyList, DestRRPid, Stats,
                                    OwnerL, BucketSizeBits, ToSend, ToReq, ToResolve,
                                    ResolveNonEmpty, BKLNonEmpty);
merkle_resolve_req_keys_noninit([], <<>>, [], DestRRPid, Stats, OwnerL, _BucketSizeBits,
                                ToSend, ToReq, ToResolve, ResolveNonEmpty,
                                _BKLNonEmpty) ->
    merkle_resolve(DestRRPid, Stats, OwnerL, ToSend, ToReq, ToResolve,
                   ResolveNonEmpty, 0, false).


%% @doc For each leaf(I)-inner(NI) comparison, gets the requested keys and
%%      resolves them (if non-empty) with a key_upd_send (called on initiator).
-spec merkle_resolve_req_keys_init(
        Sync::[merkle_sync()], BinKeyList::[[bitstring()]], DestRRPid::comm:mypid(),
        Stats, OwnerL::comm:erl_local_pid(), SendKeysAcc::[?RT:key()],
        ResolveNonEmpty::boolean())
        -> NewStats::Stats
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_req_keys_init([{leaf, inner, SigSize0, LeafNode, false = _FoundSkipHash} | TL],
                             [ReqKeys | BinKeyList],
                             DestRRPid, Stats, OwnerL, SendKeys, true) ->
    % TODO: same as above -> extract function?
    SendKeys1 =
        case ReqKeys of
            [] -> SendKeys;
            [_|_] ->
                ReqSet = gb_sets:from_list(ReqKeys),
                {SigSize, _VSize} = align_bitsize(SigSize0, get_min_version_bits()),
                [Key || {Key, _Version} <- merkle_tree:get_bucket(LeafNode),
                        gb_sets:is_member(compress_key(Key, SigSize),
                                          ReqSet)] ++ SendKeys
        end,
    merkle_resolve_req_keys_init(TL, BinKeyList, DestRRPid, Stats, OwnerL,
                                 SendKeys1, true);
merkle_resolve_req_keys_init([_ | TL], BinKeyList, DestRRPid, Stats, OwnerL,
                             SendKeys, ResolveNonEmpty) ->
    % already resolved by non-initiator / in first step or optimised empty BinKeyList
    merkle_resolve_req_keys_init(TL, BinKeyList, DestRRPid, Stats, OwnerL,
                                 SendKeys, ResolveNonEmpty);
merkle_resolve_req_keys_init([], [], DestRRPid, Stats, OwnerL, [_|_] = SendKeys,
                             _ResolveNonEmpty) ->
    SID = rr_recon_stats:get(session_id, Stats),
    % note: the resolve request is counted at the initiator and
    %       thus from_my_node must be set accordingly on this node!
    send_local(OwnerL, {request_resolve, SID,
                        {key_upd_send, DestRRPid, SendKeys, []},
                        [{feedback_request, comm:make_global(OwnerL)},
                         {from_my_node, 1}]}),
    % we will get one reply from a subsequent ?key_upd resolve
    rr_recon_stats:inc([{resolve_started, 1}], Stats);
merkle_resolve_req_keys_init([], [], _DestRRPid, Stats, _OwnerL, [],
                             _ResolveNonEmpty) ->
    Stats.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% art recon
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a single resolve request for all given leaf nodes by joining
%%      their intervals.
%%      Returns the number of resolve requests (requests with feedback count 2).
-spec resolve_leaves([merkle_tree:mt_node()], Dest::comm:mypid(),
                     rrepair:session_id(), OwnerLocal::comm:erl_local_pid())
        -> {ResolveCalled::0..1, FBCount::0..1}.
resolve_leaves([_|_] = Nodes, Dest, SID, OwnerL) ->
    resolve_leaves(Nodes, Dest, SID, OwnerL, intervals:empty(), 0);
resolve_leaves([], _Dest, _SID, _OwnerL) ->
    {0, 0}.

%% @doc Helper for resolve_leaves/4.
-spec resolve_leaves([merkle_tree:mt_node()], Dest::comm:mypid(),
                     rrepair:session_id(), OwnerLocal::comm:erl_local_pid(),
                     Interval::intervals:interval(), Items::non_neg_integer())
        -> {ResolveCalled::0..1, FBCount::0..1}.
resolve_leaves([Node | Rest], Dest, SID, OwnerL, Interval, Items) ->
    ?DBG_ASSERT(merkle_tree:is_leaf(Node)),
    LeafInterval = merkle_tree:get_interval(Node),
    IntervalNew = intervals:union(Interval, LeafInterval),
    ItemsNew = Items + merkle_tree:get_item_count(Node),
    resolve_leaves(Rest, Dest, SID, OwnerL, IntervalNew, ItemsNew);
resolve_leaves([], Dest, SID, OwnerL, Interval, Items) ->
    Options = [{feedback_request, comm:make_global(OwnerL)}],
    case intervals:is_empty(Interval) of
        true -> 0;
        _ ->
            if Items > 0 ->
                   send_local(OwnerL, {request_resolve, SID,
                                       {interval_upd_send, Interval, Dest},
                                       [{from_my_node, 1} | Options]}),
                   {1, 0};
               true ->
                   % we know that we don't have data in this range, so we must
                   % regenerate it from the other node
                   % -> send him this request directly!
                   send(Dest, {request_resolve, SID, {?interval_upd, Interval, []},
                               [{from_my_node, 0} | Options]}),
                   {1, 1}
            end
    end.

-spec art_recon(MyTree::merkle_tree:merkle_tree(), art:art(), state())
        -> rr_recon_stats:stats().
art_recon(Tree, Art, #rr_recon_state{ dest_rr_pid = DestPid,
                                      ownerPid = OwnerL,
                                      stats = Stats }) ->
    SID = rr_recon_stats:get(session_id, Stats),
    NStats =
        case merkle_tree:get_interval(Tree) =:= art:get_interval(Art) of
            true ->
                {ASyncLeafs, NComp, NSkip, NLSync} =
                    art_get_sync_leaves([merkle_tree:get_root(Tree)], Art,
                                        [], 0, 0, 0),
                {ResolveCalled, FBCount} =
                    resolve_leaves(ASyncLeafs, DestPid, SID, OwnerL),
                rr_recon_stats:inc([{tree_nodesCompared, NComp},
                                    {tree_compareSkipped, NSkip},
                                    {tree_leavesSynced, NLSync},
                                    {resolve_started, ResolveCalled},
                                    {await_rs_fb, FBCount}], Stats);
            false -> Stats
        end,
    rr_recon_stats:set([{tree_size, merkle_tree:size_detail(Tree)}], NStats).

%% @doc Gets all leaves in the merkle node list (recursively) which are not
%%      present in the art structure.
-spec art_get_sync_leaves(Nodes::NodeL, art:art(), ToSyncAcc::NodeL,
                          NCompAcc::non_neg_integer(), NSkipAcc::non_neg_integer(),
                          NLSyncAcc::non_neg_integer())
        -> {ToSync::NodeL, NComp::non_neg_integer(), NSkip::non_neg_integer(),
            NLSync::non_neg_integer()} when
    is_subtype(NodeL,  [merkle_tree:mt_node()]).
art_get_sync_leaves([], _Art, ToSyncAcc, NCompAcc, NSkipAcc, NLSyncAcc) ->
    {ToSyncAcc, NCompAcc, NSkipAcc, NLSyncAcc};
art_get_sync_leaves([Node | Rest], Art, ToSyncAcc, NCompAcc, NSkipAcc, NLSyncAcc) ->
    NComp = NCompAcc + 1,
    IsLeaf = merkle_tree:is_leaf(Node),
    case art:lookup(Node, Art) of
        true ->
            NSkip = NSkipAcc + ?IIF(IsLeaf, 0, merkle_tree:size(Node) - 1),
            art_get_sync_leaves(Rest, Art, ToSyncAcc, NComp, NSkip, NLSyncAcc);
        false ->
            if IsLeaf ->
                   art_get_sync_leaves(Rest, Art, [Node | ToSyncAcc],
                                       NComp, NSkipAcc, NLSyncAcc + 1);
               true ->
                   art_get_sync_leaves(
                     lists:append(merkle_tree:get_childs(Node), Rest), Art,
                     ToSyncAcc, NComp, NSkipAcc, NLSyncAcc)
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a compressed key-value list comparing every item in Items
%%      (at most ItemCount) with OtherItemCount other items and expecting at
%%      most min(ItemCount, OtherItemCount) version comparisons.
%%      Sets the bit sizes to have an error below P1E.
-spec compress_kv_list_p1e(Items::db_chunk_kv(), ItemCount::non_neg_integer(),
                           OtherItemCount::non_neg_integer(), P1E::float())
        -> {DBChunk::bitstring(), SigSize::signature_size(), VSize::signature_size()}.
compress_kv_list_p1e(DBItems, ItemCount, OtherItemCount, P1E) ->
    VCompareCount = erlang:min(ItemCount, OtherItemCount),
    % reduce P1E for the two parts here (key and version comparison)
    B = ?TRIVIAL_B, A = 1 - ?TRIVIAL_B,
    % cut off at 128 bit (rt_chord uses md5 - must be enough for all other RT implementations, too)
    SigSize0 = calc_signature_size_nm_pair(ItemCount, OtherItemCount, A * P1E, 128),
    % note: we have n one-to-one comparisons, assuming the probability of a
    %       failure in a single one-to-one comparison is p, the overall
    %       p1e = 1 - (1-p)^n  <=>  p = 1 - (1 - p1e)^(1/n)
    VP = 1 - math:pow(1 - B * P1E, 1 / erlang:max(1, VCompareCount)),
    VSize0 = erlang:max(get_min_version_bits(),
                        calc_signature_size_1_to_n(1, VP, 128)),
    % note: we can reach the best compression if values and versions align to
    %       byte-boundaries
    {SigSize, VSize} = align_bitsize(SigSize0, VSize0),
    DBChunkBin = compress_kv_list(DBItems, <<>>, SigSize, VSize),
    % debug compressed and uncompressed sizes:
    ?TRACE("~B vs. ~B items, SigSize: (~B -> ~B), VSize: (~B -> ~B), ChunkSize: ~p / ~p bits",
            [ItemCount, OtherItemCount, SigSize0, SigSize, VSize0, VSize, erlang:bit_size(DBChunkBin),
             erlang:bit_size(
                 erlang:term_to_binary(DBChunkBin,
                                       [{minor_version, 1}, {compressed, 2}]))]),
    {DBChunkBin, SigSize, VSize}.

%% @doc Aligns the two sizes so that their sum is a multiple of 8 in order to
%%      achieve a better compression in binaries.
-spec align_bitsize(SigSize, VSize) -> {SigSize, VSize}
    when is_subtype(SigSize, pos_integer()),
         is_subtype(VSize, pos_integer()).
align_bitsize(SigSize0, VSize0) ->
    case config:read(rr_align_to_bytes) of
        false -> {SigSize0, VSize0};
        _     ->
            FullKVSize0 = SigSize0 + VSize0,
            FullKVSize = bloom:resize(FullKVSize0, 8),
            VSize = VSize0 + ((FullKVSize - FullKVSize0) div 2),
            SigSize = FullKVSize - VSize,
            {SigSize, VSize}
    end.

-spec build_recon_struct(method(), OldSyncStruct::sync_struct() | {},
                         DestI::intervals:non_empty_interval(), db_chunk_kv(),
                         Params::parameters() | {}, BeginSync::boolean())
        -> sync_struct().
build_recon_struct(trivial, _OldSyncStruct = {}, I, DBItems, _Params, true) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
    ItemCount = length(DBItems),
    {DBChunkBin, SigSize, VSize} =
        compress_kv_list_p1e(DBItems, ItemCount, ItemCount, get_p1e()),
    #trivial_recon_struct{interval = I, reconPid = comm:this(),
                          db_chunk = DBChunkBin,
                          sig_size = SigSize, ver_size = VSize};
build_recon_struct(bloom, _OldSyncStruct = {}, I, DBItems, _Params, true) ->
    % note: for bloom, parameters don't need to match (only one bloom filter at
    %       the non-initiator is created!) - use our own parameters
    ?DBG_ASSERT(not intervals:is_empty(I)),
    MaxItems = length(DBItems),
    % FP is a single comparison's failure probability
    % * assume the other node executes MaxItems number of checks, too
    % * assume the worst case, e.g. the other node has only items not in BF and
    %   we need to account for the false positive probability
    % NOTE: reduce P1E for the two parts here (bloom and trivial RC)
    FP = 1 - math:pow(1 - (1 - ?BLOOM_B) * get_p1e(), 1 / erlang:max(MaxItems, 1)),
    BF0 = bloom:new_fpr(MaxItems, FP),
    BF = bloom:add_list(BF0, DBItems),
    #bloom_recon_struct{interval = I, reconPid = comm:this(), bloom = BF};
build_recon_struct(merkle_tree, _OldSyncStruct = {}, I, DBItems, Params, _BeginSync) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
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
                                 {keep_bucket, true}]);
build_recon_struct(merkle_tree, OldSyncStruct, _I, DBItems, _Params, BeginSync) ->
    ?DBG_ASSERT(not intervals:is_empty(_I)),
    ?DBG_ASSERT(merkle_tree:is_merkle_tree(OldSyncStruct)),
    NTree = merkle_tree:insert_list(DBItems, OldSyncStruct),
    if BeginSync ->
           % no more DB items -> finish tree
           merkle_tree:gen_hash(NTree, true);
       true ->
           % don't hash now - there will be new items
           NTree
    end;
build_recon_struct(art, _OldSyncStruct = {}, I, DBItems, _Params = {}, BeginSync) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
    BranchFactor = get_merkle_branch_factor(),
    BucketSize = merkle_tree:get_opt_bucket_size(length(DBItems), BranchFactor, 1),
    Tree = merkle_tree:new(I, DBItems, [{branch_factor, BranchFactor},
                                        {bucket_size, BucketSize},
                                        {keep_bucket, true}]),
    if BeginSync ->
           % no more DB items -> create art struct:
           #art_recon_struct{art = art:new(Tree, get_art_config()),
                             branch_factor = BranchFactor,
                             bucket_size = BucketSize};
       true ->
           % more DB items to come... stay with merkle tree
           Tree
    end;
build_recon_struct(art, OldSyncStruct, _I, DBItems, _Params = {}, BeginSync) ->
    % similar to continued merkle build
    ?DBG_ASSERT(not intervals:is_empty(_I)),
    ?DBG_ASSERT(merkle_tree:is_merkle_tree(OldSyncStruct)),
    Tree1 = merkle_tree:insert_list(DBItems, OldSyncStruct),
    if BeginSync ->
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
%%      Request responds with a list of {Key, Version, Value} tuples (if set
%%      for resolve) or {Key, Version} tuples (anything else).
%%      The mapping to DestI is not done here!
-spec send_chunk_req(DhtPid::LPid, AnswerPid::LPid, ChunkI::I, DestI::I,
                     MaxItems::pos_integer() | all, create_struct | reconcile | resolve) -> ok when
    is_subtype(LPid,        comm:erl_local_pid()),
    is_subtype(I,           intervals:interval()).
send_chunk_req(DhtPid, SrcPid, I, _DestI, MaxItems, reconcile) ->
    ?DBG_ASSERT(intervals:is_subset(I, _DestI)),
    SrcPidReply = comm:reply_as(SrcPid, 2, {reconcile, '_'}),
    send_local(DhtPid,
               {get_chunk, SrcPidReply, I, fun get_chunk_filter/1,
                fun get_chunk_kv/1, MaxItems});
send_chunk_req(DhtPid, SrcPid, I, DestI, MaxItems, create_struct) ->
    SrcPidReply = comm:reply_as(SrcPid, 3, {create_struct2, DestI, '_'}),
    send_local(DhtPid,
               {get_chunk, SrcPidReply, I, fun get_chunk_filter/1,
                fun get_chunk_kv/1, MaxItems});
send_chunk_req(DhtPid, SrcPid, I, _DestI, MaxItems, resolve) ->
    ?DBG_ASSERT(I =:= _DestI),
    SrcPidReply = comm:reply_as(SrcPid, 2, {resolve, '_'}),
    send_local(DhtPid,
               {get_chunk, SrcPidReply, I, fun get_chunk_filter/1,
                fun get_chunk_kvv/1, MaxItems}).

-spec get_chunk_filter(db_entry:entry()) -> boolean().
get_chunk_filter(DBEntry) -> db_entry:get_version(DBEntry) =/= -1.
-spec get_chunk_kv(db_entry:entry()) -> {?RT:key(), db_dht:version() | -1}.
get_chunk_kv(DBEntry) -> {db_entry:get_key(DBEntry), db_entry:get_version(DBEntry)}.
-spec get_chunk_kvv(db_entry:entry()) -> {?RT:key(), db_dht:version() | -1, db_dht:value()}.
get_chunk_kvv(DBEntry) -> {db_entry:get_key(DBEntry), db_entry:get_version(DBEntry), db_entry:get_value(DBEntry)}.

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
%%      PRE: keys in Borders and HB must be unique (as created in quadrant_intervals/0)!
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
    ?DBG_ASSERT(intervals:is_continuous(I)),
    ?DBG_ASSERT(1 =:= length([ok || Q <- quadrant_intervals(),
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
                              ?DBG_ASSERT(?RT:get_range(LKeyX, ?IIF(RKeyX =:= ?MINUS_INFINITY, ?PLUS_INFINITY, RKeyX)) =:=
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
    ?DBG_ASSERT(intervals:is_continuous(A)),
    ?DBG_ASSERT(intervals:is_continuous(B)),
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
    ?DBG_ASSERT(intervals:is_continuous(A)),
    ?DBG_ASSERT(intervals:is_continuous(B)),
    ?DBG_ASSERT(1 =:= length([ok || Q <- quadrant_intervals(),
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
    PidName = lists:flatten(io_lib:format("~s_~p.~s", [?MODULE, SessionId, randoms:getRandomString()])),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, State,
                             [{pid_groups_join_as, pid_groups:my_groupname(),
                               {short_lived, PidName}}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config parameter handling
%
% rr_recon_p1e              - probability of a single false positive,
%                             i.e. false positive absolute count
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
    config:cfg_is_in(rr_recon_method, [bloom, merkle_tree, art, trivial]) andalso
        config:cfg_is_float(rr_recon_p1e) andalso
        config:cfg_is_greater_than(rr_recon_p1e, 0) andalso
        config:cfg_is_less_than_equal(rr_recon_p1e, 1) andalso
        config:cfg_is_integer(rr_merkle_branch_factor) andalso
        config:cfg_is_greater_than(rr_merkle_branch_factor, 1) andalso
        config:cfg_is_integer(rr_merkle_bucket_size) andalso
        config:cfg_is_greater_than(rr_merkle_bucket_size, 0) andalso
        check_percent(rr_art_inner_fpr) andalso
        check_percent(rr_art_leaf_fpr) andalso
        config:cfg_is_integer(rr_art_correction_factor) andalso
        config:cfg_is_greater_than(rr_art_correction_factor, 0).

-spec get_p1e() -> float().
get_p1e() ->
    config:read(rr_recon_p1e).

%% @doc Use at least these many bits for compressed version numbers.
-spec get_min_version_bits() -> 16.
get_min_version_bits() -> 16.

%% @doc Specifies how many items to retrieve from the DB at once.
%%      Tries to reduce the load of a single request in the dht_node process.
-spec get_max_items() -> pos_integer() | all.
get_max_items() ->
    case config:read(rr_max_items) of
        failed -> 100000;
        CfgX   -> CfgX
    end.

%% @doc Merkle number of childs per inner node.
-spec get_merkle_branch_factor() -> pos_integer().
get_merkle_branch_factor() ->
    config:read(rr_merkle_branch_factor).

%% @doc Merkle max items in a leaf node.
-spec get_merkle_bucket_size() -> pos_integer().
get_merkle_bucket_size() ->
    config:read(rr_merkle_bucket_size).

-spec get_art_config() -> art:config().
get_art_config() ->
    [{correction_factor, config:read(rr_art_correction_factor)},
     {inner_bf_fpr, config:read(rr_art_inner_fpr)},
     {leaf_bf_fpr, config:read(rr_art_leaf_fpr)}].
