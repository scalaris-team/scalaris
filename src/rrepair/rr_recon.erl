% @copyright 2011-2015 Zuse Institute Berlin

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
-include("client_types.hrl").

-export([init/1, on/2, start/2, check_config/0]).
-export([map_key_to_interval/2, map_key_to_quadrant/2, map_rkeys_to_quadrant/2,
         map_interval/2,
         quadrant_intervals/0]).
-export([get_chunk_kv/1, get_chunk_filter/1]).
%-export([compress_kv_list/4, calc_signature_size_nm_pair/4]).

%export for testing
-export([find_sync_interval/2, quadrant_subints_/3, key_dist/2]).
-export([merkle_compress_hashlist/4, merkle_decompress_hashlist/3]).
-export([pos_to_bitstring/4, bitstring_to_k_list_k/3, bitstring_to_k_list_kv/3]).
%% -export([calc_signature_size_nm_pair/4, calc_n_subparts_p1e/2, calc_n_subparts_p1e/3,
%%          trivial_signature_sizes/3, trivial_worst_case_failprob/3,
%%          bloom_fp/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), log:pal("~w: [ ~p:~.0p ] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).
-define(TRACE_SEND(Pid, Msg), ?TRACE("to ~p:~.0p: ~.0p~n", [pid_groups:group_of(comm:make_local(comm:get_plain_pid(Pid))), Pid, Msg])).
-define(TRACE1(Msg, State),
        ?TRACE("~n  Msg: ~.0p~n"
               "  State: method: ~.0p;  stage: ~.0p;  initiator: ~.0p~n"
               "          destI: ~.0p~n"
               "         params: ~.0p~n",
               [Msg, State#rr_recon_state.method, State#rr_recon_state.stage,
                State#rr_recon_state.initiator, State#rr_recon_state.dest_interval,
                ?IIF(is_list(State#rr_recon_state.struct), State#rr_recon_state.struct, [])])).
-define(MERKLE_DEBUG(X,Y), ok).
%-define(MERKLE_DEBUG(X,Y), log:pal("~w: [ ~p:~.0p ] " ++ X, [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([method/0, request/0]).

-type method()         :: trivial | shash | bloom | merkle_tree | art.% | iblt.
-type stage()          :: req_shared_interval | build_struct | reconciliation | resolve.

-type exit_reason()    :: empty_interval |      %interval intersection between initator and client is empty
                          recon_node_crash |    %sync partner node crashed
                          sync_finished |       %finish recon on local node
                          sync_finished_remote. %client-side shutdown by merkle-tree recon initiator

-type db_chunk_kv()    :: [{?RT:key(), client_version()}].

-type signature_size() :: 0..160. % use an upper bound of 160 (SHA-1) to limit automatic testing
-type kvi_tree()       :: gb_trees:tree(KeyBin::bitstring(), {VersionShort::non_neg_integer(), Idx::non_neg_integer()}).
-type shash_kv_set()   :: gb_sets:set(KVBin::bitstring()).

-record(trivial_recon_struct,
        {
         interval = intervals:empty()                         :: intervals:interval(),
         reconPid = undefined                                 :: comm:mypid() | undefined,
         db_chunk = ?required(trivial_recon_struct, db_chunk) :: bitstring(),
         sig_size = ?required(trivial_recon_struct, sig_size) :: signature_size(),
         ver_size = ?required(trivial_recon_struct, ver_size) :: signature_size()
        }).

-record(shash_recon_struct,
        {
         interval = intervals:empty()                         :: intervals:interval(),
         reconPid = undefined                                 :: comm:mypid() | undefined,
         db_chunk = ?required(trivial_recon_struct, db_chunk) :: bitstring(),
         sig_size = ?required(trivial_recon_struct, sig_size) :: signature_size(),
         p1e_p2   = ?required(trivial_recon_struct, p1e_p2)   :: float()
        }).

-record(bloom_recon_struct,
        {
         interval   = intervals:empty()                         :: intervals:interval(),
         reconPid   = undefined                                 :: comm:mypid() | undefined,
         bf_bin     = ?required(bloom_recon_struct, bloom)      :: binary(),
         item_count = ?required(bloom_recon_struct, item_count) :: non_neg_integer(),
         p1e        = ?required(bloom_recon_struct, p1e)        :: float()
        }).

-record(merkle_params,
        {
         interval       = ?required(merkle_param, interval)       :: intervals:interval(),
         reconPid       = undefined                               :: comm:mypid() | undefined,
         branch_factor  = ?required(merkle_param, branch_factor)  :: pos_integer(),
         num_trees      = ?required(merkle_param, num_trees)      :: pos_integer(),
         bucket_size    = ?required(merkle_param, bucket_size)    :: pos_integer(),
         p1e            = ?required(merkle_param, p1e)            :: float(),
         ni_item_count  = ?required(merkle_param, ni_item_count)  :: non_neg_integer()
        }).

-record(art_recon_struct,
        {
         art            = ?required(art_recon_struct, art)            :: art:art(),
         branch_factor  = ?required(art_recon_struct, branch_factor)  :: pos_integer(),
         bucket_size    = ?required(art_recon_struct, bucket_size)    :: pos_integer()
        }).

-type sync_struct() :: #trivial_recon_struct{} |
                       #shash_recon_struct{} |
                       #bloom_recon_struct{} |
                       merkle_tree:merkle_tree() |
                       [merkle_tree:mt_node()] |
                       #art_recon_struct{}.
-type parameters() :: #trivial_recon_struct{} |
                      #shash_recon_struct{} |
                      #bloom_recon_struct{} |
                      #merkle_params{} |
                      #art_recon_struct{}.
-type recon_dest() :: ?RT:key() | random.

-type merkle_sync_rcv() ::
          {MyMaxItemsCount::non_neg_integer(),
           MyKVItems::merkle_tree:mt_bucket(), LeafCount::pos_integer()}.
-type merkle_sync_send() ::
          {OtherMaxItemsCount::non_neg_integer(),
           MyKVItems::merkle_tree:mt_bucket(), MyItemsCount::non_neg_integer()}.
-type merkle_sync_direct() ::
          % leaf-leaf mismatches with empty leaf hash on the initiator
          % -> these are resolved directly by the non-initiator
          {MyKItems::[?RT:key()], LeafCount::non_neg_integer()}.
-type merkle_sync() :: {[merkle_sync_send()], [merkle_sync_rcv()], merkle_sync_direct()}.

-record(rr_recon_state,
        {
         ownerPid           = ?required(rr_recon_state, ownerPid)    :: pid(),
         dest_rr_pid        = ?required(rr_recon_state, dest_rr_pid) :: comm:mypid(), %dest rrepair pid
         dest_recon_pid     = undefined                              :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                              :: method() | undefined,
         dest_interval      = intervals:empty()                      :: intervals:interval(),
         my_sync_interval   = intervals:empty()                      :: intervals:interval(),
         params             = {}                                     :: parameters() | {}, % parameters from the other node
         struct             = {}                                     :: sync_struct() | {}, % my recon structure
         stage              = req_shared_interval                    :: stage(),
         initiator          = false                                  :: boolean(),
         merkle_sync        = {[], [], {[], 0}}                      :: merkle_sync(),
         misc               = []                                     :: [{atom(), term()}], % any optional parameters an algorithm wants to keep
         kv_list            = []                                     :: db_chunk_kv(),
         k_list             = []                                     :: [?RT:key()],
         stats              = ?required(rr_recon_state, stats)       :: rr_recon_stats:stats(),
         to_resolve         = {[], []}                               :: {ToSend::rr_resolve:kvv_list(), ToReqIdx::[non_neg_integer()]}
         }).
-type state() :: #rr_recon_state{}.

% keep in sync with merkle_check_node/20
-define(recon_ok,                       1). % match
-define(recon_fail_stop_leaf,           2). % mismatch, sending node has leaf node
-define(recon_fail_stop_inner,          3). % mismatch, sending node has inner node
-define(recon_fail_cont_inner,          0). % mismatch, both inner nodes (continue!)

-type merkle_cmp_request() :: {Hash::merkle_tree:mt_node_key() | none, IsLeaf::boolean()}.

-type request() ::
    {start, method(), DestKey::recon_dest()} |
    {create_struct, method(), SenderI::intervals:interval()} | % from initiator
    {start_recon, bloom, #bloom_recon_struct{}} | % to initiator
    {start_recon, merkle_tree, #merkle_params{}} | % to initiator
    {start_recon, art, #art_recon_struct{}}. % to initiator

-type message() ::
    % API
    request() |
    % trivial/shash/bloom sync messages
    {resolve_req, BinReqIdxPos::bitstring()} |
    {resolve_req, DBChunk::bitstring(), DiffIdx::bitstring(), SigSize::signature_size(),
     VSize::signature_size(), SenderPid::comm:mypid()} |
    {resolve_req, DBChunk::bitstring(), SigSize::signature_size(),
     VSize::signature_size(), SenderPid::comm:mypid()} |
    {resolve_req, shutdown} |
    {resolve_req, BinReqIdxPos::bitstring()} |
    % merkle tree sync messages
    {?check_nodes, SenderPid::comm:mypid(), ToCheck::bitstring(), MaxItemsCount::non_neg_integer()} |
    {?check_nodes, ToCheck::bitstring(), MaxItemsCount::non_neg_integer()} |
    {?check_nodes_response, FlagsBin::bitstring(), MaxItemsCount::non_neg_integer()} |
    {resolve_req, Hashes::bitstring()} |
    {resolve_req, idx, BinKeyList::bitstring()} |
    % dht node response
    {create_struct2, {get_state_response, MyI::intervals:interval()}} |
    {create_struct2, DestI::intervals:interval(),
     {get_chunk_response, {intervals:interval(), db_chunk_kv()}}} |
    {reconcile, {get_chunk_response, {intervals:interval(), db_chunk_kv()}}} |
    % internal
    {shutdown, exit_reason()} |
    {fd_notify, fd:event(), DeadPid::comm:mypid(), Reason::fd:reason()} |
    {'DOWN', MonitorRef::reference(), process, Owner::pid(), Info::any()}
    .

-include("gen_component.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state() | kill.

on({create_struct, RMethod, SenderI} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % (first) request from initiator to create a sync struct
    This = comm:reply_as(comm:this(), 2, {create_struct2, '_'}),
    comm:send_local(pid_groups:get_my(dht_node), {get_state, This, my_range}),
    State#rr_recon_state{method = RMethod, initiator = false, dest_interval = SenderI};

on({create_struct2, {get_state_response, MyI}} = _Msg,
   State = #rr_recon_state{stage = req_shared_interval, initiator = false,
                           method = RMethod,
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
                art -> ok; % legacy (no integrated trivial sync yet)
                _   -> fd:subscribe(self(), [DestRRPid])
            end,
            % reduce SenderI to the sub-interval matching SyncI, i.e. a mapped SyncI
            SenderSyncI = map_interval(SenderI, SyncI),
            send_chunk_req(pid_groups:get_my(dht_node), self(),
                           SyncI, SenderSyncI, get_max_items(), create_struct),
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
   State = #rr_recon_state{dest_rr_pid = DestRRPid, stats = Stats, misc = Misc}) ->
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
            Params1 = Params#trivial_recon_struct{db_chunk = <<>>},
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            % convert db_chunk to a gb_tree for faster access checks
            OrigDBChunkLen = calc_items_in_chunk(DBChunk, SigSize + VSize),
            DBChunkTree =
                decompress_kv_list(DBChunk, [], SigSize, VSize, 0),
            % calculate P1E(phase1) from the point of view of the non-initiator:
            P1E_p1 = trivial_worst_case_failprob(SigSize, OrigDBChunkLen, OrigDBChunkLen),
            ?DBG_ASSERT(Misc =:= []),
            Misc1 = [{db_chunk, {DBChunkTree, OrigDBChunkLen}}];
        shash ->
            #shash_recon_struct{interval = MySyncI, reconPid = DestReconPid,
                                db_chunk = DBChunk, sig_size = SigSize} = Params,
            Params1 = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            % convert db_chunk to a gb_set for faster access checks
            OrigDBChunkLen = calc_items_in_chunk(DBChunk, SigSize),
            DBChunkSet =
                shash_decompress_kv_list(DBChunk, [], SigSize),
            % calculate P1E(phase1) from the point of view of the non-initiator:
            P1E_p1 = trivial_worst_case_failprob(SigSize, OrigDBChunkLen, OrigDBChunkLen),
            ?DBG_ASSERT(Misc =:= []),
            Misc1 = [{db_chunk, DBChunkSet},
                     {oicount, OrigDBChunkLen}];
        bloom ->
            #bloom_recon_struct{interval = MySyncI,
                                reconPid = DestReconPid,
                                bf_bin = BFBin,
                                item_count = BFCount,
                                p1e = P1E} = Params,
            Params1 = Params#bloom_recon_struct{bf_bin = <<>>},
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            ?DBG_ASSERT(Misc =:= []),
            FP = bloom_fp(BFCount, P1E),
            BF = bloom:new_bin(BFBin, ?REP_HFS:new(bloom:calc_HF_numEx(BFCount, FP)), BFCount),
            P1E_p1 = 0.0, % needs to be set when we know the number of chunks on this node!
            Misc1 = [{bloom, BF}, {item_count, 0}];
        merkle_tree ->
            #merkle_params{interval = MySyncI, reconPid = DestReconPid} = Params,
            Params1 = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            P1E_p1 = 0.0, % needs to be set at the end of phase 1!
            Misc1 = Misc;
        art ->
            MySyncI = art:get_interval(Params#art_recon_struct.art),
            Params1 = Params,
            DestReconPid = undefined,
            P1E_p1 = 0.0, % TODO
            Misc1 = Misc
    end,
    % client only sends non-empty sync intervals or exits
    ?DBG_ASSERT(not intervals:is_empty(MySyncI)),
    
    send_chunk_req(pid_groups:get_my(dht_node), self(),
                   MySyncI, MySyncI, get_max_items(), reconcile),
    NStats  = rr_recon_stats:set([{p1e_phase1, P1E_p1}], Stats),
    State#rr_recon_state{stage = reconciliation, params = Params1,
                         method = RMethod, initiator = true,
                         my_sync_interval = MySyncI,
                         dest_recon_pid = DestReconPid,
                         stats = NStats, misc = Misc1};

on({reconcile, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,        initiator = true,
                           method = trivial,
                           params = #trivial_recon_struct{sig_size = SigSize,
                                                          ver_size = VSize},
                           dest_rr_pid = DestRRPid,    stats = Stats,
                           ownerPid = OwnerL, to_resolve = {ToSend, ToReqIdx},
                           misc = [{db_chunk, {OtherDBChunk, OrigDBChunkLen}}],
                           dest_recon_pid = DestReconPid}) ->
    ?TRACE1(_Msg, State),

    % identify items to send, request and the remaining (non-matched) DBChunk:
    {ToSend1, ToReqIdx1, OtherDBChunk1} =
        get_full_diff(DBList, OtherDBChunk, ToSend, ToReqIdx, SigSize, VSize),
    ?DBG_ASSERT2(length(ToSend1) =:= length(lists:usort(ToSend1)),
                 {non_unique_send_list, ToSend, ToSend1}),

    %if rest interval is non empty get another chunk
    SyncFinished = intervals:is_empty(RestI),
    if SyncFinished ->
           NewStats = send_resolve_request(Stats, ToSend1, OwnerL, DestRRPid,
                                           true, true),
           % let the non-initiator's rr_recon process identify the remaining keys
           ReqIdx = lists:usort([Idx || {_Version, Idx} <- gb_trees:values(OtherDBChunk1)]
                                    ++ ToReqIdx1),
           ToReq2 = compress_idx_list(ReqIdx, OrigDBChunkLen, [], 0, 0),
           NewStats2 =
               if ReqIdx =/= [] ->
                      % the non-initiator will use key_upd_send and we must thus increase
                      % the number of resolve processes here!
                      rr_recon_stats:inc([{rs_expected, 1}], NewStats);
                  true -> NewStats
               end,

           ?TRACE("resolve_req Trivial Session=~p ; ToReq=~p (~p bits)",
                  [rr_recon_stats:get(session_id, Stats), length(ReqIdx),
                   erlang:bit_size(ToReq2)]),
           comm:send(DestReconPid, {resolve_req, ToReq2}),
           
           shutdown(sync_finished,
                    State#rr_recon_state{stats = NewStats2,
                                         to_resolve = {[], []},
                                         misc = []});
       true ->
           send_chunk_req(pid_groups:get_my(dht_node), self(),
                          RestI, RestI, get_max_items(), reconcile),
           State#rr_recon_state{to_resolve = {ToSend1, ToReqIdx1},
                                misc = [{db_chunk, {OtherDBChunk1, OrigDBChunkLen}}]}
    end;

on({reconcile, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,    initiator = true,
                           method = shash,
                           params = #shash_recon_struct{sig_size = SigSize,
                                                        p1e_p2 = P1E_p2} = Params,
                           stats = Stats,             kv_list = KVList,
                           misc = [{db_chunk, OtherDBChunk},
                                   {oicount, OtherItemCount}],
                           dest_recon_pid = DestReconPid,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL}) ->
    ?TRACE1(_Msg, State),
    % this is similar to the trivial sync above and the bloom sync below

    % identify differing items and the remaining (non-matched) DBChunk:
    {NewKVList, OtherDBChunk1} =
        shash_get_full_diff(DBList, OtherDBChunk, KVList, SigSize),
    NewState = State#rr_recon_state{kv_list = NewKVList,
                                    misc = [{db_chunk, OtherDBChunk1},
                                            {oicount, OtherItemCount}]},

    %if rest interval is non empty start another sync
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(pid_groups:get_my(dht_node), self(),
                          RestI, RestI, get_max_items(), reconcile),
           NewState;
       true ->
           CKVSize = length(NewKVList), CKidxSize = gb_sets:size(OtherDBChunk1),
           StartResolve = CKVSize + CKidxSize > 0,
           ?TRACE("Reconcile SHash Session=~p ; Diff=~B+~B",
                  [rr_recon_stats:get(session_id, Stats), CKVSize, CKidxSize]),
           if StartResolve andalso OtherItemCount > 0 ->
                  % send idx of non-matching other items & KV-List of my diff items
                  % start resolve similar to a trivial recon but using the full diff!
                  % (as if non-initiator in trivial recon)
                  {BuildTime, {MyDiff, SigSizeT, VSizeT}} =
                      util:tc(fun() ->
                                      compress_kv_list_p1e(
                                        NewKVList, CKVSize, CKidxSize, P1E_p2)
                              end),
                  P1E_p2_real = trivial_worst_case_failprob(
                                  SigSizeT, CKVSize, CKidxSize),
                  KList = [element(1, KV) || KV <- NewKVList],
                  OtherDBChunkOrig = Params#shash_recon_struct.db_chunk,
                  Params1 = Params#shash_recon_struct{db_chunk = <<>>},
                  OtherDiffIdx = shash_compress_k_list(OtherDBChunk1, OtherDBChunkOrig,
                                                       SigSize, 0, [], 0, 0),

                  send(DestReconPid,
                       {resolve_req, MyDiff, OtherDiffIdx, SigSizeT, VSizeT, comm:this()}),
                  % the non-initiator will use key_upd_send and we must thus increase
                  % the number of resolve processes here!
                  NewStats1 = rr_recon_stats:inc([{rs_expected, 1},
                                                  {build_time, BuildTime}], Stats),
                  NewStats  = rr_recon_stats:set([{p1e_phase2, P1E_p2_real}], NewStats1),
                  NewState#rr_recon_state{stats = NewStats, stage = resolve,
                                          params = Params1,
                                          kv_list = [], k_list = KList,
                                          misc = [{my_bin_diff_empty, MyDiff =:= <<>>}]};
              StartResolve -> % andalso OtherItemCount =:= 0 ->
                  ?DBG_ASSERT(OtherItemCount =:= 0),
                  % no need to send resolve_req message - the non-initiator already shut down
                  % the other node does not have any items but there is a diff at our node!
                  % start a resolve here:
                  KList = [element(1, KV) || KV <- NewKVList],
                  NewStats = send_resolve_request(
                               Stats, KList, OwnerL, DestRRPid, true, false),
                  NewState2 = NewState#rr_recon_state{stats = NewStats, stage = resolve},
                  shutdown(sync_finished, NewState2);
              OtherItemCount =:= 0 ->
                  shutdown(sync_finished, NewState);
              true -> % OtherItemCount > 0
                  % must send resolve_req message for the non-initiator to shut down
                  send(DestReconPid, {resolve_req, shutdown}),
                  shutdown(sync_finished, NewState)
           end
    end;

on({reconcile, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,    initiator = true,
                           method = bloom,
                           params = #bloom_recon_struct{p1e = P1E},
                           stats = Stats,             kv_list = KVList,
                           misc = [{bloom, BF}, {item_count, MyPrevItemCount}],
                           dest_recon_pid = DestReconPid,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL}) ->
    ?TRACE1(_Msg, State),
    MyItemCount = MyPrevItemCount + length(DBList0),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    BFCount = bloom:item_count(BF),
    Diff = if BFCount =:= 0 -> DBList0;
              true -> [X || X <- DBList0, not bloom:is_element(BF, X)]
           end,
    NewKVList = lists:append(KVList, Diff),

    %if rest interval is non empty start another sync
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(pid_groups:get_my(dht_node), self(),
                          RestI, RestI, get_max_items(), reconcile),
           State#rr_recon_state{kv_list = NewKVList,
                                misc = [{bloom, BF}, {item_count, MyItemCount}]};
       true ->
           FullDiffSize = length(NewKVList),
           % here, the failure probability is correct (as opposed by the
           % non-initiator) since we know how many item checks we perform in BF
           P1E_p1 = bloom_worst_case_failprob(BF, MyItemCount),
           Stats1  = rr_recon_stats:set([{p1e_phase1, P1E_p1}], Stats),
           ?TRACE("Reconcile Bloom Session=~p ; Diff=~p",
                  [rr_recon_stats:get(session_id, Stats1), FullDiffSize]),
           if FullDiffSize > 0 andalso BFCount > 0 ->
                  % start resolve similar to a trivial recon but using the full diff!
                  % (as if non-initiator in trivial recon)
                  % NOTE: use left-over P1E after phase 1 (bloom) for phase 2 (trivial RC)
                  P1E_p2 = calc_n_subparts_p1e(1, P1E, (1 - P1E_p1)),
                  {BuildTime, {MyDiff, SigSize, VSize}} =
                      util:tc(fun() ->
                                      compress_kv_list_p1e(
                                        NewKVList, FullDiffSize,
                                        BFCount, P1E_p2)
                              end),
                  ?DBG_ASSERT(MyDiff =/= <<>>),
                  P1E_p2_real = trivial_worst_case_failprob(
                                  SigSize, FullDiffSize, BFCount),
                  
                  send(DestReconPid,
                       {resolve_req, MyDiff, SigSize, VSize, comm:this()}),
                  % the non-initiator will use key_upd_send and we must thus increase
                  % the number of resolve processes here!
                  NewStats1 = rr_recon_stats:inc([{rs_expected, 1},
                                                  {build_time, BuildTime}], Stats1),
                  NewStats  = rr_recon_stats:set([{p1e_phase2, P1E_p2_real}], NewStats1),
                  State#rr_recon_state{stats = NewStats, stage = resolve,
                                       kv_list = [],
                                       k_list = [element(1, KV) || KV <- NewKVList],
                                       misc = [{my_bin_diff_empty, MyDiff =:= <<>>}]};
              FullDiffSize > 0 -> % andalso BFCount =:= 0 ->
                  ?DBG_ASSERT(BFCount =:= 0),
                  % no need to send resolve_req message - the non-initiator already shut down
                  % empty BF with diff at our node -> the other node does not have any items!
                  % start a resolve here:
                  NewStats = send_resolve_request(
                               Stats1, [element(1, KV) || KV <- NewKVList], OwnerL,
                               DestRRPid, true, false),
                  NewState = State#rr_recon_state{stats = NewStats, stage = resolve,
                                                  kv_list = NewKVList},
                  shutdown(sync_finished, NewState);
              BFCount =:= 0 -> % andalso FullDiffSize =:= 0 ->
                  shutdown(sync_finished, State#rr_recon_state{stats = Stats1,
                                                               kv_list = NewKVList});
              true -> % BFCount > 0, FullDiffSize =:= 0
                  % must send resolve_req message for the non-initiator to shut down
                  send(DestReconPid, {resolve_req, shutdown}),
                  % note: kv_list has not changed, we can thus use the old State here:
                  shutdown(sync_finished, State#rr_recon_state{stats = Stats1})
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

on({fd_notify, crash, _Pid, _Reason} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    shutdown(recon_node_crash, State);

on({fd_notify, _Event, _Pid, _Reason} = _Msg, State) ->
    State;

on({shutdown, Reason}, State) ->
    shutdown(Reason, State);

on({'DOWN', MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    gen_component:demonitor(MonitorRef),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% trivial/shash/bloom reconciliation sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({resolve_req, BinReqIdxPos} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = trivial,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           k_list = KList,            stats = Stats}) ->
    ?TRACE1(_Msg, State),
    NewStats =
        send_resolve_request(Stats, decompress_idx_to_k_list(BinReqIdxPos, KList),
                             OwnerL, DestRRPid, _Initiator = false, true),
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats});

on({resolve_req, shutdown} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = RMethod})
  when (RMethod =:= bloom orelse RMethod =:= shash) ->
    shutdown(sync_finished, State);

on({resolve_req, OtherDBChunk, MyDiffIdx, SigSize, VSize, DestReconPid} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = shash,            kv_list = KVList}) ->
    ?TRACE1(_Msg, State),
%%     log:pal("[ ~p ] CKIdx1: ~B (~B compressed)",
%%             [self(), erlang:byte_size(MyDiffIdx),
%%              erlang:byte_size(erlang:term_to_binary(MyDiffIdx, [compressed]))]),
    ?DBG_ASSERT(SigSize >= 0 andalso VSize >= 0),

    DBChunkTree =
        decompress_kv_list(OtherDBChunk, [], SigSize, VSize, 0),
    MyDiffKV = decompress_idx_to_kv_list(MyDiffIdx, KVList),
    State1 = State#rr_recon_state{kv_list = MyDiffKV},
    FBItems = [Key || {Key, _Version} <- MyDiffKV,
                      not gb_trees:is_defined(compress_key(Key, SigSize),
                                              DBChunkTree)],

    NewStats2 = shash_bloom_perform_resolve(
                  State1, DBChunkTree, SigSize, VSize, DestReconPid, FBItems),

    shutdown(sync_finished, State1#rr_recon_state{stats = NewStats2});

on({resolve_req, DBChunk, SigSize, VSize, DestReconPid} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = bloom}) ->
    ?TRACE1(_Msg, State),
    
    DBChunkTree = decompress_kv_list(DBChunk, [], SigSize, VSize, 0),

    NewStats2 = shash_bloom_perform_resolve(
                  State, DBChunkTree, SigSize, VSize, DestReconPid, []),
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats2});

on({resolve_req, BinReqIdxPos} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = true,
                           method = RMethod,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           k_list = KList,            stats = Stats,
                           misc = [{my_bin_diff_empty, MyBinDiffEmpty}]})
  when (RMethod =:= bloom orelse RMethod =:= shash) ->
    ?TRACE1(_Msg, State),
%%     log:pal("[ ~p ] CKIdx2: ~B (~B compressed)",
%%             [self(), erlang:byte_size(BinReqIdxPos),
%%              erlang:byte_size(erlang:term_to_binary(BinReqIdxPos, [compressed]))]),
    ToSend = if MyBinDiffEmpty -> KList; % optimised away by using 0 bits -> sync all!
                true           -> bitstring_to_k_list_k(BinReqIdxPos, KList, [])
             end,
    NewStats = send_resolve_request(
                 Stats, ToSend, OwnerL, DestRRPid, _Initiator = true, true),
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats});

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({?check_nodes, SenderPid, ToCheck, OtherMaxItemsCount}, State) ->
    NewState = State#rr_recon_state{dest_recon_pid = SenderPid},
    on({?check_nodes, ToCheck, OtherMaxItemsCount}, NewState);

on({?check_nodes, ToCheck0, OtherMaxItemsCount},
   State = #rr_recon_state{stage = reconciliation,    initiator = false,
                           method = merkle_tree,      merkle_sync = MerkleSync,
                           params = #merkle_params{} = Params,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           struct = TreeNodes,        stats = Stats,
                           dest_recon_pid = DestReconPid,
                           misc = [{p1e, LastP1ETotal},
                                   {p1e_phase_x, P1EPhaseX},
                                   {icount, MyLastMaxItemsCount}]}) ->
    ?DBG_ASSERT(comm:is_valid(DestReconPid)),
    {_P1E_I, _P1E_L, SigSizeI, SigSizeL, EffectiveP1E_I, EffectiveP1E_L} =
        merkle_next_signature_sizes(Params, LastP1ETotal, MyLastMaxItemsCount,
                                    OtherMaxItemsCount),
    ToCheck = merkle_decompress_hashlist(ToCheck0, SigSizeI, SigSizeL),
    PrevP0E = 1 - rr_recon_stats:get(p1e_phase1, Stats),
    {FlagsBin, RTree, MerkleSyncNew, NStats0, MyMaxItemsCount,
     NextLvlNodesAct, HashCmpI, HashCmpL} =
        merkle_check_node(ToCheck, TreeNodes, SigSizeI, SigSizeL,
                          MyLastMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                          <<>>, [], [], [], [], 0, MerkleSync, 0, 0, 0,
                          0, 0),
    ?IIF((EffectiveP1E_I > 0 andalso (1 - EffectiveP1E_I >= 1)) orelse
             (1 - EffectiveP1E_L >= 1), % EffectiveP1E_L is always greater than 0
         log:log("~w: [ ~p:~.0p ] merkle_next_signature_sizes/4 precision warning:"
                 " P1E_I = ~g, P1E_L = ~g",
                 [?MODULE, pid_groups:my_groupname(), self(),
                  EffectiveP1E_I, EffectiveP1E_L]),
         ok),
    NextP0E = PrevP0E * math:pow(1 - EffectiveP1E_I, HashCmpI)
                  * math:pow(1 - EffectiveP1E_L, HashCmpL),
    NStats = rr_recon_stats:set([{p1e_phase1, 1 - NextP0E}], NStats0),
    NewState = State#rr_recon_state{struct = RTree, merkle_sync = MerkleSyncNew},
    ?MERKLE_DEBUG("merkle (NI) - CurrentNodes: ~B~nP1E: ~g -> ~g",
                  [length(RTree), 1 - PrevP0E, 1 - NextP0E]),
    
    case MerkleSyncNew of
        {[], [], {SyncDRK, SyncDRLCount}} when RTree =:= [] ->
            send(DestReconPid, {?check_nodes_response, FlagsBin, MyMaxItemsCount}),
            % resolve the leaf-leaf comparisons's items with empty leaves on the initiator as key_upd:
            NStats1 = send_resolve_request(NStats, SyncDRK, OwnerL, DestRRPid, false, true),
            NStats2 = rr_recon_stats:inc([{tree_leavesSynced, SyncDRLCount}], NStats1),
            shutdown(sync_finished,
                     NewState#rr_recon_state{stats = NStats2, misc = []});
        {MerkleSyncNewSend, MerkleSyncNewRcv, {SyncDRK, SyncDRLCount}} when RTree =:= [] ->
            % start resolve
            ?TRACE("Sync (NI):~nsend:~.2p~n rcv:~.2p",
                   [MerkleSyncNewSend, MerkleSyncNewRcv]),
            send(DestReconPid, {?check_nodes_response, FlagsBin, MyMaxItemsCount}),
            % resolve the leaf-leaf comparisons's items with empty leaves on the initiator as key_upd:
            NStats1 = send_resolve_request(NStats, SyncDRK, OwnerL, DestRRPid, false, true),
            NStats2 = rr_recon_stats:inc([{tree_leavesSynced, SyncDRLCount}], NStats1),
            % allow the garbage collector to free the SyncDRK list here
            MerkleSyncNew1 = {MerkleSyncNewSend, MerkleSyncNewRcv, {[], SyncDRLCount}},
            
            MerkleSyncNewSendL = length(MerkleSyncNewSend),
            MerkleSyncNewRcvL = length(MerkleSyncNewRcv),
            TrivialProcs = MerkleSyncNewSendL + MerkleSyncNewRcvL,
            ?DBG_ASSERT(TrivialProcs > 0), % should always be true in this case!
            P1EAllLeaves = calc_n_subparts_p1e(1, Params#merkle_params.p1e, NextP0E),
            ?MERKLE_DEBUG("merkle (NI) - LeafSync~n~B/~B (send), ~B (receive)\tP1EAllLeaves: ~g\t"
                          "ItemsToSend: ~B+~B (~g per leaf)",
                          [SyncDRLCount, MerkleSyncNewSendL, MerkleSyncNewRcvL,
                           P1EAllLeaves, length(SyncDRK),
                           lists:sum([MyItemCount || {_, _, MyItemCount} <- MerkleSyncNewSend]),
                           ?IIF(MerkleSyncNewSend =/=[],
                                lists:sum([MyItemCount || {_, _, MyItemCount} <- MerkleSyncNewSend]) /
                                    MerkleSyncNewSendL, 0.0)]),
            if MerkleSyncNewSend =/= [] ->
                   {Hashes, NStats3} =
                       merkle_resolve_leaves_send(MerkleSyncNewSend, NStats2,
                                                  Params, P1EAllLeaves, TrivialProcs),
                   ?MERKLE_DEBUG("merkle (NI) - HashesSize: ~B (~B compressed)",
                                 [erlang:byte_size(Hashes),
                                  erlang:byte_size(
                                    erlang:term_to_binary(Hashes, [compressed]))]),
                   ?DBG_ASSERT(Hashes =/= <<>>),
                   % (leaf-leaf mismatches with empty leaves on the other node)
                   send(DestReconPid, {resolve_req, Hashes}),
                   NewState#rr_recon_state{stage = resolve, stats = NStats3,
                                           merkle_sync = MerkleSyncNew1,
                                           misc = [{all_leaf_p1e, P1EAllLeaves},
                                                   {trivial_procs, TrivialProcs}]};
               true ->
                   % only wait for the other node's resolve_req
                   NewState#rr_recon_state{stage = resolve, stats = NStats2,
                                           merkle_sync = MerkleSyncNew1,
                                           misc = [{all_leaf_p1e, P1EAllLeaves},
                                                   {trivial_procs, TrivialProcs}]}
            end;
        _ ->
            ?DBG_ASSERT(NextLvlNodesAct >= 0),
            send(DestReconPid, {?check_nodes_response, FlagsBin, MyMaxItemsCount}),
            % calculate the remaining trees' failure prob based on the already
            % used failure prob
            P1E_I_2 = calc_n_subparts_p1e(NextLvlNodesAct, P1EPhaseX, NextP0E),
            NewState#rr_recon_state{stats = NStats,
                                    misc = [{p1e, P1E_I_2},
                                            {p1e_phase_x, P1EPhaseX},
                                            {icount, MyMaxItemsCount}]}
    end;

on({?check_nodes_response, FlagsBin, OtherMaxItemsCount},
   State = #rr_recon_state{stage = reconciliation,        initiator = true,
                           method = merkle_tree,          merkle_sync = MerkleSync,
                           params = #merkle_params{} = Params,
                           struct = TreeNodes,            stats = Stats,
                           dest_recon_pid = DestReconPid,
                           misc = [{signature_size, {SigSizeI, SigSizeL}},
                                   {p1e, {EffectiveP1ETotal_I, EffectiveP1ETotal_L}},
                                   {p1e_phase_x, P1EPhaseX},
                                   {icount, MyLastMaxItemsCount},
                                   {oicount, OtherLastMaxItemsCount}]}) ->
    PrevP0E = 1 - rr_recon_stats:get(p1e_phase1, Stats),
    {RTree, MerkleSyncNew, NStats0, MyMaxItemsCount,
     NextLvlNodesAct, HashCmpI, HashCmpL} =
        merkle_cmp_result(FlagsBin, TreeNodes, SigSizeI, SigSizeL,
                          MyLastMaxItemsCount, OtherLastMaxItemsCount,
                          MerkleSync, Params, Stats, [], [], [], [], 0, 0, 0, 0,
                          0, 0),
    ?IIF((1 - EffectiveP1ETotal_I >= 1) orelse (1 - EffectiveP1ETotal_L >= 1),
         log:log("~w: [ ~p:~.0p ] merkle_next_signature_sizes/4 precision warning:"
                 " P1E_I = ~g, P1E_L = ~g",
                 [?MODULE, pid_groups:my_groupname(), self(),
                  EffectiveP1ETotal_I, EffectiveP1ETotal_L]),
         ok),
    NextP0E = PrevP0E * math:pow(1 - EffectiveP1ETotal_I, HashCmpI)
                  * math:pow(1 - EffectiveP1ETotal_L, HashCmpL),
    NStats = rr_recon_stats:set([{p1e_phase1, 1 - NextP0E}], NStats0),
    NewState = State#rr_recon_state{struct = RTree, merkle_sync = MerkleSyncNew},
    ?MERKLE_DEBUG("merkle (I) - CurrentNodes: ~B~nP1E: ~g -> ~g",
                  [length(RTree), 1 - PrevP0E, rr_recon_stats:get(p1e_phase1, NStats)]),

    case MerkleSyncNew of
        {[], [], {_SyncDRK, SyncDRLCount}} when RTree =:= [] ->
            NStats1 = rr_recon_stats:inc(
                        [{tree_leavesSynced, SyncDRLCount},
                         {rs_expected, ?IIF(SyncDRLCount > 0, 1, 0)}], NStats),
            shutdown(sync_finished,
                     NewState#rr_recon_state{stats = NStats1, misc = []});
        {MerkleSyncNewSend, MerkleSyncNewRcv, {_SyncDRK = [], SyncDRLCount}} when RTree =:= [] ->
            NStats1 = rr_recon_stats:inc(
                        [{tree_leavesSynced, SyncDRLCount},
                         {rs_expected, ?IIF(SyncDRLCount > 0, 1, 0)}], NStats),
            % start a (parallel) resolve
            ?TRACE("Sync (I):~nsend:~.2p~n rcv:~.2p",
                   [MerkleSyncNewSend, MerkleSyncNewRcv]),
            MerkleSyncNewSendL = length(MerkleSyncNewSend),
            MerkleSyncNewRcvL = length(MerkleSyncNewRcv),
            TrivialProcs = MerkleSyncNewSendL + MerkleSyncNewRcvL,
            ?DBG_ASSERT(TrivialProcs > 0), % should always be true in this case!
            P1EAllLeaves = calc_n_subparts_p1e(1, Params#merkle_params.p1e, NextP0E),
            ?MERKLE_DEBUG("merkle (I) - LeafSync~n~B (send), ~B/~B (receive)\tP1EAllLeaves: ~g\t"
                          "ItemsToSend: ~B (~g per leaf)",
                          [MerkleSyncNewSendL, SyncDRLCount, MerkleSyncNewRcvL,
                           P1EAllLeaves,
                           lists:sum([MyItemCount || {_, _, MyItemCount} <- MerkleSyncNewSend]),
                           ?IIF(MerkleSyncNewSend =/=[],
                                lists:sum([MyItemCount || {_, _, MyItemCount} <- MerkleSyncNewSend]) /
                                    MerkleSyncNewSendL, 0.0)]),
            if MerkleSyncNewSend =/= [] ->
                   {Hashes, NStats2} =
                       merkle_resolve_leaves_send(MerkleSyncNewSend, NStats1,
                                                  Params, P1EAllLeaves, TrivialProcs),
                   ?MERKLE_DEBUG("merkle (I) - HashesSize: ~B (~B compressed)",
                                 [erlang:byte_size(Hashes),
                                  erlang:byte_size(
                                    erlang:term_to_binary(Hashes, [compressed]))]),
                   ?DBG_ASSERT(Hashes =/= <<>>),
                   send(DestReconPid, {resolve_req, Hashes}),
                   NewState#rr_recon_state{stage = resolve, stats = NStats2,
                                           misc = [{all_leaf_p1e, P1EAllLeaves},
                                                   {trivial_procs, TrivialProcs}]};
               true ->
                   % only wait for the other node's resolve_req
                   NewState#rr_recon_state{stage = resolve, stats = NStats1,
                                           misc = [{all_leaf_p1e, P1EAllLeaves},
                                                   {trivial_procs, TrivialProcs}]}
            end;
        _ ->
            ?DBG_ASSERT(NextLvlNodesAct >= 0),
            % calculate the remaining trees' failure prob based on the already
            % used failure prob
            P1ETotal_I_2 = calc_n_subparts_p1e(NextLvlNodesAct, P1EPhaseX, NextP0E),
            {_P1E_I, _P1E_L, NextSigSizeI, NextSigSizeL, EffectiveP1E_I, EffectiveP1E_L} =
                merkle_next_signature_sizes(Params, P1ETotal_I_2, MyMaxItemsCount,
                                            OtherMaxItemsCount),
            Req = merkle_compress_hashlist(RTree, <<>>, NextSigSizeI, NextSigSizeL),
            send(DestReconPid, {?check_nodes, Req, MyMaxItemsCount}),
            NewState#rr_recon_state{stats = NStats,
                                    misc = [{signature_size, {NextSigSizeI, NextSigSizeL}},
                                            {p1e, {EffectiveP1E_I, EffectiveP1E_L}},
                                            {p1e_phase_x, P1EPhaseX},
                                            {icount, MyMaxItemsCount},
                                            {oicount, OtherMaxItemsCount}]}
    end;

on({resolve_req, Hashes} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = IsInitiator,
                           method = merkle_tree,
                           merkle_sync = {SyncSend, SyncRcv, DirectResolve},
                           params = Params,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           dest_recon_pid = DestRCPid, stats = Stats,
                           misc = [{all_leaf_p1e, P1EAllLeaves},
                                   {trivial_procs, TrivialProcs}]})
  when is_bitstring(Hashes) ->
    ?TRACE1(_Msg, State),
    ?DBG_ASSERT(?implies(not IsInitiator, Hashes =/= <<>>)),
    {BinIdxList, NStats} =
        merkle_resolve_leaves_receive(SyncRcv, Hashes, DestRRPid, Stats,
                                      OwnerL, Params, P1EAllLeaves, TrivialProcs,
                                      IsInitiator),
    comm:send(DestRCPid, {resolve_req, idx, BinIdxList}),
    % free up some memory:
    NewState = State#rr_recon_state{merkle_sync = {SyncSend, [], DirectResolve},
                                    stats = NStats},
    % NOTE: FIFO channels ensure that the {resolve_req, BinKeyList} is always
    %       received after the {resolve_req, Hashes} message from the other node!
    %       -> shutdown if nothing was sent
    if SyncSend =:= [] ->
           shutdown(sync_finished, NewState);
       true -> NewState
    end;

on({resolve_req, idx, BinKeyList} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = IsInitiator,
                           method = merkle_tree,
                           merkle_sync = {SyncSend, [], DirectResolve},
                           params = Params,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           stats = Stats})
    % NOTE: FIFO channels ensure that the {resolve_req, BinKeyList} is always
    %       received after the {resolve_req, Hashes} message from the other node!
  when is_bitstring(BinKeyList) ->
    ?MERKLE_DEBUG("merkle (~s) - BinKeyListSize: ~B compressed",
                  [?IIF(IsInitiator, "I", "NI"),
                   erlang:byte_size(
                     erlang:term_to_binary(BinKeyList, [compressed]))]),
    ?TRACE1(_Msg, State),
    NStats = if BinKeyList =:= <<>> ->
                    Stats;
                true ->
                    merkle_resolve_leaves_ckidx(
                      SyncSend, BinKeyList, DestRRPid, Stats, OwnerL,
                      Params, [], IsInitiator)
             end,
    NewState = State#rr_recon_state{merkle_sync = {[], [], DirectResolve},
                                    stats = NStats},
    shutdown(sync_finished, NewState).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_struct(DBList::db_chunk_kv(), DestI::intervals:non_empty_interval(),
                   RestI::intervals:interval(), state()) -> state() | kill.
build_struct(DBList, SyncI, RestI,
             State = #rr_recon_state{method = RMethod, params = Params,
                                     struct = {},
                                     initiator = Initiator, stats = Stats,
                                     stage = Stage,
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
                send_chunk_req(pid_groups:get_my(dht_node), self(),
                               RestI, SyncI, get_max_items(), Reconcile),
                false;
            true -> true
        end,
    NewKVList = lists:append(KVList, DBList),
    if BeginSync ->
           ToBuild = if Initiator andalso RMethod =:= art -> merkle_tree;
                        true -> RMethod
                     end,
           {BuildTime, {SyncStruct, P1E_p1}} =
               util:tc(fun() -> build_recon_struct(ToBuild, SyncI,
                                                   NewKVList, Params)
                       end),
           Stats1 = rr_recon_stats:inc([{build_time, BuildTime}], Stats),
           Stats2  = rr_recon_stats:set([{p1e_phase1, P1E_p1}], Stats1),
           NewState = State#rr_recon_state{struct = SyncStruct, stats = Stats2,
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
                                   dest_rr_pid = DestRRPid, kv_list = KVList}) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, trivial, MySyncStruct}}),
    State#rr_recon_state{struct = {}, stage = resolve, kv_list = [],
                         k_list = [element(1, KV) || KV <- KVList]};
begin_sync(MySyncStruct, _OtherSyncStruct = {},
           State = #rr_recon_state{method = shash, initiator = false,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid, kv_list = KVList}) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, shash, MySyncStruct}}),
    case MySyncStruct#shash_recon_struct.db_chunk of
        <<>> -> shutdown(sync_finished, State#rr_recon_state{kv_list = []});
        _    -> State#rr_recon_state{struct = {}, stage = resolve, kv_list = KVList}
    end;
begin_sync(MySyncStruct, _OtherSyncStruct = {},
           State = #rr_recon_state{method = bloom, initiator = false,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, bloom, MySyncStruct}}),
    case MySyncStruct#bloom_recon_struct.item_count of
        0 -> shutdown(sync_finished, State#rr_recon_state{kv_list = []});
        _ -> State#rr_recon_state{struct = {}, stage = resolve}
    end;
begin_sync(MySyncStruct, _OtherSyncStruct,
           State = #rr_recon_state{method = merkle_tree, initiator = Initiator,
                                   ownerPid = OwnerL, stats = Stats,
                                   params = Params,
                                   dest_recon_pid = DestReconPid,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    case Initiator of
        true ->
            MTSize = merkle_tree:size_detail(MySyncStruct),
            Stats1 = rr_recon_stats:set([{tree_size, MTSize}], Stats),
            #merkle_params{p1e = P1ETotal, num_trees = NumTrees,
                           ni_item_count = OtherItemsCount} = Params,
            MyItemCount = lists:max([0 | [merkle_tree:get_item_count(Node) || Node <- MySyncStruct]]),
            ?MERKLE_DEBUG("merkle (I) - CurrentNodes: ~B~n"
                          "Inner/Leaf/Items: ~p, EmptyLeaves: ~B",
                          [length(MySyncStruct), MTSize,
                           length([ok || L <- merkle_tree:get_leaves(MySyncStruct),
                                         merkle_tree:is_empty(L)])]),
            P1ETotal2 = calc_n_subparts_p1e(2, P1ETotal),
            P1ETotal3 = calc_n_subparts_p1e(NumTrees, P1ETotal2),

            {_P1E_I, _P1E_L, NextSigSizeI, NextSigSizeL, EffectiveP1E_I, EffectiveP1E_L} =
                merkle_next_signature_sizes(Params, P1ETotal3, MyItemCount,
                                            OtherItemsCount),

            Req = merkle_compress_hashlist(MySyncStruct, <<>>, NextSigSizeI, NextSigSizeL),
            send(DestReconPid, {?check_nodes, comm:this(), Req, MyItemCount}),
            State#rr_recon_state{stats = Stats1,
                                 misc = [{signature_size, {NextSigSizeI, NextSigSizeL}},
                                         {p1e, {EffectiveP1E_I, EffectiveP1E_L}},
                                         {p1e_phase_x, P1ETotal2},
                                         {icount, MyItemCount},
                                         {oicount, OtherItemsCount}],
                                 kv_list = []};
        false ->
            % tell the initiator to create its struct first, and then build ours
            % (at this stage, we do not have any data in the merkle tree yet!)
            DBItems = State#rr_recon_state.kv_list,
            MerkleI = merkle_tree:get_interval(MySyncStruct),
            MerkleV = merkle_tree:get_branch_factor(MySyncStruct),
            MerkleB = merkle_tree:get_bucket_size(MySyncStruct),
            NumTrees = get_merkle_num_trees(),
            P1ETotal = get_p1e(),
            P1ETotal2 = calc_n_subparts_p1e(2, P1ETotal),
            
            % split interval first and create NumTrees merkle trees later
            {BuildTime1, ICBList} =
                util:tc(fun() ->
                                merkle_tree:keys_to_intervals(
                                  DBItems, intervals:split(MerkleI, NumTrees))
                        end),
            ItemCount = lists:max([0 | [Count || {_SubI, Count, _Bucket} <- ICBList]]),
            P1ETotal3 = calc_n_subparts_p1e(NumTrees, P1ETotal2),

            MySyncParams = #merkle_params{interval = MerkleI,
                                          branch_factor = MerkleV,
                                          bucket_size = MerkleB,
                                          num_trees = NumTrees,
                                          p1e = P1ETotal,
                                          ni_item_count = ItemCount},
            SyncParams = MySyncParams#merkle_params{reconPid = comm:this()},
            SID = rr_recon_stats:get(session_id, Stats),
            send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                             {start_recon, merkle_tree, SyncParams}}),
            
            % finally create the real merkle tree containing data
            % -> this way, the initiator can create its struct in parallel!
            {BuildTime2, SyncStruct} =
                util:tc(fun() ->
                                [merkle_tree:get_root(
                                   merkle_tree:new(SubI, Bucket,
                                                   [{keep_bucket, true},
                                                    {branch_factor, MerkleV},
                                                    {bucket_size, MerkleB}]))
                                   || {SubI, _Count, Bucket} <- ICBList]
                        end),
            MTSize = merkle_tree:size_detail(SyncStruct),
            Stats1 = rr_recon_stats:set([{tree_size, MTSize}], Stats),
            Stats2 = rr_recon_stats:inc([{build_time, BuildTime1 + BuildTime2}], Stats1),
            ?MERKLE_DEBUG("merkle (NI) - CurrentNodes: ~B~n"
                          "Inner/Leaf/Items: ~p, EmptyLeaves: ~B",
                          [length(SyncStruct), MTSize,
                           length([ok || L <- merkle_tree:get_leaves(SyncStruct),
                                         merkle_tree:is_empty(L)])]),
            
            State#rr_recon_state{struct = SyncStruct,
                                 stats = Stats2, params = MySyncParams,
                                 misc = [{p1e, P1ETotal3},
                                         {p1e_phase_x, P1ETotal2},
                                         {icount, ItemCount}],
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
            send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
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
                trivial -> fd:unsubscribe(self(), [DestRR]);
                bloom   -> fd:unsubscribe(self(), [DestRR]);
                merkle_tree -> fd:unsubscribe(self(), [DestRR]);
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
%%      pairwise with each other (assuming the worst case, i.e. having M+N total
%%      hashes).
-spec calc_signature_size_nm_pair(N::non_neg_integer(), M::non_neg_integer(),
                                  P1E::float(), MaxSize::signature_size())
        -> SigSize::signature_size().
calc_signature_size_nm_pair(_, 0, P1E, _MaxSize) when P1E > 0 andalso P1E < 1 ->
    0;
calc_signature_size_nm_pair(0, _, P1E, _MaxSize) when P1E > 0 andalso P1E < 1 ->
    0;
calc_signature_size_nm_pair(N, M, P1E, MaxSize) when P1E > 0 andalso P1E < 1 ->
    NT = M + N,
%%     P = math:log(1 / (1-P1E)),
    % BEWARE: we cannot use (1-p1E) since it is near 1 and its floating
    %         point representation is sub-optimal!
    % => use Taylor expansion of math:log(1 / (1-P1E))  at P1E = 0
    %    (small terms first)
    % http://www.wolframalpha.com/input/?i=Taylor+expansion+of+log%281+%2F+%281-p%29%29++at+p+%3D+0
    P1E2 = P1E * P1E, P1E3 = P1E2* P1E, P1E4 = P1E3 * P1E, P1E5 = P1E4 * P1E,
    P = P1E + P1E2/2 + P1E3/3 + P1E4/4 + P1E5/5, % +O[p^6]
    min_max(util:ceil(util:log2(NT * (NT - 1) / (2 * P))), get_min_hash_bits(), MaxSize).

%% @doc Transforms a list of key and version tuples (with unique keys), into a
%%      compact binary representation for transfer.
-spec compress_kv_list(KVList::db_chunk_kv(), Bin,
                       SigSize::signature_size(), VSize::signature_size())
        -> Bin when is_subtype(Bin, bitstring()).
compress_kv_list([{K0, V} | TL], Bin, SigSize, VSize) ->
    KBin = compress_key(K0, SigSize),
    compress_kv_list(TL, <<Bin/bitstring, KBin/bitstring, V:VSize>>, SigSize, VSize);
compress_kv_list([], Bin, _SigSize, _VSize) ->
    Bin.

-spec calc_items_in_chunk(DBChunk::bitstring(), BitsPerItem::non_neg_integer())
-> NrItems::non_neg_integer().
calc_items_in_chunk(<<>>, 0) -> 0;
calc_items_in_chunk(DBChunk, BitsPerItem) ->
    ?DBG_ASSERT(erlang:bit_size(DBChunk) rem BitsPerItem =:= 0),
    erlang:bit_size(DBChunk) div BitsPerItem.

%% @doc De-compresses the binary from compress_kv_list/4 into a gb_tree with a
%%      binary key representation and the integer of the (shortened) version.
-spec decompress_kv_list(CompressedBin::bitstring(),
                         AccList::[{KeyBin::bitstring(), {VersionShort::non_neg_integer(), Idx::non_neg_integer()}}],
                         SigSize::signature_size(), VSize::signature_size(), CurPos::non_neg_integer())
        -> ResTree::kvi_tree().
decompress_kv_list(<<>>, AccList, _SigSize, _VSize, _CurPos) ->
    gb_trees:from_orddict(orddict:from_list(AccList));
decompress_kv_list(Bin, AccList, SigSize, VSize, CurPos) ->
    <<KeyBin:SigSize/bitstring, Version:VSize, T/bitstring>> = Bin,
    decompress_kv_list(T, [{KeyBin, {Version, CurPos}} | AccList], SigSize, VSize, CurPos + 1).

%% @doc Gets all entries from MyEntries which are not encoded in MyIOtherKvTree
%%      or the entry in MyEntries has a newer version than the one in the tree
%%      and returns them as FBItems. ReqItems contains items in the tree but
%%      where the version in MyEntries is older than the one in the tree.
-spec get_full_diff(MyEntries::db_chunk_kv(), MyIOtherKvTree::kvi_tree(),
                    AccFBItems::[?RT:key()], AccReqItems::[non_neg_integer()],
                    SigSize::signature_size(), VSize::signature_size())
        -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
            MyIOtherKvTree::kvi_tree()}.
get_full_diff(MyEntries, MyIOtKvTree, FBItems, ReqItemsIdx, SigSize, VSize) ->
    get_full_diff_(MyEntries, MyIOtKvTree, FBItems, ReqItemsIdx, SigSize,
                  util:pow(2, VSize)).

%% @doc Helper for get_full_diff/6.
-spec get_full_diff_(MyEntries::db_chunk_kv(), MyIOtherKvTree::kvi_tree(),
                     AccFBItems::[?RT:key()], AccReqItems::[non_neg_integer()],
                     SigSize::signature_size(), VMod::pos_integer())
        -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
            MyIOtherKvTree::kvi_tree()}.
get_full_diff_([], MyIOtKvTree, FBItems, ReqItemsIdx, _SigSize, _VMod) ->
    {FBItems, ReqItemsIdx, MyIOtKvTree};
get_full_diff_([{Key, Version} | Rest], MyIOtKvTree, FBItems, ReqItemsIdx, SigSize, VMod) ->
    {KeyBin, VersionShort} = compress_kv_pair(Key, Version, SigSize, VMod),
    case gb_trees:lookup(KeyBin, MyIOtKvTree) of
        none ->
            get_full_diff_(Rest, MyIOtKvTree, [Key | FBItems],
                           ReqItemsIdx, SigSize, VMod);
        {value, {OtherVersionShort, Idx}} ->
            MyIOtKvTree2 = gb_trees:delete(KeyBin, MyIOtKvTree),
            if VersionShort > OtherVersionShort ->
                   get_full_diff_(Rest, MyIOtKvTree2, [Key | FBItems],
                                  ReqItemsIdx, SigSize, VMod);
               VersionShort =:= OtherVersionShort ->
                   get_full_diff_(Rest, MyIOtKvTree2, FBItems,
                                  ReqItemsIdx, SigSize, VMod);
               true -> % VersionShort < OtherVersionShort
                   get_full_diff_(Rest, MyIOtKvTree2, FBItems,
                                  [Idx | ReqItemsIdx], SigSize, VMod)
            end
    end.

%% @doc Gets all entries from MyEntries which are in MyIOtherKvTree
%%      and the entry in MyEntries has a newer version than the one in the tree
%%      and returns them as FBItems. ReqItems contains items in the tree but
%%      where the version in MyEntries is older than the one in the tree.
-spec get_part_diff(MyEntries::db_chunk_kv(), MyIOtherKvTree::kvi_tree(),
                    AccFBItems::[?RT:key()], AccReqItems::[non_neg_integer()],
                    SigSize::signature_size(), VSize::signature_size())
        -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
            MyIOtherKvTree::kvi_tree()}.
get_part_diff(MyEntries, MyIOtKvTree, FBItems, ReqItemsIdx, SigSize, VSize) ->
    get_part_diff_(MyEntries, MyIOtKvTree, FBItems, ReqItemsIdx, SigSize,
                   util:pow(2, VSize)).

%% @doc Helper for get_part_diff/6.
-spec get_part_diff_(MyEntries::db_chunk_kv(), MyIOtherKvTree::kvi_tree(),
                     AccFBItems::[?RT:key()], AccReqItems::[non_neg_integer()],
                     SigSize::signature_size(), VMod::pos_integer())
        -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
            MyIOtherKvTree::kvi_tree()}.
get_part_diff_([], MyIOtKvTree, FBItems, ReqItemsIdx, _SigSize, _VMod) ->
    {FBItems, ReqItemsIdx, MyIOtKvTree};
get_part_diff_([{Key, Version} | Rest], MyIOtKvTree, FBItems, ReqItemsIdx, SigSize, VMod) ->
    {KeyBin, VersionShort} = compress_kv_pair(Key, Version, SigSize, VMod),
    case gb_trees:lookup(KeyBin, MyIOtKvTree) of
        none ->
            get_part_diff_(Rest, MyIOtKvTree, FBItems, ReqItemsIdx,
                           SigSize, VMod);
        {value, {OtherVersionShort, Idx}} ->
            MyIOtKvTree2 = gb_trees:delete(KeyBin, MyIOtKvTree),
            if VersionShort > OtherVersionShort ->
                   get_part_diff_(Rest, MyIOtKvTree2, [Key | FBItems], ReqItemsIdx,
                                  SigSize, VMod);
               VersionShort =:= OtherVersionShort ->
                   get_part_diff_(Rest, MyIOtKvTree2, FBItems, ReqItemsIdx,
                                  SigSize, VMod);
               true ->
                   get_part_diff_(Rest, MyIOtKvTree2, FBItems, [Idx | ReqItemsIdx],
                                  SigSize, VMod)
            end
    end.

%% @doc Transforms a single key and version tuple into a compact binary
%%      representation.
%%      Similar to compress_kv_list/4.
-spec compress_kv_pair(Key::?RT:key(), Version::client_version(),
                        SigSize::signature_size(), VMod::pos_integer())
        -> {BinKey::bitstring(), VersionShort::integer()}.
compress_kv_pair(Key, Version, SigSize, VMod) ->
    KeyBin = compress_key(Key, SigSize),
    VersionShort = Version rem VMod,
    {<<KeyBin/bitstring>>, VersionShort}.

%% @doc Transforms a single key into a compact binary representation.
%%      Similar to compress_kv_pair/4.
-spec compress_key(Key::?RT:key() | {Key::?RT:key(), Version::client_version()},
                   SigSize::signature_size()) -> BinKey::bitstring().
compress_key(Key, SigSize) ->
    KBin = erlang:md5(erlang:term_to_binary(Key)),
    RestSize = erlang:bit_size(KBin) - SigSize,
    if RestSize >= 0  ->
           <<_:RestSize/bitstring, KBinCompressed:SigSize/bitstring>> = KBin,
           KBinCompressed;
       true ->
           FillSize = erlang:abs(RestSize),
           <<0:FillSize, KBin/binary>>
    end.

%% @doc Creates a compressed version of a (key-)position list.
%%      MaxPosBound represents an upper bound on the biggest value in the list;
%%      when decoding, the same bound must be known!
%% @see shash_compress_k_list/7
-spec compress_idx_list(SortedIdxList::[non_neg_integer()],
                        MaxPosBound::non_neg_integer(), ResultIdx::[non_neg_integer()],
                        LastPos::non_neg_integer(), Max::non_neg_integer())
        -> CompressedIndices::bitstring().
compress_idx_list([Pos | Rest], MaxPosBound, AccResult, LastPos, Max) ->
    CurIdx = Pos - LastPos,
    compress_idx_list(Rest, MaxPosBound, [CurIdx | AccResult], Pos + 1,
                      erlang:max(CurIdx, Max));
compress_idx_list([], MaxPosBound, AccResult, _LastPos, Max) ->
    IdxSize = if Max =:= 0 -> 1;
                 true      -> bits_for_number(Max)
              end,
    Bin = lists:foldr(fun(Pos, Acc) ->
                              <<Acc/bitstring, Pos:IdxSize/integer-unit:1>>
                      end, <<>>, AccResult),
    case Bin of
        <<>> ->
            <<>>;
        _ ->
            IdxBitsSize = bits_for_number(bits_for_number(MaxPosBound)),
            <<IdxSize:IdxBitsSize/integer-unit:1, Bin/bitstring>>
    end.

%% @doc De-compresses a bitstring with indices from compress_idx_list/5 or
%%      shash_compress_k_list/7 into a list of keys from the original key list.
-spec decompress_idx_to_k_list(CompressedBin::bitstring(), KList::[?RT:key()])
        -> ResKeys::[?RT:key()].
decompress_idx_to_k_list(<<>>, _KList) ->
    [];
decompress_idx_to_k_list(Bin, KList) ->
    IdxBitsSize = bits_for_number(bits_for_number(length(KList))),
    <<SigSize:IdxBitsSize/integer-unit:1, Bin2/bitstring>> = Bin,
    decompress_idx_to_k_list_(Bin2, KList, SigSize).

%% @doc Helper for decompress_idx_to_k_list/2.
-spec decompress_idx_to_k_list_(CompressedBin::bitstring(), KList::[?RT:key()],
                                SigSize::signature_size()) -> ResKeys::[?RT:key()].
decompress_idx_to_k_list_(<<>>, _, _SigSize) ->
    [];
decompress_idx_to_k_list_(Bin, KList, SigSize) ->
    <<KeyPosInc:SigSize/integer-unit:1, T/bitstring>> = Bin,
    [Key | KList2] = lists:nthtail(KeyPosInc, KList),
    [Key | decompress_idx_to_k_list_(T, KList2, SigSize)].

%% @doc De-compresses a bitstring with indices from compress_idx_list/5 or
%%      shash_compress_k_list/7 into a list of keys from the original KV list.
%%      NOTE: This is essentially the same as decompress_idx_to_k_list/2 but we
%%            need the separation because of the opaque RT keys.
-spec decompress_idx_to_kv_list(CompressedBin::bitstring(), KVList::db_chunk_kv())
        -> ResKeys::db_chunk_kv().
decompress_idx_to_kv_list(<<>>, _KVList) ->
    [];
decompress_idx_to_kv_list(Bin, KVList) ->
    IdxBitsSize = bits_for_number(bits_for_number(length(KVList))),
    <<SigSize:IdxBitsSize/integer-unit:1, Bin2/bitstring>> = Bin,
    decompress_idx_to_kv_list_(Bin2, KVList, SigSize).

%% @doc Helper for decompress_idx_to_kv_list/2.
-spec decompress_idx_to_kv_list_(CompressedBin::bitstring(), KVList::db_chunk_kv(),
                                 SigSize::signature_size()) -> ResKeys::db_chunk_kv().
decompress_idx_to_kv_list_(<<>>, _, _SigSize) ->
    [];
decompress_idx_to_kv_list_(Bin, KVList, SigSize) ->
     <<KeyPosInc:SigSize/integer-unit:1, T/bitstring>> = Bin,
    [X | KVList2] = lists:nthtail(KeyPosInc, KVList),
    [X | decompress_idx_to_kv_list_(T, KVList2, SigSize)].

%% @doc Converts a list of positions to a bitstring where the x'th bit is set
%%      if the x'th position is in the list. The final bitstring may be
%%      created with erlang:list_to_bitstring(lists:reverse(Result)).
%%      A total of FinalSize bits will be used.
%%      PreCond: sorted list Pos, 0 &lt;= every pos &lt; FinalSize
-spec pos_to_bitstring(Pos::[non_neg_integer()], AccBin::[bitstring()],
                       BitsSet::non_neg_integer(), FinalSize::non_neg_integer())
        -> Result::[bitstring()].
pos_to_bitstring([Pos | Rest], AccBin, BitsSet, FinalSize) ->
    New = <<0:(Pos - BitsSet), 1:1>>,
    pos_to_bitstring(Rest, [New | AccBin], Pos + 1, FinalSize);
pos_to_bitstring([], AccBin, BitsSet, FinalSize) ->
    [<<0:(FinalSize - BitsSet)>> | AccBin].

%% @doc Converts the bitstring from pos_to_bitstring/4 into keys at the
%%      appropriate positions in KList. Result is reversly sorted.
%%      NOTE: This is essentially the same as bitstring_to_k_list_kv/3 but we
%%            need the separation because of the opaque RT keys.
-spec bitstring_to_k_list_k(PosBitString::bitstring(), KList::[?RT:key()],
                            Acc::[?RT:key()]) -> Result::[?RT:key()].
bitstring_to_k_list_k(<<1:1, RestBits/bitstring>>, [Key | RestK], Acc) ->
    bitstring_to_k_list_k(RestBits, RestK, [Key | Acc]);
bitstring_to_k_list_k(<<0:1, RestBits/bitstring>>, [_Key | RestK], Acc) ->
    bitstring_to_k_list_k(RestBits, RestK, Acc);
bitstring_to_k_list_k(<<>>, _KList, Acc) ->
    Acc; % last 0 bits may  be truncated, e.g. by setting FinalSize in pos_to_bitstring/4 accordingly
bitstring_to_k_list_k(RestBits, [], Acc) ->
    % there may be rest bits, but all should be 0:
    BitCount = erlang:bit_size(RestBits),
    ?ASSERT(<<0:BitCount>> =:= RestBits),
    Acc.

%% @doc Converts the bitstring from pos_to_bitstring/4 into keys at the
%%      appropriate positions in KVList. Result is reversly sorted.
-spec bitstring_to_k_list_kv(PosBitString::bitstring(), KVList::db_chunk_kv(),
                             Acc::[?RT:key()]) -> Result::[?RT:key()].
bitstring_to_k_list_kv(<<1:1, RestBits/bitstring>>, [{Key, _Version} | RestKV], Acc) ->
    bitstring_to_k_list_kv(RestBits, RestKV, [Key | Acc]);
bitstring_to_k_list_kv(<<0:1, RestBits/bitstring>>, [{_Key, _Version} | RestKV], Acc) ->
    bitstring_to_k_list_kv(RestBits, RestKV, Acc);
bitstring_to_k_list_kv(<<>>, _KVList, Acc) ->
    Acc; % last 0 bits may  be truncated, e.g. by setting FinalSize in pos_to_bitstring/4 accordingly
bitstring_to_k_list_kv(RestBits, [], Acc) ->
    % there may be rest bits, but all should be 0:
    BitCount = erlang:bit_size(RestBits),
    ?ASSERT(<<0:BitCount>> =:= RestBits),
    Acc.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% SHash specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Transforms a list of key and version tuples (with unique keys), into a
%%      compact binary representation for transfer.
-spec shash_compress_kv_list(KVList::db_chunk_kv(), Bin,
                             SigSize::signature_size())
        -> Bin when is_subtype(Bin, bitstring()).
shash_compress_kv_list([], Bin, _SigSize) ->
    Bin;
shash_compress_kv_list([KV | TL], Bin, SigSize) ->
    KBin = compress_key(KV, SigSize),
    shash_compress_kv_list(TL, <<Bin/bitstring, KBin/bitstring>>, SigSize).

%% @doc De-compresses the binary from shash_compress_kv_list/3 into a gb_set with a
%%      binary representation of the key and the integer of the (shortened) version.
-spec shash_decompress_kv_list(CompressedBin::bitstring(), AccList::[bitstring()],
                               SigSize::signature_size())
        -> ResSet::shash_kv_set().
shash_decompress_kv_list(<<>>, AccList, _SigSize) ->
    gb_sets:from_list(AccList);
shash_decompress_kv_list(Bin, AccList, SigSize) ->
    <<KeyBin:SigSize/bitstring, T/bitstring>> = Bin,
    shash_decompress_kv_list(T, [KeyBin | AccList], SigSize).

%% @doc Gets all entries from MyEntries which are not encoded in MyIOtKvSet.
%%      Also returns the tree with all these matches removed.
-spec shash_get_full_diff(MyEntries::KV, MyIOtherKvTree::shash_kv_set(),
                          AccDiff::KV, SigSize::signature_size())
        -> {Diff::KV, MyIOtherKvTree::shash_kv_set()}
            when is_subtype(KV, db_chunk_kv()).
shash_get_full_diff([], MyIOtKvSet, AccDiff, _SigSize) ->
    {AccDiff, MyIOtKvSet};
shash_get_full_diff([KV | Rest], MyIOtKvSet, AccDiff, SigSize) ->
    KeyBin = compress_key(KV, SigSize),
    OldSize = gb_sets:size(MyIOtKvSet),
    MyIOtKvSet2 = gb_sets:delete_any(KeyBin, MyIOtKvSet),
    case gb_sets:size(MyIOtKvSet2) of
        OldSize ->
            shash_get_full_diff(Rest, MyIOtKvSet2, [KV | AccDiff], SigSize);
        _ ->
            shash_get_full_diff(Rest, MyIOtKvSet2, AccDiff, SigSize)
    end.

%% @doc Creates a compressed version of the (unmatched) binary keys in the given
%%      set using the indices in the original KV list.
%% @see compress_idx_list/5
-spec shash_compress_k_list(KVSet::shash_kv_set(), OtherDBChunkOrig::bitstring(),
                            SigSize::signature_size(), AccPos::non_neg_integer(),
                            ResultIdx::[?RT:key()], LastPos::non_neg_integer(),
                            Max::non_neg_integer())
        -> CompressedIndices::bitstring().
shash_compress_k_list(_, <<>>, _SigSize, DBChunkLen, AccResult, _LastPos, Max) ->
    compress_idx_list([], DBChunkLen, AccResult, _LastPos, Max);
shash_compress_k_list(KVSet, Bin, SigSize, AccPos, AccResult, LastPos, Max) ->
    <<KeyBin:SigSize/bitstring, T/bitstring>> = Bin,
    NextPos = AccPos + 1,
    case gb_sets:is_member(KeyBin, KVSet) of
        false ->
            shash_compress_k_list(KVSet, T, SigSize, NextPos,
                                  AccResult, LastPos, Max);
        true ->
            CurIdx = AccPos - LastPos,
            shash_compress_k_list(KVSet, T, SigSize, NextPos,
                                  [CurIdx | AccResult], NextPos, erlang:max(CurIdx, Max))
    end.

%% @doc Part of the resolve_req message processing of the SHash and Bloom RC
%%      processes in phase 2 (trivial RC) at the non-initiator.
-spec shash_bloom_perform_resolve(
        State::state(), DBChunkTree::kvi_tree(),
        SigSize::signature_size(), VSize::signature_size(),
        DestReconPid::comm:mypid(), FBItems::[?RT:key()])
        -> rr_recon_stats:stats().
shash_bloom_perform_resolve(
  #rr_recon_state{dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                  kv_list = KVList,          stats = Stats,
                  method = _RMethod},
  DBChunkTree, SigSize, VSize, DestReconPid, FBItems) ->
    {ToSendKeys1, ToReqIdx1, DBChunkTree1} =
        get_part_diff(KVList, DBChunkTree, FBItems, [], SigSize, VSize),

    NewStats1 = send_resolve_request(Stats, ToSendKeys1, OwnerL, DestRRPid,
                                     false, false),

    % let the initiator's rr_recon process identify the remaining keys
    ReqIdx = lists:usort([Idx || {_Version, Idx} <- gb_trees:values(DBChunkTree1)] ++ ToReqIdx1),
    ToReq2 = erlang:list_to_bitstring(
               lists:reverse(
                 pos_to_bitstring(% note: ReqIdx positions start with 0
                   ReqIdx, [], 0, ?IIF(ReqIdx =:= [], 0, lists:last(ReqIdx) + 1)))),
    ?TRACE("resolve_req ~s Session=~p ; ToReq= ~p bytes",
           [_RMethod, rr_recon_stats:get(session_id, NewStats1), erlang:byte_size(ToReq2)]),
    comm:send(DestReconPid, {resolve_req, ToReq2}),
    % the initiator will use key_upd_send and we must thus increase
    % the number of resolve processes here!
    if ReqIdx =/= [] ->
           rr_recon_stats:inc([{rs_expected, 1}], NewStats1);
       true -> NewStats1
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculates from a total P1E the (next) P1E to use for signature and
%%      sub-tree reconciliations.
-spec merkle_next_p1e(BranchFactor::pos_integer(), P1ETotal::float())
    -> {P1E_I::float(), P1E_L::float()}.
merkle_next_p1e(BranchFactor, P1ETotal) ->
    % mistakes caused by:
    % inner node: current node or any of its BranchFactor children (B=BranchFactor+1)
    % leaf node: current node only (B=1) and thus P1ETotal
    % => current node's probability of 0 errors = P0E(child)^B
    P1E_I = calc_n_subparts_p1e(BranchFactor + 1, P1ETotal),
    P1E_L = P1ETotal,
%%     log:pal("merkle [ ~p ]~n - P1ETotal: ~p, \tP1E_I: ~p, \tP1E_L: ~p",
%%             [self(), P1ETotal, P1E_I, P1E_L]),
    {P1E_I, P1E_L}.

%% @doc Calculates the new signature sizes based on the next P1E as in
%%      merkle_next_p1e/2
-spec merkle_next_signature_sizes(
        Params::#merkle_params{}, P1ETotal::float(),
        MyMaxItemsCount::non_neg_integer(), OtherMaxItemsCount::non_neg_integer())
    -> {P1E_I::float(), P1E_L::float(),
        NextSigSizeI::signature_size(), NextSigSizeL::signature_size(),
        EffectiveP1E_I::float(), EffectiveP1E_L::float()}.
merkle_next_signature_sizes(
  #merkle_params{bucket_size = BucketSize, branch_factor = BranchFactor}, P1ETotal,
  MyMaxItemsCount, OtherMaxItemsCount) ->
    {P1E_I, P1E_L} = merkle_next_p1e(BranchFactor, P1ETotal),

    % note: we need to use the same P1E for this level's signature
    %       comparison as a children's tree has in total!
    if MyMaxItemsCount =/= 0 andalso OtherMaxItemsCount =/= 0 ->
           AffectedItemsI = MyMaxItemsCount + OtherMaxItemsCount,
           NextSigSizeI = min_max(util:ceil(util:log2(AffectedItemsI / P1E_I)),
                                  get_min_hash_bits(), 160),
           EffectiveP1E_I = float(AffectedItemsI / util:pow(2, NextSigSizeI));
       true ->
           NextSigSizeI = 0,
           EffectiveP1E_I = 0.0
    end,
    ?DBG_ASSERT2(EffectiveP1E_I >= 0 andalso EffectiveP1E_I < 1, EffectiveP1E_I),

    AffectedItemsL = 2 * BucketSize,
    NextSigSizeL = min_max(util:ceil(util:log2(AffectedItemsL / P1E_L)),
                           get_min_hash_bits(), 160),
    EffectiveP1E_L = float(AffectedItemsL / util:pow(2, NextSigSizeL)),
    ?DBG_ASSERT2(EffectiveP1E_L > 0 andalso EffectiveP1E_L < 1, EffectiveP1E_L),

    ?MERKLE_DEBUG("merkle - signatures~nMyMI: ~B,\tOtMI: ~B"
                  "\tP1E_I: ~g,\tP1E_L: ~g,\tSigSizeI: ~B,\tSigSizeL: ~B~n"
                  "  -> eff. P1E_I: ~g,\teff. P1E_L: ~g",
                  [MyMaxItemsCount, OtherMaxItemsCount,
                   P1E_I, P1E_L, NextSigSizeI, NextSigSizeL,
                   EffectiveP1E_I, EffectiveP1E_L]),
    {P1E_I, P1E_L, NextSigSizeI, NextSigSizeL, EffectiveP1E_I, EffectiveP1E_L}.

-compile({nowarn_unused_function, {min_max_feeder, 3}}).
-spec min_max_feeder(X::number(), Min::number(), Max::number())
        -> {X::number(), Min::number(), Max::number()}.
min_max_feeder(X, Min, Max) when Min > Max -> {X, Max, Min};
min_max_feeder(X, Min, Max) -> {X, Min, Max}.

%% @doc Sets min and max boundaries for X and returns either Min, Max or X.
-spec min_max(X::number(), Min::number(), Max::number()) -> number().
min_max(X, _Min, Max) when X >= Max ->
    ?DBG_ASSERT(_Min =< Max orelse _Min =:= get_min_hash_bits()),
    Max;
min_max(X, Min, _Max) when X =< Min ->
    ?DBG_ASSERT(Min =< _Max orelse Min =:= get_min_hash_bits()),
    Min;
min_max(X, _Min, _Max) ->
    % dbg_assert must be true:
    %?DBG_ASSERT(_Min =< _Max orelse _Min =:= get_min_hash_bits()),
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
            Bin2 = case merkle_tree:is_empty(N1) of
                       true  -> <<Bin/bitstring, 1:1, 0:1>>;
                       false -> <<Bin/bitstring, 1:1, 1:1, H1:SigSizeL>>
                   end,
            merkle_compress_hashlist(TL, Bin2, SigSizeI, SigSizeL);
        false ->
            merkle_compress_hashlist(TL, <<Bin/bitstring, 0:1, H1:SigSizeI>>,
                                     SigSizeI, SigSizeL)
    end.

%% @doc Transforms the compact binary representation of merkle hash lists from
%%      merkle_compress_hashlist/2 back into the original form.
-spec merkle_decompress_hashlist(bitstring(), SigSizeI::signature_size(),
                                 SigSizeL::signature_size())
        -> Hashes::[merkle_cmp_request()].
merkle_decompress_hashlist(<<>>, _SigSizeI, _SigSizeL) ->
    [];
merkle_decompress_hashlist(Bin, SigSizeI, SigSizeL) ->
    IsLeaf = case Bin of
                 <<1:1, 1:1, Hash:SigSizeL/integer-unit:1, Bin2/bitstring>> ->
                     true;
                 <<1:1, 0:1, Bin2/bitstring>> ->
                     Hash = none,
                     true;
                 <<0:1, Hash:SigSizeI/integer-unit:1, Bin2/bitstring>> ->
                     false
             end,
    [{Hash, IsLeaf} | merkle_decompress_hashlist(Bin2, SigSizeI, SigSizeL)].

%% @doc Compares the given Hashes from the other node with my merkle_tree nodes
%%      (executed on non-initiator).
%%      Returns the comparison results and the rest nodes to check in a next
%%      step.
-spec merkle_check_node(
        Hashes::[merkle_cmp_request()], MyNodes::NodeList,
        SigSizeI::signature_size(), SigSizeL::signature_size(),
        MyMaxItemsCount::non_neg_integer(), OtherMaxItemsCount::non_neg_integer(),
        #merkle_params{}, Stats, FlagsAcc::bitstring(), RestTreeAcc::NodeList,
        SyncAccSend::[merkle_sync_send()], SyncAccRcv::[merkle_sync_rcv()],
        SyncAccDRK::[?RT:key()], SyncAccDRLCount::Count,
        SyncIn::merkle_sync(), AccCmp::Count, AccSkip::Count,
        NextLvlNodesActIN::Count, HashCmpI_IN::Count, HashCmpL_IN::Count)
        -> {FlagsOUT::bitstring(), RestTreeOut::NodeList,
            SyncOUT::merkle_sync(), Stats, MaxItemsCount::Count,
            NextLvlNodesActOUT::Count, HashCmpI_OUT::Count, HashCmpL_OUT::Count}
    when
      is_subtype(NodeList, [merkle_tree:mt_node()]),
      is_subtype(Stats,    rr_recon_stats:stats()),
      is_subtype(Count,    non_neg_integer()).
merkle_check_node([], [], _SigSizeI, _SigSizeL,
                  _MyMaxItemsCount, _OtherMaxItemsCount, _Params, Stats, FlagsAcc, RestTreeAcc,
                  SyncAccSend, SyncAccRcv, SyncAccDRK, SyncAccDRLCount,
                  {SyncInSend, SyncInRcv, {SyncInDRK, SyncInDRLCount}},
                  AccCmp, AccSkip, NextLvlNodesActIN,
                  HashCmpI_IN, HashCmpL_IN) ->
    NStats = rr_recon_stats:inc([{tree_nodesCompared, AccCmp},
                                 {tree_compareSkipped, AccSkip}], Stats),
    % note: we can safely include all leaf nodes here although only inner nodes
    %       should go into MIC - every inner node always has more items than
    %       any leaf node (otherwise it would have been a leaf node)
    AccMIC = lists:max([0 | [merkle_tree:get_item_count(Node) || Node <- RestTreeAcc]]),
    {FlagsAcc, lists:reverse(RestTreeAcc),
     {lists:reverse(SyncAccSend, SyncInSend),
      lists:reverse(SyncAccRcv, SyncInRcv),
      {SyncAccDRK ++ SyncInDRK, SyncInDRLCount + SyncAccDRLCount}},
     NStats, AccMIC, NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN};
merkle_check_node([{Hash, IsLeafHash} | TK], [Node | TN], SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, Params, Stats, FlagsAcc, RestTreeAcc,
                  SyncAccSend, SyncAccRcv, SyncAccDRK, SyncAccDRLCount,
                  SyncIN, AccCmp, AccSkip, NextLvlNodesActIN,
                  HashCmpI_IN, HashCmpL_IN) ->
    IsLeafNode = merkle_tree:is_leaf(Node),
    EmptyNode = merkle_tree:is_empty(Node),
    NodeHash =
        if IsLeafNode andalso EmptyNode ->
               none; % to match with the hash from merkle_decompress_hashlist/3
           IsLeafNode ->
               <<X:SigSizeL/integer-unit:1>> = <<(merkle_tree:get_hash(Node)):SigSizeL>>,
               X;
           true ->
               <<X:SigSizeI/integer-unit:1>> = <<(merkle_tree:get_hash(Node)):SigSizeI>>,
               X
        end,
    if Hash =:= NodeHash andalso IsLeafHash =:= IsLeafNode ->
           Skipped = merkle_tree:size(Node) - 1,
           if IsLeafHash andalso Hash =:= none ->
                  % empty leaf hash on both nodes - this was exact!
                  HashCmpI_OUT = HashCmpI_IN,
                  HashCmpL_OUT = HashCmpL_IN;
              IsLeafHash ->
                  HashCmpI_OUT = HashCmpI_IN,
                  HashCmpL_OUT = HashCmpL_IN + 1;
              true ->
                  HashCmpI_OUT = HashCmpI_IN + 1,
                  HashCmpL_OUT = HashCmpL_IN
           end,
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ?recon_ok:2>>, RestTreeAcc,
                             SyncAccSend, SyncAccRcv,
                             SyncAccDRK, SyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip + Skipped,
                             NextLvlNodesActIN, HashCmpI_OUT, HashCmpL_OUT);
       (not IsLeafNode) andalso (not IsLeafHash) ->
           % inner hash on both nodes
           Childs = merkle_tree:get_childs(Node),
           NextLvlNodesActOUT = NextLvlNodesActIN + Params#merkle_params.branch_factor,
           HashCmpI_OUT = HashCmpI_IN + 1,
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ?recon_fail_cont_inner:2>>, lists:reverse(Childs, RestTreeAcc),
                             SyncAccSend, SyncAccRcv,
                             SyncAccDRK, SyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActOUT, HashCmpI_OUT, HashCmpL_IN);
       (not IsLeafNode) andalso IsLeafHash ->
           % don't compare hashes -> this is an exact process based on the tags
           {MyKVItems, LeafCount} = merkle_tree:get_items([Node]),
           Sync = {MyMaxItemsCount, MyKVItems, LeafCount},
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ?recon_fail_stop_inner:2>>, RestTreeAcc,
                             SyncAccSend, [Sync | SyncAccRcv], 
                             SyncAccDRK, SyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN);
       IsLeafNode ->
           if IsLeafHash andalso Hash =/= none ->
                  % leaf mismatch (our node may be empty)
                  % distinguish the case where this node is empty which is exact, too!
                  case merkle_tree:is_empty(Node) of
                      true  -> ResultCode = ?recon_fail_cont_inner,
                               HashCmpL_OUT = HashCmpL_IN;
                      false -> ResultCode = ?recon_fail_stop_leaf,
                               HashCmpL_OUT = HashCmpL_IN + 1
                  end,
                  SyncAccSend1 =
                      [{erlang:min(Params#merkle_params.bucket_size,
                                   OtherMaxItemsCount), merkle_tree:get_bucket(Node),
                        merkle_tree:get_item_count(Node)} | SyncAccSend],
                  SyncAccDRK1 = SyncAccDRK,
                  SyncAccDRLCount1 = SyncAccDRLCount;
              IsLeafHash -> % andalso Hash =:= none
                  % empty leaf hash on the other node - we can resolve
                  % this case without a trivial sub process
                  ResultCode = ?recon_fail_stop_leaf,
                  SyncAccSend1 = SyncAccSend,
                  SyncAccDRK1 =
                      [element(1, X) || X <- merkle_tree:get_bucket(Node)]
                          ++ SyncAccDRK,
                  SyncAccDRLCount1 = SyncAccDRLCount + 1,
                  HashCmpL_OUT = HashCmpL_IN;
              true ->
                  % leaf node and inner hash
                  ResultCode = ?recon_fail_stop_leaf,
                  SyncAccSend1 =
                      [{OtherMaxItemsCount, merkle_tree:get_bucket(Node),
                        merkle_tree:get_item_count(Node)} | SyncAccSend],
                  SyncAccDRK1 = SyncAccDRK,
                  SyncAccDRLCount1 = SyncAccDRLCount,
                  HashCmpL_OUT = HashCmpL_IN
           end,
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ResultCode:2>>, RestTreeAcc,
                             SyncAccSend1, SyncAccRcv,
                             SyncAccDRK1, SyncAccDRLCount1,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActIN, HashCmpI_IN, HashCmpL_OUT)
    end.

%% @doc Processes compare results from merkle_check_node/20 on the initiator.
-spec merkle_cmp_result(
        bitstring(), RestTree::NodeList,
        SigSizeI::signature_size(), SigSizeL::signature_size(),
        MyMaxItemsCount::non_neg_integer(), OtherMaxItemsCount::non_neg_integer(),
        SyncIn::merkle_sync(), #merkle_params{}, Stats, RestTreeAcc::NodeList,
        SyncAccSend::[merkle_sync_send()], SyncAccRcv::[merkle_sync_rcv()],
        SyncAccDRK::[?RT:key()], SyncAccDRLCount::Count,
        AccCmp::Count, AccSkip::Count,
        NextLvlNodesActIN::Count, HashCmpI_IN::Count, HashCmpL_IN::Count)
        -> {RestTreeOut::NodeList, MerkleSyncOut::merkle_sync(),
            NewStats::Stats, MaxItemsCount::Count,
            NextLvlNodesActOUT::Count, HashCmpI_OUT::Count, HashCmpL_OUT::Count}
    when
      is_subtype(NodeList, [merkle_tree:mt_node()]),
      is_subtype(Stats,    rr_recon_stats:stats()),
      is_subtype(Count,    non_neg_integer()).
merkle_cmp_result(<<>>, [], _SigSizeI, _SigSizeL,
                  _MyMaxItemsCount, _OtherMaxItemsCount,
                  {SyncInSend, SyncInRcv, {SyncInDRK, SyncInDRLCount}},
                  _Params, Stats, RestTreeAcc, SyncAccSend, SyncAccRcv,
                  SyncAccDRK, SyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    NStats = rr_recon_stats:inc([{tree_nodesCompared, AccCmp},
                                 {tree_compareSkipped, AccSkip}], Stats),
    % note: we can safely include all leaf nodes here although only inner nodes
    %       should go into MIC - every inner node always has more items than
    %       any leaf node (otherwise it would have been a leaf node)
    AccMIC = lists:max([0 | [merkle_tree:get_item_count(Node) || Node <- RestTreeAcc]]),
     {lists:reverse(RestTreeAcc),
      {lists:reverse(SyncAccSend, SyncInSend),
       lists:reverse(SyncAccRcv, SyncInRcv),
       {SyncAccDRK ++ SyncInDRK, SyncInDRLCount + SyncAccDRLCount}},
      NStats, AccMIC, NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN};
merkle_cmp_result(<<?recon_ok:2, TR/bitstring>>, [Node | TN], SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, MerkleSyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv,
                  SyncAccDRK, SyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    case merkle_tree:is_leaf(Node) of
        true ->
            case merkle_tree:is_empty(Node) of
                true ->
                    % empty leaf hash on both nodes - this was exact!
                    HashCmpI_OUT = HashCmpI_IN,
                    HashCmpL_OUT = HashCmpL_IN;
                false ->
                    HashCmpI_OUT = HashCmpI_IN,
                    HashCmpL_OUT = HashCmpL_IN + 1
            end;
        false ->
            HashCmpI_OUT = HashCmpI_IN + 1,
            HashCmpL_OUT = HashCmpL_IN
    end,
    Skipped = merkle_tree:size(Node) - 1,
    merkle_cmp_result(TR, TN, SigSizeI, SigSizeL,
                      MyMaxItemsCount, OtherMaxItemsCount, MerkleSyncIn, Params, Stats,
                      RestTreeAcc, SyncAccSend, SyncAccRcv,
                      SyncAccDRK, SyncAccDRLCount, AccCmp + 1, AccSkip + Skipped,
                      NextLvlNodesActIN, HashCmpI_OUT, HashCmpL_OUT);
merkle_cmp_result(<<?recon_fail_cont_inner:2, TR/bitstring>>, [Node | TN],
                  SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv,
                  SyncAccDRK, SyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    case merkle_tree:is_leaf(Node) of
        true ->
            ?DBG_ASSERT(not merkle_tree:is_empty(Node)),
            % non-empty leaf on this node, empty leaf on the other node
            % this will increase HashCmpL although the process is exact
            % -> simply remove 1 and to remain at the same count!
            merkle_cmp_result(
              <<?recon_fail_stop_leaf:2, TR/bitstring>>, [Node | TN],
              SigSizeI, SigSizeL,
              MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
              RestTreeAcc, SyncAccSend, SyncAccRcv,
              SyncAccDRK, SyncAccDRLCount, AccCmp, AccSkip,
              NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN - 1);
        false ->
            % inner hash on both nodes
            Childs = merkle_tree:get_childs(Node),
            NextLvlNodesActOUT = NextLvlNodesActIN + Params#merkle_params.branch_factor,
            merkle_cmp_result(
              TR, TN, SigSizeI, SigSizeL,
              MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
              lists:reverse(Childs, RestTreeAcc), SyncAccSend, SyncAccRcv,
              SyncAccDRK, SyncAccDRLCount, AccCmp + 1, AccSkip,
              NextLvlNodesActOUT, HashCmpI_IN + 1, HashCmpL_IN)
    end;
merkle_cmp_result(<<?recon_fail_stop_inner:2, TR/bitstring>>, [Node | TN],
                  SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv,
                  SyncAccDRK, SyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    ?DBG_ASSERT(merkle_tree:is_leaf(Node)),
    % leaf-inner mismatch -> this is an exact process based on the tags
    Sync = {OtherMaxItemsCount, merkle_tree:get_bucket(Node),
            merkle_tree:get_item_count(Node)},
    merkle_cmp_result(TR, TN, SigSizeI, SigSizeL,
                      MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                      RestTreeAcc, [Sync | SyncAccSend], SyncAccRcv,
                      SyncAccDRK, SyncAccDRLCount, AccCmp + 1, AccSkip,
                      NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN);
merkle_cmp_result(<<?recon_fail_stop_leaf:2, TR/bitstring>>, [Node | TN],
                  SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv,
                  SyncAccDRK, SyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    case merkle_tree:is_leaf(Node) of
        true  ->
            case merkle_tree:is_empty(Node) of
                false ->
                    MaxItemsCount = erlang:min(Params#merkle_params.bucket_size,
                                               MyMaxItemsCount),
                    SyncAccRcv1 =
                        [{MaxItemsCount, merkle_tree:get_bucket(Node), 1} | SyncAccRcv],
                    SyncAccDRLCount1 = SyncAccDRLCount,
                    HashCmpL_OUT = HashCmpL_IN + 1;
                true ->
                    SyncAccRcv1 = SyncAccRcv,
                    SyncAccDRLCount1 = SyncAccDRLCount + 1,
                    HashCmpL_OUT = HashCmpL_IN
            end;
        false ->
            {MyKVItems, LeafCount} = merkle_tree:get_items([Node]),
            SyncAccRcv1 =
                [{MyMaxItemsCount, MyKVItems, LeafCount} | SyncAccRcv],
            SyncAccDRLCount1 = SyncAccDRLCount,
            HashCmpL_OUT = HashCmpL_IN
    end,
    merkle_cmp_result(TR, TN, SigSizeI, SigSizeL,
                      MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                      RestTreeAcc, SyncAccSend, SyncAccRcv1,
                      SyncAccDRK, SyncAccDRLCount1, AccCmp + 1, AccSkip,
                      NextLvlNodesActIN, HashCmpI_IN, HashCmpL_OUT).

%% @doc Gets the number of bits needed to encode all possible bucket sizes with
%%      the given merkle params.
-spec merkle_max_bucket_size_bits(Params::#merkle_params{}) -> pos_integer().
merkle_max_bucket_size_bits(Params) ->
    BucketSizeBits0 = bits_for_number(Params#merkle_params.bucket_size),
    ?IIF(config:read(rr_align_to_bytes),
         bloom:resize(BucketSizeBits0, 8),
         BucketSizeBits0).

%% @doc Helper for adding a leaf node's KV-List to a compressed binary
%%      during merkle sync.
-spec merkle_resolve_add_leaf_hash(
        Bucket::merkle_tree:mt_bucket(), BucketSize::non_neg_integer(),
        P1EAllLeaves::float(), NumRestLeaves::pos_integer(),
        OtherMaxItemsCount::non_neg_integer(), BucketSizeBits::pos_integer(),
        AccIn::X) -> AccOut::X
        when is_subtype(X, {Hashes::bitstring(), PrevP0E::float()}).
merkle_resolve_add_leaf_hash(
  Bucket, BucketSize, P1EAllLeaves, NumRestLeaves, OtherMaxItemsCount, BucketSizeBits,
  {HashesReply, PrevP0E}) ->
    ?DBG_ASSERT(BucketSize < util:pow(2, BucketSizeBits)),
    ?DBG_ASSERT(BucketSize =:= length(Bucket)),
    HashesReply1 = <<HashesReply/bitstring, BucketSize:BucketSizeBits>>,
    P1E_next = calc_n_subparts_p1e(NumRestLeaves, P1EAllLeaves, PrevP0E),
%%     log:pal("merkle_send [ ~p ]:~n   ~p~n   ~p",
%%             [self(), {NumRestLeaves, P1EAllLeaves, PrevP0E}, {BucketSize, OtherMaxItemsCount, P1E_next}]),
    {SigSize, VSize} = trivial_signature_sizes(BucketSize, OtherMaxItemsCount, P1E_next),
    P1E_p1 = trivial_worst_case_failprob(SigSize, BucketSize, OtherMaxItemsCount),
    NextP0E = PrevP0E * (1 - P1E_p1),
%%     log:pal("merkle_send [ ~p ] (rest: ~B):~n   bits: ~p, P1E: ~p vs. ~p~n   P0E: ~p -> ~p",
%%             [self(), NumRestLeaves, {SigSize, VSize}, P1E_next, P1E_p1, PrevP0E, NextP0E]),
    {compress_kv_list(Bucket, HashesReply1, SigSize, VSize), NextP0E}.

%% @doc Helper for retrieving a leaf node's KV-List from the compressed binary
%%      returned by merkle_resolve_add_leaf_hash/6 during merkle sync.
-spec merkle_resolve_retrieve_leaf_hashes(
        Hashes::bitstring(), P1EAllLeaves::float(), NumRestLeaves::pos_integer(),
        PrevP0E::float(), MyMaxItemsCount::non_neg_integer(),
        BucketSizeBits::pos_integer())
        -> {NHashes::bitstring(), OtherBucketTree::kvi_tree(),
            OrigDBChunkLen::non_neg_integer(),
            SigSize::signature_size(), VSize::signature_size(), PrevP0E::float()}.
merkle_resolve_retrieve_leaf_hashes(
  Hashes, P1EAllLeaves, NumRestLeaves, PrevP0E, MyMaxItemsCount, BucketSizeBits) ->
    <<BucketSize:BucketSizeBits/integer-unit:1, HashesT/bitstring>> = Hashes,
    P1E_next = calc_n_subparts_p1e(NumRestLeaves, P1EAllLeaves, PrevP0E),
%%     log:pal("merkle_receive [ ~p ]:~n   ~p~n   ~p",
%%             [self(), {NumRestLeaves, P1EAllLeaves, PrevP0E}, {BucketSize, MyMaxItemsCount, P1E_next}]),
    {SigSize, VSize} = trivial_signature_sizes(BucketSize, MyMaxItemsCount, P1E_next),
    P1E_p1 = trivial_worst_case_failprob(SigSize, BucketSize, MyMaxItemsCount),
    NextP0E = PrevP0E * (1 - P1E_p1),
%%     log:pal("merkle_receive [ ~p ] (rest: ~B):~n   bits: ~p, P1E: ~p vs. ~p~n   P0E: ~p -> ~p",
%%             [self(), NumRestLeaves, {SigSize, VSize}, P1E_next, P1E_p1, PrevP0E, NextP0E]),
    OBucketBinSize = BucketSize * (SigSize + VSize),
    <<OBucketBin:OBucketBinSize/bitstring, NHashes/bitstring>> = HashesT,
    OBucketTree = decompress_kv_list(OBucketBin, [], SigSize, VSize, 0),
    {NHashes, OBucketTree, BucketSize, SigSize, VSize, NextP0E}.

%% @doc Creates a compact binary consisting of bitstrings with trivial
%%      reconciliations for all sync requests to send.
-spec merkle_resolve_leaves_send(
        Sync::[merkle_sync_send(),...], Stats, Params::#merkle_params{},
        P1EAllLeaves::float(), TrivialProcs::pos_integer())
        -> {Hashes::bitstring(), NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_send([_|_] = Sync, Stats, Params, P1EAllLeaves, TrivialProcs) ->
    BucketSizeBits = merkle_max_bucket_size_bits(Params),
    % note: 1 trivial proc contains 1 leaf
    {{Hashes, ThisP0E}, LeafCount} =
        lists:foldl(
          fun({OtherMaxItemsCount, MyKVItems, MyItemCount},
              {HashesAcc, LeafNAcc}) ->
                  {merkle_resolve_add_leaf_hash(
                     MyKVItems, MyItemCount, P1EAllLeaves, TrivialProcs - LeafNAcc,
                     OtherMaxItemsCount, BucketSizeBits, HashesAcc), LeafNAcc + 1}
          end, {{<<>>, 1.0}, 0}, Sync),
    % the other node will send its items from this CKV list - increase rs_expected, too
    NStats1 = rr_recon_stats:inc([{tree_leavesSynced, LeafCount},
                                  {rs_expected, 1}], Stats),
    ?DBG_ASSERT(rr_recon_stats:get(p1e_phase2, NStats1) =:= 0.0),
    NStats  = rr_recon_stats:set([{p1e_phase2, 1 - ThisP0E}], NStats1),
    {Hashes, NStats}.

%% @doc Decodes the trivial reconciliations from merkle_resolve_leaves_send/4
%%      and resolves them returning a compressed idx list each with keys to
%%      request.
-spec merkle_resolve_leaves_receive(
        Sync::[merkle_sync_rcv()], Hashes::bitstring(), DestRRPid::comm:mypid(),
        Stats, OwnerL::comm:erl_local_pid(), Params::#merkle_params{},
        P1EAllLeaves::float(), TrivialProcs::pos_integer(), IsInitiator::boolean())
        -> {HashesReq::bitstring(), NewStats::Stats}
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_receive(Sync, Hashes, DestRRPid, Stats, OwnerL, Params,
                              P1EAllLeaves, TrivialProcs, IsInitiator) ->
    BucketSizeBits = merkle_max_bucket_size_bits(Params),
    % mismatches to resolve:
    % * at initiator    : inner(I)-leaf(NI) or leaf(NI)-non-empty-leaf(I)
    % * at non-initiator: inner(NI)-leaf(I)
    % note: 1 trivial proc may contain more than 1 leaf!
    {<<>>, ToSend, ToResolve, ResolveNonEmpty, LeafNAcc, _TrivialProcsRest, ThisP0E} =
        lists:foldl(
          fun({MyMaxItemsCount, MyKVItems, LeafCount},
              {HashesAcc, ToSend, ToResolve, ResolveNonEmpty, LeafNAcc, TProcsAcc, P0EIn}) ->
                  {NHashes, OBucketTree, _OrigDBChunkLen, SigSize, VSize, ThisP0E} =
                      merkle_resolve_retrieve_leaf_hashes(
                        HashesAcc, P1EAllLeaves, TProcsAcc, P0EIn,
                        MyMaxItemsCount, BucketSizeBits),
                  % calc diff (trivial sync)
                  {ToSend1, ToReqIdx1, OBucketTree1} =
                      get_full_diff(
                        MyKVItems, OBucketTree, ToSend, [], SigSize, VSize),
                  ReqIdx = lists:usort(
                             [Idx || {_Version, Idx} <- gb_trees:values(OBucketTree1)]
                                 ++ ToReqIdx1),
                  ToResolve1 = pos_to_bitstring(ReqIdx, ToResolve, 0,
                                                Params#merkle_params.bucket_size),
                  {NHashes, ToSend1, ToResolve1,
                   ?IIF(ReqIdx =/= [], true, ResolveNonEmpty),
                   LeafNAcc + LeafCount, TProcsAcc - 1, ThisP0E}
          end, {Hashes, [], [], false, 0, TrivialProcs, 1.0}, Sync),

    % send resolve message:
    % resolve items we should send as key_upd:
    % NOTE: the other node does not know whether our ToSend is empty and thus
    %       always expects a following resolve process!
    Stats1 = send_resolve_request(Stats, ToSend, OwnerL, DestRRPid, IsInitiator, false),
    % let the other node's rr_recon process identify the remaining keys;
    % it will use key_upd_send (if non-empty) and we must thus increase
    % the number of resolve processes here!
    if ResolveNonEmpty -> 
           ToResolve1 = erlang:list_to_bitstring(lists:reverse(ToResolve)),
           MerkleResReqs = 1;
       true ->
           ToResolve1 = <<>>,
           MerkleResReqs = 0
    end,
    NStats1 = rr_recon_stats:inc([{tree_leavesSynced, LeafNAcc},
                                  {rs_expected, MerkleResReqs}], Stats1),
    PrevP1E_p2 = rr_recon_stats:get(p1e_phase2, NStats1),
    NStats  = rr_recon_stats:set(
                [{p1e_phase2, 1 - (1 - PrevP1E_p2) * ThisP0E}], NStats1),
    ?TRACE("resolve_req Merkle Session=~p ; resolve expexted=~B",
           [rr_recon_stats:get(session_id, NStats),
            rr_recon_stats:get(rs_expected, NStats)]),
    {ToResolve1, NStats}.

%% @doc Decodes all requested keys from merkle_resolve_leaves_receive/8 (as a
%%      result of sending resolve requests) and resolves the appropriate entries
%%      (if non-empty) with our data using a key_upd_send.
-spec merkle_resolve_leaves_ckidx(
        Sync::[merkle_sync_send()], BinKeyList::bitstring(), DestRRPid::comm:mypid(),
        Stats, OwnerL::comm:erl_local_pid(), Params::#merkle_params{},
        ToSend::[?RT:key()], IsInitiator::boolean())
        -> NewStats::Stats
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_ckidx([{_OtherMaxItemsCount, MyKVItems, _LeafCount} | TL],
                             BinKeyList0,
                             DestRRPid, Stats, OwnerL, Params, ToSend, IsInitiator) ->
    Positions = Params#merkle_params.bucket_size,
    <<ReqKeys:Positions/bitstring-unit:1, BinKeyList/bitstring>> = BinKeyList0,
    ToSend1 = bitstring_to_k_list_kv(ReqKeys, MyKVItems, ToSend),
    merkle_resolve_leaves_ckidx(TL, BinKeyList, DestRRPid, Stats, OwnerL, Params,
                                ToSend1, IsInitiator);
merkle_resolve_leaves_ckidx([], <<>>, DestRRPid, Stats, OwnerL, _Params,
                            [_|_] = ToSend, IsInitiator) ->
    send_resolve_request(Stats, ToSend, OwnerL, DestRRPid, IsInitiator, false).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% art recon
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a single resolve request for all given leaf nodes by joining
%%      their intervals.
%%      Returns the number of resolve requests (requests with feedback count 2).
-spec resolve_leaves([merkle_tree:mt_node()], Dest::comm:mypid(),
                     rrepair:session_id(), OwnerLocal::comm:erl_local_pid())
        -> ResolveExp::0..2.
resolve_leaves([_|_] = Nodes, Dest, SID, OwnerL) ->
    resolve_leaves(Nodes, Dest, SID, OwnerL, intervals:empty(), 0);
resolve_leaves([], _Dest, _SID, _OwnerL) ->
    0.

%% @doc Helper for resolve_leaves/4.
-spec resolve_leaves([merkle_tree:mt_node()], Dest::comm:mypid(),
                     rrepair:session_id(), OwnerLocal::comm:erl_local_pid(),
                     Interval::intervals:interval(), Items::non_neg_integer())
        -> ResolveExp::0..2.
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
                   2;
               true ->
                   % we know that we don't have data in this range, so we must
                   % regenerate it from the other node
                   % -> send him this request directly!
                   send(Dest, {request_resolve, SID, {?interval_upd, Interval, []},
                               [{from_my_node, 0} | Options]}),
                   1
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
                ResolveExp = resolve_leaves(ASyncLeafs, DestPid, SID, OwnerL),
                rr_recon_stats:inc([{tree_nodesCompared, NComp},
                                    {tree_compareSkipped, NSkip},
                                    {tree_leavesSynced, NLSync},
                                    {rs_expected, ResolveExp}], Stats);
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

%% @doc Sends a request_resolve message to the rrepair layer which sends the
%%      entries from the given keys to the other node with a feedback request.
-spec send_resolve_request(Stats, ToSend::[?RT:key()], OwnerL::comm:erl_local_pid(),
                           DestRRPid::comm:mypid(), IsInitiator::boolean(),
                           SkipIfEmpty::boolean()) -> Stats
    when is_subtype(Stats, rr_recon_stats:stats()).
send_resolve_request(Stats, [] = _ToSend, _OwnerL, _DestRRPid, _IsInitiator,
                     true = _SkipIfEmpty) ->
    ?TRACE("Resolve Session=~p ; ToSend=~p",
           [rr_recon_stats:get(session_id, Stats), 0]),
    Stats;
send_resolve_request(Stats, ToSend, OwnerL, DestRRPid, IsInitiator,
                     _SkipIfEmpty) ->
    SID = rr_recon_stats:get(session_id, Stats),
    ?TRACE("Resolve Session=~p ; ToSend=~p", [SID, length(ToSend)]),
    % note: the resolve request is counted at the initiator and
    %       thus from_my_node must be set accordingly on this node!
    ?DBG_ASSERT2(length(ToSend) =:= length(lists:usort(ToSend)),
                 {non_unique_send_list, ToSend}),
    send_local(OwnerL, {request_resolve, SID,
                        {key_upd_send, DestRRPid, ToSend, _ToReq = []},
                        [{from_my_node, ?IIF(IsInitiator, 1, 0)},
                         {feedback_request, comm:make_global(OwnerL)}]}),
    % key_upd_send + one reply from a subsequent feedback response (?key_upd)
    rr_recon_stats:inc([{rs_expected, 2}], Stats).

%% @doc Gets the number of bits needed to encode the given number.
-spec bits_for_number(Number::pos_integer()) -> pos_integer();
                     (0) -> 0.
bits_for_number(0) -> 0;
bits_for_number(Number) ->
    erlang:max(1, util:ceil(util:log2(Number + 1))).

%% @doc Splits P1E into N equal independent sub-processes and returns the P1E
%%      to use for each of these sub-processes: p_sub = 1 - (1 - p1e)^(1/n).
%%      This is based on p0e(total) = (1 - p1e(total)) = p0e(each)^n = (1 - p1e(each))^n.
-spec calc_n_subparts_p1e(N::number(), P1E::float()) -> P1E_sub::float().
calc_n_subparts_p1e(N, P1E) when N == 1 andalso P1E > 0 andalso P1E < 1 ->
    P1E;
calc_n_subparts_p1e(N, P1E) when P1E > 0 andalso P1E < 1 ->
%%     _VP = 1 - math:pow(1 - P1E, 1 / N).
    % BEWARE: we cannot use (1-p1E) since it is near 1 and its floating
    %         point representation is sub-optimal!
    % => use Taylor expansion of 1 - (1 - p1e)^(1/n)  at P1E = 0
    % http://www.wolframalpha.com/input/?i=Taylor+expansion+of+1+-+%281+-+p%29^%281%2Fn%29++at+p+%3D+0
    N2 = N * N, N3 = N2 * N, N4 = N3 * N, N5 = N4 * N,
    P1E2 = P1E * P1E, P1E3 = P1E2* P1E, P1E4 = P1E3 * P1E, P1E5 = P1E4 * P1E,
    _VP = P1E / N + (N - 1) * P1E2 / (2 * N2)
              + (2*N2 - 3*N + 1) * P1E3 / (6 * N3)
              + (6*N3 - 11*N2 + 6*N - 1) * P1E4 / (24 * N4)
              + (24*N4 - 50*N3 + 35*N2 - 10*N + 1) * P1E5 / (120 * N5). % +O[p^6]

%% @doc Splits P1E into N further (equal) independent sub-processes and returns
%%      the P1E to use for the next of these sub-processes with the previous
%%      sub-processes having a success probability of PrevP0 (a product of
%%      all (1-P1E_sub)).
%%      This is based on p0e(total) = (1 - p1e(total)) = p0e(each)^n = (1 - p1e(each))^n.
-spec calc_n_subparts_p1e(N::number(), P1E::float(), PrevP0::float())
        -> P1E_sub::float().
calc_n_subparts_p1e(N, P1E, 1.0) when N == 1 andalso P1E > 0 andalso P1E < 1 ->
    % special case with e.g. no items in the first/previous phase
    P1E;
calc_n_subparts_p1e(N, P1E, PrevP0E) when P1E > 0 andalso P1E < 1 andalso
                                             PrevP0E > 0 andalso PrevP0E =< 1 ->
    % http://www.wolframalpha.com/input/?i=Taylor+expansion+of+1+-+%28%281+-+p%29%2Fq%29^%281%2Fn%29++at+p+%3D+0
    N2 = N * N, N3 = N2 * N, N4 = N3 * N, N5 = N4 * N,
    P1E2 = P1E * P1E, P1E3 = P1E2* P1E, P1E4 = P1E3 * P1E, P1E5 = P1E4 * P1E,
    Q = math:pow(1 / PrevP0E, 1 / N),
    _VP = (1 - Q) + (P1E * Q) / N +
              ((N-1) * P1E2 * Q) / (2 * N2) +
              ((N-1) * (2 * N - 1) * P1E3 * Q) / (6 * N3) +
              ((N-1) * (2 * N - 1) * (3 * N - 1) * P1E4 * Q) / (24 * N4) +
              ((N-1) * (2 * N - 1) * (3 * N - 1) * (4 * N - 1) * P1E5 * Q) / (120 * N5). % +O[p^6]

%% @doc Calculates the signature sizes for comparing every item in Items
%%      (at most ItemCount) with OtherItemCount other items and expecting at
%%      most min(ItemCount, OtherItemCount) version comparisons.
%%      Sets the bit sizes to have an error below P1E.
-spec trivial_signature_sizes
        (ItemCount::non_neg_integer(), OtherItemCount::non_neg_integer(), P1E::float())
        -> {SigSize::signature_size(), VSize::signature_size()}.
trivial_signature_sizes(0, _, _P1E) ->
    {0, 0}; % invalid but since there are 0 items, this is ok!
trivial_signature_sizes(_, 0, _P1E) ->
    {0, 0}; % invalid but since there are 0 items, this is ok!
trivial_signature_sizes(ItemCount, OtherItemCount, P1E) ->
    VCompareCount = erlang:min(ItemCount, OtherItemCount),
    case get_min_version_bits() of
        variable ->
            % reduce P1E for the two parts here (key and version comparison)
            P1E_sub = calc_n_subparts_p1e(2, P1E),
            % cut off at 128 bit (rt_chord uses md5 - must be enough for all other RT implementations, too)
            SigSize0 = calc_signature_size_nm_pair(ItemCount, OtherItemCount, P1E_sub, 128),
            % note: we have n one-to-one comparisons
            VP = calc_n_subparts_p1e(erlang:max(1, VCompareCount), P1E_sub),
            VSize0 = min_max(util:ceil(util:log2(1 / VP)), 1, 128),
            ok;
        VSize0 ->
            SigSize0 = calc_signature_size_nm_pair(ItemCount, OtherItemCount, P1E, 128),
            ok
    end,
    Res = {_SigSize, _VSize} = align_bitsize(SigSize0, VSize0),
%%     log:pal("trivial [ ~p ] - P1E: ~p, \tSigSize: ~B, \tVSizeL: ~B~n"
%%             "MyIC: ~B, \tOtIC: ~B",
%%             [self(), P1E, _SigSize, _VSize, ItemCount, OtherItemCount]),
    Res.

%% @doc Calculates the worst-case failure probability of the trivial algorithm
%%      with the given signature size and item counts.
%%      NOTE: Precision loss may occur for very high values!
-spec trivial_worst_case_failprob(SigSize::signature_size(),
                                  ItemCount::non_neg_integer(),
                                  OtherItemCount::non_neg_integer()) -> float().
trivial_worst_case_failprob(0, 0, _OtherItemCount) ->
    % this is exact! (see special case in trivial_signature_sizes/3)
    0.0;
trivial_worst_case_failprob(0, _ItemCount, 0) ->
    % this is exact! (see special case in trivial_signature_sizes/3)
    0.0;
trivial_worst_case_failprob(SigSize, ItemCount, OtherItemCount) ->
    BK2 = util:pow(2, SigSize),
    % both solutions have their problems with floats near 1
    % -> use fastest as they are quite close
    % exact:
%%     1 - util:for_to_fold(1, (ItemCount + OtherItemCount) - 1,
%%                          fun(I) -> (1 - I / BK2) end,
%%                          fun erlang:'*'/2, 1).
    % approx:
    1 - math:exp(-((ItemCount + OtherItemCount) * (ItemCount + OtherItemCount - 1)
                       / 2) / BK2).

%% @doc Creates a compressed key-value list comparing every item in Items
%%      (at most ItemCount) with OtherItemCount other items and expecting at
%%      most min(ItemCount, OtherItemCount) version comparisons.
%%      Sets the bit sizes to have an error below P1E.
-spec compress_kv_list_p1e(Items::db_chunk_kv(), ItemCount::non_neg_integer(),
                           OtherItemCount::non_neg_integer(), P1E::float())
        -> {DBChunk::bitstring(), SigSize::signature_size(), VSize::signature_size()}.
compress_kv_list_p1e(DBItems, ItemCount, OtherItemCount, P1E) ->
    {SigSize, VSize} = trivial_signature_sizes(ItemCount, OtherItemCount, P1E),
    DBChunkBin = compress_kv_list(DBItems, <<>>, SigSize, VSize),
    % debug compressed and uncompressed sizes:
    ?TRACE("~B vs. ~B items, SigSize: ~B, VSize: ~B, ChunkSize: ~p / ~p bits",
            [ItemCount, OtherItemCount, SigSize, VSize, erlang:bit_size(DBChunkBin),
             erlang:bit_size(
                 erlang:term_to_binary(DBChunkBin,
                                       [{minor_version, 1}, {compressed, 2}]))]),
    {DBChunkBin, SigSize, VSize}.

%% @doc Calculates the signature size for comparing ItemCount items with
%%      OtherItemCount other items (including versions into the hashes).
%%      Sets the bit size to have an error below P1E.
-spec shash_signature_sizes
        (ItemCount::non_neg_integer(), OtherItemCount::non_neg_integer(), P1E::float())
        -> SigSize::signature_size().
shash_signature_sizes(0, _, _P1E) ->
    0; % invalid but since there are 0 items, this is ok!
shash_signature_sizes(_, 0, _P1E) ->
    0; % invalid but since there are 0 items, this is ok!
shash_signature_sizes(ItemCount, OtherItemCount, P1E) ->
    % reduce P1E for the two parts here (hash and trivial phases)
    P1E_sub = calc_n_subparts_p1e(2, P1E),
    % cut off at 128 bit (rt_chord uses md5 - must be enough for all other RT implementations, too)
    SigSize0 = calc_signature_size_nm_pair(ItemCount, OtherItemCount, P1E_sub, 128),
    % align if required
    Res = case config:read(rr_align_to_bytes) of
              false -> SigSize0;
              _     -> bloom:resize(SigSize0, 8)
          end,
%%     log:pal("shash [ ~p ] - P1E: ~p, \tSigSize: ~B, \tMyIC: ~B, \tOtIC: ~B",
%%             [self(), P1E, Res, ItemCount, OtherItemCount]),
    Res.

%% @doc Creates a compressed key-value list comparing every item in Items
%%      (at most ItemCount) with OtherItemCount other items (including versions
%%      into the hashes).
%%      Sets the bit size to have an error below P1E.
-spec shash_compress_k_list_p1e(Items::db_chunk_kv(), ItemCount::non_neg_integer(),
                                OtherItemCount::non_neg_integer(), P1E::float())
        -> {DBChunk::bitstring(), SigSize::signature_size()}.
shash_compress_k_list_p1e(DBItems, ItemCount, OtherItemCount, P1E) ->
    SigSize = shash_signature_sizes(ItemCount, OtherItemCount, P1E),
    DBChunkBin = shash_compress_kv_list(DBItems, <<>>, SigSize),
    % debug compressed and uncompressed sizes:
    ?TRACE("~B vs. ~B items, SigSize: ~B, ChunkSize: ~p / ~p bits",
            [ItemCount, OtherItemCount, SigSize, erlang:bit_size(DBChunkBin),
             erlang:bit_size(
                 erlang:term_to_binary(DBChunkBin,
                                       [{minor_version, 1}, {compressed, 2}]))]),
    {DBChunkBin, SigSize}.

%% @doc Aligns the two sizes so that their sum is a multiple of 8 in order to
%%      (possibly) achieve a better compression in binaries.
-spec align_bitsize(SigSize, VSize) -> {SigSize, VSize}
    when is_subtype(SigSize, pos_integer()),
         is_subtype(VSize, pos_integer()).
align_bitsize(SigSize0, VSize0) ->
    case config:read(rr_align_to_bytes) of
        false -> {SigSize0, VSize0};
        _     ->
            case get_min_version_bits() of
                variable ->
                    FullKVSize0 = SigSize0 + VSize0,
                    FullKVSize = bloom:resize(FullKVSize0, 8),
                    VSize = VSize0 + ((FullKVSize - FullKVSize0) div 2),
                    SigSize = FullKVSize - VSize,
                    {SigSize, VSize};
                _ ->
                    {bloom:resize(SigSize0, 8), VSize0}
            end
    end.

%% @doc Calculates the bloom FP, i.e. a single comparison's failure probability,
%%      assuming:
%%      * the other node executes MaxItems number of checks, too
%%      * the worst case, e.g. the other node has only items not in BF and
%%        we need to account for the false positive probability
%%      NOTE: reduces P1E for the two parts here (bloom and trivial RC)
-spec bloom_fp(MaxItems::non_neg_integer(), P1E::float()) -> float().
bloom_fp(MaxItems, P1E) ->
    P1E_sub = calc_n_subparts_p1e(2, P1E),
    1 - math:pow(1 - P1E_sub, 1 / erlang:max(MaxItems, 1)).

%% @doc Calculates the worst-case failure probability of the bloom algorithm
%%      with the Bloom filter and number of items to check inside the filter.
%%      NOTE: Precision loss may occur for very high values!
-spec bloom_worst_case_failprob(
        BF::bloom:bloom_filter(), ItemCount::non_neg_integer()) -> float().
bloom_worst_case_failprob(_BF, 0) ->
    0.0;
bloom_worst_case_failprob(BF, ItemCount) ->
    Fpr = bloom:get_property(BF, fpr),
    1 - math:pow(1 - Fpr, ItemCount).

-spec build_recon_struct(method(), DestI::intervals:non_empty_interval(),
                         db_chunk_kv(), Params::parameters() | {})
        -> {sync_struct(), P1E_p1::float()}.
build_recon_struct(trivial, I, DBItems, _Params) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
    ItemCount = length(DBItems),
    {DBChunkBin, SigSize, VSize} =
        compress_kv_list_p1e(DBItems, ItemCount, ItemCount, get_p1e()),
    {#trivial_recon_struct{interval = I, reconPid = comm:this(),
                           db_chunk = DBChunkBin,
                           sig_size = SigSize, ver_size = VSize},
     _P1E_p1 = trivial_worst_case_failprob(SigSize, ItemCount, ItemCount)};
build_recon_struct(shash, I, DBItems, _Params) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
    ItemCount = length(DBItems),
    P1E = get_p1e(),
    {DBChunkBin, SigSize} =
        shash_compress_k_list_p1e(DBItems, ItemCount, ItemCount, P1E),
    % NOTE: use left-over P1E after phase 1 (SHash) for phase 2 (trivial RC)
    P1E_p1 = trivial_worst_case_failprob(SigSize, ItemCount, ItemCount),
    P1E_p2 = calc_n_subparts_p1e(1, P1E, (1 - P1E_p1)),
    {#shash_recon_struct{interval = I, reconPid = comm:this(),
                         db_chunk = DBChunkBin, sig_size = SigSize,
                         p1e_p2 = P1E_p2},
     P1E_p1};
build_recon_struct(bloom, I, DBItems, _Params) ->
    % note: for bloom, parameters don't need to match (only one bloom filter at
    %       the non-initiator is created!) - use our own parameters
    ?DBG_ASSERT(not intervals:is_empty(I)),
    MaxItems = length(DBItems),
    P1E = get_p1e(),
    FP = bloom_fp(MaxItems, P1E),
    BF0 = bloom:new_fpr(MaxItems, FP, ?REP_HFS:new(bloom:calc_HF_numEx(MaxItems, FP))),
    BF = bloom:add_list(BF0, DBItems),
    {#bloom_recon_struct{interval = I, reconPid = comm:this(),
                         bf_bin = bloom:get_property(BF, filter),
                         item_count = MaxItems, p1e = P1E},
    % Note: we can only guess the number of items of the initiator here, so
    %       this is not exactly the P1E of phase 1!
     _P1E_p1 = bloom_worst_case_failprob(BF, MaxItems)};
build_recon_struct(merkle_tree, I, DBItems, Params) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
    P1E_p1 = 0.0, % needs to be set at the end of phase 1!
    case Params of
        {} ->
            % merkle_tree - at non-initiator!
            MOpts = [{branch_factor, get_merkle_branch_factor()},
                     {bucket_size, get_merkle_bucket_size()}],
            % do not build the real tree here - build during begin_sync so that
            % the initiator can start creating its struct earlier and in parallel
            % the actual build process is executed in begin_sync/3
            {merkle_tree:new(I, [{keep_bucket, true} | MOpts]), P1E_p1};
        #merkle_params{branch_factor = BranchFactor,
                       bucket_size = BucketSize,
                       num_trees = NumTrees} ->
            % merkle_tree - at initiator!
            MOpts = [{branch_factor, BranchFactor},
                     {bucket_size, BucketSize}],
            % build now
            RootsI = intervals:split(I, NumTrees),
            ICBList = merkle_tree:keys_to_intervals(DBItems, RootsI),
            {[merkle_tree:get_root(
               merkle_tree:new(SubI, Bucket, [{keep_bucket, true} | MOpts]))
               || {SubI, _Count, Bucket} <- ICBList], P1E_p1};
        #art_recon_struct{branch_factor = BranchFactor,
                          bucket_size = BucketSize} ->
            % ART at initiator
            MOpts = [{branch_factor, BranchFactor},
                     {bucket_size, BucketSize},
                     {leaf_hf, fun art:merkle_leaf_hf/2}],
            {merkle_tree:new(I, DBItems, [{keep_bucket, true} | MOpts]),
             P1E_p1 % TODO
            }
    end;
build_recon_struct(art, I, DBItems, _Params = {}) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
    BranchFactor = get_merkle_branch_factor(),
    BucketSize = merkle_tree:get_opt_bucket_size(length(DBItems), BranchFactor, 1),
    Tree = merkle_tree:new(I, DBItems, [{branch_factor, BranchFactor},
                                        {bucket_size, BucketSize},
                                        {leaf_hf, fun art:merkle_leaf_hf/2},
                                        {keep_bucket, true}]),
    % create art struct:
    {#art_recon_struct{art = art:new(Tree, get_art_config()),
                       branch_factor = BranchFactor,
                       bucket_size = BucketSize},
     _P1E_p1 = 0.0 % TODO
    }.

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
                     MaxItems::pos_integer() | all, create_struct | reconcile) -> ok when
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
                fun get_chunk_kv/1, MaxItems}).

-spec get_chunk_filter(db_entry:entry()) -> boolean().
get_chunk_filter(DBEntry) -> db_entry:get_version(DBEntry) =/= -1.
-spec get_chunk_kv(db_entry:entry()) -> {?RT:key(), client_version() | -1}.
get_chunk_kv(DBEntry) -> {db_entry:get_key(DBEntry), db_entry:get_version(DBEntry)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec exit_reason_to_rc_status(exit_reason()) -> rr_recon_stats:status().
exit_reason_to_rc_status(sync_finished) -> finish;
exit_reason_to_rc_status(sync_finished_remote) -> finish;
exit_reason_to_rc_status(_) -> abort.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Maps any key (K) into a given interval (I). If K is already in I, K is returned.
%%      If K has more than one associated key in I, the closest one is returned.
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

%% @doc Maps an abitrary key to its associated key in replication quadrant Q.
-spec map_key_to_quadrant(?RT:key(), rt_beh:segment()) -> ?RT:key().
map_key_to_quadrant(Key, Q) ->
    RKeys = ?RT:get_replica_keys(Key),
    map_rkeys_to_quadrant(RKeys, Q).

%% @doc Returns a key in the given replication quadrant Q from a list of
%%      replica keys.
-spec map_rkeys_to_quadrant([?RT:key(),...], rt_beh:segment()) -> ?RT:key().
map_rkeys_to_quadrant(RKeys, Q) ->
    SegM = case lists:member(?MINUS_INFINITY, RKeys) of
               true -> Q rem config:read(replication_factor) + 1;
               _ -> Q
           end,
    hd(lists:dropwhile(fun(K) -> ?RT:get_key_segment(K) =/= SegM end, RKeys)).

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
                              % this debug statement only holds for replication factors that are a power of 2:
%%                               ?DBG_ASSERT(?RT:get_range(LKeyX, ?IIF(RKeyX =:= ?MINUS_INFINITY, ?PLUS_INFINITY, RKeyX)) =:=
%%                                           ?RT:get_range(LKey, RKey0)),
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
    _ = gen_component:monitor(State#rr_recon_state.ownerPid),
    State.

-spec start(SessionId::rrepair:session_id(), SenderRRPid::comm:mypid())
        -> {ok, pid()}.
start(SessionId, SenderRRPid) ->
    State = #rr_recon_state{ ownerPid = self(),
                             dest_rr_pid = SenderRRPid,
                             stats = rr_recon_stats:new(SessionId) },
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
    config:cfg_is_in(rr_recon_method, [trivial, shash, bloom, merkle_tree, art]) andalso
        config:cfg_is_float(rr_recon_p1e) andalso
        config:cfg_is_greater_than(rr_recon_p1e, 0) andalso
        config:cfg_is_less_than_equal(rr_recon_p1e, 1) andalso
        config:cfg_test_and_error(rr_recon_version_bits,
                                  fun(variable) -> true;
                                     (X) -> erlang:is_integer(X) andalso X > 0
                                  end, "is not 'variable' or an integer > 0"),
        config:cfg_is_integer(rr_recon_min_sig_size) andalso
        config:cfg_is_greater_than(rr_recon_min_sig_size, 0) andalso
        config:cfg_is_integer(rr_merkle_branch_factor) andalso
        config:cfg_is_greater_than(rr_merkle_branch_factor, 1) andalso
        config:cfg_is_integer(rr_merkle_bucket_size) andalso
        config:cfg_is_greater_than(rr_merkle_bucket_size, 0) andalso
        config:cfg_is_integer(rr_merkle_num_trees) andalso
        config:cfg_is_greater_than(rr_merkle_num_trees, 0) andalso
        check_percent(rr_art_inner_fpr) andalso
        check_percent(rr_art_leaf_fpr) andalso
        config:cfg_is_integer(rr_art_correction_factor) andalso
        config:cfg_is_greater_than(rr_art_correction_factor, 0).

-spec get_p1e() -> float().
get_p1e() ->
    config:read(rr_recon_p1e).

%% @doc Use at least these many bits for compressed version numbers.
-spec get_min_version_bits() -> pos_integer() | variable.
get_min_version_bits() ->
    config:read(rr_recon_version_bits).

%% @doc Use at least these many bits for hashes.
-spec get_min_hash_bits() -> pos_integer().
get_min_hash_bits() ->
    config:read(rr_recon_min_sig_size).

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

%% @doc Merkle number of childs per inner node.
-spec get_merkle_num_trees() -> pos_integer().
get_merkle_num_trees() ->
    config:read(rr_merkle_num_trees).

%% @doc Merkle max items in a leaf node.
-spec get_merkle_bucket_size() -> pos_integer().
get_merkle_bucket_size() ->
    config:read(rr_merkle_bucket_size).

-spec get_art_config() -> art:config().
get_art_config() ->
    [{correction_factor, config:read(rr_art_correction_factor)},
     {inner_bf_fpr, config:read(rr_art_inner_fpr)},
     {leaf_bf_fpr, config:read(rr_art_leaf_fpr)}].
