% @copyright 2011-2016 Zuse Institute Berlin

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
%% @author Nico Kruber <kruber@zib.de>
%% @doc    replica reconcilication module
%% @end
%% @version $Id$
-module(rr_recon).
-author('malange@informatik.hu-berlin.de').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").
-include("client_types.hrl").

-export([init/1, on/2, start/2, check_config/0]).
-export([map_key_to_interval/2, map_key_to_quadrant/2, map_rkeys_to_quadrant/2,
         map_interval/2,
         quadrant_intervals/0]).
-export([get_chunk_kv/1, get_chunk_filter/1]).
%-export([compress_kv_list/5]).

%export for testing
-export([find_sync_interval/2, quadrant_subints_/3, key_dist/2]).
-export([merkle_compress_hashlist/4, merkle_decompress_hashlist/3]).
-export([pos_to_bitstring/4, bitstring_to_k_list_k/3, bitstring_to_k_list_kv/3]).
-export([calc_n_subparts_FR/2, calc_n_subparts_FR/3]).
%% -export([calc_max_different_hashes/3,
%%          trivial_signature_sizes/4, trivial_worst_case_failrate/4,
%%          shash_signature_sizes/4, shash_worst_case_failrate/4,
%%          calc_max_different_items_node/3, calc_max_different_items_total/3,
%%          bloom_target_fp/4, bloom_worst_case_failrate_/5]).
-export([tester_create_kvi_tree/1, tester_is_kvi_tree/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), log:pal("~w: [ ~.0p:~.0p ] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).
-define(TRACE_SEND(Pid, Msg), ?TRACE("to ~.0p:~.0p: ~.0p~n", [pid_groups:group_of(comm:make_local(comm:get_plain_pid(Pid))), Pid, Msg])).
-define(TRACE_RCV(Msg, State),
        ?TRACE("~n  Msg: ~.0p~n"
                 "State: method: ~.0p;  stage: ~.0p;  initiator: ~.0p~n"
                 "      syncI@I: ~.0p~n"
                 "       params: ~.0p~n",
               [Msg, State#rr_recon_state.method, State#rr_recon_state.stage,
                State#rr_recon_state.initiator, State#rr_recon_state.'sync_interval@I',
                ?IIF(is_list(State#rr_recon_state.struct), State#rr_recon_state.struct, [])])).
-define(ALG_DEBUG(X,Y), ok).
%-define(ALG_DEBUG(X,Y), log:pal("~w: [ ~.0p:~.0p ] " ++ X, [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).

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
-type kvi_tree()       :: mymaps:mymap(). % KeyShort::non_neg_integer() => {VersionShort::non_neg_integer(), Idx::non_neg_integer()}

-record(trivial_sync,
        {
         interval       = intervals:empty()                       :: intervals:interval(),
         reconPid       = undefined                               :: comm:mypid() | undefined,
         exp_delta      = ?required(trivial_sync, exp_delta)      :: number(),
         db_chunk       = ?required(trivial_sync, db_chunk)       :: {bitstring(), bitstring()} | {bitstring(), bitstring(), ResortedKV::db_chunk_kv(), Dupes::db_chunk_kv()}, % two binaries for transfer, the four-tuple only temporarily (locally)
         '+item_count'  = ?required(trivial_sync, '+item_count')  :: non_neg_integer(), % number of items not present in db_chunk
         sig_size       = ?required(trivial_sync, sig_size)       :: signature_size()
        }).

-record(shash_sync,
        {
         interval       = intervals:empty()                       :: intervals:interval(),
         reconPid       = undefined                               :: comm:mypid() | undefined,
         exp_delta      = ?required(shash_sync, exp_delta)        :: number(),
         db_chunk       = ?required(shash_sync, db_chunk)         :: bitstring() | {bitstring(), ResortedKV::db_chunk_kv(), Dupes::db_chunk_kv()}, % binary for transfer, the three-tuple only temporarily (locally)
         '+item_count'  = ?required(shash_sync, '+item_count')    :: non_neg_integer(), % number of items not present in db_chunk
         sig_size       = ?required(shash_sync, sig_size)         :: signature_size(),
         fail_rate      = ?required(shash_sync, fail_rate)        :: float()
        }).

-record(bloom_sync,
        {
         interval       = intervals:empty()                       :: intervals:interval(),
         reconPid       = undefined                               :: comm:mypid() | undefined,
         exp_delta      = ?required(bloom_sync, exp_delta)        :: number(),
         bf             = ?required(bloom_sync, bloom)            :: bitstring() | bloom:bloom_filter(), % bitstring for transfer, the full filter locally
         item_count     = ?required(bloom_sync, item_count)       :: non_neg_integer(),
         hf_count       = ?required(bloom_sync, hf_count)         :: pos_integer(),
         fail_rate      = ?required(bloom_sync, fail_rate)        :: float()
        }).

-record(merkle_params,
        {
         interval       = ?required(merkle_param, interval)       :: intervals:interval(),
         reconPid       = undefined                               :: comm:mypid() | undefined,
         exp_delta      = ?required(merkle_param, exp_delta)      :: number(),
         branch_factor  = ?required(merkle_param, branch_factor)  :: pos_integer(),
         num_trees      = ?required(merkle_param, num_trees)      :: pos_integer(),
         bucket_size    = ?required(merkle_param, bucket_size)    :: pos_integer(),
         fail_rate      = ?required(merkle_param, fail_rate)      :: float(),
         ni_item_count  = ?required(merkle_param, ni_item_count)  :: non_neg_integer(),
         ni_max_ic      = ?required(merkle_param, ni_max_ic)      :: non_neg_integer()
        }).

-record(art_recon_struct,
        {
         art            = ?required(art_recon_struct, art)            :: art:art(),
         reconPid       = undefined                                   :: comm:mypid() | undefined,
         branch_factor  = ?required(art_recon_struct, branch_factor)  :: pos_integer(),
         bucket_size    = ?required(art_recon_struct, bucket_size)    :: pos_integer()
        }).

-type sync_struct() :: #trivial_sync{} |
                       #shash_sync{} |
                       #bloom_sync{} |
                       merkle_tree:merkle_tree() |
                       [merkle_tree:mt_node()] |
                       #art_recon_struct{}.
-type parameters() :: #trivial_sync{} |
                      #shash_sync{} |
                      #bloom_sync{} |
                      #merkle_params{} |
                      #art_recon_struct{}.
-type recon_dest() :: ?RT:key() | random.

-type merkle_sync_rcv() ::
          {MyMaxItemsCount::non_neg_integer(),
           MyKVItems::merkle_tree:mt_bucket()}.
-type merkle_sync_send() ::
          {OtherMaxItemsCount::non_neg_integer(),
           MyKVItems::merkle_tree:mt_bucket()}.
-type merkle_sync_direct() ::
          % mismatches with an empty leaf on either node
          % -> these are resolved directly
          {MyKItems::[?RT:key()], MyLeafCount::non_neg_integer(), OtherNodeCount::non_neg_integer()}.
-type merkle_sync() :: {[merkle_sync_send()], [merkle_sync_rcv()],
                        SynRcvLeafCount::non_neg_integer(), merkle_sync_direct()}.

-record(rr_recon_state,
        {
         ownerPid           = ?required(rr_recon_state, ownerPid)    :: pid(),
         dest_rr_pid        = ?required(rr_recon_state, dest_rr_pid) :: comm:mypid(), %dest rrepair pid
         dest_recon_pid     = undefined                              :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                              :: method() | undefined,
         'sync_interval@I'  = intervals:empty()                      :: intervals:interval(),
         'max_items@I'      = undefined                              :: non_neg_integer() | undefined,
         params             = {}                                     :: parameters() | {}, % parameters from the other node
         struct             = {}                                     :: sync_struct() | {}, % my recon structure
         stage              = req_shared_interval                    :: stage(),
         initiator          = false                                  :: boolean(),
         merkle_sync        = {[], [], 0, {[], 0, 0}}                :: merkle_sync(),
         misc               = []                                     :: [{atom(), term()}], % any optional parameters an algorithm wants to keep
         kv_list            = []                                     :: db_chunk_kv() | [db_chunk_kv()], % list of KV chunks only temporarily when retrieving the DB in pieces
         k_list             = []                                     :: [?RT:key()],
         next_phase_kv      = []                                     :: db_chunk_kv(), % KV items that go directly into the next phase, e.g. items from colliding hashes (dupes)
         stats              = ?required(rr_recon_state, stats)       :: rr_recon_stats:stats()
         }).
-type state() :: #rr_recon_state{}.

% keep in sync with merkle_check_node/22
-define(recon_ok,                       1). % match
-define(recon_fail_stop_leaf,           2). % mismatch, sending node has leaf node
-define(recon_fail_stop_inner,          3). % mismatch, sending node has inner node
-define(recon_fail_cont_inner,          0). % mismatch, both inner nodes (continue!)

-type merkle_cmp_request() :: {Hash::merkle_tree:mt_node_key() | none, IsLeaf::boolean()}.

-type request() ::
    {start, method(), DestKey::recon_dest()} |
    {create_struct, method(), SenderI::intervals:interval(), SenderMaxItems::non_neg_integer()} | % from initiator
    {start_recon, bloom, #bloom_sync{}} | % to initiator
    {start_recon, merkle_tree, #merkle_params{}} | % to initiator
    {start_recon, art, #art_recon_struct{}}. % to initiator

-type message() ::
    % API
    request() |
    % trivial/shash/bloom sync messages
    {reconcile_req, DiffBFBin::bitstring(), OtherBFCount::non_neg_integer(),
     OtherDiffCount::non_neg_integer(), SenderPid::comm:mypid()} |
    {resolve_req, BinReqIdxPos::bitstring()} |
    {resolve_req, DBChunk::{bitstring(), bitstring()}, DupesCount::non_neg_integer(),
     Payload::any(), SigSize::signature_size(), SenderPid::comm:mypid()} |
    {resolve_req, shutdown, Payload::any()} |
    % merkle tree sync messages
    {?check_nodes, SenderPid::comm:mypid(), ToCheck::bitstring(), MaxItemsCount::non_neg_integer()} |
    {?check_nodes, ToCheck::bitstring(), MaxItemsCount::non_neg_integer()} |
    {?check_nodes_response, FlagsBin::bitstring(), MaxItemsCount::non_neg_integer()} |
    {resolve_req, HashesK::bitstring(), HashesV::bitstring()} |
    {resolve_req, BinKeyList::bitstring(), SyncSendFr_real::float()} |
    % dht node response
    {create_struct2, SenderI::intervals:interval(), {get_state_response, MyI::intervals:interval()}} |
    {process_db, {get_chunk_response, {intervals:interval(), db_chunk_kv()}}} |
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

on({create_struct, RMethod, SenderI, SenderMaxItems} = _Msg, State) ->
    ?TRACE_RCV(_Msg, State),
    % (first) request from initiator to create a sync struct
    This = comm:reply_as(comm:this(), 3, {create_struct2, SenderI, '_'}),
    comm:send_local(pid_groups:get_my(dht_node), {get_state, This, my_range}),
    State#rr_recon_state{method = RMethod, initiator = false,
                         'max_items@I' = SenderMaxItems};

on({create_struct2, SenderI, {get_state_response, MyI}} = _Msg,
   State = #rr_recon_state{stage = req_shared_interval, initiator = false,
                           method = RMethod,            dest_rr_pid = DestRRPid}) ->
    ?TRACE_RCV(_Msg, State),
    % target node got sync request, asked for its interval
    % dest_interval contains the interval of the initiator
    % -> client creates recon structure based on common interval, sends it to initiator
    SyncI = find_sync_interval(MyI, SenderI),
    case intervals:is_empty(SyncI) of
        false ->
            send_chunk_req(pid_groups:get_my(dht_node), self(),
                           SyncI, get_max_items()),
            case RMethod of
                art -> ok; % legacy (no integrated trivial sync yet)
                _   -> fd:subscribe(self(), [DestRRPid])
            end,
            % reduce SenderI to the sub-interval matching SyncI, i.e. a mapped SyncI
            SenderSyncI = map_interval(SenderI, SyncI),
            State#rr_recon_state{stage = build_struct,
                                 'sync_interval@I' = SenderSyncI};
        true ->
            shutdown(empty_interval, State)
    end;

on({process_db, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = build_struct,       initiator = false,
                           'sync_interval@I' = SenderSyncI}) ->
    ?TRACE_RCV(_Msg, State),
    % create recon structure based on all elements in sync interval
    DBList = [{Key, VersionX} || {KeyX, VersionX} <- DBList0,
                                 none =/= (Key = map_key_to_interval(KeyX, SenderSyncI))],
    build_struct(DBList, RestI, State);

on({start_recon, RMethod, Params} = _Msg,
   State = #rr_recon_state{dest_rr_pid = DestRRPid, misc = Misc}) ->
    ?TRACE_RCV(_Msg, State),
    % initiator got other node's sync struct or parameters over sync interval
    % (mapped to the initiator's range)
    % -> create own recon structure based on sync interval and reconcile
    % note: sync interval may be an outdated sub-interval of this node's range
    %       -> pay attention when saving values to DB!
    %       (it could be outdated then even if we retrieved the current range now!)
    case RMethod of
        trivial ->
            #trivial_sync{interval = MySyncI, reconPid = DestReconPid,
                          db_chunk = CKV, sig_size = SigSize} = Params,
            Params1 = Params#trivial_sync{db_chunk = {<<>>, <<>>}},
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            % convert db_chunk to a map for faster access checks
            {CKVTree, VSize} = decompress_kv_list(CKV, unknown, SigSize),
            ?DBG_ASSERT(Misc =:= []),
            Misc1 = [{db_chunk, CKVTree},
                     {ver_size, VSize}];
        shash ->
            #shash_sync{interval = MySyncI, reconPid = DestReconPid,
                        db_chunk = SH, sig_size = SigSize} = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            % convert db_chunk to a map for faster access checks
            {SHTree, 0} = decompress_kv_list({SH, <<>>}, unknown, SigSize),
            Params1 = Params#shash_sync{db_chunk = <<>>},
            ?DBG_ASSERT(Misc =:= []),
            Misc1 = [{db_chunk, SHTree}];
        bloom ->
            #bloom_sync{interval = MySyncI, reconPid = DestReconPid,
                        bf = BFBin, item_count = BFCount,
                        hf_count = HfCount} = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            ?DBG_ASSERT(Misc =:= []),
            BF = bloom:new_bin(BFBin, HfCount, BFCount),
            Params1 = Params#bloom_sync{bf = BF},
            Misc1 = [{item_count, 0},
                     {my_bf, bloom:new(erlang:max(1, erlang:bit_size(BFBin)), HfCount)}];
        merkle_tree ->
            #merkle_params{interval = MySyncI, reconPid = DestReconPid} = Params,
            Params1 = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            fd:subscribe(self(), [DestRRPid]),
            Misc1 = Misc;
        art ->
            #art_recon_struct{art = ART, reconPid = DestReconPid} = Params,
            MySyncI = art:get_interval(ART),
            Params1 = Params,
            ?DBG_ASSERT(DestReconPid =/= undefined),
            Misc1 = Misc
    end,
    % client only sends non-empty sync intervals or exits
    ?DBG_ASSERT(not intervals:is_empty(MySyncI)),
    
    send_chunk_req(pid_groups:get_my(dht_node), self(),
                   MySyncI, get_max_items()),
    State#rr_recon_state{stage = reconciliation, params = Params1,
                         method = RMethod, initiator = true,
                         'sync_interval@I' = MySyncI,
                         dest_recon_pid = DestReconPid,
                         misc = Misc1};

on({process_db, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,        initiator = true,
                           method = trivial,              kv_list = KVList}) ->
    ?TRACE_RCV(_Msg, State),
    % we need all items first and the process them further (just as on the non-initiator)
    % collect them in the 'kv_list' property of #rr_recon_state{}

    % if rest interval is non-empty get another chunk
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(pid_groups:get_my(dht_node), self(),
                          RestI, get_max_items()),
           State#rr_recon_state{kv_list = [DBList | KVList]};
       true ->
           #rr_recon_state{params = #trivial_sync{exp_delta = ExpDelta,
                                                  sig_size = SigSize,
                                                  '+item_count' = AddToChunkSize},
                           dest_rr_pid = DestRRPid, stats = Stats,
                           ownerPid = OwnerL,
                           misc = [{db_chunk, CKVTree},
                                   {ver_size, VSize}],
                           dest_recon_pid = DestReconPid} = State,
           FullKVList = lists:append([DBList | KVList]),
           MyDBSize1 = length(FullKVList),
           ChunkSize = mymaps:size(CKVTree),
           % identify items to send, request and the remaining (non-matched) DBChunk:
           % remove colliding hashes in DBList and add them to phase 2 (in turn,
           % colliding items in CKV are also added to phase 2 on the non-initiator)
           {ToSend, ToReqIdx, OtherDBChunk1, _Dupes} =
               get_diff_with_dupes(FullKVList, CKVTree, [], [], SigSize, VSize,
                                   fun get_full_diff/4),
           ItemCountNI_p1 = ChunkSize + AddToChunkSize,
           ?ALG_DEBUG("CheckCKV ~B+~Bckv vs. ~B+~Bcmp items",
                      [ChunkSize, AddToChunkSize, MyDBSize1 - length(_Dupes), length(_Dupes)]),
           ?DBG_ASSERT2(length(ToSend) =:= length(lists:usort(ToSend)),
                        {non_unique_send_list, ToSend, _Dupes}),
           % note: the actual acc(phase1) may be different from what the non-initiator expected
           Fr_p1_real = trivial_worst_case_failrate(
                          SigSize, MyDBSize1, ItemCountNI_p1, ExpDelta),
           Stats1  = rr_recon_stats:set([{fail_rate_p1, Fr_p1_real}], Stats),
           NewStats = send_resolve_request(Stats1, ToSend, OwnerL, DestRRPid,
                                           true, true),
           % let the non-initiator's rr_recon process identify the remaining keys
           ReqIdx = lists:usort([Idx || {_Version, Idx} <- mymaps:values(OtherDBChunk1)]
                                    ++ ToReqIdx),
           ToReq2 = compress_idx_list(ReqIdx, ChunkSize - 1, [], 0, 0, integrate_size),
           NewStats2 =
               if ReqIdx =/= [] orelse AddToChunkSize > 0 ->
                      % the non-initiator will use key_upd_send and we must thus increase
                      % the number of resolve processes here!
                      rr_recon_stats:inc([{rs_expected, 1}], NewStats);
                  true -> NewStats
               end,

           ?ALG_DEBUG("resolve_req Trivial~n  Session=~.0p ; ToReq=~p (~p bits)",
                      [rr_recon_stats:get(session_id, Stats1), length(ReqIdx),
                       erlang:bit_size(ToReq2)]),
           comm:send(DestReconPid, {resolve_req, ToReq2}),
           
           shutdown(sync_finished,
                    State#rr_recon_state{stats = NewStats2, misc = []})
    end;

on({process_db, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,    initiator = true,
                           method = shash,            kv_list = KVList}) ->
    ?TRACE_RCV(_Msg, State),
    % this is similar to the trivial sync above:
    % we need all items first and the process them further (just as on the non-initiator)
    % collect them in the 'kv_list' property of #rr_recon_state{}

    %if rest interval is non empty start another sync
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(pid_groups:get_my(dht_node), self(),
                          RestI, get_max_items()),
           State#rr_recon_state{kv_list = [DBList | KVList]};
       true ->
           #rr_recon_state{params = #shash_sync{exp_delta = ExpDelta,
                                                sig_size = SigSize,
                                                fail_rate = FR,
                                                '+item_count' = AddToChunkSize},
                           stats = Stats, misc = [{db_chunk, SHTree}]} = State,
           FullKVList = lists:append([DBList | KVList]),
           MyDBSize1 = length(FullKVList),
           ChunkSize = mymaps:size(SHTree),
           % identify differing items and the remaining (non-matched) DBChunk:
           % remove colliding hashes in DBList and add them to phase 2 (in turn,
           % colliding items in SH are also added to phase 2 on the non-initiator)
           {FullKVList1, Dupes0} =
               if FullKVList =:= [] -> {[], []};
                  true -> sort_extractdupes([{compress_key(KV, SigSize), KV}
                                             || KV <- FullKVList])
               end,
           ItemCountNI_p1 = ChunkSize + AddToChunkSize,
           ?ALG_DEBUG("CheckSH ~B+~Bckv vs. ~B+~Bcmp items",
                      [ChunkSize, AddToChunkSize, MyDBSize1 - length(Dupes0), length(Dupes0)]),
           {NewKVList, OtherDBChunk1} =
               shash_get_full_diff(FullKVList1, SHTree,
                                   [element(2, D) || D <- Dupes0], SigSize),
           % note: the actual fr(phase1) may be different from what the non-initiator expected
           Fr_p1_real =
               shash_worst_case_failrate(SigSize, MyDBSize1, ItemCountNI_p1, ExpDelta),
           FR_p2 = calc_n_subparts_FR(1, FR, Fr_p1_real),
           Stats1  = rr_recon_stats:set([{fail_rate_p1, Fr_p1_real}], Stats),
           CKidxSize = mymaps:size(OtherDBChunk1),
           ItemCountNI_p2 = CKidxSize + AddToChunkSize,
           ReqIdx = lists:usort([Idx || {_Version, Idx} <- mymaps:values(OtherDBChunk1)]),
           OtherDiffIdx = compress_idx_list(ReqIdx, ChunkSize - 1, [], 0, 0, integrate_size),
           phase2_run_trivial_on_diff(
             NewKVList, OtherDiffIdx, ItemCountNI_p1 =:= 0, FR_p2,
             ItemCountNI_p2,
             State#rr_recon_state{kv_list = [], stats = Stats1, misc = []})
    end;

on({process_db, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,    initiator = true,
                           method = bloom,            kv_list = KVList,
                           params = #bloom_sync{bf = BF} = Params,
                           dest_recon_pid = DestReconPid, stats = Stats,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           misc = [{item_count, MyDBSize}, {my_bf, MyBF}]}) ->
    ?TRACE_RCV(_Msg, State),
    % if rest interval is non empty start another sync
    % (do this early to allow some parallelism)
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(pid_groups:get_my(dht_node), self(),
                          RestI, get_max_items());
       true -> ok
    end,
    
    MyDBSize1 = MyDBSize + length(DBList),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    BFCount = bloom:item_count(BF),
    % get a subset of Delta without what is missing on this node:
    % NOTE: The only errors which might occur are bloom:filter_neg_and_add/3
    %       omitting items which are not on the other node (with the filter's
    %       false positive probability/rate). These are the items we would miss.
    {Diff, MyBF1} = bloom:filter_neg_and_add(BF, DBList, MyBF),
    NewKVList = lists:append(KVList, Diff),

    if not SyncFinished ->
           State#rr_recon_state{kv_list = NewKVList,
                                misc = [{item_count, MyDBSize1}, {my_bf, MyBF1}]};
       BFCount =:= 0 ->
           % in this case, the other node has already shut down
           % -> we need to send our diff (see phase2_run_trivial_on_diff/7)
           KList = [element(1, KV) || KV <- NewKVList],
           NewStats = send_resolve_request(
                        Stats, KList, OwnerL, DestRRPid, _IsInitiator = true, false),
           NewState = State#rr_recon_state{stage = resolve, kv_list = NewKVList,
                                           stats = NewStats},
           shutdown(sync_finished, NewState);
       true ->
           DiffBF = util:bin_xor(bloom:get_property(BF, filter),
                                 bloom:get_property(MyBF1, filter)),
           send(DestReconPid, {reconcile_req, DiffBF, MyDBSize1, length(NewKVList),
                               comm:this()}),
           % allow the garbage collector to free the original Bloom filter here
           Params1 = Params#bloom_sync{bf = <<>>},
           State#rr_recon_state{stage = resolve, params = Params1, kv_list = NewKVList}
    end;

on({process_db, {get_chunk_response, {RestI, DBList}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     initiator = true,
                           method = RMethod})
  when RMethod =:= merkle_tree orelse RMethod =:= art->
    ?TRACE_RCV(_Msg, State),
    build_struct(DBList, RestI, State);

on({fd_notify, crash, _Pid, _Reason} = _Msg, State) ->
    ?TRACE_RCV(_Msg, State),
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
%% trivial/shash/bloom/art reconciliation sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({resolve_req, shutdown, _Payload} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = RMethod})
  when (RMethod =:= shash orelse RMethod =:= art) ->
    ?DBG_ASSERT(?implies(RMethod =:= shash, _Payload =:= <<>>)),
    ?DBG_ASSERT(?implies(RMethod =:= art, _Payload =:= none)),
    shutdown(sync_finished, State);

on({resolve_req, shutdown, Fr_p1_real} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = true,
                           method = bloom,            stats = Stats}) ->
    % note: no real phase 2 since the other node has no items
    Stats1  = rr_recon_stats:set([{fail_rate_p1, Fr_p1_real},
                                  {fail_rate_p2, 0.0}], Stats),
    shutdown(sync_finished, State#rr_recon_state{stats = Stats1});

on({resolve_req, BinReqIdxPos} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = trivial,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           k_list = KList,            next_phase_kv = AdditionalItems,
                           stats = Stats}) ->
    ?TRACE_RCV(_Msg, State),
    ToSend0 = decompress_idx_to_list(BinReqIdxPos, KList),
    ToSend = [element(1, KV) || KV <- AdditionalItems] ++ ToSend0,
    NewStats =
        send_resolve_request(Stats, ToSend,
                             OwnerL, DestRRPid, _Initiator = false, true),
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats});

on({resolve_req, OtherDBChunk, DupesCount, MyDiffIdx, SigSize, DestReconPid} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = shash,            stats = Stats,
                           kv_list = KVList,          next_phase_kv = AdditionalItems}) ->
    ?TRACE_RCV(_Msg, State),
%%     log:pal("[ ~p ] CKIdx1: ~B (~B compressed)",
%%             [self(), erlang:byte_size(MyDiffIdx),
%%              erlang:byte_size(erlang:term_to_binary(MyDiffIdx, [compressed]))]),
    ?DBG_ASSERT(SigSize >= 0),

    {DBChunkTree, VSize} = decompress_kv_list(OtherDBChunk, unknown, SigSize),
    OrigDBChunkLen = mymaps:size(DBChunkTree),
    % worst case diff here is 100% since both nodes now operate on the
    % differences only and each node may have what the other node does not
    Fr_p2_real = trivial_worst_case_failrate(
                   % note: initiator and non-initiator are exchanged here (in phase 2)
                   SigSize, length(KVList), OrigDBChunkLen + DupesCount, 100),
    Stats1  = rr_recon_stats:set([{fail_rate_p2, Fr_p2_real}], Stats),

    MyDiffKV = AdditionalItems ++ decompress_idx_to_list(MyDiffIdx, KVList),
    % items not in CKidx cannot be in the diff! (except for the AdditionalItems)
    State1 = State#rr_recon_state{kv_list = MyDiffKV, stats = Stats1},
    NewStats2 = shash_bloom_perform_resolve(
                  State1, DBChunkTree, DupesCount, SigSize, VSize, DestReconPid,
                  fun get_full_diff/4),

    shutdown(sync_finished, State1#rr_recon_state{stats = NewStats2});

on({reconcile_req, DiffBFBin, OtherBFCount, OtherDiffCount, DestReconPid} = _Msg,
   State = #rr_recon_state{stage = reconciliation,    initiator = false,
                           method = bloom,            kv_list = KVList,
                           struct = #bloom_sync{bf = BF, hf_count = MyHfCount,
                                                exp_delta = ExpDelta,
                                                fail_rate = FR} = Struct,
                           stats = Stats}) ->
    ?TRACE_RCV(_Msg, State),
    
    OtherBF = bloom:new_bin(util:bin_xor(bloom:get_property(BF, filter),
                                         DiffBFBin), MyHfCount, OtherBFCount),
    % get a subset of Delta without what is missing on this node:
    % NOTE: The only errors which might occur are bloom:filter_neg/2
    %       omitting items which are not on the other node (with the filter's
    %       false positive probability/rate). These are the items we would miss.
    Diff = bloom:filter_neg(OtherBF, KVList),
    % allow the garbage collector to free the original Bloom filter
    Struct1 = Struct#bloom_sync{bf = <<>>},
    State1 = State#rr_recon_state{kv_list = Diff, struct = Struct1,
                                  dest_recon_pid = DestReconPid},
    
    % here, the failure probability is correct (in contrast to the
    % initiator) since we know the number of item checks and BF sizes of both
    % Bloom filters
    Fr_p1_real = bloom_worst_case_failrate(BF, OtherBF, ExpDelta),
    % NOTE: use left-over failure rate after phase 1 (bloom) for phase 2 (trivial RC)
    FR_p2 = calc_n_subparts_FR(1, FR, Fr_p1_real),
    ?ALG_DEBUG("I:~.0p, FR(phase1)=~p~n"
               "  Bloom1: m=~B k=~B BFCount=~B Checks=~B"
               "  Bloom2: m=~B k=~B BFCount=~B Checks=~B"
               "->  fr(phase1)=~p, FR(phase2)=~p",
               [State1#rr_recon_state.dest_recon_pid,
                calc_n_subparts_FR(2, FR),
                bloom:get_property(BF, size), bloom:get_property(BF, hfs_size),
                bloom:item_count(BF), OtherBFCount,
                bloom:get_property(OtherBF, size), bloom:get_property(OtherBF, hfs_size),
                bloom:item_count(OtherBF), length(KVList), Fr_p1_real, FR_p2]),
    Stats1 = rr_recon_stats:set([{fail_rate_p1, Fr_p1_real}], Stats),
    phase2_run_trivial_on_diff(
      Diff, Fr_p1_real, false, FR_p2, OtherDiffCount,
      State1#rr_recon_state{params = {}, stats = Stats1});

on({resolve_req, DBChunk, DupesCount, Fr_p1_real, SigSize, _DestReconPid} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = true,
                           method = bloom,            kv_list = KVList,
                           stats = Stats,
                           dest_recon_pid = DestReconPid}) ->
    ?TRACE_RCV(_Msg, State),
    
    {DBChunkTree, VSize} = decompress_kv_list(DBChunk, unknown, SigSize),
    OrigDBChunkLen = mymaps:size(DBChunkTree),
    % worst case diff here is 100% since both nodes now operate on the
    % differences only and each node may have what the other node does not
    Fr_p2_real = trivial_worst_case_failrate(
                   SigSize, length(KVList), OrigDBChunkLen + DupesCount, 100),
    Stats1  = rr_recon_stats:set([{fail_rate_p1, Fr_p1_real},
                                  {fail_rate_p2, Fr_p2_real}], Stats),
    State1 = State#rr_recon_state{stats = Stats1},
    NewStats2 = shash_bloom_perform_resolve(
                  State1, DBChunkTree, DupesCount, SigSize, VSize, DestReconPid,
                  fun get_full_diff/4),
    shutdown(sync_finished, State1#rr_recon_state{stats = NewStats2});

on({resolve_req, DBChunk, DupesCount, none, SigSize, DestReconPid} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           method = art}) ->
    ?TRACE_RCV(_Msg, State),
    
    {DBChunkTree, VSize} = decompress_kv_list(DBChunk, unknown, SigSize),

    NewStats2 = shash_bloom_perform_resolve(
                  State, DBChunkTree, DupesCount, SigSize, VSize, DestReconPid,
                  fun get_part_diff/4),
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats2});

on({resolve_req, BinReqIdxPos} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = IsInitiator,
                           method = RMethod,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           k_list = KList,            next_phase_kv = AdditionalItems,
                           stats = Stats,
                           misc = []})
  when (RMethod =:= bloom orelse RMethod =:= shash orelse RMethod =:= art) andalso
           ((RMethod =/= bloom) =:= IsInitiator) -> % non-initiator with Bloom, otherwise initiator
    ?TRACE_RCV(_Msg, State),
%%     log:pal("[ ~p ] CKIdx2: ~B (~B compressed)",
%%             [self(), erlang:byte_size(BinReqIdxPos),
%%              erlang:byte_size(erlang:term_to_binary(BinReqIdxPos, [compressed]))]),
    ToSend0 = bitstring_to_k_list_k(BinReqIdxPos, KList, []),
    ToSend = [element(1, KV) || KV <- AdditionalItems] ++ ToSend0,
    NewStats = send_resolve_request(
                 Stats, ToSend, OwnerL, DestRRPid, IsInitiator, true),
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats});

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({?check_nodes, SenderPid, ToCheck, OtherItemsCount, OtherMaxItemsCount},
   State = #rr_recon_state{params = #merkle_params{exp_delta = ExpDelta},
                           misc = [{fail_rate_target_per_node, LastFRPerNode},
                                   {fail_rate_target_phase1, FR_p1},
                                   {prev_used_fr, PrevUsedFr},
                                   {icount, MyMaxItemCount},
                                   {total_icount, MyItemCount}]}) ->
    % this is the first check_nodes message from the initiator and we finally
    % learn about the exact number of items to synchronise with
    MaxAffectedItems = calc_max_different_items_total(
                         MyItemCount, OtherItemsCount, ExpDelta),
    on({?check_nodes, ToCheck, OtherMaxItemsCount},
       State#rr_recon_state{dest_recon_pid = SenderPid,
                            misc = [{max_affected_items, MaxAffectedItems},
                                    {fail_rate_target_per_node, LastFRPerNode},
                                    {fail_rate_target_phase1, FR_p1},
                                    {prev_used_fr, PrevUsedFr},
                                    {icount, MyMaxItemCount}]});

on({?check_nodes, ToCheck0, OtherMaxItemsCount},
   State = #rr_recon_state{stage = reconciliation,    initiator = false,
                           method = merkle_tree,      merkle_sync = Sync,
                           params = #merkle_params{} = Params,
                           struct = TreeNodes,        stats = Stats,
                           dest_recon_pid = DestReconPid,
                           misc = [{max_affected_items, MaxAffectedItems},
                                   {fail_rate_target_per_node, LastFRPerNode},
                                   {fail_rate_target_phase1, FR_p1},
                                   {prev_used_fr, PrevUsedFr},
                                   {icount, MyLastMaxItemsCount}]}) ->
    ?DBG_ASSERT(comm:is_valid(DestReconPid)),
    {_FR_I, _FR_L, SigSizeI, SigSizeL, EffectiveFr_I, EffectiveFr_L} =
        merkle_next_signature_sizes(Params, LastFRPerNode, MyLastMaxItemsCount,
                                    OtherMaxItemsCount, MaxAffectedItems),
    ToCheck = merkle_decompress_hashlist(ToCheck0, SigSizeI, SigSizeL),
    {FlagsBin, RTree, SyncNew, NStats, MyMaxItemsCount,
     NextLvlNodesAct, HashCmpI, HashCmpL} =
        merkle_check_node(ToCheck, TreeNodes, SigSizeI, SigSizeL,
                          MyLastMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                          <<>>, [], [], [], 0, [], 0, 0, Sync, 0, 0, 0,
                          0, 0),
    UsedFr = merkle_calc_used_fr(PrevUsedFr, EffectiveFr_I,
                                 EffectiveFr_L, HashCmpI, HashCmpL),
    NewState = State#rr_recon_state{struct = RTree, merkle_sync = SyncNew,
                                    stats = NStats},
    ?ALG_DEBUG("merkle (NI)~n  fail_rate(p1): ~p -> ~p - NextNodes: ~B",
               [PrevUsedFr, UsedFr, NextLvlNodesAct]),
    send(DestReconPid, {?check_nodes_response, FlagsBin, MyMaxItemsCount}),
    
    if RTree =:= [] ->
           % start a (parallel) resolve (if items to resolve)
           merkle_resolve_leaves_send(NewState, UsedFr);
       true ->
           % calculate the remaining trees' target failure rate based on the
           % already used failure prob
           FRPerNode = calc_n_subparts_FR(NextLvlNodesAct, FR_p1, element(1, UsedFr)),
           NewState#rr_recon_state{misc = [{max_affected_items, MaxAffectedItems},
                                           {fail_rate_target_per_node, FRPerNode},
                                           {fail_rate_target_phase1, FR_p1},
                                           {prev_used_fr, UsedFr},
                                           {icount, MyMaxItemsCount}]}
    end;

on({?check_nodes_response, FlagsBin, OtherMaxItemsCount},
   State = #rr_recon_state{stage = reconciliation,        initiator = true,
                           method = merkle_tree,          merkle_sync = Sync,
                           params = #merkle_params{} = Params,
                           struct = TreeNodes,            stats = Stats,
                           dest_recon_pid = DestReconPid,
                           misc = [{signature_size, {SigSizeI, SigSizeL}},
                                   {max_affected_items, MaxAffectedItems},
                                   {fail_rate_target_per_node, {EffectiveFr_I, EffectiveFr_L}},
                                   {fail_rate_target_phase1, FR_p1},
                                   {prev_used_fr, PrevUsedFr},
                                   {icount, MyLastMaxItemsCount},
                                   {oicount, OtherLastMaxItemsCount}]}) ->
    {RTree, SyncNew, NStats, MyMaxItemsCount,
     NextLvlNodesAct, HashCmpI, HashCmpL} =
        merkle_cmp_result(FlagsBin, TreeNodes, SigSizeI, SigSizeL,
                          MyLastMaxItemsCount, OtherLastMaxItemsCount,
                          Sync, Params, Stats, [], [], [], 0, [], 0, 0, 0, 0, 0,
                          0, 0),
    UsedFr = merkle_calc_used_fr(PrevUsedFr, EffectiveFr_I,
                                 EffectiveFr_L, HashCmpI, HashCmpL),
    NewState = State#rr_recon_state{struct = RTree, merkle_sync = SyncNew,
                                    stats = NStats},
    ?ALG_DEBUG("merkle (I)~n  fail_rate(p1): ~p -> ~p - NextNodes: ~B",
               [PrevUsedFr, UsedFr, NextLvlNodesAct]),

    if RTree =:= [] ->
           % start a (parallel) resolve (if items to resolve)
           merkle_resolve_leaves_send(NewState, UsedFr);
       true ->
           % calculate the remaining trees' failure prob based on the already
           % used failure prob
           NextFRPerNode = calc_n_subparts_FR(NextLvlNodesAct, FR_p1, element(1, UsedFr)),
           {_FR_I, _FR_L, NextSigSizeI, NextSigSizeL, NextEffectiveFr_I, NextEffectiveFr_L} =
               merkle_next_signature_sizes(Params, NextFRPerNode, MyMaxItemsCount,
                                           OtherMaxItemsCount, MaxAffectedItems),
           Req = merkle_compress_hashlist(RTree, <<>>, NextSigSizeI, NextSigSizeL),
           send(DestReconPid, {?check_nodes, Req, MyMaxItemsCount}),
           NewState#rr_recon_state{misc = [{signature_size, {NextSigSizeI, NextSigSizeL}},
                                           {max_affected_items, MaxAffectedItems},
                                           {fail_rate_target_per_node, {NextEffectiveFr_I, NextEffectiveFr_L}},
                                           {fail_rate_target_phase1, FR_p1},
                                           {prev_used_fr, UsedFr},
                                           {icount, MyMaxItemsCount},
                                           {oicount, OtherMaxItemsCount}]}
    end;

on({resolve_req, SyncStruct_p2} = _Msg,
   State = #rr_recon_state{stage = resolve,           method = merkle_tree})
  when is_tuple(SyncStruct_p2) ->
    ?TRACE_RCV(_Msg, State),
    % NOTE: FIFO channels ensure that the {resolve_req, BinKeyList} is always
    %       received after the {resolve_req, Hashes} message from the other node!
    merkle_resolve_leaves_receive(State, SyncStruct_p2);

on({resolve_req, BinKeyList, SyncSendFr_real} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = IsInitiator,
                           method = merkle_tree,
                           merkle_sync = {SyncSend, [], SyncRcvLeafCount, DirectResolve},
                           params = Params,           next_phase_kv = ToSend,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           stats = Stats})
    % NOTE: FIFO channels ensure that the {resolve_req, BinKeyList} is always
    %       received after the {resolve_req, Hashes} message from the other node!
  when is_bitstring(BinKeyList) ->
    ?TRACE_RCV(_Msg, State),
    PrevFr_p2 = rr_recon_stats:get(fail_rate_p2, Stats), % from sync_receive
    NextFr_p2 = PrevFr_p2 + SyncSendFr_real,
    ?ALG_DEBUG("merkle (~s) - BinKeyListSize: ~B compressed~n"
               "  fail_rate(p2): ~p (rcv) + ~p (send) = ~p",
               [?IIF(IsInitiator, "I", "NI"),
                erlang:byte_size(
                  erlang:term_to_binary(BinKeyList, [compressed])),
                PrevFr_p2, SyncSendFr_real, NextFr_p2]),
    Stats1  = rr_recon_stats:set([{fail_rate_p2, NextFr_p2}], Stats),
    ToSendKeys = [K || {K, _V} <- ToSend],
    NStats = if BinKeyList =:= <<>> andalso ToSendKeys =:= [] ->
                    Stats1;
                BinKeyList =:= <<>> ->
                    merkle_resolve_leaves_ckidx(
                      [], BinKeyList, DestRRPid, Stats1, OwnerL,
                      Params, ToSendKeys, IsInitiator);
                true ->
                    merkle_resolve_leaves_ckidx(
                      SyncSend, BinKeyList, DestRRPid, Stats1, OwnerL,
                      Params, ToSendKeys, IsInitiator)
             end,
    NewState = State#rr_recon_state{merkle_sync = {[], [], SyncRcvLeafCount, DirectResolve},
                                    stats = NStats},
    shutdown(sync_finished, NewState).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_struct(DBList::db_chunk_kv(),
                   RestI::intervals:interval(), state()) -> state() | kill.
build_struct(DBList, RestI,
             State = #rr_recon_state{method = RMethod, params = Params,
                                     struct = {},
                                     initiator = Initiator, stats = Stats,
                                     kv_list = KVList,
                                     'sync_interval@I' = SyncI,
                                     'max_items@I' = InitiatorMaxItems}) ->
    ?DBG_ASSERT(?implies(RMethod =/= merkle_tree andalso RMethod =/= art,
                         not Initiator)),
    % note: RestI already is a sub-interval of the sync interval
    BeginSync = intervals:is_empty(RestI),
    NewKVList0 = [DBList | KVList],
    if BeginSync ->
           NewKVList = lists:append(lists:reverse(NewKVList0)),
           ToBuild = if Initiator andalso RMethod =:= art -> merkle_tree;
                        true -> RMethod
                     end,
           {BuildTime, {SyncStruct, Fr_p1}} =
               util:tc(
                 fun() -> build_recon_struct(ToBuild, SyncI, NewKVList,
                                             InitiatorMaxItems, Params)
                 end),
           Stats1 = rr_recon_stats:inc([{build_time, BuildTime}], Stats),
           Stats2  = rr_recon_stats:set([{fail_rate_p1, Fr_p1}], Stats1),
           NewState = State#rr_recon_state{struct = SyncStruct, stats = Stats2,
                                           kv_list = NewKVList},
           begin_sync(NewState#rr_recon_state{stage = reconciliation});
       true ->
           send_chunk_req(pid_groups:get_my(dht_node), self(),
                          RestI, get_max_items()),
           % keep stage (at initiator: reconciliation, at other: build_struct)
           State#rr_recon_state{kv_list = NewKVList0}
    end.

-spec begin_sync(state()) -> state() | kill.
begin_sync(State = #rr_recon_state{method = trivial, params = {}, initiator = false,
                                   struct = MySyncStruct,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid, kv_list = _KVList}) ->
    ?ALG_DEBUG("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    {KList, VList, ResortedKVOrigList, Dupes} = MySyncStruct#trivial_sync.db_chunk,
    MySyncStruct1 = MySyncStruct#trivial_sync{db_chunk = {KList, VList}},
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, trivial, MySyncStruct1}}),
    State#rr_recon_state{struct = {}, stage = resolve, kv_list = [],
                         k_list = [K || {K, _V} <- ResortedKVOrigList],
                         next_phase_kv = Dupes};
begin_sync(State = #rr_recon_state{method = shash, params = {}, initiator = false,
                                   struct = MySyncStruct,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid, kv_list = _KVList}) ->
    ?ALG_DEBUG("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    {KList, ResortedKVOrigList, Dupes} = MySyncStruct#shash_sync.db_chunk,
    MySyncStruct1 = MySyncStruct#shash_sync{db_chunk = KList},
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, shash, MySyncStruct1}}),
    if KList =:= <<>> andalso Dupes =:= [] -> % no items on this node
           shutdown(sync_finished, State#rr_recon_state{struct = {}, kv_list = []});
       true ->
           State#rr_recon_state{struct = {}, stage = resolve,
                                kv_list = ResortedKVOrigList,
                                next_phase_kv = Dupes}
    end;
begin_sync(State = #rr_recon_state{method = bloom, params = {}, initiator = false,
                                   struct = MySyncStruct,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?ALG_DEBUG("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    BFBin = bloom:get_property(MySyncStruct#bloom_sync.bf, filter),
    MySyncStruct1 = MySyncStruct#bloom_sync{bf = BFBin},
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, bloom, MySyncStruct1}}),
    case MySyncStruct#bloom_sync.item_count of
        0 -> shutdown(sync_finished, State#rr_recon_state{kv_list = []});
        _ -> State#rr_recon_state{stage = reconciliation}
    end;
begin_sync(State = #rr_recon_state{method = merkle_tree, params = {}, initiator = false,
                                   struct = MySyncStruct,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?ALG_DEBUG("BEGIN SYNC", []),
    % tell the initiator to create its struct first, and then build ours
    % (at this stage, we do not have any data in the merkle tree yet!)
    DBItems = State#rr_recon_state.kv_list,
    MerkleI = merkle_tree:get_interval(MySyncStruct),
    MerkleV = merkle_tree:get_branch_factor(MySyncStruct),
    MerkleB = merkle_tree:get_bucket_size(MySyncStruct),
    NumTrees = get_merkle_num_trees(),
    FRTotal = get_failure_rate(),
    FRTotal_p1 = calc_n_subparts_FR(2, FRTotal),
    
    % split interval first and create NumTrees merkle trees later
    {BuildTime1, ICBList} =
        util:tc(fun() ->
                        merkle_tree:keys_to_intervals(
                          DBItems, intervals:split(MerkleI, NumTrees))
                end),
    ItemCount = lists:sum([Count || {_SubI, Count, _Bucket} <- ICBList]),
    MaxItemCount = lists:max([0 | [Count || {_SubI, Count, _Bucket} <- ICBList]]),
    FRTotal_p1_perNode = calc_n_subparts_FR(NumTrees, FRTotal_p1),
    
    MySyncParams = #merkle_params{exp_delta = get_max_expected_delta(),
                                  interval = MerkleI,
                                  branch_factor = MerkleV,
                                  bucket_size = MerkleB,
                                  num_trees = NumTrees,
                                  fail_rate = FRTotal,
                                  ni_item_count = ItemCount,
                                  ni_max_ic = MaxItemCount},
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
    ?ALG_DEBUG("merkle (NI) - NextNodes: ~B~n"
               "  Inner/Leaf/EmptyLeaves/Items: ~p",
               [length(SyncStruct), MTSize]),
    
    State#rr_recon_state{struct = SyncStruct,
                         stats = Stats2, params = MySyncParams,
                         misc = [{fail_rate_target_per_node, FRTotal_p1_perNode},
                                 {fail_rate_target_phase1, FRTotal_p1},
                                 {prev_used_fr, {0.0, 0.0}},
                                 {icount, MaxItemCount},
                                 {total_icount, ItemCount}],
                         kv_list = []};
begin_sync(State = #rr_recon_state{method = merkle_tree, params = Params, initiator = true,
                                   struct = MySyncStruct, stats = Stats,
                                   dest_recon_pid = DestReconPid}) ->
    ?ALG_DEBUG("BEGIN SYNC", []),
    MTSize = merkle_tree:size_detail(MySyncStruct),
    Stats1 = rr_recon_stats:set([{tree_size, MTSize}], Stats),
    #merkle_params{fail_rate = FRTotal, num_trees = NumTrees, exp_delta = ExpDelta,
                   ni_item_count = OtherItemsCount, ni_max_ic = OtherMaxItemsCount} = Params,
    MyItemCount =
        lists:sum([merkle_tree:get_item_count(Node) || Node <- MySyncStruct]),
    MyMaxItemCount =
        lists:max([0 | [merkle_tree:get_item_count(Node) || Node <- MySyncStruct]]),
    ?ALG_DEBUG("merkle (I) - NextNodes: ~B~n"
               "  Inner/Leaf/EmptyLeaves/Items: ~p",
               [length(MySyncStruct), MTSize]),
    FRTotal_p1 = calc_n_subparts_FR(2, FRTotal),
    FRTotal_p1_perNode = calc_n_subparts_FR(NumTrees, FRTotal_p1),
    MaxAffectedItems = calc_max_different_items_total(
                         MyItemCount, OtherItemsCount, ExpDelta),
    
    {_FR_I, _FR_L, NextSigSizeI, NextSigSizeL, EffectiveFr_I, EffectiveFr_L} =
        merkle_next_signature_sizes(Params, FRTotal_p1_perNode, MyMaxItemCount,
                                    OtherMaxItemsCount, MaxAffectedItems),
    
    Req = merkle_compress_hashlist(MySyncStruct, <<>>, NextSigSizeI, NextSigSizeL),
    send(DestReconPid, {?check_nodes, comm:this(), Req, MyItemCount, MyMaxItemCount}),
    State#rr_recon_state{stats = Stats1,
                         misc = [{signature_size, {NextSigSizeI, NextSigSizeL}},
                                 {max_affected_items, MaxAffectedItems},
                                 {fail_rate_target_per_node, {EffectiveFr_I, EffectiveFr_L}},
                                 {fail_rate_target_phase1, FRTotal_p1},
                                 {prev_used_fr, {0.0, 0.0}},
                                 {icount, MyMaxItemCount},
                                 {oicount, OtherMaxItemsCount}],
                         kv_list = []};
begin_sync(State = #rr_recon_state{method = art, params = {}, initiator = false,
                                   struct = MySyncStruct,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_rr_pid = DestRRPid}) ->
    ?ALG_DEBUG("BEGIN SYNC", []),
    SID = rr_recon_stats:get(session_id, Stats),
    send(DestRRPid, {continue_recon, comm:make_global(OwnerL), SID,
                     {start_recon, art, MySyncStruct}}),
    case art:get_property(MySyncStruct#art_recon_struct.art, items_count) of
        0 -> shutdown(sync_finished, State#rr_recon_state{kv_list = []});
        _ -> State#rr_recon_state{struct = {}, stage = resolve}
    end;
begin_sync(State = #rr_recon_state{method = art, params = Params, initiator = true,
                                   struct = MySyncStruct, stats = Stats,
                                   dest_recon_pid = DestReconPid}) ->
    ?ALG_DEBUG("BEGIN SYNC", []),
    ART = Params#art_recon_struct.art,
    Stats1 = rr_recon_stats:set(
               [{tree_size, merkle_tree:size_detail(MySyncStruct)}], Stats),
    OtherItemCount = art:get_property(ART, items_count),
    case merkle_tree:get_interval(MySyncStruct) =:= art:get_interval(ART) of
        true ->
            {ASyncLeafs, NComp, NSkip, NLSync} =
                art_get_sync_leaves([merkle_tree:get_root(MySyncStruct)], ART,
                                    [], 0, 0, 0),
            Diff = lists:append([merkle_tree:get_bucket(N) || N <- ASyncLeafs]),
            MyItemCount = merkle_tree:get_item_count(MySyncStruct),
            FRTotal = get_failure_rate(),
            % TODO: correctly calculate the probabilities and select appropriate parameters beforehand
            LeafBf = art:get_property(ART, leaf_bf),
            LeafBfFpr = bloom:get_property(LeafBf, fpr),
            NrChecksNotInBF =
                calc_max_different_items_node(
                  bloom:get_property(LeafBf, items_count),
                  MyItemCount, get_max_expected_delta()),
            EffectiveFr_p1 = NrChecksNotInBF * LeafBfFpr,
            FR_p2 =
                if EffectiveFr_p1 > FRTotal ->
                       log:log("~w: [ ~.0p:~.0p ] FR constraint broken (phase 1 overstepped?)~n"
                               "  continuing with smallest possible failure rate",
                               [?MODULE, pid_groups:my_groupname(), self()]),
                       1.0e-16;
                   true ->
                       % NOTE: use left-over failure rate after phase 1 (ART) for phase 2 (trivial RC)
                       calc_n_subparts_FR(1, FRTotal, EffectiveFr_p1)
                end,
            Stats2  = rr_recon_stats:set([{fail_rate_p1, EffectiveFr_p1}], Stats1),
            
            Stats3 = rr_recon_stats:inc([{tree_nodesCompared, NComp},
                                         {tree_compareSkipped, NSkip},
                                         {tree_leavesSynced, NLSync}], Stats2),
            
            phase2_run_trivial_on_diff(Diff, none, OtherItemCount =:= 0,
                                       FR_p2, OtherItemCount,
                                       State#rr_recon_state{stats = Stats3});
        false when OtherItemCount =/= 0 ->
            % must send resolve_req message for the non-initiator to shut down
            send(DestReconPid, {resolve_req, shutdown, none}),
            shutdown(sync_finished, State#rr_recon_state{stats = Stats1,
                                                         kv_list = []});
        false ->
            shutdown(sync_finished, State#rr_recon_state{stats = Stats1,
                                                         kv_list = []})
    end.

-spec shutdown(exit_reason(), state()) -> kill.
shutdown(Reason, #rr_recon_state{ownerPid = OwnerL, stats = Stats,
                                 initiator = Initiator, dest_rr_pid = DestRR,
                                 dest_recon_pid = DestRC, method = RMethod,
                                 'sync_interval@I' = SyncI}) ->
    ?ALG_DEBUG("SHUTDOWN~n  Session=~.0p Reason=~p",
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

%% @doc Helper calculating 1 - (1 - Z)^X.
-spec calc_one_m_xpow_one_m_z(X::number(), Z::float()) -> float().
calc_one_m_xpow_one_m_z(X, _Z) when X == 0 ->
    0.0;
calc_one_m_xpow_one_m_z(_X, Z) when Z == 0 ->
    0.0;
calc_one_m_xpow_one_m_z(_X, Z) when Z == 1 ->
    1.0;
calc_one_m_xpow_one_m_z(X, Z) when Z < 1.0e-8 ->
    % BEWARE: we cannot use (1-z) since it is near 1 and its floating
    %         point representation is sub-optimal!
    % => use Taylor expansion of 1 - (1 - z)^x  at z = 0
    % http://www.wolframalpha.com/input/?i=Taylor+expansion+of+1+-+(1+-+z)%5Ex++at+z+%3D+0
    X2 = X * X, X3 = X2 * X, X4 = X3 * X, X5 = X4 * X,
    Z2 = Z * Z, Z3 = Z2* Z, Z4 = Z3 * Z, Z5 = Z4 * Z,
    % small terms first:
%%     Z5 * (X5 - 10*X4 + 35*X3 - 50*X2 + 24*X) / 120
%%         + Z4 * (X4 - 6*X3 + 11*X2 - 6*X) / 24
%%         + Z3 * (X3 - 3*X2 + 2*X) / 6
%%         + Z2 * (X2 - X) / 2 + Z * X; % +O[p^6]
    {S5, C5} = util:kahan_sum([X5, -10*X4, 35*X3, -50*X2, 24*X], 0.0, 0.0),
    {S4, C4} = util:kahan_sum([X4, -6*X3, 11*X2, -6*X], 0.0, 0.0),
    {S3, C3} = util:kahan_sum([X3, -3*X2, 2*X], 0.0, 0.0),
    {S2, C2} = util:kahan_sum([X2, -X], 0.0, 0.0),
    
    {Sum, _C} = util:kahan_sum(
                  [Z5 * C5 / 120, Z4 * C4 / 24, Z3 * C3 / 6, Z2 * C2 / 2,
                   Z5 * S5 / 120, Z4 * S4 / 24, Z3 * S3 / 6, Z2 * S2 / 2,
                   Z * X % +O[z^6]
                  ], 0.0, 0.0),
    Sum;
calc_one_m_xpow_one_m_z(X, Z) ->
    1 - math:pow(1 - Z, X).

%% @doc Helper for calculating the maximum number of different hashes when
%%      an upper bound on the delta is known, ignoring cases where it is
%%      known to be wrong based on N and M.
-spec calc_max_different_hashes_(N::non_neg_integer(), M::non_neg_integer(),
                                 ExpDelta::number()) -> non_neg_integer().
calc_max_different_hashes_(N, M, ExpDelta) when ExpDelta >= 0 andalso ExpDelta =< 100 ->
    if ExpDelta == 0 ->
           % M and N may differ anyway if the actual delta is higher
           % -> target no collisions among items on any node!
           erlang:max(M, N);
       ExpDelta == 100 ->
           M + N; % special case of the ones below
       is_float(ExpDelta) ->
           % assume the worst case, i.e. ExpDelta percent different hashes
           % on both nodes together due to missing items (out-dated items have
           % the same key!), and thus:
           % N = NT * (100 - ExpDelta * alpha) / 100 and
           % M = NT * (100 - ExpDelta * (1-alpha)) / 100
           util:ceil(((M + N) * 100) / (200 - ExpDelta));
       is_integer(ExpDelta) ->
           % -> use integer division (and round up) for higher precision:
           ((M + N) * 100 + 200 - ExpDelta - 1) div (200 - ExpDelta)
    end.

%% @doc Helper for calculating the maximum number of different hashes when
%%      an upper bound on the delta is known. This method also handles cases
%%      where ExpDelta is known to be wrong and sets the minimum known value
%%      after printing a warning.
-spec calc_max_different_hashes(N::non_neg_integer(), M::non_neg_integer(),
                                ExpDelta::number()) -> non_neg_integer().
calc_max_different_hashes(N, M, ExpDelta) when ExpDelta >= 0 andalso ExpDelta =< 100 ->
    Differences0 = calc_max_different_hashes_(N, M, ExpDelta),
    % in case ExpDelta is wrong, at least |N-M| items must differ and are missing on one node
    MinDiff = erlang:abs(N - M),
    if MinDiff =< Differences0 ->
           Differences0;
       true ->
           log:log("~w: [ ~.0p:~.0p ] expected delta ~B must be wrong,~n"
                   "  continuing with the minimum number of differences we know: ~B",
                   [?MODULE, pid_groups:my_groupname(), self(), Differences0, MinDiff]),
           MinDiff
    end.

%% @doc Calculates the maximum number of different items on both nodes (with N
%%      and M items each) which may differ, i.e. which may not exist on either
%%      node when an upper bound on the delta is known.
-spec calc_max_different_items_total(N::non_neg_integer(), M::non_neg_integer(),
                               ExpDelta::number()) -> non_neg_integer().
calc_max_different_items_total(N, M, ExpDelta) ->
    % the worst case with the maximal number of item checks is when
    % this node has all ExpDelta items missing
    % -> calculate the (expected) original item count
    % -> have ExpDelta percent different items on either node
    % (if items are only outdated, there are ExpDelta differences from (N+M)/2
    % items which is less than the worst case above)

    % note: avoid duplicate warnings if ExpDelta is wrong - if it is wrong here,
    %       we do not need to fix MaxItems because the actual Differences are
    %       set below either way:
    MaxItems = calc_max_different_hashes_(N, M, ExpDelta),
    Differences0 =
        if ExpDelta == 0   -> 0;
           ExpDelta == 100 -> MaxItems; % special case of the one below
           is_float(ExpDelta) ->
               % worst case: we have all the ExpDelta percent items the other node does not have
               util:ceil(MaxItems * ExpDelta / 100);
           is_integer(ExpDelta) ->
               % -> use integer division (and round up) for higher precision:
               (MaxItems * ExpDelta + 99) div 100
        end,
    % in case ExpDelta is wrong, at least |N-M| items must differ and are missing on one node
    MinDiff = erlang:abs(N - M),
    Differences =
        if MinDiff =< Differences0 ->
               Differences0;
           true ->
               log:log("~w: [ ~.0p:~.0p ] expected delta ~B must be wrong,~n"
                       "  continuing with the minimum number of differences we know: ~B",
                       [?MODULE, pid_groups:my_groupname(), self(), Differences0, MinDiff]),
               MinDiff
        end,
%%     log:pal("[ ~p ] MaxItems: ~B Differences: ~B -> ~B", [self(), MaxItems, Differences0, Differences]),
    Differences.

%% @doc Calculates the maximum number of items on the first node (N items)
%%      which do not exist on the second node (M items) when an upper bound
%%      on the delta is known. Note that the result is limited by N which
%%      is the maximum number of differences on the first node.
%% @see calc_max_different_items_total/3
-spec calc_max_different_items_node(N::non_neg_integer(), M::non_neg_integer(),
                               ExpDelta::number()) -> non_neg_integer().
calc_max_different_items_node(N, M, ExpDelta) ->
    erlang:min(calc_max_different_items_total(N, M, ExpDelta), N).

-spec calc_items_in_chunk(DBChunk::bitstring(), BitsPerItem::non_neg_integer())
-> NrItems::non_neg_integer().
calc_items_in_chunk(<<>>, 0) -> 0;
calc_items_in_chunk(DBChunk, BitsPerItem) ->
    ?ASSERT(erlang:bit_size(DBChunk) rem BitsPerItem =:= 0),
    erlang:bit_size(DBChunk) div BitsPerItem.

%% @doc Sorts the given list of tuples, and extracts duplicates (non-sorted)
%%      based on the first tuple element.
-spec sort_extractdupes(List::[Tuple,...])
        -> {SortedList::[Tuple], Dupes::[Tuple]}
   when is_subtype(Tuple, {term()} | {term(), term()} | {term(), term(), term()}).
sort_extractdupes([_ | _] = List) ->
    SortedList0 = lists:sort(List),
    % detect and remove duplicates:
    {Last, LastDup, Rest, Dupes0} =
        lists:foldl(fun(X, {LastX, _, RestX, Dupes})
                         when element(1, X) =:= element(1, LastX) ->
                            {X, dup, RestX, [LastX | Dupes]};
                       (X, {LastX, nodup, RestX, Dupes}) ->
                            {X, nodup, [LastX | RestX], Dupes};
                       (X, {LastX, dup, RestX, Dupes}) ->
                            {X, nodup, RestX, [LastX | Dupes]}
                    end, {hd(SortedList0), nodup, [], []}, tl(SortedList0)),
    if LastDup =:= nodup ->
           {lists:reverse(Rest, [Last]), Dupes0};
       true ->
           {lists:reverse(Rest),
            % this is reversed but the order does not matter for duplicates:
            [Last | Dupes0]}
    end.

%% @doc Transforms a list of key and version tuples (with unique keys), into a
%%      compact binary representation for transfer.
-spec compress_kv_list
        (KVList::db_chunk_kv(), SigSize, VSize::signature_size(),
         KeyComprFun::fun(({?RT:key(), client_version()}, SigSize) -> bitstring()),
         integrate_size)
    -> {KeyDiff::Bin, VBin::Bin, ResortedKOrigList::db_chunk_kv(), Dupes::db_chunk_kv()};
        (KVList::db_chunk_kv(), SigSize, VSize::signature_size(),
         KeyComprFun::fun(({?RT:key(), client_version()}, SigSize) -> bitstring()),
         return_size)
    -> {{KeyDiff::Bin, DeltaSigSizeBits::non_neg_integer(), DeltaSigSize::non_neg_integer()},
        VBin::Bin, ResortedKOrigList::db_chunk_kv(), Dupes::db_chunk_kv()}
    when is_subtype(Bin, bitstring()),
         is_subtype(SigSize, signature_size()).
compress_kv_list([_ | _], 0, 0, _KeyComprFun, integrate_size) ->
    {<<>>, <<>>, [], []};
compress_kv_list([_ | _], 0, 0, _KeyComprFun, return_size) ->
    {{<<>>, 0, 0}, <<>>, [], []};
compress_kv_list([_ | _] = KVList, SigSize, VSize, KeyComprFun, SizeOption) ->
    {SortedKVList, Dupes0} =
        sort_extractdupes([{KeyComprFun(X, SigSize), V, X}
                           || X = {_K0, V} <- KVList]),
    {KList, VList, KV0List} = lists:unzip3(SortedKVList),
    DiffKStruct = compress_idx_list(KList, util:pow(2, SigSize) - 1, [], 0, 0, SizeOption),
    DiffVBin = if VSize =:= 0 -> <<>>;
                  true -> lists:foldl(fun(V, Acc) ->
                                              <<Acc/bitstring, V:VSize>>
                                      end, <<>>, VList)
               end,
    Dupes = [element(3, D) || D <- Dupes0],
    {DiffKStruct, DiffVBin, KV0List, Dupes};
compress_kv_list([], _SigSize, _VSize, _KeyComprFun, integrate_size) ->
    {<<>>, <<>>, [], []};
compress_kv_list([], _SigSize, _VSize, _KeyComprFun, return_size) ->
    {{<<>>, 0, 0}, <<>>, [], []}.

%% @doc De-compresses the binary from compress_kv_list/5 into a map with a
%%      binary key representation and the integer of the (shortened) version.
-spec decompress_kv_list(CompressedBin::{KeyDiff::bitstring(), VBin::bitstring()},
                         DeltaSigSize::signature_size() | unknown,
                         SigSize::signature_size())
        -> {ResTree::kvi_tree(), VSize::signature_size()}.
decompress_kv_list({<<>>, <<>>}, _DeltaSigSize, _SigSize) ->
    {mymaps:new(), 0};
decompress_kv_list({KeyDiff, VBin}, DeltaSigSize, SigSize) ->
    {KList, KListLen} = decompress_idx_list(KeyDiff, DeltaSigSize, util:pow(2, SigSize) - 1),
    VBinSize = erlang:bit_size(VBin),
    VSize = VBinSize div KListLen,
    ?ASSERT(VBinSize rem KListLen =:= 0),
    {<<>>, Res, _} =
        lists:foldl(
          fun(CurKeyX, {<<Version:VSize, T/bitstring>>, AccX, CurPosX}) ->
                  {T, [{CurKeyX, {Version, CurPosX}} | AccX], CurPosX + 1}
          end, {VBin, [], 0}, KList),
    KVMap = mymaps:from_list(Res),
    % hashes, i.e. compressed keys, should be unique in KeyDiff!
    ?DBG_ASSERT(mymaps:size(KVMap) =:= KListLen),
    {KVMap, VSize}.

%% @doc Wrapper for GetDiffFun which also extracts items with colliding
%%      hashes in MyEntries. These are added to the items to send and since
%%      they are not matched with MyIOtherKvTree, colliding items there remain. 
%% @see get_full_diff/4
%% @see get_part_diff/4
-spec get_diff_with_dupes(
        MyEntries::db_chunk_kv(), MyIOtherKvTree::kvi_tree(),
        AccFBItems::[?RT:key()], AccReqItems::[non_neg_integer()],
        SigSize::signature_size(), VSize::signature_size(),
        GetDiffFun::fun((MyEntries::[EntryWithHash], MyIOtherKvTree::kvi_tree(),
                         AccFBItems::[?RT:key()], AccReqItems::[non_neg_integer()])
                       -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
                           MyIOtherKvTree::kvi_tree()}))
        -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
            MyIOtherKvTree::kvi_tree(), Dupes::[EntryWithHash]}
    when is_subtype(EntryWithHash, {HashedKey::non_neg_integer(), Key::?RT:key(), VShort::integer()}).
get_diff_with_dupes([], MyIOtKvTree, FBItems, ReqItemsIdx, _SigSize, _VSize, _GetDiffFun) ->
    {FBItems, ReqItemsIdx, MyIOtKvTree, []};
get_diff_with_dupes([_ | _] = KVList, MyIOtKvTree, FBItems, ReqItemsIdx, SigSize, VSize, GetDiffFun) ->
    VMod = util:pow(2, VSize),
    {KVList1, Dupes} =
        sort_extractdupes([begin
                               {KeyShort, VersionShort} =
                                   compress_kv_pair(Key, Version, SigSize, VMod),
                               {KeyShort, Key, VersionShort}
                           end || {Key, Version} <- KVList]),
    {ToSend1, ToReqIdx1, OtherDBChunk1} =
        GetDiffFun(KVList1, MyIOtKvTree,
                   [element(2, D) || D <- Dupes] ++ FBItems, ReqItemsIdx),
    {ToSend1, ToReqIdx1, OtherDBChunk1, Dupes}.

%% @doc Gets all entries from MyEntries which are not encoded in MyIOtherKvTree
%%      or the entry in MyEntries has a newer version than the one in the tree
%%      and returns them as FBItems. ReqItems contains items in the tree but
%%      where the version in MyEntries is older than the one in the tree.
%% @see get_part_diff/4
-spec get_full_diff(MyEntries::[{HashedKey::non_neg_integer(), Key::?RT:key(), VShort::integer()}],
                    MyIOtherKvTree::kvi_tree(), AccFBItems::[?RT:key()],
                    AccReqItems::[non_neg_integer()])
        -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
            MyIOtherKvTree::kvi_tree()}.
get_full_diff([], MyIOtKvTree, FBItems, ReqItemsIdx) ->
    {FBItems, ReqItemsIdx, MyIOtKvTree};
get_full_diff([{KeyShort, Key, VersionShort} | Rest], MyIOtKvTree, FBItems, ReqItemsIdx) ->
    case mymaps:find(KeyShort, MyIOtKvTree) of
        error ->
            get_full_diff(Rest, MyIOtKvTree, [Key | FBItems], ReqItemsIdx);
        {ok, {OtherVersionShort, Idx}} ->
            MyIOtKvTree2 = mymaps:remove(KeyShort, MyIOtKvTree),
            if VersionShort > OtherVersionShort ->
                   get_full_diff(Rest, MyIOtKvTree2, [Key | FBItems], ReqItemsIdx);
               VersionShort =:= OtherVersionShort ->
                   get_full_diff(Rest, MyIOtKvTree2, FBItems, ReqItemsIdx);
               true -> % VersionShort < OtherVersionShort
                   get_full_diff(Rest, MyIOtKvTree2, FBItems, [Idx | ReqItemsIdx])
            end
    end.

%% @doc Gets all entries from MyEntries which are in MyIOtherKvTree
%%      and the entry in MyEntries has a newer version than the one in the tree
%%      and returns them as FBItems. ReqItems contains items in the tree but
%%      where the version in MyEntries is older than the one in the tree.
%% @see get_full_diff/4
-spec get_part_diff(MyEntries::[{HashedKey::non_neg_integer(), Key::?RT:key(), VShort::integer()}],
                    MyIOtherKvTree::kvi_tree(), AccFBItems::[?RT:key()],
                    AccReqItems::[non_neg_integer()])
        -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
            MyIOtherKvTree::kvi_tree()}.
get_part_diff([], MyIOtKvTree, FBItems, ReqItemsIdx) ->
    {FBItems, ReqItemsIdx, MyIOtKvTree};
get_part_diff([{KeyShort, Key, VersionShort} | Rest], MyIOtKvTree, FBItems, ReqItemsIdx) ->
    case mymaps:find(KeyShort, MyIOtKvTree) of
        error ->
            get_part_diff(Rest, MyIOtKvTree, FBItems, ReqItemsIdx);
        {ok, {OtherVersionShort, Idx}} ->
            MyIOtKvTree2 = mymaps:remove(KeyShort, MyIOtKvTree),
            if VersionShort > OtherVersionShort ->
                   get_part_diff(Rest, MyIOtKvTree2, [Key | FBItems], ReqItemsIdx);
               VersionShort =:= OtherVersionShort ->
                   get_part_diff(Rest, MyIOtKvTree2, FBItems, ReqItemsIdx);
               true ->
                   get_part_diff(Rest, MyIOtKvTree2, FBItems, [Idx | ReqItemsIdx])
            end
    end.

%% @doc Transforms a single key and version into compact representations
%%      based on the given size and VMod, respectively.
%% @see compress_kv_list/5.
-spec compress_kv_pair(Key::?RT:key(), Version::client_version(),
                        SigSize::signature_size(), VMod::pos_integer())
        -> {KeyShort::non_neg_integer(), VersionShort::integer()}.
compress_kv_pair(Key, Version, SigSize, VMod) ->
    {compress_key(Key, SigSize), Version rem VMod}.

%% @doc Transforms a key or a KV-tuple into a compact binary representation
%%      based on the given size.
%% @see compress_kv_list/5.
-spec compress_key(Key::?RT:key() | {Key::?RT:key(), Version::client_version()},
                   SigSize::signature_size()) -> KeyShort::non_neg_integer().
compress_key(Key, SigSize) ->
    KBin = erlang:md5(erlang:term_to_binary(Key)),
    RestSize = erlang:bit_size(KBin) - SigSize,
    % return an integer based on the last SigSize bits:
    if RestSize >= 0  ->
           <<_:RestSize/bitstring, KeyShort:SigSize/integer-unit:1>> = KBin,
           KeyShort;
       true ->
           FillSize = -RestSize,
           <<KeyShort:SigSize/integer-unit:1>> = <<0:FillSize, KBin/binary>>,
           KeyShort
    end.

%% @doc Transforms a key from a KV-tuple into a compact binary representation
%%      based on the given size.
%% @see compress_kv_list/5.
-spec trivial_compress_key({Key::?RT:key(), Version::client_version()},
                   SigSize::signature_size()) -> KeyShort::non_neg_integer().
trivial_compress_key(KV, SigSize) ->
    compress_key(element(1, KV), SigSize).

%% @doc Creates a compressed version of a (key-)position list.
%%      MaxPosBound represents an upper bound on the biggest value in the list;
%%      when decoding, the same bound must be known!
-spec compress_idx_list
        (SortedIdxList::[non_neg_integer()], MaxPosBound::non_neg_integer(),
         ResultIdx::[non_neg_integer()], LastPos::non_neg_integer(),
         Max::non_neg_integer(), integrate_size)
    -> CompressedIndices::bitstring();
        (SortedIdxList::[non_neg_integer()], MaxPosBound::non_neg_integer(),
         ResultIdx::[non_neg_integer()], LastPos::non_neg_integer(),
         Max::non_neg_integer(), return_size)
    -> {CompressedIndices::bitstring(), DeltaSigSizeBits::non_neg_integer(),
        DeltaSigSize::non_neg_integer()}.
compress_idx_list([Pos | Rest], MaxPosBound, AccResult, LastPos, Max, SizeOption) ->
    CurIdx0 = Pos - LastPos,
    % need a positive value to encode:
    CurIdx = if CurIdx0 >= 0 -> CurIdx0;
                true -> Mod = MaxPosBound + 1,
                        ((CurIdx0 rem Mod) + Mod) rem Mod
             end,
    compress_idx_list(Rest, MaxPosBound, [CurIdx | AccResult], Pos + 1,
                      erlang:max(CurIdx, Max), SizeOption);
compress_idx_list([], MaxPosBound, AccResult, _LastPos, Max, SizeOption) ->
    IdxSize = if Max =:= 0 -> 1;
                 true      -> bits_for_number(Max)
              end,
    Bin = lists:foldr(fun(Pos, Acc) ->
                              <<Acc/bitstring, Pos:IdxSize/integer-unit:1>>
                      end, <<>>, AccResult),
    if Bin =/= <<>> andalso SizeOption =:= integrate_size ->
           IdxBitsSize = bits_for_number(bits_for_number(MaxPosBound)),
           <<IdxSize:IdxBitsSize/integer-unit:1, Bin/bitstring>>;
       Bin =/= <<>> andalso SizeOption =:= return_size ->
           IdxBitsSize = bits_for_number(bits_for_number(MaxPosBound)),
           {Bin, IdxBitsSize, IdxSize};
       Bin =:= <<>> andalso SizeOption =:= integrate_size ->
           Bin;
       Bin =:= <<>> andalso SizeOption =:= return_size ->
           {Bin, 0, 0}
    end.

%% @doc De-compresses a bitstring with indices from compress_idx_list/5
%%      into a list of indices encoded by that function.
-spec decompress_idx_list(CompressedBin::bitstring(),
                          DeltaSigSize::signature_size() | unknown,
                          MaxPosBound::non_neg_integer())
        -> {[non_neg_integer()], Count::non_neg_integer()}.
decompress_idx_list(<<>>, _, _MaxPosBound) ->
    {[], 0};
decompress_idx_list(Bin, unknown, MaxPosBound) ->
    IdxBitsSize = bits_for_number(bits_for_number(MaxPosBound)),
    <<SigSize0:IdxBitsSize/integer-unit:1, Bin2/bitstring>> = Bin,
    decompress_idx_list(Bin2, SigSize0, MaxPosBound);
decompress_idx_list(Bin, DeltaSigSize, MaxPosBound) ->
    SigSize = erlang:max(1, DeltaSigSize),
    Count = calc_items_in_chunk(Bin, SigSize),
    IdxList = decompress_idx_list_(Bin, 0, SigSize, MaxPosBound + 1),
    ?DBG_ASSERT(Count =:= length(IdxList)),
    {IdxList, Count}.

%% @doc Helper for decompress_idx_list/3.
-spec decompress_idx_list_(CompressedBin::bitstring(), LastPos::non_neg_integer(),
                           SigSize::signature_size(), Mod::pos_integer())
        -> ResKeys::[non_neg_integer()].
decompress_idx_list_(<<>>, _LastPos, _SigSize, _Mod) ->
    [];
decompress_idx_list_(Bin, LastPos, SigSize, Mod) ->
    <<Diff:SigSize/integer-unit:1, T/bitstring>> = Bin,
    CurPos = (LastPos + Diff) rem Mod,
    [CurPos | decompress_idx_list_(T, CurPos + 1, SigSize, Mod)].

%% @doc De-compresses a bitstring with indices from compress_idx_list/5
%%      into the encoded sub-list of the original list.
%%      NOTE: in contrast to decompress_idx_list/3 (which is used for
%%            decompressing KV lists as well), we do not support duplicates
%%            in the original list fed into compress_idx_list/5 and will fail
%%            during the decode!
-spec decompress_idx_to_list(CompressedBin::bitstring(), [X]) -> [X].
decompress_idx_to_list(<<>>, _List) ->
    [];
decompress_idx_to_list(Bin, List) ->
    IdxBitsSize = bits_for_number(bits_for_number(length(List) - 1)),
    <<SigSize0:IdxBitsSize/integer-unit:1, Bin2/bitstring>> = Bin,
    SigSize = erlang:max(1, SigSize0),
    decompress_idx_to_list_(Bin2, List, SigSize).

%% @doc Helper for decompress_idx_to_list/2.
-spec decompress_idx_to_list_(CompressedBin::bitstring(), [X],
                              SigSize::signature_size()) -> [X].
decompress_idx_to_list_(<<>>, _, _SigSize) ->
    [];
decompress_idx_to_list_(Bin, List, SigSize) ->
    <<KeyPosInc:SigSize/integer-unit:1, T/bitstring>> = Bin,
    % note: this fails if there have been duplicates in the original list or
    %       KeyPosInc was negative!
    [X | List2] = lists:nthtail(KeyPosInc, List),
    [X | decompress_idx_to_list_(T, List2, SigSize)].

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
%% @see pos_to_bitstring/4
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
%% @see pos_to_bitstring/4
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

%% @doc Gets all entries from MyEntries which are not encoded in MyIOtKvSet.
%%      Also returns the tree with all these matches removed.
-spec shash_get_full_diff(KVList::[{HashedKey::non_neg_integer(),
                                    {Key::?RT:key(), Version::client_version()}}],
                          MyIOtherKvTree::kvi_tree(),
                          AccDiff::KVList, SigSize::signature_size())
        -> {Diff::KVList, MyIOtherKvTree::kvi_tree()}
    when is_subtype(KVList, db_chunk_kv()).
shash_get_full_diff([], MyIOtKvSet, AccDiff, _SigSize) ->
    {AccDiff, MyIOtKvSet};
shash_get_full_diff([{CurKey, KV} | Rest], MyIOtKvSet, AccDiff, SigSize) ->
    OldSize = mymaps:size(MyIOtKvSet),
    MyIOtKvSet2 = mymaps:remove(CurKey, MyIOtKvSet),
    case mymaps:size(MyIOtKvSet2) of
        OldSize ->
            shash_get_full_diff(Rest, MyIOtKvSet2, [KV | AccDiff], SigSize);
        _ ->
            shash_get_full_diff(Rest, MyIOtKvSet2, AccDiff, SigSize)
    end.

%% @doc Part of the resolve_req message processing of the SHash, Bloom, ART RC
%%      processes in phase 2 (trivial RC) at the initiator for Bloom and at
%%      the non-initiator for the other protocols.
-spec shash_bloom_perform_resolve(
        State::state(), DBChunkTree::kvi_tree(), DupesCount::non_neg_integer(),
        SigSize::signature_size(), VSize::signature_size(),
        DestReconPid::comm:mypid(),
        GetDiffFun::fun((MyEntries::[{HashedKey::non_neg_integer(), Key::?RT:key(), VShort::integer()}],
                         MyIOtherKvTree::kvi_tree(),
                         AccFBItems::[?RT:key()], AccReqItems::[non_neg_integer()])
                       -> {FBItems::[?RT:key()], ReqItemsIdx::[non_neg_integer()],
                           MyIOtherKvTree::kvi_tree()}))
        -> rr_recon_stats:stats().
shash_bloom_perform_resolve(
  #rr_recon_state{dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                  kv_list = KVList,          stats = Stats,
                  method = _RMethod,         initiator = IsInitiator},
  DBChunkTree, DupesCount, SigSize, VSize, DestReconPid, GetDiffFun) ->
    {ToSendKeys1, ToReqIdx1, DBChunkTree1, _Dupes} =
        get_diff_with_dupes(KVList, DBChunkTree, [], [], SigSize, VSize,
                            GetDiffFun),
    ?ALG_DEBUG("CheckCKV ~B+~Bckv vs. ~B+~Bcmp items",
               [mymaps:size(DBChunkTree), DupesCount,
                length(KVList) - length(_Dupes), length(_Dupes)]),

    NewStats1 = send_resolve_request(Stats, ToSendKeys1, OwnerL, DestRRPid,
                                     IsInitiator, false),

    % let the initiator's rr_recon process identify the remaining keys
    ReqIdx = lists:usort([Idx || {_Version, Idx} <- mymaps:values(DBChunkTree1)] ++ ToReqIdx1),
    ToReq2 = erlang:list_to_bitstring(
               lists:reverse(
                 pos_to_bitstring(% note: ReqIdx positions start with 0
                   ReqIdx, [], 0, ?IIF(ReqIdx =:= [], 0, lists:last(ReqIdx) + 1)))),
    ?ALG_DEBUG("resolve_req ~s~n  Session=~.0p ; ToReq= ~p bytes",
               [_RMethod, rr_recon_stats:get(session_id, NewStats1), erlang:byte_size(ToReq2)]),
    comm:send(DestReconPid, {resolve_req, ToReq2}),
    % the other node will use key_upd_send and we must thus increase
    % the number of resolve processes here!
    if ReqIdx =/= [] orelse DupesCount > 0 ->
           rr_recon_stats:inc([{rs_expected, 1}], NewStats1);
       true -> NewStats1
    end.

%% @doc Sets up a phase2 trivial synchronisation on the identified differences
%%      where the mapping of the differences is not clear yet and only the
%%      current node knows them.
%%      NOTE: Payload is only sent if the other node has not shutdown yet!
-spec phase2_run_trivial_on_diff(
  UnidentifiedDiff::db_chunk_kv(), Payload::any(), OtherHasShutdown::boolean(),
  FR_p2::float(), OtherCmpItemCount::non_neg_integer(), State::state())
        -> NewState::state().
phase2_run_trivial_on_diff(
  UnidentifiedDiff, Payload, OtherHasShutdown, FR_p2,
  OtherCmpItemCount, % number of items the other nodes compares CKV entries with
  State = #rr_recon_state{stats = Stats, dest_recon_pid = DestReconPid,
                          initiator = IsInitiator,
                          dest_rr_pid = DestRRPid, ownerPid = OwnerL}) ->
    CKVItems = UnidentifiedDiff,
    CKVSize = length(CKVItems),
    StartResolve = CKVSize + OtherCmpItemCount > 0,
    ?ALG_DEBUG("Reconcile SHash/Bloom/ART~n  Session=~.0p ; Phase2=~B vs. ~B",
               [rr_recon_stats:get(session_id, Stats), CKVSize, OtherCmpItemCount]),
    if StartResolve andalso not OtherHasShutdown ->
           % send idx of non-matching other items & KV-List of my diff items
           % start resolve similar to a trivial recon but using the full diff!
           % (as if non-initiator in trivial recon)
           ExpDelta = 100, % the sets at I and NI may be distinct (in the worst-case)
           {BuildTime, {MyDiffK, MyDiffV, ResortedKVOrigList, Dupes, SigSizeT, _VSizeT}} =
               util:tc(fun() ->
                               compress_kv_list_fr(
                                 CKVItems, OtherCmpItemCount, CKVSize, ExpDelta, FR_p2,
                                 fun trivial_signature_sizes/4, fun trivial_compress_key/2)
                       end),
           Fr_p2_real = trivial_worst_case_failrate(
                          SigSizeT, OtherCmpItemCount, CKVSize, ExpDelta),

           send(DestReconPid,
                {resolve_req, {MyDiffK, MyDiffV}, _DupesCount = length(Dupes),
                 Payload, SigSizeT, comm:this()}),
           % the other node will use key_upd_send and we must thus increase
           % the number of resolve processes here!
           NewStats1 = rr_recon_stats:inc([{rs_expected, 1},
                                           {build_time, BuildTime}], Stats),
           NewStats  = rr_recon_stats:set([{fail_rate_p2, Fr_p2_real}], NewStats1),
           KList = [element(1, KV) || KV <- ResortedKVOrigList],
           State#rr_recon_state{stats = NewStats, stage = resolve,
                                kv_list = [], k_list = KList,
                                next_phase_kv = Dupes, misc = []};
       OtherHasShutdown ->
           % no need to send resolve_req message - the non-initiator already shut down
           % the other node does not have any items but there may be a diff at our node!
           % start a resolve here:
           KList = [element(1, KV) || KV <- CKVItems],
           NewStats = send_resolve_request(
                        Stats, KList, OwnerL, DestRRPid, IsInitiator, true),
           NewState = State#rr_recon_state{stats = NewStats, stage = resolve},
           shutdown(sync_finished, NewState);
       true -> % not OtherHasShutdown, CKVSize =:= 0
           % must send resolve_req message for the non-initiator to shut down
           send(DestReconPid, {resolve_req, shutdown, Payload}),
           shutdown(sync_finished, State)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculates from a total failure rate target FR the (next) failure rate
%%      targets to use for signature and sub-tree reconciliations.
-spec merkle_next_fr_targets(BranchFactor::pos_integer(), FRPerNode::float())
    -> {FR_I::float(), FR_L::float()}.
merkle_next_fr_targets(BranchFactor, FRPerNode) ->
    % mistakes caused by:
    % inner node: current node or any of its BranchFactor children (B=BranchFactor+1)
    % leaf node: current node only (B=1) and thus FRPerNode
    FR_I = calc_n_subparts_FR(BranchFactor + 1, FRPerNode),
    FR_L = FRPerNode,
%%     log:pal("merkle [ ~p ]~n  FRPerNode: ~p, \tFR_I: ~p, \tFR_L: ~p",
%%             [self(), FRPerNode, FR_I, FR_L]),
    {FR_I, FR_L}.

%% @doc Calculates the new signature sizes based on the next failure targets
%%      as in merkle_next_fr_targets/2
-spec merkle_next_signature_sizes(
        Params::#merkle_params{}, FRPerNode::float(),
        MyMaxItemsCount::non_neg_integer(), OtherMaxItemsCount::non_neg_integer(),
        MaxAffectedItems::non_neg_integer())
    -> {FR_I::float(), FR_L::float(),
        NextSigSizeI::signature_size(), NextSigSizeL::signature_size(),
        EffectiveFr_I::float(), EffectiveFr_L::float()}.
merkle_next_signature_sizes(
  #merkle_params{bucket_size = BucketSize, branch_factor = BranchFactor},
  FRPerNode, MyMaxItemsCount, OtherMaxItemsCount, MaxAffectedItems) ->
    {FR_I, FR_L} = merkle_next_fr_targets(BranchFactor, FRPerNode),

    % note: we need to use the same failure rate for this level's signature
    %       comparison as a children's tree has in total!
    if MaxAffectedItems =:= 0 ->
           NextSigSizeL = NextSigSizeI = get_min_hash_bits(),
           _AffectedItemsI = _AffectedItemsL = 0,
           EffectiveFr_L = EffectiveFr_I = float(0 / util:pow(2, NextSigSizeI));
       MyMaxItemsCount =/= 0 andalso OtherMaxItemsCount =/= 0 ->
           _AffectedItemsI =
               AffectedItemsI = erlang:min(MyMaxItemsCount + OtherMaxItemsCount,
                                           MaxAffectedItems),
           NextSigSizeI = min_max(util:ceil(util:log2(AffectedItemsI / FR_I)),
                                  get_min_hash_bits(), 160),
           EffectiveFr_I = float(AffectedItemsI / util:pow(2, NextSigSizeI)),
           _AffectedItemsL =
               AffectedItemsL = lists:min([MyMaxItemsCount + OtherMaxItemsCount,
                                           2 * BucketSize, MaxAffectedItems]),
           NextSigSizeL = min_max(util:ceil(util:log2(AffectedItemsL / FR_L)),
                                  get_min_hash_bits(), 160),
           EffectiveFr_L = float(AffectedItemsL / util:pow(2, NextSigSizeL));
       true ->
           % should only occur during the reconciliation initiation in begin_sync/1
           NextSigSizeL = NextSigSizeI = 0,
           _AffectedItemsI = _AffectedItemsL = 0,
           EffectiveFr_L = EffectiveFr_I = 0.0
    end,

    ?ALG_DEBUG("merkle - signatures~n  MyMI: ~B,\tOtMI: ~B,\tMaxAffected: ~B,\tMaxAffectedI: ~B,\tMaxAffectedL: ~B~n"
               "  FR_I: ~g,\tFR_L: ~g,\tSigSizeI: ~B,\tSigSizeL: ~B~n"
               "  -> eff. FR_I: ~g,\teff. FR_L: ~g",
               [MyMaxItemsCount, OtherMaxItemsCount, MaxAffectedItems, _AffectedItemsI, _AffectedItemsL,
                FR_I, FR_L, NextSigSizeI, NextSigSizeL,
                EffectiveFr_I, EffectiveFr_L]),
    {FR_I, FR_L, NextSigSizeI, NextSigSizeL, EffectiveFr_I, EffectiveFr_L}.

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
        SyncAccRcvLeafCount::Count,
        MySyncAccDRK::[?RT:key()], MySyncAccDRLCount::Count, OtherSyncAccDRLCount::Count,
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
                  SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                  MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                  {SyncInSend, SyncInRcv, SyncInRcvLeafCount,
                   {MySyncInDRK, MySyncInDRLCount, OtherSyncInDRLCount}},
                  AccCmp, AccSkip, NextLvlNodesActIN,
                  HashCmpI_IN, HashCmpL_IN) ->
    NStats = rr_recon_stats:inc([{tree_nodesCompared, AccCmp},
                                 {tree_compareSkipped, AccSkip}], Stats),
    % note: we can safely include all leaf nodes here although only inner nodes
    %       should go into MIC - every inner node always has more items than
    %       any leaf node (otherwise it would have been a leaf node)
    AccMIC = lists:max([0 | [merkle_tree:get_item_count(Node) || Node <- RestTreeAcc]]),
    ?DBG_ASSERT(NextLvlNodesActIN =:= length(RestTreeAcc)),
    {FlagsAcc, lists:reverse(RestTreeAcc),
     {lists:reverse(SyncAccSend, SyncInSend),
      lists:reverse(SyncAccRcv, SyncInRcv),
      SyncInRcvLeafCount + SyncAccRcvLeafCount,
      {MySyncAccDRK ++ MySyncInDRK, MySyncInDRLCount + MySyncAccDRLCount,
       OtherSyncInDRLCount + OtherSyncAccDRLCount}},
     NStats, AccMIC, NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN};
merkle_check_node([{Hash, IsLeafHash} | TK], [Node | TN], SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, Params, Stats, FlagsAcc, RestTreeAcc,
                  SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                  MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                  SyncIN, AccCmp, AccSkip, NextLvlNodesActIN,
                  HashCmpI_IN, HashCmpL_IN) ->
    IsLeafNode = merkle_tree:is_leaf(Node),
    EmptyNode = merkle_tree:is_empty(Node),
    EmptyLeafNode = IsLeafNode andalso EmptyNode,
    NonEmptyLeafNode = IsLeafNode andalso (not EmptyNode),
    NodeHash =
        if EmptyLeafNode ->
               none; % to match with the hash from merkle_decompress_hashlist/3
           IsLeafNode ->
               <<X:SigSizeL/integer-unit:1>> = <<(merkle_tree:get_hash(Node)):SigSizeL>>,
               X;
           true ->
               <<X:SigSizeI/integer-unit:1>> = <<(merkle_tree:get_hash(Node)):SigSizeI>>,
               X
        end,
    EmptyLeafHash = IsLeafHash andalso Hash =:= none,
    NonEmptyLeafHash = IsLeafHash andalso Hash =/= none,
    % note that only hash matches(!) count for HashCmpI_OUT and HashCmpL_OUT
    % since mismatches are always correct (the items must differ!)
    % -> don't count mismatches since these values ebter the failure probability
    if Hash =:= NodeHash andalso IsLeafHash =:= IsLeafNode ->
           Skipped = merkle_tree:size(Node) - 1,
           if EmptyLeafHash -> % empty leaf hash on both nodes - this was exact!
                  HashCmpI_OUT = HashCmpI_IN,
                  HashCmpL_OUT = HashCmpL_IN;
              IsLeafHash -> % both non-empty leaf nodes
                  HashCmpI_OUT = HashCmpI_IN,
                  HashCmpL_OUT = HashCmpL_IN + 1;
              true -> % both inner nodes
                  HashCmpI_OUT = HashCmpI_IN + 1,
                  HashCmpL_OUT = HashCmpL_IN
           end,
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ?recon_ok:2>>, RestTreeAcc,
                             SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                             MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip + Skipped,
                             NextLvlNodesActIN, HashCmpI_OUT, HashCmpL_OUT);
       (not IsLeafNode) andalso (not IsLeafHash) ->
           % both inner nodes
           Childs = merkle_tree:get_childs(Node),
           NextLvlNodesActOUT = NextLvlNodesActIN + Params#merkle_params.branch_factor,
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ?recon_fail_cont_inner:2>>, lists:reverse(Childs, RestTreeAcc),
                             SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                             MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActOUT, HashCmpI_IN, HashCmpL_IN);
       (not IsLeafNode) andalso NonEmptyLeafHash ->
           % inner node here, non-empty leaf there
           % no need to compare hashes - this is an exact process based on the tags
           {MyKVItems, LeafCount} = merkle_tree:get_items(Node),
           Sync = {MyMaxItemsCount, MyKVItems},
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ?recon_fail_stop_inner:2>>, RestTreeAcc,
                             SyncAccSend, [Sync | SyncAccRcv], SyncAccRcvLeafCount + LeafCount,
                             MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN);
       NonEmptyLeafNode andalso (NonEmptyLeafHash orelse not IsLeafHash) ->
           % non-empty leaf here, non-empty leaf or inner node there
           OtherMaxItemsCount1 =
               if NonEmptyLeafHash -> % both non-empty leaf nodes
                      erlang:min(Params#merkle_params.bucket_size,
                                 OtherMaxItemsCount);
                  true -> % inner node there
                      OtherMaxItemsCount
               end,
           SyncAccSend1 =
               [{OtherMaxItemsCount1, merkle_tree:get_bucket(Node)} | SyncAccSend],
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ?recon_fail_stop_leaf:2>>, RestTreeAcc,
                             SyncAccSend1, SyncAccRcv, SyncAccRcvLeafCount,
                             MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN);
       (NonEmptyLeafNode orelse not IsLeafNode) andalso EmptyLeafHash ->
           % non-empty leaf or inner node here, empty leaf there
           % no need to compare hashes - this is an exact process based on the tags
           % -> resolve directly here, i.e. without a trivial sub process
           ResultCode = if not IsLeafNode -> ?recon_fail_stop_inner; % stop_empty_leaf1
                           NonEmptyLeafNode -> ?recon_fail_stop_leaf % stop_empty_leaf2
                        end,
           {MyKVItems, LeafCount} = merkle_tree:get_items(Node),
           MySyncAccDRK1 = [element(1, X) || X <- MyKVItems] ++ MySyncAccDRK,
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ResultCode:2>>, RestTreeAcc,
                             SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                             MySyncAccDRK1, MySyncAccDRLCount + LeafCount,
                             OtherSyncAccDRLCount,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN);
       EmptyLeafNode andalso (NonEmptyLeafHash orelse not IsLeafHash) ->
           % empty leaf here, non-empty leaf or inner node there
           % no need to compare hashes - this is an exact process based on the tags
           % -> resolved directly at the other node, i.e. without a trivial sub process
           ResultCode = if not IsLeafHash -> ?recon_fail_stop_inner; % stop_empty_leaf3
                           NonEmptyLeafHash -> ?recon_fail_cont_inner % stop_empty_leaf4
                        end,
           merkle_check_node(TK, TN, SigSizeI, SigSizeL,
                             MyMaxItemsCount, OtherMaxItemsCount, Params, Stats,
                             <<FlagsAcc/bitstring, ResultCode:2>>, RestTreeAcc,
                             SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                             MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount + 1,
                             SyncIN, AccCmp + 1, AccSkip,
                             NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN)
    end.

%% @doc Processes compare results from merkle_check_node/22 on the initiator.
%       Note that only hash matches(!) count for HashCmpI_OUT and HashCmpL_OUT
%       since mismatches are always correct (the items must differ!)
%       -> don't count mismatches since these values ebter the failure probability
-spec merkle_cmp_result(
        bitstring(), RestTree::NodeList,
        SigSizeI::signature_size(), SigSizeL::signature_size(),
        MyMaxItemsCount::non_neg_integer(), OtherMaxItemsCount::non_neg_integer(),
        SyncIn::merkle_sync(), #merkle_params{}, Stats, RestTreeAcc::NodeList,
        SyncAccSend::[merkle_sync_send()], SyncAccRcv::[merkle_sync_rcv()],
        SyncAccRcvLeafCount::Count,
        MySyncAccDRK::[?RT:key()], MySyncAccDRLCount::Count, OtherSyncAccDRLCount::Count,
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
                  {SyncInSend, SyncInRcv, SyncInRcvLeafCount,
                   {MySyncInDRK, MySyncInDRLCount, OtherSyncInDRLCount}},
                  _Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                  MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    NStats = rr_recon_stats:inc([{tree_nodesCompared, AccCmp},
                                 {tree_compareSkipped, AccSkip}], Stats),
    % note: we can safely include all leaf nodes here although only inner nodes
    %       should go into MIC - every inner node always has more items than
    %       any leaf node (otherwise it would have been a leaf node)
    AccMIC = lists:max([0 | [merkle_tree:get_item_count(Node) || Node <- RestTreeAcc]]),
    ?DBG_ASSERT(NextLvlNodesActIN =:= length(RestTreeAcc)),
    {lists:reverse(RestTreeAcc),
     {lists:reverse(SyncAccSend, SyncInSend),
      lists:reverse(SyncAccRcv, SyncInRcv),
      SyncInRcvLeafCount + SyncAccRcvLeafCount,
      {MySyncAccDRK ++ MySyncInDRK, MySyncInDRLCount + MySyncAccDRLCount,
       OtherSyncInDRLCount + OtherSyncAccDRLCount}},
     NStats, AccMIC, NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN};
merkle_cmp_result(<<?recon_ok:2, TR/bitstring>>, [Node | TN], SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, MerkleSyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                  MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                  AccCmp, AccSkip, NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
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
                      RestTreeAcc, SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                      MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                      AccCmp + 1, AccSkip + Skipped,
                      NextLvlNodesActIN, HashCmpI_OUT, HashCmpL_OUT);
merkle_cmp_result(<<?recon_fail_cont_inner:2, TR/bitstring>>, [Node | TN],
                  SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                  MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount,
                  AccCmp, AccSkip, NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    % either cont_inner or stop_empty_leaf4
    case merkle_tree:is_leaf(Node) of
        false -> % cont_inner
            % inner hash on both nodes
            Childs = merkle_tree:get_childs(Node),
            NextLvlNodesActOUT = NextLvlNodesActIN + Params#merkle_params.branch_factor,
            RestTreeAcc1 = lists:reverse(Childs, RestTreeAcc),
            MySyncAccDRK1 = MySyncAccDRK,
            MySyncAccDRLCount1 = MySyncAccDRLCount;
        true -> % stop_empty_leaf4
            ?DBG_ASSERT(not merkle_tree:is_empty(Node)),
            % non-empty leaf on this node, empty leaf on the other node
            % -> resolve directly here, i.e. without a trivial sub process
            NextLvlNodesActOUT = NextLvlNodesActIN,
            RestTreeAcc1 = RestTreeAcc,
            MyKVItems = merkle_tree:get_bucket(Node),
            MySyncAccDRK1 = [element(1, X) || X <- MyKVItems] ++ MySyncAccDRK,
            MySyncAccDRLCount1 = MySyncAccDRLCount + 1
    end,
    merkle_cmp_result(TR, TN, SigSizeI, SigSizeL,
                      MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                      RestTreeAcc1, SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                      MySyncAccDRK1, MySyncAccDRLCount1, OtherSyncAccDRLCount,
                      AccCmp + 1, AccSkip,
                      NextLvlNodesActOUT, HashCmpI_IN, HashCmpL_IN);
merkle_cmp_result(<<?recon_fail_stop_inner:2, TR/bitstring>>, [Node | TN],
                  SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                  MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    % either stop_inner or stop_empty_leaf1 or stop_empty_leaf3
    % NOTE: all these mismatches are exact process based on the tags
    IsLeafNode = merkle_tree:is_leaf(Node),
    EmptyLeafNode = IsLeafNode andalso merkle_tree:is_empty(Node),
    
    if IsLeafNode andalso (not EmptyLeafNode) -> % stop_inner
           SyncAccSend1 =
               [{OtherMaxItemsCount, merkle_tree:get_bucket(Node)} | SyncAccSend],
           OtherSyncAccDRLCount1 = OtherSyncAccDRLCount,
           MySyncAccDRK1 = MySyncAccDRK,
           MySyncAccDRLCount1 = MySyncAccDRLCount;
       EmptyLeafNode -> % stop_empty_leaf1
           % -> resolved directly at the other node, i.e. without a trivial sub process
           SyncAccSend1 = SyncAccSend,
           OtherSyncAccDRLCount1 = OtherSyncAccDRLCount + 1, % this will deviate from the other node!
           MySyncAccDRK1 = MySyncAccDRK,
           MySyncAccDRLCount1 = MySyncAccDRLCount;
       not IsLeafNode -> % stop_empty_leaf3
           % -> resolve directly here, i.e. without a trivial sub process
           SyncAccSend1 = SyncAccSend,
           OtherSyncAccDRLCount1 = OtherSyncAccDRLCount,
           {MyKVItems, LeafCount} = merkle_tree:get_items(Node),
           MySyncAccDRK1 = [element(1, X) || X <- MyKVItems] ++ MySyncAccDRK,
           MySyncAccDRLCount1 = MySyncAccDRLCount + LeafCount
    end,
    merkle_cmp_result(TR, TN, SigSizeI, SigSizeL,
                      MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                      RestTreeAcc, SyncAccSend1, SyncAccRcv, SyncAccRcvLeafCount,
                      MySyncAccDRK1, MySyncAccDRLCount1, OtherSyncAccDRLCount1,
                      AccCmp + 1, AccSkip,
                      NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN);
merkle_cmp_result(<<?recon_fail_stop_leaf:2, TR/bitstring>>, [Node | TN],
                  SigSizeI, SigSizeL,
                  MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                  RestTreeAcc, SyncAccSend, SyncAccRcv, SyncAccRcvLeafCount,
                  MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount, AccCmp, AccSkip,
                  NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN) ->
    % either stop_leaf or stop_empty_leaf2
    case merkle_tree:is_leaf(Node) of
        true  ->
            case merkle_tree:is_empty(Node) of
                false -> % stop_leaf
                    MaxItemsCount = erlang:min(Params#merkle_params.bucket_size,
                                               MyMaxItemsCount),
                    SyncAccRcv1 =
                        [{MaxItemsCount, merkle_tree:get_bucket(Node)} | SyncAccRcv],
                    SyncAccRcvLeafCount1 = SyncAccRcvLeafCount + 1,
                    OtherSyncAccDRLCount1 = OtherSyncAccDRLCount;
                true -> % stop_empty_leaf2
                    % -> resolved directly at the other node, i.e. without a trivial sub process
                    SyncAccRcv1 = SyncAccRcv,
                    SyncAccRcvLeafCount1 = SyncAccRcvLeafCount,
                    OtherSyncAccDRLCount1 = OtherSyncAccDRLCount + 1
            end;
        false -> % stop_leaf
            {MyKVItems, LeafCount} = merkle_tree:get_items(Node),
            SyncAccRcv1 =
                [{MyMaxItemsCount, MyKVItems} | SyncAccRcv],
            SyncAccRcvLeafCount1 = SyncAccRcvLeafCount + LeafCount,
            OtherSyncAccDRLCount1 = OtherSyncAccDRLCount
    end,
    merkle_cmp_result(TR, TN, SigSizeI, SigSizeL,
                      MyMaxItemsCount, OtherMaxItemsCount, SyncIn, Params, Stats,
                      RestTreeAcc, SyncAccSend, SyncAccRcv1, SyncAccRcvLeafCount1,
                      MySyncAccDRK, MySyncAccDRLCount, OtherSyncAccDRLCount1,
                      AccCmp + 1, AccSkip,
                      NextLvlNodesActIN, HashCmpI_IN, HashCmpL_IN).

%% @doc Helper for calculating the actual used failure rate during merkle
%%      hash comparisons.
-spec merkle_calc_used_fr(PrevUsedFr::UsedFrSum, EffectiveFr_I::float(),
                          EffectiveFr_L::float(), HashCmpI::non_neg_integer(),
                          HashCmpL::non_neg_integer()) -> UsedFrSum
        when is_subtype(UsedFrSum, {Sum::float(), Compensation::float()}).
merkle_calc_used_fr({PrevUsedFr, PrevCompensation}, EffectiveFr_I,
                    EffectiveFr_L, HashCmpI, HashCmpL) ->
    % PrevUsedFr + HashCmpI * EffectiveFr_I +  HashCmpL * EffectiveFr_L
    util:kahan_sum([HashCmpI * EffectiveFr_I,  HashCmpL * EffectiveFr_L],
                   PrevUsedFr, PrevCompensation).

%% @doc Helper for adding a leaf node's KV-List to a compressed binary
%%      during merkle sync.
%% @see merkle_resolve_retrieve_leaf_hashes/14
-spec merkle_resolve_add_leaf_hashes(
        [SyncSend], FRAllLeaves::float(), NumRestLeaves::non_neg_integer(),
        BucketSizeBits::non_neg_integer(), DupesSizeBits::pos_integer(),
        Params::#merkle_params{}, PrevFR::UsedFrSum,
        HashesK::Bin, HashesV::Bin, BucketSizesBin::Bin, DiffSigSizesBin::Bin, DupesCount::Bin,
        NewSyncAcc::[SyncSend], Dupes::[db_chunk_kv()])
        -> {{HashesK::Bin, HashesV::Bin, BucketSizesBin::Bin, DiffSigSizesBin::Bin, DupesCount::Bin},
            NewSyncSend::[SyncSend], Dupes::db_chunk_kv()}
    when is_subtype(Bin, bitstring()),
         is_subtype(SyncSend, {OtherMaxItemsCount::non_neg_integer(),
                               Bucket::merkle_tree:mt_bucket()}),
         is_subtype(UsedFrSum, {Sum::float(), Compensation::float()}).
merkle_resolve_add_leaf_hashes(
  [{OtherMaxItemsCount, Bucket} | Rest], FRAllLeaves, NumRestLeaves,
  BucketSizeBits, DupesSizeBits, Params, {PrevFRSum, PrevFrC} = _PrevFR,
  HashesK, HashesV, BucketSizesBin, DiffSigSizesBin, DupesCount, SyncAcc, Dupes) ->
    BucketSize = length(Bucket),
    ExpDelta = 100, % TODO: establish a bound of maximum MaxAffectedItems items for very low (initial) ExpDelta?
    ?DBG_ASSERT(BucketSize > 0),
    ?DBG_ASSERT(BucketSize =< util:pow(2, BucketSizeBits)),
    % note: this includes duplicates which are actually not encoded!
    ?DBG_ASSERT(?implies(BucketSizeBits =:= 0, BucketSize =:= 1)),
    BucketSizesBinNew = <<BucketSizesBin/bitstring, (BucketSize - 1):BucketSizeBits>>,
    FR_next = calc_n_subparts_FR(NumRestLeaves, FRAllLeaves, PrevFRSum),
    {SigSize, VSize} =
        trivial_signature_sizes(OtherMaxItemsCount, BucketSize, ExpDelta, FR_next),
    % note: we can only estimate the real FR of this part here - the other
    %       node will report back the exact probability based on its actual
    %       number of items (we nonetheless need this value to adjust the
    %       individual trivial syncs' signatures!)
    ThisFR_upper_bound =
        trivial_worst_case_failrate(SigSize, OtherMaxItemsCount, BucketSize, ExpDelta),
    NextFR = util:kahan_sum([ThisFR_upper_bound], PrevFRSum, PrevFrC),
%%     log:pal("merkle_send [ ~p ] (rest: ~B):~n  bits: ~p, FR: ~p vs. ~p~n  acc total: ~p -> ~p",
%%             [self(), NumRestLeaves, {SigSize, VSize}, FR_next, FR_p1, _PrevFR, NextFR]),
    {{HashesKBucket, IdxBitsSize, DiffSigSize1}, HashesVBucket, ResortedBucket, AddDupes} =
        compress_kv_list(Bucket, SigSize, VSize, fun trivial_compress_key/2, return_size),
    AddDupesL = length(AddDupes),
    % beware: buckets with 0 encoded items are optimised to <<>>
    if (BucketSize - AddDupesL) > 0 ->
           DiffSigSizesBinNew = <<DiffSigSizesBin/bitstring, DiffSigSize1:IdxBitsSize/integer-unit:1>>,
           HashesKNew = <<HashesK/bitstring, HashesKBucket/bitstring>>,
           HashesVNew = <<HashesV/bitstring, HashesVBucket/bitstring>>,
           ok;
       true ->
           ?DBG_ASSERT(HashesKBucket =:= <<>>),
           ?DBG_ASSERT(HashesVBucket =:= <<>>),
           DiffSigSizesBinNew = DiffSigSizesBin,
           HashesKNew = HashesK,
           HashesVNew = HashesV,
           ok
    end,
%%     log:pal("merkle_send [ ~p ]:~n  ~p~n  ~p",
%%             [self(), {NumRestLeaves, FRAllLeaves, _PrevFR},
%%              {BucketSize, AddDupesL, OtherMaxItemsCount, FR_next}]),
    DupesCountNew = <<DupesCount/bitstring, AddDupesL:DupesSizeBits>>,
    merkle_resolve_add_leaf_hashes(
      Rest, FRAllLeaves, NumRestLeaves - 1, BucketSizeBits, DupesSizeBits, Params, NextFR,
      HashesKNew, HashesVNew, BucketSizesBinNew, DiffSigSizesBinNew, DupesCountNew, [{OtherMaxItemsCount, ResortedBucket} | SyncAcc],
      [AddDupes | Dupes]);
merkle_resolve_add_leaf_hashes(
    [], _FRAllLeaves, _NumRestLeaves, _BucketSizeBits, _DupesSizeBits,
    _Params, _PrevFR, HashesK, HashesV, BucketSizesBin, DiffSigSizesBin, DupesCount, SyncAcc, Dupes) ->
    ?DBG_ASSERT(HashesK =/= <<>> orelse HashesV =/= <<>> orelse DupesCount > 0),
    {{HashesK, HashesV, BucketSizesBin, DiffSigSizesBin, DupesCount},
     lists:reverse(SyncAcc), lists:flatten(Dupes)}.

%% @doc Helper for retrieving a leaf node's KV-List from the compressed binary
%%      returned by merkle_resolve_add_leaf_hashes/9 during merkle sync.
%% @see merkle_resolve_add_leaf_hashes/9
-spec merkle_resolve_retrieve_leaf_hashes(
        [SyncRcv], HashesK::Bin, HashesV::Bin, BucketSizesBin::Bin, DiffSigSizesBin::Bin, DupesCount::Bin, FRAllLeaves::float(),
        NumRestLeaves::non_neg_integer(), PrevFR::UsedFrSum, PrevFR_real::UsedFrSum,
        BucketSizeBits::non_neg_integer(), DupesSizeBits::pos_integer(), Params::#merkle_params{},
        ToSendKeysAcc::[?RT:key()], ToResolveIdxAcc::[bitstring()],
        ResolveNonEmpty::boolean(), DupesCountTotal::non_neg_integer())
        -> {ToSendKeys::[?RT:key()], ToResolveIdx::Bin,
            OtherHasResolve::boolean(), ThisFR_real::float()}
    when is_subtype(Bin, bitstring()),
         is_subtype(SyncRcv, {MyMaxItemsCount::non_neg_integer(),
                              Bucket::merkle_tree:mt_bucket()}),
         is_subtype(UsedFrSum, {Sum::float(), Compensation::float()}).
merkle_resolve_retrieve_leaf_hashes(
  [{MyMaxItemsCount, MyKVItems} | Rest],
  HashesK, HashesV, BucketSizesBin, DiffSigSizesBin, DupesCount, FRAllLeaves, NumRestLeaves,
  {PrevFRSum, PrevFrC} = _PrevFR, {PrevFR_real_Sum, PrevFr_real_C} = _PrevFR_real,
  BucketSizeBits, DupesSizeBits, Params, ToSendKeys, ToResolveIdx,
  ResolveNonEmpty, DupesCountTotal) ->
    MyActualItemsCount = length(MyKVItems),
    ExpDelta = 100, % TODO: establish a bound of maximum MaxAffectedItems items for very low (initial) ExpDelta?
    <<BucketSize0:BucketSizeBits/integer-unit:1, NBucketSizesBin/bitstring>> = BucketSizesBin,
    <<Dupes:DupesSizeBits/integer-unit:1, NDupesCount/bitstring>> = DupesCount,
    OtherActualItemsCount = BucketSize0 + 1,
    BucketSize = OtherActualItemsCount - Dupes,
    FR_next = calc_n_subparts_FR(NumRestLeaves, FRAllLeaves, PrevFRSum),
%%     log:pal("merkle_receive [ ~p ]:~n  ~p~n  ~p",
%%             [self(), {NumRestLeaves, FRAllLeaves, _PrevFR},
%%              {BucketSize, Dupes, MyMaxItemsCount, FR_next}]),
    {SigSize, VSize} =
        trivial_signature_sizes(MyMaxItemsCount, OtherActualItemsCount, ExpDelta, FR_next),
    ThisFR_upper_bound =
        trivial_worst_case_failrate(SigSize, MyMaxItemsCount, OtherActualItemsCount, ExpDelta),
    NextFR = util:kahan_sum([ThisFR_upper_bound], PrevFRSum, PrevFrC),
    ThisFR_real =
        trivial_worst_case_failrate(SigSize, MyActualItemsCount, OtherActualItemsCount, ExpDelta),
    NextFR_real = util:kahan_sum([ThisFR_real], PrevFR_real_Sum, PrevFr_real_C),
%%     log:pal("merkle_receive [ ~p ] (rest: ~B):~n  bits: ~p, acc: ~p vs. ~p~n  acc total: ~p -> ~p",
%%             [self(), NumRestLeaves, {SigSize, VSize}, FR_next, ThisFR_upper_bound, _PrevFR, NextFR]),
    % beware: buckets with 0 encoded items are optimised to <<>>
    if BucketSize > 0 ->
           IdxBitsSize = bits_for_number(SigSize),
           <<DiffSigSize1:IdxBitsSize/integer-unit:1, NDiffSigSizesBin/bitstring>> = DiffSigSizesBin,
           OBucketKBinSize = BucketSize * DiffSigSize1,
           %log:pal("merkle: ~B", [OBucketKBinSize]),
           OBucketVBinSize = BucketSize * VSize,
           <<OBucketKBin:OBucketKBinSize/bitstring, NHashesK/bitstring>> = HashesK,
           <<OBucketVBin:OBucketVBinSize/bitstring, NHashesV/bitstring>> = HashesV,
           {OBucketTree, VSize} = decompress_kv_list({OBucketKBin, OBucketVBin}, DiffSigSize1, SigSize),
           ok;
       true ->
           OBucketTree = mymaps:new(),
           NHashesK = HashesK,
           NHashesV = HashesV,
           NDiffSigSizesBin = DiffSigSizesBin,
           ok
    end,

    % calc diff (trivial sync)
    ?DBG_ASSERT(MyKVItems =/= []),
%%     log:pal("Tree: ~.2p", [mymaps:values(OBucketTree)]),
    % note: dupes are already added to ToSendKeys1
    {ToSendKeys1, ToReqIdx1, OBucketTree1, _Dupes} =
        get_diff_with_dupes(
          MyKVItems, OBucketTree, ToSendKeys, [], SigSize, VSize,
          fun get_full_diff/4),
    ReqIdx = lists:usort(
               [Idx || {_Version, Idx} <- mymaps:values(OBucketTree1)]
                   ++ ToReqIdx1),
%%     log:pal("pos_to_bitstring(~.2p, ~.2p, 0, ~B)",
%%             [ReqIdx, ToResolveIdx, Params#merkle_params.bucket_size]),
    ToResolveIdx1 = pos_to_bitstring(ReqIdx, ToResolveIdx, 0,
                                     Params#merkle_params.bucket_size),
    merkle_resolve_retrieve_leaf_hashes(
      Rest, NHashesK, NHashesV, NBucketSizesBin, NDiffSigSizesBin, NDupesCount, FRAllLeaves, NumRestLeaves - 1, NextFR,
      NextFR_real, BucketSizeBits, DupesSizeBits, Params, ToSendKeys1, ToResolveIdx1,
      ?IIF(ReqIdx =/= [], true, ResolveNonEmpty), DupesCountTotal + Dupes);
merkle_resolve_retrieve_leaf_hashes(
  [], HashesK, HashesV, BucketSizesBin, DiffSigSizesBin, DupesCount, _FRAllLeaves, _NumRestLeaves, _PrevFR,
  PrevFR_real, _BucketSizeBits, _DupesSizeBits, _Params, ToSendKeys, ToResolveIdx,
  ResolveNonEmpty, DupesCountTotal) ->
    ?ASSERT(HashesK =:= <<>>),
    ?ASSERT(HashesV =:= <<>>),
    ?ASSERT(BucketSizesBin =:= <<>>),
    ?ASSERT(DiffSigSizesBin =:= <<>>),
    ?ASSERT(DupesCount =:= <<>>),
    ToResolveIdx1 =
        if ResolveNonEmpty ->
               erlang:list_to_bitstring(lists:reverse(ToResolveIdx));
           true ->
               % optimise this case away (otherwise all entries will be 0)
               % (also ref. the calls of merkle_resolve_leaves_ckidx/8)
               <<>>
        end,
    {ToSendKeys, ToResolveIdx1, DupesCountTotal > 0 orelse ResolveNonEmpty,
     element(1, PrevFR_real)}.

%% @doc Creates a compact binary consisting of bitstrings with trivial
%%      reconciliations for all sync requests to send.
-spec merkle_resolve_leaves_send(
        State::state(), UsedFr::{Sum::float(), Compensation::float()})
    -> NewState::state().
merkle_resolve_leaves_send(
  State = #rr_recon_state{params = Params, initiator = IsInitiator,
                          stats = Stats, dest_recon_pid = DestReconPid,
                          dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                          merkle_sync = {SyncSend, SyncRcv, SyncRcvLeafCount,
                                         {MySyncDRK, MySyncDRLCount, OtherSyncDRLCount}}},
  {UsedFrSum, _UsedFrC}) ->
%%     log:pal("Sync (~s):~n  send:~.2p~n  rcv:~.2p",
%%             [?IIF(IsInitiator, "I", "NI"), SyncSend, SyncRcv]),
    % resolve items from emptyLeaf-* comparisons with empty leaves on any node as key_upd:
    NStats0 = send_resolve_request(Stats, MySyncDRK, OwnerL, DestRRPid, IsInitiator, true),
    NStats1 = rr_recon_stats:set([{fail_rate_p1, UsedFrSum}], NStats0),
    NStats2 = rr_recon_stats:inc(
                [{tree_leavesSynced, MySyncDRLCount + OtherSyncDRLCount},
                 {rs_expected, ?IIF(OtherSyncDRLCount > 0, 1, 0)}], NStats1),
    % allow the garbage collector to free the SyncDRK list here
    SyncNew1 = {SyncSend, SyncRcv, SyncRcvLeafCount, {[], MySyncDRLCount, OtherSyncDRLCount}},

    SyncSendL = length(SyncSend),
    SyncRcvL = length(SyncRcv),
    TrivialProcs = SyncSendL + SyncRcvL,
    FRAllLeaves = calc_n_subparts_FR(1, Params#merkle_params.fail_rate, UsedFrSum),
    ?ALG_DEBUG("merkle (~s) - LeafSync~n  ~B (send), ~B (receive), ~B direct (~s)\tFRAllLeaves: ~g\t"
               "ItemsToSend: ~B (~g per leaf)~n  send: ~B (L-L) + ~B (L-I), receive: ~B (L-L) + ~B (I-L)",
               [?IIF(IsInitiator, "I", "NI"), SyncSendL, SyncRcvL,
                MySyncDRLCount, ?IIF(IsInitiator, "in", "out"),
                FRAllLeaves,
                lists:sum([length(MyKVItems) || {_, MyKVItems} <- SyncSend]),
                ?IIF(SyncSend =/= [],
                     lists:sum([length(MyKVItems) || {_, MyKVItems} <- SyncSend]) /
                         SyncSendL, 0.0),
                length([ok || {OtherMaxIC, _MyKVItems} <- SyncSend, OtherMaxIC =< Params#merkle_params.bucket_size]),
                SyncSendL - length([ok || {OtherMaxIC, _MyKVItems} <- SyncSend, OtherMaxIC =< Params#merkle_params.bucket_size]),
                length([ok || {MyMaxIC, _MyKVItems} <- SyncRcv, MyMaxIC =< Params#merkle_params.bucket_size]),
                SyncRcvL - length([ok || {MyMaxIC, _MyKVItems} <- SyncRcv, MyMaxIC =< Params#merkle_params.bucket_size])]),
    
    if SyncSendL =:= 0 andalso SyncRcvL =:= 0 ->
           % nothing to do
           shutdown(sync_finished,
                    State#rr_recon_state{stats = NStats2,
                                         merkle_sync = SyncNew1, misc = []});
       SyncSend =/= [] ->
           % note: we do not have empty buckets here and thus always store (BucketSize - 1)
           BucketSizeBits = bits_for_number(Params#merkle_params.bucket_size - 1),
           % note: duplicates can be 0 up to BucketSize
           DupesSizeBits = bits_for_number(Params#merkle_params.bucket_size),
           {SyncStruct, NewSyncSend, Dupes} =
               merkle_resolve_add_leaf_hashes(
                 SyncSend, FRAllLeaves, TrivialProcs, BucketSizeBits, DupesSizeBits,
                 Params, {0.0, 0.0}, <<>>, <<>>, <<>>, <<>>, <<>>, [], []),
           % note: 1 trivial proc contains 1 leaf
           % the other node will send its items from this CKV list - increase rs_expected, too
           NStats3 = rr_recon_stats:inc([{tree_leavesSynced, SyncSendL},
                                         {rs_expected, 1}], NStats2),
           
           MerkleSyncNew1 = {NewSyncSend, SyncRcv, SyncRcvLeafCount,
                             {[], MySyncDRLCount, OtherSyncDRLCount}},
           ?ALG_DEBUG("merkle (~s) - HashesSize: ~B (~B compressed)",
                      [?IIF(IsInitiator, "I", "NI"),
                       erlang:byte_size(erlang:term_to_binary(SyncStruct)),
                       erlang:byte_size(
                         erlang:term_to_binary(SyncStruct, [compressed]))]),
           send(DestReconPid, {resolve_req, SyncStruct}),
           State#rr_recon_state{stage = resolve, stats = NStats3,
                                merkle_sync = MerkleSyncNew1,
                                next_phase_kv = Dupes,
                                misc = [{all_leaf_acc, FRAllLeaves},
                                        {trivial_procs, TrivialProcs}]};
       true ->
           % only wait for the other node's resolve_req
           State#rr_recon_state{stage = resolve, merkle_sync = SyncNew1,
                                stats = NStats2,
                                misc = [{all_leaf_acc, FRAllLeaves},
                                        {trivial_procs, TrivialProcs}]}
    end.

%% @doc Decodes the trivial reconciliations from merkle_resolve_leaves_send/5
%%      and resolves them returning a compressed idx list each with keys to
%%      request.
-spec merkle_resolve_leaves_receive(
        State::state(), {HashesK::Bin, HashesV::Bin, BucketSizesBin::Bin,
                         DiffSigSizesBin::Bin, DupesCount::Bin}) -> NewState::state()
    when is_subtype(Bin, bitstring()).
merkle_resolve_leaves_receive(
  State = #rr_recon_state{initiator = IsInitiator,
                          merkle_sync = {SyncSend, SyncRcv, SyncRcvLeafCount, DirectResolve},
                          params = Params,
                          dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                          dest_recon_pid = DestRCPid, stats = Stats,
                          misc = [{all_leaf_acc, FRAllLeaves},
                                  {trivial_procs, TrivialProcs}]},
  {HashesK, HashesV, BucketSizesBin, DiffSigSizesBin, DupesCount}) ->
    ?DBG_ASSERT(HashesK =/= <<>> orelse HashesV =/= <<>> orelse DupesCount > 0),
    % note: we do not have empty buckets here and thus always store (BucketSize - 1)
    BucketSizeBits = bits_for_number(Params#merkle_params.bucket_size - 1),
    % note: duplicates can be 0 up to BucketSize
    DupesSizeBits = bits_for_number(Params#merkle_params.bucket_size),
    % mismatches to resolve:
    % * at initiator    : inner(I)-leaf(NI) or leaf(NI)-non-empty-leaf(I)
    % * at non-initiator: inner(NI)-leaf(I)
    % note: 1 trivial proc may thus contain more than 1 leaf!
    {ToSend, ToResolve, OtherHasResolve, ThisFR_real} =
        merkle_resolve_retrieve_leaf_hashes(
          SyncRcv, HashesK, HashesV, BucketSizesBin, DiffSigSizesBin, DupesCount, FRAllLeaves, TrivialProcs,
          {0.0, 0.0}, {0.0, 0.0},
          BucketSizeBits, DupesSizeBits, Params, [], [], false, 0),

    % send resolve message:
    % resolve items we should send as key_upd:
    % NOTE: the other node does not know whether our ToSend is empty and thus
    %       always expects a following resolve process!
    Stats1 = send_resolve_request(Stats, ToSend, OwnerL, DestRRPid, IsInitiator, false),
    % let the other node's rr_recon process identify the remaining keys;
    % it will use key_upd_send (if non-empty) and we must thus increase
    % the number of resolve processes here!
    MerkleResReqs = ?IIF(OtherHasResolve, 1, 0),
    Stats2 = rr_recon_stats:inc([{tree_leavesSynced, SyncRcvLeafCount},
                                 {rs_expected, MerkleResReqs}], Stats1),
    ?DBG_ASSERT(rr_recon_stats:get(fail_rate_p2, Stats2) =:= 0.0),
    NStats  = rr_recon_stats:set([{fail_rate_p2, ThisFR_real}], Stats2),
    ?ALG_DEBUG("resolve_req Merkle~n  Session=~.0p ; resolve expexted=~B",
               [rr_recon_stats:get(session_id, NStats),
                rr_recon_stats:get(rs_expected, NStats)]),

    comm:send(DestRCPid, {resolve_req, ToResolve, ThisFR_real}),
    % free up some memory:
    NewState = State#rr_recon_state{merkle_sync = {SyncSend, [], SyncRcvLeafCount, DirectResolve},
                                    stats = NStats},
    % shutdown if nothing was sent (otherwise we need to wait for the returning CKidx):
    if SyncSend =:= [] -> shutdown(sync_finished, NewState);
       true -> NewState
    end.

%% @doc Decodes all requested keys from merkle_resolve_leaves_receive/2 (as a
%%      result of sending resolve requests) and resolves the appropriate entries
%%      (if non-empty) with our data using a key_upd_send.
-spec merkle_resolve_leaves_ckidx(
        Sync::[merkle_sync_send()], BinKeyList::bitstring(), DestRRPid::comm:mypid(),
        Stats, OwnerL::comm:erl_local_pid(), Params::#merkle_params{},
        ToSend::[?RT:key()], IsInitiator::boolean()) -> NewStats::Stats
    when is_subtype(Stats, rr_recon_stats:stats()).
merkle_resolve_leaves_ckidx([{_OtherMaxItemsCount, MyKVItems} | TL],
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
    ?ALG_DEBUG("Resolve~n  Session=~.0p ; ToSend=~p",
               [rr_recon_stats:get(session_id, Stats), 0]),
    Stats;
send_resolve_request(Stats, ToSend, OwnerL, DestRRPid, IsInitiator,
                     _SkipIfEmpty) ->
    SID = rr_recon_stats:get(session_id, Stats),
    ?ALG_DEBUG("Resolve~n  Session=~.0p ; ToSend=~p", [SID, length(ToSend)]),
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
    util:ceil(util:log2(Number + 1)).

%% @doc Splits the target failure rate FR into N (not necessarily independent)
%%      sub-processes with equal failure rates and returns the FR to use for
%%      each of them: FR_sub = FR / N.
%%      This is based on the linearity of the expected number of failures.
-spec calc_n_subparts_FR(N::pos_integer(), FR::float()) -> FR_sub::float().
calc_n_subparts_FR(N, FR) when FR > 0 andalso N >= 1 ->
    _FR_sub = FR / N.

%% @doc Splits the target failure rate FR into N further almost equal (not
%%      necessarily independent) independent sub-processes and returns the
%%      target FR use for the next of these sub-processes with all the previous
%%      sub-processes having a combined failure rate of PrevFr.
%%      This is based on the linearity of the expected number of failures.
-spec calc_n_subparts_FR(N::pos_integer(), FR::float(), PrevFr::float())
        -> FR_sub::float().
calc_n_subparts_FR(N, FR, PrevFr)
  when FR > 0 andalso PrevFr >= 0 andalso N >= 1 ->
    FR_sub = (FR - PrevFr) / N,
    % a previous phase may overstep (if item counts change during the setup)
    % -> allow that and print a warning
    if FR_sub > 0 ->
           FR_sub;
       true ->
           log:log("~w: [ ~.0p:~.0p ] FR constraint broken (phase 1 overstepped?)~n"
                   "  continuing with ~p instead (~p, ~p, ~B)",
                   [?MODULE, pid_groups:my_groupname(), self(), 1.0e-16,
                    FR, PrevFr, N]),
           1.0e-16
    end.

%% @doc Calculates the signature sizes for comparing every item in Items
%%      (at most ItemCount) with OtherItemCount other items and expecting at
%%      most min(ItemCount, OtherItemCount) version comparisons.
%%      Sets the bit sizes to have a failure rate below FR.
%% @see shash_signature_sizes/4
-spec trivial_signature_sizes
        (ItemCountI::non_neg_integer(), ItemCountNI::non_neg_integer(),
         ExpDelta::number(), FR::float())
        -> {SigSize::signature_size(), VSize::signature_size()}.
trivial_signature_sizes(0, _ItemCountNI, _ExpDelta, FR) when FR > 0 ->
    {0, 0}; % invalid but since there are 0 items, this is ok!
trivial_signature_sizes(_ItemCountI, 0, _ExpDelta, FR) when FR > 0 ->
    {0, 0}; % invalid but since there are 0 items, this is ok!
trivial_signature_sizes(ItemCountI, ItemCountNI, ExpDelta, FR) when FR > 0 ->
    MaxKeySize = 128, % see compress_key/2
    VSize = get_min_version_bits(),
    NT = calc_max_different_hashes(ItemCountI, ItemCountNI, ExpDelta),
    SigSize =
        if NT > 1 ->
               %% log_2(1 / (1 - (1 - FR / (2*NT))^(1 / (NT-1))))
               Y = calc_one_m_xpow_one_m_z(1 / (NT - 1), FR / (2 * NT)),
               min_max(util:ceil(util:log2(1 / Y)), get_min_hash_bits(), MaxKeySize);
           NT =:= 1 ->
               % should only happen for ExpDelta == 0, but may occur in other
               % cases due to floating point issues
               get_min_hash_bits()
        end,
%%     log:pal("trivial [ ~p ] - FR: ~p, \tSigSize: ~B, \tVSizeL: ~B~n"
%%             "IC@I: ~B, \tIC@NI: ~B",
%%             [self(), FR, SigSize, VSize, ItemCountI, ItemCountNI]),
    {SigSize, VSize}.

%% @doc Calculates the worst-case failure rate of the trivial algorithm
%%      with the given signature size, item counts and expected delta.
%%      NOTE: Precision loss may occur for very high values!
%% @see shash_worst_case_failrate/4
-spec trivial_worst_case_failrate(
        SigSize::signature_size(), ItemCountI::non_neg_integer(),
        ItemCountNI::non_neg_integer(), ExpDelta::number()) -> float().
trivial_worst_case_failrate(0, 0, _ItemCountNI, _ExpDelta) ->
    % this is exact! (see special case in trivial_signature_sizes/4)
    0.0;
trivial_worst_case_failrate(0, _ItemCountI, 0, _ExpDelta) ->
    % this is exact! (see special case in trivial_signature_sizes/4)
    0.0;
trivial_worst_case_failrate(SigSize, ItemCountI, ItemCountNI, ExpDelta) ->
    BK2 = util:pow(2, SigSize),
    NT = calc_max_different_hashes(ItemCountI, ItemCountNI, ExpDelta),
    % exact but with problems for small 1 / BK2:
%%     2 * NT * (1 - math:pow(1 - 1 / BK2, NT - 1)).
    2 * NT * calc_one_m_xpow_one_m_z(NT - 1, 1 / BK2).

%% @doc Creates a compressed key-value list for comparing every item in Items
%%      (at most ItemCount) with OtherItemCount other items and expecting at
%%      most min(ItemCount, OtherItemCount) version comparisons.
%%      Sets the bit sizes to have an error below FR.
-spec compress_kv_list_fr(
        Items::db_chunk_kv(), ItemCountI::IC, ItemCountNI::IC, ExpDelta, FR,
        SigFun::fun((ItemCountI::IC, ItemCountNI::IC, ExpDelta, FR) -> {SigSize, VSize::signature_size()}),
        KeyComprFun::fun(({?RT:key(), client_version()}, SigSize) -> bitstring()))
        -> {KeyDiff::Bin, VBin::Bin, ResortedKOrigList::db_chunk_kv(),
            Dupes::db_chunk_kv(), SigSize::signature_size(), VSize::signature_size()}
    when is_subtype(Bin, bitstring()),
         is_subtype(IC, non_neg_integer()),
         is_subtype(ExpDelta, number()),
         is_subtype(FR, float()),
         is_subtype(SigSize, signature_size()).
compress_kv_list_fr(DBItems, ItemCountI, ItemCountNI, ExpDelta, FR, SigFun, KeyComprFun) ->
    {SigSize, VSize} = SigFun(ItemCountI, ItemCountNI, ExpDelta, FR),
    {HashesKNew, HashesVNew, ResortedBucket, Dupes} =
        compress_kv_list(DBItems, SigSize, VSize, KeyComprFun, integrate_size),
    % debug compressed and uncompressed sizes:
    ?ALG_DEBUG("compress_kv_list (ExpDelta = ~p)~n"
               "  ~Bckv vs. ~Bcmp items (~B dupes), SigSize: ~B, VSize: ~B, ChunkSize: ~B+~B / ~B+~B bits",
               [ExpDelta, ItemCountNI, ItemCountI, length(Dupes), SigSize, VSize,
                erlang:bit_size(erlang:term_to_binary(HashesKNew)),
                erlang:bit_size(erlang:term_to_binary(HashesVNew)),
                erlang:bit_size(
                  erlang:term_to_binary(HashesKNew,
                                        [{minor_version, 1}, {compressed, 2}])),
                erlang:bit_size(
                  erlang:term_to_binary(HashesVNew,
                                        [{minor_version, 1}, {compressed, 2}]))]),
    ?DBG_ASSERT(?implies(VSize =/= 0, (HashesKNew =:= <<>>) =:= (HashesVNew =:= <<>>))),
    {HashesKNew, HashesVNew, ResortedBucket, Dupes, SigSize, VSize}.

%% @doc Calculates the signature size for comparing ItemCount items with
%%      OtherItemCount other items (including versions into the hashes).
%%      Sets the bit size to have a failure rate below FR.
%% @see trivial_signature_sizes/4
-spec shash_signature_sizes(
        ItemCountI::non_neg_integer(), ItemCountNI::non_neg_integer(),
        ExpDelta::number(), FR::float())
        -> {SigSize::signature_size(), _VSize::0}.
shash_signature_sizes(0, _ItemCountNI, _ExpDelta, FR) when FR > 0 ->
    {0, 0}; % invalid but since there are 0 items, this is ok!
shash_signature_sizes(_ItemCountI, 0, _ExpDelta, FR) when FR > 0 ->
    {0, 0}; % invalid but since there are 0 items, this is ok!
shash_signature_sizes(ItemCountI, ItemCountNI, ExpDelta, FR) when FR > 0 ->
    MaxSize = 128, % see compress_key/2
    N_delta_I = calc_max_different_items_node(ItemCountI, ItemCountNI, ExpDelta),
    N_delta_NI = calc_max_different_items_node(ItemCountNI, ItemCountI, ExpDelta),
    SigSize0 = util:ceil(
                 util:log2(
                   % if the delta is low and/or FR is high, this term is negative
                   % and we cannot apply the logarithm - here, any signature
                   % size suffices and we choose the lowest non-zero value
                   erlang:max(1,
                              2 * N_delta_I * N_delta_NI / FR
                                  - N_delta_NI - ItemCountI + 2))),
    SigSize = min_max(SigSize0, get_min_hash_bits(), MaxSize),
%%     log:pal("shash [ ~p ] - FR: ~p, \tSigSize: ~B, \tIC@I: ~B, \tIC@NI: ~B~n"
%%             "  Ndelta_@I: ~B, \tNdelta_@I: ~B",
%%             [self(), FR, SigSize, ItemCountI, ItemCountNI, N_delta_I, N_delta_NI]),
    {SigSize, 0}.

%% @doc Calculates the worst-case failure rate of the SHash algorithm
%%      with the given signature size, item counts and expected delta.
%%      NOTE: Precision loss may occur for very high values!
%% @see trivial_worst_case_failrate/4
-spec shash_worst_case_failrate(
        SigSize::signature_size(), ItemCountI::non_neg_integer(),
        ItemCountNI::non_neg_integer(), ExpDelta::number()) -> float().
shash_worst_case_failrate(0, 0, _ItemCountNI, _ExpDelta) ->
    % this is exact! (see special case in trivial_signature_sizes/4)
    0.0;
shash_worst_case_failrate(0, _ItemCountI, 0, _ExpDelta) ->
    % this is exact! (see special case in trivial_signature_sizes/4)
    0.0;
shash_worst_case_failrate(SigSize, ItemCountI, ItemCountNI, ExpDelta) ->
    BK2 = util:pow(2, SigSize),
    N_delta_I = calc_max_different_items_node(ItemCountI, ItemCountNI, ExpDelta),
    N_delta_NI = calc_max_different_items_node(ItemCountNI, ItemCountI, ExpDelta),
    % exact but with problems for small 1 / BK2:
%%     2 * N_delta_I * N_delta_NI / BK2 * math:pow(1 - 1 / BK2, N_delta_NI + ItemCountI - 2),
    2 * N_delta_I * N_delta_NI / BK2 * math:exp((N_delta_NI + ItemCountI - 2) * util:log1p(-1 / BK2)).

%% @doc Calculates the bloom target FP for each of two Bloom filters so that
%%      their combined expected failure rate is below FR, assuming the worst
%%      case in the number of item checks that could yield false positives,
%%      i.e. items that are not encoded in the Bloom filter, taking the
%%      expected delta into account.
-spec bloom_target_fp(
        ItemCountI::non_neg_integer(), ItemCountNI::non_neg_integer(),
        ExpDelta::number(), FR::float()) -> TargetFP::float().
bloom_target_fp(ItemCountI, ItemCountNI, ExpDelta, FR) ->
    NrChecksNotInBFI = calc_max_different_items_node(ItemCountI, ItemCountNI, ExpDelta),
    NrChecksNotInBFNI = calc_max_different_items_node(ItemCountNI, ItemCountI, ExpDelta),
    % assume at least one comparison, even if empty sets are reconciled
    FR / erlang:max(NrChecksNotInBFI + NrChecksNotInBFNI, 1).

%% @doc Calculates the worst-case failure probability of the bloom
%%      reconciliation with the given two Bloom filters from either node.
%%      NOTE: Precision loss may occur for very high values!
-spec bloom_worst_case_failrate(
        BFI::bloom:bloom_filter(), BFNI::bloom:bloom_filter(),
        ExpDelta::number()) -> float().
bloom_worst_case_failrate(BFI, BFNI, ExpDelta) ->
    FprI = bloom:get_property(BFI, fpr),
    FprNI = bloom:get_property(BFNI, fpr),
    ItemCountI = bloom:get_property(BFI, items_count),
    ItemCountNI = bloom:get_property(BFNI, items_count),
    bloom_worst_case_failrate_(FprI, FprNI, ItemCountI, ItemCountNI, ExpDelta).

%% @doc Helper for bloom_worst_case_failrate/3.
%% @see bloom_worst_case_failrate/3
-spec bloom_worst_case_failrate_(
        FprI::float(), FprNI::float(),
        ItemCountI::non_neg_integer(), ItemCountNI::non_neg_integer(),
        ExpDelta::number()) -> float().
bloom_worst_case_failrate_(FprI, FprNI, ItemCountI, ItemCountNI, ExpDelta) ->
    ?DBG_ASSERT2(FprI >= 0 andalso FprI =< 1, FprI),
    ?DBG_ASSERT2(FprNI >= 0 andalso FprNI =< 1, FprNI),
    NrChecksNotInBFI = calc_max_different_items_node(ItemCountI, ItemCountNI, ExpDelta),
    NrChecksNotInBFNI = calc_max_different_items_node(ItemCountNI, ItemCountI, ExpDelta),
    NrChecksNotInBFI * FprNI + NrChecksNotInBFNI * FprI.

-spec build_recon_struct(
        method(), DestI::intervals:non_empty_interval(), db_chunk_kv(),
        InitiatorMaxItems::non_neg_integer() | undefined, % not applicable on iniator
        Params::parameters() | {})
        -> {sync_struct(), Fr_p1::float()}.
build_recon_struct(trivial, I, DBItems, InitiatorMaxItems, _Params) ->
    % at non-initiator
    ?DBG_ASSERT(not intervals:is_empty(I)),
    ?DBG_ASSERT(InitiatorMaxItems =/= undefined),
    ItemCount = length(DBItems),
    ExpDelta = get_max_expected_delta(),
    {MyDiffK, MyDiffV, ResortedKVOrigList, Dupes, SigSize, _VSize} =
        compress_kv_list_fr(DBItems, InitiatorMaxItems, ItemCount,
                             ExpDelta, get_failure_rate(),
                             fun trivial_signature_sizes/4, fun trivial_compress_key/2),
    {#trivial_sync{interval = I, reconPid = comm:this(), exp_delta = ExpDelta,
                   db_chunk = {MyDiffK, MyDiffV, ResortedKVOrigList, Dupes},
                   '+item_count' = length(Dupes), sig_size = SigSize},
     % Note: we can only guess the number of items of the initiator here, so
     %       this is not exactly the failure rate of phase 1!
     _Fr_p1 = trivial_worst_case_failrate(SigSize, InitiatorMaxItems, ItemCount, ExpDelta)};
build_recon_struct(shash, I, DBItems, InitiatorMaxItems, _Params) ->
    % at non-initiator
    ?DBG_ASSERT(not intervals:is_empty(I)),
    ?DBG_ASSERT(InitiatorMaxItems =/= undefined),
    ItemCount = length(DBItems),
    FR = get_failure_rate(),
    FR_p1 = calc_n_subparts_FR(2, FR),
    ExpDelta = get_max_expected_delta(),
    {MyDiffK, <<>>, ResortedKVOrigList, Dupes, SigSize, 0} =
        compress_kv_list_fr(DBItems, InitiatorMaxItems, ItemCount,
                             ExpDelta, FR_p1,
                             fun shash_signature_sizes/4, fun compress_key/2),
    {#shash_sync{interval = I, reconPid = comm:this(), exp_delta = ExpDelta,
                 db_chunk = {MyDiffK, ResortedKVOrigList, Dupes},
                 '+item_count' = length(Dupes), sig_size = SigSize,
                 fail_rate = FR},
     % Note: we can only guess the number of items of the initiator here, so
     %       this is not exactly the failure rate of phase 1!
     _Fr_p1 = shash_worst_case_failrate(SigSize, InitiatorMaxItems, ItemCount, ExpDelta)};
build_recon_struct(bloom, I, DBItems, InitiatorMaxItems, _Params) ->
    % at non-initiator
    ?DBG_ASSERT(not intervals:is_empty(I)),
    ?DBG_ASSERT(InitiatorMaxItems =/= undefined),
    MyMaxItems = length(DBItems),
    FR = get_failure_rate(),
    FR_p1 = calc_n_subparts_FR(2, FR),
    ExpDelta = get_max_expected_delta(),
    % decide for a common Bloom filter size (and number of hash functions)
    % for an efficient diff BF - use a combination where both Bloom filters
    % are below the targeted FR_p1_bf (we may thus not reach FR_p1 exactly)
    % based on a common target FPt:
    FPt = bloom_target_fp(InitiatorMaxItems, MyMaxItems, ExpDelta, FR_p1),
    {K1, M1} = bloom:calc_HF_num_Size_opt(MyMaxItems, FPt),
    {K2, M2} = bloom:calc_HF_num_Size_opt(InitiatorMaxItems, FPt),
    % total failure rates in phase 1 for the two options {K1, M1} vs. {K2, M2}
    FP1_I = bloom:calc_FPR(M1, InitiatorMaxItems, K1),
    FP1_NI = bloom:calc_FPR(M1, MyMaxItems, K1),
    Fr1_p1 = bloom_worst_case_failrate_(FP1_I, FP1_NI, InitiatorMaxItems,
                                        MyMaxItems, ExpDelta),
    FP2_I = bloom:calc_FPR(M2, InitiatorMaxItems, K2),
    FP2_NI = bloom:calc_FPR(M2, MyMaxItems, K2),
    Fr2_p1 = bloom_worst_case_failrate_(FP2_I, FP2_NI, InitiatorMaxItems,
                                        MyMaxItems, ExpDelta),
    ?ALG_DEBUG("Bloom My: ~B OtherMax: ~B FPt: ~p~n  bloom1: ~p~n  bloom2: ~p",
               [MyMaxItems, InitiatorMaxItems, FPt,
                {K1, M1, FP1_I, FP1_NI}, {K2, M2, FP2_I, FP2_NI}]),
    {K, M, Fr_p1} = bloom:select_best(FR_p1, K1, M1, Fr1_p1, K2, M2, Fr2_p1),
    BF0 = bloom:new(M, K),
    if Fr_p1 > FR_p1 ->
           log:log("~w: [ ~.0p:~.0p ] FR constraint for phase 1 probably broken",
                   [?MODULE, pid_groups:my_groupname(), self()]);
       true -> ok
    end,
    ?ALG_DEBUG("NI:~.0p, FR(phase1)=~p, ExpDelta = ~p~n"
               "  m=~B k=~B NICount=~B ICount=~B~n"
               "  fr(phase1)=~p",
               [comm:this(), FR_p1, ExpDelta, M, K,
                MyMaxItems, InitiatorMaxItems, Fr_p1]),
    BF = bloom:add_list(BF0, DBItems),
    {#bloom_sync{interval = I, reconPid = comm:this(), exp_delta = ExpDelta,
                 bf = BF, item_count = MyMaxItems, hf_count = K,
                 fail_rate = FR},
     % Note: we can only guess the number of items of the initiator here, so
     %       this is not exactly the failure rate of phase 1!
     Fr_p1};
build_recon_struct(merkle_tree, I, DBItems, _InitiatorMaxItems, Params) ->
    ?DBG_ASSERT(not intervals:is_empty(I)),
    Fr_p1 = 0.0, % needs to be set at the end of phase 1!
    case Params of
        {} ->
            % merkle_tree - at non-initiator!
            ?DBG_ASSERT(_InitiatorMaxItems =/= undefined),
            MOpts = [{branch_factor, get_merkle_branch_factor()},
                     {bucket_size, get_merkle_bucket_size()}],
            % do not build the real tree here - build during begin_sync so that
            % the initiator can start creating its struct earlier and in parallel
            % the actual build process is executed in begin_sync/2
            {merkle_tree:new(I, [{keep_bucket, true} | MOpts]), Fr_p1};
        #merkle_params{branch_factor = BranchFactor,
                       bucket_size = BucketSize,
                       num_trees = NumTrees} ->
            % merkle_tree - at initiator!
            ?DBG_ASSERT(_InitiatorMaxItems =:= undefined),
            MOpts = [{branch_factor, BranchFactor},
                     {bucket_size, BucketSize}],
            % build now
            RootsI = intervals:split(I, NumTrees),
            ICBList = merkle_tree:keys_to_intervals(DBItems, RootsI),
            {[merkle_tree:get_root(
               merkle_tree:new(SubI, Bucket, [{keep_bucket, true} | MOpts]))
               || {SubI, _Count, Bucket} <- ICBList], Fr_p1};
        #art_recon_struct{branch_factor = BranchFactor,
                          bucket_size = BucketSize} ->
            % ART at initiator
            MOpts = [{branch_factor, BranchFactor},
                     {bucket_size, BucketSize},
                     {leaf_hf, fun art:merkle_leaf_hf/2}],
            {merkle_tree:new(I, DBItems, [{keep_bucket, true} | MOpts]),
             Fr_p1
            }
    end;
build_recon_struct(art, I, DBItems, _InitiatorMaxItems, _Params = {}) ->
    % ART at non-initiator
    ?DBG_ASSERT(not intervals:is_empty(I)),
    ?DBG_ASSERT(_InitiatorMaxItems =/= undefined),
    BranchFactor = get_merkle_branch_factor(),
    BucketSize = merkle_tree:get_opt_bucket_size(length(DBItems), BranchFactor, 1),
    Tree = merkle_tree:new(I, DBItems, [{branch_factor, BranchFactor},
                                        {bucket_size, BucketSize},
                                        {leaf_hf, fun art:merkle_leaf_hf/2},
                                        {keep_bucket, true}]),
    % create art struct:
    {#art_recon_struct{art = art:new(Tree, get_art_config()),
                       reconPid = comm:this(),
                       branch_factor = BranchFactor,
                       bucket_size = BucketSize},
     _Fr_p1 = 0.0
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
-spec send_chunk_req(DhtPid::LPid, AnswerPid::LPid, ChunkI::intervals:interval(),
                     MaxItems::pos_integer() | all) -> ok when
    is_subtype(LPid,        comm:erl_local_pid()).
send_chunk_req(DhtPid, SrcPid, I, MaxItems) ->
    SrcPidReply = comm:reply_as(SrcPid, 2, {process_db, '_'}),
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
% rr_recon_failure_rate     - expected number of failures during the
%                             (approximate) reconciliation process
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
     % deprecated, old, and unused config parameter
    case config:read(rr_recon_p1e) of
        failed -> true;
        _ -> error_logger:error_msg("rr_recon_p1e parameter exists - "
                                    "please migrate to rr_recon_failure_rate "
                                    "(see scalaris.cfg and scalaris.local.cfg)~n"),
             false
    end andalso
        config:cfg_is_in(rr_recon_method, [trivial, shash, bloom, merkle_tree, art]) andalso
        config:cfg_is_number(rr_recon_failure_rate) andalso
        config:cfg_is_greater_than(rr_recon_failure_rate, 0) andalso
        config:cfg_is_number(rr_recon_expected_delta) andalso
        config:cfg_is_in_range(rr_recon_expected_delta, 0, 100) andalso
        config:cfg_is_integer(rr_recon_version_bits) andalso
        config:cfg_is_greater_than(rr_recon_version_bits, 0) andalso
        config:cfg_test_and_error(rr_max_items,
                                  fun(all) -> true;
                                     (X) -> erlang:is_integer(X) andalso X > 0
                                  end, "is not 'all' or an integer > 0"),
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

-spec get_failure_rate() -> float().
get_failure_rate() ->
    config:read(rr_recon_failure_rate).

%% @doc Specifies what the maximum expected delta is (in percent between 0 and
%%      100, inclusive). The failure probabilities will take this into account.
-spec get_max_expected_delta() -> number().
get_max_expected_delta() ->
    config:read(rr_recon_expected_delta).

%% @doc Use at least these many bits for compressed version numbers.
-spec get_min_version_bits() -> pos_integer().
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
    config:read(rr_max_items).

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

-spec tester_create_kvi_tree(
        [{KeyShort::non_neg_integer(),
          {VersionShort::non_neg_integer(), Idx::non_neg_integer()}}]) -> kvi_tree().
tester_create_kvi_tree(KVList) ->
    mymaps:from_list(KVList).

-spec tester_is_kvi_tree(Map::any()) -> boolean().
tester_is_kvi_tree(Map) ->
    try mymaps:to_list(Map) of
        KVList -> lists:all(fun({K, {V, Idx}}) when is_integer(K) andalso K >= 0
                                 andalso is_integer(V) andalso V >= 0
                                 andalso is_integer(Idx) andalso Idx >= 0 ->
                                    true;
                               ({_, _}) ->
                                    false
                            end, KVList)
    catch _:_ -> false % probably no map
    end.
