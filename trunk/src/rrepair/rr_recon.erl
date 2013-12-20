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
-export([map_key_to_interval/2, map_key_to_quadrant/2, map_interval/2,
         quadrant_intervals/0]).
-export([get_chunk_kv/1, get_chunk_kvv/1, get_chunk_filter/1]).
%-export([compress_kv_list/4, calc_signature_size_1_to_n/3, calc_signature_size_n_pair/3]).

%export for testing
-export([find_sync_interval/2, quadrant_subints_/3]).
-export([merkle_compress_hashlist/3, merkle_decompress_hashlist/3,
         merkle_compress_cmp_result/4, merkle_decompress_cmp_result/4]).

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
-export_type([method/0, request/0, merkle_cmp_result/0,
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
-type db_chunk_kvv()   :: [{?RT:key(), db_dht:version(), db_dht:value()}].

-type signature_size() :: 1..160. % upper bound of 160 (SHA-1) to also limit testing

-record(trivial_recon_struct,
        {
         interval = intervals:empty()                         :: intervals:interval(),
         reconPid = undefined                                 :: comm:mypid() | undefined,
         db_chunk = ?required(trivial_recon_struct, db_chunk) :: bitstring() | gb_tree(),
         sig_size = 128                                       :: signature_size(),
         ver_size = 8                                         :: signature_size()
        }).

-record(bloom_recon_struct,
        {
         interval = intervals:empty()                       :: intervals:interval(),
         bloom    = ?required(bloom_recon_struct, bloom)    :: bloom:bloom_filter()
        }).

-record(merkle_params,
        {
         interval       = ?required(merkle_param, interval)       :: intervals:interval(),
         reconPid       = undefined                               :: comm:mypid() | undefined,
         branch_factor  = ?required(merkle_param, branch_factor)  :: pos_integer(),
         bucket_size    = ?required(merkle_param, bucket_size)    :: pos_integer()
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

-record(rr_recon_state,
        {
         ownerPid           = ?required(rr_recon_state, ownerPid)    :: comm:erl_local_pid(),
         dhtNodePid         = ?required(rr_recon_state, dhtNodePid)  :: comm:erl_local_pid(),
         dest_rr_pid        = ?required(rr_recon_state, dest_rr_pid) :: comm:mypid(), %dest rrepair pid
         dest_recon_pid     = undefined                              :: comm:mypid() | undefined, %dest recon process pid
         method             = undefined                              :: method() | undefined,
         dest_interval      = intervals:empty()                      :: intervals:interval(),
         my_sync_interval   = intervals:empty()                      :: intervals:interval(),
         params             = {}                                     :: parameters() | {}, % parameters from the other node
         struct             = {}                                     :: sync_struct() | {}, % my recon structure
         stage              = req_shared_interval                    :: stage(),
         initiator          = false                                  :: boolean(),
         misc               = []                                     :: [{atom(), term()}], % any optional parameters an algorithm wants to keep
         kv_list            = []                                     :: db_chunk_kv(),
         stats              = rr_recon_stats:new()                   :: rr_recon_stats:stats(),
         to_resolve         = {[], []}                               :: {ToSend::rr_resolve:kvv_list(), ToReq::[?RT:key()]}
         }).
-type state() :: #rr_recon_state{}.

% keep in sync with check_node/2
-define(recon_ok,         1).
-define(recon_fail_stop,  2).
-define(recon_fail_cont,  3).
-define(recon_fail_cont2, 0). % failed leaf with included hash

-type merkle_cmp_result()  :: ?recon_ok |
                              ?recon_fail_stop |
                              {merkle_tree:mt_node_key()} |
                              ?recon_fail_cont.
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
    % merkle tree sync messages
    {?check_nodes, SenderPid::comm:mypid(), ToCheck::bitstring(), SigSize::signature_size()} |
    {?check_nodes, ToCheck::bitstring(), SigSize::signature_size()} |
    {?check_nodes_response, Results::bitstring(), HashKeys::bitstring(), MaxLeafCount::non_neg_integer()} |
    % dht node response
    {create_struct2, {get_state_response, MyI::intervals:interval()}} |
    {create_struct2, DestI::intervals:interval(),
     {get_chunk_response, {intervals:interval(), db_chunk_kv()}}} |
    {reconcile, {get_chunk_response, {intervals:interval(), db_chunk_kv()}}} |
    {resolve, {get_chunk_response, {intervals:interval(), db_chunk_kvv()}}} |
    % internal
    {shutdown, exit_reason()} |
    {crash, DeadPid::comm:mypid()} |
    {'DOWN', MonitorRef::reference(), process, Owner::comm:erl_local_pid(), Info::any()}
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

on({start_recon, RMethod, Params} = _Msg, State) ->
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
            ?ASSERT(DestReconPid =/= undefined),
            fd:subscribe(DestReconPid),
            % convert db_chunk to a gb_tree for faster access checks
            DBChunkTree =
                decompress_kv_list(DBChunk, gb_trees:empty(), SigSize, VSize),
            Params1 = Params#trivial_recon_struct{db_chunk = DBChunkTree},
            Reconcile = resolve,
            Stage = resolve;
        bloom ->
            MySyncI = Params#bloom_recon_struct.interval,
            DestReconPid = undefined,
            Params1 = Params,
            Reconcile = reconcile,
            Stage = reconciliation;
        merkle_tree ->
            #merkle_params{interval = MySyncI, reconPid = DestReconPid} = Params,
            ?ASSERT(DestReconPid =/= undefined),
            fd:subscribe(DestReconPid),
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
    ?ASSERT(not intervals:is_empty(MySyncI)),
    
    DhtNodePid = State#rr_recon_state.dhtNodePid,
    send_chunk_req(DhtNodePid, self(), MySyncI, MySyncI, get_max_items(), Reconcile),
    State#rr_recon_state{stage = Stage, params = Params1,
                         method = RMethod, initiator = true,
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
        get_diff(DBList, OtherDBChunk, ToSend, ToReq, SigSize, VSize),

    %if rest interval is non empty start another sync
    SID = rr_recon_stats:get(session_id, Stats),
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(DhtNodePid, self(), RestI, RestI, get_max_items(), resolve);
       true -> ok
    end,
    ?TRACE("Reconcile Trivial Session=~p ; ToSend=~p ; ToReq=~p",
           [SID, length(ToSend1), length(ToReq1)]),
    NewStats =
        if ToSend1 =/= [] orelse ToReq1 =/= [] orelse SyncFinished ->
               send(DestRR_Pid, {request_resolve, SID,
                                 {?key_upd, ToSend1, ToReq1},
                                 [{from_my_node, 0},
                                  {feedback_request, comm:make_global(OwnerL)}]}),
               % we will get one reply from a subsequent feedback response
               rr_recon_stats:inc([{resolve_started, 1},
                                   {await_rs_fb, 1}], Stats);
           true ->
               Stats
        end,
    Params1 = Params#trivial_recon_struct{db_chunk = OtherDBChunk1},
    NewState = State#rr_recon_state{to_resolve = {[], []}},
    if SyncFinished ->
           % let the non-initiator's rr_recon process identify the remaining keys
           Req2Count = gb_trees:size(OtherDBChunk1),
           ToReq2 = util:gb_trees_foldl(
                      fun(KeyBin, _VersionShort, Acc) ->
                              <<Acc/bitstring, KeyBin/bitstring>>
                      end, <<>>, OtherDBChunk1),
           NewStats2 =
               if Req2Count > 0 ->
                      rr_recon_stats:inc([{resolve_started, 1}], NewStats);
                  true -> NewStats
               end,

           ?TRACE("resolve_req Trivial Session=~p ; ToReq=~p",
                  [SID, Req2Count]),
           comm:send(DestReconPid, {resolve_req, ToReq2, SigSize}),
           
           Params2 = Params1#trivial_recon_struct{db_chunk = gb_trees:empty()},
           shutdown(sync_finished,
                    NewState#rr_recon_state{stats = NewStats2, params = Params2});
       true ->
           NewState#rr_recon_state{stats = NewStats, params = Params1}
    end;

on({reconcile, {get_chunk_response, {RestI, DBList0}}} = _Msg,
   State = #rr_recon_state{stage = reconciliation,     initiator = true,
                           method = bloom,             dhtNodePid = DhtNodePid,
                           params = #bloom_recon_struct{bloom = BF},
                           dest_rr_pid = DestRR_Pid,   stats = Stats,
                           ownerPid = OwnerL}) ->
    ?TRACE1(_Msg, State),
    % no need to map keys since the other node's bloom filter was created with
    % keys mapped to our interval
    Diff = case bloom:item_count(BF) of
               0 -> DBList0;
               _ -> [X || X <- DBList0, not bloom:is_element(BF, X)]
           end,
    %if rest interval is non empty start another sync
    SID = rr_recon_stats:get(session_id, Stats),
    SyncFinished = intervals:is_empty(RestI),
    if not SyncFinished ->
           send_chunk_req(DhtNodePid, self(), RestI, RestI, get_max_items(), reconcile);
       true -> ok
    end,
    ?TRACE("Reconcile Bloom Session=~p ; Diff=~p", [SID, length(Diff)]),
    NewStats =
        case Diff of
            [_|_] ->
                send(DestRR_Pid, {request_resolve, SID,
                                  {?key_upd2, Diff, comm:make_global(OwnerL)},
                                    [{from_my_node, 0}]}),
                % we will get one reply from a subsequent ?key_upd resolve
                rr_recon_stats:inc([{resolve_started, 1}], Stats);
            [] -> Stats
        end,
    NewState = State#rr_recon_state{stats = NewStats},
    if SyncFinished -> shutdown(sync_finished, NewState);
       true         -> NewState
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

on({crash, _Pid} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    shutdown(recon_node_crash, State);

on({shutdown, Reason}, State) ->
    shutdown(Reason, State);

on({'DOWN', _MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% trivial reconciliation sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({resolve_req, BinKeys, SigSize} = _Msg,
   State = #rr_recon_state{stage = resolve,           initiator = false,
                           dest_rr_pid = DestRRPid,   ownerPid = OwnerL,
                           kv_list = KVList,          stats = Stats}) ->
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
                % note: the resolve request was counted at the initiator and
                %       thus from_my_node must be 0 on this node!
                send_local(OwnerL, {request_resolve, SID,
                                    {key_upd_send, DestRRPid, ReqKeys},
                                    [{feedback_request, comm:make_global(OwnerL)},
                                     {from_my_node, 0}]}),
                % we will get one reply from a subsequent ?key_upd resolve
                rr_recon_stats:inc([{resolve_started, 1}], Stats)
        end,
    shutdown(sync_finished, State#rr_recon_state{stats = NewStats});

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merkle tree sync messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({?check_nodes, SenderPid, ToCheck, SigSize}, State) ->
    NewState = State#rr_recon_state{dest_recon_pid = SenderPid},
    on({?check_nodes, ToCheck, SigSize}, NewState);

on({?check_nodes, ToCheck0, SigSize},
   State = #rr_recon_state{ stage = reconciliation,    initiator = false,
                            struct = Tree,             ownerPid = OwnerL,
                            dest_rr_pid = DestNodePid, stats = Stats,
                            dest_recon_pid = DestReconPid }) ->
    ?ASSERT(comm:is_valid(DestReconPid)),
    ToCheck = merkle_decompress_hashlist(ToCheck0, [], SigSize),
    {Result, RestTree, ToResolve, MaxLeafCount} =
        check_node(ToCheck, Tree, SigSize),
    {Flags, HashKeys} =
        merkle_compress_cmp_result(Result, <<>>, <<>>, SigSize),
    ToResolveLength = length(ToResolve),
    NStats =
        if ToResolveLength > 0 ->
               SID = rr_recon_stats:get(session_id, Stats),
               ResolveCount = resolve_leaves(ToResolve, DestNodePid, SID, OwnerL),
               rr_recon_stats:inc([{tree_leavesSynced, ToResolveLength},
                                   {resolve_started, ResolveCount}], Stats);
           true -> Stats
        end,
    send(DestReconPid, {?check_nodes_response, Flags, HashKeys, MaxLeafCount}),
    State#rr_recon_state{ struct = RestTree, stats = NStats };

on({?check_nodes_response, Flags, HashKeys, OtherMaxLeafCount}, State =
       #rr_recon_state{ stage = reconciliation,        initiator = true,
                        struct = Tree,                 ownerPid = OwnerL,
                        dest_rr_pid = DestNodePid,     stats = Stats,
                        dest_recon_pid = DestReconPid, params = Params,
                        misc = [{signature_size, SigSize}] }) ->
    CmpResults = merkle_decompress_cmp_result(Flags, HashKeys, [], SigSize),
    SID = rr_recon_stats:get(session_id, Stats),
    case process_tree_cmp_result(CmpResults, Tree,
                                 Params#merkle_params.branch_factor,
                                 SigSize, Stats) of
        {[], ToResolve, NStats0, RTree, _MyMaxLeafCount} ->
            NextSigSize = SigSize,
            ok;
        {[_|_] = Req0, ToResolve, NStats0, RTree, MyMaxLeafCount} ->
            % note: we have m one-to-n comparisons, assuming the probability of
            %       a failure in a single one-to-n comparison is p, the overall
            %       p1e = 1 - (1-p)^n  <=>  p = 1 - (1 - p1e)^(1/n)
            P = 1 - math:pow(1 - get_p1e(), 1 / erlang:max(1, length(Req0))),
            NextSigSize = calc_signature_size_1_to_n(
                            erlang:max(MyMaxLeafCount, OtherMaxLeafCount),
                            P, 160),
%%             log:pal("MyMLC: ~p~n    OtherMLC: ~p~n     SigSize: ~p",
%%                     [MyMaxLeafCount, OtherMaxLeafCount, NextSigSize]),
            Req = merkle_compress_hashlist(Req0, <<>>, NextSigSize),
            send(DestReconPid, {?check_nodes, Req, NextSigSize})
    end,
    ResolveCount = resolve_leaves(ToResolve, DestNodePid, SID, OwnerL),
    NStats = rr_recon_stats:inc([{tree_leavesSynced, length(ToResolve)},
                                 {resolve_started, ResolveCount}], NStats0),
    CompLeft = rr_recon_stats:get(tree_compareLeft, NStats),
    NewState = State#rr_recon_state{stats = NStats, struct = RTree,
                                    misc = [{signature_size, NextSigSize}]},
    if CompLeft =:= 0 ->
           send(DestReconPid, {shutdown, sync_finished_remote}),
           shutdown(sync_finished, NewState);
       true -> NewState
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_struct(DBList::db_chunk_kv(), DestI::intervals:non_empty_interval(),
                   RestI::intervals:interval(), state()) -> state() | kill.
build_struct(DBList, DestI, RestI,
             State = #rr_recon_state{method = RMethod, params = Params,
                                     struct = OldSyncStruct,
                                     initiator = Initiator, stats = Stats,
                                     dhtNodePid = DhtNodePid, stage = Stage,
                                     kv_list = KVList}) ->
    ?ASSERT(not intervals:is_empty(DestI)),
    % bloom may fork more recon processes if un-synced elements remain
    BeginSync =
        case intervals:is_empty(RestI) of
            false ->
                SubSyncI = map_interval(DestI, RestI),
                case intervals:is_empty(SubSyncI) of
                    false ->
                        MySubSyncI = map_interval(RestI, DestI), % mapped to my range
                        Reconcile =
                            if Initiator andalso (Stage =:= reconciliation) -> reconcile;
                               true -> create_struct
                            end,
                        send_chunk_req(DhtNodePid, self(), MySubSyncI, SubSyncI,
                                       get_max_items(), Reconcile),
                        false;
                    true -> true
                end;
            true -> true
        end,
    NewKVList = lists:append(KVList, DBList),
    if BeginSync ->
           ToBuild = if Initiator andalso RMethod =:= art -> merkle_tree;
                        true -> RMethod
                     end,
           {BuildTime, SyncStruct} =
               util:tc(fun() -> build_recon_struct(ToBuild, OldSyncStruct, DestI,
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
    shutdown(sync_finished, State#rr_recon_state{kv_list = []});
begin_sync(MySyncStruct, _OtherSyncStruct,
           State = #rr_recon_state{method = merkle_tree, initiator = Initiator,
                                   ownerPid = OwnerL, stats = Stats,
                                   dest_recon_pid = DestReconPid,
                                   dest_rr_pid = DestRRPid}) ->
    ?TRACE("BEGIN SYNC", []),
    Stats1 =
        rr_recon_stats:set(
          [{tree_compareLeft, ?IIF(Initiator, 1, 0)},
           {tree_size, merkle_tree:size_detail(MySyncStruct)}], Stats),
    case Initiator of
        true ->
            SigSize = 160,
            Req = merkle_compress_hashlist([merkle_tree:get_root(MySyncStruct)],
                                           <<>>, SigSize),
            send(DestReconPid, {?check_nodes, comm:this(), Req, SigSize}),
            State#rr_recon_state{stats = Stats1,
                                 misc = [{signature_size, SigSize}],
                                 kv_list = []};
        false ->
            MerkleI = merkle_tree:get_interval(MySyncStruct),
            MerkleV = merkle_tree:get_branch_factor(MySyncStruct),
            MerkleB = merkle_tree:get_bucket_size(MySyncStruct),
            MySyncParams = #merkle_params{interval = MerkleI,
                                          branch_factor = MerkleV,
                                          bucket_size = MerkleB},
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
                                 dest_recon_pid = DestRC}) ->
    ?TRACE("SHUTDOWN Session=~p Reason=~p",
           [rr_recon_stats:get(session_id, Stats), Reason]),
    Status = exit_reason_to_rc_status(Reason),
    NewStats = rr_recon_stats:set([{status, Status}], Stats),
    send_local(OwnerL, {recon_progress_report, comm:this(), Initiator, DestRR,
                        DestRC, NewStats}),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% KV-List compression
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculates the minimum number of bits needed to have a hash collision
%%      probability of P1E, given we compare N hashes pairwise with each other.
-spec calc_signature_size_n_pair(N::non_neg_integer(), P1E::float(),
                                 MaxSize::signature_size())
        -> SigSize::signature_size().
calc_signature_size_n_pair(0, P1E, _MaxSize) when P1E > 0 ->
    1;
calc_signature_size_n_pair(N, P1E, MaxSize) when P1E > 0 ->
    erlang:min(MaxSize, erlang:max(1, util:ceil(util:log2(N * (N - 1) / P1E) - 1))).

%% @doc Transforms a list of key and version tuples (with unique keys), into a
%%      compact binary representation for transfer.
-spec compress_kv_list(KVList::[{?RT:key(), db_dht:version()}], Bin,
                       SigSize::signature_size(), VSize::signature_size())
        -> Bin when is_subtype(Bin, bitstring()).
compress_kv_list(KVList, Bin, SigSize, VSize) ->
    compress_kv_list_(KVList, Bin, SigSize, VSize, util:pow(2, VSize)).

%% @doc Helper for compress_kv_list/4.
-spec compress_kv_list_(KVList::[{?RT:key(), db_dht:version()}], Bin,
                       SigSize::signature_size(), VSize::signature_size(),
                       VMod::pos_integer())
        -> Bin when is_subtype(Bin, bitstring()).
compress_kv_list_([], Bin, _SigSize, _VSize, _VMod) ->
    Bin;
compress_kv_list_([{K0, V0} | TL], Bin, SigSize, VSize, VMod) ->
    KBin = compress_key(K0, SigSize),
    V = V0 rem VMod,
    compress_kv_list_(TL, <<Bin/bitstring, KBin/bitstring, V:VSize>>, SigSize, VSize, VMod).

%% @doc De-compresses the binary from compress_kv_list/4 into a gb_tree with a
%%      binary key representation and the integer of the (shortened) version.
-spec decompress_kv_list(CompressedBin::bitstring(), AccTree::gb_tree(),
                         SigSize::signature_size(), VSize::signature_size())
        -> ResTree::gb_tree().
decompress_kv_list(<<>>, Tree, _SigSize, _VSize) ->
    Tree;
decompress_kv_list(Bin, Tree, SigSize, VSize) ->
    <<KeyBin:SigSize/bitstring, Version:VSize, T/bitstring>> = Bin,
    Tree1 = gb_trees:enter(KeyBin, Version, Tree),
    decompress_kv_list(T, Tree1, SigSize, VSize).

-spec get_diff(MyEntries::db_chunk_kvv(), MyIOtherKvTree::gb_tree(),
               AccFBItems::rr_resolve:kvv_list(), AccReqItems::[?RT:key()],
               SigSize::signature_size(), VSize::signature_size())
        -> {FBItems::rr_resolve:kvv_list(), ReqItems::[?RT:key()], MyIOtherKvTree::gb_tree()}.
get_diff([], MyIOtherKvTree, FBItems, ReqItems, _SigSize, _VSize) ->
    {FBItems, ReqItems, MyIOtherKvTree};
get_diff([{Key, Version, Value} | Rest], MyIOtherKvTree, FBItems, ReqItems, SigSize, VSize) ->
    {KeyBin, VersionShort} = compress_kv_pair(Key, Version, SigSize, VSize),
    case gb_trees:lookup(KeyBin, MyIOtherKvTree) of
        none ->
            get_diff(Rest, MyIOtherKvTree, [{Key, Value, Version} | FBItems],
                     ReqItems, SigSize, VSize);
        {value, OtherVersionShort} ->
            MyIOtherKvTree2 = gb_trees:delete(KeyBin, MyIOtherKvTree),
            if VersionShort > OtherVersionShort ->
                   get_diff(Rest, MyIOtherKvTree2,
                            [{Key, Value, Version} | FBItems], ReqItems,
                            SigSize, VSize);
               VersionShort =:= OtherVersionShort ->
                   get_diff(Rest, MyIOtherKvTree2, FBItems, ReqItems,
                            SigSize, VSize);
               true ->
                   get_diff(Rest, MyIOtherKvTree2, FBItems, [Key | ReqItems],
                            SigSize, VSize)
            end
    end.

%% @doc Transforms a single key and version tuple into a compact binary
%%      representation.
%%      Similar to compress_kv_list/4.
-spec compress_kv_pair(Key::?RT:key(), Version::db_dht:version(),
                       SigSize::signature_size(), VSize::signature_size())
        -> {BinKey::bitstring(), VersionShort::integer()}.
compress_kv_pair(Key, Version, SigSize, VSize) ->
    compress_kv_pair_(Key, Version, SigSize, util:pow(2, VSize)).

%% @doc Helper for compress_kv_pair/4.
-spec compress_kv_pair_(Key::?RT:key(), Version::db_dht:version(),
                        SigSize::signature_size(), VMod::pos_integer())
        -> {BinKey::bitstring(), VersionShort::integer()}.
compress_kv_pair_(Key, Version, SigSize, VMod) ->
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
    <<_:RestSize/bitstring, KBinCompressed:SigSize/bitstring>> = KBin,
    KBinCompressed.

%% @doc De-compresses a bitstring with hashes of SigSize number of bits
%%      into a gb_set with a binary key representation.
-spec decompress_k_list(CompressedBin::bitstring(), AccSet::gb_set(),
                         SigSize::signature_size()) -> ResSet::gb_set().
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
    erlang:min(MaxSize, erlang:max(1, util:ceil(util:log2(N / P1E)))).

%% @doc Transforms a list of merkle keys, i.e. hashes, into a compact binary
%%      representation for transfer.
-spec merkle_compress_hashlist(Nodes::[merkle_tree:mt_node()], Bin,
                               SigSize::signature_size()) -> Bin
    when is_subtype(Bin, bitstring()).
merkle_compress_hashlist([], Bin, _SigSize) ->
    Bin;
merkle_compress_hashlist([N1 | TL], Bin, SigSize) ->
    H1 = merkle_tree:get_hash(N1),
    IsLeaf = ?IIF(merkle_tree:is_leaf(N1), 1, 0),
    merkle_compress_hashlist(TL, <<Bin/bitstring, H1:SigSize, IsLeaf:1>>, SigSize).

%% @doc Transforms the compact binary representation of merkle hash lists from
%%      merkle_compress_hashlist/2 back into the original form.
-spec merkle_decompress_hashlist(bitstring(), Hashes, SigSize::signature_size()) -> Hashes
    when is_subtype(Hashes, [merkle_cmp_request()]).
merkle_decompress_hashlist(<<>>, HashListR, _SigSize) ->
    lists:reverse(HashListR);
merkle_decompress_hashlist(Bin, HashListR, SigSize) ->
    <<Hash:SigSize/integer-unit:1, IsLeaf0:1, T/bitstring>> = Bin,
    IsLeaf = if IsLeaf0 =:= 1 -> true;
                true          -> false
             end,
    merkle_decompress_hashlist(T, [{Hash, IsLeaf} | HashListR], SigSize).

%% @doc Transforms merkle compare results into a compact representation for
%%      transfer.
-spec merkle_compress_cmp_result(
        [merkle_cmp_result()], FlagsIN::Bin, HashesBinIN::Bin,
        SigSize::signature_size()) -> {FlagsOUT::Bin, HashesBinOUT::Bin}
    when is_subtype(Bin, bitstring()).
merkle_compress_cmp_result([], Flags, HashesBin, _SigSize) ->
    {Flags, HashesBin};
merkle_compress_cmp_result([H1 | TL], Bin, HashesBin, SigSize) ->
    case H1 of
        {K} ->
            % compress key similar to merkle_compress_hashlist/3
            merkle_compress_cmp_result(TL, <<Bin/bitstring, ?recon_fail_cont2:2>>,
                                       <<HashesBin/bitstring, K:SigSize>>, SigSize);
        _ ->
            ?ASSERT(is_integer(H1) andalso 0 =< H1 andalso H1 < 4),
            merkle_compress_cmp_result(TL, <<Bin/bitstring, H1:2>>, HashesBin, SigSize)
    end.

%% @doc Transforms the compact representation of merkle compare results from
%%      merkle_compress_cmp_result/3 back into the original form.
-spec merkle_decompress_cmp_result(Flags::Bin, HashesBin::Bin, CmpRes,
                                   SigSize::signature_size()) -> CmpRes
    when is_subtype(Bin, bitstring()),
         is_subtype(CmpRes, [merkle_cmp_result()]).
merkle_decompress_cmp_result(<<>>, <<>>, CmpRes, _SigSize) ->
    lists:reverse(CmpRes);
merkle_decompress_cmp_result(<<?recon_fail_cont2:2, T/bitstring>>, HashesBin, CmpRes, SigSize) ->
    % similar to merkle_decompress_hashlist/3
    <<K:SigSize/integer-unit:1, HashesBinRest/bitstring>> = HashesBin,
    merkle_decompress_cmp_result(T, HashesBinRest, [{K} | CmpRes], SigSize);
merkle_decompress_cmp_result(<<X:2, T/bitstring>>, HashesBin, CmpRes, SigSize) ->
    merkle_decompress_cmp_result(T, HashesBin, [X | CmpRes], SigSize).

%% @doc Compares the given Hashes from the other node with my merkle_tree nodes
%%      (executed on non-initiator).
%%      Returns the comparison results and the rest nodes to check in a next
%%      step.
-spec check_node(Hashes::[merkle_cmp_request()],
                 merkle_tree:merkle_tree() | NodeList, SigSize::signature_size())
        -> {[merkle_cmp_result()], RestTree::NodeList, ResolveReq::NodeList,
            MaxLeafCount::non_neg_integer()}
    when is_subtype(NodeList, [merkle_tree:mt_node()]).
check_node(Hashes, Tree, SigSize) ->
    TreeNodes = case merkle_tree:is_merkle_tree(Tree) of
                    false -> Tree;
                    true -> [merkle_tree:get_root(Tree)]
                end,
    p_check_node(Hashes, TreeNodes, SigSize, [], [], [], 0).

%% @doc Helper for check_node/2.
-spec p_check_node(Hashes::[merkle_cmp_request()], MyNodes::NodeList,
                   SigSize::signature_size(),
                   Result::[merkle_cmp_result()], RestTreeIn::[NodeList],
                   AccResolve::NodeList, AccMLC::MaxLeafCount)
        -> {[merkle_cmp_result()], RestTreeOut::NodeList, ResolveReq::NodeList,
            MaxLeafCount}
    when
      is_subtype(NodeList, [merkle_tree:mt_node()]),
      is_subtype(MaxLeafCount, non_neg_integer()).
p_check_node([], [], _SigSize, AccR, AccN, AccRes, AccMLC) ->
    {lists:reverse(AccR), lists:append(lists:reverse(AccN)), AccRes, AccMLC};
p_check_node([{Hash, IsLeafHash} | TK], [Node | TN], SigSize, AccR, AccN,
             AccRes, AccMLC) ->
    IsLeafNode = merkle_tree:is_leaf(Node),
    NodeHash0 = merkle_tree:get_hash(Node),
    <<NodeHash:SigSize/integer-unit:1>> = <<NodeHash0:SigSize>>,
    if Hash =:= NodeHash andalso IsLeafHash =:= IsLeafNode ->
           p_check_node(TK, TN, SigSize, [?recon_ok | AccR], AccN,
                        AccRes, AccMLC);
       IsLeafNode andalso IsLeafHash ->
           p_check_node(TK, TN, SigSize, [?recon_fail_stop | AccR], AccN,
                        [Node | AccRes], AccMLC);
       IsLeafNode andalso (not IsLeafHash) ->
           p_check_node(TK, TN, SigSize, [{NodeHash} | AccR], AccN,
                        AccRes, AccMLC);
       (not IsLeafNode) andalso IsLeafHash ->
           ToResolve = merkle_get_sync_leaves([Node], Hash, SigSize, AccRes),
           p_check_node(TK, TN, SigSize, [?recon_fail_stop | AccR], AccN,
                        ToResolve, AccMLC);
       (not IsLeafNode) andalso (not IsLeafHash) ->
           NewAccMLC = erlang:max(AccMLC, merkle_tree:get_leaf_count(Node)),
           Childs = merkle_tree:get_childs(Node),
           p_check_node(TK, TN, SigSize, [?recon_fail_cont | AccR], [Childs | AccN],
                        AccRes, NewAccMLC)
    end.

%% @doc Processes compare results from check_node/3 on the initiator.
-spec process_tree_cmp_result([merkle_cmp_result()],
                              merkle_tree:merkle_tree() | NodeList,
                              BranchSize::pos_integer(), SigSize::signature_size(),
                              Stats)
        -> {Requests::NodeList, Resolve::NodeList, New::Stats, New::NodeList,
            MaxLeafCount::non_neg_integer()}
    when
      is_subtype(NodeList,     [merkle_tree:mt_node()]),
      is_subtype(Stats,        rr_recon_stats:stats()).
process_tree_cmp_result(CmpResult, Tree, BranchSize, SigSize, Stats) ->
    Compared = length(CmpResult),
    NStats = rr_recon_stats:inc([{tree_compareLeft, -Compared},
                                 {tree_nodesCompared, Compared}], Stats),
    TreeNodes = case merkle_tree:is_merkle_tree(Tree) of
                    false -> Tree;
                    true -> [merkle_tree:get_root(Tree)]
                end,
    p_process_tree_cmp_result(CmpResult, TreeNodes, BranchSize, SigSize, NStats,
                              [], [], [], 0).

%% @doc Helper for process_tree_cmp_result/4.
-spec p_process_tree_cmp_result([merkle_cmp_result()], RestTree::NodeList,
                                BranchSize, SigSize::signature_size(), Stats,
                                AccReq::NodeList, AccRes::NodeList,
                                AccRTree::NodeList, AccMLC::MaxLeafCount)
        -> {Req::NodeList, Res::NodeList, Stats, NodeList, MaxLeafCount}
    when
      is_subtype(BranchSize,   pos_integer()),
      is_subtype(NodeList,     [merkle_tree:mt_node()]),
      is_subtype(Stats,        rr_recon_stats:stats()),
      is_subtype(MaxLeafCount, non_neg_integer()).
p_process_tree_cmp_result([], [], _BS, _SS, Stats, Req, Res, RTree, AccMLC) ->
    {lists:reverse(Req), Res, Stats, lists:reverse(RTree), AccMLC};
p_process_tree_cmp_result([?recon_ok | TR], [Node | TN], BS, SS, Stats,
                          Req, Res, RTree, AccMLC) ->
    NStats = rr_recon_stats:inc([{tree_compareSkipped, merkle_tree:size(Node)}], Stats),
    p_process_tree_cmp_result(TR, TN, BS, SS, NStats, Req, Res, RTree, AccMLC);
p_process_tree_cmp_result([?recon_fail_stop | TR], [_Node | TN], BS, SS, Stats,
                          Req, Res, RTree, AccMLC) ->
    ?ASSERT(merkle_tree:is_leaf(_Node)),
    p_process_tree_cmp_result(TR, TN, BS, SS, Stats, Req, Res, RTree, AccMLC);
p_process_tree_cmp_result([{Hash} | TR], [Node | TN], BS, SS, Stats,
                          Req, Res, RTree, AccMLC) ->
    NewRes = merkle_get_sync_leaves([Node], Hash, SS, Res),
    p_process_tree_cmp_result(TR, TN, BS, SS, Stats, Req, NewRes, RTree, AccMLC);
p_process_tree_cmp_result([?recon_fail_cont | TR], [Node | TN], BS, SS, Stats,
                          Req, Res, RTree, AccMLC) ->
    ?ASSERT(not merkle_tree:is_leaf(Node)),
    NewAccMLC = erlang:max(AccMLC, merkle_tree:get_leaf_count(Node)),
    Childs = merkle_tree:get_childs(Node),
    NStats = rr_recon_stats:inc([{tree_compareLeft, length(Childs)}], Stats),
    p_process_tree_cmp_result(TR, TN, BS, SS, NStats, lists:reverse(Childs, Req),
                              Res, lists:reverse(Childs, RTree), NewAccMLC).

%% @doc Gets all leaves in the given merkle node list whose hash =/= skipHash.
-spec merkle_get_sync_leaves(Nodes::NodeL, Skip::Hash, SigSize::signature_size(),
                             LeafAcc::NodeL) -> ToSync::NodeL
    when
      is_subtype(Hash,  merkle_tree:mt_node_key()),
      is_subtype(NodeL, [merkle_tree:mt_node()]).
merkle_get_sync_leaves([], _Skip, _SigSize, ToSyncAcc) ->
    ToSyncAcc;
merkle_get_sync_leaves([Node | Rest], Skip, SigSize, ToSyncAcc) ->
    case merkle_tree:is_leaf(Node) of
        true  ->
            NodeHash0 = merkle_tree:get_hash(Node),
            <<NodeHash:SigSize/integer-unit:1>> = <<NodeHash0:SigSize>>,
            if NodeHash =:= Skip ->
                   merkle_get_sync_leaves(Rest, Skip, SigSize, ToSyncAcc);
               true ->
                   merkle_get_sync_leaves(Rest, Skip, SigSize, [Node | ToSyncAcc])
            end;
        false ->
            merkle_get_sync_leaves(
              lists:append(merkle_tree:get_childs(Node), Rest), Skip, SigSize, ToSyncAcc)
    end.

%% @doc Starts a single resolve request for all given leaf nodes by joining
%%      their intervals.
%%      Returns the number of resolve requests (requests with feedback count 2).
-spec resolve_leaves([merkle_tree:mt_node()], Dest::comm:mypid(),
                     rrepair:session_id(), OwnerLocal::comm:erl_local_pid())
        -> 0..1.
resolve_leaves([], _Dest, _SID, _OwnerL) -> 0;
resolve_leaves(Nodes, Dest, SID, OwnerL) ->
    resolve_leaves(Nodes, Dest, SID, OwnerL, intervals:empty(), 0).

%% @doc Helper for resolve_leaves/4.
-spec resolve_leaves([merkle_tree:mt_node()], Dest::comm:mypid(),
                     rrepair:session_id(), OwnerLocal::comm:erl_local_pid(),
                     Interval::intervals:interval(), Items::non_neg_integer())
        -> 0..1.
resolve_leaves([Node | Rest], Dest, SID, OwnerL, Interval, Items) ->
    ?ASSERT(merkle_tree:is_leaf(Node)),
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
                   1;
               true ->
                   % we know that we don't have data in this range, so we must
                   % regenerate it from the other node
                   % -> send him this request directly!
                   send(Dest, {request_resolve, SID, {?interval_upd, Interval, []},
                               [{from_my_node, 0} | Options]}),
                   1
            end
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
    NStats =
        case merkle_tree:get_interval(Tree) =:= art:get_interval(Art) of
            true ->
                {ASyncLeafs, NComp, NSkip, NLSync} =
                    art_get_sync_leaves([merkle_tree:get_root(Tree)], Art,
                                        [], 0, 0, 0),
                ResolveCalled = resolve_leaves(ASyncLeafs, DestPid, SID, OwnerL),
                rr_recon_stats:inc([{tree_nodesCompared, NComp},
                                    {tree_compareSkipped, NSkip},
                                    {tree_leavesSynced, NLSync},
                                    {resolve_started, ResolveCalled}], Stats);
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
            NSkip = NSkipAcc + ?IIF(IsLeaf, 0, merkle_tree:size(Node)),
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

-spec build_recon_struct(method(), OldSyncStruct::sync_struct() | {},
                         DestI::intervals:non_empty_interval(), db_chunk_kv(),
                         Params::parameters() | {}, BeginSync::boolean())
        -> sync_struct().
build_recon_struct(trivial, _OldSyncStruct = {}, I, DBItems, _Params, true) ->
    ?ASSERT(not intervals:is_empty(I)),
    P1E = get_p1e(),
    ElementNum = length(DBItems),
    % cut off at 128 bit (rt_chord uses md5 - must be enough for all other RT implementations, too)
    SigSize0 = calc_signature_size_n_pair(ElementNum, P1E, 128),
    % note: we have n one-to-one comparisons, assuming the probability of a
    %       failure in a single one-to-one comparison is p, the overall
    %       p1e = 1 - (1-p)^n  <=>  p = 1 - (1 - p1e)^(1/n)
    VP = 1 - math:pow(1 - P1E, 1 / erlang:max(1, ElementNum)),
    VSize0 = calc_signature_size_1_to_n(1, VP, 128),
    % note: we can reach the best compression if values and versions align to
    %       byte-boundaries
    FullKVSize0 = SigSize0 + VSize0,
    FullKVSize = bloom:resize(FullKVSize0, 8),
    VSize = VSize0 + ((FullKVSize - FullKVSize0) div 2),
    SigSize = FullKVSize - VSize, 
    DBChunkBin = compress_kv_list(DBItems, <<>>, SigSize, VSize),
    #trivial_recon_struct{interval = I, reconPid = comm:this(),
                          db_chunk = DBChunkBin,
                          sig_size = SigSize, ver_size = VSize};
build_recon_struct(bloom, _OldSyncStruct = {}, I, DBItems, _Params, true) ->
    % note: for bloom, parameters don't need to match (only one bloom filter at
    %       the non-initiator is created!) - use our own parameters
    ?ASSERT(not intervals:is_empty(I)),
    P1E = get_p1e(),
    ElementNum = length(DBItems),
    BF0 = bloom:new_p1e(ElementNum, P1E),
    BF = bloom:add_list(BF0, DBItems),
    #bloom_recon_struct{ interval = I, bloom = BF };
build_recon_struct(merkle_tree, _OldSyncStruct = {}, I, DBItems, Params, BeginSync) ->
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
                                 {keep_bucket, not BeginSync}]);
build_recon_struct(merkle_tree, OldSyncStruct, _I, DBItems, _Params, BeginSync) ->
    ?ASSERT(not intervals:is_empty(_I)),
    ?ASSERT(merkle_tree:is_merkle_tree(OldSyncStruct)),
    NTree = merkle_tree:insert_list(DBItems, OldSyncStruct),
    if BeginSync ->
           % no more DB items -> finish tree, remove buckets
           merkle_tree:gen_hash(NTree, true);
       true ->
           % don't hash now - there will be new items
           NTree
    end;
build_recon_struct(art, _OldSyncStruct = {}, I, DBItems, _Params = {}, BeginSync) ->
    ?ASSERT(not intervals:is_empty(I)),
    BranchFactor = get_merkle_branch_factor(),
    BucketSize = merkle_tree:get_opt_bucket_size(length(DBItems), BranchFactor, 1),
    Tree = merkle_tree:new(I, DBItems, [{branch_factor, BranchFactor},
                                        {bucket_size, BucketSize},
                                        {keep_bucket, not BeginSync}]),
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
    ?ASSERT(not intervals:is_empty(_I)),
    ?ASSERT(merkle_tree:is_merkle_tree(OldSyncStruct)),
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
    ?ASSERT(I =:= _DestI),
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
    ?ASSERT(I =:= _DestI),
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
    config:cfg_is_in(rr_recon_method, [bloom, merkle_tree, art]) andalso
        config:cfg_is_float(rr_recon_p1e) andalso
        config:cfg_is_greater_than(rr_recon_p1e, 0) andalso
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

%% @doc Sepcifies how many items to retrieve from the DB at once. 
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
