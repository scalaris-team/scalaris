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

-export([init/1, on/2, start_sync/3]).
-export([get_sync_method/1]).

-ifdef(with_export_type_support).
-export_type([sync_struct/0, sync_stage/0]).
-endif.

-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
%-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type keyValVers() :: {?RT:key(), ?DB:value(), ?DB:version()}.

-type sync_stage()  :: negotiate_interval | reconciliation | resolution.

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
         interval = intervals:empty()                       :: intervals:interval(), 
         srcNode  = ?required(merkle_sync_struct, srcNode)  :: comm:mypid(),
         tree     = ?required(merkle_sync_struct, tree)     :: merkle_tree:merkle_tree()
        }).
-type merkle_sync_struct() :: #merkle_sync_struct{}.

-type simple_detail_sync() :: {SrcNode::comm:mypid(), [keyValVers()]}.

-type sync_struct() :: bloom_sync_struct() |
                       merkle_sync_struct() |
                       simple_detail_sync() |
                       {SrcNode::comm:mypid(), intervals:interval()}. %for sync interval negotiation 

-record(rep_upd_sync_stats,
        {
         diffCount          = 0 :: non_neg_integer(),
         updatedCount       = 0 :: non_neg_integer(),
         notUpdatedCount    = 0 :: non_neg_integer()
         }).
-type rep_upd_sync_stats() :: #rep_upd_sync_stats{}.

-record(rep_upd_sync_state,
        {
         ownerLocalPid  = ?required(rep_upd_sync_state, ownerLocalPid)  :: comm:erl_local_pid(),
         ownerRemotePid = ?required(rep_upd_sync_state, ownerRemotePid) :: comm:mypid(),
         dhtNodePid     = ?required(rep_upd_sync_state, ownerDhtPid)    :: comm:erl_local_pid(),
         sync_struct    = {}                                            :: sync_struct() | {},
         sync_stage     = reconciliation                                :: sync_stage(),
         sync_master    = ?required(rep_upd_sync_state, sync_master)    :: boolean(),           %true if process is sync leader
         sync_round     = 0                                             :: float(),
         sync_stats     = #rep_upd_sync_stats{}                         :: rep_upd_sync_stats(),    
         maxItems       = ?required(rep_upd_sync_state, maxItems)       :: pos_integer(),       %max items in a sync structure 
         feedback       = []                                            :: [keyValVers()],
         sendFeedback   = true                                          :: boolean()
         }).
-type state() :: #rep_upd_sync_state{}.

-type build_args() :: [] | 
                      [Fpr::float()]. %bloom build args

-type message() ::
    {get_state_response, intervals:interval()} |
    {get_chunk_response, rep_upd:db_chunk()} |
    {update_key_entry_ack, db_entry:entry(), Exists::boolean(), Done::boolean()} |
    {start_sync_client, sync_stage(), Feedback::boolean(), sync_struct()} |
    {build_sync_struct, rep_upd:sync_method(), rep_upd:db_chunk(), Args::build_args()} |
    {shutdown, exit_reason()}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_sync_method(sync_struct()) -> rep_upd:sync_method().
get_sync_method(#bloom_sync_struct{}) -> bloom;
get_sync_method(#merkle_sync_struct{}) -> merkleTree;
get_sync_method(_) -> art.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, NodeDBInterval}, 
   State = #rep_upd_sync_state{sync_stage = negotiate_interval,
                               sync_struct = {SrcPid, SrcInterval},
                               dhtNodePid = DhtNodePid,
                               maxItems = MaxItems}) ->
    Intersection = intervals:intersection(NodeDBInterval, SrcInterval),
    comm:send_local(DhtNodePid, {get_chunk, self(), Intersection, MaxItems}),
    comm:send(SrcPid, {get_sync_interval_response, comm:this(), Intersection}),
    State;
on({get_state_response, NodeDBInterval}, State = #rep_upd_sync_state{sync_stage = reconciliation}) ->
    #rep_upd_sync_state{dhtNodePid = DhtNodePid, 
                        sync_struct = SyncStruct, 
                        maxItems = MaxItems} = State,
    BloomInterval = SyncStruct#bloom_sync_struct.interval,
    SyncInterval = intervals:intersection(NodeDBInterval, BloomInterval),
    case intervals:is_empty(SyncInterval) of
        true ->
            comm:send_local(self(), {shutdown, empty_interval});
        false ->
            comm:send_local(DhtNodePid, {get_chunk, self(), SyncInterval, MaxItems})
    end,
    State;
on({get_state_response, NodeDBInterval}, State = #rep_upd_sync_state{sync_stage = resolution}) 
  when size(State#rep_upd_sync_state.sync_struct) =:= 2 ->
    %simple detail sync case
    #rep_upd_sync_state{dhtNodePid = DhtNodePid, sync_struct = SyncStruct} = State,
    MyPid = comm:this(),
    {_, DiffList} = SyncStruct,
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
    State;

%% @doc receive intersection sync interval
on({get_sync_interval_response, SrcPid, Interval}, 
   State = #rep_upd_sync_state{ dhtNodePid = DhtNodePid, maxItems = MaxItems }) ->
    comm:send_local(DhtNodePid, {get_chunk, self(), Interval, MaxItems}),
    State#rep_upd_sync_state{ sync_struct = {SrcPid, Interval}, sync_stage = negotiate_interval };

on({get_chunk_response, {RestI, DBList}}, State = #rep_upd_sync_state{sync_stage = negotiate_interval, 
                                                                      sync_struct = {SrcPid, Interval},
                                                                      sync_master = IsMaster}) ->  
    Tree = build_merkle_sync_struct({Interval, DBList}, SrcPid),
    %TODO what to do with RestI
    %IsMaster andalso
    %    send_local(), %TODO Start Sync
    State#rep_upd_sync_state{ sync_stage = reconciliation, sync_struct = Tree };
on({get_chunk_response, {RestI, DBList}}, State = #rep_upd_sync_state{sync_struct = #bloom_sync_struct{}}) ->
    %unpack parameters
    #rep_upd_sync_state{ ownerRemotePid = OwnerPid,
                         sync_struct = #bloom_sync_struct{ srcNode = SrcNode,
                                                           keyBF = KeyBF,
                                                           versBF = VersBF},
                         dhtNodePid = DhtNodePid,
                         sync_round = Round,
                         maxItems = MaxItems } = State,
    %if rest interval is non empty start another sync
    SyncFinished = intervals:is_empty(RestI),
    not SyncFinished andalso
        comm:send_local(DhtNodePid, {get_chunk, self(), RestI, MaxItems}),
    %set reconciliation
    {Obsolete, _Missing} = 
        filterPartitionMap(fun(A) -> 
                                   db_entry:get_version(A) > -1 andalso
                                       not ?REP_BLOOM:is_element(VersBF, rep_upd:concatKeyVer(A)) 
                           end,
                           fun(B) -> 
                                   ?REP_BLOOM:is_element(KeyBF, rep_upd:minKey(db_entry:get_key(B)))
                           end,
                           fun(C) ->
                                   { rep_upd:minKey(db_entry:get_key(C)), 
                                     db_entry:get_value(C), 
                                     db_entry:get_version(C) }
                           end,
                           DBList),
    %TODO possibility of DETAIL SYNC IMPL - NOW SEND COMPLETE obsolete Entries (key-val-vers)
    length(Obsolete) > 0 andalso
        comm:send(SrcNode, {request_sync, resolution, true, Round, {OwnerPid, Obsolete}}),
    SyncFinished andalso
        comm:send_local(self(), {shutdown, {ok, reconciliation}}),
    State;

on({update_key_entry_ack, Entry, Exists, Done}, State) ->
    #rep_upd_sync_state{
                        ownerRemotePid = Owner,
                        sync_stats = 
                            #rep_upd_sync_stats{
                                                diffCount = DiffCount,
                                                updatedCount = OkCount, 
                                                notUpdatedCount = FailedCount                                                         
                                               } = Stats,
                        feedback = Feedback,
                        sync_struct = {Sender, _},
                        sync_round = Round,
                        sendFeedback = SendFeedback
                       } = State,
    NewStats = case Done of 
                   true  -> Stats#rep_upd_sync_stats{ updatedCount = OkCount +1 };
                   false -> Stats#rep_upd_sync_stats{ notUpdatedCount = FailedCount + 1 } 
               end,    
    NewState = case not Done andalso Exists andalso SendFeedback of
                   true -> State#rep_upd_sync_state{ sync_stats = NewStats,
                                                     feedback = [{db_entry:get_key(Entry),
                                                                  db_entry:get_value(Entry),
                                                                  db_entry:get_version(Entry)} | Feedback]};
                   false -> State#rep_upd_sync_state{ sync_stats = NewStats }
               end,
    _ = case DiffCount - 1 =:= OkCount + FailedCount of
            true ->
                SendFeedback andalso
                    comm:send(Sender, {request_sync, bloom, resolution, false, Round, {Owner, NewState#rep_upd_sync_state.feedback}}),
                comm:send_local(self(), {shutdown, {ok, NewState#rep_upd_sync_state.sync_stats}});
            _ ->
                ok
        end,
    NewState;

on({recv_hash_check, Interval, Hash}, State) ->
    %TODO check if hashes match
    State;

on({start_sync_client, SyncStage, Feedback, SyncStruct}, State) ->
    comm:send_local(State#rep_upd_sync_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#rep_upd_sync_state{ sync_stage = SyncStage, 
                              sync_struct = SyncStruct, 
                              sendFeedback = Feedback };

on({shutdown, Reason}, State = #rep_upd_sync_state{ sync_round = Round }) ->
    comm:send_local(State#rep_upd_sync_state.ownerLocalPid, 
                    {sync_progress_report, self(), shutdown, io_lib:format("[R~p] ~p", [Round, Reason])}),
    kill;

on({build_sync_struct, merkleTree, {ChunkInterval, _} = Chunk, _}, State) ->
    #rep_upd_sync_state{ ownerLocalPid = OwnerLocalPid,
                         sync_round = Round } = State,      
    SyncStruct = build_merkle_sync_struct(Chunk, State#rep_upd_sync_state.ownerRemotePid),
    comm:send_local(OwnerLocalPid, {build_sync_struct_response, ChunkInterval, Round, SyncStruct}),
    comm:send_local(self(), {shutdown, {ok, build_merkleTree_sync_struct}}),    
    State;

on({build_sync_struct, bloom, {ChunkInterval, DBItems}, [Fpr]}, State) ->
    #rep_upd_sync_state{ ownerLocalPid = OwnerLocalPid,
                         sync_round = Round } = State,    
    ElementNum = length(DBItems),
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?REP_HFS:new(HFCount),
    BF1 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),
    BF2 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),    
    {KeyBF, VerBF} = fill_bloom(DBItems, BF1, BF2),
    SyncStruct = #bloom_sync_struct{ interval = ChunkInterval,
                                     srcNode = State#rep_upd_sync_state.ownerRemotePid,
                                     keyBF = KeyBF,
                                     versBF = VerBF
                                   },
    comm:send_local(OwnerLocalPid, {build_sync_struct_response, ChunkInterval, Round, SyncStruct}),
    comm:send_local(self(), {shutdown, {ok, build_bloom_sync_struct}}),
    State;

on({build_sync_struct, _, _, _, _}, State) ->
    comm:send_local(self(), {shutdown, {fail, build_bloom_sync_struct_parameter_missmatch}}),
    State.

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
    AddKey = rep_upd:minKey(Key),
    NewKeyBF = ?REP_BLOOM:add(KeyBF, AddKey),
    NewVerBF = ?REP_BLOOM:add(VerBF, rep_upd:concatKeyVer(AddKey, Ver)),
    fill_bloom(T, NewKeyBF, NewVerBF).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Merkle Tree specific
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_merkle_sync_struct(rep_upd:db_chunk(), comm:mypid()) -> merkle_sync_struct().
build_merkle_sync_struct({ChunkInterval, DBItems}, RemotePid) ->
    MTree = merkle_tree:gen_hashes(
              lists:foldl(
                fun({Key, _, _, _, Ver}, Tree) -> 
                        merkle_tree:insert(rep_upd:concatKeyVer(rep_upd:minKey(Key), Ver), "", Tree)
                end, 
                merkle_tree:new(ChunkInterval), 
                DBItems)),
    #merkle_sync_struct{ interval = ChunkInterval,
                         tree = MTree,
                         srcNode = RemotePid }.

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc init module
-spec init({comm:erl_local_pid(), sync_struct()}) -> state().
init(State) ->
    State.

-spec start_sync(pos_integer(), boolean(), float()) -> {ok, pid()}.
start_sync(MaxItems, SyncMaster, Round) ->
    State = #rep_upd_sync_state{ ownerLocalPid = self(), 
                                 ownerRemotePid = comm:this(), 
                                 dhtNodePid = pid_groups:get_my(dht_node), 
                                 maxItems = MaxItems,
                                 sync_master = SyncMaster,
                                 sync_round = Round },
    gen_component:start(?MODULE, State, []).