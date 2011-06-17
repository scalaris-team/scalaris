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
%% @doc    bloom filter synchronization protocol
%% @end
%% @version $Id$

-module(bloom_sync).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start_bloom_sync/1]).
-export([concatKeyVer/1, concatKeyVer/2, minKey/1]).

-ifdef(with_export_type_support).
-export_type([bloom_sync_struct/0]).
-endif.

-define(TRACE(X,Y), io:format("[~p] " ++ X ++ "~n", [self()] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type keyValVers() :: {?RT:key(), ?DB:value(), ?DB:version()}.
-type exit_reason() :: empty_interval | {ok, ItemsUpdated::non_neg_integer()}.
-type step() :: bloom_sync | diff_sync.

-record(bloom_sync_struct, 
        {
         interval = intervals:empty()                       :: intervals:interval(), 
         srcNode  = ?required(bloom_sync_struct, srcNode)   :: comm:mypid(),
         keyBF    = ?required(bloom_sync_struct, keyBF)     :: ?REP_BLOOM:bloomFilter(),
         versBF   = ?required(bloom_sync_struct, versBF)    :: ?REP_BLOOM:bloomFilter(),
         round    = 0                                       :: non_neg_integer()
        }).
-record(bloom_sync_state,
        {
         ownerPid       = ?required(bloom_sync_state, ownerPid)   :: comm:erl_local_pid(),
         dhtNodePid     = ?required(bloom_sync_state, ownerDhtPid):: comm:erl_local_pid(),
         syncStruct     = {}                                      :: bloom_sync_struct(),
         diffList       = []                                      :: [keyValVers()],
         diffCount      = 0                                       :: non_neg_integer(),
         diffSenderPid                                            :: comm:mypid(),
         updatedCount   = 0                                       :: non_neg_integer(),
         notUpdatedCount= 0                                       :: non_neg_integer(),
         feedback       = []                                      :: [keyValVers()],
         sendFeedback   = true                                    :: boolean(),
         step           = bloom_sync                              :: step()
         }).

-type bloom_sync_struct() :: #bloom_sync_struct{}.
-type state() :: #bloom_sync_state{}.

-type message() ::
    {get_state_response, intervals:interval()} |
    {get_chunk_response, rep_upd:db_chunk()} |
    {diff_list, comm:erl_pid(), [{?RT:key(), ?DB:version()}]} |
    {build_sync_struct, comm:erl_local_pid(), rep_upd:db_chunk(), non_neg_integer()} |
    {shutdown, exit_reason()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, NodeDBInterval}, State) ->
    #bloom_sync_state{ 
                      diffList = DiffList,
                      dhtNodePid = DhtNodePid, 
                      syncStruct = SyncStruct, 
                      step = Step } = State,
    _ = case Step of
            bloom_sync ->
                BloomInterval = SyncStruct#bloom_sync_struct.interval,
                SyncInterval = intervals:intersection(NodeDBInterval, BloomInterval),
                case intervals:is_empty(SyncInterval) of
                    true ->
                        comm:send_local(self(), {shutdown, empty_interval});
                    false ->
                        comm:send_local(DhtNodePid, {get_chunk, self(), SyncInterval, ?BLOOM_MAX_SIZE})
                end;
            diff_sync -> 
                MyPid = comm:this(),
                erlang:spawn(lists, 
                             foreach, 
                             [fun({MinKey, Val, Vers}) ->
                                      PosKeys = ?RT:get_replica_keys(MinKey),
                                      UpdKeys = lists:filter(fun(X) -> 
                                                                     intervals:in(X, NodeDBInterval) 
                                                             end, 
                                                             PosKeys),
                                      lists:foreach(fun(Key) ->
                                                            comm:send_local(DhtNodePid, 
                                                                            {update_key_entry, MyPid, Key, Val, Vers})
                                                    end, 
                                                    UpdKeys)
                              end, DiffList])
        end,
    State;
on({get_chunk_response, {_, DBList}}, State) ->
    SyncStruct = State#bloom_sync_state.syncStruct,
    #bloom_sync_struct{
                       srcNode = SrcNode,
                       keyBF = KeyBF,
                       versBF = VersBF
                       } = SyncStruct,
    {Obsolete, Missing} = 
        filterPartitionMap(fun(A) -> 
                                   db_entry:get_version(A) > -1 andalso
                                       not ?REP_BLOOM:is_element(VersBF, concatKeyVer(A)) 
                           end,
                           fun(B) -> 
                                   ?REP_BLOOM:is_element(KeyBF, minKey(db_entry:get_key(B)))
                           end,
                           fun(C) ->
                                   { minKey(db_entry:get_key(C)), 
                                     db_entry:get_value(C), 
                                     db_entry:get_version(C) }
                           end,
                           DBList),
	?TRACE("SYNC WITH [~p] RESULT: DBListLength=[~p] -> Missing=[~p] Obsolete=[~p]", 
           [SrcNode, length(DBList), length(Missing), length(Obsolete)]),
    %TODO possibility of DETAIL SYNC IMPL - NOW SEND COMPLETE obsolete Entries
    comm:send(SrcNode, {diff_list, comm:this(), Obsolete}),
    State#bloom_sync_state{ sendFeedback = false };
on({diff_list, Sender, DiffList}, State) ->
    ?TRACE("RECV DiffList [~p] FROM [~p]", [length(DiffList), Sender]),
    DhtNodePid = State#bloom_sync_state.dhtNodePid,
    comm:send_local(DhtNodePid, {get_state, comm:this(), my_range}),
    State#bloom_sync_state{
                           diffList = DiffList, 
                           diffCount = length(DiffList),
                           diffSenderPid = Sender,
                           step = diff_sync };
on({update_key_entry_ack, Entry, Exists, Done}, State) ->
    #bloom_sync_state{
                      diffCount = DiffCount,
                      updatedCount = OkCount, 
                      notUpdatedCount = FailedCount, 
                      feedback = Feedback,
                      diffSenderPid = Sender } = State,
    NewState = case Done of
                   true ->
                       State#bloom_sync_state{ updatedCount = OkCount + 1 };
                   false when Exists ->
                       State#bloom_sync_state{ notUpdatedCount = FailedCount + 1,
                                               feedback = [{db_entry:get_key(Entry),
                                                            db_entry:get_value(Entry),
                                                            db_entry:get_version(Entry)} | Feedback]};
                   _ ->
                       State#bloom_sync_state{ notUpdatedCount = FailedCount + 1 }
               end,
    _ = case DiffCount - 1 =:= OkCount + FailedCount of
            true when NewState#bloom_sync_state.sendFeedback ->                
                ?TRACE("Send Feedback and Shutdown", []),
                comm:send(Sender, {diff_list, comm:this(), NewState#bloom_sync_state.feedback}),
                comm:send_local(self(), {shutdown, {ok, NewState#bloom_sync_state.updatedCount}});
            true ->
                ?TRACE("Send NO Feedback and Shutdown", []),
                comm:send_local(self(), {shutdown, {ok, NewState#bloom_sync_state.updatedCount}});
            _ ->
                %?TRACE("UPDATE OK OK=~p  Fail=~p  Diff=~p", [OkCount, FailedCount, DiffCount]),
                ok
        end,
    NewState;
on({shutdown, Reason}, State) ->
    Owner = State#bloom_sync_state.ownerPid,
    comm:send_local(Owner, {sync_progress_report, self(), io_lib:format("SHUTDOWN Reason=~p", [Reason])}),
    kill;
on({start_sync, SyncStruct}, State) ->
    DhtNodePid = State#bloom_sync_state.dhtNodePid,
    comm:send_local(DhtNodePid, {get_state, comm:this(), my_range}),
    State#bloom_sync_state{ syncStruct = SyncStruct };
on({build_sync_struct, Sender, {ChunkInterval, DBItems}, Round}, State) ->
    ?TRACE("BUILD REQ REV", []),
    Fpr = 0.001,           %TODO move to config?
    ElementNum = ?BLOOM_MAX_SIZE * 2,    %TODO set to node db item cout + 5%?
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?REP_HFS:new(HFCount),
    BF1 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),
    BF2 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),    
    {KeyBF, VerBF} = fill_bloom(DBItems, BF1, BF2),
    SyncStruct = #bloom_sync_struct{ interval = ChunkInterval,
                                     srcNode = comm:this(),
                                     keyBF = KeyBF,
                                     versBF = VerBF,
                                     round = Round
                                   },
    comm:send_local(Sender, {build_sync_struct_response, ChunkInterval, SyncStruct}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% BloomFilter building
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Create two bloom filter of a given database chunk.
%%      One over all keys and one over all keys concatenated with their version.
-spec fill_bloom(?DB:db_as_list(), 
                 KeyBF::?REP_BLOOM:bloomFilter(), 
                 VerBF::?REP_BLOOM:bloomFilter()) -> 
          {?REP_BLOOM:bloomFilter(), ?REP_BLOOM:bloomFilter()}.
fill_bloom([], KeyBF, VerBF) ->
    {KeyBF, VerBF};
fill_bloom([H | T], KeyBF, VerBF) ->
    {Key, _, _, _, Ver} = H,
    case Ver of
        -1 ->
            fill_bloom(T, KeyBF, VerBF);
        _ ->
            AddKey = bloom_sync:minKey(Key),
            NewKeyBF = ?REP_BLOOM:add(KeyBF, AddKey),
            NewVerBF = ?REP_BLOOM:add(VerBF, bloom_sync:concatKeyVer(AddKey, Ver)),
            fill_bloom(T, NewKeyBF, NewVerBF)            
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% PUBLIC HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc transforms a key to its smallest associated key
-spec minKey(?RT:key()) -> ?RT:key().
minKey(Key) ->
    lists:min(?RT:get_replica_keys(Key)).
-spec concatKeyVer(db_entry:entry()) -> binary().
concatKeyVer(DBEntry) ->
    concatKeyVer(minKey(db_entry:get_key(DBEntry)), db_entry:get_version(DBEntry)).
-spec concatKeyVer(?RT:key(), ?DB:version()) -> binary().
concatKeyVer(Key, Version) ->
    erlang:list_to_binary([term_to_binary(Key), "#", Version]).


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
% @ doc filter, partition and map items of a list in one run
-spec filterPartitionMap(fun((A) -> boolean()), fun((A) -> boolean()), fun((A) -> any()), [A]) -> {Satisfying::[any()], NonSatisfying::[any()]}.
filterPartitionMap(Filter, Pred, Map, List) ->
    filterPartitionMap(Filter, Pred, Map, List, [], []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc INITIALISES THE MODULE
-spec init({comm:erl_local_pid(), bloom_sync_struct()}) -> state().
init(State) ->    
    State.

-spec start_bloom_sync(comm:erl_local_pid()) -> {ok, pid()}.
start_bloom_sync(DhtNodePid) ->
    State = #bloom_sync_state{ ownerPid = self(), dhtNodePid = DhtNodePid },
    gen_component:start(?MODULE, State, []).

    
