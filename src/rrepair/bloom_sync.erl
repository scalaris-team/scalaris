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

-export([init/1, on/2, start_bloom_sync/2]).
-export([concatKeyVer/1, concatKeyVer/2, minKey/1]).

-ifdef(with_export_type_support).
-export_type([bloom_sync_struct/0]).
-endif.

-define(TRACE(X,Y), io:format("[~p] " ++ X ++ "~n", [self()] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(bloom_sync_struct, 
        {
         interval = intervals:empty()                       :: intervals:interval(), 
         srcNode  = ?required(bloom_sync_struct, srcNode)   :: comm:mypid(),
         keyBF    = ?required(bloom_sync_struct, keyBF)     :: ?REP_BLOOM:bloomFilter(),
         versBF   = ?required(bloom_sync_struct, versBF)    :: ?REP_BLOOM:bloomFilter(),
         round    = 0                                       :: non_neg_integer()
        }).

-type bloom_sync_struct() :: #bloom_sync_struct{}.

-type state() ::
    {
        Owner        :: comm:erl_local_pid(),
        OwnerDhtNod  :: comm:erl_local_pid(),
        SyncStruct   :: bloom_sync_struct()
    }.

-type exit_reason() :: empty_interval.
-type message() ::
    {get_state_response, intervals:interval()} |
    {get_chunk_response, rep_upd:db_chunk()} |
    {shutdown, exit_reason()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, NodeDBInterval}, {_, DhtNodePid, SyncStruct} = State) ->
    BloomInterval = SyncStruct#bloom_sync_struct.interval,    
    SyncInterval = intervals:intersection(NodeDBInterval, BloomInterval),
    case intervals:is_empty(SyncInterval) of
         true ->
            comm:send_local(self(), {shutdown, empty_interval});
         false ->
            comm:send_local(DhtNodePid, {get_chunk, self(), SyncInterval, 10000}) %TODO remove 10k constant
    end,
    State;
on({get_chunk_response, {_, DBList}}, {Owner, _, SyncStruct} = State) ->	
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
                                   minKey(db_entry:get_key(C))
                           end,
                           DBList),
    
    %Diff = [ minKey(db_entry:get_key(DBEntry)) || DBEntry <- DBList, 
    %                                              db_entry:get_version(DBEntry) > -1, 
    %                                              not ?REP_BLOOM:is_element(VersBF, concatKeyVer(DBEntry))],
    %Missing = [ Key || Key <- Diff, not ?REP_BLOOM:is_element(KeyBF, Key) ],
    %Obsolete = lists:subtract(Diff, Missing),
	?TRACE("SYNC WITH [~p] RESULT: DBListLength=[~p] -> Missing=[~p] Obsolete=[~p]", 
           [SrcNode, length(DBList), length(Missing), length(Obsolete)]),
    comm:send_local(Owner, {sync_progress_report, self(), "ok"}),
    %TODO inform SrcNode about Diff Entries - IMPL DETAIL SYNC
    State;
on({shutdown, Reason}, {Owner, SyncStruct}) ->
    RoundId = SyncStruct#bloom_sync_struct.round,
    comm:send_local(Owner, {sync_progress_report, self(), io_lib:format("Round=~p - SHUTDOWN Reason=~p", [RoundId, Reason])}),
    kill;
on({start_sync}, {_Owner, DhtNodePid, _SyncStruct} = State) ->
    comm:send_local(DhtNodePid, {get_state, comm:this(), my_range}),
    State.

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
    comm:send_local(self(), {start_sync}),
    State.

-spec start_bloom_sync(bloom_sync_struct(), comm:erl_local_pid()) -> {ok, pid()}.
start_bloom_sync(SyncStruct, DhtNodePid) ->
    gen_component:start(?MODULE, {self(), DhtNodePid, SyncStruct}, []).
