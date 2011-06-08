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
%% @doc    replica update protocol 
%% @end
%% @version $Id$

-module(rep_upd).

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1, init/1, on/2, check_config/0]).

-ifdef(with_export_type_support).
-export_type([db_chunk/0]).
-endif.

-define(TRACE(X,Y), io:format("[~p] " ++ X ++ "~n", [self()] ++ Y)).
%-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% constants
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(PROCESS_NAME, ?MODULE).
-define(TRIGGER_NAME, rep_update_trigger).
-define(BLOOM_MAX_SIZE, 10000). %TODO remove 10k constant

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type db_chunk() :: {intervals:interval(), ?DB:db_as_list()}.
-type sync_method() :: bloom | merkleTree | art.

-type sync_struct() :: %TODO add merkleTree + art
    bloom_sync:bloom_sync_struct(). 

-type state() :: 
    {
        Sync_method     :: sync_method(),  
        TriggerState    :: trigger:state(),
        SyncRound       :: non_neg_integer(),
        MonitorTable    :: pdb:tableid()
    }.

-type message() ::
    {?TRIGGER_NAME} |
    {get_state_response, any()} |
    {get_chunk_response, db_chunk()} |
    {get_sync_struct_response, intervals:interval(), sync_struct()} |
    {request_sync, sync_method(), sync_struct()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {sync_progress_report, Sender::comm:erl_local_pid(), Text::string()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when trigger triggers (INITIATE SYNC BY TRIGGER)
-spec on(message(), state()) -> state().
on({?TRIGGER_NAME}, {SyncMethod, TriggerState, Round, MonitorTable}) ->
    DhtNodePid = pid_groups:get_my(dht_node),
    comm:send_local(DhtNodePid, {get_state, comm:this(), my_range}),
    
    NewTriggerState = trigger:next(TriggerState),
    ?TRACE("Trigger NEXT", []),
    {SyncMethod, NewTriggerState, Round, MonitorTable};

%% @doc retrieve node responsibility interval
on({get_state_response, NodeDBInterval}, State) ->
    DhtNodePid = pid_groups:get_my(dht_node),
    comm:send_local(DhtNodePid, {get_chunk, self(), NodeDBInterval, ?BLOOM_MAX_SIZE}),
    State;

%% @doc retrieve local node db
on({get_chunk_response, {RestI, [First | T] = DBList}}, {SyncMethod, TriggerState, Round, MonitorTable}) ->
    MyPid = self(),    
    _ = case intervals:is_empty(RestI) of
            true -> ok;
            _ -> 
                ?TRACE("SPAWNING ADDITIONAL SYNC FOR RestI ~p", [RestI]),
                DhtNodePid = pid_groups:get_my(dht_node),
                comm:send_local(DhtNodePid, {get_chunk, self(), RestI, ?BLOOM_MAX_SIZE})
        end,
    %Get Interval of DBList
    %TODO: IMPROVEMENT getChunk should return ChunkInterval 
    %       (db is traved twice! - 1st getChunk, 2nd here)
    ChunkI = intervals:new('[', db_entry:get_key(First), db_entry:get_key(lists:last(T)), ']'),
    %?TRACE("RECV CHUNK interval= ~p  - RestInterval= ~p - DBLength=~p", [ChunkI, RestI, length(DBList)]),
    _Pid = spawn(fun() -> build_SyncStruct(MyPid, SyncMethod, {ChunkI, DBList}, Round) end),    
    %?TRACE("RECV Chunk let [~p] build SyncStruct", [_Pid]),
    {SyncMethod, TriggerState, Round + 1, MonitorTable};

%% @doc SyncStruct is build and can be send to a node for synchronization
on({get_sync_struct_response, Interval, SyncStruct}, {SyncMethod, _TriggerState, Round, MonitorTable} = State) ->
    ?TRACE("~p", [SyncStruct]),
    _ = case intervals:is_empty(Interval) of	
            false ->
                {_, _, RKey, RBr} = intervals:get_bounds(Interval),
                Key = case RBr of
                          ')' -> RKey - 1;
                          ']' -> RKey
                      end,
                Keys = lists:delete(Key, ?RT:get_replica_keys(Key)),
                DestKey = lists:nth(random:uniform(erlang:length(Keys)), Keys),
                DhtNodePid = pid_groups:get_my(dht_node),
                ?TRACE("SEND SYNC REQ TO [~p]", [DestKey]),
                comm:send_local(DhtNodePid, 
                                {lookup_aux, DestKey, 0, 
                                 {send_to_group_member, ?PROCESS_NAME, 
                                  {request_sync, SyncMethod, SyncStruct}}}),
                monitor:proc_set_value(MonitorTable, 
                                       io_lib:format("~p", [erlang:localtime()]), 
                                       io_lib:format("SEND SyncReq Round=[~B] to Key [~p]", [Round, DestKey]));	    
            _ ->
                ok
		end,
    State;
%% @doc receive sync request and spawn a new process which executes a sync protocol
on({request_sync, Sync_method, SyncStruct}, {_SM, _TriggerState, _Round, MonitorTable} = State) ->	
    _ = case Sync_method of
            bloom ->
                {_, _, SrcNode, _, _, _} = SyncStruct,
                ?TRACE("RECV SYNC REQUEST FROM ~p", [SrcNode]),
                monitor:proc_inc_value(MonitorTable, "Recv-Sync-Req-Count"),
                DhtNodePid = pid_groups:get_my(dht_node),
                bloom_sync:start_bloom_sync(SyncStruct, DhtNodePid);
            merkleTree  -> ok;
            art         -> ok
        end,
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Web Debug Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({web_debug_info, Requestor}, 
   {SyncMethod, _TriggerState, Round, _MonitorTable} = State) ->
    KeyValueList =
        [{"Sync Method:", SyncMethod},
         {"Bloom Module:", ?REP_BLOOM},
         {"Sync Round:", Round}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Monitor Reporting
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({sync_progress_report, _Sender, Msg}, {_SyncMethod, _TriggerState, _Round, MonitorTable} = State) ->
    monitor:proc_set_value(MonitorTable, io_lib:format("~p", [erlang:localtime()]), Msg),
    State;
on({report_to_monitor}, {_SyncMethod, _TriggerState, _Round, MonitorTable} = State) ->
    monitor:proc_report_to_my_monitor(MonitorTable),
    comm:send_local_after(monitor:proc_get_report_interval() * 1000, 
                          self(),
                          {report_to_monitor}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% SyncStruct building
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc builds desired synchronization structure and replies it to SourcePid
-spec build_SyncStruct(comm:erl_local_pid(), sync_method(), db_chunk(), Round::non_neg_integer()) -> ok.
build_SyncStruct(SourcePid, SyncMethod, {ChunkInterval, _DBItems} = DB, Round) ->
    SyncStruct = case SyncMethod of
                     bloom ->
                         build_bloom(DB, Round, SourcePid);
                     merkleTree ->
                         ok; %TODO
                     art ->
                         ok %TODO
                 end,
    comm:send_local(SourcePid, {get_sync_struct_response, ChunkInterval, SyncStruct}),
    ?TRACE("build_SyncStruct - finished send response to [~p]", [SourcePid]),
    ok.

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
            AddKey = lists:min(?RT:get_replica_keys(Key)), %map keys to their smallest associated key
            NewKeyBF = ?REP_BLOOM:add(KeyBF, AddKey),
            NewVerBF = ?REP_BLOOM:add(VerBF, erlang:list_to_binary([term_to_binary(AddKey), "#", Ver])),
            fill_bloom(T, NewKeyBF, NewVerBF)            
    end.

%% @doc Build bloom filter sync struct.
-spec build_bloom(db_chunk(), non_neg_integer(), pid()) -> sync_struct().
build_bloom({Interval, DB}, Round, SourcePid) ->
    Fpr = 0.001,           %TODO move to config?
    ElementNum = 10,    %TODO set to node db item cout + 5%?
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?REP_HFS:new(HFCount),
    BF1 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),
    BF2 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),    
    ?TRACE("FillBloom with [~p] Items of Interval ~p", [length(DB), Interval]),
    {KeyBF, VerBF} = fill_bloom(DB, BF1, BF2),
    {bloom_sync_struct, Interval, SourcePid, KeyBF, VerBF, Round}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the replica update process, 
%%      registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = get_update_trigger(),
    gen_component:start_link(?MODULE, Trigger,
                             [{pid_groups_join_as, DHTNodeGroup, ?PROCESS_NAME}]).

%% @doc Initialises the module and starts the trigger
-spec init(module()) -> state().
init(Trigger) ->	
    TriggerState = trigger:init(Trigger, fun get_update_interval/0, ?TRIGGER_NAME),
    comm:send_local_after(monitor:proc_get_report_interval() * 1000, 
                          self(),
                          {report_to_monitor}),
    {get_sync_method(), trigger:next(TriggerState), 0, monitor:proc_init(?MODULE)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rep_update_activate) of
        true ->
            config:is_module(rep_update_trigger) and
            config:is_atom(rep_update_sync_method) and	
            config:is_integer(rep_update_interval) and
            config:is_greater_than(rep_update_interval, 0);
        _ -> true
    end.

-spec get_sync_method() -> sync_method().
get_sync_method() -> 
	config:read(rep_update_sync_method).

-spec get_update_trigger() -> Trigger::module().
get_update_trigger() -> 
	config:read(rep_update_trigger).

-spec get_update_interval() -> pos_integer().
get_update_interval() ->
    config:read(rep_update_interval).

