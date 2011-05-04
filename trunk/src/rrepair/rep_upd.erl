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

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% constants
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(PROCESS_NAME, ?MODULE).
-define(TRIGGER_NAME, rep_update_trigger).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type db_chunk() :: {intervals:interval(), ?DB:db_as_list()}.
-type sync_struct() :: %TODO add merkleTree + art
    {   
        Interval    :: intervals:interval(), 
        SrcNode     :: pid(),
        KeyBF       :: ?REP_BLOOM:bloomFilter(),
        KeyVersBF   :: ?REP_BLOOM:bloomFilter()
    }. 
-type sync_method() :: bloom | merkleTree | art.
-type state() :: 
    {
        Sync_method     :: sync_method(),  
        TriggerState    :: trigger:state()
    }.
-type message() ::
	{?TRIGGER_NAME} |
    {recv_sync, sync_method(), sync_struct()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when trigger triggers (INITIATE SYNC BY TRIGGER)
-spec on(message(), state()) -> state().
on({?TRIGGER_NAME}, {SyncMethod, TriggerState}) ->
	%STEPS
	%1) SELECT DEST
	%2) CREATE SYNC STRUCT
    %retrieve node db-intervall
    DhtNodePid = pid_groups:find_a(dht_node),
    comm:send_local(DhtNodePid, {get_state, comm:this(), my_range}),
	%3) SEND SyncStruct 2 DEST
	%4) DONE - NEW TRIGGER
	NewTriggerState = trigger:next(TriggerState),
    ?TRACE("~nDEBUG Trigger NEXT~n", []),
	{SyncMethod, NewTriggerState};

%% @doc retrieve node responsibility interval
on({get_state_response, NodeDBInterval}, State) ->
    ?TRACE("~nDEBUG DB-Intervall ~p~n", [NodeDBInterval]),
    DhtNodePid = pid_groups:find_a(dht_node),
    comm:send_local(DhtNodePid, {get_chunk, self(), NodeDBInterval, 10000}), %TODO remove 10k constant
    State;
%% @doc retriebe local node db
on({get_chunk_response, DB}, {SyncMethod, _TriggerState} = State) ->
    ?TRACE("~nDEBUG Node-DB ~p~n", [DB]),
    spawn(fun build_SyncStruct/3, [self(), SyncMethod, DB]),
    State;

on({recv_sync, bloom, {_Interval, _Bloom}}, State) ->
	%1) FIND ALL DB-ENTRIES NOT IN KEY VERS BLOOM (KV_BF)
    %a) CHECK IF ENTRY ALSO NOT IN K_BF -> MISSING REP -> DO REGEN
    %b) IF NOT IN KV_BF AND IN K_BF -> outdated found -> DO UDPATE
	State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MerkleTree Sync Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%TODO
on({recv_sync, merkleTree, _}, _) ->
	ok;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Web Debug Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({web_debug_info, Requestor}, 
   {SyncMethod, _TriggerState} = State) ->
    KeyValueList =
        [{"Sync Method:", SyncMethod},
         {"Bloom Module:", ?REP_BLOOM}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% SyncStruct building
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_SyncStruct(comm:erl_local_pid(), sync_method(), db_chunk()) -> sync_struct().
build_SyncStruct(SourcePid, SyncMethod, DB) ->
    SyncStruct = case SyncMethod of
                     bloom ->
                         build_bloom(DB);
                     merkleTree ->
                         ok; %TODO
                     art ->
                         ok %TODO
                 end,
    comm:send_local(SourcePid, {get_sync_struct_response, SyncStruct}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% BloomFilter building
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Build two bloom filter, one over all keys (BF1), 
%%      one over all keys concatenated with their version (BF2)
-spec build_bloom(db_chunk()) -> sync_struct().
build_bloom({Interval, _DB}) ->
    %create bloom filter
    Fpr = 0.001,           %TODO move to config?
    ElementNum = 10000,    %TODO set to node db item cout + 5%?
    HFCount = bloom:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?REP_HFS:new(HFCount),
    BF1 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),
    BF2 = ?REP_BLOOM:new(ElementNum, Fpr, Hfs),    
    %fill bloom filter
    %TODO
	%lists:min(?RT:get_replica_keys(Key)) %map keys to their smallest associated key
	{Interval, comm:this(), BF1, BF2}.

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
    {get_sync_method(), trigger:next(TriggerState)}.

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

