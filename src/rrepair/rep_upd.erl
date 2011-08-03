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

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([start_link/1, init/1, on/2, check_config/0]).
-export([concatKeyVer/1, concatKeyVer/2, minKey/1]).

-ifdef(with_export_type_support).
-export_type([db_chunk/0, sync_method/0]).
-endif.

%-define(TRACE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
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
-type sync_method() :: bloom | merkleTree | art.

-record(rep_upd_state,
        {
         trigger_state  = ?required(rep_upd_state, trigger_state)   :: trigger:state(),
         sync_round     = 0.0                                       :: float()
         }).
-type state() :: #rep_upd_state{}.

-type message() ::
    {?TRIGGER_NAME} |
    {get_state_response, any()} |
    {get_chunk_response, db_chunk()} |
    {build_sync_struct_response, intervals:interval(), Round::float(), rep_upd_sync:sync_struct()} |
    {request_sync, rep_upd_sync:sync_stage(), Feedback::boolean(), Round::float(), rep_upd_sync:sync_struct()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {sync_progress_report, Sender::comm:erl_local_pid(), Key::term(), Value::term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when trigger triggers (INITIATE SYNC BY TRIGGER)
-spec on(message(), state()) -> state().
on({?TRIGGER_NAME}, State) ->
    DhtNodePid = pid_groups:get_my(dht_node),
    comm:send_local(DhtNodePid, {get_state, comm:this(), my_range}),
    NewTriggerState = trigger:next(State#rep_upd_state.trigger_state),
    ?TRACE("Trigger NEXT", []),
    State#rep_upd_state{ trigger_state = NewTriggerState };

%% @doc retrieve own responsibility interval
on({get_state_response, NodeDBInterval}, State) ->
    DhtNodePid = pid_groups:get_my(dht_node),
    case get_sync_method() of
        bloom -> 
            comm:send_local(DhtNodePid, {get_chunk, self(), NodeDBInterval, get_max_items()}),
            State;
        merkleTree -> 
            Round = State#rep_upd_state.sync_round,
            {ok, Pid} = rep_upd_sync:start_sync(get_max_items(), true, Round),
            %get dest node pid and start failure detector
            comm:send_local(DhtNodePid, 
                            {lookup_aux, select_sync_node(NodeDBInterval), 0, 
                             {send_to_group_member, ?PROCESS_NAME, 
                              {request_sync, negotiate_interval, true, Round, {Pid, NodeDBInterval}}}}),
            State#rep_upd_state{ sync_round = Round + 1 };    
        art -> ok
    end;    

%% @doc retrieve local node db
on({get_chunk_response, {_, []}}, State) ->
    State;
on({get_chunk_response, {RestI, [First | T] = DBList}}, State) ->
    Round = State#rep_upd_state.sync_round,
    DhtNodePid = pid_groups:get_my(dht_node),
    RestIEmpty = intervals:is_empty(RestI),
    not RestIEmpty andalso
        comm:send_local(DhtNodePid, {get_chunk, self(), RestI, get_max_items()}),
    %Get Interval of DBList
    %TODO: IMPROVEMENT getChunk should return ChunkInterval (db is traversed twice! - 1st getChunk, 2nd here)
    ChunkI = intervals:new('[', db_entry:get_key(First), db_entry:get_key(lists:last(T)), ']'),
    %?TRACE("RECV CHUNK interval= ~p  - RestInterval= ~p - DBLength=~p", [ChunkI, RestI, length(DBList)]),
    SyncMethod = get_sync_method(),
    Args = case SyncMethod of
               bloom -> [get_sync_fpr()];
               _ -> []
           end,
    {ok, Pid} = rep_upd_sync:start_sync(get_max_items(), true, Round),
    comm:send_local(Pid, {build_sync_struct, SyncMethod, {ChunkI, DBList}, Args}),
    %?TRACE("[~p] will build SyncStruct", [Pid]),
    case RestIEmpty of
        true -> State#rep_upd_state{ sync_round = Round + 1 };
        false -> State#rep_upd_state{ sync_round = Round + 0.1 }
    end;

%% @doc SyncStruct is build and can be send to a node for synchronization
on({build_sync_struct_response, Interval, Round, Sync_Struct}, State) ->
    _ = case intervals:is_empty(Interval) of	
            false ->
                DestKey = select_sync_node(Interval),
                DhtNodePid = pid_groups:get_my(dht_node),
                %?TRACE("SEND SYNC REQ TO [~p]", [DestKey]),
                comm:send_local(DhtNodePid, 
                                {lookup_aux, DestKey, 0, 
                                 {send_to_group_member, ?PROCESS_NAME, 
                                  {request_sync, reconciliation, true, Round, Sync_Struct}}}),            
                monitor:proc_set_value(?MODULE, "Send-Sync-Req",
                    fun(Old) -> rrd:add_now({Round, DestKey}, Old) end);
            _ -> ok
		end,
    State;

%% @doc receive sync request and spawn a new process which executes a sync protocol
on({request_sync, SyncStage, Feedback, Round, SyncStruct}, State) ->
    monitor:proc_set_value(?MODULE, "Recv-Sync-Req-Count",
                           fun(Old) -> rrd:add_now(1, Old) end),
    {ok, Pid} = rep_upd_sync:start_sync(get_max_items(), false, Round),
    comm:send_local(Pid, {start_sync_client, SyncStage, Feedback, SyncStruct}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Web Debug Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({web_debug_info, Requestor}, State) ->
    #rep_upd_state{ sync_round = Round } = State,
    KeyValueList =
        [{"Sync Method:", get_sync_method()},
         {"Bloom Module:", ?REP_BLOOM},
         {"Sync Round:", Round}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Monitor Reporting
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({sync_progress_report, _Sender, _Key, Value}, State) ->
    monitor:proc_set_value(?MODULE, "Progress",
                           fun(Old) -> rrd:add_now(Value, Old) end),
    ?TRACE("SYNC FINISHED - Key=~p REASON=[~p]", [Key, Value]),
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
    term_to_binary([Key, "#", Version]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% INTERNAL HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc selects a random associated key of an interval ending
-spec select_sync_node(intervals:interval()) -> ?RT:key().
select_sync_node(Interval) ->
    {_, _, RKey, RBr} = intervals:get_bounds(Interval),
    Key = case RBr of
              ')' -> RKey - 1;
              ']' -> RKey
          end,
    Keys = lists:delete(Key, ?RT:get_replica_keys(Key)),
    lists:nth(random:uniform(erlang:length(Keys)), Keys).


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
    monitor:proc_set_value(?MODULE, "Recv-Sync-Req-Count", rrd:create(60 * 1000000, 3, counter)), % 60s monitoring interval
    monitor:proc_set_value(?MODULE, "Send-Sync-Req", rrd:create(60 * 1000000, 3, event)), % 60s monitoring interval
    monitor:proc_set_value(?MODULE, "Progress", rrd:create(60 * 1000000, 3, event)), % 60s monitoring interval
    #rep_upd_state{trigger_state = trigger:next(TriggerState)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rep_update_activate) of
        true ->
            config:is_module(rep_update_trigger) andalso
            config:is_atom(rep_update_sync_method) andalso	
            config:is_integer(rep_update_interval) andalso
            config:is_float(rep_update_fpr) andalso
            config:is_greater_than(rep_update_fpr, 0) andalso
            config:is_less_than(rep_update_fpr, 1) andalso
            config:is_integer(rep_update_max_items) andalso
            config:is_greater_than(rep_update_max_items, 0) andalso
            config:is_greater_than(rep_update_interval, 0);
        _ -> true
    end.

-spec get_max_items() -> pos_integer().
get_max_items() ->
    config:read(rep_update_max_items).

-spec get_sync_fpr() -> float().
get_sync_fpr() ->
    config:read(rep_update_fpr).

-spec get_sync_method() -> sync_method().
get_sync_method() -> 
	config:read(rep_update_sync_method).

-spec get_update_trigger() -> Trigger::module().
get_update_trigger() -> 
	config:read(rep_update_trigger).

-spec get_update_interval() -> pos_integer().
get_update_interval() ->
    config:read(rep_update_interval).

