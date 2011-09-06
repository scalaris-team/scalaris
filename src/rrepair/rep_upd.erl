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

-ifdef(with_export_type_support).
-export_type([db_chunk/0, sync_method/0]).
-endif.

-define(TRACE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
%-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% constants
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(TRIGGER_NAME, rep_update_trigger).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type db_chunk() :: {intervals:interval(), ?DB:db_as_list()}.
-type sync_method() :: bloom | merkleTree | art | undefined.

-record(rep_upd_state,
        {
         trigger_state  = ?required(rep_upd_state, trigger_state)   :: trigger:state(),
         sync_round     = 0.0                                       :: float(),
         running_jobs   = 0                                         :: non_neg_integer()
         }).
-type state() :: #rep_upd_state{}.

-type message() ::
    {?TRIGGER_NAME} |
    {request_sync, Round::float(), rep_upd_sync:sync_stage(), sync_method(), rep_upd_sync:sync_struct()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {sync_progress_report, Sender::comm:erl_local_pid(), Key::term(), Value::term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when trigger triggers (INITIATE SYNC BY TRIGGER)
-spec on(message(), state()) -> state().
on({?TRIGGER_NAME}, State = #rep_upd_state{ sync_round = Round,
                                            running_jobs = RunningJobs }) ->
    {ok, Pid} = rep_upd_sync:start_sync(true, Round),
    case get_sync_method() of
        bloom -> comm:send_local(Pid, {start_sync, get_sync_method(), build_struct});
        merkleTree -> comm:send_local(Pid, {start_sync, get_sync_method(), req_shared_interval});
        _ -> ok
    end,
    NewTriggerState = trigger:next(State#rep_upd_state.trigger_state),
    %?TRACE("Trigger NEXT", []),
    State#rep_upd_state{ trigger_state = NewTriggerState, 
                         sync_round = Round + 1,
                         running_jobs = RunningJobs + 1 };

%% @doc receive sync request and spawn a new process which executes a sync protocol
on({request_sync, Round, SyncStage, SyncMethod, SyncStruct}, State) ->
    monitor:proc_set_value(?MODULE, "Recv-Sync-Req-Count",
                           fun(Old) -> rrd:add_now(1, Old) end),
    {ok, Pid} = rep_upd_sync:start_sync(false, Round),
    comm:send_local(Pid, {start_sync, SyncMethod, SyncStage, SyncStruct}),
    RunningJobs = State#rep_upd_state.running_jobs,
    State#rep_upd_state{ running_jobs = RunningJobs + 1 };

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
on({sync_progress_report, Sender, Round, SyncMaster, SyncStats}, 
    State = #rep_upd_state{ running_jobs = Jobs}) ->
    %monitor:proc_set_value(?MODULE, "Progress",
    %                       fun(Old) -> rrd:add_now(Value, Old) end),
    SyncMaster andalso
        ?TRACE("SYNC FINISHED - Round=~p - Sender=~p - Master=~p~nStats=~p", 
               [Round, Sender, SyncMaster, rep_upd_sync:print_sync_stats(SyncStats)]),
    %?TRACE("Running Jobs = [~p]", [State#rep_upd_state.running_jobs - 1]),
    State#rep_upd_state{ running_jobs = Jobs - 1 }.

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
                             [{pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

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
            config:cfg_is_module(rep_update_trigger) andalso
            config:cfg_is_atom(rep_update_sync_method) andalso	
            config:cfg_is_integer(rep_update_interval) andalso
            config:cfg_is_greater_than(rep_update_interval, 0);
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
