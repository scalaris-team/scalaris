% @copyright 2011, 2012 Zuse Institute Berlin

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
%% @doc    replica repair module 
%%         Replica sets will be synchronized in two steps.
%%          I) reconciliation - find set differences  (rr_recon.erl)
%%         II) resolution - resolve found differences (rr_resolve.erl)
%%
%%         Examples:
%%            1) remote node should get a single kvv-pair (key,value,version)
%%               >>comm:send(RemoteRRepairPid, {request_resolve, {key_upd, [{Key, Value, Version}]}, []}).
%%
%% @end
%% @version $Id$

-module(rrepair).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([start_link/1, init/1, on/2, check_config/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE_KILL(X,Y), ok).
%-define(TRACE_KILL(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

-define(TRACE_RECON(X,Y), ok).
%-define(TRACE_RECON(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

-define(TRACE_RESOLVE(X,Y), ok).
%-define(TRACE_RESOLVE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% constants
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(TRIGGER_NAME, rr_trigger).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([round/0]).
-endif.

-type round() :: {non_neg_integer(), non_neg_integer()}.

-record(rrepair_state,
        {
         trigger_state  = ?required(rrepair_state, trigger_state)   :: trigger:state(),
         round          = {0, 0}                                    :: round(),
         open_recon     = 0                                         :: non_neg_integer(),
         open_resolve   = 0                                         :: non_neg_integer()
         }).
-type state() :: #rrepair_state{}.

-type message() ::
    {?TRIGGER_NAME} |    
    % API
    {request_sync, Method::rr_recon:method(), DestKey::random | ?RT:key()} |
    {request_resolve, Round::round(), rr_resolve:operation(), rr_resolve:options()} |
    {get_state, Sender::comm:mypid(), Key::atom()} |
    % internal
    {continue_recon, SenderRRPid::comm:mypid(), Round::round(), ReqMsg::rr_recon:request()} |
    {recon_forked} |
    % misc
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    % rr statistics
    {rr_stats, rr_statistics:requests()} |
    % report
    {recon_progress_report, Sender::comm:erl_local_pid(), Round::round(), 
        Master::boolean(), Stats::rr_recon_stats:stats()} |
    {resolve_progress_report, Sender::comm:erl_local_pid(), Round::round(), 
        Stats::rr_resolve:stats()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().

% @doc request replica repair status
on({get_state, Sender, Key}, State = 
       #rrepair_state{ open_recon = Recon,
                       open_resolve = Resolve,
                       round = Round }) ->
    Value = case Key of
                open_recon -> Recon;
                open_resolve -> Resolve;
                round -> Round;
                open_sync -> Recon + Resolve
            end,
    comm:send(Sender, {get_state_response, Value}),
    State;

on({?TRIGGER_NAME}, State) ->
    ?TRACE_KILL("RR: SYNC TRIGGER", []),
    comm:send_local(self(), {request_sync, get_recon_method(), random}),
    NewTriggerState = trigger:next(State#rrepair_state.trigger_state),
    State#rrepair_state{ trigger_state = NewTriggerState };

% @doc Requests database synchronization with DestPid (DestPid=DhtNodePid or random).
%      Random leads to sync with a node which is associated with this (e.g. symmetric partner)  
on({request_sync, Method, DestKey}, State = #rrepair_state{ round = Round, 
                                                            open_recon = OpenRecon }) ->
    ?TRACE_KILL("RR: REQUEST SYNC WITH ~p", [DestKey]),
    {ok, Pid} = rr_recon:start(Round),
    comm:send_local(Pid, {start, Method, DestKey}),
    State#rrepair_state{ round = next_round(Round),
                         open_recon = OpenRecon + 1 };

on({request_resolve, Round, Operation, Options}, State = #rrepair_state{ open_resolve = OpenResolve }) ->
    _ = rr_resolve:start(Round, Operation, Options),
    State#rrepair_state{ open_resolve = OpenResolve + 1 };

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc receive sync request and spawn a new process which executes a sync protocol
on({continue_recon, Sender, Round, Msg}, State) ->
    ?TRACE_KILL("CONTINUE RECON FROM ~p", [Sender]),
    {ok, Pid} = rr_recon:start(Round, Sender),
    comm:send_local(Pid, Msg),
    State#rrepair_state{ open_recon = State#rrepair_state.open_recon + 1 };

on({rr_stats, Msg}, State) ->
    {ok, Pid} = rr_statistics:start(),
    comm:send_local(Pid, Msg),
    State;

on({recon_forked}, State) ->
    State#rrepair_state{ open_recon = State#rrepair_state.open_recon + 1 };

on({recon_progress_report, _Sender, _Round, _Master, Stats}, State) ->
    OpenRecon = State#rrepair_state.open_recon - 1,
    rr_recon_stats:get(finish, Stats) andalso
        ?TRACE_RECON("~nRECON OK - Round=~p - Sender=~p - Master=~p~nStats=~p~nOpenRecon=~p", 
                     [_Round, _Sender, _Master, rr_recon_stats:print(Stats), OpenRecon]),
    State#rrepair_state{ open_recon = OpenRecon };

on({resolve_progress_report, _Sender, _Stats}, State) ->
    OpenResolve = State#rrepair_state.open_resolve - 1,
    ?TRACE_RESOLVE("~nRESOLVE OK - Sender=~p ~nStats=~p~nOpenRecon=~p ; OpenResolve=~p", 
           [_Sender, rr_resolve:print_resolve_stats(_Stats),
            State#rrepair_state.open_recon, OpenResolve]),    
    State#rrepair_state{ open_resolve = OpenResolve };

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc Info Messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({web_debug_info, Requestor}, State) ->
    #rrepair_state{ round = Round, 
                    open_recon = OpenRecon, 
                    open_resolve = OpenResol } = State,
    KeyValueList =
        [{"Recon Method:", get_recon_method()},
         {"Bloom Module:", ?REP_BLOOM},
         {"Sync Round:", Round},
         {"Open Recon Jobs:", OpenRecon},
         {"Open Resolve Jobs:", OpenResol}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec next_round(round()) -> round().
next_round({R, F}) -> {R + 1, F}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the replica update process, 
%%      registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = get_update_trigger(),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Trigger,
                             [{pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

%% @doc Initialises the module and starts the trigger
-spec init(module()) -> state().
init(Trigger) ->	
    TriggerState = trigger:init(Trigger, fun get_update_interval/0, ?TRIGGER_NAME),
    monitor:proc_set_value(?MODULE, 'Recv-Sync-Req-Count', rrd:create(60 * 1000000, 3, counter)), % 60s monitoring interval
    monitor:proc_set_value(?MODULE, 'Send-Sync-Req', rrd:create(60 * 1000000, 3, event)), % 60s monitoring interval
    monitor:proc_set_value(?MODULE, 'Progress', rrd:create(60 * 1000000, 3, event)), % 60s monitoring interval
    #rrepair_state{trigger_state = trigger:next(TriggerState)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rrepair_enabled) of
        true ->
            config:cfg_is_module(rr_trigger) andalso
            config:cfg_is_atom(rr_recon_method) andalso
            config:cfg_is_integer(rr_trigger_interval) andalso
            config:cfg_is_greater_than(rr_trigger_interval, 0);
        _ -> true
    end.

-spec get_recon_method() -> rr_recon:method().
get_recon_method() -> 
	config:read(rr_recon_method).

-spec get_update_trigger() -> Trigger::module().
get_update_trigger() -> 
	config:read(rr_trigger).

-spec get_update_interval() -> pos_integer().
get_update_interval() ->
    config:read(rr_trigger_interval).
