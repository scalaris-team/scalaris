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
%%          I) reconciliation   - find set differences  (rr_recon.erl)
%%         II) resolution       - resolve found differences (rr_resolve.erl)
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

-define(TRACE_RECON(X,Y), ok).
%-define(TRACE_RECON(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

-define(TRACE_RESOLVE(X,Y), ok).
%-define(TRACE_RESOLVE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

-define(TRACE_COMPLETE(X,Y), ok).
%-define(TRACE_COMPLETE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% constants
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(TRIGGER_NAME, rr_trigger).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% export
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([start_link/1, init/1, on/2, check_config/0,
         fork_session/1, session_id_equal/2]).

-ifdef(with_export_type_support).
-export_type([session_id/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type round()       :: {non_neg_integer(), non_neg_integer()}.
-type session_id()  :: {round(), comm:mypid()}.
% @doc session contains only data of the sync request initiator thus rs_stats:regen_count represents only 
%      number of regenerated db items on the initator
-record(session, 
        { id                = ?required(session, id)            :: session_id(), 
          rc_method         = ?required(session, rc_method)     :: rr_recon:method(), 
          rc_stats          = none                              :: rr_recon_stats:stats() | none,
          rs_stats          = none                              :: rr_resolve:stats() | none,
          rs_called         = 0                                 :: non_neg_integer(),
          rs_finish         = 0                                 :: non_neg_integer()
        }).
-type session() :: #session{}.
%TODO Session TTL

-record(rrepair_state,
        {
         trigger_state  = ?required(rrepair_state, trigger_state)   :: trigger:state(),
         round          = {0, 0}                                    :: round(),
         open_recon     = 0                                         :: non_neg_integer(),
         open_resolve   = 0                                         :: non_neg_integer(),
         open_sessions  = []                                        :: [] | [session()]   % List of running request_sync calls (only rounds initiated by this process) 
         }).
-type state() :: #rrepair_state{}.

-type state_field() :: round |           %next round id
                       open_recon |      %number of open recon processes
                       open_resolve |    %number of open resolve processes
                       open_sessions.    %number of current running sync sessions

-type message() ::
    % API
    {request_sync, DestKey::random | ?RT:key()} |
    {request_sync, Method::rr_recon:method(), DestKey::random | ?RT:key()} |
    {request_resolve, rr_resolve:operation(), rr_resolve:options()} |
    {get_state, Sender::comm:mypid(), Key::state_field()} |
    % internal
    {?TRIGGER_NAME} |
	{continue_recon, SenderRRPid::comm:mypid(), session_id() | null, ReqMsg::rr_recon:request()} |
    {request_resolve, session_id() | null, rr_resolve:operation(), rr_resolve:options()} |
    {recon_forked} |
    {request_sync_complete, session()} |
    % misc
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    % rr statistics
    {rr_stats, rr_statistics:requests()} |
    % report
    {recon_progress_report, Sender::comm:erl_local_pid(), Initiator::boolean(), Stats::rr_recon_stats:stats()} |
    {resolve_progress_report, Sender::comm:erl_local_pid(), Stats::rr_resolve:stats()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().

% Requests db sync with DestKey using default recon method (given in config).
on({request_sync, DestKey}, State) ->
    comm:send_local(self(), {request_sync, get_recon_method(), DestKey}),
    State;

% Requests database synchronization with DestPid (DestPid=DhtNodePid or random).
%   Random leads to sync with a node which is associated with this (e.g. symmetric partner)  
on({request_sync, Method, DestKey}, State = #rrepair_state{ round = Round, 
                                                            open_recon = OpenRecon,
                                                            open_sessions = Sessions }) ->
    ?TRACE("RR: REQUEST SYNC WITH ~p", [DestKey]),
    S = new_session(Round, comm:this(), Method),
    {ok, Pid} = rr_recon:start(S#session.id),
    comm:send_local(Pid, {start, Method, DestKey}),
    State#rrepair_state{ round = next_round(Round),
                         open_recon = OpenRecon + 1,
                         open_sessions = [S | Sessions] };

on({request_resolve, Operation, Options}, State = #rrepair_state{ open_resolve = OpenResolve }) ->
    {ok, Pid} = rr_resolve:start(),
    comm:send_local(Pid, {start, Operation, Options}),
    State#rrepair_state{ open_resolve = OpenResolve + 1 };

% request replica repair status
on({get_state, Sender, Key}, State = 
       #rrepair_state{ open_recon = Recon,
                       open_resolve = Resolve,
                       round = Round,
                       open_sessions = Sessions }) ->
    Value = case Key of
                open_recon -> Recon;
                open_resolve -> Resolve;
                round -> Round;
                open_sessions -> length(Sessions)
            end,
    comm:send(Sender, {get_state_response, Value}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% internal messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({?TRIGGER_NAME}, State) ->
    ?TRACE("RR: SYNC TRIGGER", []),
    Prob = get_start_prob(),
    Random = randoms:rand_uniform(1, 100),
    if Random =< Prob ->           
           comm:send_local(self(), {request_sync, get_recon_method(), random});
       true -> ok
    end,
    NewTriggerState = trigger:next(State#rrepair_state.trigger_state),
    State#rrepair_state{ trigger_state = NewTriggerState };

on({request_resolve, SessionID, Operation, Options}, State = #rrepair_state{ open_resolve = OpenResolve }) ->
    {ok, Pid} = rr_resolve:start(SessionID),
    comm:send_local(Pid, {start, Operation, Options}),
    State#rrepair_state{ open_resolve = OpenResolve + 1 };

%% @doc receive sync request and spawn a new process which executes a sync protocol
on({continue_recon, Sender, SessionID, Msg}, State) ->
    ?TRACE("CONTINUE RECON FROM ~p", [Sender]),
    {ok, Pid} = rr_recon:start(SessionID, Sender),
    comm:send_local(Pid, Msg),
    State#rrepair_state{ open_recon = State#rrepair_state.open_recon + 1 };

on({recon_forked}, State) ->
    State#rrepair_state{ open_recon = State#rrepair_state.open_recon + 1 };

%% @doc will be called after finishing a request_sync API call
%%      Could be used to send request caller a finished Msg.
on({request_sync_complete, Session}, State = #rrepair_state{ open_sessions = Sessions }) ->
    ?TRACE_COMPLETE("--SESSION COMPLETE--~n~p", [Session]),
    NewOpen = lists:delete(Session, Sessions),
    State#rrepair_state{ open_sessions = NewOpen };

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({rr_stats, Msg}, State) ->
    {ok, Pid} = rr_statistics:start(),
    comm:send_local(Pid, Msg),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% report messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({recon_progress_report, _Sender, Initiator, Stats}, State = #rrepair_state{ open_recon = OpenRecon,
                                                                             open_sessions = Sessions }) ->
    %rr_recon_stats:get(status, Stats) =:= finish andalso
        ?TRACE_RECON("~nRECON OK - Sender=~p - Initiator=~p~nStats=~p~nOpenRecon=~p~nSessions=~p", 
                     [_Sender, Initiator, rr_recon_stats:print(Stats), OpenRecon - 1, Sessions]),    
    NSessions = case Initiator of
                    true -> 
                        case extract_session(rr_recon_stats:get(session_id, Stats), Sessions) of
                            {S, TSessions} ->
                                SUpd = update_session_recon(S, Stats),
                                check_session_complete(SUpd),
                                [SUpd | TSessions];
                            not_found ->
                                %caused by error or forked rc instances by bloom filter rc
                                %log:log(error, "[ ~p ] SESSION NOT FOUND BY INITIATOR ~p", [?MODULE, rr_recon_stats:get(session_id, Stats)]),
                                Sessions
                        end;
                    false -> Sessions
                end,
    State#rrepair_state{ open_recon = OpenRecon - 1,
                         open_sessions = NSessions };

on({resolve_progress_report, _Sender, Stats}, State = #rrepair_state{open_resolve = OpenResolve,
                                                                     open_sessions = Sessions}) ->
    NSessions = case extract_session(rr_resolve:get_stats_session_id(Stats), Sessions) of
                    not_found -> Sessions;
                    {S, T} -> 
                        SUpd = update_session_resolve(S, Stats),
                        check_session_complete(SUpd),
                        [SUpd | T]
                end,
    ?TRACE_RESOLVE("~nRESOLVE OK - Sender=~p ~nStats=~p~nOpenRecon=~p ; OpenResolve=~p ; OldSession=~p~nNewSessions=~p", 
                   [_Sender, rr_resolve:print_resolve_stats(Stats),
                    State#rrepair_state.open_recon, OpenResolve - 1, Sessions, NSessions]),        
    State#rrepair_state{ open_resolve = OpenResolve - 1,
                         open_sessions = NSessions };

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% misc info messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({web_debug_info, Requestor}, #rrepair_state{ round = Round, 
                                                open_recon = OpenRecon, 
                                                open_resolve = OpenResol,
                                                open_sessions = Sessions } = State) ->
    ?TRACE("WEB DEBUG INFO", []),
    KeyValueList =
        [{"Recon Method:", get_recon_method()},
         {"Bloom Module:", ?REP_BLOOM},
         {"Sync Round:", Round},
         {"Open Recon Jobs:", OpenRecon},
         {"Open Resolve Jobs:", OpenResol},
         {"Open Sessions:", length(Sessions)}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec next_round(round()) -> round().
next_round({R, _Fork}) -> {R + 1, 0}.

-spec new_session(round(), comm:mypid(), rr_recon:method()) -> session().
new_session(Round, Pid, RCMethod) ->
    #session{ id = {Round, Pid}, rc_method = RCMethod }.

-spec session_id_equal(session_id(), session_id()) -> boolean().
session_id_equal({{R, _}, Pid}, {{R, _}, Pid}) -> true;
session_id_equal(_, _) -> false.

-spec fork_session(session_id()) -> session_id().
fork_session({{R, F}, Pid}) ->
    {{R, F + 1}, Pid}.

-spec extract_session(session_id(), [session()]) -> {session(), Remain::[session()]} | not_found.
extract_session(Id, Sessions) ->
    {Satis, NotSatis} = lists:partition(fun(#session{ id = I }) -> 
                                                session_id_equal(Id, I)
                                        end, 
                                        Sessions),
    case length(Satis) of
        1 -> {hd(Satis), NotSatis};
        0 -> not_found;
        _ -> 
            log:log(error, "[ ~p ] SESSION NOT UNIQUE! ~p - OpenSessions=~p", [?MODULE, Id, Sessions]),
            not_found
    end.

-spec update_session_recon(session(), rr_recon_stats:stats()) -> session().
update_session_recon(Session, New) ->
    case rr_recon_stats:get(status, New) of
        wait -> Session;
        _ -> Session#session{ rc_stats  = New,
                              rs_called = rr_recon_stats:get(resolve_started, New) }
    end.

-spec update_session_resolve(session(), rr_resolve:stats()) -> session().
update_session_resolve(#session{ rs_stats = none, rs_finish = RSCount } = S, Stats) ->
    S#session{ rs_stats = Stats, rs_finish = RSCount + 1 };
update_session_resolve(#session{ rs_stats = Old, rs_finish = RSCount } = S, New) ->
    Merge = rr_resolve:merge_stats(Old, New),
    S#session{ rs_stats = Merge, rs_finish = RSCount + 1 }.

-spec check_session_complete(session()) -> ok.
check_session_complete(#session{ rc_stats = RCStats, 
                                 rs_called = C, rs_finish = C } = S) 
  when RCStats =/= none->
    case rr_recon_stats:get(status, RCStats) of
        finish -> comm:send_local(self(), {request_sync_complete, S});
        _ -> ok
    end;
check_session_complete(_Session) ->
    ok.

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
    #rrepair_state{trigger_state = trigger:next(TriggerState)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%
% USED CONFIG FIELDS
%	I) 	 rr_trigger: module name of any trigger
%	II)  rr_trigger_interval: integer duration until next triggering
%	III) rr_recon_metod: set reconciliation algorithm name
%   IV)  rr_trigger_probability: this is the probability of starting a synchronisation 
%								 with a random node if trigger has fired. ]0,100]   
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    case config:read(rrepair_enabled) of
        true ->
            config:cfg_is_module(rr_trigger) andalso
            config:cfg_is_atom(rr_recon_method) andalso
			config:cfg_is_integer(rr_trigger_probability) andalso
			config:cfg_is_greater_than(rr_trigger_probability, 0) andalso
			config:cfg_is_less_than_equal(rr_trigger_probability, 100) andalso
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

-spec get_start_prob() -> pos_integer().
get_start_prob() ->
	config:read(rr_trigger_probability).
