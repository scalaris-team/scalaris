% @copyright 2011-2015 Zuse Institute Berlin

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
%%            1) remote node should get a single kvv-pair (Key, Value, Version)
%%               with Key mapped into first quadrant
%%               >>comm:send(RemoteRRepairPid, {request_resolve, {?key_upd, [{Key, Value, Version}], []}, []}).
%%
%% @end
%% @version $Id$
-module(rrepair).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), log:pal("~w [~p:~.0p] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).

-define(TRACE_RECON(X,Y), ok).
%-define(TRACE_RECON(X,Y), log:pal("~w [~p:~.0p] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).

-define(TRACE_RESOLVE(X,Y), ok).
%-define(TRACE_RESOLVE(X,Y), log:pal("~w [~p:~.0p] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).

-define(TRACE_COMPLETE(X,Y), ok).
%-define(TRACE_COMPLETE(X,Y), log:pal("~w [~p:~.0p] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% export
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([start_link/1, init/1, on/2, check_config/0,
         select_sync_node/2,
         session_get/2]).

-export_type([session_id/0, session/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type round()       :: non_neg_integer().
-type session_id()  :: {round(), comm:mypid()}.
-type principal_id():: comm:mypid() | none.

% @doc session contains only data of the sync request initiator thus rs_stats:regen_count represents only
%      number of regenerated db items on the initator
-record(session,
        { id                = ?required(session, id)            :: session_id(),
          principal         = none                              :: principal_id(),
          rc_method         = ?required(session, rc_method)     :: rr_recon:method(),
          rc_stats          = none                              :: rr_recon_stats:stats() | none,
          rs_stats          = none                              :: rr_resolve:stats() | none,
          rs_expected       = 0                                 :: non_neg_integer(),
          rs_finish         = 0                                 :: non_neg_integer(),
          ttl               = ?required(session, ttl)           :: pos_integer()    %time to live in milliseconds
        }).
-type session() :: #session{}.

-record(rrepair_state,
        {
         round          = 0                                         :: round(),
         open_recon     = 0                                         :: non_neg_integer(),
         open_resolve   = 0                                         :: non_neg_integer(),
         open_sessions  = []                                        :: [session()]   % List of running request_sync calls (only rounds initiated by this process)
         }).
-type state() :: #rrepair_state{}.

-type state_field() :: round |           %next round id
                       open_recon |      %number of open recon processes
                       open_resolve |    %number of open resolve processes
                       open_sessions.    %list of current running sync sessions

-type message() ::
    % API
    {request_sync, DestKey::random | ?RT:key()} |
    {request_sync, Method::rr_recon:method(), DestKey::random | ?RT:key()} |
    {request_sync, Method::rr_recon:method(), DestKey::random | ?RT:key(), Principal::principal_id()} |
    {request_resolve, rr_resolve:operation(), rr_resolve:options()} |
    {get_state, Sender::comm:mypid(), Keys::state_field() | [state_field(),...]} |
    % internal
    {rr_trigger} |
    {rr_gc_trigger} |
    {start_sync, get_range, session_id(), rr_recon:method(), DestKey::random | ?RT:key(),
     {get_state_response, [{my_range, intervals:interval()} | {load, non_neg_integer()},...]}} |
	{start_recon | continue_recon, SenderRRPid::comm:mypid(), session_id(), ReqMsg::rr_recon:request()} |
    {request_resolve | continue_resolve, session_id() | null, rr_resolve:operation(), rr_resolve:options()} |
    % misc
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    % report
    {recon_progress_report, Sender::comm:mypid(), Initiator::boolean(),
     DestRR::comm:mypid(), DestRC::comm:mypid() | undefined, Stats::rr_recon_stats:stats()} |
    {resolve_progress_report, Sender::comm:erl_local_pid(), Stats::rr_resolve:stats()}.

-include("gen_component.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().

% Requests db sync with DestKey using default recon method (given in config).
on({request_sync, DestKey}, State) ->
    request_sync(State, get_recon_method(), DestKey, none);

on({request_sync, Method, DestKey}, State) ->
    request_sync(State, Method, DestKey, none);

on({request_sync, Method, DestKey, Principal}, State) ->
    request_sync(State, Method, DestKey, Principal);

% initial resolve request
on({request_resolve, Operation, Options}, State) ->
    gen_component:post_op({request_resolve, null, Operation, Options}, State);

% request replica repair status
on({get_state, Sender, Key}, State =
       #rrepair_state{ open_recon = Recon, open_resolve = Resolve,
                       round = Round, open_sessions = Sessions }) ->
    Keys = if is_list(Key) -> Key;
              is_atom(Key) -> [Key]
           end,
    Values0 = [case KeyX of
                   open_recon -> Recon;
                   open_resolve -> Resolve;
                   round -> Round;
                   open_sessions -> Sessions
               end || KeyX <- Keys],
    Value = if is_list(Key) -> Values0;
               is_atom(Key) -> hd(Values0)
            end,
    comm:send(Sender, {get_state_response, Value}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% internal messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({rr_trigger} = Msg, State) ->
    ?TRACE("RR: SYNC TRIGGER", []),
    Prob = get_start_prob(),
    Random = randoms:rand_uniform(1, 100),
    if Random =< Prob ->
           comm:send_local(self(), {request_sync, get_recon_method(), random});
       true -> ok
    end,
    case get_trigger_interval() of
        0 -> ok;
        X -> msg_delay:send_trigger(X, Msg)
    end,
    State;

on({rr_gc_trigger} = Msg, State = #rrepair_state{ open_sessions = Sessions }) ->
    Elapsed = get_gc_interval() * 1000,
    NewSessions = [S#session{ ttl = S#session.ttl - Elapsed }
                            || S <- Sessions,
                               S#session.ttl - Elapsed > 0],
    msg_delay:send_trigger(Elapsed div 1000, Msg),
    State#rrepair_state{ open_sessions = NewSessions };

on({start_sync, get_range, SessionId, Method, DestKey,
    {get_state_response, [{my_range, MyI}, {load, MyLoad}]}}, State) ->
    Msg = {?send_to_group_member, rrepair,
           {start_recon, comm:this(), SessionId,
            {create_struct, Method, MyI, MyLoad}}},
    DKey = case DestKey of
               random -> select_sync_node(MyI, true);
               _ -> DestKey
           end,
    % skip if no key outside my range found
    case DKey of
        not_found ->
            #rrepair_state{open_recon = OR, open_sessions = OS} = State,
            % similar to handling of recon_progress_report as initiator
            % assume the session is present (request_sync has created it!)
            {S, TSessions} = extract_session(SessionId, OS),
            ?TRACE_RECON("~nRECON OK3 - ~p", [S]),
            % this session is aborted, so it is complete!
            Stats = rr_recon_stats:new(SessionId, [{status, finish}]),
            SUpd = update_session_recon(S, Stats),
            true = check_session_complete(SUpd),
            State#rrepair_state{open_recon = OR - 1,
                                open_sessions = TSessions};
        _ ->
            ?TRACE_RECON("START_TO_DEST ~p", [DKey]),
            api_dht_raw:unreliable_lookup(DKey, Msg),
            State
    end;

%% @doc receive sync request at the non-initiator and spawn a new process
%%      which executes a sync protocol
on({start_recon, Sender, SessionID, Msg}, State) ->
    ?TRACE_RECON("START RECON FROM ~p", [Sender]),
    {ok, Pid} = rr_recon:start(SessionID, Sender),
    comm:send_local(Pid, Msg),
    State#rrepair_state{ open_recon = State#rrepair_state.open_recon + 1 };

%% @doc receive sync request at the initiator (after setting up the request via
%%      request_sync) and spawn a new process which executes a sync protocol
on({continue_recon, Sender, SessionID, Msg}, State) ->
    ?TRACE_RECON("CONTINUE RECON FROM ~p", [Sender]),
    {ok, Pid} = rr_recon:start(SessionID, Sender),
    comm:send_local(Pid, Msg),
    % note: don't increase open_recon (this is a continued process previously
    %       counted by request_sync)
    State;

% (first or continued) resolve request
on({Tag, SessionID, Operation, Options},
   State = #rrepair_state{open_resolve = OpenResolve})
  when Tag =:= request_resolve orelse Tag =:= continue_resolve ->
    {ok, Pid} = rr_resolve:start(SessionID),
    comm:send_local(Pid, {start, Operation, Options, Tag}),
    State#rrepair_state{ open_resolve = OpenResolve + 1 };

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% report messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({recon_progress_report, Sender, _Initiator = false, DestRR, DestRC, Stats},
   State = #rrepair_state{ open_recon = OR }) ->
    ?TRACE_RECON("~nRECON-NI OK - Sender=~p~nStats=~p~nOpenRecon=~p~nSessions=~p,~nDestRR: ~p, DestRC: ~p",
                 [Sender, rr_recon_stats:print(Stats), OR - 1, State#rrepair_state.open_sessions, DestRR, DestRC]),
    % TODO: integrate non-initiator stats into the stats of the initiator?
    %       -> may need to be integrated into the 'continue_recon' message of rr_recon
    case rr_recon_stats:get(status, Stats) of
        abort when DestRC =:= undefined ->
            % report to Initiator since he still has a session laying around
            % and no local rr_recon process to terminate it
            % use empty stats with status abort for now since non-initiator
            % stats are not integrated in the successful case either
            SID = rr_recon_stats:get(session_id, Stats),
            StatsToSend = rr_recon_stats:new(SID, [{status, abort}]),
            comm:send(DestRR, {recon_progress_report, Sender, true,
                               comm:this(), undefined, StatsToSend});
        _ -> ok
    end,
    State#rrepair_state{ open_recon = OR - 1 };
on({recon_progress_report, _Sender, _Initiator = true, _DestRR, _DestRC, Stats},
   State = #rrepair_state{open_recon = ORC, open_resolve = _ORS,
                          open_sessions = OS}) ->
    {S, TSessions} = extract_session(rr_recon_stats:get(session_id, Stats), OS),
    SUpd = update_session_recon(S, Stats),
    ?TRACE_RECON("~nRECON-I OK - Sender=~p~nStats=~p~nSession=~.2p~nOpenRecon=~p ; OpenResolve=~p~nOldSessions=~p,~n~p",
                 [_Sender, rr_recon_stats:print(Stats), SUpd, ORC - 1, _ORS, OS, rr_recon_stats:print(Stats)]),
    NewOS = case check_session_complete(SUpd) of
                true -> TSessions;
                _    -> [SUpd | TSessions]
            end,
    State#rrepair_state{open_recon = ORC - 1, open_sessions = NewOS};

on({resolve_progress_report, _Sender, Stats},
   State = #rrepair_state{open_resolve = OR, open_sessions = OS}) ->
    SID = rr_resolve:get_stats_session_id(Stats),
    NSessions = case extract_session(SID, OS) of
                    not_found when SID =:= null ->
                        % all other session IDs should be there!
                        OS;
                    {S, TSessions} ->
                        SUpd = update_session_resolve(S, Stats),
                        case check_session_complete(SUpd) of
                            true -> TSessions;
                            _    -> [SUpd | TSessions]
                        end
                end,
    ?TRACE_RESOLVE("~nRESOLVE OK - Sender=~p ~nStats=~p~nOpenRecon=~p ; OpenResolve=~p~nOldSessions=~p~nNewSessions=~p",
                   [_Sender, rr_resolve:print_resolve_stats(Stats),
                    State#rrepair_state.open_recon, OR - 1, OS, NSessions]),
    State#rrepair_state{open_resolve = OR - 1,
                        open_sessions = NSessions};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% misc info messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({web_debug_info, Requestor}, #rrepair_state{ round = Round,
                                                open_recon = OpenRecon,
                                                open_resolve = OpenResol,
                                                open_sessions = Sessions } = State) ->
    ?TRACE("WEB DEBUG INFO", []),
    KeyValueList =
        [{"Recon Method:",      webhelpers:safe_html_string("~p", [get_recon_method()])},
         {"Sync Round:",        webhelpers:safe_html_string("~p", [Round])},
         {"Open Recon Jobs:",   webhelpers:safe_html_string("~p", [OpenRecon])},
         {"Open Resolve Jobs:", webhelpers:safe_html_string("~p", [OpenResol])},
         {"Open Sessions:",     webhelpers:safe_html_string("~p", [length(Sessions)])}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% - Requests database synchronization with DestPid (DestPid=DhtNodePid or random).
%   Random leads to sync with a node which is associated with this (e.g. symmetric partner)
% - Principal will eventually get an request_sync_complete message
%   (no result message will be send if request receiver dies etc.).
-spec request_sync(State::state(), Method::rr_recon:method(),
                   DestKey::random | ?RT:key(), Principal::principal_id()) -> state().
request_sync(State = #rrepair_state{round = Round, open_recon = OpenRecon,
                                    open_sessions = Sessions},
             Method, DestKey, Principal) ->
    ?TRACE("RR: REQUEST SYNC WITH ~p", [DestKey]),
    This0 = comm:this(),
    S = new_session(Round, This0, Method, Principal),
    This = comm:reply_as(This0, 6, {start_sync, get_range, S#session.id, Method, DestKey, '_'}),
    comm:send_local(pid_groups:get_my(dht_node), {get_state, This, [my_range, load]}),
    State#rrepair_state{ round = next_round(Round),
                         open_recon = OpenRecon + 1,
                         open_sessions = [S | Sessions] }.

%% @doc Selects a random key in the given (continuous) interval and returns one
%%      of its replicas which is not in the interval (if ExcludeInterval is true).
%%      If ExcludeInterval is false, any of its replica keys is returned.
-spec select_sync_node
        (intervals:continuous_interval(), ExcludeInterval::false) -> ?RT:key();
        (intervals:continuous_interval(), ExcludeInterval::true)  -> ?RT:key() | not_found.
select_sync_node(Interval, ExcludeInterval) ->
    ?DBG_ASSERT(intervals:is_continuous(Interval)),
    case intervals:is_all(Interval) of
        true when ExcludeInterval -> not_found; % no sync partner here!
        _ ->
            Bounds = intervals:get_bounds(Interval),
            Key = ?RT:get_random_in_interval(Bounds),
            Keys = if ExcludeInterval ->
                          [K || K <- ?RT:get_replica_keys(Key),
                                not intervals:in(K, Interval)];
                      true -> ?RT:get_replica_keys(Key)
                   end,
            case Keys of
                [] -> not_found;
                [_|_] -> util:randomelem(Keys)
            end
    end.

-spec next_round(round()) -> round().
next_round(R) -> R + 1.

-spec new_session(round(), comm:mypid(), rr_recon:method(), principal_id()) -> session().
new_session(Round, Pid, RCMethod, Principal) ->
    #session{ id = {Round, Pid}, rc_method = RCMethod, ttl = get_session_ttl(), principal = Principal }.

-spec extract_session(session_id() | null, [session()]) -> {session(), Remain::[session()]} | not_found.
extract_session(null, _Sessions) -> not_found;
extract_session(Id, Sessions) ->
    {Satis, NotSatis} = lists:partition(fun(#session{ id = I }) ->
                                                Id =:= I
                                        end,
                                        Sessions),
    case Satis of
        [X] -> {X, NotSatis};
        [] -> not_found;
        _ ->
            log:log(error, "[ ~p ] SESSION NOT UNIQUE! ~p - OpenSessions=~p", [?MODULE, Id, Sessions]),
            not_found
    end.

-spec update_session_recon(session(), rr_recon_stats:stats()) -> session().
update_session_recon(Session = #session{rs_expected = 0}, New) ->
    ?ASSERT(rr_recon_stats:get(status, New) =/= wait),
    NewRS = rr_recon_stats:get(rs_expected, New),
    Session#session{rc_stats  = New, rs_expected = NewRS}.

%% @doc Increases the rs_finish field and merges the new stats.
-spec update_session_resolve(session(), rr_resolve:stats()) -> session().
update_session_resolve(#session{ rs_stats = none, rs_finish = RSCount } = S, Stats) ->
    S#session{rs_stats = Stats, rs_finish = RSCount + 1};
update_session_resolve(#session{ rs_stats = Old, rs_finish = RSCount} = S, New) ->
    Merge = rr_resolve:merge_stats(Old, New),
    S#session{rs_stats = Merge, rs_finish = RSCount + 1}.

%% @doc Checks if the session is complete (rs_called =:= rs_finish, stats
%%      available) and in this case informs the principal and returns 'true'.
%%      Otherwise 'false'.
-spec check_session_complete(session()) -> boolean().
check_session_complete(#session{rc_stats = RCStats, principal = PrincipalPid,
                                rs_expected = C, rs_finish = C} = S)
  when RCStats =/= none ->
    case rr_recon_stats:get(status, RCStats) of
        X when X =:= finish orelse X =:= abort ->
            ?TRACE_COMPLETE("--SESSION COMPLETE--~n~p", [S]),
            case PrincipalPid of
                none -> ok;
                _ -> comm:send(PrincipalPid, {request_sync_complete, S})
            end,
            true;
        wait ->
            false
    end;
check_session_complete(_Session) ->
    false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the replica update process,
%%      registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

%% @doc Initialises the module and starts the trigger
-spec init([]) -> state().
init([]) ->
    case get_trigger_interval() of
        0 -> ok;
        X -> msg_delay:send_trigger(X, {rr_trigger})
    end,
    msg_delay:send_trigger(get_gc_interval(), {rr_gc_trigger}),
    #rrepair_state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API functions for the session record
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec session_get(id, session())          -> session_id();
                 (principal, session())   -> principal_id();
                 (rc_method, session())   -> rr_recon:method();
                 (rc_stats, session())    -> rr_recon_stats:stats() | none;
                 (rs_stats, session())    -> rr_resolve:stats() | none;
                 (rs_expected, session()) -> non_neg_integer();
                 (rs_finish, session())   -> non_neg_integer();
                 (ttl, session())         -> pos_integer().
session_get(id         , #session{id          = X}) -> X;
session_get(principal  , #session{principal   = X}) -> X;
session_get(rc_method  , #session{rc_method   = X}) -> X;
session_get(rc_stats   , #session{rc_stats    = X}) -> X;
session_get(rs_stats   , #session{rs_stats    = X}) -> X;
session_get(rs_expected, #session{rs_expected = X}) -> X;
session_get(rs_finish  , #session{rs_finish   = X}) -> X;
session_get(ttl        , #session{ttl         = X}) -> X.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config handling
%
% USED CONFIG FIELDS
%	* rr_trigger_interval: integer duration until next triggering (milliseconds) (0 = de-activate)
%	* rr_recon_method: set reconciliation algorithm name
%   * rr_trigger_probability: this is the probability of starting a synchronisation
%                             with a random node if trigger has fired. ]0,100]
%   * rr_session_ttl: time to live for sessions until they are garbage collected (milliseconds)
%   * rr_gc_interval: garbage collector execution interval (milliseconds)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(rr_recon_method, [trivial, shash, bloom, merkle_tree, art]) andalso % theoretically also 'iblt', but no full support for that yet
        config:cfg_is_integer(rr_session_ttl) andalso
        config:cfg_is_greater_than(rr_session_ttl, 0) andalso
        config:cfg_is_integer(rr_trigger_probability) andalso
        config:cfg_is_greater_than(rr_trigger_probability, 0) andalso
        config:cfg_is_less_than_equal(rr_trigger_probability, 100) andalso
        config:cfg_is_integer(rr_gc_interval) andalso
        config:cfg_is_greater_than(rr_gc_interval, 0) andalso
        config:cfg_is_integer(rr_trigger_interval) andalso
        config:cfg_is_greater_than_equal(rr_trigger_interval, 0).

-spec get_recon_method() -> rr_recon:method().
get_recon_method() ->  config:read(rr_recon_method).

-spec get_trigger_interval() -> non_neg_integer().
get_trigger_interval() ->
    %% deactivated when 0,so ceil when larger than 0
    util:ceil(config:read(rr_trigger_interval) / 1000).

-spec get_start_prob() -> pos_integer().
get_start_prob() -> config:read(rr_trigger_probability).

-spec get_session_ttl() -> pos_integer().
get_session_ttl() -> config:read(rr_session_ttl).

-spec get_gc_interval() -> pos_integer().
get_gc_interval() -> config:read(rr_gc_interval) div 1000.
