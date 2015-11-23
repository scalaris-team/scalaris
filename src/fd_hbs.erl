% @copyright 2010-2015 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc    Heartbeat server (HBS) for fd.erl.
%%         Instatiated per pair of Erlang nodes/VMs.
%%         Sends heartbeats (symmetrically), and is proxy for local /
%%         remote subscriptions on Pid granularity. Uses
%%         erlang:monitor/2 to watch local Pids and forwards crash
%%         notification, when a watched Pid finishes.
%% @end
%% @version $Id$
-module(fd_hbs).
-author('schintke@zib.de').
-vsn('$Id$').

-compile({inline, [state_get_rem_hbs/1, state_set_rem_hbs/2,
                   state_get_rem_pids/1, state_set_rem_pids/2,
                   state_get_last_pong/1, state_set_last_pong/2,
                   state_get_crashed_after/1, state_set_crashed_after/2,
                   state_get_table/1,
                   state_get_monitor_tab/1%, state_set_monitor_tab/2
                  ]}).

-compile({inline, [rempid_get_rempid/1,
                   rempid_refcount/1,
                   rempid_get_last_modified/1, rempid_set_last_modified/2,
                   rempid_get_pending_demonitor/1, rempid_set_pending_demonitor/2
                  ]}).

%% -define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).
%-define(TRACEPONG(X,Y), io:format(X,Y)).
-define(TRACEPONG(_X,_Y), ok).
%%-define(TRACE_NOT_SUBSCRIBED_UNSUBSCRIBE(X,Y), log:log(warn, X, Y)).
-define(TRACE_NOT_SUBSCRIBED_UNSUBSCRIBE(_X,_Y), ok).

-behaviour(gen_component).
-include("scalaris.hrl").

-export([init/1, on/2, start_link/2, check_config/0]).

-include("gen_component.hrl").

-type(rempid() :: %% locally existing subscriptions for remote pids
        {
          comm:mypid(),  %% remote pid a local subscriber is subscribed to
                         %% the other end (fd_hbs) has a monitor
                         %% established for this
          non_neg_integer(), %% number of local subscribers for the remote pid
          erlang_timestamp(),   %% delay remote demonitoring:
                         %%   time of ref count reached 0
                         %%   all other modifications change this to {0,0,0}
          boolean()      %% delayed demonitoring requested and still open?
        }).

-type(state() :: {
       comm:mypid(), %% remote hbs
       [rempid()],  %% subscribed rem. pids + ref counting
       erlang_timestamp(),  %% local time of last pong arrival
       erlang_timestamp(),  %% remote is crashed if no pong arrives until this
       LocalSubscriberTab::pdb:tableid(),
       %% locally erlang:monitored() pids for a remote hbs:
       MonitorTab::pdb:tableid()
     }).

-define(SEND_OPTIONS, [{channel, prio}]).

%% Akronyms: HBS =:= (local) heartbeat server instance

%% @doc spawns a fd_hbs instance
-spec start_link(pid_groups:groupname(), comm:mypid()) -> {ok, pid()}.
start_link(ServiceGroup, RemotePid) ->
    RemoteFDPid = comm:get(fd, RemotePid),
    Name = lists:flatten(io_lib:format("fd <-> ~p", [RemoteFDPid])),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, RemotePid,
                             [%% {wait_for_init}, %% when using protected ets table
                              {pid_groups_join_as, ServiceGroup, Name}]).

-spec init(comm:mypid()) -> state().
init(RemotePid) ->
    ?TRACE("fd_hbs init: RemotePid ~p~n", [RemotePid]),
    LocalSubscriberTab = pdb:new(?MODULE, [set]), %% debugging: ++ [protected]),
    MonitorTab = pdb:new(?MODULE, [set]), %% debugging: ++ [protected]),
    RemoteFDPid = comm:get(fd, RemotePid),
    comm:send(RemoteFDPid,
              {subscribe_heartbeats, comm:this(), RemotePid},
              ?SEND_OPTIONS ++ [{shepherd, shepherd_new(0)}]),

    %% no periodic alive check inside same vm (to succeed unittests)
    case comm:is_local(RemotePid) of
        false -> comm:send_local(self(), {periodic_alive_check});
        true -> ok
    end,
    Now = os:timestamp(),
    state_new(_RemoteHBS = RemoteFDPid,
              _RemotePids = [],
              _LastPong = Now,
              _CrashedAfter = util:time_plus_ms(Now, delayfactor() * failureDetectorInterval()),
              LocalSubscriberTab, MonitorTab).

-spec on(comm:message(), state()) -> state() | kill.
on({add_subscriber, Subscriber, WatchedPid} = _Msg, State) ->
    ?TRACE("fd_hbs add_subscriber ~.0p~n", [_Msg]),
    %% register subscriber locally
    S1 = state_add_entry(State, {Subscriber, WatchedPid}),
    %% add watched pid remotely, if not already watched
    _S2 = state_add_watched_pid(S1, WatchedPid);

on({del_subscriber, Subscriber, WatchedPid} = _Msg, State) ->
    ?TRACE("fd_hbs del_subscriber ~.0p~n", [_Msg]),
    %% unregister subscriber locally
    {Changed, S1} = state_del_entry(State, {Subscriber, WatchedPid}),
    %% delete watched pid remotely, if no longer needed
    case Changed of
        deleted -> _S2 = state_del_watched_pid(S1, WatchedPid, Subscriber);
        unchanged -> S1
    end;

on({del_all_subscriptions, Subscribers} = _Msg, State) ->
    ?TRACE("fd_hbs del_all_subscriptions ~.0p~n", [_Msg]),
    state_del_all_subscriptions(State, Subscribers);

on({check_delayed_del_watching_of, WatchedPid, Time} = _Msg, State) ->
    ?TRACE("fd_hbs check_delayed_del_watching_of ~.0p~n", [_Msg]),
    %% initiate demonitoring and delete local entry if
    %% entry time is still unmodified since this message was triggered
    RemPids = state_get_rem_pids(State),
    case lists:keyfind(WatchedPid, 1, RemPids) of
        false ->
            %% WatchedPid may be crashed and therefore the entry was
            %% already removed in on({crashed, ...}, ...).
            log:log(info, "req. to delete non watched/crashed pid ~p.~n",
                    [WatchedPid]),
            State;
        Entry ->
            NewRemPids =
                case rempid_get_last_modified(Entry) of
                    Time -> %% untouched for whole wait period
                        RemHBS = state_get_rem_hbs(State),
                        case comm:make_local(RemHBS) of
                            fd -> comm:send(RemHBS, {del_watching_of_via_fd, comm:this(), WatchedPid}, ?SEND_OPTIONS);
                            _ -> comm:send(RemHBS, {del_watching_of, WatchedPid}, ?SEND_OPTIONS)
                        end,
                        lists:keydelete(WatchedPid, 1, RemPids);
                    _ ->
                        NewEntry =
                            case rempid_refcount(Entry) of
                                0 -> trigger_delayed_del_watching(Entry);
                                _ -> rempid_set_pending_demonitor(Entry, false)
                            end,
                        lists:keyreplace(WatchedPid, 1, RemPids, NewEntry)
                end,
            state_set_rem_pids(State, NewRemPids)
    end;

on({add_watching_of, WatchedPid} = _Msg, State) ->
    ?TRACE("fd_hbs add_watching_of ~.0p~n", [_Msg]),
    %% request from remote fd_hbs: watch a pid locally and forward
    %% events on it to the other side
    state_add_monitor(State, WatchedPid);

on({del_watching_of, WatchedPid} = _Msg, State) ->
    ?TRACE("fd_hbs del_watching_of ~.0p~n", [_Msg]),
    %% request from remote fd_hbs: no longer watch a pid locally
    element(2, state_del_monitor(State, WatchedPid));

on({update_remote_hbs_to, Pid}, State) ->
    ?TRACE("fd_hbs update_remote_hbs_to ~p~n", [Pid]),
    %% process Pid is remote contact for this fd_hbs. First, we
    %% register the fd process of the remote side. When a fd_hbs is
    %% started remotely, we get its reference via this message.
    state_set_rem_hbs(State, Pid);

on({stop}, _State) ->
    ?TRACE("fd_hbs stop~n", []),
    kill;

on({pong_via_fd, RemHBSSubscriber, RemoteDelay}, State) ->
    ?TRACEPONG("fd_hbs pong via fd~n", []),
    comm:send(RemHBSSubscriber, {update_remote_hbs_to, comm:this()}, ?SEND_OPTIONS),
    NewState = state_set_rem_hbs(State, RemHBSSubscriber),
    on({pong, RemHBSSubscriber, RemoteDelay}, NewState);

on({pong, _Subscriber, RemoteDelay}, State) ->
    ?TRACEPONG("Pinger pong for ~p~n", [_Subscriber]),
    Now = os:timestamp(),
    LastPong = state_get_last_pong(State),
    CrashedAfter = state_get_crashed_after(State),
    PongDelay = abs(timer:now_diff(Now, LastPong)),
    Delay = erlang:max(PongDelay, failureDetectorInterval()),
    S1 = state_set_last_pong(State, Now),
    NewCrashedAfter = lists:max(
                        [util:time_plus_us(Now, delayfactor() * Delay),
                         util:time_plus_ms(CrashedAfter, 1000),
                         util:time_plus_us(Now, RemoteDelay)]),
    %% io:format("Time for next pong: ~p s~n",
    %%           [timer:now_diff(NewCrashedAfter, Now)/1000000]),
    state_set_crashed_after(S1, NewCrashedAfter);

on({periodic_alive_check}, State) ->
    ?TRACEPONG("Pinger periodic_alive_check~n", []),
    Now = os:timestamp(),
    CrashedAfter = state_get_crashed_after(State),
    comm:send(
      state_get_rem_hbs(State),
      {pong, comm:this(),
       timer:now_diff(
         CrashedAfter,
         util:time_plus_ms(state_get_last_pong(State),
                           failureDetectorInterval()
                           %% the following is the reduction rate
                           %% when increased earlier
                           + failureDetectorInterval() div 3))},
      ?SEND_OPTIONS ++ [{shepherd, shepherd_new(0)}]),
    NewState = case 0 < timer:now_diff(Now, CrashedAfter) of
                   true -> report_connection_crash(State);
                   false -> State
    end,
    %% trigger next timeout
    _ = comm:send_local_after(failureDetectorInterval(),
                              self(), {periodic_alive_check}),
    NewState;

on({send_retry, {send_error, Target, Message, _Reason} = Err, Count}, State) ->
    NextOp =
        case Count of
            1 -> {retry};
            2 -> {delay, _Wait = 1};
            3 -> {retry};
            4 -> {delay, _Wait = 2};
            5 -> {retry};
            6 -> {giveup}
        end,
    case NextOp of
        {retry} ->
            comm:send(Target, Message,
                      ?SEND_OPTIONS ++ [{shepherd, shepherd_new(Count)}]),
            State;
        {delay, Sec} ->
            msg_delay:send_local(
              Sec, self(), {send_retry, Err, Count + 1}),
            State;
        {giveup} ->
            log:log(warn, "[ FD ] Sending ~.0p failed 3 times. "
                    "Report target ~.0p as crashed.~n", [Message, Target]),
            %% report whole node as crashed, when not reachable via tcp:
            report_connection_crash(State)
    end;

on({crashed, WatchedPid, Reason}, State) ->
    ?TRACE("fd_hbs crashed ~p~n", [WatchedPid]),
    report_crashed_remote_pid(State, WatchedPid, Reason, warn);

on({fd_notify, Event, WatchedPids, Data}, State) ->
    ?TRACE("fd_hbs: fd_notify ~p for pids ~.2p with data ~.2p~n",
           [Event, WatchedPids, Data]),
    lists:foldl(fun(WatchedPid, StateX) ->
                        fd_notify(StateX, Event, WatchedPid, Data)
                end, State, WatchedPids);

on({report, crash, LocalPids, Reason}, State) ->
    ?TRACE("fd_hbs: report crash for pids ~.2p with reason ~.2p~n",
           [LocalPids, Reason]),
    % similar to 'DOWN' report below
    {WatchedPids, State1} =
        lists:foldl(
          fun(LocalPid, {WatchedPidsX, StateX}) ->
                  GlobalPid = comm:make_global(LocalPid),
                  %% delete WatchedPid and MonRef locally (if existing)
                  case state_del_monitor(StateX, GlobalPid) of
                      {true, StateX1} -> {[GlobalPid | WatchedPidsX], StateX1};
                      {false, StateX1} -> {WatchedPidsX, StateX1}
                  end
          end, {[], State}, LocalPids),
    %% send crash report to remote end.
    comm:send(state_get_rem_hbs(State),
              {fd_notify, crash, WatchedPids, Reason},
              ?SEND_OPTIONS),
    State1;

on({report, Event, LocalPids, Data}, State) ->
    ?TRACE("fd_hbs: report ~p for pids ~.2p with data ~.2p~n",
           [Event, LocalPids, Data]),
    % similar to 'DOWN' report below
    WatchedPids =
        [GlobalPid || LocalPid <- LocalPids,
                      state_has_monitor(State,
                                        GlobalPid = comm:make_global(LocalPid))],
    %% send event to remote end.
    comm:send(state_get_rem_hbs(State),
              {fd_notify, Event, WatchedPids, Data},
              ?SEND_OPTIONS),
    State;

on({'DOWN', _MonitorRef, process, WatchedPid, _}, State) ->
    ?TRACE("fd_hbs DOWN reported ~.0p, ~.0p~n",
           [WatchedPid, pid_groups:group_and_name_of(WatchedPid)]),
    %% send crash report to remote end.
    GlobalPid = comm:make_global(WatchedPid),
    comm:send(state_get_rem_hbs(State),
              {crashed, GlobalPid, 'DOWN'}, ?SEND_OPTIONS),
    %% delete WatchedPid and MonRef locally (MonRef is already
    %% invalid, as Pid crashed)
    _S1 = element(2, state_del_monitor(State, GlobalPid)).

%% @doc Checks existence and validity of config parameters for this module.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(failure_detector_interval) and
    config:cfg_is_greater_than(failure_detector_interval, 0).

%% @doc Reports a crashed connection to local subscribers.
%% @private
-spec report_crashed_remote_pid(state(), WatchedPid::comm:mypid(),
                                Reason::fd:reason(), warn | nowarn) -> state().
report_crashed_remote_pid(State, WatchedPid, Reason, Warn) ->
    %% inform all local subscribers
    Subscriptions = state_get_subscriptions(State, WatchedPid),
    %% only there because of delayed demonitoring?
    ?TRACE("~p found subs: ~p~n", [self(), Subscriptions]),
    RemPids = state_get_rem_pids(State),
    case Subscriptions of
        [] ->
            %% Report if subcription entry exists not because of a
            %% delayed demonitoring
            Report = case lists:keyfind(WatchedPid, 1, RemPids) of
                         false -> true;
                         RemPid ->
                             not rempid_get_pending_demonitor(RemPid)
                     end,
            case Report andalso (nowarn =/= Warn) of
                true ->
                    log:log(info, "No one to inform on crash of ~.0p~n",
                            [WatchedPid]);
                false -> ok
            end;
        _ -> ok
    end,
    This = comm:this(),
    _ = [ begin
              log:log(debug, "[ FD ~p ] Sending crash to ~.0p/~.0p~n",
                      [This, X, pid_groups:group_and_name_of(comm:get_plain_pid(X))]),
              comm:send_local(X, {fd_notify, crash, WatchedPid, Reason}, [{?quiet}])
          end
          || X <- Subscriptions ],
    %% delete from remote_pids
    NewRemPids = lists:keydelete(WatchedPid, 1, RemPids),
    S1 = state_set_rem_pids(State, NewRemPids),
    %% delete subscription entries with this pid
    lists:foldl(fun(Sub, StAgg) ->
                        {_, Res} = state_del_entry(StAgg, {Sub, WatchedPid}),
                        Res
                end,
                S1, Subscriptions).

%% @doc Reports a crashed connection to local subscribers.
%% @private
-spec fd_notify(state(), Event::fd:event(), Pid::comm:mypid(),
                Data::fd:reason()) -> state().
fd_notify(State, crash, WatchedPid, Reason) ->
    report_crashed_remote_pid(State, WatchedPid, Reason, nowarn);
fd_notify(State, Event, WatchedPid, Data) ->
    %% inform all local subscribers
    Subscriptions = state_get_subscriptions(State, WatchedPid),
    ?TRACE("~p found subs: ~p~n", [self(), Subscriptions]),
    % note: in contrast to report_crashed_remote_pid, do not warn if no subscribers exist
    This = comm:this(),
    _ = [ begin
              log:log(debug, "[ FD ~p ] Sending crash to ~.0p/~.0p~n",
                      [This, X, pid_groups:group_and_name_of(comm:get_plain_pid(X))]),
              comm:send_local(X, {fd_notify, Event, WatchedPid, Data})
          end
          || X <- Subscriptions ],
    State.

%% @doc Reports a crashed connection to local subscribers.
%% @private
-spec report_connection_crash(state()) -> kill.
report_connection_crash(State) ->
    RemPids = state_get_rem_pids(State),
    case RemPids of
        [] ->
            log:log(info, "[ FD ~p ] remote host ~p (fd_hbs) is unresponsive.~n"
                    ++    "          No pids to report as crashed locally.~n"
                    ++    "          Finishing own fd_hbs.",
                    [comm:this(), state_get_rem_hbs(State)]);
        _ -> log:log(warn, "[ FD ~p ] reports ~.0p as crashed and finishes.",
                     [comm:this(), RemPids])
    end,
    FD = pid_groups:find_a(fd),
    comm:send_local(FD, {hbs_finished, state_get_rem_hbs(State)}),
    erlang:unlink(FD),
    _ = try
            lists:foldl(fun(X, S) -> on({crashed, X, noconnection}, S) end,
                        State, [ rempid_get_rempid(RemPidEntry)
                                 || RemPidEntry <- state_get_rem_pids(State)])
        catch _:_ -> ignore_exception
        end,
    kill.

%% @doc The interval between two failure detection runs.
-spec failureDetectorInterval() -> pos_integer().
failureDetectorInterval() -> config:read(failure_detector_interval).

-spec delayfactor() -> pos_integer().
delayfactor() -> 4.

-spec trigger_delayed_del_watching(rempid()) -> rempid().
trigger_delayed_del_watching(RemPidEntry) ->
    %% delayed demonitoring: remember current time self-inform on
    %% pending demonitoring with current time.
    %% actually delete if timestamp of the entry is still the same
    %% after delay
    WatchedPid = rempid_get_rempid(RemPidEntry),
    Time = os:timestamp(),
    msg_delay:send_local(1, self(),
                         {check_delayed_del_watching_of, WatchedPid, Time}),
    E1 = rempid_set_last_modified(RemPidEntry, Time),
    rempid_set_pending_demonitor(E1, true).

-spec state_new(RemoteHBS::comm:mypid(), RemotePids::[rempid()],
                LastPong::erlang_timestamp(), CrashedAfter::erlang_timestamp(),
                LocalSubscriberTab::pdb:tableid(),
                MonitorTab::pdb:tableid()) -> state().
state_new(RemoteHBS, RemotePids, LastPong, CrashedAfter, LocalSubscriberTab, MonitorTab) ->
    {RemoteHBS, RemotePids, LastPong, CrashedAfter, LocalSubscriberTab, MonitorTab}.

-spec state_get_rem_hbs(state())    -> comm:mypid().
state_get_rem_hbs(State)            -> element(1, State).
-spec state_set_rem_hbs(state(), comm:mypid()) -> state().
state_set_rem_hbs(State, Val)       -> setelement(1, State, Val).
-spec state_get_rem_pids(state())   -> [rempid()].
state_get_rem_pids(State)           -> element(2, State).
-spec state_set_rem_pids(state(), [rempid()]) -> state().
state_set_rem_pids(State, Val)      -> setelement(2, State, Val).
-spec state_get_last_pong(state())  -> erlang_timestamp().
state_get_last_pong(State)          -> element(3, State).
-spec state_set_last_pong(state(), erlang_timestamp()) -> state().
state_set_last_pong(State, Val)     -> setelement(3, State, Val).
-spec state_get_crashed_after(state()) -> erlang_timestamp().
state_get_crashed_after(State)      -> element(4, State).
-spec state_set_crashed_after(state(), erlang_timestamp()) -> state().
state_set_crashed_after(State, Val) -> setelement(4, State, Val).
-spec state_get_table(state()) -> pdb:tableid().
state_get_table(State)              -> element(5, State).
-spec state_get_monitor_tab(state())-> pdb:tableid().
state_get_monitor_tab(State)        -> element(6, State).
%% -spec state_set_monitor_tab(state(), pdb:tableid()) -> state().
%% state_set_monitor_tab(State, Val)   -> setelement(6, State, Val).

-spec state_add_entry(state(), {comm:erl_local_pid(), comm:mypid()}) -> state().
state_add_entry(State, {_Subscriber, _WatchedPid} = X) ->
    %% implement reference counting on subscriptions:
    %% instead of storing in the state, we silently store in a pdb for
    %% better performance.
    Table = state_get_table(State),
    case pdb:get(X, Table) of
        undefined ->
            pdb:set({X, 1}, Table);
        {X, Count} ->
            pdb:set({X, Count + 1}, Table)
    end,
    State.

-spec state_del_entry(state(), {comm:erl_local_pid(), comm:mypid()})
        -> {deleted | unchanged, state()}.
state_del_entry(State, {_Subscriber, _WatchedPid} = X) ->
    %% implement reference counting on subscriptions:
    %% instead of storing in the state, we silently store in a pdb for
    %% better performance.
    Table = state_get_table(State),
    case pdb:get(X, Table) of
        undefined ->
            ?TRACE_NOT_SUBSCRIBED_UNSUBSCRIBE(
               "got unsubscribe for not registered subscription ~.0p, "
               "Subscriber ~.0p, Watching group and name: ~.0p.~n",
               [{unsubscribe, _Subscriber, _WatchedPid},
                pid_groups:group_and_name_of(comm:get_plain_pid(_Subscriber)),
                pid_groups:group_and_name_of(comm:get_plain_pid(comm:make_local(_WatchedPid)))]),
            {unchanged, State};
        {X, 1} ->
            pdb:delete(X, Table),
            {deleted, State};
        {X, Count} when Count > 1 ->
            pdb:set({X, Count - 1}, Table),
            {unchanged, State}
    end.

-spec state_get_subscriptions(state(), comm:mypid()) -> [comm:erl_local_pid()].
state_get_subscriptions(State, SearchedPid) ->
    Table = state_get_table(State),
    Entries = pdb:tab2list(Table),
    _Res = [ Subscriber
             || {{Subscriber, WatchedPid}, _Num} <- Entries,
                %% pdb:tab2list may contain unrelated entries, but <- lets
                %% only pass structurally matching entries here without an
                %% assignment exception.
                Subscriber =/= '$monitor_count',
                Subscriber =/= '$monitor',
                SearchedPid =:= WatchedPid].

-spec state_del_all_subscriptions(state(), [comm:erl_local_pid()]) -> state().
state_del_all_subscriptions(State, SubscriberPids) ->
    Table = state_get_table(State),
    Set = gb_sets:from_list(SubscriberPids),
    Entries = pdb:tab2list(Table),
    lists:foldl(
      fun({Key = {Subscriber, WatchedPid}, _Num}, StateX) ->
              case gb_sets:is_member(Subscriber, Set) of
                  true ->
                      %% unregister subscriber locally
                      pdb:delete(Key, Table),
                      %% delete watched pid remotely, if no longer needed
                      state_del_watched_pid(StateX, WatchedPid, Subscriber);
                  false ->
                      StateX
              end;
         (_, StateX) ->
              StateX
      end, State, Entries).

-spec state_add_watched_pid(state(), comm:mypid()) -> state().
state_add_watched_pid(State, WatchedPid) ->
    %% add watched pid remotely, if not already watched
    RemPids = state_get_rem_pids(State),
    case lists:keytake(WatchedPid, 1, RemPids) of
        {value, Entry, RestRemPids} ->
            state_set_rem_pids(State, [rempid_inc(Entry) | RestRemPids]);
        false ->
            %% add to remote site
            RemHBS = state_get_rem_hbs(State),
            case comm:make_local(RemHBS) of
                fd -> comm:send(
                        RemHBS, {add_watching_of_via_fd, comm:this(), WatchedPid},
                        ?SEND_OPTIONS ++ [{shepherd, shepherd_new(0)}]);
                _  -> comm:send(
                        RemHBS, {add_watching_of, WatchedPid},
                        ?SEND_OPTIONS ++ [{shepherd, shepherd_new(0)}])
            end,
            %% add to list
            state_set_rem_pids(
              State, [rempid_inc(rempid_new(WatchedPid)) | RemPids])
    end.

-spec state_del_watched_pid(state(), comm:mypid(), comm:erl_local_pid()) -> state().
state_del_watched_pid(State, WatchedPid, Subscriber) ->
    %% del watched pid remotely, if not longer necessary
    RemPids = state_get_rem_pids(State),
    case lists:keytake(WatchedPid, 1, RemPids) of
        {value, Entry, RestRemPids} ->
            TmpEntry = rempid_dec(Entry),
            NewEntry =
                case {rempid_refcount(TmpEntry),
                      rempid_get_pending_demonitor(TmpEntry)} of
                    {0, false} -> %% dec to 0 and triggger new delayed message
                        trigger_delayed_del_watching(TmpEntry);
                    {0, true} -> %% dec to 0 and no new delayed message needed
                        TmpEntry;
                    _ -> TmpEntry
                end,
            state_set_rem_pids(State, [NewEntry | RestRemPids]);
        false -> log:log(warn,
                         "req. from ~p (~p) to delete non watched pid ~p.~n",
                        [Subscriber,
                         pid_groups:group_and_name_of(
                           comm:get_plain_pid(Subscriber)),
                         WatchedPid]),
                 State
    end.

%% @doc Helper to extract the real pid from a watched (local!) pid.
-spec get_real_pid(comm:mypid()) -> pid().
get_real_pid(WatchedPid) ->
    PlainPid = comm:make_local(comm:get_plain_pid(WatchedPid)),
    if is_atom(PlainPid) ->
           case whereis(PlainPid) of
               undefined ->
                   % this process is not alive anymore -> store the
                   % registered name instead so the check in
                   % gen_component:demonitor/2 does not hit
                   PlainPid;
               X when is_pid(X) ->
                   X
           end;
       is_pid(PlainPid) ->
           PlainPid
    end.

-spec state_add_monitor(state(), comm:mypid()) -> state().
state_add_monitor(State, WatchedPid) ->
    Table = state_get_monitor_tab(State),
    PlainPid = get_real_pid(WatchedPid),
    CountKey = {'$monitor_count', PlainPid},
    X = case pdb:get(CountKey, Table) of
            undefined ->
                MonRef = gen_component:monitor(PlainPid),
                pdb:set({{'$monitor', PlainPid}, MonRef}, Table),
                1;
            {CountKey, I} when is_integer(I) ->
                I + 1
        end,
    pdb:set({CountKey, X}, Table),
    State.

-spec state_del_monitor(state(), comm:mypid()) -> {Found::boolean(), state()}.
state_del_monitor(State, WatchedPid) ->
    Table = state_get_monitor_tab(State),
    PlainPid = get_real_pid(WatchedPid),
    CountKey = {'$monitor_count', PlainPid},
    Found = case pdb:take(CountKey, Table) of
                undefined ->
                    false;
                {CountKey, 1} ->
                    MonKey = {'$monitor', PlainPid},
                    {MonKey, MonRef} = pdb:take(MonKey, Table),
                    gen_component:demonitor(MonRef),
                    true;
                {CountKey, X} when is_integer(X) andalso X > 1 ->
                    pdb:set({CountKey, X - 1}, Table),
                    true
            end,
    {Found, State}.

-spec state_has_monitor(state(), comm:mypid()) -> boolean().
state_has_monitor(State, WatchedPid) ->
    Table = state_get_monitor_tab(State),
    PlainPid = get_real_pid(WatchedPid),
    MonKey = {'$monitor', PlainPid},
    pdb:get(MonKey, Table) =/= undefined.

-spec rempid_new(comm:mypid()) -> rempid().
rempid_new(Pid) ->
    {Pid, _RefCount = 0, _DecTo0 = {0,0,0}, _PendingDemonitor = false}.
-spec rempid_get_rempid(rempid()) -> comm:mypid().
rempid_get_rempid(Entry) -> element(1, Entry).
-spec rempid_refcount(rempid()) -> non_neg_integer().
rempid_refcount(Entry) -> element(2, Entry).
-spec rempid_inc(rempid()) -> rempid().
rempid_inc(Entry) ->
    TmpEntry = setelement(2, Entry, element(2, Entry) + 1),
    rempid_set_last_modified(TmpEntry, {0,0,0}).
-spec rempid_dec(rempid()) -> rempid().
rempid_dec(Entry) ->
    TmpEntry = setelement(2, Entry, element(2, Entry) - 1),
    rempid_set_last_modified(TmpEntry, {0,0,0}).
-spec rempid_get_last_modified(rempid()) -> erlang_timestamp().
rempid_get_last_modified(Entry) -> element(3, Entry).
-spec rempid_set_last_modified(rempid(), erlang_timestamp()) -> rempid().
rempid_set_last_modified(Entry, Time) -> setelement(3, Entry, Time).
-spec rempid_get_pending_demonitor(rempid()) -> boolean().
rempid_get_pending_demonitor(Entry) -> element(4, Entry).
-spec rempid_set_pending_demonitor(rempid(), boolean()) -> rempid().
rempid_set_pending_demonitor(Entry, Val) -> setelement(4, Entry, Val).

-spec shepherd_new(non_neg_integer()) -> comm:erl_local_pid_with_reply_as().
shepherd_new(Count) ->
    comm:reply_as(self(), 2, {send_retry, '_', Count + 1}).
