% @copyright 2010-2011 Zuse Institute Berlin

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

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).
%-define(TRACEPONG(X,Y), io:format(X,Y)).
-define(TRACEPONG(_X,_Y), ok).
-behavior(gen_component).
-include("scalaris.hrl").

-export([init/1, on/2, start_link/1, check_config/0]).
-type(state() :: {
       comm:mypid(),   %% remote hbs
       [{comm:mypid(), pos_integer()}], %% subscribed rem. pids + ref counting
       util:time(),    %% local time of last pong arrival
       util:time(),    %% remote is crashed if no pong arrives until this
       atom(),         %% ets table name
       %% locally monitored pids for a remote hbs:
       [{comm:mypid(), reference()}]
     }).

%% Akronyms: HBS =:= (local) heartbeat server instance

%% @doc spawns a fd_hbs instance
-spec start_link(comm:mypid()) -> {ok, pid()}.
start_link(RemotePid) ->
   gen_component:start_link(?MODULE, [RemotePid], [wait_for_init]).

-spec init([pid() | comm:mypid()]) -> state().
init([RemotePid]) ->
    ?TRACE("fd_hbs init: RemotePid ~p~n", [RemotePid]),
    TableName = pdb:new(?MODULE, [set, protected]),
    RemoteFDPid = comm:get(fd, RemotePid),
    comm:send(RemoteFDPid,
              {subscribe_heartbeats, comm:this(), RemotePid}),

    %% no periodic alive check inside same vm (to succeed unittests)
    case comm:is_local(RemotePid) of
        false -> comm:send_local(self(), {periodic_alive_check});
        true -> ok
    end,
    Now = erlang:now(),
    state_new(_RemoteHBS = RemoteFDPid, _RemotePids = [{RemotePid, 1}],
              _LastPong = {0,0,0},
              _CrashedAfter = util:time_plus_ms(Now, 3 * failureDetectorInterval()),
              TableName).

-spec on(comm:message(), state()) -> state() | kill.
on({add_subscriber, Subscriber, WatchedPid, Cookie} = _Msg, State) ->
    ?TRACE("fd_hbs add_subscriber ~.0p~n", [_Msg]),
    %% register subscriber locally
    S1 = state_add_entry(State, {Subscriber, WatchedPid, Cookie}),
    %% add watched pid remotely, if not already watched
    _S2 = state_add_watched_pid(S1, WatchedPid);

on({del_subscriber, Subscriber, WatchedPid, Cookie} = _Msg, State) ->
    ?TRACE("fd_hbs del_subscriber ~.0p~n", [_Msg]),
    %% unregister subscriber locally
    S1 = state_del_entry(State, {Subscriber, WatchedPid, Cookie}),
    %% delete watched pid remotely, if no longer needed
    _S2 = state_del_watched_pid(S1, WatchedPid);

on({add_watching_of, WatchedPid} = _Msg, State) ->
    ?TRACE("fd_hbs add_watching_of ~.0p~n", [_Msg]),
    %% request from remote fd_hbs: watch a pid locally and forward
    %% events on it to the other side
    state_add_monitor(State, WatchedPid);

on({del_watching_of, WatchedPid} = _Msg, State) ->
    ?TRACE("fd_hbs del_watching_of ~.0p~n", [_Msg]),
    %% request from remote fd_hbs: no longer watch a pid locally
    state_del_monitor(State, WatchedPid);

on({update_remote_hbs_to, Pid}, State) ->
    ?TRACE("fd_hbs update_remote_hbs_to ~p~n", [Pid]),
    %% process Pid is remote contact for this fd_hbs. First, we
    %% register the fd process of the remote side. When a fd_hbs is
    %% started remotely, we get its reference via this message.
    state_set_rem_hbs(State, Pid);

on({stop}, _State) ->
    ?TRACE("fd_hbs stop~n", []),
    kill;

on({pong_via_fd, RemHBSSubscriber}, State) ->
    ?TRACEPONG("fd_hbs pong via fd~n", []),
    comm:send(RemHBSSubscriber, {update_remote_hbs_to, comm:this()}),
    NewState = state_set_rem_hbs(State, RemHBSSubscriber),
    on({pong, RemHBSSubscriber}, NewState);

on({pong, _Subscriber}, State) ->
    ?TRACEPONG("Pinger pong for ~p~n", [_Subscriber]),
    Now = erlang:now(),
    LastPong = state_get_last_pong(State),
%    CrashedAfter = state_get_crashed_after(State),
    Delay = abs(timer:now_diff(Now, LastPong)),
    S1 = state_set_last_pong(State, Now),
    NewCrashedAfter = util:time_plus_us(Now, 3 * Delay),
%    state_set_crashed_after(S1, erlang:max(NewCrashedAfter, CrashedAfter));
    state_set_crashed_after(S1, NewCrashedAfter);

on({periodic_alive_check}, State) ->
    ?TRACEPONG("Pinger periodic_alive_check~n", []),
    comm:send(state_get_rem_hbs(State), {pong, comm:this()}),
    Now = erlang:now(),
    CrashedAfter = state_get_crashed_after(State),
    NewState = case 0 < timer:now_diff(Now, CrashedAfter) of
                   true -> report_crash(State);
                   false -> State
    end,
    %% trigger next timeout
    comm:send_local_after(failureDetectorInterval(),
                          self(), {periodic_alive_check}),
    NewState;

on({crashed, WatchedPid}, State) ->
    ?TRACE("fd_hbs crashed ~p~n", [WatchedPid]),
    %% inform all local subscribers
    Subscriptions = state_get_subscriptions(State, WatchedPid),
    _ = [ case Cookie of
              '$fd_nil' ->
                  log:log(debug, "[ FD ~p ] Sending crash to ~.0p/~.0p~n",
                            [comm:this(), X, pid_groups:group_and_name_of(X)]),
                  comm:send_local(X, {crash, WatchedPid});
              _ ->
                  log:log(debug, "[ FD ~p ] Sending crash to ~.0p/~.0p with ~.0p~n",
                            [comm:this(), X, pid_groups:group_and_name_of(X), Cookie]),
                  comm:send_local(X, {crash, WatchedPid, Cookie})
          end
          || {X, Cookie} <- Subscriptions ],
    %% delete from remote_pids
    NewRemPids = lists:delete(WatchedPid, state_get_rem_pids(State)),
    S1 = state_set_rem_pids(State, NewRemPids),
    %% delete subscription entries with this pid
    lists:foldl(fun({Sub, Cook}, StAgg) ->
                        state_del_entry(StAgg, {Sub, WatchedPid, Cook}) end,
                S1, Subscriptions);

on({'DOWN', _Monref, process, WatchedPid, _}, State) ->
    ?TRACE("fd_hbs DOWN reported ~.0p, ~.0p~n", [WatchedPid, pid_groups:group_and_name_of(WatchedPid)]),
    %% send crash report to remote end.
    comm:send(state_get_rem_hbs(State),
              {crashed, comm:make_global(WatchedPid)}),
    %% delete WatchedPid and MonRef locally (MonRef is already
    %% invalid, as Pid crashed)
    _S1 = state_del_monitor(State, WatchedPid).

%% @doc Checks existence and validity of config parameters for this module.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(failure_detector_interval) and
    config:is_greater_than(failure_detector_interval, 0).

%% @doc Reports the crash to local subscribers.
%% @private
-spec report_crash(state()) -> kill.
report_crash(State) ->
    log:log(warn, "[ FD ~p ] reports ~.0p as crashed",
            [comm:this(), state_get_rem_pids(State)]),
    FD = pid_groups:find_a(fd),
    comm:send_local(FD, {hbs_finished, state_get_rem_hbs(State)}),
    erlang:unlink(FD),
    _ = try
            lists:foldl(fun(X, S) -> on({crashed, X}, S) end,
                        State, [RemPid || {RemPid, _} <- state_get_rem_pids(State)])
        catch _:_ -> ignore_exception
        end,
    kill.

%% @doc The interval between two failure detection runs.
-spec failureDetectorInterval() -> pos_integer().
failureDetectorInterval() -> config:read(failure_detector_interval).

-spec state_new(comm:mypid(), [{comm:mypid(), pos_integer()}],
                util:time(), util:time(), atom()) -> state().
state_new(RemoteHBS, RemotePids, LastPong, CrashedAfter,Table) ->
    {RemoteHBS, RemotePids, LastPong, CrashedAfter, Table, []}.

-spec state_get_rem_hbs(state()) -> comm:mypid().
state_get_rem_hbs(State)            -> element(1, State).
-spec state_set_rem_hbs(state(), comm:mypid()) -> state().
state_set_rem_hbs(State, Val)       -> setelement(1, State, Val).
-spec state_get_rem_pids(state()) -> [{comm:mypid(), pos_integer()}].
state_get_rem_pids(State)           -> element(2, State).
-spec state_set_rem_pids(state(), [{comm:mypid(), pos_integer()}]) -> state().
state_set_rem_pids(State, Val)      -> setelement(2, State, Val).
-spec state_get_last_pong(state()) -> util:time().
state_get_last_pong(State)          -> element(3, State).
-spec state_set_last_pong(state(), util:time()) -> state().
state_set_last_pong(State, Val)     -> setelement(3, State, Val).
-spec state_get_crashed_after(state()) -> util:time().
state_get_crashed_after(State)      -> element(4, State).
-spec state_set_crashed_after(state(), util:time()) -> state().
state_set_crashed_after(State, Val) -> setelement(4, State, Val).
-spec state_get_table(state()) -> atom().
state_get_table(State)              -> element(5, State).
-spec state_get_monitors(state()) -> [{comm:mypid(), reference()}].
state_get_monitors(State)           -> element(6, State).
-spec state_set_monitors(state(), [{comm:mypid(), reference()}]) -> state().
state_set_monitors(State, Val)      -> setelement(6, State, Val).

-spec state_add_entry(state(), {comm:mypid(), comm:mypid(), any()}) -> state().
state_add_entry(State, {Subscriber, WatchedPid, Cookie}) ->
    %% implement reference counting on subscriptions:
    %% instead of storing in the state, we silently store in a pdb for
    %% better performance.
    Table = state_get_table(State),
    Entry = pdb:get({Subscriber, WatchedPid}, Table),
    case Entry of
        undefined ->
            pdb:set({{Subscriber, WatchedPid}, [Cookie], 1}, Table);
        Entry ->
            EntryWithCookie =
                setelement(2, Entry, [Cookie | element(2, Entry)]),
            NewEntry =
                setelement(3, EntryWithCookie, 1 + element(3, EntryWithCookie)),
            pdb:set(NewEntry, Table)
    end,
    State.

-spec state_del_entry(state(), {comm:mypid(), comm:mypid(), any()}) -> state().
state_del_entry(State, {Subscriber, WatchedPid, Cookie}) ->
    %% implement reference counting on subscriptions:
    %% instead of storing in the state, we silently store in a pdb for
    %% better performance.
    Table = state_get_table(State),
    Entry = pdb:get({Subscriber, WatchedPid}, Table),
    case Entry of
        undefined ->
            log:log(warn, "got unsubscribe for not registered subscription ~.0p~n", [{unsubscribe, Subscriber, WatchedPid, Cookie}]),
            State;
        Entry ->
            %% delete cookie
            EntryWithoutCookie =
                setelement(2, Entry, lists:delete(Cookie, element(2, Entry))),
            NewEntry =
                setelement(3, EntryWithoutCookie,
                           element(3, EntryWithoutCookie) - 1),
            case element(3, NewEntry) of
                0 -> pdb:delete(Entry, Table);
                _ -> pdb:set(NewEntry, Table)
            end,
            State
    end.

-spec state_get_subscriptions(state(), comm:mypid()) -> [{pid(), any()}].
state_get_subscriptions(State, SearchedPid) ->
    Table = state_get_table(State),
    Entries = pdb:tab2list(Table),
    Res = [ [ {Subscriber, Cookie} || Cookie <- Cookies ]
      || {{Subscriber, WatchedPid}, Cookies, _Num} <- Entries,
          SearchedPid =:= WatchedPid],
    lists:flatten(Res).

-spec state_add_watched_pid(state(), comm:mypid()) -> state().
state_add_watched_pid(State, WatchedPid) ->
    %% add watched pid remotely, if not already watched
    RemPids = state_get_rem_pids(State),
    case lists:keyfind(WatchedPid, 1, RemPids) of
        false ->
            %% add to remote site
            RemHBS = state_get_rem_hbs(State),
            case comm:make_local(RemHBS) of
                fd -> comm:send(RemHBS, {add_watching_of_via_fd, comm:this(), WatchedPid});
                _ -> comm:send(RemHBS, {add_watching_of, WatchedPid})
            end,
            %% add to list
            state_set_rem_pids(State, [{WatchedPid, 1} | RemPids]);
        Entry ->
            NewEntry = setelement(2, Entry, 1 + element(2, Entry)),
            state_set_rem_pids(
              State, lists:keyreplace(WatchedPid, 1, RemPids, NewEntry))
    end.

-spec state_del_watched_pid(state(), comm:mypid()) -> state().
state_del_watched_pid(State, WatchedPid) ->
    %% del watched pid remotely, if not longer necessary
    RemPids = state_get_rem_pids(State),
    case lists:keyfind(WatchedPid, 1, RemPids) of
        false -> log:log(error, "req. to delete non watched pid~n"),
                 State;
        Entry ->
            NewEntry = setelement(2, Entry, element(2, Entry) - 1),
            NewRemPids =
                case element(2, NewEntry) of
                    0 ->
                        RemHBS = state_get_rem_hbs(State),
                        case comm:make_local(RemHBS) of
                            fd -> comm:send(RemHBS, {del_watching_of_via_fd, comm:this(), WatchedPid});
                            _ -> comm:send(RemHBS, {del_watching_of, WatchedPid})
                        end,
                        lists:delete(WatchedPid, RemPids);
                    _ -> lists:keyreplace(WatchedPid, 1, RemPids, NewEntry)
                end,
            state_set_rem_pids(State, NewRemPids)
    end.

-spec state_add_monitor(state(), comm:mypid()) -> state().
state_add_monitor(State, WatchedPid) ->
    MonRef = erlang:monitor(process, comm:make_local(WatchedPid)),
    state_set_monitors(
      State, [{WatchedPid, MonRef} | state_get_monitors(State)]).

-spec state_del_monitor(state(), pid()) -> state().
state_del_monitor(State, WatchedPid) ->
    Monitors = state_get_monitors(State),
    case lists:keyfind(WatchedPid, 1, Monitors) of
        false -> State;
        {WatchedPid, MonRef} ->
            erlang:demonitor(MonRef),
            state_set_monitors(State,
                               lists:delete({WatchedPid, MonRef}, Monitors))
    end.
