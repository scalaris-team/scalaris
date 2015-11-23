% @copyright 2007-2015 Zuse Institute Berlin

%  Licensed under the Apache License, Version 2.0 (the "License");
%  you may not use this file except in compliance with the License.
%  You may obtain a copy of the License at
%
%      http://www.apache.org/licenses/LICENSE-2.0
%
%  Unless required by applicable law or agreed to in writing, software
%  distributed under the License is distributed on an "AS IS" BASIS,
%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%  See the License for the specific language governing permissions and
%  limitations under the License.

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Failure detector based on Guerraoui.
%% @end
%% @version $Id$
-module(fd).
-author('schuett@zib.de').
-vsn('$Id$').

%% -define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).
-behaviour(gen_component).
-include("scalaris.hrl").

-export_type([reason/0, event/0]).

-export([subscribe/2, subscribe_refcount/2]).
-export([unsubscribe/2, unsubscribe_refcount/2]).
-export([update_subscriptions/3]).
-export([report/3]).
%% gen_server & gen_component callbacks
-export([start_link/1, init/1, on/2]).

%% debug purposes
-export([subscriptions/0]).

-include("gen_component.hrl").

-type reason() :: 'DOWN' | noconnection | term().
-type event() :: crash | jump | leave.
-type state() :: [HBPid::pid()]. % a list of all hbs processes launched by this fd

-define(SEND_OPTIONS, [{channel, prio}]).

%% @doc Generates a failure detector for the calling process on the given pid.
-spec subscribe(Subscriber::comm:erl_local_pid(), WatchedPids::[comm:mypid()]) -> ok.
subscribe(Subscriber, [_|_] = GlobalPids) ->
    FD = my_fd_pid(),
    _ = [subscribe_single(FD, Subscriber, Pid) || Pid <- GlobalPids],
    ok;
subscribe(_Subscriber, []) ->
    ok.

-spec subscribe_single(FD::pid(), Subscriber::comm:erl_local_pid(),
                       WatchedPid::comm:mypid()) -> ok.
subscribe_single(FD, Subscriber, GlobalPid) ->
    comm:send_local(FD, {add_subscriber_via_fd, Subscriber, GlobalPid}).

%% @doc Generates a failure detector for the calling process on the
%%      given pid - uses reference counting to be subscribed to a pid only once.
%%      Unsubscribe with unsubscribe_refcount/3!
-spec subscribe_refcount(Subscriber::comm:erl_local_pid(), WatchedPids::[comm:mypid()]) -> ok.
subscribe_refcount(Subscriber, [_|_] = GlobalPids) ->
    FD = my_fd_pid(),
    _ = [subscribe_single_refcount(FD, Subscriber, Pid) || Pid <- GlobalPids],
    ok;
subscribe_refcount(_Subscriber, []) ->
    ok.

-spec subscribe_single_refcount(FD::pid(), Subscriber::comm:erl_local_pid(),
                                WatchedPid::comm:mypid()) -> ok.
subscribe_single_refcount(FD, Subscriber, GlobalPid) ->
    Key = {'$fd_subscribe', GlobalPid},
    OldCount = case erlang:get(Key) of
                   undefined -> subscribe_single(FD, Subscriber, GlobalPid),
                                0;
                   X -> X
               end,
    erlang:put(Key, OldCount + 1),
    ok.

%% @doc Deletes the failure detector for the given pid.
-spec unsubscribe(Subscriber::comm:erl_local_pid(), WatchedPids::[comm:mypid()]) -> ok.
unsubscribe(Subscriber, [_|_] = GlobalPids) ->
    FD = my_fd_pid(),
    _ = [unsubscribe_single(FD, Subscriber, Pid) || Pid <- GlobalPids],
    ok;
unsubscribe(_Subscriber, []) ->
    ok.

-spec unsubscribe_single(FD::pid(), Subscriber::comm:erl_local_pid(),
                         WatchedPid::comm:mypid()) -> ok.
unsubscribe_single(FD, Subscriber, GlobalPid) ->
    comm:send_local(FD, {del_subscriber_via_fd, Subscriber, GlobalPid}).

%% @doc Deletes the failure detector for the given pid - uses
%%      reference counting to be subscribed to a pid only once.
%%      Subscribe with subscribe_refcount/3!
-spec unsubscribe_refcount(Subscriber::comm:erl_local_pid(),
                           WatchedPids::[comm:mypid()]) -> ok.
unsubscribe_refcount(Subscriber, [_|_] = GlobalPids) ->
    FD = my_fd_pid(),
    _ = [unsubscribe_single_refcount(FD, Subscriber, Pid) || Pid <- GlobalPids],
    ok;
unsubscribe_refcount(_Subscriber, []) ->
    ok.

-spec unsubscribe_single_refcount(FD::pid(), Subscriber::comm:erl_local_pid(),
                                  WatchedPid::comm:mypid()) -> ok.
unsubscribe_single_refcount(FD, Subscriber, GlobalPid) ->
    Key = {'$fd_subscribe', GlobalPid},
    _ = case erlang:get(Key) of
            undefined -> ok; % TODO: warn here?
            1 -> %% delay the actual unsubscribe for better perf.?
                unsubscribe_single(FD, Subscriber, GlobalPid),
                erlang:erase(Key);
            OldCount ->
                erlang:put(Key, OldCount - 1)
        end,
    ok.

%% @doc Unsubscribes Subscriber from the pids in OldPids but not in NewPids
%%      and subscribes to the pids in NewPids but not in OldPids
%%      (Subscribers can be pid() or an envelop as created by comm:reply_as/3).
-spec update_subscriptions(Subscriber::comm:erl_local_pid(),
                           OldWatchedPids::[comm:mypid()],
                           NewWatchedPids::[comm:mypid()]) -> ok.
update_subscriptions(Subscriber, OldPids, NewPids) ->
    {OnlyOldPids, _Same, OnlyNewPids} = util:split_unique(OldPids, NewPids),
    unsubscribe(Subscriber, OnlyOldPids),
    subscribe(Subscriber, OnlyNewPids).

%% @doc Reports the calling process' group as being shut down due to a graceful
%%      leave operation.
-spec report(Event::event(), LocalPids::[pid()], Data::term()) -> ok.
report(Event, LocalPids, Data) ->
    comm:send_local(my_fd_pid(), {report, Event, LocalPids, Data}).

%% gen_component functions
%% @doc Starts the failure detector server
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
      [%% {wait_for_init}, %% when using protected table (for debugging)
       {erlang_register, ?MODULE},
       {pid_groups_join_as, ServiceGroup, ?MODULE}]).

%% @doc Initialises the module with an empty state.
-spec init([]) -> state().
init([]) ->
    % local heartbeat processes
    _ = pdb:new(fd_hbs, [set]), %% for debugging ++ [protected, named_table]),
    [].

%% @private
-spec on(comm:message(), state()) -> state().
on({hbs_finished, RemoteWatchedPid}, State) ->
    RemFD = comm:get(fd, RemoteWatchedPid),
    case pdb:take(RemFD, fd_hbs) of
        undefined -> State;
        {RemFD, HBPid} -> lists:delete(HBPid, State)
    end;

on({subscribe_heartbeats, Subscriber, TargetPid}, State) ->
    %% we establish the back-direction here, so we subscribe to the
    %% subscriber and add the TargetPid to the local monitoring.
    ?TRACE("FD: subscribe_heartbeats~n", []),
    {HBPid, NewState} = forward_to_hbs(State, Subscriber, {add_watching_of, TargetPid}),
    comm:send(Subscriber, {update_remote_hbs_to, comm:make_global(HBPid)}, ?SEND_OPTIONS),
    NewState;

on({pong, RemHBSSubscriber, RemoteDelay}, State) ->
    ?TRACE("FD: pong, ~p~n", [RemHBSSubscriber]),
    element(2, forward_to_hbs(State, RemHBSSubscriber,
                              {pong_via_fd, RemHBSSubscriber, RemoteDelay}));

on({add_subscriber_via_fd, Subscriber, WatchedPid}, State) ->
    ?TRACE("FD: subscribe ~p to ~p~n", [Subscriber, WatchedPid]),
    element(2, forward_to_hbs(State, WatchedPid,
                              {add_subscriber, Subscriber, WatchedPid}));

on({del_subscriber_via_fd, Subscriber, WatchedPid}, State) ->
    ?TRACE("FD: unsubscribe ~p to ~p~n", [Subscriber, WatchedPid]),
    element(2, forward_to_hbs(State, WatchedPid,
                              {del_subscriber, Subscriber, WatchedPid}));

on({add_watching_of_via_fd, Subscriber, Pid}, State) ->
    ?TRACE("FD: add_watching_of ~p~n", [Pid]),
    element(2, forward_to_hbs(State, Subscriber, {add_watching_of, Pid}));

on({del_watching_of_via_fd, Subscriber, Pid}, State) ->
    ?TRACE("FD: del_watching_of ~p~n", [Pid]),
    element(2, forward_to_hbs(State, Subscriber, {del_watching_of, Pid}));

on({crashed, WatchedPid, _Warn} = Msg, State) ->
    ?TRACE("FD: crashed message via fd for watched pid ~p~n", [WatchedPid]),
    element(2, forward_to_hbs(State, WatchedPid, Msg, false));

%% on({web_debug_info, _Requestor}, State) ->
%%     ?TRACE("FD: web_debug_info~n", []),
%% TODO: reimplement for new fd.
%%     Subscriptions = fd_db:get_subscriptions(),
%%     % resolve (local and remote) pids to names:
%%     S2 = [begin
%%               case comm:is_local(TargetPid) of
%%                   true -> {Subscriber,
%%                            pid_groups:pid_to_name(comm:make_local(TargetPid))};
%%                   _ ->
%%                       comm:send(comm:get(pid_groups, TargetPid),
%%                                 {group_and_name_of, TargetPid, comm:this()}, ?SEND_OPTIONS),
%%                       receive
%%                           {group_and_name_of_response, Name} ->
%%                               {Subscriber, pid_groups:pid_to_name2(Name)}
%%                       after 2000 -> X
%%                       end
%%               end
%%           end || X = {Subscriber, TargetPid} <- Subscriptions],
%%     KeyValueList =
%%         [{"subscriptions", length(Subscriptions)},
%%          {"subscriptions (subscriber, target):", ""} |
%%          [{pid_groups:pid_to_name(Pid),
%%            webhelpers:safe_html_string("~p", [X]))} || {Pid, X} <- S2]],
%%     comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
%%     State;

on({report, _Event, _LocalPids, _Data} = Msg, State) ->
    ?TRACE("FD: report ~p for pids ~.2p with data ~.2p~n",
           [_Event, _LocalPids, _Data]),
    % don't create new hbs processes!
    _ = [comm:send_local(HBS, Msg) || HBS <- State],
    State;
on({del_all_subscriptions, _Subscribers} = Msg, State) ->
    _ = [comm:send_local(HBS, Msg) || HBS <- State],
    State.

%%% Internal functions
-spec my_fd_pid() -> pid() | failed.
my_fd_pid() ->
    case whereis(?MODULE) of
        undefined ->
            log:log(error, "[ FD ] call of my_fd_pid undefined"),
            failed;
        PID -> PID
    end.

%% @doc Forwards the given message to the registered HBS or creates a new HBS.
-spec forward_to_hbs(state(), comm:mypid(), comm:message()) -> {HBPid::pid(), state()}.
forward_to_hbs(State, Pid, Msg) ->
    forward_to_hbs(State, Pid, Msg, true).

%% @doc Forwards the given message to the registered hbs or either creates a
%%      new hbs inside the fd process context (if Create is true) or
%%      ignores the message (Create is false).
-spec forward_to_hbs(state(), comm:mypid(), comm:message(), Create::true) -> {HBPid::pid(), state()};
                    (state(), comm:mypid(), comm:message(), Create::false) -> {HBPid::pid() | undefined, state()}.
forward_to_hbs(State, Pid, Msg, Create) ->
    FDPid = comm:get(fd, Pid),
    case pdb:get(FDPid, fd_hbs) of
        undefined when Create ->
            % synchronously create new hb process
            {ok, HBSPid} = fd_hbs:start_link(pid_groups:my_groupname(), FDPid),
            pdb:set({FDPid, HBSPid}, fd_hbs),
            comm:send_local(HBSPid, Msg),
            {HBSPid, [HBSPid | State]};
        undefined ->
            {undefined, State};
        Entry ->
            HBSPid = element(2, Entry),
            comm:send_local(HBSPid, Msg),
            {HBSPid, State}
    end.

%% @doc show subscriptions
-spec subscriptions() -> ok.
subscriptions() ->
    FD = my_fd_pid(),
    _ = case FD of
            failed -> [];
            FD ->
                TranslatePid = fun(Pid) ->
                                       case pid_groups:group_and_name_of(Pid) of
                                           failed       -> Pid;
                                           GroupAndName -> GroupAndName
                                       end
                               end,
                {dictionary, Dictionary} = process_info(FD, dictionary),
                All_HBS = [ X || {{_,_,fd},{{_,_,fd},X}} <- Dictionary ],
                io:format("Remote nodes watched: ~p~n", [length(All_HBS)]),
                [ begin
                      io:format("fd_hbs: ~p~n", [pid_groups:group_and_name_of(X)]),
                      {dictionary, FD_HBS_Dict} = process_info(X, dictionary),
                      [ begin
                            PlainPid = comm:get_plain_pid(LSub),
                            SubPid = TranslatePid(PlainPid),
                            Watched = case comm:is_local(WPid) of
                                          true ->
                                              TranslatePid(comm:make_local(WPid));
                                          false ->
                                              WPid
                                      end,
                            case PlainPid of
                                LSub -> io:format("  ~p -> ~p ~p~n",
                                                  [SubPid, Watched, Count]);
                                _ -> io:format("  ~p -> ~p ~p~n    (subscribed as ~p)~n",
                                               [SubPid, Watched, Count, LSub])
                            end
                        end
                        || {{LSub, {_,_,_} = WPid} = Key, {Key, Count}} <- FD_HBS_Dict ]
                  end || X <- All_HBS ]
        end,
    ok.
