% @copyright 2007-2014 Zuse Institute Berlin

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

-ifdef(with_export_type_support).
-export_type([cookie/0, reason/0]).
-endif.

-export([subscribe/1, subscribe/2, subscribe_refcount/2, subscribe_pid/2,
         subscribe_pid/3]).
-export([unsubscribe/1, unsubscribe/2, unsubscribe_refcount/2,
         unsubscribe_pid/2, unsubscribe_pid/3]).
-export([update_subscriptions/2, update_subscriptions_pid/3]).
-export([report_my_crash/1]).
%% gen_server & gen_component callbacks
-export([start_link/1, init/1, on/2]).

%% debug purposes
-export([subscriptions/0]).

-type cookie() :: {pid(), '$fd_nil'} | any().
-type reason() :: 'DOWN' | noconnection | term().
-type state() :: ok.

-define(SEND_OPTIONS, [{channel, prio}]).

%% @doc Generate a failure detector for the Subscriber pid on the Monitored pids
%% (Subscribers can be self(), pid() or an envelop as created by
%% comm:reply_as/3).
-spec subscribe_pid(Monitored::comm:mypid() | [comm:mypid()], Subscriber::comm:mypid()) ->
    ok.
subscribe_pid(GlobalPids, Subscriber) ->
    subscribe_pid(GlobalPids, Subscriber, {self(), '$fd_nil'}).

%% @doc Generate a failure detector for the Subscriber pid and cookie on the Monitored pids
%% (Subscribers can be self(), pid() or an envelop as created by
%% comm:reply_as/3).
-spec subscribe_pid(Monitored::comm:mypid() | [comm:mypid()], Subscriber::comm:mypid(),
                    cookie()) -> ok.
subscribe_pid([], _Subscriber, _Cookie) -> ok;
subscribe_pid(GlobalPids, Subscriber, Cookie) when
      is_list(GlobalPids)->
    [Pid | RestPids] = GlobalPids,
    subscribe_single(my_fd_pid(), Pid, Subscriber, Cookie),
    subscribe_pid(RestPids, Subscriber, Cookie);
subscribe_pid(Pid, Subscriber, Cookie) ->
    subscribe_single(my_fd_pid(), Pid, Subscriber, Cookie).

%% @doc Generates a failure detector for the calling process on the given pid.
-spec subscribe(comm:mypid() | [comm:mypid()]) -> ok.
subscribe([]) -> ok;
subscribe(GlobalPids) ->
    subscribe(GlobalPids, {self(), '$fd_nil'}).

%% @doc Generates a failure detector for the calling process and cookie on the
%%      given pid.
-spec subscribe(comm:mypid() | [comm:mypid()], cookie()) -> ok.
subscribe([], _Cookie)         -> ok;
subscribe(GlobalPids, Cookie) when is_list(GlobalPids) ->
    FD = my_fd_pid(),
    _ = [subscribe_single(FD, Pid, Cookie) || Pid <- GlobalPids],
    ok;
subscribe(GlobalPid, Cookie) ->
    subscribe_single(my_fd_pid(), GlobalPid, Cookie).

-spec subscribe_single(FD::pid(), comm:mypid() | [comm:mypid()], cookie()) -> ok.
subscribe_single(FD, GlobalPid, Cookie) ->
    comm:send_local(FD, {add_subscriber_via_fd, self(), GlobalPid, Cookie}).

-spec subscribe_single(FD::pid(), comm:mypid() | [comm:mypid()], comm:mypid(), cookie()) -> ok.
subscribe_single(FD, GlobalPid, Subscriber, Cookie) ->
    comm:send_local(FD, {add_subscriber_via_fd, Subscriber, GlobalPid, Cookie}).

%% @doc Generates a failure detector for the calling process and cookie on the
%%      given pid - uses reference counting to be subscribed to a pid only once.
%%      Unsubscribe with unsubscribe_refcount/2!
-spec subscribe_refcount(comm:mypid() | [comm:mypid()], cookie()) -> ok.
subscribe_refcount([], _Cookie)         -> ok;
subscribe_refcount(GlobalPids, Cookie) when is_list(GlobalPids) ->
    FD = my_fd_pid(),
    _ = [subscribe_single_refcount(FD, Pid, Cookie) || Pid <- GlobalPids],
    ok;
subscribe_refcount(GlobalPid, Cookie) ->
    subscribe_single_refcount(my_fd_pid(), GlobalPid, Cookie).

-spec subscribe_single_refcount(FD::pid(), comm:mypid() | [comm:mypid()], cookie()) -> ok.
subscribe_single_refcount(FD, GlobalPid, Cookie) ->
    Key = {'$fd_subscribe', GlobalPid, Cookie},
    OldCount = case erlang:get(Key) of
                   undefined -> subscribe_single(FD, GlobalPid, Cookie),
                                0;
                   X -> X
               end,
    erlang:put(Key, OldCount + 1),
    ok.

%% @doc Deletes the failure detector for the given pid and subscriber.
-spec unsubscribe_pid(Monitored::comm:mypid() | [comm:mypid()], Subscriber::comm:mypid()) ->
    ok.
unsubscribe_pid(GlobalPids, Subscriber) ->
    unsubscribe_pid(GlobalPids, Subscriber, {self(), '$fd_nil'}).

%% @doc Deletes the failure detector for the given pid, subscriber and cookie.
-spec unsubscribe_pid(Monitored::comm:mypid() | [comm:mypid()], Subscriber::comm:mypid(),
                    cookie()) -> ok.
unsubscribe_pid([], _Subscriber, _Cookie) -> ok;
unsubscribe_pid(GlobalPids, Subscriber, Cookie) when
      is_list(GlobalPids)->
    [Pid | RestPids] = GlobalPids,
    unsubscribe_single(my_fd_pid(), Pid, Subscriber, Cookie),
    unsubscribe_pid(RestPids, Subscriber, Cookie);
unsubscribe_pid(Pid, Subscriber, Cookie) ->
    unsubscribe_single(my_fd_pid(), Pid, Subscriber, Cookie).

%% @doc Deletes the failure detector for the given pid.
-spec unsubscribe(comm:mypid() | [comm:mypid()]) -> ok.
unsubscribe([])-> ok;
unsubscribe(GlobalPids)->
    unsubscribe(GlobalPids, {self(), '$fd_nil'}).

%% @doc Deletes the failure detector for the given pid and cookie.
-spec unsubscribe(comm:mypid() | [comm:mypid()], cookie()) -> ok.
unsubscribe([], _Cookie)         -> ok;
unsubscribe(GlobalPids, Cookie) when is_list(GlobalPids) ->
    FD = my_fd_pid(),
    _ = [begin
             unsubscribe_single(FD, Pid, Cookie)
         end
         || Pid <- GlobalPids],
    ok;
unsubscribe(GlobalPid, Cookie) ->
    unsubscribe_single(my_fd_pid(), GlobalPid, Cookie).

-spec unsubscribe_single(FD::pid(), comm:mypid() | [comm:mypid()], cookie()) -> ok.
unsubscribe_single(FD, GlobalPid, Cookie) ->
    comm:send_local(FD, {del_subscriber_via_fd, self(), GlobalPid, Cookie}).

-spec unsubscribe_single(FD::pid(), comm:mypid() | [comm:mypid()], comm:mypid(), cookie()) -> ok.
unsubscribe_single(FD, GlobalPid, Subscriber, Cookie) ->
    comm:send_local(FD, {del_subscriber_via_fd, Subscriber, GlobalPid, Cookie}).

%% @doc Deletes the failure detector for the given pid and cookie - uses
%%      reference counting to be subscribed to a pid only once.
%%      Subscribe with subscribe_refcount/2!
-spec unsubscribe_refcount(comm:mypid() | [comm:mypid()], cookie()) -> ok.
unsubscribe_refcount([], _Cookie)         -> ok;
unsubscribe_refcount(GlobalPids, Cookie) when is_list(GlobalPids) ->
    FD = my_fd_pid(),
    _ = [begin
             unsubscribe_single_refcount(FD, Pid, Cookie)
         end
         || Pid <- GlobalPids],
    ok;
unsubscribe_refcount(GlobalPid, Cookie) ->
    unsubscribe_single_refcount(my_fd_pid(), GlobalPid, Cookie).

-spec unsubscribe_single_refcount(FD::pid(), comm:mypid() | [comm:mypid()], cookie()) -> ok.
unsubscribe_single_refcount(FD, GlobalPid, Cookie) ->
    Key = {'$fd_subscribe', GlobalPid, Cookie},
    _ = case erlang:get(Key) of
            undefined -> ok; % TODO: warn here?
            1 -> %% delay the actual unsubscribe for better perf.?
                unsubscribe_single(FD, GlobalPid, Cookie),
                erlang:erase(Key);
            OldCount ->
                erlang:put(Key, OldCount - 1)
        end,
    ok.

%% @doc Unsubscribes Subscriber from the pids in OldPids but not in NewPids and subscribes
%%      to the pids in NewPids but not in OldPids
%%      (Subscribers can be self(), pid() or an envelop as created by
%%      comm:reply_as/3).
-spec update_subscriptions_pid([comm:mypid()], [comm:mypid()],
                               Subscriber::comm:mypid()) -> ok.
update_subscriptions_pid(OldPids, NewPids, Subscriber) ->
    {OnlyOldPids, _Same, OnlyNewPids} = util:split_unique(OldPids, NewPids),
    unsubscribe_pid(OnlyOldPids, Subscriber),
    subscribe_pid(OnlyNewPids, Subscriber).

%% @doc Unsubscribes from the pids in OldPids but not in NewPids and subscribes
%%      to the pids in NewPids but not in OldPids.
-spec update_subscriptions([comm:mypid()], [comm:mypid()]) -> ok.
update_subscriptions(OldPids, NewPids) ->
    {OnlyOldPids, _Same, OnlyNewPids} = util:split_unique(OldPids, NewPids),
    unsubscribe(OnlyOldPids),
    subscribe(OnlyNewPids).

%% @doc Reports the calling process' group as being shut down due to a graceful
%%      leave operation.
-spec report_my_crash(Reason::reason()) -> ok.
report_my_crash(Reason) ->
    case pid_groups:get_my(sup_dht_node) of
        failed ->
            log:log(error, "[ FD ] call to report_my_crash(~p) from ~p "
                           "outside a valid dht_node group!", [Reason, self()]);
        DhtNodeSupPid ->
            FD = my_fd_pid(),
            comm:send_local(FD, {report_crash,
                                 sup:sup_get_all_children(DhtNodeSupPid),
                                 Reason})
    end.

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
    ok.

%% @private
-spec on(comm:message(), state()) -> state().
on({hbs_finished, RemoteWatchedPid}, State) ->
    pdb:delete(comm:get(fd, RemoteWatchedPid), fd_hbs),
    State;

on({subscribe_heartbeats, Subscriber, TargetPid}, State) ->
    %% we establish the back-direction here, so we subscribe to the
    %% subscriber and add the TargetPid to the local monitoring.
    ?TRACE("FD: subscribe_heartbeats~n", []),
    SubscriberFDPid = comm:get(fd, Subscriber),
    HBPid = case pdb:get(SubscriberFDPid, fd_hbs) of
                undefined -> start_and_register_hbs(SubscriberFDPid);
                Res -> element(2, Res)
            end,
    comm:send_local(HBPid, {add_watching_of, TargetPid}),
    comm:send(Subscriber, {update_remote_hbs_to, comm:make_global(HBPid)}, ?SEND_OPTIONS),
    State;

on({pong, RemHBSSubscriber, RemoteDelay}, State) ->
    ?TRACE("FD: pong, ~p~n", [RemHBSSubscriber]),
    forward_to_hbs(RemHBSSubscriber, {pong_via_fd, RemHBSSubscriber, RemoteDelay}),
    State;

on({add_subscriber_via_fd, Subscriber, WatchedPid, Cookie}, State) ->
    ?TRACE("FD: subscribe ~p to ~p (cookie: ~p)~n", [Subscriber, WatchedPid, Cookie]),
    forward_to_hbs(WatchedPid, {add_subscriber, Subscriber, WatchedPid, Cookie}),
    State;

on({del_subscriber_via_fd, Subscriber, WatchedPid, Cookie}, State) ->
    ?TRACE("FD: unsubscribe ~p to ~p (cookie: ~p)~n", [Subscriber, WatchedPid, Cookie]),
    forward_to_hbs(WatchedPid, {del_subscriber, Subscriber, WatchedPid, Cookie}),
    State;

on({add_watching_of_via_fd, Subscriber, Pid}, State) ->
    ?TRACE("FD: add_watching_of ~p~n", [Pid]),
    forward_to_hbs(Subscriber, {add_watching_of, Pid}),
    State;

on({del_watching_of_via_fd, Subscriber, Pid}, State) ->
    ?TRACE("FD: del_watching_of ~p~n", [Pid]),
    forward_to_hbs(Subscriber, {del_watching_of, Pid}),
    State;

on({crashed, WatchedPid, _Warn} = Msg, State) ->
    ?TRACE("FD: crashed message via fd for watched pid ~p~n", [WatchedPid]),
    forward_to_hbs(WatchedPid, Msg, false),
    State;

%% on({web_debug_info, _Requestor}, State) ->
%%     ?TRACE("FD: web_debug_info~n", []),
%% TODO: reimplement for new fd.
%%     Subscriptions = fd_db:get_subscriptions(),
%%     % resolve (local and remote) pids to names:
%%     S2 = [begin
%%               case comm:is_local(TargetPid) of
%%                   true -> {Subscriber,
%%                            {pid_groups:pid_to_name(comm:make_local(TargetPid)), Cookie}};
%%                   _ ->
%%                       comm:send(comm:get(pid_groups, TargetPid),
%%                                 {group_and_name_of, TargetPid, comm:this()}, ?SEND_OPTIONS),
%%                       receive
%%                           {group_and_name_of_response, Name} ->
%%                               {Subscriber, {pid_groups:pid_to_name2(Name), Cookie}}
%%                       after 2000 -> X
%%                       end
%%               end
%%           end || X = {Subscriber, {TargetPid, Cookie}} <- Subscriptions],
%%     KeyValueList =
%%         [{"subscriptions", length(Subscriptions)},
%%          {"subscriptions (subscriber, {target, cookie}):", ""} |
%%          [{pid_groups:pid_to_name(Pid),
%%            webhelpers:safe_html_string("~p", [X]))} || {Pid, X} <- S2]],
%%     comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
%%     State;

on({report_crash, [], _Reason}, State) ->
    State;
on({report_crash, [_|_] = LocalPids, Reason}, State) ->
    ?TRACE("FD: report_crash ~p with reason ~p~n", [LocalPids, Reason]),
    ?DBG_ASSERT([] =:= [X || X <- LocalPids, not is_pid(X)]),
    Msg = {report_crash, LocalPids, Reason},
    % don't create new hbs processes!
    forward_to_hbs(comm:make_global(hd(LocalPids)), Msg, false),
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

%% @doc start a new hbs process inside the fd process context (ets owner)
%%      precond: FDPid points to the fd process at the target node
-spec start_and_register_hbs(FDPid::comm:mypid()) -> pid().
start_and_register_hbs(FDPid) ->
    {ok, NewHBS} = fd_hbs:start_link(pid_groups:my_groupname(), FDPid),
    pdb:set({FDPid, NewHBS}, fd_hbs),
    NewHBS.

%% @doc Forwards the given message to the registered HBS or creates a new HBS.
-spec forward_to_hbs(comm:mypid(), comm:message()) -> ok.
forward_to_hbs(Pid, Msg) ->
    forward_to_hbs(Pid, Msg, true).

%% @doc Forwards the given message to the registered hbs or either creates a
%%      new hbs (if Create is true) or ignores the message (Create is false).
-spec forward_to_hbs(comm:mypid(), comm:message(), Create::boolean()) -> ok.
forward_to_hbs(Pid, Msg, Create) ->
    FDPid = comm:get(fd, Pid),
    case pdb:get(FDPid, fd_hbs) of
        undefined when Create ->
            % synchronously create new hb process
            HBSPid = start_and_register_hbs(FDPid),
            comm:send_local(HBSPid, Msg);
        undefined ->
            ok;
        Entry ->
            HBSPid = element(2, Entry),
            comm:send_local(HBSPid, Msg)
    end.

%% @doc show subscriptions
-spec subscriptions() -> ok.
subscriptions() ->
    FD = my_fd_pid(),
    _ = case FD of
            failed -> [];
            FD ->
                {dictionary, Dictionary} = process_info(FD, dictionary),
                All_HBS = [ X || {{_,_,fd},{{_,_,fd},X}} <- Dictionary ],
                io:format("Remote nodes watched: ~p~n", [length(All_HBS)]),
                [ begin
                      io:format("fd_hbs: ~p~n", [pid_groups:group_and_name_of(X)]),
                      {dictionary, FD_HBS_Dict} = process_info(X, dictionary),
                      [ begin
                            Sub = case pid_groups:group_and_name_of(LSub) of
                                failed ->
                                          LSub;
                                GroupAndName ->
                                    GroupAndName
                            end,
                            io:format("  ~p ~p ~p~n",
                                      [Sub, Cookies, Count])
                        end
                        || {{LSub,{_,_,_}},
                            {{LSub,{_,_,_}}, Cookies, Count}}
                               <- FD_HBS_Dict ]
                  end || X <- All_HBS ]
        end,
    ok.
