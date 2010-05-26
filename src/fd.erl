%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%             2009 onScale solutions GmbH
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    fd.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Failure detector based on Guerraoui.
%%% @end
%%%
%%% Created :  25 Nov 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(fd).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

% API
-export([subscribe/1,subscribe/2]).
-export([unsubscribe/1,unsubscribe/2]).
-export([get_subscribers/1,get_subscribers/2]).
-export([get_subscriptions/0]).
-export([remove_subscriber/1]).

%% gen_server & gen_component callbacks
-export([start_link/0, init/1, on/2]).

-type(cookie() :: '$fd_nil' | any()).
-type(state() :: {null}).
-type(message() ::
    {subscribe_list, Subscriber::pid(), PidList::[cs_send:mypid()], Cookie::cookie()} |
    {unsubscribe_list, Subscriber::pid(), PidList::[cs_send:mypid()], Cookie::cookie()} |
    {get_subscribers, Client::pid(), GlobalPid::cs_send:mypid()} |
    {get_subscribers, Client::pid(), GlobalPid::cs_send:mypid(), Cookie::cookie()} |
    {get_subscriptions, Subscriber::pid()} |
    {remove_subscriber, Subscriber::pid()} |
    {crash, Target::pid()}).

%%% Public Interface
%% @doc generates a failure detector for the calling process on the given pid.
-spec subscribe(cs_send:mypid() | [cs_send:mypid()]) -> ok.
subscribe([]) ->
    ok;
subscribe(GlobalPids) when is_list(GlobalPids) ->
    subscribe(GlobalPids, '$fd_nil');
subscribe(GlobalPid) ->
    subscribe([GlobalPid], '$fd_nil').

%% @doc generates a failure detector for the calling process and cookie on the
%%      given pid.
-spec subscribe(cs_send:mypid() | [cs_send:mypid()], cookie()) -> ok.
subscribe(GlobalPids, Cookie) when is_list(GlobalPids) ->
    cs_send:send_local(my_fd_pid(),
                       {subscribe_list, self(), GlobalPids, Cookie}),
    ok;
subscribe(GlobalPid, Cookie) ->
    subscribe([GlobalPid], Cookie).

%% @doc deletes the failure detector for the given pid.
-spec unsubscribe(cs_send:mypid() | [cs_send:mypid()]) -> ok.
unsubscribe([]) ->
    ok;
unsubscribe(GlobalPids) when is_list(GlobalPids) ->
    unsubscribe(GlobalPids, '$fd_nil');
unsubscribe(GlobalPid) ->
    unsubscribe([GlobalPid], '$fd_nil').

%% @doc deletes the failure detector for the given pid and cookie.
-spec unsubscribe(cs_send:mypid() | [cs_send:mypid()], cookie()) -> ok.
unsubscribe(GlobalPids, Cookie) when is_list(GlobalPids) ->
    cs_send:send_local(my_fd_pid(),
                       {unsubscribe_list, self(), GlobalPids, Cookie}),
    ok;
unsubscribe(GlobalPid, Cookie) ->
    unsubscribe([GlobalPid], Cookie).

%% @doc who is informed on events on a given Pid?
-spec get_subscribers(cs_send:mypid()) -> ok.
get_subscribers(GlobalPid) ->
    cs_send:send_local(my_fd_pid(),
                       {get_subscribers, self(), GlobalPid}),
    ok.
%% @doc who is informed on events on a given Pid and Cookie?
-spec get_subscribers(cs_send:mypid(), cookie()) -> ok.
get_subscribers(GlobalPid, Cookie) ->
    cs_send:send_local(my_fd_pid(),
                       {get_subscribers, self(), GlobalPid, Cookie}),
    ok.

%% @doc on what am I informed?
-spec get_subscriptions() -> ok.
get_subscriptions() ->
    cs_send:send_local(my_fd_pid() , {get_subscriptions, self()}),
    ok.

%% not needed until now
%%% %% @doc delete all my subscriptions
%%% unsubscribe_all() ->
%%%     cs_send:send_local(my_fd_pid() , {unsubscribe_all, self()}),
%%%     ok.
%%% unsubscribe_all(Cookie) ->
%%%     cs_send:send_local(my_fd_pid() , {unsubscribe_all, self(), Cookie}),
%%%     ok.

%% @doc remove all subscriptions of a given Pid
-spec remove_subscriber(pid()) -> ok.
remove_subscriber(Pid) ->
    cs_send:send_local(my_fd_pid() , {remove_subscriber, Pid}).

%% Ping Process
-spec start_pinger(cs_send:mypid()) -> pid().
start_pinger(Pid) ->
   {ok, Pid2} = fd_pinger:start_link([my_fd_pid(), Pid]),
   Pid2.


%% gen_component functions
%% @doc Starts the failure detector server
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_component:start_link(?MODULE, [], [wait_for_init, {register_native, ?MODULE}]).

%% @doc Initialises the module with an empty state.
-spec init(any()) -> state().
init(_Args) ->
    fd_db:init(),
    log:log(info,"[ FD ~p ] starting FD", [self()]),
    %% Linker = start_linker(),
    {null}.

%% @private
-spec on(message(), state()) -> state() | unknown_event.
on({subscribe_list, Subscriber, PidList, Cookie}, State) ->
    [make_pinger(Pid) || Pid <- PidList],
    [fd_db:add_subscription(Subscriber, Pid, Cookie) || Pid <- PidList],
    State;
on({unsubscribe_list, Subscriber, PidList, Cookie}, State) ->
    %% my_unsubscribe itself checks whether to kill pinger
    [my_unsubscribe(Subscriber, Pid, Cookie) || Pid <- PidList ],
    State;

on({get_subscribers, Client, GlobalPid}, State) ->
    Subscribers = fd_db:get_subscribers(GlobalPid),
    cs_send:send_local(Client, {get_subscribers_reply, GlobalPid, Subscribers}),
    State;

on({get_subscribers, Client, GlobalPid, Cookie}, State) ->
    Subscribers = fd_db:get_subscribers(GlobalPid, Cookie),
    cs_send:send_local(Client, {get_subscribers_reply, GlobalPid,
                                        Cookie, Subscribers}),
    State;

on({get_subscriptions, Subscriber}, State) ->
    TmpTargets = fd_db:get_subscriptions(Subscriber),
    Targets = [ case X of
                    {Target, '$fd_nil'} -> Target;
                    Any -> Any
                end || X <- TmpTargets ],
    cs_send:send_local(Subscriber, {get_subscriptions_reply, Targets}),
    State;

%% not used until now
%% on({unsubscribe_all, self()}, State) ->
%% on({unsubscribe_all, self(), Cookie}, State) ->

on({remove_subscriber, Subscriber}, State) ->
    WatchedPids = fd_db:get_subscriptions(Subscriber),
    [ my_unsubscribe(Subscriber, WatchedPid, Cookie)
      || {WatchedPid, Cookie} <- WatchedPids ],
    State;

on({crash, Target}, State) ->
    fd_db:del_pinger(Target),
    case fd_db:get_subscribers(Target) of
        [] ->
            log:log(error, "[ FD ] shouldn't happen1");
        Subscribers ->
            %% notify with cookies and unsubscribe
            [ begin
                  my_notify(Subscriber, Target, Cookie),
                  my_unsubscribe(Subscriber, Target, Cookie)
              end
              || {Subscriber, Cookie} <- Subscribers]
    end,
    State;

 on(_, _State) ->
    unknown_event.

%%% Internal functions
-spec make_pinger(Target::cs_send:mypid()) -> ok.
make_pinger(Target) ->
    case fd_db:get_pinger(Target) of
        none ->
            log:log(info,"[ FD ~p ] starting pinger for ~p", [self(), Target]),
            Pinger = start_pinger(Target),
            fd_db:add_pinger(Target, Pinger),
            ok;
        {ok, _Pinger} ->
            ok
    end.

-spec kill_pinger(Target::cs_send:mypid()) -> true | failed.
kill_pinger(Target) ->
    case fd_db:get_pinger(Target) of
        {ok, Pinger} ->
            cs_send:send_local(Pinger, {stop}),
            fd_db:del_pinger(Target);
        none ->
            failed
    end.

-spec my_notify(Subscriber::pid(), Target::cs_send:mypid(), Cookie::cookie()) -> ok.
my_notify(Subscriber, Target, Cookie) ->
    case Cookie of
        '$fd_nil' ->
            cs_send:send_local(Subscriber, {crash, Target});
        _ ->
            cs_send:send_local(Subscriber, {crash, Target, Cookie})
    end.

-spec my_unsubscribe(Subscriber::pid(), Target::cs_send:mypid(), cookie()) -> true | failed.
my_unsubscribe(Subscriber, Target, Cookie) ->
    fd_db:del_subscription(Subscriber, Target, Cookie),
    case fd_db:get_subscribers(Target) of
        [] -> kill_pinger(Target); %% make a send after and cache pinger?
        _X -> true
    end.

-spec my_fd_pid() -> pid() | failed.
my_fd_pid() ->
    case whereis(?MODULE) of
        undefined ->
            log:log(error, "[ FD ] call of my_fd_pid undefined"),
            failed;
        PID ->
            %% log:log(info, "[ FD ] find right pid"),
            PID
    end.
