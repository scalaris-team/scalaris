%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : failuredetector2.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Failure detector based on Guerraoui
%%%
%%% Created :  25 Nov 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(failuredetector2).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_server).

-export([start_link/0, subscribe/1, unsubscribe/1, remove_subscriber/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc generates a failure detector for the calling process on the given pid.
-spec(subscribe/1 :: (cs_send:mypid() | list(cs_send:mypid())) -> ok).
subscribe(PidList) when is_list(PidList) ->
    gen_server:call(?MODULE, {subscribe_list, PidList, self()}, 20000);
subscribe(Pid) ->
    gen_server:call(?MODULE, {subscribe_list, [Pid], self()}, 20000).

%% @doc deletes the failure detector for the given pid.
-spec(unsubscribe/1 :: (cs_send:mypid()) -> ok).
unsubscribe(Pid) ->
    gen_server:call(?MODULE, {unsubscribe, Pid, self()}, 20000).

%% @private
-spec(remove_subscriber/1 :: (pid()) -> ok).
remove_subscriber(Pid) ->
    gen_server:call(?MODULE, {remove_subscriber, Pid}, 20000).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ping Process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_pinger(Pid) ->
    spawn(fun() ->
		  loop_ping(Pid, 0)
	  end).

loop_ping(Pid, Count) ->
    cs_send:send(Pid, {ping, cs_send:this(), Count}),
    timer:send_after(config:failureDetectorInterval(), {timeout}), 
    receive
	{pong, _Count} ->
	    receive
		{timeout} ->
		    loop_ping(Pid, Count + 1)
	    end;
	{timeout} ->
	    report_crash(Pid)
	    %loop_ping(Pid, Count + 1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Link Process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_linker() ->
    spawn(fun() ->
		  process_flag(trap_exit, true),
		  loop_link()
	  end).

loop_link() ->
    receive
	{'EXIT', Pid, _Reason} ->
	    remove_subscriber(Pid),
	    loop_link();
	{link, Pid} ->
	    link(Pid),
	    loop_link();
	{unlink, Pid} ->
	    unlink(Pid),
	    loop_link()
    end.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    MyPid2Subscribers = ets:new(?MODULE, [set, protected]),
    Pid2WatchedPids = ets:new(?MODULE, [set, protected]),
    Pinger = ets:new(?MODULE, [set, protected]),
    Linker = start_linker(),
    {ok, {MyPid2Subscribers, Pid2WatchedPids, Pinger, Linker}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
%% @private
handle_call({subscribe_list, PidList, Owner}, _From, 
	    {MyPid2Subscribers, Pid2WatchedPids, PingerTable, Linker} = State) ->
    % link subscriber
    Linker ! {link, Owner},
    % make ping processes for subscription
    [make_pinger(PingerTable, Pid) || Pid <- PidList],
    % store subscription
    [subscribe(MyPid2Subscribers, Pid2WatchedPids, Pid, Owner) || Pid <- PidList],
    {reply, ok, State};
handle_call({unsubscribe, Pid, Owner}, _From, 
	    {MyPid2Subscribers, Pid2WatchedPids, PingerTable, _Linker} = State) ->
    unsubscribe(MyPid2Subscribers, Pid2WatchedPids, PingerTable, Pid, Owner),
    {reply, ok, State};
handle_call({crash, Pid}, _From, 
	    {MyPid2Subscribers, Pid2WatchedPids, PingerTable, _Linker} = State) ->
    ets:delete(PingerTable, Pid),
    case ets:lookup(MyPid2Subscribers, Pid) of
	[{Pid, Subscribers}] ->
	    % notify and unsubscribe
	    [crash_and_unsubscribe(MyPid2Subscribers, Pid2WatchedPids, PingerTable, Pid, X) || X <- Subscribers];
	[] ->
	    io:format("@fd: shouldn't happen1~n", [])
    end,
    {reply, ok, State};
handle_call({remove_subscriber, Subscriber}, _From, 
	    {MyPid2Subscribers, Pid2WatchedPids, PingerTable, _Linker} = State) ->
    WatchedPids = ets:lookup(Pid2WatchedPids, Subscriber),
    [unsubscribe(MyPid2Subscribers, Pid2WatchedPids, PingerTable, WatchedPid, Subscriber) 
     || WatchedPid <- WatchedPids],
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
%% @private
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

make_pinger(PingerTable, Pid) ->
    case ets:lookup(PingerTable, Pid) of
	[] ->
	    io:format("[ I | FD     | ~p ] starting pinger for ~p~n", [self(), Pid]),
	    Pinger = start_pinger(Pid),
	    ets:insert(PingerTable, {Pid, Pinger}),
	    ok;
	[{Pid, _Pinger}] ->
	    ok
    end.

kill_pinger(PingerTable, Pid) ->
    case ets:lookup(PingerTable, Pid) of
	[] ->
	    ok;
	[{Pid, Pinger}] ->
	    ets:delete(PingerTable, {Pid, Pinger}),
	    Pinger ! {stop}
    end.   

%% (mypid() -> list(pid()), pid() -> mypid())
subscribe(MyPid2Subscribers, Pid2WatchedPids, Pid, Owner) ->
    case ets:lookup(MyPid2Subscribers, Pid) of
	[] ->
	    ets:insert(MyPid2Subscribers, {Pid, [Owner]}),
	    ets:insert(Pid2WatchedPids, {Owner, Pid}),
	    ok;
	[{Pid, Subscribers}] ->
	    case lists:member(Owner, Subscribers) of
		true ->
		    ok;
		false ->
		    ets:insert(MyPid2Subscribers, {Pid, [Owner | Subscribers]}),
		    ets:insert(Pid2WatchedPids, {Owner, Pid}),
		    ok
	    end
    end.

%% (mypid() -> list(pid()), pid() -> mypid())
unsubscribe(MyPid2Subscribers, Pid2WatchedPids, PingerTable, Pid, Owner) ->
    ets:delete(Pid2WatchedPids, {Pid, Owner}),
    case ets:lookup(MyPid2Subscribers, Pid) of
	[] ->
	    ok;
	[{Pid, Subscribers}] ->
	    NewSubscribers = lists:delete(Owner, Subscribers),
	    case length(NewSubscribers) of
		0 ->
		    kill_pinger(PingerTable, Pid);
		_ ->
		    ok
	    end
    end.

crash_and_unsubscribe(MyPid2Subscribers, Pid2WatchedPids, PingerTable, Pid, Owner) ->
    Owner ! {crash, Pid},
    unsubscribe(MyPid2Subscribers, Pid2WatchedPids, PingerTable, Pid, Owner).

report_crash(Pid) ->
    gen_server:call(?MODULE, {crash, Pid}, 20000).
    
