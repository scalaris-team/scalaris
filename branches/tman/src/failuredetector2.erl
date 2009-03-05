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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fÃ¼r Informationstechnik Berlin
%% @version $Id$
-module(failuredetector2).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_server).

-export([start_link/0, subscribe/1, unsubscribe/1,remove_subscriber/1,getmytargets/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc generates a failure detector for the calling process on the given pid.
-spec(subscribe/1 :: (cs_send:mypid() | list(cs_send:mypid())) -> ok).
subscribe(PidList) when is_list(PidList) ->
		io:format("Subscrib by ~p : ~p~n",[process_dictionary:lookup_process(self()),PidList]),
    gen_server:call(?MODULE, {subscribe_list, PidList, self()}, 20000);
subscribe(Pid) ->
		io:format("Subscrib by ~p : ~p~n",[process_dictionary:lookup_process(self()),Pid]),
    gen_server:call(?MODULE, {subscribe_list, [Pid], self()}, 20000).

%% @doc deletes the failure detector for the given pid.
-spec(unsubscribe/1 :: (cs_send:mypid()) -> ok).
unsubscribe(TargetList) when is_list(TargetList) ->
    gen_server:call(?MODULE, {unsubscribe_list, TargetList, self()}, 20000);
unsubscribe(Target) ->
    gen_server:call(?MODULE, {unsubscribe_list, [Target], self()}, 20000).

%% 
-spec(remove_subscriber/1 :: (pid()) -> ok).
remove_subscriber(Pid) ->
    gen_server:call(?MODULE, {remove_subscriber, Pid}, 20000).
    
getmytargets()  ->
    gen_server:call(?MODULE, {getmytargets, self()}, 20000).	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ping Process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_pinger(Pid) ->
    spawn(fun() ->
		  loop_ping(Pid, 0)
	  end).

loop_ping(Pid, Count) ->
    cs_send:send(Pid, {ping, cs_send:this(), Count}),
    erlang:send_after(config:failureDetectorInterval(), self(), {timeout}), 
    receive
	{stop} ->
	    ok;
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
    process_dictionary:register_process("fd", failure_detector, self()),
    fd_db:init(),
	Linker = start_linker(),
    {ok, {Linker}}.

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
handle_call({subscribe_list, PidList, Subscriber}, _From,{ Linker} = State) ->
    % link subscriber
    Linker ! {link, Subscriber},
    % make ping processes for subscription
    [make_pinger(Pid) || Pid <- PidList],
    % store subscription
    [subscribe( Pid, Subscriber) || Pid <- PidList],
    {reply, ok, State};
handle_call({unsubscribe_list, TargetList, Unsubscriber}, _From,{_Linker} = State) ->
	[ unsubscribe(Target, Unsubscriber) || Target <- TargetList ],
    {reply, ok, State};

handle_call({crash, Target}, _From,  {_Linker} = State) ->
   	
    fd_db:del_pinger(fd_db:get_pinger(Target)),
   	
	Subscribers = fd_db:get_subscribers(Target),
	    
    case fd_db:get_subscribers(Target) of
		[] ->
	    	log:log(error,"[ FD ] shouldn't happen1");
		Subscribers ->
	    	% notify and unsubscribe
	    	[crash_and_unsubscribe(Target, Subscriber) || Subscriber <- Subscribers]
   
    end,
    {reply, ok, State};

handle_call({remove_subscriber, Subscriber}, _From, 
	    {_Linker} = State) ->
    WatchedPids = fd_db:get_subscriptions(Subscriber),
    [ fd_db:del_subscription(Subscriber,WatchedPid) || WatchedPid <- WatchedPids ],

    lists:map(fun (WatchedPid) ->
                       case fd_db:get_subscribers(WatchedPid) of
                           [] ->
                               kill_pinger(WatchedPid);
                           _ ->
                               ok
                       end
              	end, WatchedPids),
    {reply, ok, State};
handle_call({getmytargets, Subscriber}, _From,{_Linker} = State) ->
    Targets=fd_db:get_subscriptions(Subscriber),
    {reply, Targets, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
%@private
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

make_pinger(Target) ->
    case fd_db:get_pinger(Target) of
		none ->
	   		log:log(info,"[ FD ~p ] starting pinger for ~p", [self(), Target]),
	    	Pinger = start_pinger(Target),
		    fd_db:add_pinger(Target, Pinger),
	    	ok;
		{ok,_} ->
		    ok
    end.

kill_pinger(Target) ->
	case fd_db:get_pinger(Target) of
      {ok,X} ->
          X	! {stop},
      		fd_db:del_pinger(Target);
      none ->
          failed
	end.
  
          
    

%% (mypid() -> list(pid()), pid() -> mypid())
subscribe(Target, Subscriber) ->
	fd_db:add_subscription(Subscriber, Target).

-spec(unsubscribe/2 :: (cs_node:mypid(),pid()) -> ok).
unsubscribe(Target, Subscriber) ->
	fd_db:del_subscription(Subscriber, Target),
	case fd_db:get_subscribers(Target) of
  	[] ->
    	kill_pinger(Target);
	_X ->
		ok
	end.

crash_and_unsubscribe(Target, Subscriber) ->
    io:format("~p got a crash message for ~p~n",[process_dictionary:lookup_process(Subscriber), Target]),
    Subscriber ! {crash, Target},
    unsubscribe(Target,Subscriber).



report_crash(Target) ->
   log:log(warn,"[ FD ] ~p crashed at ~p",[Target, erlang:time()]),
   
	%{Group, Name} = process_dictionary:lookup_process(Target),
  %log:log(warn,"[ FD ] a ~p process died",[Name]),
  (catch erlang:process_display(erlang:element(3,Target), backtrace)),
 	%TManPid = process_dictionary:lookup_process(Group, ring_maintenance),
 	%(catch erlang:process_display(TManPid, backtrace)),
 	 gen_server:call(?MODULE, {crash, Target}, 20000).


dump_to_file(Pid) when not(is_pid(Pid)) ->
    failed;
dump_to_file(Pid) ->
   %	Res =  (catch erlang:process_info(Pid, backtrace)),
    
	%	case Res of
   % 	{backtrace, Bin} ->
   %   	Trace =  binary_to_list(Bin),
		%		N = "/tmp/scalaris-crash.log",
			%	{ok,S} = file:open(N,[append]),
			%	io:format(S,"FD ~p | ~p crashed~n#~p~n",[self(),Pid,Trace]),
			%	file:close(S),
			%	ok;
			%_X -> 
			%	log:log(error,"[ FD ] Trying to get Backtrace of ~p ~n   Res = ~p~n",[Pid,Res]),
				ok. 
	%end.
