%  Copyright 2007-2008 Konrad-Zuse-Zentrum f�r Informationstechnik Berlin
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

-behaviour(gen_component).

-export([init/1, on/2]).

-export([start_link/0, subscribe/1, unsubscribe/1,remove_subscriber/1,getmytargets/0]).

%% gen_server callbacks
-export([terminate/2, code_change/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc generates a failure detector for the calling process on the given pid.
%-spec(subscribe/1 :: (cs_send:mypid() | list(cs_send:mypid())) -> ok).
subscribe(PidList) when is_list(PidList) ->
		%log:log(debug,"Subscrib by ~p : ~p~n",[process_dictionary:lookup_process(self()),PidList]),
    cs_send:send_local(get_pid() , {subscribe_list, PidList, self()}),
    ok;
subscribe(Pid) ->
		%log:log(debug,"Subscrib by ~p : ~p~n",[process_dictionary:lookup_process(self()),Pid]),
    cs_send:send_local(get_pid() , {subscribe_list, [Pid], self()}),
    ok.

%% @doc deletes the failure detector for the given pid.
-spec(unsubscribe/1 :: (cs_send:mypid()) -> ok).
unsubscribe(TargetList) when is_list(TargetList) ->
    cs_send:send_local(get_pid() , {unsubscribe_list, TargetList, self()});
unsubscribe(Target) ->
    cs_send:send_local(get_pid() , {unsubscribe_list, [Target], self()}).

%% 
-spec(remove_subscriber/1 :: (pid()) -> ok).
remove_subscriber(Pid) ->
    cs_send:send_local(get_pid() , {remove_subscriber, Pid}).
    
getmytargets()  ->
    cs_send:send_local(get_pid() , {getmytargets, self()}).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ping Process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_pinger(Pid) ->
   {ok,Pid2} = fd_pinger:start_link([get_pid(),Pid]),
   Pid2.

%% loop_ping(Pid, Count) ->
%%     cs_send:send(Pid, {ping, cs_send:this(), Count}),
%%     cs_send:send_after(config:failureDetectorInterval(), self(), {timeout}), 
%%     receive
%% 	{stop} ->
%% 	    ok;
%% 	{pong, _Count} ->
%% 	    receive
%% 		{timeout} ->
%% 		    loop_ping(Pid, Count + 1)
%% 	    end;
%% 	{timeout} ->
%%      	    report_crash(Pid)
%% 	    %loop_ping(Pid, Count + 1)
%%     end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Link Process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%start_linker() ->
%    fd_linker:start_link(randoms:getRandomId(),[get_pid(),{process_flag(trap_exit, true)}]).


%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------

start_link() ->
   gen_component:start_link(?MODULE, [], [{register_native, failure_detector2}]).


init(_Args) ->
    fd_db:init(),
    log:log(info,"[ FD ~p ] starting FD", [self()]),
	%Linker = start_linker(),
    {null}.

%% @private
on({subscribe_list, PidList, Subscriber},State) ->
    % link subscriber
    % Linker ! {link, Subscriber},
    % make ping processes for subscription
    [make_pinger(Pid) || Pid <- PidList],
    % store subscription
    [subscribe( Pid, Subscriber) || Pid <- PidList],
    State;
on({unsubscribe_list, TargetList, Unsubscriber},{_Linker} = State) ->
	[ unsubscribe(Target, Unsubscriber) || Target <- TargetList ],
    {State};

on({crash, Target}, {_Linker} = State) ->
   	
    fd_db:del_pinger(fd_db:get_pinger(Target)),
  	Subscribers = fd_db:get_subscribers(Target),
	case fd_db:get_subscribers(Target) of
		[] ->
	    	log:log(error,"[ FD ] shouldn't happen1");
		Subscribers ->
	    	% notify and unsubscribe
	    	[crash_and_unsubscribe(Target, Subscriber) || Subscriber <- Subscribers]
    end,
    {State};

on({remove_subscriber, Subscriber},{_Linker} = State) ->
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
    {State};
on({getmytargets, Subscriber},{_Linker} = State) ->
    Targets=fd_db:get_subscriptions(Subscriber),
    cs_send:send_local(Subscriber , Targets),
    State;


 on(_, _State) ->
    unknown_event.


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
          cs_send:send_local(X	, {stop}),
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
    %io:format("~p got a crash message for ~p~n",[process_dictionary:lookup_process(Subscriber), Target]),
    cs_send:send_local(Subscriber , {crash, Target}),
    unsubscribe(Target,Subscriber).



get_pid() ->
    case whereis(failure_detector2) of
        undefined ->
            log:log(error, "[ FD ] call of get_pid undefined");
        PID ->
            %log:log(info, "[ FD ] find right pid"),
            PID
    end.
    
