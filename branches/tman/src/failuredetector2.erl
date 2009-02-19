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
    gen_server:call(?MODULE, {subscribe_list, PidList, self()}, 20000);
subscribe(Pid) ->
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
    timer:send_after(config:failureDetectorInterval(), {timeout}), 
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
    %% mapping: remote_process -> list(subscribers)
    %%MyPid2Subscribers = ets:new(?MODULE, [set, protected]),
    %% list of {subscriber (local process), watched process}
    %% {pid(), cs_send:mypid()}
    SubscriberTable = ets:new(?MODULE, [bag, protected]),
    Pinger = ets:new(?MODULE, [set, protected]),
    Linker = start_linker(),
    {ok, {SubscriberTable, Pinger, Linker}}.

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
handle_call({subscribe_list, PidList, Subscriber}, _From,{SubscriberTable, PingerTable, Linker} = State) ->
    % link subscriber
    Linker ! {link, Subscriber},
    % make ping processes for subscription
    [make_pinger(PingerTable, Pid) || Pid <- PidList],
    % store subscription
    [subscribe(SubscriberTable, Pid, Subscriber) || Pid <- PidList],
    {reply, ok, State};
handle_call({unsubscribe_list, TargetList, Unsubscriber}, _From,{SubscriberTable, PingerTable, _Linker} = State) ->
	[ unsubscribe(SubscriberTable, PingerTable, Target, Unsubscriber) || Target <- TargetList ],
    {reply, ok, State};
handle_call({crash, Pid}, _From,  {SubscriberTable, PingerTable, _Linker} = State) ->
   	
    %G = (catch  erlang:process_display(element(3,Pid),backtrace)),
    %io:format("~p~n",[G]),
    
    ets:delete(PingerTable, Pid),
    case ets:match(SubscriberTable, {'$1', Pid}) of
		[] ->
	    	log:log(error,"[ FD ] shouldn't happen1");
		Subscribers ->
	    	% notify and unsubscribe
            
	    	[crash_and_unsubscribe(SubscriberTable, PingerTable, Pid, X) || [X] <- Subscribers]
    
    end,
    {reply, ok, State};
handle_call({remove_subscriber, Subscriber}, _From, 
	    {SubscriberTable, PingerTable, _Linker} = State) ->
    WatchedPids = ets:lookup(SubscriberTable, Subscriber),
    ets:delete(SubscriberTable, Subscriber),
    lists:map(fun (Pid) ->
                       case ets:match(SubscriberTable, {'$1', Pid}) of
                           [] ->
                               kill_pinger(PingerTable, Pid);
                           _ ->
                               ok
                       end
              	end, WatchedPids),
    {reply, ok, State};
handle_call({getmytargets, Pid}, _From,{SubscriberTable, _PingerTable, _Linker} = State) ->
    Targets=ets:lookup(SubscriberTable, Pid),
    {reply, Targets, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
%@private
handle_cast({debug_info, Requestor}, {SubscriberTable, PingerTable, _Linker} = State) ->
    Subscribers = length(lists:usort(ets:match(SubscriberTable, {'$1', '_'}))), 
    Targets     = length(lists:usort(ets:match(SubscriberTable, {'_', '$1'}))), 
    Requestor ! {debug_info_response, [
					       	{"SubscriberTable", lists:flatten(io_lib:format("~p", [length(ets:tab2list(SubscriberTable))]))},
						   	{"Pinger", lists:flatten(io_lib:format("~p", [length(ets:tab2list(PingerTable))]))},
					       	{"Subscribers", lists:flatten(io_lib:format("~p", [Subscribers]))},
						   	{"Targets", lists:flatten(io_lib:format("~p", [Targets]))}
                                      ]},
    {noreply, State};
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

make_pinger(PingerTable, Target) ->
    case ets:lookup(PingerTable, Target) of
	[] ->
	   log:log(info,"[ FD ~p ] starting pinger for ~p", [self(), Target]),
	    Pinger = start_pinger(Target),
	    ets:insert(PingerTable, {Target, Pinger}),
	    ok;
	[{_Pid, _Pinger}] ->
	    ok
    end.

kill_pinger(PingerTable, Target) ->
    case ets:lookup(PingerTable, Target) of
	[] ->
	    aua;
	[{_Pid, Pinger}] ->
	    ets:delete(PingerTable, {Target, Pinger}),
	    Pinger ! {stop}
    end.   

%% (mypid() -> list(pid()), pid() -> mypid())
subscribe(SubscriberTable, Pid, Subscriber) ->
	ets:insert(SubscriberTable, {Subscriber, Pid}).

%% (mypid() -> list(pid()), pid() -> mypid())
unsubscribe(SubscriberTable, PingerTable, Target, Unsubscriber) ->
    %io:format("unsub: ~p~n",[{Unsubscriber, Target}]),
    ets:delete_object(SubscriberTable, {Unsubscriber, Target}),
	case ets:match(SubscriberTable, {'$1', Target}) of
    	[] ->
        	RES= kill_pinger(PingerTable, Target),
			case RES==aua of 
				true ->
					 %io:format("should not happend!~n"),
					 %io:format("unsub: ~p~n",[{Unsubscriber, Target}]);
                    mhh;
				false ->
					ok
			end;
        _X ->
            %io:format("After del: ~p~n",[X])
            ok
     end.

crash_and_unsubscribe(SubscriberTable, PingerTable, Pid, Owner) ->
    %io:format("+ ~p~n", [{SubscriberTable, PingerTable, Pid, Owner}]),
    Owner ! {crash, Pid},
    %io:format("Send crash to ~p~n",[Pid]),
    unsubscribe(SubscriberTable, PingerTable, Pid, Owner).
	%io:format("after unsubscribe~n").

report_crash(Pid) ->
   log:log(warn,"[ FD ] ~p crashed",[Pid]),
	case dump_to_file(Pid) of
		ok ->
			{Group, Name} = process_dictionary:lookup_process(Pid),
   			log:log(warn,"[ FD ] a ~p process died",[Name]),
			TManPid = process_dictionary:lookup_process(Group, ring_maintenance),
			dump_to_file(TManPid);
		failed ->
			ok
	end,
    %io:format("~p b crashed ~n",[Pid]),
    gen_server:call(?MODULE, {crash, Pid}, 20000).
    
dump_to_file(Pid) ->
   	Res =  (catch erlang:process_info(Pid, backtrace)),
    (catch erlang:process_display(Pid, backtrace)),
	case Res of
        {backtrace, Bin} ->
            Trace =  binary_to_list(Bin),
			N = "/tmp/scalaris-crash.log",
			{ok,S} = file:open(N,[append]),
			io:format(S,"FD ~p | ~p crashed~n#~p~n",[self(),Pid,Trace]),
			file:close(S),
			ok;
        _ -> 
			failed
	end.
