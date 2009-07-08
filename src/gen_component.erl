%%%------------------------------------------------------------------------------
%%% File    : gen_component.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created : 19 Dec 2008 by Thorsten Schuett <schuett@zib.de>
%%%------------------------------------------------------------------------------
%% @doc 
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(gen_component).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([behaviour_info/1]).

-export([start_link/2, start_link/3, start/4]).

%================================================================================
% behaviour definition
%================================================================================

behaviour_info(callbacks) ->
    [
     % init component
     {init, 1},
     % handle message
     {on, 2}
    ];
behaviour_info(_Other) ->
    undefined.

%================================================================================
% API
%================================================================================


%================================================================================
% generic framework
%================================================================================
% Options:
% register, InstanceId, Name
% profile
-spec(start_link/2 :: (any(), list()) -> {ok, pid()}).
start_link(Module, Args) ->
    start_link(Module, Args, []).

-spec(start_link/3 :: (any(), list(), list()) -> {ok, pid()}).
start_link(Module, Args, Options) ->
    Pid = spawn_link(?MODULE, start, [Module, Args, Options, self()]),
    receive
	{started, Pid} ->
	    {ok, Pid}
    end.
    

start(Module, Args, Options, Supervisor) ->
    case lists:keysearch(register, 1, Options) of
	{value, {register, InstanceId, Name}} ->
	    process_dictionary:register_process(InstanceId, Name, self()),
	    Supervisor ! {started, self()};
	false ->
	    Supervisor ! {started, self()},
	    ok
    end,
    InitialState = Module:init(Args),
    case lists:member(profile, Options) of
	true ->
	    loop_profile(Module, InitialState, {Options, 0.0});
	false ->
	    loop(Module, InitialState, {Options, 0.0})
    end.


loop_profile(Module, State, {Options, Slowest} = _ComponentState) ->
    receive
	Message ->
	    %io:format("~p ~p ~n", [Message, State]),
	    Start = erlang:now(),
	    case fprof:apply(Module, on, [Message, State]) of
		unknown_event ->
		    {NewState, NewComponentState} = 
			handle_unknown_event(Message, State, 
					     {Options, Slowest}),
		    loop(Module, NewState, NewComponentState);
		kill ->
		    ok;
		NewState ->
		    Stop = erlang:now(),
		    case timer:now_diff(Stop, Start) > 30000 of
			true ->
			    io:format("~p:~p~n", [timer:now_diff(Stop, Start), Message]),
			    fprof:profile(),
			    fprof:analyse(),
			    ok;
			false ->
			    ok
		    end,
		    loop_profile(Module, NewState, {Options, Slowest})
	    end
    end.

loop(Module, State, {Options, Slowest} = _ComponentState) ->
    receive
	Message ->
	    case (try Module:on(Message, State) catch
                                                    throw:Term -> {exception, Term};
                                                    exit:Reason -> {exception,Reason};
                                                    error:Reason -> {exception, {Reason,
                                                                                 erlang:get_stacktrace()}}
                                                end) of
                {exception, Exception} ->
                    io:format("Error: exception ~p during handling of ~p in module ~p~n",
                              [Exception, Message, Module]),
		    loop(Module, State, {Options, Slowest});
		unknown_event ->
		    {NewState, NewComponentState} =
			handle_unknown_event(Message, State,
					     {Options, Slowest}),
		    loop(Module, NewState, NewComponentState);
		kill ->
		    ok;
		NewState ->
		    loop(Module, NewState, {Options, Slowest})
	    end
    end.

handle_unknown_event(UnknownMessage, State, ComponentState) ->
    io:format("unknown message: ~p~n", [UnknownMessage]),
    {State, ComponentState}.
