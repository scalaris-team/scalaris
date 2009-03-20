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
% sync_start
% profile
-spec(start_link/2 :: (any(), list()) -> {ok, pid()}).
start_link(Module, Args) ->
    start_link(Module, Args, []).

-spec(start_link/3 :: (any(), list(), list()) -> {ok, pid()}).
start_link(Module, Args, Options) ->
    case lists:member(sync_start, Options) of
	false ->
	    {ok, spawn_link(?MODULE, start, [Module, Args, Options, self()])};
	true ->
	    Pid = spawn_link(?MODULE, start, [Module, Args, Options, self()]),
	    receive
		{started, Pid} ->
		    {ok, Pid}
	    end
    end.
    

start(Module, Args, Options, Supervisor) ->
    InitialState = Module:init(Args),
    case lists:member(sync_start, Options) of
	true ->
	    Supervisor ! {started, self()};
	false ->
	    ok
    end,
    loop(Module, InitialState, {Options, 0.0}).

loop(Module, State, {Options, Slowest} = _ComponentState) ->
    receive
	Message ->
	    %io:format("~p ~p ~n", [Message, State]),
	    case timer:tc(Module, on, [Message, State]) of
		{T, unknown_event} ->
		    case T > Slowest of
			true ->
			    %io:format("slowest message: ~p ~p~n", [T, Message]),
			    ok;
			false ->
			    ok
		    end,
		    {NewState, NewComponentState} = 
			handle_unknown_event(Message, State, 
					     {Options, util:max(Slowest, T)}),
		    loop(Module, NewState, NewComponentState);
		{_T, kill} ->
		    ok;
		{T, NewState} ->
		    if 
			T > 1000 ->
			    %io:format("slow message: ~p ~p~n", [T / 1000.0, Message]),
			    ok;
			true ->
			    ok
		    end,
		    loop(Module, NewState, {Options, util:max(Slowest, T)})
	    end
    end.

handle_unknown_event(UnknownMessage, State, ComponentState) ->
    io:format("unknown message: ~p~n", [UnknownMessage]),
    {State, ComponentState}.
    

    
