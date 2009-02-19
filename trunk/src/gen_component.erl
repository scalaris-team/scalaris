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

-export([start_link/2, start_link/3, start/3]).

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
start_link(Module, Args) ->
    {ok, spawn_link(?MODULE, start, [Module, Args, []])}.

start_link(Module, Args, Options) ->
    {ok, spawn_link(?MODULE, start, [Module, Args, Options])}.

start(Module, Args, Options) ->
    InitialState = Module:init(Args),
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
		{T, NewState} ->
		    case T > Slowest of
			true ->
			    %io:format("slowest message: ~p ~p~n", [T, Message]),
			    ok;
			false ->
			    ok
		    end,
		    loop(Module, NewState, {Options, util:max(Slowest, T)})
	    end
    end.

handle_unknown_event(UnknownMessage, State, ComponentState) ->
    io:format("unknown message: ~p~n", [UnknownMessage]),
    {State, ComponentState}.
    

    
