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

%-define(REALTIME, true). % TCP communication
%-define(BUILTIN, true). 
-define(SIMULATION, true).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([behaviour_info/1]).

-export([start_link/2, start_link/3, start/4, wait_for_ok/0]).

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
    %io:format("Sarting ~p~n",[Module]),
    case lists:keysearch(register, 1, Options) of
	{value, {register, InstanceId, Name}} ->
	    process_dictionary:register_process(InstanceId, Name, self()),
	    Supervisor ! {started, self()};
	false ->
            case lists:keysearch(register_native, 1, Options) of
            {value, {register_native,Name}} ->
                register(Name, self()),
                Supervisor ! {started, self()};
            false ->
                Supervisor ! {started, self()},
                ok
            end
    end,
    InitialState = Module:init(Args),
    
   
        loop(Module, InitialState, {Options, 0.0}).



-ifdef(REALTIME).
loop(Module, State, {Options, Slowest} = _ComponentState) ->
    receive
    Message ->
        %io:format("~p ~p ~n", [Message, Module]),
	    case Module:on(Message, State) of
		unknown_event ->
		    {NewState, NewComponentState} = 
			handle_unknown_event(Message, State, 
					     {Options, Slowest},Module),
		    loop(Module, NewState, NewComponentState);
		kill ->
		    ok;
		NewState ->
		    loop(Module, NewState, {Options, Slowest})
	    end
    end.
wait_for_ok() ->
    receive
    {ok} ->
        ok
    end.
-endif.

-ifdef(SIMULATION).
loop(Module, State, {Options, Slowest} = _ComponentState) ->
    
    receive
    Message ->
        %io:format("~p ~p ~n", [Message, Module]),
        case Module:on(Message, State) of
        unknown_event ->
            {NewState, NewComponentState} = 
            handle_unknown_event(Message, State, 
                         {Options, Slowest},Module),
            unlock(),
            loop(Module, NewState, NewComponentState);
        kill ->
            unlock(),
            ok;
        NewState ->
            unlock(),
            loop(Module, NewState, {Options, Slowest})
        end
    end.
wait_for_ok() ->
    %io:format("Warte auf ok~n"),
    release_ok(),
    receive
    {ok} ->
        %io:format("ok is da~n"),
        ok
    end.


-endif.

release_ok() ->
    scheduler ! {release_ok}.

unlock() ->
    scheduler ! {unlock}.


handle_unknown_event(UnknownMessage, State, ComponentState,Module) ->
   log:log(error,"unknown message: ~p in ~p~n",[UnknownMessage,Module]),
    {State, ComponentState}.
