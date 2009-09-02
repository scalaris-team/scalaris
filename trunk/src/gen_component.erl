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

-include("../include/scalaris.hrl").

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([behaviour_info/1]).

-export([start_link/2, start_link/3, start/4, start/2, start/3,wait_for_ok/0]).

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

-spec(start/2 :: (any(), list()) -> {ok, pid()}).
start(Module, Args) ->
    start(Module, Args, []).

-spec(start/3 :: (any(), list(), list()) -> {ok, pid()}).
start(Module, Args, Options) ->
    Pid = spawn(?MODULE, start, [Module, Args, Options, self()]),
    receive
        {started, Pid} ->
            {ok, Pid}
    end.

start(Module, Args, Options, Supervisor) ->
    %io:format("Sarting ~p~n",[Module]),
    case lists:keysearch(register, 1, Options) of
	{value, {register, InstanceId, Name}} ->
            process_dictionary:register_process(InstanceId, Name, self()),
            ?DEBUG_REGISTER(list_to_atom(lists:flatten(io_lib:format("~p_~p",[Module,randoms:getRandomId()]))),self()),
	    Supervisor ! {started, self()};
	false ->
            
            case lists:keysearch(register_native, 1, Options) of
            {value, {register_native,Name}} ->
               
                register(Name, self()),
                Supervisor ! {started, self()};
            false ->
                ?DEBUG_REGISTER(list_to_atom(lists:flatten(io_lib:format("~p_~p",[Module,randoms:getRandomId()]))),self()),
                Supervisor ! {started, self()},
                ok
            end
    end,
    try
        InitialState = Module:init(Args),
        loop(Module, InitialState, {Options, 0.0})
    catch
        throw:Term ->
            log:log(error,"exception in init/loop of ~p: ~p~n", [Module, Term]),
            throw(Term);
        exit:Reason ->
            log:log(error,"exception in init/loop of ~p: ~p~n", [Module, Reason]),
            throw(Reason);
        error:Reason ->
            log:log(error,"exception in init/loop of ~p: ~p~n", [Module, {Reason,
                                                                      erlang:get_stacktrace()}]),
            throw(Reason)
    end.

-ifndef(SIMULATION).
loop(Module, State, {Options, Slowest} = _ComponentState) ->
    receive
        Message ->
            %Start = erlang:now(),
           
            case (try Module:on(Message, State) catch
                                                    throw:Term -> {exception, Term};
                                                    exit:Reason -> {exception,Reason};
                                                    error:Reason -> {exception, {Reason,
                                                    erlang:get_stacktrace()}}
                                                end) of
                {exception, Exception} ->
                    log:log(error,"Error: exception ~p during handling of ~p in module ~p~n",
                              [Exception, Message, Module]),
                    loop(Module, State, {Options, Slowest});
                unknown_event ->
                    {NewState, NewComponentState} =
                        handle_unknown_event(Message, State,
                         {Options, Slowest},Module),
                    loop(Module, NewState, NewComponentState);
                kill ->
                    ok;
                NewState ->
                    %Stop = erlang:now(),
                    %Span = timer:now_diff(Stop, Start),
                    %if
                        %Span > Slowest ->
                            %io:format("slow message ~p (~p)~n", [Message, Span]),
                            %loop(Module, NewState, {Options, Span});
                        %true ->
                            loop(Module, NewState, {Options, Slowest})
                    %end
            end
    end.

% Waiting for finish a  RPC
% Workaround
wait_for_ok() ->
    receive
    {ok} ->
        %io:format("ok is da~n"),
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
            yield(),
            loop(Module, NewState, NewComponentState);
        kill ->
            yield(),
            ok;
        NewState ->
            yield(),
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

release_ok() ->
    scheduler ! {release_ok}.

yield() ->
    scheduler ! {yield}.

-endif.




handle_unknown_event(UnknownMessage, State, ComponentState,Module) ->
   log:log(error,"unknown message: ~p in Module: ~p ~n in State ~p ~n",[UnknownMessage,Module,State]),
    {State, ComponentState}.
