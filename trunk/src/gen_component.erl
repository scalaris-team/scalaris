%  Copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%%------------------------------------------------------------------------------
%%% File    : gen_component.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description :
%%%
%%% Created : 19 Dec 2008 by Thorsten Schuett <schuett@zib.de>
%%%------------------------------------------------------------------------------
%% @doc
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008, 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(gen_component).

-include("../include/scalaris.hrl").

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([behaviour_info/1]).

-export([start_link/2, start_link/3, start/4, start/2, start/3,wait_for_ok/0,
         change_handler/2]).

-export([kill/1, sleep/2, get_state/1]).

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

kill(Pid) ->
    Pid ! {'$gen_component', kill}.

sleep(Pid, Time) ->
    Pid ! {'$gen_component', time, Time}.

get_state(Pid) ->
    Pid ! {'$gen_component', get_state, self()},
    receive
        {'$gen_component', get_state_response, State} ->
            State
    end.

%% @doc change the handler for handling messages
change_handler(State, Handler) when is_atom(Handler) ->
    {'$gen_component', [{on_handler, Handler}], State}.

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
        case Module:init(Args) of
            {'$gen_component', Config, InitialState} ->
                {value, {on_handler, NewHandler}} =
                    lists:keysearch(on_handler, 1, Config),
                loop(Module, NewHandler, InitialState, {Options, 0.0});
            InitialState ->
                loop(Module, on, InitialState, {Options, 0.0})
        end
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

loop(Module, On, State, {Options, Slowest} = ComponentState) ->
    receive
        {'$gen_component', sleep, Time} ->
            timer:sleep(Time),
            loop(Module, On, State, ComponentState);
        {'$gen_component', get_state, Pid} ->
            cs_send:send_local(Pid, {'$gen_component', get_state_response, State}),
            loop(Module, On, State, ComponentState);
        {'$gen_component', kill} ->
            ok;
        % handle failure detector messages
        {ping, Pid} ->
            cs_send:send(Pid, {pong}),
            loop(Module, On, State, ComponentState);
        %% forward a message to group member by its process name
        %% initiated via cs_send:send_to_group_member()
        {send_to_group_member, Processname, Msg} ->
            Pid = process_dictionary:get_group_member(Processname),
            case Pid of
                failed ->
                    ok;
                _ -> cs_send:send_local(Pid , Msg)
            end,
            loop(Module, On, State, ComponentState);
        Message ->
            %Start = erlang:now(),
            case (try Module:On(Message, State) catch
                                                    throw:Term -> {exception, Term};
                                                    exit:Reason -> {exception,Reason};
                                                    error:Reason -> {exception, {Reason,
                                                    erlang:get_stacktrace()}}
                                                end) of
                {exception, Exception} ->
                    log:log(error,"Error: exception ~p during handling of ~p in module ~p in (~p)~n",
                              [Exception, Message, Module, State]),
                    loop(Module, On, State, ComponentState);
                unknown_event ->
                    {NewState, NewComponentState} =
                        handle_unknown_event(Message, State,
                         {Options, Slowest},Module),
                    loop(Module, On, NewState, NewComponentState);
                kill ->
                    ok;
                {'$gen_component', Config, NewState} ->
                    {value, {on_handler, NewHandler}} =
                        lists:keysearch(on_handler, 1, Config),
                    loop(Module, NewHandler, NewState, ComponentState);
                NewState ->
                    %Stop = erlang:now(),
                    %Span = timer:now_diff(Stop, Start),
                    %if
                        %Span > Slowest ->
                            %io:format("slow message ~p (~p)~n", [Message, Span]),
                            %loop(Module, NewState, {Options, Span});
                        %true ->
                            loop(Module, On, NewState, ComponentState)
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

handle_unknown_event(UnknownMessage, State, ComponentState,Module) ->
   log:log(error,"unknown message: ~p in Module: ~p ~n in State ~p ~n",[UnknownMessage,Module,State]),
    {State, ComponentState}.
