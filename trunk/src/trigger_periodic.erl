%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    trigger_periodic.erl
%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    Periodic trigger for (parameterized) modules.
%%%
%%% Can be used by a module <code>Module</code> in order to get a
%%% <code>{trigger}</code> message every <code>Module:get_base_interval()</code>
%%% milliseconds. For this the module needs to initialize the trigger with an
%%% instantiation of itself (including the trigger) and tell it to send the
%%% first message, e.g. by calling this (and storing the TriggerState2 for later
%%% use):
%%% <p><code>
%%%  TriggerState = Trigger:init(THIS),<br />
%%%  TriggerState2 = Trigger:trigger_first(TriggerState, 1)
%%% </code></p>
%%% Then on each received <code>{trigger}</code> message, the trigger needs to
%%% be told to issue another <code>{trigger}</code> message:
%%% <p><code>
%%%  NewTriggerState = Trigger:trigger_next(TriggerState, 1),
%%% </code></p>
%%% @end
%%% Created :  2 Oct 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(trigger_periodic).

-author('hennig@zib.de').
-vsn('$Id$ ').

-behaviour(trigger).

-export([init/1, trigger_first/2, trigger_next/2]).

-ifdef(types_not_builtin).
-type reference() :: erlang:reference().
-endif.

-type par_module() :: any(). % parameterized module
-type state() :: {par_module(), reference() | ok}.

%% @doc Initializes the trigger.
-spec init(par_module()) -> {par_module(), ok}.
init(Module) ->
    %io:format("[ TR ~p ] ~p init ~n", [self(),Module]),
    {Module, ok}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization.
-spec trigger_first(state(), any()) -> state().
trigger_first({Module, ok}, _U) ->
    %io:format("[ TR ~p ] ~p first ~n", [self(),Module]),
    TimerRef = cs_send:send_local_after(0, self(), {trigger}),
    {Module, TimerRef}.

%% @doc Sets the trigger to send its message after Module:get_base_interval()
%%      milliseconds.
-spec trigger_next(state(), any()) -> state().
trigger_next({Module, ok}, _U) ->
    NewTimerRef = cs_send:send_local_after(Module:get_base_interval(), self(), {trigger}),
    {Module, NewTimerRef};

trigger_next({Module, TimerRef}, _U) ->
    % timer still running?
    case erlang:read_timer(TimerRef) of
        false ->
            %io:format("[ TR ~p ] ~p next ~n", [self(),Module]),
            NewTimerRef = cs_send:send_local_after(Module:get_base_interval(), self(), {trigger});
        _T ->
            %io:format("[ TR ~p ] ~p call next befor Timer Release ~p ms ~n", [self(), Module, T]),
            NewTimerRef = TimerRef
    end,
    {Module, NewTimerRef}.
