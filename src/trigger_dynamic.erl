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
%%% File    trigger_dynamic.erl
%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    Dynamic trigger for (parameterized) modules.
%%%
%%% Can be used by a module <code>Module</code> in order to get a
%%% <code>{trigger}</code> message every
%%% <code>Module:get_base_interval()</code>,
%%% <code>Module:get_min_interval()</code>,
%%% <code>0</code> and <code>Module:get_min_interval()</code> or
%%% <code>Module:get_max_interval()</code> milliseconds depending on a
%%% user-provided function (note: this is not fully implemented yet).
%%% For this the module needs to initialize the trigger with an instantiation
%%% of itself (including the trigger) and tell it to send the first message,
%%% e.g. by calling this (and storing the TriggerState2 for later use):
%%% <p><code>
%%%  TriggerState = Trigger:init(?MODULE:new(Trigger)),<br />
%%%  TriggerState2 = Trigger:trigger_first(TriggerState, TriggerFun)
%%% </code></p>
%%% Then on each received <code>{trigger}</code> message, the trigger needs to
%%% be told to issue another <code>{trigger}</code> message:
%%% <p><code>
%%%  NewTriggerState = Trigger:trigger_next(TriggerState, TriggerFun),
%%% </code></p>
%%% @end
%%% Created :  2 Oct 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(trigger_dynamic).

-author('hennig@zib.de').
-vsn('$Id$ ').

-behaviour(trigger).

-export([init/1, trigger_first/2, trigger_next/2]).

-ifdef(types_not_builtin).
-type reference() :: erlang:reference().
-endif.

-type par_module() :: any(). % parameterized module
-type state() :: {par_module(), reference() | ok}.
-type my_fun() :: fun((number(), number()) -> 0..3).

%% @doc Initializes the trigger.
-spec init(par_module()) -> {par_module(), ok}.
init(Module) ->
    {Module, ok}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization.
-spec trigger_first(state(), any()) -> state().
trigger_first({Module, ok}, _U) ->
    TimerRef = cs_send:send_local_after(0, self(), {trigger}),
    {Module, TimerRef}.

%% @doc Sets the trigger to send its message after some delay (in milliseconds).
%%      If the trigger has not been called before, Module:get_base_interval()
%%      will be used, otherwise function U will be evaluated in order to decide
%%      whether to use Module:get_max_interval() (return value 0),
%%      Module:get_min_interval() (return value 2),
%%      0 (now) and Module:get_min_interval() (return value 3) or
%%      Module:get_base_interval() (any other return value) for the delay.
-spec trigger_next({par_module(), ok}, any()) -> state()
                 ; ({par_module(), reference()}, my_fun()) -> state().
trigger_next({Module, ok}, _U) ->
    NewTimerRef = cs_send:send_local_after(Module:get_base_interval(), self(), {trigger}),
    {Module,NewTimerRef};

% 0 - > max
% 1 - > base
% 2 - > min
% 3 - > now,min
trigger_next({Module, TimerRef}, U) ->
    % timer still running?
    case erlang:read_timer(TimerRef) of
        false ->
            ok;
        _ ->
            erlang:cancel_timer(TimerRef)
    end,
    %io:format("[ TD ] ~p U(0,0) ~p~n",[self(),U(0,0)]),
    % TODO: make use of the functions parameters
    NewTimerRef = 
        case U(0, 0) of
            0 ->
                cs_send:send_local_after(Module:get_max_interval(),self(), {trigger});
            1 ->
                cs_send:send_local_after(Module:get_base_interval(),self(), {trigger});
            2 ->
                cs_send:send_local_after(Module:get_min_interval(),self(), {trigger});
            3 ->
                cs_send:send_local(self(), {trigger}),
                cs_send:send_local_after(Module:get_min_interval(), self(), {trigger});
            _ ->
                cs_send:send_local_after(Module:get_base_interval(),self(), {trigger})
     end,
    {Module, NewTimerRef}.
