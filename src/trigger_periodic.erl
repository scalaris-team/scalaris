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
%%% Can be used by a module <code>Module</code> in order to get a configurable
%%% message (by default <code>{trigger}</code>) every
%%% <code>BaseIntervalFun()</code> (default: <code>Module:get_base_interval()</code>)
%%% milliseconds.
%%% 
%%% Use this module through the interface provided by the trigger module,
%%% initializing it with trigger_periodic!
%%% @end
%%% Created :  2 Oct 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(trigger_periodic).

-author('hennig@zib.de').
-vsn('$Id$ ').

-behaviour(gen_trigger).

-include("../include/scalaris.hrl").

-export([init/4, first/2, next/2]).

-type interval_fun() :: trigger:interval_fun().
-type message_tag() :: cs_send:message_tag().
-type state() :: {interval_fun(), message_tag(), reference() | ok}.

%% @doc Initializes the trigger with the given interval functions and the given
%%      message tag used for the trigger message.
-spec init(BaseIntervalFun::interval_fun(), MinIntervalFun::interval_fun(), MaxIntervalFun::interval_fun(), message_tag()) -> {interval_fun(), message_tag(), ok}.
init(BaseIntervalFun, _MinIntervalFun, _MaxIntervalFun, MsgTag) when is_function(BaseIntervalFun, 0) ->
    {BaseIntervalFun, MsgTag, ok}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization.
-spec first(state(), any()) -> {interval_fun(), message_tag(), reference()}.
first({BaseIntervalFun, MsgTag, ok}, _U) ->
    TimerRef = cs_send:send_local_after(0, self(), {MsgTag}),
    {BaseIntervalFun, MsgTag, TimerRef}.

%% @doc Sets the trigger to send its message after BaseIntervalFun()
%%      milliseconds.
-spec next(state(), any()) -> {interval_fun(), message_tag(), reference()}.
next({BaseIntervalFun, MsgTag, ok}, _U) ->
    NewTimerRef = cs_send:send_local_after(BaseIntervalFun(), self(), {MsgTag}),
    {BaseIntervalFun, MsgTag, NewTimerRef};

next({BaseIntervalFun, MsgTag, TimerRef}, _U) ->
    % timer still running?
    case erlang:read_timer(TimerRef) of
        false ->
            NewTimerRef = cs_send:send_local_after(BaseIntervalFun(), self(), {MsgTag});
        _T ->
            NewTimerRef = TimerRef
    end,
    {BaseIntervalFun, MsgTag, NewTimerRef}.
