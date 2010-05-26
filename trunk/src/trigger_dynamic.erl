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
%%% Can be used by a module <code>Module</code> in order to get a configurable
%%% message (by default <code>{trigger}</code>) every
%%% <code>BaseIntervalFun()</code> (default: <code>Module:get_base_interval()</code>),
%%% <code>MinIntervalFun()</code> (default: <code>Module:get_min_interval()</code>),
%%% <code>0</code> and <code>MinIntervalFun()</code> or
%%% <code>MaxIntervalFun()</code> (default: <code>Module:get_max_interval()</code>),
%%% milliseconds depending on a user-provided function (note: this is not fully
%%% implemented yet).
%%% 
%%% Use this module through the interface provided by the trigger module,
%%% initializing it with trigger_periodic!
%%% @end
%%% Created :  2 Oct 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(trigger_dynamic).

-author('hennig@zib.de').
-vsn('$Id$').

-behaviour(trigger_beh).

-include("scalaris.hrl").

-export([init/4, first/1, next/2]).

-type interval() :: trigger:interval().
-type interval_fun() :: trigger:interval_fun().
-type message_tag() :: cs_send:message_tag().
-type state() :: {interval_fun(), interval_fun(), interval_fun(), message_tag(), reference() | ok}.

%% @doc Initializes the trigger with the given interval functions and the given
%%      message tag used for the trigger message.
-spec init(BaseIntervalFun::interval_fun(), MinIntervalFun::interval_fun(), MaxIntervalFun::interval_fun(), message_tag()) -> {interval_fun(), interval_fun(), interval_fun(), message_tag(), ok}.
init(BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag) when is_function(BaseIntervalFun, 0) ->
    {BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag, ok}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization.
-spec first(state()) -> {interval_fun(), interval_fun(), interval_fun(), message_tag(), reference()}.
first({BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag, ok}) ->
    TimerRef = cs_send:send_local_after(0, self(), {MsgTag}),
    {BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag, TimerRef}.

%% @doc Sets the trigger to send its message after some delay (in milliseconds).
%%      If the trigger has not been called before, BaseIntervalFun()
%%      will be used, otherwise function U will be evaluated in order to decide
%%      whether to use MaxIntervalFun() (return value 0),
%%      MinIntervalFun() (return value 2),
%%      0 (now) and MinIntervalFun() (return value 3) or
%%      BaseIntervalFun() (any other return value) for the delay.
-spec next({interval_fun(), interval_fun(), interval_fun(), message_tag(), ok | reference()}, IntervalTag::interval()) ->
              {interval_fun(), interval_fun(), interval_fun(), message_tag(), reference()}.
next({BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag, ok}, IntervalTag) ->
    NewTimerRef = send_message(IntervalTag, BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag),
    {BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag, NewTimerRef};

next({BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag, TimerRef}, IntervalTag) ->
    % timer still running?
    case erlang:read_timer(TimerRef) of
        false ->
            ok;
        _ ->
            erlang:cancel_timer(TimerRef)
    end,
    NewTimerRef = send_message(IntervalTag, BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag),
    {BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag, NewTimerRef}.

-spec send_message(interval(), interval_fun(), interval_fun(), interval_fun(), message_tag()) -> reference().
send_message(IntervalTag, BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag) ->
    case IntervalTag of
        max_interval ->
            cs_send:send_local_after(MaxIntervalFun(), self(), {MsgTag});
        base_interval ->
            cs_send:send_local_after(BaseIntervalFun(), self(), {MsgTag});
        min_interval ->
            cs_send:send_local_after(MinIntervalFun(), self(), {MsgTag});
        now_and_min_interval ->
            cs_send:send_local(self(), {MsgTag}),
            cs_send:send_local_after(MinIntervalFun(), self(), {MsgTag});
        _ ->
            cs_send:send_local_after(BaseIntervalFun(), self(), {MsgTag})
     end.
