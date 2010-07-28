%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Christian Hennig <hennig@zib.de>
%% @doc    Periodic trigger for (parameterized) modules.
%%
%% Can be used by a module <code>Module</code> in order to get a configurable
%% message (by default <code>{trigger}</code>) every
%% <code>BaseIntervalFun()</code> (default: <code>Module:get_base_interval()</code>)
%% milliseconds.
%% 
%% Use this module through the interface provided by the trigger module,
%% initializing it with trigger_periodic!
%% @version $Id$
-module(trigger_periodic).
-author('hennig@zib.de').
-vsn('$Id$').

-behaviour(trigger_beh).

-include("scalaris.hrl").

-export([init/4, now/1, next/2, stop/1]).

-opaque state() :: {trigger:interval_fun(), comm:message_tag(), reference() | ok}.

%% @doc Initializes the trigger with the given interval functions and the given
%%      message tag used for the trigger message.
-spec init(BaseIntervalFun::trigger:interval_fun(), MinIntervalFun::trigger:interval_fun(), MaxIntervalFun::trigger:interval_fun(), comm:message_tag()) -> state().
init(BaseIntervalFun, _MinIntervalFun, _MaxIntervalFun, MsgTag) when is_function(BaseIntervalFun, 0) ->
    {BaseIntervalFun, MsgTag, ok}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization.
-spec now(state()) -> state().
now({BaseIntervalFun, MsgTag, TimerRef}) ->
    comm:send_local(self(), {MsgTag}),
    {BaseIntervalFun, MsgTag, TimerRef}.

%% @doc Sets the trigger to send its message after BaseIntervalFun()
%%      milliseconds.
-spec next(state(), IntervalTag::trigger:interval()) -> state().
next({BaseIntervalFun, MsgTag, ok}, _IntervalTag) ->
    NewTimerRef = comm:send_local_after(BaseIntervalFun(), self(), {MsgTag}),
    {BaseIntervalFun, MsgTag, NewTimerRef};

next({BaseIntervalFun, MsgTag, TimerRef} = State, _U) ->
    % timer still running?
    case erlang:read_timer(TimerRef) of
        false ->
            {BaseIntervalFun, MsgTag,
             comm:send_local_after(BaseIntervalFun(), self(), {MsgTag})};
        _T ->
            State
    end.

-spec stop(state()) -> state().
stop({_BaseIntervalFun, _MsgTag, ok} = State) ->
    State;
stop({BaseIntervalFun, MsgTag, TimerRef}) ->
    % timer still running?
    erlang:cancel_timer(TimerRef),
    {BaseIntervalFun, MsgTag, ok}.
