%  @copyright 2009-2012 Zuse Institute Berlin

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
%% @doc    Dynamic trigger for (parameterized) modules.
%%
%% Can be used by a module <code>Module</code> in order to get a configurable
%% message (by default <code>{trigger}</code>) every
%% <code>BaseInterval</code>,
%% <code>MinInterval</code> or
%% <code>MaxInterval</code>
%% milliseconds depending on a user-provided interval tag specified with next/2.
%% 
%% Use this module through the interface provided by the trigger module,
%% initializing it with trigger_dynamic!
%% @version $Id$
-module(trigger_dynamic).
-author('hennig@zib.de').
-vsn('$Id$').

-behaviour(trigger_beh).

-include("scalaris.hrl").

-export([init/4, now/1, next/2, stop/1]).

-type state() :: {BaseInterval::trigger:interval_time(),
                  MinInterval::trigger:interval_time(),
                  MaxInterval::trigger:interval_time(),
                  MsgTag::comm:msg_tag(), TimerRef::ok | reference()}.

%% @doc Initializes the trigger with the given interval functions and the given
%%      message tag used for the trigger message.
-spec init(BaseInterval::trigger:interval_time(), MinInterval::trigger:interval_time(),
           MaxInterval::trigger:interval_time(), comm:msg_tag()) -> state().
init(BaseInterval, MinInterval, MaxInterval, MsgTag) when is_integer(BaseInterval) ->
    {BaseInterval, MinInterval, MaxInterval, MsgTag, ok}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization. Any previous trigger will be canceled!
-spec now(state()) -> state().
now({BaseInterval, MinInterval, MaxInterval, MsgTag, ok}) ->
    TimerRef = comm:send_local(self(), {MsgTag}),
    {BaseInterval, MinInterval, MaxInterval, MsgTag, TimerRef};
now({BaseInterval, MinInterval, MaxInterval, MsgTag, TimerRef}) ->
    % timer still running
    _ = erlang:cancel_timer(TimerRef),
    NewTimerRef = comm:send_local(self(), {MsgTag}),
    {BaseInterval, MinInterval, MaxInterval, MsgTag, NewTimerRef}.

%% @doc Sets the trigger to send its message after some delay. The given
%%      IntervalTag will determine which of the three interval functions will
%%      be evaluated in order to get the number of milliseconds of this delay.
%%      Any previous trigger will be canceled!
-spec next(state(), IntervalTag::trigger:interval()) -> state().
next({BaseInterval, MinInterval, MaxInterval, MsgTag, ok}, IntervalTag) ->
    NewTimerRef = send_message(IntervalTag, BaseInterval, MinInterval, MaxInterval, MsgTag),
    {BaseInterval, MinInterval, MaxInterval, MsgTag, NewTimerRef};
next({BaseInterval, MinInterval, MaxInterval, MsgTag, TimerRef}, IntervalTag) ->
    % timer still running?
    _ = erlang:cancel_timer(TimerRef),
    NewTimerRef = send_message(IntervalTag, BaseInterval, MinInterval, MaxInterval, MsgTag),
    {BaseInterval, MinInterval, MaxInterval, MsgTag, NewTimerRef}.

-spec send_message(IntervalTag::trigger:interval(),
                   BaseInterval::trigger:interval_time(),
                   MinInterval::trigger:interval_time(),
                   MaxInterval::trigger:interval_time(),
                   MsgTag::comm:msg_tag()) -> reference().
send_message(IntervalTag, BaseInterval, MinInterval, MaxInterval, MsgTag) ->
    case IntervalTag of
        max_interval ->
            comm:send_local_after(MaxInterval, self(), {MsgTag});
        base_interval ->
            comm:send_local_after(BaseInterval, self(), {MsgTag});
        min_interval ->
            comm:send_local_after(MinInterval, self(), {MsgTag});
        _ ->
            comm:send_local_after(BaseInterval, self(), {MsgTag})
     end.

%% @doc Stops the trigger until next or now are called again.
-spec stop(state()) -> state().
stop({_BaseInterval, _MinInterval, _MaxInterval, _MsgTag, ok} = State) ->
    State;
stop({BaseInterval, MinInterval, MaxInterval, MsgTag, TimerRef}) ->
    % timer still running?
    _ = erlang:cancel_timer(TimerRef),
    {BaseInterval, MinInterval, MaxInterval, MsgTag, ok}.
