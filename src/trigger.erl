%  @copyright 2010-2012 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Generic trigger for (parameterized) modules.
%%
%% Can be used by a module <code>Module</code> in order to get a configurable
%% message (by default <code>{trigger}</code>) in intervals defined by a given
%% trigger.
%% The basic pattern for the use of this module is as follows:
%% <p><code>
%%  TriggerState = trigger:init(Trigger, TriggerDelay),<br />
%%  TriggerState2 = trigger:now(TriggerState)
%% </code></p>
%% Then on each received <code>{trigger}</code> message, the trigger needs to
%% be told to issue another <code>{trigger}</code> message with one of the
%% following calls:
%% <p><code>
%%  NewTriggerState1 = trigger:next(TriggerState),
%%  NewTriggerState1 = trigger:next(TriggerState, base_interval),
%% </code></p>
%% @version $Id$
-module(trigger).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([init/2, init/3, init/4, init/5, now/1, next/2, next/1, stop/1]).

-ifdef(with_export_type_support).
-export_type([interval/0, interval_time/0, state/0]).
-endif.

-type interval() :: max_interval | base_interval | min_interval.
-type interval_time() :: pos_integer().
-opaque state() :: {TriggerModule::module(), TriggerState::term()}.

%% @doc Initializes the given trigger with the given base interval function
%%      (also used for min and max interval).
-spec init(Trigger::module(), Module_or_BaseInterval :: interval_time()) -> state().
init(Trigger, BaseInterval) when is_integer(BaseInterval) ->
    init(Trigger, BaseInterval, trigger).

%% @doc Initializes the given trigger with the given base interval function
%%      (also used for min and max interval) and the given message tag used for
%%      the trigger message.
-spec init(Trigger::module(), BaseInterval::interval_time(), MsgTag::comm:msg_tag()) -> state().
init(Trigger, BaseInterval, MsgTag) when is_integer(BaseInterval) ->
    {Trigger, Trigger:init(BaseInterval, BaseInterval, BaseInterval, MsgTag)}.

%% @doc Initializes the trigger with the given interval functions.
-spec init(Trigger::module(), BaseInterval::interval_time(), MinInterval::interval_time(), MaxInterval::interval_time()) -> state().
init(Trigger, BaseInterval, MinInterval, MaxInterval)
  when is_integer(BaseInterval) and
           is_integer(MinInterval) and
           is_integer(MaxInterval) ->
    {Trigger, Trigger:init(BaseInterval, MinInterval, MaxInterval, trigger)}.

%% @doc Initializes the trigger with the given interval functions and the given
%%      message tag.
-spec init(Trigger::module(), BaseInterval::interval_time(), MinInterval::interval_time(), MaxInterval::interval_time(), MsgTag::comm:msg_tag()) -> state().
init(Trigger, BaseInterval, MinInterval, MaxInterval, MsgTag)
  when is_integer(BaseInterval) and
           is_integer(MinInterval) and
           is_integer(MaxInterval) ->
    {Trigger, Trigger:init(BaseInterval, MinInterval, MaxInterval, MsgTag)}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization. Any previous trigger will be canceled!
-spec now(state()) -> state().
now({Trigger, TriggerState}) ->
    {Trigger, Trigger:now(TriggerState)}.

%% @doc Sets the trigger to send its message after BaseInterval
%%      milliseconds. Any previous trigger will be canceled!
-spec next(state()) -> state().
next(State) ->
    next(State, base_interval).

%% @doc Sets the trigger to send its message after the given interval's number
%%      of milliseconds. Any previous trigger will be canceled!
-spec next(state(), IntervalTag::interval()) -> state().
next({Trigger, TriggerState}, IntervalTag) ->
    {Trigger, Trigger:next(TriggerState, IntervalTag)}.

%% @doc Stops the trigger until next or now are called again.
-spec stop(state()) -> state().
stop({Trigger, TriggerState}) ->
    {Trigger, Trigger:stop(TriggerState)}.
