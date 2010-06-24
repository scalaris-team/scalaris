%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    trigger.erl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Generic trigger for (parameterized) modules.
%%%
%%% Can be used by a module <code>Module</code> in order to get a configurable
%%% message (by default <code>{trigger}</code>) in intervals defined by a given
%%% trigger.
%%% The basic pattern for the use of this module is as follows:
%%% <p><code>
%%%  TriggerState = trigger:init(Trigger, ?MODULE),<br />
%%%  TriggerState2 = trigger:first(TriggerState)
%%% </code></p>
%%% Then on each received <code>{trigger}</code> message, the trigger needs to
%%% be told to issue another <code>{trigger}</code> message:
%%% <p><code>
%%%  NewTriggerState1 = trigger:next(TriggerState),
%%%  NewTriggerState2 = trigger:next(TriggerState, base_interval),
%%% </code></p>
%%% Note: When parameterized modules are used, trigger:init(Trigger, THIS) does
%%% not work. Use code like the following instead:
%%% <p><code>
%%%  TriggerState = trigger:init(Trigger, fun get_base_interval/0)
%%% </code></p>
%%% @end
%%% Created : 26 Jan 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(trigger).

-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([init/2, init/3, init/4, init/5, first/1, next/2, next/1]).

-ifdef(with_export_type_support).
-export_type([interval/0, interval_fun/0, state/0]).
-endif.

-type interval() :: max_interval | base_interval | min_interval | now_and_min_interval.
-type interval_fun() :: fun(() -> pos_integer()).
-type message_tag() :: comm:message_tag().
-type state() :: {module(), term()}.

%% @doc Initializes the given trigger with the given base interval function
%%      (also used for min and max interval). If a Module is given instead,
%%      the trigger will use the module's get_base_interval(),
%%      get_min_interval() and get_max_interval() functions for the according
%%      intervals.
-spec init(Trigger::module(), Module_or_BaseIntervalFun :: module() | interval_fun()) -> state().
init(Trigger, BaseIntervalFun) when is_function(BaseIntervalFun, 0) ->
    {Trigger, Trigger:init(BaseIntervalFun, BaseIntervalFun, BaseIntervalFun, trigger)};
init(Trigger, Module) ->
    BaseIntervalFun = fun() -> Module:get_base_interval() end,
    MinIntervalFun = fun() -> Module:get_min_interval() end,
    MaxIntervalFun = fun() -> Module:get_max_interval() end,
    {Trigger, Trigger:init(BaseIntervalFun, MinIntervalFun, MaxIntervalFun, trigger)}.

%% @doc Initializes the given trigger with the given base interval function
%%      (also used for min and max interval) and the given message tag used for
%%      the trigger message.
-spec init(Trigger::module(), BaseIntervalFun::interval_fun(), message_tag()) -> state().
init(Trigger, BaseIntervalFun, MsgTag) when is_function(BaseIntervalFun, 0) ->
    {Trigger, Trigger:init(BaseIntervalFun, BaseIntervalFun, BaseIntervalFun, MsgTag)}.

%% @doc Initializes the trigger with the given interval functions.
-spec init(Trigger::module(), BaseIntervalFun::interval_fun(), MinIntervalFun::interval_fun(), MaxIntervalFun::interval_fun()) -> state().
init(Trigger, BaseIntervalFun, MinIntervalFun, MaxIntervalFun)
  when is_function(BaseIntervalFun, 0) and
           is_function(MinIntervalFun, 0) and
           is_function(MaxIntervalFun, 0) ->
    {Trigger, Trigger:init(BaseIntervalFun, MinIntervalFun, MaxIntervalFun, trigger)}.

%% @doc Initializes the trigger with the given interval functions and the given
%%      message tag.
-spec init(Trigger::module(), BaseIntervalFun::interval_fun(), MinIntervalFun::interval_fun(), MaxIntervalFun::interval_fun(), message_tag()) -> state().
init(Trigger, BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag)
  when is_function(BaseIntervalFun, 0) and
           is_function(MinIntervalFun, 0) and
           is_function(MaxIntervalFun, 0) ->
    {Trigger, Trigger:init(BaseIntervalFun, MinIntervalFun, MaxIntervalFun, MsgTag)}.

%% @doc Sets the trigger to send its message immediately, for example after
%%      its initialization.
-spec first(state()) -> state().
first({Trigger, TriggerState}) ->
    {Trigger, Trigger:first(TriggerState)}.

%% @doc Sets the trigger to send its message after BaseIntervalFun()
%%      milliseconds.
-spec next(state()) -> state().
next(State) ->
    next(State, base_interval).

%% @doc Sets the trigger to send its message after the given interval's number
%%      of milliseconds.
-spec next(state(), IntervalTag::interval()) -> state().
next({Trigger, TriggerState}, IntervalTag) ->
    {Trigger, Trigger:next(TriggerState, IntervalTag)}.
