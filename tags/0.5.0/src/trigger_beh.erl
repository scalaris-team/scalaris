% @copyright 2009-2012 Zuse Institute Berlin

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
%% @doc trigger behaviour
%% @version $Id$
-module(trigger_beh).
-author('hennig@zib.de').
-vsn('$Id$').

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-type state() :: term().

-callback init(BaseIntervalFun::trigger:interval_fun(), MinIntervalFun::trigger:interval_fun(),
               MaxIntervalFun::trigger:interval_fun(), comm:msg_tag()) -> state().
-callback now(state()) -> state().
-callback next(state(), IntervalTag::trigger:interval()) -> state().
-callback stop(state()) -> state().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init, 4},
     {now, 1},
     {next, 2},
     {stop, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
