% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Monitors timing behaviour of transactions.
%% @version $Id$
-module(monitor_timing).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-export([start_link/1, get_timers/0, log/2,
         on/2, init/1]).

-ifdef(with_export_type_support).
-export_type([timer/0]).
-endif.

-type timer() :: {Timer::atom(), Count::pos_integer(), Min::integer(),
                  Avg::number(), Max::integer()}.

% state of the module
-type(state() :: {}).

% accepted messages the module
-type(message() ::
    {log, Timer::atom(), Time::integer()} |
    {get_timers, From::comm:erl_local_pid()}).

%% @doc log a timespan for a given timer
-spec log(Timer::atom(), Time::integer()) -> ok.
log(Timer, Time) ->
    comm:send_local(?MODULE, {log, Timer, Time}).

%% @doc read the statistics about the known timers
-spec get_timers() -> [timer()].
get_timers() ->
    comm:send_local(?MODULE, {get_timers, self()}),
    receive
        {get_timers_response, Timers} -> Timers
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec on(Message::message(), State::state()) -> state().
on({log, Timer, Time}, State) ->
    case ets:lookup(?MODULE, Timer) of
        [{Timer, Sum, Count, Min, Max}] ->
            ets:insert(?MODULE, {Timer,
                                 Sum + Time,
                                 Count + 1,
                                 erlang:min(Min, Time),
                                 erlang:max(Max, Time)});
        [] ->
            ets:insert(?MODULE, {Timer, Time, 1, Time, Time})
    end,
    State;

on({get_timers, From}, State) ->
    Result = [{Timer, Count, Min, Sum / Count, Max} ||
                 {Timer, Sum, Count, Min, Max} <- ets:tab2list(?MODULE)],
    comm:send_local(From, {get_timers_response, Result}),
    ets:delete_all_objects(?MODULE),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(any()) -> state().
init(_) ->
    _ = ets:new(?MODULE, [set, protected, named_table]),
    {}.

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, [],
                             [{erlang_register, ?MODULE},
                              {pid_groups_join_as, ServiceGroup, ?MODULE}]).
