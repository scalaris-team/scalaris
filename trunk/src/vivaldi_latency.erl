%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : vivaldi_latency.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : vivaldi helper module for measuring latency between nodes
%%%
%%% Created :  8 July 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(vivaldi_latency).

-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([on/2, init/1]).

-export([measure_latency/3, check_config/0]).

% state of the vivaldi loop
-type(state() :: {comm:erl_local_pid(),
                  comm:mypid(),
                  {vivaldi:network_coordinate(), vivaldi:error()},
                  {integer(), integer(), integer()} | unknown,
                  [vivaldi:latency()]}).


% accepted messages of vivaldi_latency processes
-type(message() :: {pong} | {start_ping} | {shutdown}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec on(Message::message(), State::state()) -> state().
on({pong}, {Owner, RemotePid, Token, Start, Latencies}) ->
    Stop = erlang:now(),
    NewLatencies = [timer:now_diff(Stop, Start) | Latencies],
    case length(NewLatencies) =:= config:read(vivaldi_count_measurements) of
        true ->
            comm:send_local(Owner, {update_vivaldi_coordinate, calc_latency(NewLatencies), Token}),
            kill;
        false ->
            comm:send_local_after(config:read(vivaldi_measurements_delay),
                              self(), {start_ping}),
            {Owner, RemotePid, Token, unknown, NewLatencies}
    end;

on({start_ping}, {Owner, RemotePid, Token, _, Latencies}) ->
    comm:send(RemotePid, {ping, comm:this()}),
    {Owner, RemotePid, Token, erlang:now(), Latencies};

on({shutdown}, _State) ->
    log:log(info, "shutdown vivaldi_latency due to timeout", []),
    kill;

on(_, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init({comm:erl_local_pid(), comm:mypid(), {vivaldi:network_coordinate(), vivaldi:error()}}) -> state().
init({Owner, RemotePid, Token}) ->
    %io:format("vivaldi_latency start ~n"),
    comm:send_local_after(config:read(vivaldi_latency_timeout), self(), {shutdown}),
    comm:send_local(self(), {start_ping}),
    {Owner, RemotePid, Token, unknown, []}.

-spec measure_latency(comm:mypid(), vivaldi:network_coordinate(), vivaldi:error()) -> {ok, pid()}.
measure_latency(RemotePid, RemoteCoordinate, RemoteConfidence) ->
    gen_component:start(?MODULE, {self(), RemotePid, {RemoteCoordinate, RemoteConfidence}}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec calc_latency([vivaldi:latency(),...]) -> number().
calc_latency(Latencies) ->
    mathlib:median(Latencies).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters of the vivaldi_latency process exist
%%      and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(vivaldi_count_measurements) and
    config:is_greater_than(vivaldi_count_measurements, 0) and
    
    config:is_integer(vivaldi_measurements_delay) and
    config:is_greater_than_equal(vivaldi_measurements_delay, 0) and
    
    config:is_integer(vivaldi_latency_timeout) and
    config:is_greater_than(vivaldi_latency_timeout, 0).
