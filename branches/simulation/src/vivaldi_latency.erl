%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%% @copyright 2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(vivaldi_latency).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-export([measure_latency/3]).

-export([on/2, init/1]).

% vivaldi types
-type(latency() :: float()).

% state of the vivaldi loop
-type(state() :: {cs_send:mypid(),
                  cs_send:mypid(),
                  any(),
                  {integer(), integer(), integer()},
                  [latency()]}).


% accepted messages of vivaldi_latency processes
-type(message() :: any()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec(on/2 :: (Message::message(), State::state()) -> state()).
on({pong, _}, {Owner, RemoteNode, Token, Start, Latencies}) ->
    Stop = erlang:now(),
    NewLatencies = [timer:now_diff(Stop, Start)| Latencies],
    case length(NewLatencies) == config:read(vivaldi_count_measurements, 4) of
        true ->
            Owner ! {update_vivaldi_coordinate, calc_latency(NewLatencies), Token},
            exit;
        false ->
            erlang:send_after(config:read(vivaldi_measurements_delay),
                              self(),
                              {start_ping}),
            {Owner, RemoteNode, Token, 0, NewLatencies}
    end;

on({start_ping}, {Owner, RemoteNode, Token, _, Latencies}) ->
    cs_send:send(RemoteNode, {ping, cs_send:this()}),
    {Owner, RemoteNode, Token, erlang:now(), Latencies};

on({shutdown}, _State) ->
    log:log(info, "shutdown vivaldi_latency", []),
    exit;

on(_, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(init/1 :: ([any()]) -> vivaldi:state()).
init([Owner, RemoteNode, Token]) ->
    %io:format("vivaldi_latency start ~n"),
    erlang:send_after(config:read(vivaldi_latency_timeout, 60*1000),
                      self(),
                      {shutdown}),
    self() ! {start_ping},
    {Owner, RemoteNode, Token, 0, []}.

-spec(start/3 :: (pid(), any(), any()) -> {ok, pid()}).
start(Owner, RemoteNode, Token) ->
    gen_component:start(?MODULE, [Owner, RemoteNode, Token], []).

measure_latency(RemoteNode, RemoteCoordinate, RemoteConfidence) ->
    start(self(), RemoteNode, {RemoteCoordinate, RemoteConfidence}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
calc_latency(Latencies) ->
    mathlib:median(Latencies).
