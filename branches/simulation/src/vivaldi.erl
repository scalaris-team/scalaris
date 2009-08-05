%  Copyright 2007-2008 Konrad-Zuse-Zentrum f�r Informationstechnik Berlin
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
%%% File    : vivaldi.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : vivaldi is a network coordinate system
%%%
%%% Created :  8 July 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum f�r Informationstechnik Berlin
%% @version $Id$
%% @reference Frank Dabek, Russ Cox, Frans Kaahoek, Robert Morris. <em>
%% Vivaldi: A Decentralized Network Coordinate System</em>. SigComm 2004.
%% @reference Jonathan Ledlie, Peter Pietzuch, Margo Seltzer. <em>Stable
%% and Accurate Network Coordinates</em>. ICDCS 2006.
-module(vivaldi).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-export([start_link/1]).

-export([on/2, init/1]).

% vivaldi types
-type(network_coordinate() :: [float()]).

-type(error() :: float()).


-type(latency() :: float()).

% state of the vivaldi loop
-type(state() :: {network_coordinate(), error()}).


% accepted messages of vivaldi processes
-type(message() :: any()).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% start new vivaldi shuffle
%% @doc message handler
-spec(on/2 :: (Message::message(), State::state()) -> state()).
on({start_vivaldi_shuffle}, State) ->
    %io:format("{start_vivaldi_shuffle}: ~p~n", [get_local_cyclon_pid()]),
    erlang:send_after(config:read(vivaldi_interval), self(), {start_vivaldi_shuffle}),
    case get_local_cyclon_pid() of
        failed ->
            ok;
        CyclonPid ->
            cs_send:send_local(CyclonPid,{get_subset, 1, self()})
    end,
    State;

% got random node from cyclon
on({cache, Cache}, {Coordinate, Confidence} = State) ->
    case Cache of
        [] ->
            State;
        [Node] ->
            cs_send:send_to_group_member(node:pidX(Node), vivaldi, {vivaldi_shuffle,
                                                                    cs_send:this(),
                                                                    Coordinate,
                                                                    Confidence}),
            State
    end;

%
on({vivaldi_shuffle, RemoteNode, RemoteCoordinate, RemoteConfidence},
   {Coordinate, Confidence} = State) ->
    %io:format("{shuffle, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    cs_send:send(RemoteNode, {vivaldi_shuffle_reply,
                              cs_send:this(),
                              Coordinate,
                              Confidence}),
    vivaldi_latency:measure_latency(RemoteNode, RemoteCoordinate, RemoteConfidence),
    State;

on({vivaldi_shuffle_reply, _RemoteNode, _RemoteCoordinate, _RemoteConfidence}, State) ->
    %io:format("{shuffle_reply, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    %vivaldi_latency:measure_latency(RemoteNode, RemoteCoordinate, RemoteConfidence),
    State;

on({update_vivaldi_coordinate, Latency, {RemoteCoordinate, RemoteConfidence}},
   {Coordinate, Confidence}) ->
    %io:format("latency is ~pus~n", [Latency]),
    update_coordinate(RemoteCoordinate,
                      RemoteConfidence,
                      Latency,
                      Coordinate,
                      Confidence);

on({ping, Pid}, State) ->
    %log:log(info, "ping ~p", [Pid]),
    cs_send:send(Pid, {pong, cs_send:this()}),
    State;

on(_, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(init/1 :: ([any()]) -> vivaldi:state()).
init([_InstanceId, []]) ->
    %io:format("vivaldi start ~n"),
    cs_send:send_local(self(),{start_vivaldi_shuffle}),
    {random_coordinate(), 1.0}.

%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    gen_component:start_link(?MODULE, [InstanceId, []], [{register, InstanceId, vivaldi}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_local_cyclon_pid() ->
    InstanceId = erlang:get(instance_id),
    if
        InstanceId == undefined ->
            log:log(error,"[ Node ] ~p", [util:get_stacktrace()]);
        true ->
            ok
    end,
    process_dictionary:lookup_process(InstanceId, cyclon).

-spec(random_coordinate/0 :: () -> network_coordinate()).
random_coordinate() ->
    % @TODO
    Dim = config:read(vivaldi_dimensions, 2),
    lists:map(fun(_) -> crypto:rand_uniform(1, 10) end, lists:seq(1, Dim)).

-spec(update_coordinate/5 :: (network_coordinate(), error(), latency(),
                              network_coordinate(), error()) -> vivaldi:state()).

update_coordinate(_RemoteCoordinate, _RemoteError, _Latency, Coordinate, Error) ->
    Cc = 0.5, Ce = 0.5,
    % sample weight balances local and remote error
    W = Error/(Error + _RemoteError),
    % relative error of sample
    Es = abs(mathlib:euclideanDist(_RemoteCoordinate, Coordinate) - _Latency) / _Latency,
    % update weighted moving average of local error
    Error1 = Es * Ce * W + Error * (1 - Ce * W),
    % update local coordinates
    Delta = Cc * W,
    %io:format('expected latency: ~p~n', [mathlib:euclideanDist(Coordinate, _RemoteCoordinate)]),
    C1 = mathlib:u(Coordinate, _RemoteCoordinate),
    C2 = mathlib:euclideanDist(Coordinate, _RemoteCoordinate),
    C3 = _Latency - C2,
    C4 = C3 * Delta,
    Coordinate1 = mathlib:vecAdd(Coordinate, mathlib:vecMult(C1, C4)),
    %io:format("new coordinate ~p and error ~p~n", [Coordinate1, Error1]),
    {Coordinate1, Error1}.
