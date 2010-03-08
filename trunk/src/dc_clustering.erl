%  Copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : dc-clustering.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created :  26 August 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @author Marie Hoffmann <hoffmann@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
%% @reference T. Schuett, A. Reinefeld,F. Schintke, M. Hoffmann.
%% Gossip-based Topology Inference for Efficient Overlay Mapping on Data Centers.
%% 9th Int. Conf. on Peer-to-Peer Computing Seattle, Sept. 2009.
-module(dc_clustering).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-export([start_link/1]).

-export([on/2, init/1]).

-type(relative_size() :: float()).
-type(centroid() :: vivaldi:network_coordinate()).
-type(centroids() :: [centroid()]).
-type(sizes() :: [relative_size()]).

% state of the clustering loop
-type(state() :: {centroids(), sizes()}).

% accepted messages of cluistering process
-type(message() :: any()).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% start new clustering shuffle
%% @doc message handler
-spec(on/2 :: (Message::message(), State::state()) -> state()).
on({start_clustering_shuffle}, State) ->
    %io:format("~p~n", [State]),
    erlang:send_after(config:read(dc_clustering_interval), self(),
                      {start_clustering_shuffle}),
    case get_local_cyclon_pid() of
        failed ->
            ok;
        CyclonPid ->
            cs_send:send_local(CyclonPid,{get_subset, 1, self()})
    end,
    State;

% ask vivaldi for network coordinate
on({reset_clustering}, State) ->
    erlang:send_after(config:read(dc_clustering_reset_interval), self(), {reset_clustering}),
    cs_send:send_local(get_local_vivaldi_pid(), {query_vivaldi, cs_send:this()}),
    State;

% reset the local state
on({query_vivaldi_response, Coordinate, _Confidence}, _State) ->
    {[Coordinate], [1.0]};

% got random node from cyclon
on({cache, Cache}, {Centroids, Sizes} = State) ->
    %io:format("~p~n",[Cache]),
    case Cache of
        [] ->
            State;
        [Node] ->
            cs_send:send_to_group_member(node:pidX(Node), dc_clustering, {clustering_shuffle,
                                                                          cs_send:this(),
                                                                          Centroids, Sizes}),
            State
    end;

% have been ask to shuffle
on({clustering_shuffle, RemoteNode, RemoteCentroids, RemoteSizes},
   {Centroids, Sizes}) ->
   %io:format("{shuffle, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    cs_send:send(RemoteNode, {clustering_shuffle_reply,
                              cs_send:this(),
                              Centroids, Sizes}),
    NewState = cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes),
    NewState;

% got shuffle response
on({clustering_shuffle_reply, _RemoteNode, RemoteCentroids, RemoteSizes},
   {Centroids, Sizes}) ->
    %io:format("{shuffle_reply, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    %vivaldi_latency:measure_latency(RemoteNode, RemoteCoordinate, RemoteConfidence),
    NewState = cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes),
    NewState;

% return my clusters
on({query_clustering, Pid}, State) ->
    cs_send:send(Pid,{query_clustering_response, State}),
    State;

on(_, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(init/1 :: ([any()]) -> state()).
init([_InstanceId, []]) ->
    erlang:send_after(config:read(dc_clustering_reset_interval), self(), {reset_clustering}),
    cs_send:send_local(self(),{reset_clustering}),
    cs_send:send_local(self(),{start_clustering_shuffle}),
    {[], []}.

-spec(start_link(term()) -> {ok, pid()} | ignore).
start_link(InstanceId) ->
    case config:read(dc_clustering_enable) of
        true ->
            gen_component:start_link(?MODULE, [InstanceId, []], [{register, InstanceId, dc_clustering}]);
        false ->
            ignore
    end.

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

get_local_vivaldi_pid() ->
    InstanceId = erlang:get(instance_id),
    if
        InstanceId == undefined ->
            log:log(error,"[ Node ] ~p", [util:get_stacktrace()]);
        true ->
            ok
    end,
    process_dictionary:lookup_process(InstanceId, vivaldi).

-spec(cluster/4 :: (centroids(), sizes(), centroids(), sizes()) -> {centroids(), sizes()}).
cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes) ->
    Radius = config:read(dc_clustering_radius),
    {NewCentroids, NewSizes} = mathlib:aggloClustering(Centroids ++ RemoteCentroids,
                                                       Sizes ++ RemoteSizes, Radius),
    NormalizedSizes = lists:map(fun (S) -> 0.5*S end, NewSizes),
    {NewCentroids, NormalizedSizes}.
