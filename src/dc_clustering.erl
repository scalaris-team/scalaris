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
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).

-export([on/2, init/1]).

-type(relative_size() :: float()).
-type(centroid() :: vivaldi:network_coordinate()).
-type(centroids() :: [centroid()]).
-type(sizes() :: [relative_size()]).

% state of the clustering loop
-type(state() :: {centroids(), sizes(), trigger:state(), trigger:state()}).

% accepted messages of cluistering process
-type(message() :: 
    {start_clustering_shuffle} |
    {reset_clustering} |
    {vivaldi_get_coordinate_response, vivaldi:network_coordinate(), vivaldi:error()} |
    {cy_cache, [node:node_type()]} |
    {clustering_shuffle, cs_send:mypid(), centroids(), sizes()} |
    {clustering_shuffle_reply, cs_send:mypid(), centroids(), sizes()} |
    {query_clustering, cs_send:mypid()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the dc_clustering process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(instanceid()) -> {ok, pid()} | ignore.
start_link(InstanceId) ->
    case config:read(dc_clustering_enable) of
        true ->
            ResetTrigger = config:read(dc_clustering_reset_trigger),
            ClusterTrigger = config:read(dc_clustering_cluster_trigger),
            gen_component:start_link(?MODULE, {ResetTrigger, ClusterTrigger}, [{register, InstanceId, dc_clustering}]);
        false ->
            ignore
    end.

%% @doc Initialises the module with an empty state.
-spec init({module(), module()}) -> state().
init({ResetTrigger, ClusterTrigger}) ->
    ResetTriggerState = trigger:init(ResetTrigger, fun get_clustering_reset_interval/0, reset_clustering),
    ResetTriggerState2 = trigger:first(ResetTriggerState),
    ClusterTriggerState = trigger:init(ClusterTrigger, fun get_clustering_interval/0, start_clustering_shuffle),
    ClusterTriggerState2 = trigger:first(ClusterTriggerState),
    log:log(info,"dc_clustering spawn: ~p~n", [cs_send:this()]),
    {[], [], ResetTriggerState2, ClusterTriggerState2}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% start new clustering shuffle
%% @doc message handler
-spec(on/2 :: (Message::message(), State::state()) -> state()).
on({start_clustering_shuffle},
   {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
    %io:format("~p~n", [State]),
    NewClusterTriggerState = trigger:next(ClusterTriggerState),
    cyclon:get_subset_rand(1),
    {Centroids, Sizes, ResetTriggerState, NewClusterTriggerState};

% ask vivaldi for network coordinate
on({reset_clustering},
   {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
    NewResetTriggerState = trigger:next(ResetTriggerState),
    vivaldi:get_coordinate(),
    {Centroids, Sizes, NewResetTriggerState, ClusterTriggerState};

% reset the local state
on({vivaldi_get_coordinate_response, Coordinate, _Confidence},
   {_Centroids, _Sizes, ResetTriggerState, ClusterTriggerState}) ->
    {[Coordinate], [1.0], ResetTriggerState, ClusterTriggerState};

on({cy_cache, []}, State)  ->
    % ignore empty cache from cyclon
    State;

% got random node from cyclon
on({cy_cache, [Node] = _Cache},
   {Centroids, Sizes, _ResetTriggerState, _ClusterTriggerState} = State) ->
    %io:format("~p~n",[_Cache]),
    cs_send:send_to_group_member(node:pidX(Node), dc_clustering,
                                 {clustering_shuffle, cs_send:this(),
                                  Centroids, Sizes}),
    State;

% have been asked to shuffle
on({clustering_shuffle, RemoteNode, RemoteCentroids, RemoteSizes},
   {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
   %io:format("{shuffle, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    cs_send:send(RemoteNode, {clustering_shuffle_reply,
                              cs_send:this(),
                              Centroids, Sizes}),
    {NewCentroids, NewSizes} = cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes),
    {NewCentroids, NewSizes, ResetTriggerState, ClusterTriggerState};

% got shuffle response
on({clustering_shuffle_reply, _RemoteNode, RemoteCentroids, RemoteSizes},
   {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
    %io:format("{shuffle_reply, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    %vivaldi_latency:measure_latency(RemoteNode, RemoteCoordinate, RemoteConfidence),
    {NewCentroids, NewSizes} = cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes),
    {NewCentroids, NewSizes, ResetTriggerState, ClusterTriggerState};

% return my clusters
on({query_clustering, Pid}, {Centroids, Sizes, _ResetTriggerState, _ClusterTriggerState} = State) ->
    cs_send:send(Pid, {query_clustering_response, {Centroids, Sizes}}),
    State;

on(_, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(cluster/4 :: (centroids(), sizes(), centroids(), sizes()) -> {centroids(), sizes()}).
cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes) ->
    Radius = config:read(dc_clustering_radius),
    {NewCentroids, NewSizes} = mathlib:aggloClustering(Centroids ++ RemoteCentroids,
                                                       Sizes ++ RemoteSizes, Radius),
    NormalizedSizes = lists:map(fun (S) -> 0.5 * S end, NewSizes),
    {NewCentroids, NormalizedSizes}.

%% @doc Gets the clustering reset interval set in scalaris.cfg.
-spec get_clustering_reset_interval() -> pos_integer().
get_clustering_reset_interval() ->
    config:read(dc_clustering_reset_interval).

%% @doc Gets the clustering interval, e.g. how often to calculate clusters, set
%%      in scalaris.cfg.
-spec get_clustering_interval() -> pos_integer().
get_clustering_interval() ->
    config:read(dc_clustering_interval).
