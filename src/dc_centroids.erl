%  @copyright 2012 Zuse Institute Berlin

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

%% @author Magnus Mueller <mamuelle@informatik.hu-berlin.de>
%% @doc    Centroids data structure for dc_clustering.
%% @end
%% @reference T. Schuett, A. Reinefeld,F. Schintke, M. Hoffmann.
%% Gossip-based Topology Inference for Efficient Overlay Mapping on Data Centers.
%% 9th Int. Conf. on Peer-to-Peer Computing Seattle, Sept. 2009.
%% @version $Id$
-module(dc_centroids).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-export([
        new/2
        , empty_centroids_list/0

        %% getters
        , get_coordinate/1
        , get_relative_size/1
        , get_coordinate_and_relative_size/1

        %% setters
        , set_relative_size/2

        %% helpers
        , distance/2
    ]).

-type(coordinate() :: gossip_vivaldi:network_coordinate()).
-type(relative_size() :: float()).

-record(centroid,
    {
        coordinate = [] :: coordinate()
        , relative_size = 1.0 :: relative_size()
    }).

-type(centroid() :: #centroid{}).
-type(centroids() :: [centroid()]).

-export_type([centroid/0, centroids/0]).

% @doc Create a new centroid.
-spec new(coordinate(), relative_size()) -> centroid().
new(Coordinate, RelativeSize) ->
    #centroid{coordinate=Coordinate,relative_size=RelativeSize}.

% @doc Get centroid's coordinate
-spec get_coordinate(Centroid :: centroid()) -> coordinate().
get_coordinate(#centroid{coordinate=Coordinate}) -> Coordinate.

% @doc Get centroid's relative size
-spec get_relative_size(Centroid :: centroid()) -> relative_size().
get_relative_size(#centroid{relative_size=RelativeSize}) -> RelativeSize.

% @doc Get a centroid's coordinate and relative size as a tuple
-spec get_coordinate_and_relative_size(Centroid :: centroid())
            -> {coordinate(), relative_size()}.
get_coordinate_and_relative_size(#centroid{coordinate=Coordinate, relative_size=RelativeSize}) ->
    {Coordinate, RelativeSize}.

% @doc Set the relative size of a centroid
-spec set_relative_size(Centroid :: centroid(), relative_size()) -> centroid().
set_relative_size(#centroid{coordinate=Coordinate}, RelativeSize) ->
    #centroid{coordinate=Coordinate, relative_size=RelativeSize}.

%% @doc Helper to return an empty list of centroids.
-spec empty_centroids_list() -> centroids().
empty_centroids_list() -> [].

%% @doc Get the distance between two centroids
-spec distance(U :: centroid(), V :: centroid()) -> float().
distance(U,V) ->
    mathlib:euclideanDistance(get_coordinate(U), get_coordinate(V)).

