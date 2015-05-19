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
%% @doc    Test suite for the mathlib module.
%% @end
%% @version $Id$

-module(mathlib_SUITE).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() -> [
        euclidian_distance
        , nearest_centroid
        , closest_points
        , agglomerative_clustering
    ].

suite() ->
    [
        {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.


%% helper functions
-spec in(X::any(), List::[any()]) -> boolean().
in(X, List) ->
    lists:foldl(fun(_, true) -> true;
            (E, false) when E == X -> true;
            (_, _) -> false
        end, false, List)
    .

% Test mathlib:euclideanDistance/2
euclidian_distance(_Config) ->
    {U1, V1, V2, V3, V4} = {
        [0.0,0.0],
        [0, 1.0],
        [1.0, 0],
        [1.0, 1.0],
        [-1.0,-1.0]
    },
    ?equals(mathlib:euclideanDistance(U1, U1), 0.0),
    ?equals(mathlib:euclideanDistance(U1, V1), 1.0),
    ?equals(mathlib:euclideanDistance(U1, V2), 1.0),
    ?equals(mathlib:euclideanDistance(U1, V3), math:sqrt(2)),
    ?equals(mathlib:euclideanDistance(U1, V4), math:sqrt(2)),
    ok.


% Test mathlib:nearestCentroid/2
%
% Testcases:
% - Centroids empty (problemcase)
% - Centroids non-empty and items are unique, U not contained (good case)
% - Centroids contains U again (problemcase)
% - Centroids non-empty, not unique, doesn't contain U (should be ok)
% - Centroids non-empty, not unique, contains U
nearest_centroid(_Config) ->
    % Note: The relative size will be ignored in this unittest, so we set it to zero

    U = dc_centroids:new([0.0, 0.0], 0.0),

    %% ----- Good cases which should work ------

    %% List of Centroids is not empty. U is not an element and only one
    %   element in the list is nearest to U
    C1 = [C1Nearest1, C1Nearest2 | _] = [dc_centroids:new([float(X), float(X)], 0.0)
                                           || X <- lists:seq(1,5)],
    ?equals(mathlib:nearestCentroid(U, C1), {math:sqrt(2), C1Nearest1}),
    ?equals(mathlib:nearestCentroid(U, tl(C1)), {math:sqrt(8), C1Nearest2}),

    %% List not empty, U is an element, only one nearest element
    C2 = [U|C1],
    ?equals(mathlib:nearestCentroid(U, C2), {math:sqrt(2), C1Nearest1}),
    C3 = C1 ++ [U],
    ?equals(mathlib:nearestCentroid(U, C3), {math:sqrt(2), C1Nearest1}),

    %% List contains the same entry multiple times
    C4 = C1 ++ C1 ++ C1,
    ?equals(mathlib:nearestCentroid(U, C4), {math:sqrt(2), C1Nearest1}),

    %% Order of the list should not be important
    RC4 = lists:reverse(C4),
    ?equals(mathlib:nearestCentroid(U, RC4), {math:sqrt(2), C1Nearest1}),

    %% 

    %% ----- Cases that should behave well ------
    % empty centroids: return 'none'
    ?equals(mathlib:nearestCentroid(U, []), none),

    % ambiguous centroids: return a node from a set of nodes
    Ambiguous1 = [dc_centroids:new([float(X), float(Y)], 0.0)
                    || X <- lists:seq(1,5), Y <- lists:seq(1,5)],
    {AmbiguousDistance1, Element} = mathlib:nearestCentroid(U, Ambiguous1),
    ?equals(AmbiguousDistance1, math:sqrt(2)),
    AllowedElements1 = [dc_centroids:new([X, Y], 0.0) || {X, Y} <- [
            {1.0,1.0},{1.0,-1.0},{-1.0,1.0},{-1.0,-1.0}
        ]],
    ?assert_w_note(in(Element, AllowedElements1),
        "Nearest element not in list of allowed coordinates"),

    % regression test
    U2 = dc_centroids:new([0.0, 0.0], 1.0),
    V2 = dc_centroids:new([1.0, 1.0], 1.0),
    FarAway = dc_centroids:new([100.0, 100.0], 1.0),
    ?equals(mathlib:nearestCentroid(U2, [V2, FarAway]), {dc_centroids:distance(U2, V2),
            V2}),
    ?equals(mathlib:nearestCentroid(U2, [FarAway, V2]), {dc_centroids:distance(U2, V2),
            V2}),
    ok.

% Test mathlib:closestPoints/1
%
% Testcases:
% - Return none for an empty list of centroids
% - Return none for a list containing only one centroid
% - Return the two elements with the smallest distance
% - When ambiguous, pick any two elements with a smallest distance
closest_points(_Config) ->
    %% ----- Good cases which should work ------
    C1 = [C1_1, C1_2 | _] = [dc_centroids:new([float(X), float(X)], 0.0)
                               || X <- lists:seq(1,5)],
    Dist1 = dc_centroids:distance(C1_1, C1_2),
    ?equals(mathlib:closestPoints(C1), {Dist1, C1_1, C1_2}),

    %% ----- Cases that should behave well ------
    % empty list
    ?equals(mathlib:closestPoints([]), none),

    % list with only one element
    U = dc_centroids:new([0.0, 0.0], 0.0),
    ?equals(mathlib:closestPoints([U]), none),

    % ambiguous list
    C2 = [C2_1, C2_2 | _] = [dc_centroids:new([float(X), float(Y)], 0.0)
                               || X <- lists:seq(1,5),
                                  Y <- lists:seq(1,5)],
    Dist2 = dc_centroids:distance(C2_1, C2_2),
    ?equals(mathlib:closestPoints(C2), {Dist2, C2_1, C2_2}),

    % shuffled ambiguous list
    C3 = util:shuffle(C2),
    {Dist3, _A, _B} = mathlib:closestPoints(C3),
    ?equals(Dist3, 1.0),

    % regression test
    U2 = dc_centroids:new([0.0, 0.0], 1.0),
    V2 = dc_centroids:new([1.0, 1.0], 1.0),
    FarAway = dc_centroids:new([100.0, 100.0], 1.0),
    ?equals(mathlib:closestPoints([U2, V2, FarAway]),
        {dc_centroids:distance(U2,V2), U2, V2}),
    ?equals(mathlib:closestPoints([U2, FarAway, V2]),
        {dc_centroids:distance(U2,V2), U2, V2}),
    ok.


% Test mathlib:aggloClustering/1
%
% Testcases:
% - Clustering should fail with error when Radius < 0
% - Clustering an empty list should return an empty list
% - Clustering of one centroid should return the same centroid
% - Clustering two centroids with a distance less/equal Radius should return a merged centroid
% - Clustering two centroids with a distance > Radius should return both centroids
% - Clustering a set of centroids should return the correct set of merged centroids
% - The sum of the relative size over all centroids should remain the same
% - XXX What should it do if elements/coordinates are duplicated?
agglomerative_clustering(_Config) ->
    % crash when radius < 0
    ?expect_exception(mathlib:aggloClustering([], -1), error, function_clause),

    % empty centroid list
    ?equals(mathlib:aggloClustering([], 0), []),

    % single node
    U = dc_centroids:new([0.0, 0.0], 1.0),
    ?equals(mathlib:aggloClustering([U], 0), [U]),

    % merge two nodes
    V = dc_centroids:new([1.0, 1.0], 1.0),
    MergedUV = dc_centroids:new([0.5, 0.5], 2.0),
    ?equals(mathlib:aggloClustering([U,V], 2), [MergedUV]),
    ?equals(mathlib:aggloClustering([V,U], 2), [MergedUV]),

    % don't merge far-away nodes
    FarAway = dc_centroids:new([100.0, 100.0], 1.0),
    ?equals(mathlib:aggloClustering([V, FarAway], 50), [V, FarAway]),
    ?equals(mathlib:aggloClustering([V, U, FarAway], 99), [MergedUV, FarAway]),
    ?equals(mathlib:aggloClustering([V, FarAway, U], 99), [MergedUV, FarAway]),

    % merge many nodes, relative size sum should remain the same
    C = [dc_centroids:new([float(X), float(X)], 1/6) || X <- lists:seq(1,6)],
    ?equals(mathlib:aggloClustering(C, 7), [dc_centroids:new([3.5, 3.5], 1.0)]),
    ok.
