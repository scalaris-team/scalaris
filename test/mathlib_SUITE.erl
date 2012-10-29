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
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() -> [
        euclidian_distance
        , nearest_centroid
    ].

suite() ->
    [
        {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.

% Test mathlib:euclideanDistance/2
euclidian_distance(_Config) ->
    {U1, V1, V2, V3, V4} = {
        [0.0,0.0]
        , [0, 1.0]
        , [1.0, 0]
        , [1.0, 1.0]
        , [-1.0,-1.0]
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

    U = dc_centroids:new([0.0,0.0], 0),

    %% ----- Good cases which should work ------

    %% List of Centroids is not empty. U is not an element and only one
    %   element in the list is nearest to U
    C1 = [C1Nearest1, C1Nearest2|_]=[dc_centroids:new([X,X], 0) || X <- lists:seq(1,5)],
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
    Ambiguous1 = [dc_centroids:new([X,Y],0) || X <- lists:seq(1,5), Y <- lists:seq(1,5)],
    {AmbiguousDistance1, Element} = mathlib:nearestCentroid(U, Ambiguous1),
    ?equals(AmbiguousDistance1, math:sqrt(2)),
    AllowedElements1 = [dc_centroids:new([X,Y],0) || {X, Y} <- [
            {1.0,1.0},{1.0,-1.0},{-1.0,1.0},{-1.0,-1.0}
        ]],
    ?assert_w_note(lists:foldl(fun(_, true) -> true;
                                  (E, false) when E == Element -> true;
                                  (_, _) -> false
        end, false, AllowedElements1),
        "Nearest element not in list of allowed coordinates"),

    ok.
