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

%% @author Marie Hoffmann <ozymandiaz147@googlemail.com>
%% @doc Math utility functions.
%% @version $Id$
-module(mathlib).
-author('ozymandiaz147@googlemail.com').
-vsn('$Id$').

-export([closestPoints/1, euclideanDistance/1, euclideanDistance/2, u/1,
         vecAdd/2, vecSub/2, vecMult/2, vecWeightedAvg/4, zeros/1, median/1,
        aggloClustering/3]).

-type(vector() :: [number(),...]).
-type(centroid() :: vector()).

%% @doc Median of an unsorted non-empty list of numbers, i.e. a vector.
-spec median(vector()) -> number().
median(L) ->
    L1 = lists:sort(L),
    N = length(L1),
    case N rem 2 of
        1 -> lists:nth(round(N / 2), L1);
        0 -> (lists:nth(trunc(N / 2), L1) + lists:nth(trunc(N / 2) + 1, L1)) / 2
    end.

%% @doc Add two vectors X,Y, i.e. X + Y.
-spec vecAdd(X::vector(), Y::vector()) -> vector().
vecAdd(X, Y) ->
    lists:zipwith(fun(Xi, Yi) -> Xi + Yi end, X, Y).

%% @doc Substract two vectors X,Y, i.e. X - Y.
-spec vecSub(X::vector(), Y::vector()) -> vector().
vecSub(X, Y) ->
    lists:zipwith(fun(Xi, Yi) -> Xi - Yi end, X, Y).

%% @doc Multiply vector V with a scalar S.
-spec vecMult(V::vector(), S::float()) -> vector().
vecMult(V, S) ->
    lists:map(fun(X) -> S*X end, V).

-spec vecWeightedAvg(V1::vector(), V2::vector(), W1::float(), W2::float()) -> vector().
vecWeightedAvg(V1, V2, W1, W2) ->
    vecMult(vecAdd(vecMult(V1, W1), vecMult(V2, W2)), 1 / (W1 + W2)).

%% @doc Euclidean distance between origin and V.
-spec euclideanDistance(V::vector()) -> Distance::float().
euclideanDistance(V) ->
    math:sqrt(lists:foldl(fun(Vi, OldDist) -> OldDist + math:pow(Vi, 2) end,
                          0.0, V)).

%% @doc Euclidean distance between two vectors.
-spec euclideanDistance(V::vector(), W::vector()) -> Distance::float().
euclideanDistance(V, W) ->
    math:sqrt(util:zipfoldl(fun(Vi, Wi) -> math:pow(Vi - Wi, 2) end,
                            fun(Dist, OldDist) -> OldDist + Dist end,
                            V, W, 0.0)).

%% @doc Unit vector u(v) = v/||v||
-spec u(V::vector()) -> UV::vector().
u(V) ->
    vecMult(V, 1 / euclideanDistance(V)).

%% @doc Find indices of closest centroids.
-spec closestPoints(Centroids::[centroid()])
        -> {Min::number(), I::pos_integer(), J::pos_integer()} | {-1, -1, -1}.
closestPoints([C1, C2 | Rest]) ->
    closestPointsForI([C1, C2 | Rest], 1, 2, euclideanDistance(C1, C2), 1, 2);
closestPoints(_) ->
    {-1, -1, -1}.

-spec closestPointsForI(Centroids::[centroid()], I::pos_integer(), J::pos_integer(),
                        Min::number(), MinI::pos_integer(), MinJ::pos_integer())
        -> {DistMin::number(), IMin::pos_integer(), JMin::pos_integer()}.
closestPointsForI([First | Rest], I, J, Min, MinI, MinJ) ->
    {Min1, MinI1, MinJ1} = closestPointsForJ(First, Rest, I, J, Min, MinI, MinJ),
    I1 = I + 1,
    J1 = J + 1,
    closestPointsForI(Rest, I1, J1, Min1, MinI1, MinJ1);
closestPointsForI([], _, _, Min, I, J) ->
    {Min, I, J}.

-spec closestPointsForJ(First::centroid(), Rest::[centroid()],
                        I::pos_integer(), J::pos_integer(),
                        Min::number(), MinI::pos_integer(), MinJ::pos_integer())
        -> {DistMin::number(), IMin::pos_integer(), JMin::pos_integer()}.
closestPointsForJ(First, [Centroid | Rest], I, J, Min, MinI, MinJ) ->
    Dist = euclideanDistance(First, Centroid),
    {Min1, MinI1, MinJ1} = condExchange(Min, MinI, MinJ, Dist, I, J),
    J1 = J + 1,
    closestPointsForJ(First, Rest, I, J1, Min1, MinI1, MinJ1);
closestPointsForJ(_, [], _, _, Min, MinI, MinJ) ->
    {Min, MinI, MinJ}.

%% @doc Update smallest distance and its indices.
-spec condExchange(Min::number(), MinI::pos_integer(), MinJ::pos_integer(),
                   Dist::number(), DistI::pos_integer(), DistJ::pos_integer())
        -> {DistMin::number(), IMin::integer(), JMin::integer()}.
condExchange(Min, I, J, Dist, _, _) when Min =< Dist ->
    {Min, I, J};

condExchange(_, _, _, Dist, I, J) ->
    {Dist, I, J}.

%% @doc Create a list with N zeros.
-spec zeros(N::0) -> [];
           (N::pos_integer()) -> [0,...].
zeros(0) ->
    [];
zeros(N) ->
    [0 || _ <- lists:seq(1,N)].

%% @doc Get closest centroids and merge them if their distance is within Radius.
-spec aggloClustering(Centroids::[centroid()], Sizes::vector(),
                      Radius::number()) -> {[centroid()], vector()}.
aggloClustering(Centroids, Sizes, Radius) ->
    {Min, I, J} = closestPoints(Centroids),
    aggloClusteringHelper(Centroids, Sizes, Radius, Min, I, J).

-spec aggloClusteringHelper
        (Centroids::[centroid(),...], Sizes::vector(), Radius::number(),
         Min::number(), I::pos_integer(), J::pos_integer()) -> {[centroid()], vector()};
        (Centroids::[centroid()], Sizes::vector(), Radius::number(),
         Min::-1, I::-1, J::-1) -> {[centroid()], vector()}.
% Note: closestPoints/1 creates Min, I, J and only returns {-1, -1, -1} if
% Centroids contains less than two elements. This is not the case in the first
% pattern and we can thus assume these values are pos_integer().
aggloClusteringHelper([_,_|_] = Centroids, [_,_|_] = Sizes, Radius, Min, I, J) when Min =< Radius ->
    C1 = lists:nth(I, Centroids),
    C2 = lists:nth(J, Centroids),
    S1 = lists:nth(I, Sizes),
    S2 = lists:nth(J, Sizes),
    Centroids1 = [vecWeightedAvg(C1, C2, S1, S2) | tools:rmvTwo(Centroids, I, J)],
    {Min1, I1, J1} = closestPoints(Centroids1),
    aggloClusteringHelper(Centroids1, [S1 + S2 | tools:rmvTwo(Sizes, I, J)],
                          Radius, Min1, I1, J1);
aggloClusteringHelper(Centroids, Sizes, _Radius, _Min, _I, _J) ->
    {Centroids, Sizes}.
