% @copyright 2007-2014 Zuse Institute Berlin

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
%% TODO change from camel case to prefix_infix_postfix()
%% @doc Math utility functions.
%% @version $Id$
-module(mathlib).
-author('ozymandiaz147@googlemail.com').
-vsn('$Id$').

-export([closestPoints/1, euclideanDistance/1, euclideanDistance/2, u/1,
         vecAdd/2, vecSub/2, vecMult/2, vecWeightedAvg/4, zeros/1, median/1,
         nearestCentroid/2,
        aggloClustering/2]).

-export([factorial/1, binomial_coeff/2, gcd/2]).

%% for type_check_SUITE
-export([binomial_coeff_feeder/2,
         factorial_feeder/1,
         zeros_feeder/1]).

-type(vector() :: [number(),...]).

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

%% @doc Get the nearest centroid to U from the list Centroids, including the euclidian
%  distance. The function returns 'none' if no nearest centroid can be found. Ambiguity is
%  resolved by picking the first one of the nearest centroids.
-spec nearestCentroid(U::dc_centroids:centroid(), dc_centroids:centroids()) ->
    {Distance::float(), NearestCentroid::dc_centroids:centroid()} | none.
nearestCentroid(_U, []) -> none;
nearestCentroid(U, [U|T]) -> nearestCentroid(U, T);
nearestCentroid(U, [X|Centroids]) ->
    CoordU = dc_centroids:get_coordinate(U),
    CoordsX = dc_centroids:get_coordinate(X),
    First = {euclideanDistance(CoordU, CoordsX), X},
    lists:foldl(fun
            (C, {CurrentDistance, _CurrentMin} = Current) ->
                case C of
                    U -> Current;
                    _ -> CoordC = dc_centroids:get_coordinate(C),
                         NewDistance = euclideanDistance(CoordU, CoordC),
                         case NewDistance < CurrentDistance of
                             true -> {NewDistance, C};
                             false -> Current
                         end
                end
        end, First, Centroids).

%% @doc Find indices of closest centroids.
-spec closestPoints(dc_centroids:centroids())
    -> {float(), dc_centroids:centroid(), dc_centroids:centroid()} | none.
closestPoints([]) -> none;
closestPoints([_]) -> none;
closestPoints([First, Second|_] = Centroids) ->
    FirstDist = dc_centroids:distance(First, Second),
    lists:foldl(
        fun
            (Centroid, {CurrentMinDist, _, _} = Acc) ->
                % get the centroid with minimum distance to Centroid. If this is less than
                % the distance of the centroids in Acc, exchange
                case nearestCentroid(Centroid, Centroids) of
                    none -> Acc;
                    {Dist, CentroidMin} ->
                        case Dist < CurrentMinDist of
                            true -> {Dist, Centroid, CentroidMin};
                            false -> Acc
                        end
                end
    end, {FirstDist, First, Second}, Centroids).

-spec zeros_feeder(0..10000) -> {0..10000}.
zeros_feeder(N) -> {N}.

%% @doc Create a list with N zeros.
-spec zeros(N::0) -> [];
           (N::pos_integer()) -> [0,...].
zeros(N) -> lists:duplicate(N, 0).

%% @doc Get closest centroids and merge them if their distance is within Radius.
-spec aggloClustering(Centroids::dc_centroids:centroids(), Radius::number()) -> dc_centroids:centroids().
aggloClustering(Centroids, Radius) when Radius >= 0 ->
    case closestPoints(Centroids) of
        none -> Centroids;
        {Min, I, J} -> aggloClusteringHelper(Centroids, Radius, Min, I, J)
    end.

-spec aggloClusteringHelper
        (Centroids::[dc_centroids:centroid(),...], Radius::number(),
         Min::float(), I::dc_centroids:centroid(), J::dc_centroids:centroid()) ->
         dc_centroids:centroids().
% Note: closestPoints/1 creates Min, I, J and only returns {-1, -1, -1} if
% Centroids contains less than two elements. This is not the case in the first
% pattern and we can thus assume these values are pos_integer().
aggloClusteringHelper(Centroids, _Radius, 0.0, _, _) -> Centroids;
aggloClusteringHelper(Centroids, Radius, Min, I, J) when Min =< Radius ->
    {C1, S1} = dc_centroids:get_coordinate_and_relative_size(I),
    {C2, S2} = dc_centroids:get_coordinate_and_relative_size(J),
    NewCoordinate = vecWeightedAvg(C1, C2, S1, S2),
    NewCentroid = dc_centroids:new(NewCoordinate, S1+S2),

    NewCentroids = [NewCentroid | lists:subtract(Centroids, [I,J])],
    case closestPoints(NewCentroids) of
        none -> NewCentroids;
        {Min1, I1, J1} ->
            aggloClusteringHelper(NewCentroids, Radius, Min1, I1, J1)
    end;
aggloClusteringHelper(Centroids, _Radius, _Min, _I, _J) ->
    Centroids.

% @doc Calculates the binomial coefficient of n over k for n >= k.
%      see http://rosettacode.org/wiki/Evaluate_binomial_coefficients#Erlang
-spec binomial_coeff(non_neg_integer(), non_neg_integer()) -> integer().
binomial_coeff(_, 0) -> 1;
binomial_coeff(N, K) when N >= K ->
  choose(N, K, 1, 1).

-spec binomial_coeff_feeder(0..100, 0..100) ->
                                   {non_neg_integer(), non_neg_integer()}.
binomial_coeff_feeder(X, Y) ->
    {erlang:max(X, Y), erlang:min(X, Y)}.

-spec choose(non_neg_integer(), non_neg_integer(),
             non_neg_integer(), non_neg_integer()) -> non_neg_integer().
choose(N, K, K, Acc) ->
  (Acc * (N-K+1)) div K;
choose(N, K, I, Acc) ->
  choose(N, K, I+1, (Acc * (N-I+1)) div I).

-spec factorial_feeder(0..20) -> {0..20}.
factorial_feeder(N) -> {N}.

% @doc calculates N!
-spec factorial(non_neg_integer()) -> pos_integer().
factorial(N) -> factorial(N, 1).

-compile({nowarn_unused_function, {factorial_feeder, 2}}).
-spec factorial_feeder(0..20, pos_integer()) -> {0..20, pos_integer()}.
factorial_feeder(N, Acc) -> {N, Acc}.

-spec factorial(non_neg_integer(), pos_integer()) -> pos_integer().
factorial(0, Acc) -> Acc;
factorial(N, Acc) ->
    factorial(N - 1, N * Acc).

%% @doc Calculates the greatest common divisor of two integers.
-spec gcd(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
gcd(A, 0) -> A;
gcd(A, B) -> gcd(B, A rem B).
