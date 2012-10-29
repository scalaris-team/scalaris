% @copyright 2007-2012 Zuse Institute Berlin

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
        aggloClustering/2]).

-export([factorial/1, binomial_coeff/2]).

%% for type_check_SUITE
-export([binomial_coeff_feeder/2,
         factorial_feeder/1,
         zeros_feeder/1]).

-type(vector() :: [number(),...]).
-type(centroid() :: {vivaldi:network_coordinate(), float()}).
-type(centroids() :: [centroid()]).

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
-spec closestPoints(Centroids::centroids())
        -> {Min::number(), I::pos_integer(), J::pos_integer()} | {-1, -1, -1}.
closestPoints([{C1,_S1}, {C2,_S2} |_] = L) ->
    closestPointsForI(L, 0, 1, euclideanDistance(C1, C2), 0, 1);
closestPoints(_) ->
    {-1, -1, -1}.

-spec closestPointsForI(Centroids::centroids(), I::non_neg_integer(), J::non_neg_integer(),
                        Min::number(), MinI::non_neg_integer(), MinJ::non_neg_integer())
        -> {DistMin::number(), IMin::pos_integer(), JMin::pos_integer()}.
closestPointsForI([First | Rest], I, J, Min, MinI, MinJ) ->
    {Min1, MinI1, MinJ1} = closestPointsForJ(First, Rest, I, J, Min, MinI, MinJ),
    I1 = I + 1,
    J1 = J + 1,
    closestPointsForI(Rest, I1, J1, Min1, MinI1, MinJ1);
closestPointsForI([], _, _, Min, I, J) ->
    {Min, I, J}.

-spec closestPointsForJ(First::centroid(), Rest::centroids(),
                        I::non_neg_integer(), J::non_neg_integer(),
                        Min::number(), MinI::non_neg_integer(), MinJ::non_neg_integer())
        -> {DistMin::number(), IMin::non_neg_integer(), JMin::non_neg_integer()}.
closestPointsForJ(F = {First, _SF}, [{Centroid, _ST} | Rest], I, J, Min, MinI, MinJ) ->
    Dist = euclideanDistance(First, Centroid),
    {Min1, MinI1, MinJ1} = condExchange(Min, MinI, MinJ, Dist, I, J),
    J1 = J + 1,
    closestPointsForJ(F, Rest, I, J1, Min1, MinI1, MinJ1);
closestPointsForJ(_, [], _, _, Min, MinI, MinJ) ->
    {Min, MinI, MinJ}.

%% @doc Update smallest distance and its indices.
-spec condExchange(Min::number(), MinI::non_neg_integer(), MinJ::non_neg_integer(),
                   Dist::number(), DistI::non_neg_integer(), DistJ::non_neg_integer())
        -> {DistMin::number(), IMin::integer(), JMin::integer()}.
condExchange(Min, I, J, Dist, _, _) when Min =< Dist ->
    {Min, I, J};

condExchange(_, _, _, Dist, I, J) ->
    {Dist, I, J}.

-spec zeros_feeder(0..10000) -> {0..10000}.
zeros_feeder(N) -> {N}.

%% @doc Create a list with N zeros.
-spec zeros(N::0) -> [];
           (N::pos_integer()) -> [0,...].
zeros(N) -> lists:duplicate(N, 0).

%% @doc Get closest centroids and merge them if their distance is within Radius.
-spec aggloClustering(Centroids::centroids(), Radius::number()) -> centroids().
aggloClustering(Centroids, Radius) when Radius >= 0 ->
    {Min, I, J} = closestPoints(Centroids),
    aggloClusteringHelper(Centroids, Radius, Min, I, J).

-spec aggloClusteringHelper
        (Centroids::[centroid(),...], Radius::number(),
            Min::number(), I::pos_integer(), J::pos_integer()) -> centroids();
        (Centroids::centroids(), Radius::number(),
            Min::-1, I::-1, J::-1) -> centroids().
% Note: closestPoints/1 creates Min, I, J and only returns {-1, -1, -1} if
% Centroids contains less than two elements. This is not the case in the first
% pattern and we can thus assume these values are pos_integer().
aggloClusteringHelper([{_C1,_S1},{_C2,_S2}|_] = Centroids, Radius, Min, I, J) when Min =< Radius ->
    {C1, S1} = lists:nth(I+1, Centroids),
    {C2, S2} = lists:nth(J+1, Centroids),
    Centroids1 = [{vecWeightedAvg(C1, C2, S1, S2), S1+S2} |
        util:lists_remove_at_indices(Centroids, [I, J])],
    {Min1, I1, J1} = closestPoints(Centroids1),
    aggloClusteringHelper(Centroids1, Radius, Min1, I1, J1);
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

-spec factorial_feeder(0..20, pos_integer()) -> {0..20, pos_integer()}.
factorial_feeder(N, Acc) -> {N, Acc}.

-spec factorial(non_neg_integer(), pos_integer()) -> pos_integer().
factorial(0, Acc) -> Acc;
factorial(N, Acc) ->
    factorial(N - 1, N * Acc).
