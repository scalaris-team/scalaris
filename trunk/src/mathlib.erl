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
%%% File    : mathlib.erl
%%% Author  : Marie Hoffmann <ozymandiaz147@googlemail.com>
%%% Description : math utility functions
%%%
%%% Created :  28 July 2009 by Marie Hoffmann <ozymandiaz147@googlemail.com>
%%%-------------------------------------------------------------------
%% @author Marie Hoffmann <ozymandiaz147@googlemail.com>
%% @copyright 2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin

-module(mathlib).

-export([closestPoints/1, euclideanDistance/1, euclideanDistance/2, max/2, u/1,
         vecAdd/2, vecSub/2, vecMult/2, vecWeightedAvg/4, zeros/1, median/1,
        aggloClustering/3]).

-type(vector() :: [float()]).
-type(centroid() :: vector()).

% get maximum of two values
max(V1, V2) ->
    T1 = V1, T2 = V2, if T1 > T2 -> T1 ; true -> T2 end.

% median of an unsorted list
-spec(median/1 :: (vector()) -> float()).
median(L) ->
     L1 = lists:sort(L),
     N = length(L1),
     case N rem 2 of
 	1 -> lists:nth(round(N/2), L1);
 	0 -> (lists:nth(trunc(N/2), L1) + lists:nth(trunc(N/2)+1, L1))/2
     end.


% add two vectors X,Y
-spec(vecAdd/2 :: (vector(), vector()) -> vector()).
vecAdd(L1, L2) ->
    lists:zipwith(fun(X, Y) -> X+Y end, L1, L2).

% substract two vectors X,Y
-spec(vecSub/2 :: (vector(), vector()) -> vector()).
vecSub(L1, L2) ->
    lists:zipwith(fun(X, Y) -> X - Y end, L1, L2).

% multiply Vector V with a scalar S
-spec(vecMult/2 :: (vector(), float()) -> vector()).
vecMult(V, S) ->
    lists:map(fun(X) -> S*X end, V).

-spec(vecWeightedAvg/4 :: (vector(), vector(), float(), float()) -> vector()).
vecWeightedAvg(L1, L2, W1, W2) ->
    vecMult(vecAdd(vecMult(L1, W1), vecMult(L2, W2)), 1/(W1+W2)).

% euclidean distance between origin and V
-spec(euclideanDistance/1 :: (vector()) -> float()).
euclideanDistance(V) ->
    euclidDistToZero(V, 0).

euclidDistToZero([HV | V], Acc) ->
    euclidDistToZero(V, Acc + math:pow(HV,2));

euclidDistToZero([], Acc) ->
    math:sqrt(Acc).

% euclidean distance between two vectors
-spec(euclideanDistance/2 :: (vector(), vector()) -> float()).
euclideanDistance(V, W) ->
    euclidDist(V, W, 0).

euclidDist([HV | V], [HW | W], Acc) ->
    euclidDist(V, W, Acc + math:pow(HV-HW, 2));

euclidDist([], [], Acc) ->
    math:sqrt(Acc).


% unit vector u(v) = v/||v||
-spec(u/1 :: (vector()) -> vector()).
u(V) ->
    vecMult(V, 1/euclideanDistance(V)).

% find indices of closest centroids
closestPoints([C1, C2| Rest]) ->
    closestPointsForI([C1, C2| Rest], 1, 2, euclideanDistance(C1, C2), 1, 2);

closestPoints(_) ->
    {-1, -1, -1}.

closestPointsForI([First | Rest], I, J, Min, MinI, MinJ) ->
  {Min1, MinI1, MinJ1} = closestPointsForJ(First, Rest, I, J, Min, MinI, MinJ),
    I1 = I + 1,
    J1 = J + 1,
  closestPointsForI(Rest, I1, J1, Min1, MinI1, MinJ1);

closestPointsForI([], _, _, Min, I, J) ->
    {Min, I, J}.

closestPointsForJ(First, [Centroid | Rest], I, J, Min, MinI, MinJ) ->
    Dist = euclideanDistance(First, Centroid),
    {Min1, MinI1, MinJ1} = condExchange(Min, MinI, MinJ, Dist, I, J),
    J1 = J + 1,
    closestPointsForJ(First, Rest, I, J1, Min1, MinI1, MinJ1);

closestPointsForJ(_, [], _, _, Min, MinI, MinJ) ->
  {Min, MinI, MinJ}.

% update smallest distance and its indices
condExchange(Min, I, J, Dist, _, _) when Min =< Dist ->
    {Min, I, J};

condExchange(_, _, _, Dist, I, J) ->
    {Dist, I, J}.

% initialize list with zeros
zeros(N) ->
    zerosRec(N, []).

zerosRec(N, L) when N > 0->
    zerosRec(N-1, [0 | L]);

zerosRec(_, L) ->
    L.

% get closest centroids and melt them if their distance is within radius()
-spec(aggloClustering/3 :: ([centroid()], vector(), float()) -> {centroid(), vector()}).
aggloClustering(Centroids, Sizes, Radius) ->
    {Min, I, J} = mathlib:closestPoints(Centroids),
    aggloClusteringHelper(Centroids, Sizes, Radius, Min, I, J).

aggloClusteringHelper(Centroids, Sizes, Radius, Min, I, J) when Min =< Radius, length(Centroids) > 1 ->
    C1 = lists:nth(I, Centroids),
    C2 = lists:nth(J, Centroids),
    S1 = lists:nth(I, Sizes),
    S2 = lists:nth(J, Sizes),
    Centroids1 = [mathlib:vecWeightedAvg(C1, C2, S1, S2)| tools:rmvTwo(Centroids, I, J)],
    {Min1, I1, J1} = mathlib:closestPoints(Centroids1),
    aggloClusteringHelper(Centroids1, [S1+S2 | tools:rmvTwo(Sizes, I, J)], Radius, Min1, I1, J1);

aggloClusteringHelper(Centroids, Sizes, _, _, _, _) ->
    {Centroids, Sizes}.
