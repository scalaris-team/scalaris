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
%%% File    : vivaldi.erl
%%% Author  : Marie Hoffmann <ozymandiaz147@googlemail.com>
%%% Description : math utility functions
%%%
%%% Created :  28 July 2009 by Marie Hoffmann <ozymandiaz147@googlemail.com>
%%%-------------------------------------------------------------------
%% @author Marie Hoffmann <ozymandiaz147@googlemail.com>
%% @copyright 2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
-module(mathlib).

-export([max/2, vecAdd/2, vecSub/2, vecMult/2, euclideanDist/1, euclideanDist/2, u/1, u/2, closestPointsHelper/1, vecWeightedAvg/4, median/1]).

% get maximum of two values
max(V1, V2) ->
    T1 = V1, T2 = V2, if T1 > T2 -> T1 ; true -> T2 end.

% add two vectors X,Y element by element
vecAdd(L1, L2) ->
    lists:zipwith(fun(X, Y) -> X+Y end, L1, L2).


% substract two vectors X,Y
vecSub(L1, L2) ->
    lists:zipwith(fun(X, Y) -> X - Y end, L1, L2).

% multiply Vector V with a scalar S
vecMult(V, S) ->
    lists:map(fun(X) -> S*X end, V).

vecWeightedAvg(L1, L2, W1, W2) ->
    [vecMult(vecAdd(vecMult(L1, W1), vecMult(L2, W2)), 1/(W1+W2))].

% euclidean distance between origin and V
euclideanDist(V) ->
    math:sqrt(lists:foldl(fun(X, Acc) -> Acc + math:pow(X, 2) end, 0, V)).

% euclidean distance between two Vectors V, W
euclideanDist(V, W) ->
    euclideanDist(vecSub(V,W)).

% unit vector u(v) = v/||v||
u(V) -> 
    vecMult(V, 1/euclideanDist(V)).

% unit vector u(v-w)
u(V, W) ->
    u(vecSub(V,W)).

% find indices of closest centroids
closestPointsHelper([C1, C2| Rest]) ->
    closestPoints([C1, C2| Rest], euclideanDist(C1, C2), 1, 2, 1, 2);

closestPointsHelper(_) ->
    {-1, -1, -1}.

closestPoints(Centroids, Min, Idx1, Idx2, I, J) when I < J-1 ->
    I2 = I + 1, 
    Dist = euclideanDist(lists:nth(I2, Centroids), lists:nth(J, Centroids)),
    {Min1, Idx3, Idx4} = condExchange(Min, Idx1, Idx2, Dist, I2, J),
    closestPoints(Centroids, Min1, Idx3, Idx4, I2, J);

closestPoints(Centroids, Min, Idx1, Idx2, _, J) when J < length(Centroids) ->
    I2 = 1,
    J2 = J + 1,
    Dist = euclideanDist(lists:nth(I2, Centroids), lists:nth(J2, Centroids)),
    {Min1, Idx3, Idx4} = condExchange(Min, Idx1, Idx2, Dist, I2, J2),
    closestPoints(Centroids, Min1, Idx3, Idx4, I2, J2);

closestPoints(_, Min, Idx1, Idx2, _, _) ->
    {Min, Idx1, Idx2}.

% update smallest distance and its indices
condExchange(Min, I, J, Dist, _, _) when Min =< Dist ->
    {Min, I, J};

condExchange(_, _, _, Dist, I, J) ->
    {Dist, I, J}.

% median of an unsorted list
median(L) ->
    L1 = lists:sort(L),
    N = length(L1),
    case N rem 2 of
	1 -> lists:nth(round(N/2), L1);
	0 -> (lists:nth(trunc(N/2), L1) + lists:nth(trunc(N/2)+1, L1))/2
    end.