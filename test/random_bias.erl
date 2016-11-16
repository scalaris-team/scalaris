% @copyright 2011-2016 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    biased random number generator
%% @end
%% @version $Id$
-module(random_bias).
-author('malange@informatik.hu-berlin.de').

-export([binomial/2]).
-export([next/1]).
-export([numbers_left/1]).

% for tester:
-export([tester_create_generator/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([generator/0]).

-type approx() :: none | {normal, M::float(), Dev::float(), A::float()}.

-type binomial_state() :: {binom,
                           N         :: pos_integer(),
                           P         :: float(),             %only works for ]0,1[
                           X         :: non_neg_integer(),
                           Approx    :: approx()
                          }.

-type distribution_state() :: binomial_state(). %or others
-type generator() :: { State       :: distribution_state(),
                       CalcFun     :: fun((distribution_state()) -> float()),
                       NewStateFun :: fun((distribution_state()) -> distribution_state() | exit)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a new generator state for a binomial distribution for use with
%%      next/1.
%%      Note: a binomial distribution will generate (N+1) values!
-spec binomial(N::pos_integer(), P::float()) -> generator().
binomial(N, P) when P > 0 andalso P < 1 ->
    {{binom, N, P, 0, approx_valid(N, P)},
     fun calc_binomial/1, fun next_state/1}.

%% @doc Gets the next random value (and an updated state).
-spec next(generator()) -> {ok, float(), generator()} | {last, float(), exit}.
next({DS, CalcFun, NextFun}) ->
    V = CalcFun(DS),
    case NextFun(DS) of
        exit -> {last, V, exit};
        NewDS -> {ok, V, {NewDS, CalcFun, NextFun}}
    end.

-spec numbers_left(generator()) -> pos_integer().
numbers_left({{binom, N, _P, X, _Approx}, _CalcFun, _NextFun}) ->
    N + 1 - X.

-define(SQRT_2_PI, 2.5066282746310002). % math:sqrt(2 * math:pi()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% -spec calc_normal(X::float(), M::float(), Dev::float()) -> float().
%% calc_normal(X, M, Dev) ->
%%     A = 1 / (Dev * ?SQRT_2_PI),
%%     calc_normal(X, M, Dev, A).

-spec calc_normal(X::float(), M::float(), Dev::float(), A::float()) -> float().
calc_normal(X, M, Dev, A) ->
    B = -1/2 * math:pow(((X-M) / Dev), 2),
    A * math:exp(B).

-spec calc_binomial(binomial_state()) -> float().
calc_binomial({binom, _N, _P, X, _Approx = {normal, M, Dev, A}}) ->
    calc_normal(X, M, Dev, A);
calc_binomial({binom, N, P, X, _Approx = none}) ->
    try begin
            NOverX = mathlib:binomial_coeff(N, X),
            Pow = math:pow(P, X) * math:pow(1 - P, N - X),
            if Pow == 0 ->
                   % rather use approximation because we lost precision
                   calc_binomial({binom, N, P, X, normal_approx_of_binomial(N, P)});
               true -> NOverX * Pow
            end
        end
    catch _:_ ->
              % use approximation instead because cannot calculate the result exactly:
              % log:pal("calc_binomial(~p, ~p, ~p)~n  ~p", [N, P, X, erlang:get_stacktrace()])
              calc_binomial({binom, N, P, X, normal_approx_of_binomial(N, P)})
    end.

-spec next_state(distribution_state()) -> distribution_state() | exit.
next_state({binom, X, _P, X, _}) ->
    exit;
next_state({binom, N, P, X, Approx}) -> 
    {binom, N, P, X + 1, Approx}.

% @doc approximation is good if this conditions hold
%      SRC: http://www.vosesoftware.com/ModelRiskHelp/index.htm#Distributions/Approximating_one_distribution_with_another/Approximations_to_the_Binomial_Distribution.htm
-spec approx_valid(pos_integer(), float()) -> approx().
approx_valid(_N, 0) -> none;
approx_valid(_N, 1) -> none;
approx_valid(N, P) ->
    if (N > ((9 * P) / (1 - P))) andalso (N > ((9 * (1 - P)) / P)) ->
           normal_approx_of_binomial(N, P);
       true ->
           none
    end.

-spec normal_approx_of_binomial(N::pos_integer(), P::float())
        -> {normal, M::float(), Dev::float(), A::float()}. 
normal_approx_of_binomial(N, P) ->
    Dev = math:sqrt(N * P * (1 - P)),
    {normal, N * P, Dev, 1 / (Dev * ?SQRT_2_PI)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Tester
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec tester_create_generator(pos_integer(), 1..1000000, 1..1000000)
        -> generator().
tester_create_generator(N, P1, P2) when P2 > P1 ->
    case binomial(N, P1 / P2) of
        {{binom, N, P, X, Approx = none}, CalcFun, NewStateFun} ->
            % too high factorials slow down the tests too much
            {{binom, erlang:min(50, N), P, X, Approx}, CalcFun, NewStateFun};
        X -> X
    end;
tester_create_generator(N, P1, P2) when P2 < P1 ->
    tester_create_generator(N, P2, P1);
tester_create_generator(N, P, P) ->
    tester_create_generator(N, 999999, 1000000).

