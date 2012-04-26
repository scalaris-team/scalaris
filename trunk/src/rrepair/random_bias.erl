% @copyright 2011, 2012 Zuse Institute Berlin

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
%% @version $Id: rr_recon.erl 3028 2012-04-20 13:57:59Z lakedaimon300@gmail.com $

-module(random_bias).


-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([binomial/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(binomial_state, { n = ?required(binomial_state, n) :: pos_integer(),
                          p = ?required(binomial_state, p) :: float(),
                          k = 0                            :: non_neg_integer()
                        }).
-type binomial_state() :: #binomial_state{}.


-type distribution_state() :: #binomial_state{}. %or others
-type generator_state() :: { State       :: distribution_state(),
                             CalcFun     :: fun((distribution_state()) -> any()),
                             NewStateFun :: fun((distribution_state()) -> distribution_state() | exit)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% creates a new binomial distribution generation fun.
-spec binomial(pos_integer(), float()) -> fun().
binomial(N, P) ->
    State = { #binomial_state{ n = N, p = P },
              fun calc_binomial/1,
              fun next_state/1 },
    Pid = spawn(fun() -> generator(State) end),
    fun() ->
            comm:send_local(Pid, {next, self()}),
            receive 
                {last_response, V} -> {last, V};
                {next_response, V} -> {ok, V}
            end
    end.

-spec generator(generator_state()) -> any().
generator({ DS, CalcFun, NextFun }) ->
    receive
        {next, Pid} ->
            V = CalcFun(DS),
            case NextFun(DS) of
                exit -> comm:send_local(Pid, {last_response, V});
                NewDS -> comm:send_local(Pid, {next_response, V}),
                         generator({NewDS, CalcFun, NextFun})
            end            
    end.

-spec calc_binomial(binomial_state()) -> float().
calc_binomial(#binomial_state{ n = N, p = P, k = K }) ->
    mathlib:binomial_coeff(N, K) * math:pow(P, K) * math:pow(1 - P, N - K).

-spec next_state(distribution_state()) -> distribution_state() | exit.
next_state(#binomial_state{ n = N, k = K}) when K =:= N -> exit;
next_state(#binomial_state{ k = K} = S) -> 
    S#binomial_state{ k = K + 1}. 
