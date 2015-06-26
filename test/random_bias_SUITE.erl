%  @copyright 2010-2012 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de
%% @doc    Tests for random_bias module.
%% @end
%% @version $Id$
-module(random_bias_SUITE).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").
-include("record_helpers.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() ->
    [test1,
     test2,
     tester_sum_test,
     tester_value_count].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test1(_) ->
    N = 5,
    P = 0.3,
    R = random_bias:binomial(N, P),
    Vals = lists:reverse(gen_values(R, [])),
    EV = expected_value(Vals),
    ?equals_w_note(1, trunc(EV), io_lib:format("EV=~p~nVals=~p", [EV, Vals])).

test2(_) ->
    N = 10,
    P = 2/7,
    R = random_bias:binomial(N, P),
    Vals = lists:reverse(gen_values(R, [])),
    EV = expected_value(Vals),
    ct:pal("Binomial N = ~p ; P = ~p~nResult=~p~nSum=~p~nExpectedValue=~p", 
           [N, P, Vals, lists:sum(Vals), EV]),
    ?assert(EV > 2.85) andalso ?assert(EV < 2.87).

-spec prop_sum_test(1..100000, 1..1000000, 1..1000000) -> true.
prop_sum_test(N, P1, P1) ->
    prop_sum_test(N, P1 - 1, P1);
prop_sum_test(N0, P1, P2) ->
    P = ?IIF(P2 > P1, P1 / P2, P2 / P1),
    % similar as in random_bias:tester_create_generator/3:
    R = case random_bias:binomial(N0, P) of
            {{binom, N0, P, X, Approx = none}, CalcFun, NewStateFun} ->
                % too high factorials slow down the tests too much
                N = erlang:min(50, N0),
                {{binom, N, P, X, Approx}, CalcFun, NewStateFun};
            X ->
                N = N0,
                X
        end,
    Vals = gen_values(R, []),
    Sum = lists:sum(Vals),
    ?compare(fun erlang:'=<'/2, Sum, 1.0 + 1.0e12),
    ?compare(fun erlang:'>='/2, Sum, 0.99),
    N2 = lists:foldl(fun(V, Acc) -> Acc + (V * N) end, 0, Vals),
    ?compare(fun erlang:'=<'/2, N2, N + 1.0e12),
    ?compare(fun erlang:'>='/2, N2, 0.99 * N).

tester_sum_test(_) ->
    prop_sum_test(20, 816034, 257824),
    tester:register_value_creator({typedef, random_bias, generator, []},
                                  random_bias, tester_create_generator, 3),
    tester:test(?MODULE, prop_sum_test, 3, 100, [{threads, 4}]),
    tester:unregister_value_creator({typedef, random_bias, generator, []}).

-spec prop_value_count(1..100000) -> true.
prop_value_count(Count) ->
    R = random_bias:binomial(Count, 0.3),
    Values = gen_values(R, []),
    Len = length(Values),
    ?equals_w_note(Count + 1, Len, io_lib:format("Count = ~p - Generated=~p", [Count, Len])).

tester_value_count(_) ->
    tester:test(?MODULE, prop_value_count, 1, 50, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec gen_values(random_bias:generator(), [float()]) -> [float()].
gen_values(RanGen, Acc) ->
    case random_bias:next(RanGen) of
        {ok, V, RanGen1} -> gen_values(RanGen1, [V | Acc]);
        {last, V, exit}  -> [V | Acc]
    end.

-spec expected_value([float()]) -> float().
expected_value(List) ->
    {EV, _} = lists:foldl(fun(P, {Sum, K}) -> {Sum + P * K, K + 1} end, {0, 0}, List),
    EV.

