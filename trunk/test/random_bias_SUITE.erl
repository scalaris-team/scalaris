%  @copyright 2010-2012 Zuse Institute Berlin
%  @end
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
%%% @author Maik Lange <malange@informatik.hu-berlin.de
%%% @doc    Tests for random_bias module.
%%% @end
%%% Created : 2012-04-26
%%%-------------------------------------------------------------------
%% @version $Id $


-module(random_bias_SUITE).

-author('malange@informatik.hu-berlin.de').

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

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_binom_values(X, Acc) ->
    case X() of
        {ok, V} -> get_binom_values(X, [V | Acc]);
        {last, V} -> lists:reverse([V | Acc])
    end.

test1(_) ->
    N = 5,
    P = 0.3,
    R = random_bias:binomial(N, P),
    Vals = get_binom_values(R, []),
    EV = expected_value(Vals),
    ?equals_w_note(1, trunc(EV), io_lib:format("EV=~p~nVals=~p", [EV, Vals])).

test2(_) ->
    N = 10,
    P = 2/7,
    R = random_bias:binomial(N, P),
    Vals = get_binom_values(R, []),
    EV = expected_value(Vals),
    ct:pal("Binomial N = ~p ; P = ~p~nResult=~p~nSum=~p~nExpectedValue=~p", 
           [N, P, Vals, lists:sum(Vals), EV]),
    ?assert(EV > 2.85) andalso ?assert(EV < 2.87).

-spec sum_test(100..100000) -> boolean().
sum_test(N) ->
    P = 3/7,
    R = random_bias:binomial(N, P),
    Vals = get_binom_values(R, []),
    Sum = lists:sum(Vals),
    N2 = lists:foldl(fun(V, Acc) -> Acc + (V * N) end, 0, Vals),
    ?assert_w_note(1 - Sum =< 0.00001, io_lib:format("Sum=~p", [Sum])) andalso
        ?assert_w_note(N2 >= 0.99*N, io_lib:format("N2=~p - N=~p - 0.99*N=~p", [N2, N, 0.99*N])).

tester_sum_test(_) ->    
    tester:test(?MODULE, sum_test, 1, 12, [{threads, 4}]).

-spec prop_value_count(pos_integer()) -> boolean().
prop_value_count(Count) ->
    BFun = random_bias:binomial(Count, 0.3),
    Values = gen_values(BFun, []),
    Len = length(Values),
    ?equals_w_note(Count, Len, io_lib:format("Count = ~p - Generated=~p", [Count, Len])).

tester_value_count(_) ->
    tester:test(?MODULE, prop_value_count, 1, 50, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec gen_values(random_bias:distribution_fun(), [float()]) -> [float()].
gen_values(Fun, Acc) ->
    case Fun() of
        {ok, V} -> gen_values(Fun, [V | Acc]);
        {last, V} -> [V | Acc]
    end.

-spec expected_value([float()]) -> float().
expected_value(List) ->
    {EV, _} = lists:foldl(fun(P, {Sum, K}) -> {Sum + P * K, K + 1} end, {0, 0}, List),
    EV.

