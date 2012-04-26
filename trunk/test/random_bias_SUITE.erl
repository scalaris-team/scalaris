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
     test2].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_binom_values(X, Acc) ->
    case X() of
        {ok, V} -> get_binom_values(X, [V | Acc]);
        {last, V} -> lists:reverse([V | Acc])
    end.

test1(_) ->
    N = 5,
    P = 0.2,
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec expected_value([float()]) -> float().
expected_value(List) ->
    {EV, _} = lists:foldl(fun(P, {Sum, K}) -> {Sum + P * K, K + 1} end, {0, 0}, List),
    EV.

