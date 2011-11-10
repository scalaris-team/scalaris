%  @copyright 2010-2011 Zuse Institute Berlin
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
%%% File    bloom_SUITE.erl
%%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%%% @doc    Utility functions for execution time measurement.
%%%         Src = http://www.trapexit.org/Measuring_Function_Execution_Time
%%% @end
%%% Created : 01/07/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id $

-module(measure_util).


-export([time_avg/4, print_result/2]).

-type measure_result() :: { Min::non_neg_integer(), 
                            Max::non_neg_integer(),
                            Med::non_neg_integer(), 
                            Avg::non_neg_integer()
                           }.

% @doc Measures average execution time with possibiliy of skipping 
%      the first measured value.
%      Result = {MinTime, MaxTime, MedianTime, AverageTime}
-spec time_avg(fun(), [term()], pos_integer(), boolean()) -> measure_result().
time_avg(Fun, Args, ExecTimes, SkipFirstValue) ->
    L = util:s_repeatAndCollect(
          fun() -> {Time, _} = util:tc(Fun, Args), Time end,
          [], ExecTimes),
    Times = case SkipFirstValue of
                true -> lists:nthtail(1, L);
                _ -> L
            end,   
    Length = length(Times),
    Min = lists:min(Times),
    Max = lists:max(Times),
    Med = lists:nth(((Length + 1) div 2), lists:sort(Times)),
    Avg = round(lists:foldl(fun(X, Sum) -> X + Sum end, 0, Times) / Length),
    {Min, Max, Med, Avg}.

-spec print_result(measure_result(), us|ms|s) -> [{atom(), any()}].
print_result({Min, Max, Med, Avg}, us) ->
    [{min, Min},
     {max, Max},
     {med, Med},
     {avg, Avg}];
print_result({Min, Max, Med, Avg}, ms) ->
    [{min, Min / 1000},
     {max, Max / 1000},
     {med, Med / 1000},
     {avg, Avg / 1000}];
print_result({Min, Max, Med, Avg}, s) ->
    [{min, Min / 100000},
     {max, Max / 100000},
     {med, Med / 100000},
     {avg, Avg / 100000}].
