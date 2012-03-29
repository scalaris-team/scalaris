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


-export([time_avg/3, time_with_result/3]).
-export([print/1, print/2, get/3]).

-type measure_result() :: { Min::non_neg_integer(), 
                            Max::non_neg_integer(),
                            Med::non_neg_integer(), 
                            Avg::non_neg_integer(),
                            Iterations::pos_integer()
                           }.

-type measure_options() :: skip_first_value.

-type time_unit() :: us | ms | s.
-type mr_type() :: min | max | med | avg.


time_with_result(Fun, Iterations, Options) ->
    Time = time_avg(Fun, Iterations, Options),
    Result = Fun(),
    {Result, Time}.

% @doc Measures average execution time with possibiliy of skipping 
%      the first measured value.
-spec time_avg(fun(), pos_integer(), [measure_options()]) -> measure_result().
time_avg(Fun, Iterations, Options) ->
    L = util:repeat(fun() -> erlang:element(1, util:tc(Fun, [])) end, [], Iterations, [collect]),
    Times = case lists:member(skip_first_value, Options) of
                true -> lists:nthtail(1, L);
                _ -> L
            end,   
    Length = length(Times),
    Min = lists:min(Times),
    Max = lists:max(Times),
    Med = lists:nth(((Length + 1) div 2), lists:sort(Times)),
    Avg = round(lists:foldl(fun(X, Sum) -> X + Sum end, 0, Times) / Length),
    {Min, Max, Med, Avg, Iterations}.

-spec print(measure_result()) -> [{atom(), any()}].
print({Min, Max, Med, Avg, _} = Values) ->
    MaxVal = lists:max([Min, Max, Med, Avg]),
    if
        MaxVal > 1000000 -> print(Values, s);
        MaxVal > 1000 -> print(Values, ms);
        true -> print(Values, us)
    end.
        

-spec print(measure_result(), time_unit()) -> [{atom(), any()}].
print({Min, Max, Med, Avg, Iter}, Unit) ->
    [{unit, Unit},
     {min, value_to_unit(Min, Unit)},
     {max, value_to_unit(Max, Unit)},
     {med, value_to_unit(Med, Unit)},
     {avg, value_to_unit(Avg, Unit)},
     {iterations, Iter}].

-spec get(measure_result(), mr_type(), time_unit()) -> float().
get({Min, Max, Med, Avg, _}, Type, Unit) ->
    case Type of
        min -> value_to_unit(Min, Unit);
        max -> value_to_unit(Max, Unit);
        med -> value_to_unit(Med, Unit);
        avg -> value_to_unit(Avg, Unit)
    end.

-spec value_to_unit(non_neg_integer(), time_unit()) -> float().
value_to_unit(Val, Unit) ->
    case Unit of
        us -> erlang:float(Val);
        ms -> Val / 1000;
        s -> Val / 1000000
    end.
