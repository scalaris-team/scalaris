%  @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%% @doc    Utility functions for execution time measurement.
%%         Src = http://www.trapexit.org/Measuring_Function_Execution_Time
%% @end
%% @version $Id$
-module(measure_util).
-author('mlange@informatik.hu-berlin.de').
-vsn('$Id$').

-export([time_avg/3, time_with_result/3]).
-export([print/1, print/2, get/2, get/3]).
-export([add/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([result/0, options/0]).

-type result() :: { Min::non_neg_integer(), 
                    Max::non_neg_integer(),
                    Med::non_neg_integer(), 
                    Avg::float(),
                    Sum::non_neg_integer(),
                    Iterations::pos_integer()
                  }.

-type options() :: skip_first_value.

-type time_unit() :: us | ms | s.
-type mr_type() :: min | max | med | avg | sum.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% real type spec (not compatible with R13):
%-spec time_with_result(fun((...) -> Result), pos_integer(), [options()]) -> {Result, Time::result()}.
-spec time_with_result(fun(), pos_integer(), [options()]) -> {Result::term(), Time::result()}.
time_with_result(Fun, Iterations, Options) ->
    Time = time_avg(Fun, Iterations, Options),
    Result = Fun(),
    {Result, Time}.

% @doc Measures average execution time with possibiliy of skipping 
%      the first measured value.
%      i.e.: time_avg(fun() -> myFun(A, B, C) end, 100, []).
-spec time_avg(fun(), pos_integer(), [options()]) -> result().
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
    Sum = lists:foldl(fun(X, Sum) -> X + Sum end, 0, Times),
    Avg = Sum / Length,
    {Min, Max, Med, Avg, Sum, Iterations}.

-spec add(result(), result()) -> result().
add({AMin, AMax, AMed, AAvg, ASum, AIt}, {BMin, BMax, BMed, BAvg, BSum, BIt}) ->
    {AMin + BMin,
     AMax + BMax,
     AMed + BMed,
     AAvg + BAvg,
     ASum + BSum,
     erlang:round((AIt + BIt) / 2)}.

-spec print(result()) -> [{atom(), any()}].
print({Min, Max, Med, Avg, _Sum, _} = Values) ->
    MaxVal = lists:max([Min, Max, Med, Avg]),
    if
        MaxVal > 1000000 -> print(Values, s);
        MaxVal > 1000    -> print(Values, ms);
        true             -> print(Values, us)
    end.
        

-spec print(result(), time_unit()) -> [{atom(), any()}].
print({Min, Max, Med, Avg, Sum, Iter}, Unit) ->
    [{unit, Unit},
     {min, value_to_unit(Min, Unit)},
     {max, value_to_unit(Max, Unit)},
     {med, value_to_unit(Med, Unit)},
     {avg, value_to_unit(Avg, Unit)},
     {sum, value_to_unit(Sum, Unit)},
     {iterations, Iter}].

-spec get(result(), mr_type()) -> number().
get({Min, Max, Med, Avg, Sum, _}, Type) ->
    case Type of
        min -> Min;
        max -> Max;
        med -> Med;
        avg -> Avg;
        sum -> Sum
    end.

-spec get(result(), mr_type(), time_unit()) -> float().
get(Value, Type, Unit) ->
    value_to_unit(get(Value, Type), Unit).

-spec value_to_unit(number(), time_unit()) -> float().
value_to_unit(Val, Unit) ->
    case Unit of
        us -> erlang:float(Val);
        ms -> Val / 1000;
        s -> Val / 1000000
    end.
