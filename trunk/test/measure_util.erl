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

-export([time_avg/4]).

% @doc Measures average execution time with possibiliy of skipping 
%      the first measured value.
%      Result = {MinTime, MaxTime, MedianTime, AverageTime}
% @end
-spec time_avg(fun(), [term()], pos_integer(), boolean()) -> 
                        {Min::integer(), Max::integer(), Med::integer(), Avg::integer()}.
time_avg(Fun, Args, ExecTimes, SkipFirstValue) when ExecTimes > 0 ->
    L = util:s_repeatAndCollect(fun() -> 
                                        {Time, _} = timer:tc(Fun, Args), 
                                        Time 
                                end, 
                                [], 
                                ExecTimes),
    Values = case SkipFirstValue of
                 true -> lists:nthtail(1, L);
                 _ -> L
             end,        
    Length = length(Values),
    Min = lists:min(Values),
    Max = lists:max(Values),
    Med = lists:nth(round((Length / 2)), lists:sort(Values)),
    Avg = round(lists:foldl(fun(X, Sum) -> X + Sum end, 0, Values) / Length),
    {Min, Max, Med, Avg}.