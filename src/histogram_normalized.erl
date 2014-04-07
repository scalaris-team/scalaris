%  @copyright 2013-2014 Zuse Institute Berlin

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

%% @author Maximilian Michels <michels@zib.de>
%% @doc Normalized histogram based on the histogram module.
%%      Introduce two functions to normalize/denormalize
%%      values before/after inserting them into the histogram.
%% @version $Id$
-module(histogram_normalized).
-author('michels@zib.de').
-vsn('$Id$').

-ifdef(with_export_type_support).
-export_type([histogram/0]).
-endif.

-export([create/3, add/2, add/3, get_data/1,
         get_num_elements/1, get_num_inserts/1]).
-export([foldl_until/2, foldr_until/2]).

-include("scalaris.hrl").

-type value() :: number().
-type data_item() :: {value(), pos_integer()}.
-type data_list() :: list(data_item()).

-type norm_fun() :: fun((value()) -> value()).

-opaque(histogram() :: {Histogram::histogram:histogram(),
                        NormalizationFun::norm_fun(),
                        NormalizationInverseFun::norm_fun()
                       }).

-spec create(Size::non_neg_integer(), NormFun::norm_fun(), InverseFun::norm_fun()) -> histogram().
create(Size, NormFun, InverseFun) ->
    Histogram = histogram:create(Size),
    {Histogram, NormFun, InverseFun}.

-spec add(Value::value(), Histogram::histogram()) -> histogram().
add(Value, Histogram) ->
    add(Value, 1, Histogram).

-spec add(Value::value(), Count::pos_integer(), Histogram::histogram()) -> histogram().
add(Value, Count, {Histogram, NormFun, _InverseFun}) ->
    ?DBG_ASSERT(Value =:= _InverseFun(NormFun(Value))),
    NewHistogram = histogram:add(NormFun(Value), Count, Histogram),
    {NewHistogram, NormFun, _InverseFun}.

-spec get_data(Histogram::histogram()) -> data_list().
get_data({Histogram, _NormFun, InverseFun}) ->
    %% data needs to be denormalized first
    Data = histogram:get_data(Histogram),
    lists:map(fun({Value, Count}) -> {InverseFun(Value), Count} end, Data).

-spec get_num_elements(Histogram::histogram()) -> non_neg_integer().
get_num_elements({Histogram, _NormFun, _InverseFun}) ->
    histogram:get_num_elements(Histogram).

-spec get_num_inserts(Histogram::histogram()) -> non_neg_integer().
get_num_inserts({Histogram, _NormFun, _InverseFun}) ->
    histogram:get_num_inserts(Histogram).

%% TODO merge/2 not implemented

%% @doc Traverses the histogram until TargetCount entries have been found
%%      and returns the value at this position.
-spec foldl_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value() | nil, Sum::non_neg_integer()}.
foldl_until(TargetVal, CircularHist) ->
    HistData = get_data(CircularHist),
    histogram:foldl_until_helper(TargetVal, HistData, _SumSoFar = 0, _BestValue = nil).

%% @doc Like foldl_until but traverses the list from the right
-spec foldr_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value() | nil, Sum::non_neg_integer()}.
foldr_until(TargetVal, CircularHist) ->
    HistData = get_data(CircularHist),
    histogram:foldl_until_helper(TargetVal, lists:reverse(HistData), _SumSoFar = 0, _BestValue = nil).
