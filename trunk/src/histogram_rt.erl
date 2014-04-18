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
%% @doc Like histogram.erl but takes ?RT:key() as value and operates
%%      in the key space according to the used routing table.
%% @version $Id$
-module(histogram_rt).
-author('michels@zib.de').
-vsn('$Id$').

-export([create/2, add/2, add/3, get_data/1, get_size/1, merge/2,
         get_num_elements/1, get_num_inserts/1]).
-export([foldl_until/2, foldr_until/2]).
-export([is_histogram/1]).

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([histogram/0, base_key/0]).
-endif.

-type key() :: ?RT:key().
-type internal_value() :: histogram:value().

-type external_data_item() :: {key(), pos_integer()}.
-type external_data_list() :: [external_data_item()].

-type base_key() :: key().

-opaque(histogram() :: {Histogram::histogram:histogram(),
                        BaseKey::base_key()
                       }).

-spec create(Size::non_neg_integer(), BaseKey::base_key()) -> histogram().
create(Size, BaseKey) ->
    Histogram = histogram:create(Size),
    {Histogram, BaseKey}.

-spec add(Value::key(), Histogram::histogram()) -> histogram().
add(Value, Histogram) ->
    add(Value, 1, Histogram).

-spec add(Value::key(), Count::pos_integer(), Histogram::histogram()) -> histogram().
add(Value, Count, {Histogram, BaseKey}) ->
    NewHistogram = histogram:add(normalize(Value, BaseKey), Count, Histogram),
    {NewHistogram, BaseKey}.

-spec get_data(Histogram::histogram()) -> external_data_list().
get_data({Histogram, BaseKey}) ->
    Data = histogram:get_data(Histogram),
    lists:map(fun({Value, Count}) ->
                      {denormalize(Value, BaseKey), Count}
              end, Data).

-spec get_size(Histogram::histogram()) -> non_neg_integer().
get_size({Histogram, _BaseKey}) ->
    histogram:get_size(Histogram).

-spec get_num_elements(Histogram::histogram()) -> non_neg_integer().
get_num_elements({Histogram, _BaseKey}) ->
    histogram:get_num_elements(Histogram).

-spec get_num_inserts(Histogram::histogram()) -> non_neg_integer().
get_num_inserts({Histogram, _BaseKey}) ->
    histogram:get_num_inserts(Histogram).

%% @doc: Merges the given two histograms by adding every data point of Hist2
-spec merge(Hist1::histogram(), Hist2::histogram()) -> histogram().
merge(Hist1, Hist2) ->
    case get_size(Hist1) of
        0 -> Hist1;
        _ ->
            DataHist2 = get_data(Hist2),
            lists:foldl(fun({Value, Count}, Hist) ->
                                add(Value, Count, Hist)
                        end,
                        Hist1, DataHist2)
    end.

%% @doc Traverses the histogram until TargetCount entries have been found
%%      and returns the value at this position.
-spec foldl_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::key() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::key() | nil, Sum::non_neg_integer()}.
foldl_until(TargetCount, {Histogram, BaseKey}) ->
    Result = histogram:foldl_until(TargetCount, Histogram),
    case Result of
        {_Status, nil, _Sum} -> Result;
        {Status, Range, Sum} -> {Status, denormalize(Range, BaseKey), Sum}
    end.

%% @doc Like foldl_until but traverses the list from the right
-spec foldr_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::key() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::key() | nil, Sum::non_neg_integer()}.
foldr_until(TargetCount, {Histogram, BaseKey}) ->
    Result = histogram:foldr_until(TargetCount, Histogram),
    case Result of
        {_Status, nil, _Sum} -> Result;
        {Status, Range, Sum} -> {Status, denormalize(Range, BaseKey), Sum}
    end.

-compile({inline, [normalize/2]}).
-spec normalize(Value::key(), BaseKey::base_key()) -> internal_value().
normalize(Value, BaseKey) ->
    ?RT:get_range(BaseKey, Value).

-compile({inline, [denormalize/2]}).
-spec denormalize(Value::internal_value(), BaseKey::base_key()) -> key().
denormalize(Value, BaseKey) ->
    ?RT:get_split_key(BaseKey, BaseKey, {Value, ?RT:n()}).

-spec is_histogram(histogram() | any()) -> boolean().
is_histogram({_Histogram, _BaseKey}) ->
    true;
is_histogram(_) ->
    false.
