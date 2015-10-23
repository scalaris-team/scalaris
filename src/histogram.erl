% @copyright 2011-2014 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Basic Histogram.
%%         Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel
%%         decision tree algorithm", J. Machine Learning Research 11
%%         (2010), pp. 849--872.
%% @end
%% @version $Id$
-module(histogram).
-author('schuett@zib.de').
-vsn('$Id$').

% external API
-export([create/1, add/2, add/3, get_data/1, get_size/1,
         get_num_elements/1, get_num_inserts/1, merge/2]).
-export([merge_weighted/3, normalize_count/2]).

% private API for unit tests:
-export([find_smallest_interval/1, merge_interval/2,
         tester_create_histogram/2, tester_is_valid_histogram/1]).

-export([foldl_until/2, foldr_until/2]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export_type([histogram/0, value/0]).

-type value() :: number().
-type data_item() :: {value(), pos_integer()}.
-type data_list() :: list(data_item()).
-record(histogram, {size = ?required(histogram, size):: non_neg_integer(),
                    data = [] :: data_list(),
                    data_size = 0 :: non_neg_integer(),
                    inserts = 0 :: non_neg_integer()
                   }).

-opaque histogram() :: #histogram{}.

%% @doc Creates an empty Size sized histogram
-spec create(Size::non_neg_integer()) -> histogram().
create(Size) ->
    #histogram{size = Size}.

-spec add(Value::value(), Histogram::histogram()) -> histogram().
add(Value, Histogram) ->
    add(Value, 1, Histogram).

-spec add(Value::value(), Count::pos_integer(), Histogram::histogram()) -> histogram().
add(_Value, _Count, Histogram = #histogram{size = 0}) ->
    Histogram;
add(Value, Count, Histogram = #histogram{data = OldData, data_size = DataSize, inserts = Inserts}) ->
    DataNew = insert({Value, Count}, OldData),
    resize(Histogram#histogram{data = DataNew, data_size = DataSize + 1,
                               inserts = Inserts + Count}).

-spec get_data(Histogram::histogram()) -> data_list().
get_data(Histogram) ->
    Histogram#histogram.data.

-spec get_size(Histogram::histogram()) -> non_neg_integer().
get_size(Histogram) ->
    Histogram#histogram.size.

-spec get_num_elements(Histogram::histogram()) -> non_neg_integer().
get_num_elements(Histogram) ->
    Histogram#histogram.data_size.

-spec get_num_inserts(Histogram::histogram()) -> non_neg_integer().
get_num_inserts(Histogram) ->
    Histogram#histogram.inserts.

%% @doc Merges the given two histograms by adding every data point of Hist2
%%      to Hist1.
-spec merge(Hist1::histogram(), Hist2::histogram()) -> histogram().
merge(Hist1 = #histogram{size = 0}, _Hist2) -> Hist1;
merge(Hist1 = #histogram{data = Hist1Data}, #histogram{data = Hist2Data}) ->
    NewData = lists:foldl(fun insert/2, Hist1Data, Hist2Data),
    resize(Hist1#histogram{data = NewData, data_size = length(NewData)}).

%% @doc Merges Hist2 into Hist1 and applies a weight to the Count of Hist2
-spec merge_weighted(Hist1::histogram(), Hist2::histogram(), Weight::pos_integer()) -> histogram().
merge_weighted(Hist1, #histogram{data = Hist2Data} = Hist2, Weight) ->
    WeightedData = lists:keymap(fun(Count) -> Count * Weight end, 2, Hist2Data),
    WeightedHist2 = Hist2#histogram{data = WeightedData},
    merge(Hist1, WeightedHist2).

%% @doc Normalizes the Count by a normalization constant N
-spec normalize_count(N::pos_integer(), Histogram::histogram()) -> histogram().
normalize_count(N, Histogram) ->
    Data = histogram:get_data(Histogram),
    DataNew = lists:keymap(fun(Count) -> Count div N end, 2, Data),
    DataNew2 = lists:filter(fun({_Value, Count}) ->
                                    Count > 0
                            end, DataNew),
    resize(Histogram#histogram{data = DataNew2, data_size = length(DataNew2)}).

%% @doc Traverses the histogram until TargetCount entries have been found
%%      and returns the value at this position.
%% TODO change this to expect non empty histogram
-spec foldl_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value() | nil, Sum::non_neg_integer()}.
foldl_until(TargetCount, Histogram) ->
    HistData = get_data(Histogram),
    foldl_until_helper(TargetCount, HistData, _SumSoFar = 0, _BestValue = nil).

%% @doc Like foldl_until but traverses the list from the right
-spec foldr_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value() | nil, Sum::non_neg_integer()}.
foldr_until(TargetCount, Histogram) ->
    HistData = get_data(Histogram),
    foldl_until_helper(TargetCount, lists:reverse(HistData), _SumSoFar = 0, _BestValue = nil).

%% @doc Private method only exported for histogram_rt
-spec foldl_until_helper(TargetCount::non_neg_integer(), DataList::data_list(),
                         SumSoFar::non_neg_integer(), BestValue::nil | non_neg_integer())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value() | nil, Sum::non_neg_integer()}.
foldl_until_helper(TargetCount, _List, SumSoFar, BestValue)
  when SumSoFar >= TargetCount ->
    {ok, BestValue, SumSoFar};
foldl_until_helper(_TargetVal, [], SumSoFar, BestValue) ->
    {fail, BestValue, SumSoFar};
foldl_until_helper(TargetCount, [{Val, Count} | Other], SumSoFar, _BestValue) ->
    foldl_until_helper(TargetCount, Other, SumSoFar + Count, Val).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% private
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Resizes the given histogram to fit its maximum size (reduces the data).
%%      PRE: histogram maximum size > 0 (from create/1)
-spec resize(Histogram::histogram()) -> histogram().
resize(Histogram = #histogram{data = Data, size = Size, data_size = DataSize})
  when DataSize > Size andalso DataSize > 1 ->
    ?DBG_ASSERT(Size > 0),
    %% we need at least two items to do the following:
    MinFirstValue = find_smallest_interval(Data),
    NewHistogram = Histogram#histogram{data = merge_interval(MinFirstValue, Data),
                                       data_size = DataSize - 1},
    resize(NewHistogram);
resize(#histogram{} = Histogram) ->
    Histogram.

-spec insert(Value::data_item(), Data::data_list()) -> data_list().
insert({Value, _} = DataItem, [{Value2, _} | _] = Data) when Value =< Value2 ->
    [DataItem | Data];
insert(DataItem, [DataItem2 | Rest]) ->
    [DataItem2 | insert(DataItem, Rest)];
insert(DataItem, []) ->
    [DataItem].

%% @doc Finds the smallest interval between two consecutive values and returns
%%      the position of the first value (in the list's order).
%%      Returning the position instead of the value ensures that the correct
%%      items are merged when duplicate entries are in the histogram.
%%      PRE: length(Data) >= 2
-spec find_smallest_interval(Data::data_list()) -> MinFirstValue::value().
find_smallest_interval([{Value, _}, {Value2, _} | Rest]) ->
    find_smallest_interval_loop(Value2 - Value, Value, Value2, Rest).

-spec find_smallest_interval_loop(MinInterval::value(), MinFirstValue::value(), LastValue::number(), Data::data_list()) -> MinValue::value().
find_smallest_interval_loop(MinInterval, MinFirstValue, LastValue, [{Value, _} | Rest]) ->
    Diff = Value - LastValue,
    case MinInterval =< Diff of
        true -> NewMinInterval = MinInterval,
                NewMinFirstValue = MinFirstValue;
        _    -> NewMinInterval = Diff,
                NewMinFirstValue = LastValue
    end,
    find_smallest_interval_loop(NewMinInterval, NewMinFirstValue, Value, Rest);
find_smallest_interval_loop(_MinInterval, MinFirstValue, _LastValue, []) ->
    MinFirstValue.

%% @doc Merges two consecutive values if the first one of them is at PosMinValue.
%%      Stops after the first match.
%%      PRE: length(Data) >= 2, two consecutive values with the given difference
-spec merge_interval(MinFirstValue::value(), Data::data_list()) -> data_list().
merge_interval(Value, [{Value, Count}, {Value2, Count2} | Rest]) when is_float(Value) orelse is_float(Value2) ->
    [{(Value * Count + Value2 * Count2) / (Count + Count2), Count + Count2} | Rest];
merge_interval(Value, [{Value, Count}, {Value2, Count2} | Rest]) ->
    [{(Value * Count + Value2 * Count2) div (Count + Count2), Count + Count2} | Rest];
merge_interval(MinFirstValue, [DataItem | Rest]) ->
    [DataItem | merge_interval(MinFirstValue, Rest)].

-spec tester_create_histogram(Size::non_neg_integer(), Data::data_list()) -> histogram().
tester_create_histogram(Size = 0, _Data) ->
    #histogram{size = Size, data = [], data_size = 0};
tester_create_histogram(Size, Data) ->
    DataSize = length(Data),
    if DataSize > Size ->
           #histogram{size = DataSize, data = Data, data_size = DataSize};
       true ->
           #histogram{size = Size, data = Data, data_size = DataSize}
    end.

-spec tester_is_valid_histogram(X::term()) -> boolean().
tester_is_valid_histogram(#histogram{size = 0, data = [], data_size = 0}) ->
    true;
tester_is_valid_histogram(#histogram{size = Size, data = Data, data_size = DataSize})
  when Size > 0->
    Size >= DataSize andalso is_list(Data) andalso length(Data) =:= DataSize;
tester_is_valid_histogram(_) ->
    false.
