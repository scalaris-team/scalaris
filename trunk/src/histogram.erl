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
-export([create/1, add/2, add/3, get_data/1,
         get_num_elements/1, get_num_inserts/1, merge/2]).

% private API for unit tests:
-export([find_smallest_interval/1, merge_interval/2,
         tester_create_histogram/2, tester_is_valid_histogram/1]).

-export([find_largest_window/2, find_largest_window_feeder/2]).
-export([foldl_until/2, foldr_until/2]).
% private but shared with histogram_normalized
-export([foldl_until_helper/4]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-ifdef(with_export_type_support).
-export_type([histogram/0]).
-endif.

-type value() :: number().
-type data_item() :: {value(), pos_integer()}.
-type data_list() :: list(data_item()).
-record(histogram, {size = ?required(histogram, size):: non_neg_integer(),
                    data = [] :: data_list(),
                    data_size = 0 :: non_neg_integer(),
                    inserts = 0 :: non_neg_integer()
                   }).

-opaque histogram() :: #histogram{}.

-spec create(Size::non_neg_integer()) -> histogram().
create(Size) ->
    #histogram{size = Size}.

-spec add(Value::value(), Histogram::histogram()) -> histogram().
add(Value, Histogram) ->
    add(Value, 1, Histogram).

-spec add(Value::value(), Count::pos_integer(), Histogram::histogram()) -> histogram().
add(_Value, _Count, Histogram = #histogram{size = 0}) ->
    Histogram;
add(Value, Count, Histogram = #histogram{data = OldData, inserts = Inserts}) ->
    DataNew = insert({Value, Count}, OldData),
    resize(Histogram#histogram{data = DataNew, data_size = length(DataNew),
                               inserts = Inserts + Count}).

-spec get_data(Histogram::histogram()) -> data_list().
get_data(Histogram) ->
    Histogram#histogram.data.

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

%% @doc Traverses the histogram until TargetCount entries have been found
%%      and returns the value at this position.
-spec foldl_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value(), Sum::pos_integer()}.
foldl_until(TargetVal, CircularHist) ->
    HistData = get_data(CircularHist),
    foldl_until_helper(TargetVal, HistData, _SumSoFar = 0, _BestValue = nil).

%% @doc Like foldl_until but traverses the list from the right
-spec foldr_until(TargetCount::non_neg_integer(), histogram())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value(), Sum::pos_integer()}.
foldr_until(TargetVal, CircularHist) ->
    HistData = get_data(CircularHist),
    foldl_until_helper(TargetVal, lists:reverse(HistData), _SumSoFar = 0, _BestValue = nil).

-spec foldl_until_helper(TargetVal::non_neg_integer(), DataList::data_list(),
                         SumSoFar::non_neg_integer(), BestValue::non_neg_integer())
        -> {fail, Value::value() | nil, SumSoFar::non_neg_integer()} |
           {ok, Value::value(), Sum::pos_integer()}.
foldl_until_helper(TargetVal, _List, SumSoFar, BestValue)
  when SumSoFar >= TargetVal ->
    {ok, BestValue, SumSoFar};
foldl_until_helper(_TargetVal, [], SumSoFar, BestValue) ->
    {fail, BestValue, SumSoFar};
foldl_until_helper(TargetVal, [{Val, Count} | Other], SumSoFar, _BestValue) ->
    foldl_until_helper(TargetVal, Other, SumSoFar + Count, Val).

%% @doc Determines the maximum number of occurances under a sliding window
-spec find_largest_window(WindowSize::pos_integer(), Histogram::histogram()) -> {Pos::pos_integer(), Sum::pos_integer()}.
find_largest_window(WindowSize, #histogram{data = HistData, data_size = Len})  when Len >= WindowSize ->
    Data = lists:map(fun({_, Count}) -> Count end, HistData),
    sliding_window_max(WindowSize, 1, 0, Data, 1, queue:new(), 0).

%% @doc Feeder function to create valid window sizes for the tester
%%      In case of empty or zero sized histograms a default histogram is used
-spec find_largest_window_feeder(WindowSize::pos_integer(), Hist::histogram()) -> {WindowSize::pos_integer(), Histogram::histogram()}.
find_largest_window_feeder(WindowSize, #histogram{size = Size, data_size = DataSize} = Hist) ->
    case DataSize =:= 0 orelse Size =:= 0 of
        true ->
            Hist2 = histogram:create(100),
            Elements = lists:seq(1, 100),
            Hist3 = lists:foldl(fun histogram:add/2, Hist2, Elements),
            {20, Hist3};
        _ ->
            {erlang:min(WindowSize, DataSize), Hist}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% private
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Helper function for find_largest_window/2
-spec sliding_window_max(WindowSize::pos_integer(), MaxPos::pos_integer(), MaxSum::non_neg_integer(),
                         Data::list(), Pos::pos_integer(), OldVals::queue(), Sum::non_neg_integer())
                      -> Max::{Pos::pos_integer(), Sum::pos_integer()}.
sliding_window_max(0, MaxPos, MaxSum, [], _Pos, _OldVals, _Sum) ->
    {MaxPos, MaxSum};
sliding_window_max(0, MaxPos, MaxSum, List, Pos, OldVals, Sum) ->
    {{value, OldVal}, NewOldVals} = queue:out(OldVals),
    sliding_window_max(1, MaxPos, MaxSum, List, Pos + 1, NewOldVals, Sum - OldVal);
sliding_window_max(WindowSize, MaxPos, MaxSum, [H|T], Pos, OldVals, LastSum) ->
    Sum = LastSum + H,
    case Sum > MaxSum of
        true -> sliding_window_max(WindowSize - 1, Pos   , Sum   , T, Pos, queue:in(H, OldVals), Sum);
        _    -> sliding_window_max(WindowSize - 1, MaxPos, MaxSum, T, Pos, queue:in(H, OldVals), Sum)
    end.

%% @doc Resizes the given histogram to fit its maximum size (reduces the data).
%%      PRE: histogram maximum size > 0 (from create/1)
-spec resize(Histogram::histogram()) -> histogram().
resize(Histogram = #histogram{data = Data, size = ExpectedSize, data_size = ActualSize}) ->
    ?DBG_ASSERT(ExpectedSize > 0),
    if
        (ActualSize > ExpectedSize) andalso (1 < ActualSize) ->
            %% we need at least two items to do the following:
            MinSecondValue = find_smallest_interval(Data),
            NewHistogram = Histogram#histogram{data = merge_interval(MinSecondValue, Data),
                                               data_size = ActualSize - 1},
            resize(NewHistogram);
        true ->
            Histogram
    end.

-spec insert(Value::data_item(), Data::data_list()) -> data_list().
insert({Value, _}, [{Value2, Count} | Rest]) when Value =:= Value2 ->
    [{Value, Count + 1} | Rest];
insert({Value, _} = DataItem, [{Value2, _} | _] = Data) when Value < Value2 ->
    [DataItem | Data];
insert(DataItem, [DataItem2 | Rest]) ->
    [DataItem2 | insert(DataItem, Rest)];
insert(DataItem, []) ->
    [DataItem].

%% @doc Finds the smallest interval between two consecutive values and returns
%%      the second value (in the list's order).
%%      PRE: length(Data) >= 2
-spec find_smallest_interval(Data::data_list()) -> MinSecondValue::value().
find_smallest_interval([{Value, _}, {Value2, _} | Rest]) ->
    find_smallest_interval_loop(Value2 - Value, Value2, Value2, Rest).

-spec find_smallest_interval_loop(MinInterval::value(), MinSecondValue::value(), LastValue::value(), Data::data_list()) -> MinSecondValue::value().
find_smallest_interval_loop(MinInterval, MinSecondValue, LastValue, [{Value, _} | Rest]) ->
    Diff = Value - LastValue,
    case MinInterval =< Diff of
        true -> NewMinInterval = MinInterval,
                NewMinSecondValue = MinSecondValue;
        _    -> NewMinInterval = Diff,
                NewMinSecondValue = Value
    end,
    find_smallest_interval_loop(NewMinInterval, NewMinSecondValue, Value, Rest);
find_smallest_interval_loop(_MinInterval, MinSecondValue, _LastValue, []) ->
    MinSecondValue.

%% @doc Merges two consecutive values if the second of them is MinSecondValue.
%%      Stops after the first match.
%%      PRE: length(Data) >= 2, two consecutive values with the given difference
-spec merge_interval(MinSecondValue::value(), Data::data_list()) -> data_list().
merge_interval(Value2, [{Value, Count}, {Value2, Count2} | Rest]) when is_float(Value) orelse is_float(Value2) ->
    [{(Value * Count + Value2 * Count2) / (Count + Count2), Count + Count2} | Rest];
merge_interval(Value2, [{Value, Count}, {Value2, Count2} | Rest]) ->
    [{(Value * Count + Value2 * Count2) div (Count + Count2), Count + Count2} | Rest];
merge_interval(MinSecondValue, [DataItem | Rest]) ->
    [DataItem | merge_interval(MinSecondValue, Rest)].

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
    Size >= DataSize andalso length(Data) =:= DataSize;
tester_is_valid_histogram(_) ->
    false.
