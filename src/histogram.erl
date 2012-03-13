% @copyright 2011 Zuse Institute Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Basic Histogram.
%%%         Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel
%%%         decision tree algorithm", J. Machine Learning Research 11
%%%         (2010), pp. 849--872.
%%% @end
%% @version $Id$
-module(histogram).
-author('schuett@zib.de').
-vsn('$Id$').

-ifdef(with_export_type_support).
-export_type([histogram/0]).
-endif.

% external API
-export([create/1, add/2, add/3, get_data/1, merge/2]).

% private API
-export([resize/1, insert/2, find_smallest_interval/1, merge_interval/2]).

-include("record_helpers.hrl").

-type data_item() :: {float(), pos_integer()}.
-type data_list() :: list(data_item()).
-record(histogram, {size = ?required(histogram, size):: non_neg_integer(),
                    data = [] :: data_list(),
                    data_size = 0 :: non_neg_integer()}).

-opaque histogram() :: #histogram{}.

-spec create(Size::non_neg_integer()) -> histogram().
create(Size) ->
    #histogram{size = Size}.

-spec add(Value::float(), Histogram::histogram()) -> histogram().
add(Value, Histogram) ->
    add(Value, 1, Histogram).

-spec add(Value::float(), Count::pos_integer(), Histogram::histogram()) -> histogram().
add(_Value, _Count, Histogram = #histogram{size = 0}) ->
    Histogram;
add(Value, Count, Histogram = #histogram{data = OldData, data_size = OldDataSize}) ->
    resize(Histogram#histogram{data = insert({Value, Count}, OldData),
                               data_size = OldDataSize + 1}).

-spec get_data(Histogram::histogram()) -> data_list().
get_data(Histogram) ->
    Histogram#histogram.data.

%% @doc Merges the given two histograms by adding every data point of Hist2
%%      to Hist1.
-spec merge(Hist1::histogram(), Hist2::histogram()) -> histogram().
merge(Hist1 = #histogram{data = Hist1Data, data_size = Hist1DataSize},
      Hist2 = #histogram{data_size = Hist2DataSize}) ->
    NewData = lists:foldl(fun insert/2, Hist1Data, get_data(Hist2)),
    resize(Hist1#histogram{data = NewData, data_size = Hist1DataSize + Hist2DataSize}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% private
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec resize(Histogram::histogram()) -> histogram().
resize(Histogram = #histogram{data = Data, size = ExpectedSize, data_size = ActualSize}) ->
    if
        ActualSize > ExpectedSize ->
            MinSecondValue = find_smallest_interval(Data),
            NewHistogram = Histogram#histogram{data = merge_interval(MinSecondValue, Data),
                                               data_size = ActualSize - 1},
            resize(NewHistogram);
        true ->
            Histogram
    end.

-spec insert(Value::data_item(), Data::data_list()) -> data_list().
insert(DataItem, []) ->
    [DataItem];
insert({Value, _} = DataItem, [{Value2, _} | _] = Data) when Value < Value2 ->
    [DataItem | Data];
insert(DataItem, [DataItem2 | Rest]) ->
    [DataItem2 | insert(DataItem, Rest)].

%% @doc Finds the smallest interval between two consecutive values and returns
%%      the second value (in the list's order).
%%      PRE: length(Data) >= 2
-spec find_smallest_interval(Data::data_list()) -> MinSecondValue::float().
find_smallest_interval([{Value, _}, {Value2, _} | Rest]) ->
    find_smallest_interval_loop(Value2 - Value, Value2, Value2, Rest).

-spec find_smallest_interval_loop(MinInterval::float(), MinSecondValue::float(), LastValue::float(), Data::data_list()) -> MinSecondValue::float().
find_smallest_interval_loop(_MinInterval, MinSecondValue, _LastValue, []) ->
    MinSecondValue;
find_smallest_interval_loop(MinInterval, MinSecondValue, LastValue, [{Value, _} | Rest]) ->
    Diff = Value - LastValue,
    case MinInterval =< Diff of
        true -> NewMinInterval = MinInterval,
                NewMinSecondValue = MinSecondValue;
        _    -> NewMinInterval = Diff,
                NewMinSecondValue = Value
    end,
    find_smallest_interval_loop(NewMinInterval, NewMinSecondValue, Value, Rest).

%% @doc Merges two consecutive values if the second of them is MinSecondValue.
%%      Stops after the first match.
%%      PRE: length(Data) >= 2, two consecutive values with the given difference
-spec merge_interval(MinSecondValue::float(), Data::data_list()) -> data_list().
merge_interval(Value2, [{Value, Count}, {Value2, Count2} | Rest]) ->
    [{(Value * Count + Value2 * Count2) / (Count + Count2), Count + Count2} | Rest];
merge_interval(MinSecondValue, [DataItem | Rest]) ->
    [DataItem | merge_interval(MinSecondValue, Rest)].
