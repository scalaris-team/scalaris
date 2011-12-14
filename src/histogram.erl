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
                    data = [] :: data_list()}).

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
add(Value, Count, Histogram = #histogram{data = OldData}) ->
    resize(Histogram#histogram{data = insert({Value, Count}, OldData)}).

-spec get_data(Histogram::histogram()) -> data_list().
get_data(Histogram) ->
    Histogram#histogram.data.

%% @doc Merges the given two histograms by adding every data point of Hist2
%%      to Hist1.
-spec merge(Hist1::histogram(), Hist2::histogram()) -> histogram().
merge(Hist1 = #histogram{data = OldData}, Hist2) ->
    NewData = lists:foldl(fun({Value, Count}, Acc) ->
                                  insert({Value, Count}, Acc)
                          end, OldData, get_data(Hist2)),
    resize(Hist1#histogram{data = NewData}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% private
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec resize(Histogram::histogram()) -> histogram().
resize(Histogram = #histogram{data = Data, size = ExpectedSize}) ->
    ActualSize = length(Data),
    if
        ActualSize > ExpectedSize ->
            SmallestInterval = find_smallest_interval(Data),
            NewHistogram = Histogram#histogram{data = merge_interval(SmallestInterval, Data)},
            resize(NewHistogram);
        true ->
            Histogram
    end.

-spec insert(Value::data_item(), Data::data_list()) -> data_list().
insert({Value, Count}, []) ->
    [{Value, Count}];
insert({Value, Count}, [{Value2, Count2} | Rest]) ->
    case Value < Value2 of
        true ->
            [{Value, Count}, {Value2, Count2} | Rest];
        false ->
            [{Value2, Count2} | insert({Value, Count}, Rest)]
    end.

%@doc PRE: length(Data) >= 2
-spec find_smallest_interval(Data::data_list()) -> float().
find_smallest_interval([{Value, _}, {Value2, _} | Rest]) ->
    find_smallest_interval_loop(Value2 - Value, Value2, Rest).

-spec find_smallest_interval_loop(MinInterval::float(), LastValue::float(), Data::data_list()) -> float().
find_smallest_interval_loop(MinInterval, _LastValue, []) ->
    MinInterval;
find_smallest_interval_loop(MinInterval, LastValue, [{Value, _} | Rest]) ->
    find_smallest_interval_loop(erlang:min(MinInterval, Value - LastValue), Value, Rest).

%@doc PRE: length(Data) >= 2
-spec merge_interval(Interval::float(), Data::data_list()) -> data_list().
merge_interval(Interval, [{Value, Count}, {Value2, Count2} | Rest]) ->
    case Value2 - Value of
        Interval ->
            [{(Value*Count + Value2*Count2) / (Count + Count2), Count + Count2} | Rest];
        _ ->
            [{Value, Count} | merge_interval(Interval, [{Value2, Count2} | Rest])]
    end.
