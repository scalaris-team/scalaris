%% @copyright 2011 Zuse Institute Berlin

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
%% @doc    Unit tests for src/histogram.erl
%% @end
%% @version $Id$
-module(histogram_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-include("unittest.hrl").

-compile(export_all).

all()   -> [
            add2,
            add3,
            add2_identical,
            add3_identical,
            add_integer,
            resize,
            get_num_inserts,
            find_smallest_interval,
            merge_interval,
            perf_add,
            foldl_until,
            foldr_until
           ].

suite() -> [ {timetrap, {seconds, 40}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

add2(_Config) ->
    H = histogram:create(10),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1.0,1}, {2.0,1}, {3.0,1}, {3.5,1}]),
    ok.

add3(_Config) ->
    H = histogram:create(10),
    Values = [{3.5, 2}, {3.0, 1}, {2.0, 5}, {1.0, 7}],
    H2 = lists:foldl(fun ({Value, Count}, Histogram) ->
                              histogram:add(Value, Count, Histogram)
                     end, H, Values),
    ?equals(histogram:get_data(H2), [{1.0, 7}, {2.0, 5}, {3.0, 1}, {3.5, 2}]),
    ok.

add2_identical(_Config) ->
    H = histogram:create(2),
    Values = [1.25, 5.0, 5.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1.25, 1}, {5.0, 2}]),
    H3 = lists:foldl(fun histogram:add/2, H2, Values),
    ?equals(histogram:get_data(H3), [{1.25, 2}, {5.0, 4}]),
    ok.

add3_identical(_Config) ->
    H = histogram:create(4),
    Values = [{2.0, 1}, {3.5, 2}, {3.0, 1}, {2.0, 6}, {1.0, 3}],
    H2 = lists:foldl(fun({Value, Count}, Histogram) ->
                             histogram:add(Value, Count, Histogram)
                     end,
                     H, Values),
    ?equals(histogram:get_data(H2), [{1.0, 3}, {2.0, 7}, {3.0, 1}, {3.5, 2}]),
    ok.

add_integer(_Config) ->
    H = histogram:create(5),
    Values = [1, 2, 3, 4, 5],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}]),
    Values2 = [3, 4, 10],
    H3 = lists:foldl(fun histogram:add/2, H2, Values2),
    ?equals(histogram:get_data(H3), [{1, 2}, {3, 2}, {4, 2}, {5, 1}, {10, 1}]),
    ok.


resize(_Config) ->
    H = histogram:create(3),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1.0,1}, {2.0,1}, {3.25,2}]),
    ok.

get_num_inserts(_Config) ->
    H = histogram:create(10),
    ?equals(histogram:get_num_inserts(H), 0),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_num_inserts(H2), 4),
    Values2 = [{2.0, 1}, {3.5, 2}, {3.0, 1}, {2.0, 6}, {1.0, 3}],
    H3 = lists:foldl(fun({Value, Count}, Histogram) ->
                             histogram:add(Value, Count, Histogram)
                     end,
                     H2, Values2),
    ?equals(histogram:get_num_inserts(H3), 17),
    ok.

find_smallest_interval(_Config) ->
    H1a = histogram:create(10),
    Values1a = [3.5, 3.0, 2.0, 1.0],
    H1b = lists:foldl(fun histogram:add/2, H1a, Values1a),
    ?equals(3.0, histogram:find_smallest_interval(histogram:get_data(H1b))),
    Values2a = [4.0, 2.5, 2.0, 1.0],
    H2b = lists:foldl(fun histogram:add/2, H1a, Values2a),
    ?equals(2.0, histogram:find_smallest_interval(histogram:get_data(H2b))),
    ok.

merge_interval(_Config) ->
    H = histogram:create(10),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    MinFirstValue = histogram:find_smallest_interval(histogram:get_data(H2)),
    H3 = histogram:merge_interval(MinFirstValue, histogram:get_data(H2)),
    ?equals(3.0, MinFirstValue),
    ?equals(H3, [{1.0,1}, {2.0,1}, {3.25,2}]),
    ok.

perf_add(_Config) ->
    Hist = histogram:create(10),
    AddIntFun = fun(I, AccH) -> histogram:add(float(I), AccH) end,
    Hist2 = performance_SUITE:iter2_foldl(performance_SUITE:count(), AddIntFun, Hist, "histogram:add (1)"),
    _Hist3 = performance_SUITE:iter2_foldl(performance_SUITE:count(), AddIntFun, Hist2, "histogram:add (2)"),
    ok.

foldl_until(_Config) ->
    H1 = histogram:create(10),
    Values = [1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 5],
    H2 = lists:foldl(fun histogram:add/2, H1, Values),
    %% sum found
    {ok, Val, Sum} = histogram:foldl_until(6, H2),
    ?equals(Val, 3),
    ?equals(Sum, 6),
    %% target value too high
    {fail, Val2, Sum2} = histogram:foldl_until(1000, H2),
    ?equals(Val2, 5),
    ?equals(Sum2, 12),
    %% target value too low
    {ok, Val3, Sum3} = histogram:foldl_until(0, H2),
    ?equals(Val3, nil),
    ?equals(Sum3, 0),
    ok.

foldr_until(_Config) ->
    H1 = histogram:create(10),
    Values = [1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 5],
    H2 = lists:foldl(fun histogram:add/2, H1, Values),
    %% sum found
    {ok, Val, Sum} = histogram:foldr_until(6, H2),
    ?equals(Val, 4),
    ?equals(Sum, 6),
    %% target value too high
    {fail, Val2, Sum2} = histogram:foldr_until(1000, H2),
    ?equals(Val2, 1),
    ?equals(Sum2, 12),
    %% target value too low
    {ok, Val3, Sum3} = histogram:foldr_until(0, H2),
    ?equals(Val3, nil),
    ?equals(Sum3, 0),
    ok.