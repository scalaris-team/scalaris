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
            resize,
            insert,
            get_num_inserts,
            find_smallest_interval,
            merge_interval,
            perf_add,
            find_largest_window
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
    H = histogram:create(3),
    Values = [1.25, 5.0, 5.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1.25, 1}, {5.0, 2}]),
    H3 = histogram:add(7.0, H2),
    H4 = lists:foldl(fun histogram:add/2, H3, Values),
    ?equals(histogram:get_data(H4), [{1.25, 2}, {5.0, 4}, {7.0, 1}]),
    ok.

add3_identical(_Config) ->
    H = histogram:create(10),
    Values = [{2.0, 1}, {3.5, 2}, {3.0, 1}, {2.0, 6}, {1.0, 3}],
    H2 = lists:foldl(fun({Value, Count}, Histogram) ->
                             histogram:add(Value, Count, Histogram)
                     end,
                     H, Values),
    ?equals(histogram:get_data(H2), [{1.0, 3}, {2.0, 7}, {3.0, 1}, {3.5, 2}]),
    ok.

resize(_Config) ->
    H = histogram:create(3),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1.0,1}, {2.0,1}, {3.25,2}]),
    ok.

insert(_Config) ->
    H = histogram:create(10),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1.0,1}, {2.0,1}, {3.0,1}, {3.5,1}]),
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
    ?equals(3.5, histogram:find_smallest_interval(histogram:get_data(H1b))),
    Values2a = [4.0, 2.5, 2.0, 1.0],
    H2b = lists:foldl(fun histogram:add/2, H1a, Values2a),
    ?equals(2.5, histogram:find_smallest_interval(histogram:get_data(H2b))),
    ok.

merge_interval(_Config) ->
    H = histogram:create(10),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    MinInterval = histogram:find_smallest_interval(histogram:get_data(H2)),
    H3 = histogram:merge_interval(MinInterval, histogram:get_data(H2)),
    ?equals(3.5, MinInterval),
    ?equals(H3, [{1.0,1}, {2.0,1}, {3.25,2}]),
    ok.

perf_add(_Config) ->
    Hist = histogram:create(10),
    AddIntFun = fun(I, AccH) -> histogram:add(float(I), AccH) end,
    Hist2 = performance_SUITE:iter2_foldl(performance_SUITE:count(), AddIntFun, Hist, "histogram:add (1)"),
    _Hist3 = performance_SUITE:iter2_foldl(performance_SUITE:count(), AddIntFun, Hist2, "histogram:add (2)"),
    ok.

find_largest_window(_Config) ->
    H1 = histogram:create(10),
    Values = [1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 5],
    H2 = lists:foldl(fun histogram:add/2, H1, Values),
    {MaxPos, MaxVal} = histogram:find_largest_window(2, H2),
    ?equals(MaxVal, 6),
    ?equals(MaxPos, 4),
    Values2 = [3, 3],
    H3 = lists:foldl(fun histogram:add/2, H2, Values2),
    {MaxPos2, MaxVal2} = histogram:find_largest_window(2, H3),
    ?equals(MaxVal2, 7),
    ?equals(MaxPos2, 3),
    F = fun(N, WindowSize, Result) ->
                ct:pal("Histogram size: ~p, Adding ~p, SlidingWindow ~p~n", [N, N, WindowSize]),
                H4 = histogram:create(N),
                Values3 = lists:seq(1, N),
                H5 = lists:foldl(fun histogram:add/2, H4, Values3),
                {Time, Result} = util:tc(fun() -> histogram:find_largest_window(WindowSize, H5) end),
                ct:pal("Time: ~p~n", [Time])
        end,
    F(100, 20, {1, 20}),
    F(1000, 100, {1, 100}),
    F(10000, 300, {1, 300}),
    ok.
