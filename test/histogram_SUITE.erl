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
%% @version $Id$
-module(histogram_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-include("unittest.hrl").

-compile(export_all).

all()   -> [
            add,
            resize,
            insert,
            find_smallest_interval,
            merge_interval
           ].

suite() -> [ {timetrap, {seconds, 40}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

add(_Config) ->
    H = histogram:create(10),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(histogram:get_data(H2), [{1.0,1}, {2.0,1}, {3.0,1}, {3.5,1}]),
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

find_smallest_interval(_Config) ->
    H = histogram:create(10),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    ?equals(0.5, histogram:find_smallest_interval(histogram:get_data(H2))),
    ok.

merge_interval(_Config) ->
    H = histogram:create(10),
    Values = [3.5, 3.0, 2.0, 1.0],
    H2 = lists:foldl(fun histogram:add/2, H, Values),
    MinInterval = histogram:find_smallest_interval(histogram:get_data(H2)),
    H3 = histogram:merge_interval(MinInterval, histogram:get_data(H2)),
    ?equals(0.5, MinInterval),
    ?equals(H3, [{1.0,1}, {2.0,1}, {3.25,2}]),
    ok.
