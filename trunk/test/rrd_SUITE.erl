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
%% @version $Id: api_tx_SUITE.erl 1697 2011-04-29 09:25:23Z schintke $
-module(rrd_SUITE).
-author('schuett@zib.de').
-vsn('$Id: api_tx_SUITE.erl 1697 2011-04-29 09:25:23Z schintke $').

-include("unittest.hrl").

-compile(export_all).

all()   -> [simple_create,
            fill_test,
            create_gauge,
            create_counter,
            create_event,
            create_timing,
            timestamp,
            add_nonexisting_timeslots,
            reduce_timeslots
           ].
suite() -> [ {timetrap, {seconds, 40}} ].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    unittest_helper:start_minimal_procs(Config2, [{rrd_timing_hist_size, 0}], false).

end_per_suite(Config) ->
    unittest_helper:stop_minimal_procs(Config),
    _ = unittest_helper:end_per_suite(Config),
    ok.

simple_create(_Config) ->
    Adds = [{20, 5}, {25, 6}],
    DB0 = rrd:create(10, 10, gauge, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,20}, {0,0,30}, 6}]),
    ok.

fill_test(_Config) ->
    Adds = [{20, 1}, {30, 2}, {40, 3}, {60, 5}],
    DB0 = rrd:create(10, 3, gauge, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,60}, {0,0,70}, 5}, {{0,0,40}, {0,0,50}, 3}]),
    ok.

create_gauge(_Config) ->
    Adds = [{20, 5}, {25, 6}, {30, 1}, {42, 2}],
    DB0 = rrd:create(10, 10, gauge, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}, {{0,0,20}, {0,0,30}, 6}]),
    ok.

create_counter(_Config) ->
    Adds = [{20, 5}, {25, 6}, {30, 1}, {42, 2}],
    DB0 = rrd:create(10, 10, counter, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}, {{0,0,20}, {0,0,30}, 11}]),
    ok.

create_event(_Config) ->
    Adds = [{20, "20"}, {25, "25"}, {30, "30"}, {42, "42"}],
    DB0 = rrd:create(10, 10, event, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,40}, {0,0,50}, [{42, "42"}]}, {{0,0,30}, {0,0,40}, [{30, "30"}]}, {{0,0,20}, {0,0,30}, [{20, "20"}, {25, "25"}]}]),
    ok.

create_timing(_Config) ->
    Adds = [{20, 1}, {25, 3}, {30, 30}, {42, 42}],
    DB0 = rrd:create(10, 10, timing, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1),
            [{{0,0,40}, {0,0,50}, {42, 42*42, 1, 42, 42, {histogram,0,[]}}},
             {{0,0,30}, {0,0,40}, {30, 30*30, 1, 30, 30, {histogram,0,[]}}},
             {{0,0,20}, {0,0,30}, {1 + 3, 1*1 + 3*3, 2, 1, 3, {histogram,0,[]}}}]),
    ok.

timestamp(_Config) ->
    TS = erlang:now(),
    ?equals(TS, rrd:us2timestamp(rrd:timestamp2us(TS))),
    ok.

add_nonexisting_timeslots(_Config) ->
    Adds = [{20, 5}, {25, 6}, {30, 1}, {42, 2}],
    DB0 = rrd:create(10, 10, counter, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    DB2 = rrd:add_nonexisting_timeslots(DB0, DB1),
    ?equals(rrd:dump(DB2), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}, {{0,0,20}, {0,0,30}, 11}]),
    
    DB0a = rrd:create(10, 10, counter, {0,0,40}),
    DB2a = rrd:add_nonexisting_timeslots(DB0a, DB1),
    ?equals(rrd:dump(DB2a), [{{0,0,40}, {0,0,50}, 2}]),
    
    
    DB0b = rrd:create(10, 10, counter, {0,0,50}),
    DB2b = rrd:add_nonexisting_timeslots(DB0b, DB1),
    ?equals(rrd:dump(DB2b), []),
    
    ok.

reduce_timeslots(_Config) ->
    Adds = [{20, 5}, {25, 6}, {30, 1}, {42, 2}],
    DB0 = rrd:create(10, 10, counter, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    DB2 = rrd:reduce_timeslots(1, DB1),
    ?equals(rrd:dump(DB2), [{{0,0,40}, {0,0,50}, 2}]),

    DB2a = rrd:reduce_timeslots(2, DB1),
    ?equals(rrd:dump(DB2a), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}]),

    DB2b = rrd:reduce_timeslots(3, DB1),
    ?equals(rrd:dump(DB2b), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}, {{0,0,20}, {0,0,30}, 11}]),

    DB2c = rrd:reduce_timeslots(4, DB1),
    ?equals(rrd:dump(DB2c), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}, {{0,0,20}, {0,0,30}, 11}]),
    
    ok.

apply({Time, Value}, DB) ->
    rrd:add(Time, Value, DB).
