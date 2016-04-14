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
-module(rrd_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-include("types.hrl").
-include("unittest.hrl").

-compile(export_all).

all()   -> [simple_create,
            fill_test,
            create_gauge,
            create_counter,
            create_event,
            create_timing,
            add_nonexisting_timeslots,
            reduce_timeslots,
            {group, tester_tests}
           ].

groups() ->
    [{tester_tests, [], 
      [
       tester_empty_rrd,
       tester_counter_rrd,
       tester_gauge_rrd]
     }
    ].

suite() -> [ {timetrap, {seconds, 40}} ].

init_per_suite(Config) ->
    unittest_helper:start_minimal_procs(Config, [{rrd_timing_hist_size, 0}], false).

end_per_suite(Config) ->
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

init_per_group(Group, Config) ->
    unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    unittest_helper:end_per_group(Group, Config).

simple_create(_Config) ->
    Adds = [{20, 5}, {25, 6}],
    DB0 = rrd:create(10, 10, gauge, {0,0,0}),
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,20}, {0,0,30}, 6}]),
    ok.

fill_test(_Config) ->
    Adds = [{20, 1}, {30, 2}, {40, 3}, {60, 5}],
    DB0 = rrd:create(10, 3, gauge, {0,0,0}),
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,60}, {0,0,70}, 5}, {{0,0,40}, {0,0,50}, 3}]),
    ok.

create_gauge(_Config) ->
    Adds = [{20, 5}, {25, 6}, {30, 1}, {42, 2}],
    DB0 = rrd:create(10, 10, gauge, {0,0,0}),
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}, {{0,0,20}, {0,0,30}, 6}]),
    ok.

create_counter(_Config) ->
    Adds = [{20, 5}, {25, 6}, {30, 1}, {42, 2}],
    DB0 = rrd:create(10, 10, counter, {0,0,0}),
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,40}, {0,0,50}, 2}, {{0,0,30}, {0,0,40}, 1}, {{0,0,20}, {0,0,30}, 11}]),
    ok.

create_event(_Config) ->
    Adds = [{20, "20"}, {25, "25"}, {30, "30"}, {42, "42"}],
    DB0 = rrd:create(10, 10, event, {0,0,0}),
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1), [{{0,0,40}, {0,0,50}, [{42, "42"}]}, {{0,0,30}, {0,0,40}, [{30, "30"}]}, {{0,0,20}, {0,0,30}, [{20, "20"}, {25, "25"}]}]),
    ok.

create_timing(_Config) ->
    Adds = [{20, 1}, {25, 3}, {30, 30}, {42, 42}],
    DB0 = rrd:create(10, 10, {timing, us}, {0,0,0}),
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1),
            [{{0,0,40}, {0,0,50}, {42, 42*42, 1, 42, 42, {histogram,0,[],0,0}}},
             {{0,0,30}, {0,0,40}, {30, 30*30, 1, 30, 30, {histogram,0,[],0,0}}},
             {{0,0,20}, {0,0,30}, {1 + 3, 1*1 + 3*3, 2, 1, 3, {histogram,0,[],0,0}}}]),
    ok.

add_nonexisting_timeslots(_Config) ->
    Adds = [{20, 5}, {25, 6}, {30, 1}, {42, 2}],
    DB0 = rrd:create(10, 10, counter, {0,0,0}),
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
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
    DB1 = lists:foldl(fun ?MODULE:apply/2, DB0, Adds),
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

%% @doc Performance evaluating of rrd:add_now/2 with a timing type.
%%      Useful for profiling, e.g. with fprof.
timing_perf() ->
    Init = rrd:create(60 * 1000000, 1, {timing, count}),
    _ = lists:foldl(fun(_, Old) -> rrd:add_now(1, Old) end, Init, lists:seq(1, 10000)),
    ok.

%% @doc Converts a time (either a tuple as returned by erlang:now/0 or a number
%%      representing the number of microseconds since epoch) to the number of
%%      microseconds since epoch.
-spec time2us(erlang_timestamp() | rrd:internal_time()) -> rrd:internal_time().
time2us({_, _, _} = Time) ->
    util:timestamp2us(Time);
time2us(Time) ->
    Time.

-spec prop_empty_rrd(SlotLength::rrd:timespan(), Count::1..10, Type::rrd:timeseries_type(),
                     StartTime::erlang_timestamp() | rrd:internal_time(),
                     Offsets::[non_neg_integer(),...],
                     Times::[erlang_timestamp() | rrd:internal_time(),...]) -> true.
prop_empty_rrd(SlotLength, Count, Type, StartTime, Offsets, Times) ->
    R = rrd:create(SlotLength, Count, Type, StartTime),
    StartTime_us = time2us(StartTime),
    ?equals(rrd:get_slot_start(0, R), StartTime_us),
    ?equals(rrd:get_type(R), Type),
    ?equals(rrd:get_slot_length(R), SlotLength),
    ?equals(rrd:get_current_time(R), StartTime_us),
    ?equals(rrd:dump(R), []),
    _ = [?equals(rrd:get_value(R, Time), undefined) || Time <- Times],
    _ = [?equals(rrd:get_value_by_offset(R, Offset), undefined) || Offset <- Offsets],
    true.

-type rrd_data() :: [{erlang_timestamp() | rrd:internal_time(), number()},...].
-type rrd_data_us() :: [{rrd:internal_time(), number()},...].

tester_empty_rrd(_Config) ->
    tester:test(?MODULE, prop_empty_rrd, 6, 1000, [{threads, 2}]).

-spec round_us_time(Time::rrd:internal_time(), SlotLength::rrd:timespan(),
                    StartTime::rrd:internal_time()) -> rrd:internal_time().
%% round_us_time(Time, SlotLength, StartTime) when Time < StartTime ->
%%     StartTime;
round_us_time(Time, SlotLength, StartTime) ->
    case (Time - StartTime) rem SlotLength of
        X when X >=  0 -> Time - X;
        X              -> Time - (X + SlotLength)
    end.
%%     StartTime + (Time div SlotLength) * SlotLength.
%%     Time - ((Time - StartTime) rem SlotLength).

%% @doc Merges consecutive data for a 'counter' rrd into a single date item.
%%      PRE: Data_us2 must be sorted (by its time components) using a stable
%%           sort.
-spec merge_conseq_data(Data_us2::rrd_data_us(), gauge | counter, [] | rrd_data_us()) -> rrd_data_us().
merge_conseq_data([], _, Result) ->
    lists:reverse(Result);
merge_conseq_data([X | TD], Type, []) ->
    merge_conseq_data(TD, Type, [X]);
merge_conseq_data([{Time, X1} | TD], counter = Type, [{Time, X2} | TR]) ->
    merge_conseq_data(TD, Type, [{Time, X1 + X2} | TR]);
merge_conseq_data([{Time, X1} | TD], gauge = Type, [{Time, _X2} | TR]) ->
    merge_conseq_data(TD, Type, [{Time, X1} | TR]);
merge_conseq_data([X | TD], Type, Result) ->
    merge_conseq_data(TD, Type, [X | Result]).

-spec prop_counter_gauge_rrd(SlotLength::rrd:timespan(), Count::1..10,
                             StartTime::erlang_timestamp() | rrd:internal_time(),
                             Data::rrd_data(), Offsets::[non_neg_integer(),...],
                             Times::[erlang_timestamp() | rrd:internal_time(),...],
                             Type::gauge | counter) -> true.
prop_counter_gauge_rrd(SlotLength, Count, StartTime, Data, _Offsets, _Times, Type) ->
    R0 = rrd:create(SlotLength, Count, Type, StartTime),
    % we need sorted data (old data is not inserted into the rrd, only data in the current time slot)
    % can not reliably sort the time tuples -> only use rrd:internal_time() but
    Data_us = lists:keysort(1, [{time2us(TimeX), CountX} || {TimeX, CountX} <- Data]),
    R1 = lists:foldl(fun ?MODULE:apply/2, R0, Data_us),
    StartTime_us = time2us(StartTime),
%%     ct:pal("StartTime_us: ~.0p~n", [StartTime_us]),
    ?equals(rrd:get_type(R1), Type),
    ?equals(rrd:get_slot_length(R1), SlotLength),
%%     ct:pal("Data_us: ~.0p~n", [Data_us]),
    % round down to StartTime + x * SlotLength:
    Data_us2 = [{round_us_time(TimeX, SlotLength, StartTime_us), CountX} || {TimeX, CountX} <- Data_us],
%%     ct:pal("Data_us2: ~.0p~n", [Data_us2]),
    Times_us2 = [TimeX || {TimeX, _} <- Data_us2],
    CurTime_us = lists:max([StartTime_us | Times_us2]),
    ?equals(rrd:get_slot_start(0, R1), CurTime_us),
    ?equals(rrd:get_current_time(R1), CurTime_us),
    
    % check data:
    MinTime_us = erlang:max(StartTime_us - 1, CurTime_us - Count * SlotLength),
    Data_us2_merged = merge_conseq_data(Data_us2, Type, []),
%%     ct:pal("Data_us2_merged: ~.0p~n", [Data_us2_merged]),
    {DataIn, _DataOut} =
        lists:partition(fun({TimeX, _}) ->
                                TimeX > MinTime_us andalso
                                    TimeX =< CurTime_us
                        end, Data_us2_merged),
%%     ct:pal("DataOut: ~.0p~n", [DataOut]),
%%     ct:pal("DataIn: ~.0p~n", [DataIn]),
    Dump = rrd:dump_with(R1, fun(_, FromX, _ToX, ValueX) -> {FromX, ValueX} end),
    % note: dump returns the newest value first
    ?equals(Dump, lists:reverse(DataIn)),
%%     ?equals(rrd:dump(R), []),
%%     _ = [?equals(rrd:get_value(R, Time), undefined) || Time <- Times],
%%     _ = [?equals(rrd:get_value_by_offset(R, Offset), undefined) || Offset <- Offsets],
    true.

-spec prop_counter_rrd(SlotLength::rrd:timespan(), Count::1..10,
                       StartTime::erlang_timestamp() | rrd:internal_time(),
                       Data::rrd_data(), Offsets::[non_neg_integer(),...],
                       Times::[erlang_timestamp() | rrd:internal_time(),...]) -> true.
prop_counter_rrd(SlotLength, Count, StartTime, Data, Offsets, Times) ->
    prop_counter_gauge_rrd(SlotLength, Count, StartTime, Data, Offsets, Times, counter).

tester_counter_rrd(_Config) ->
    rrd_SUITE:prop_counter_rrd(87, 10, {130,381,3}, [{78,153},{64,-0.236915872440062}], [2,1000], [{388,0,5000}]),
    rrd_SUITE:prop_counter_rrd(85, 6, 4, [{107,4},{52,0.9981532170299772},{302,4},{232,417}], [126,42,3,4], [{143,255,5},209,138,{5,407,0}]),
    tester:test(?MODULE, prop_counter_rrd, 6, 5000, [{threads, 2}]).

-spec prop_gauge_rrd(SlotLength::rrd:timespan(), Count::1..10,
                       StartTime::erlang_timestamp() | rrd:internal_time(),
                       Data::rrd_data(), Offsets::[non_neg_integer(),...],
                       Times::[erlang_timestamp() | rrd:internal_time(),...]) -> true.
prop_gauge_rrd(SlotLength, Count, StartTime, Data, Offsets, Times) ->
    prop_counter_gauge_rrd(SlotLength, Count, StartTime, Data, Offsets, Times, gauge).

tester_gauge_rrd(_Config) ->
    rrd_SUITE:prop_gauge_rrd(87, 10, {130,381,3}, [{78,153},{64,-0.236915872440062}], [2,1000], [{388,0,5000}]),
    rrd_SUITE:prop_gauge_rrd(85, 6, 4, [{107,4},{52,0.9981532170299772},{302,4},{232,417}], [126,42,3,4], [{143,255,5},209,138,{5,407,0}]),
    tester:test(?MODULE, prop_gauge_rrd, 6, 5000, [{threads, 2}]).
