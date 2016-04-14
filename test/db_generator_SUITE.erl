%  @copyright 2013 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc Unit tests for test/db_generator.erl.
%% @end
%% @version $Id$
-module(db_generator_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [
     tester_get_db3,
     tester_get_db4
     ].

suite() ->
    [
     {timetrap, {seconds, 90}}
    ].

init_per_suite(Config) ->
    tester:register_type_checker({typedef, intervals, interval, []}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, continuous_interval, []}, intervals, is_continuous),
    tester:register_value_creator({typedef, random_bias, generator, []},
                                  random_bias, tester_create_generator, 3),
    tester:register_value_creator({typedef, intervals, interval, []}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval, []}, intervals, tester_create_continuous_interval, 4),
    rt_SUITE:register_value_creator(),
    unittest_helper:start_minimal_procs(Config, [], true).

end_per_suite(Config) ->
    tester:unregister_value_creator({typedef, random_bias, generator, []}),
    tester:unregister_value_creator({typedef, intervals, interval, []}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval, []}),
    tester:unregister_type_checker({typedef, intervals, interval, []}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval, []}),
    rt_SUITE:unregister_value_creator(),
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_generator:get_db/3 and db_generator:get_db/4
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_get_db3(I::intervals:continuous_interval(), ItemCount::1..1000,
                   db_generator:db_distribution()) -> true.
prop_get_db3(Interval, ItemCount0, Distribution0 = {non_uniform, RanGen}) ->
    ItemCount = erlang:min(ItemCount0, random_bias:numbers_left(RanGen)),
    prop_get_db3_(Interval, ItemCount, db_generator:feeder_fix_rangen(Distribution0, ItemCount));
prop_get_db3(Interval, ItemCount, Distribution) ->
    prop_get_db3_(Interval, ItemCount, Distribution).

-spec prop_get_db3_(I::intervals:continuous_interval(), ItemCount::1..1000,
                    db_generator:db_distribution()) -> true.
prop_get_db3_(Interval, ItemCount, Distribution) ->
    Result = db_generator:get_db(Interval, ItemCount, Distribution),
    ?equals([Key || Key <- Result,
                    not intervals:in(Key, Interval)],
            []),
    ?compare(fun erlang:'=<'/2, length(Result), ItemCount),
    ?equals(length(Result), length(lists:usort(Result))),
    ?implies(length(intervals:split(Interval, ItemCount)) == ItemCount,
             ?equals(length(Result), ItemCount)).

-spec prop_get_db4(I::intervals:continuous_interval(), ItemCount::1..1000,
                   db_generator:db_distribution(), Options::[db_generator:option()]) -> true.
prop_get_db4(Interval, ItemCount0, Distribution0 = {non_uniform, RanGen}, Options) ->
    ItemCount = erlang:min(ItemCount0, random_bias:numbers_left(RanGen)),
    prop_get_db4_(Interval, ItemCount, db_generator:feeder_fix_rangen(Distribution0, ItemCount), Options);
prop_get_db4(Interval, ItemCount, Distribution, Options) ->
    prop_get_db4_(Interval, ItemCount, Distribution, Options).


-spec prop_get_db4_(I::intervals:continuous_interval(), ItemCount::1..1000,
                    db_generator:db_distribution(), Options::[db_generator:option()]) -> true.
prop_get_db4_(Interval, ItemCount, Distribution, Options) ->
    Result = db_generator:get_db(Interval, ItemCount, Distribution, Options),
    case proplists:get_value(output, Options, list_key) of
        list_key ->
            ?equals([Key || Key <- Result,
                            not intervals:in(Key, Interval)],
                    []);
        list_keytpl ->
            ?equals([Key || {Key} <- Result,
                            not intervals:in(Key, Interval)],
                    []);
        list_key_val ->
            ?equals([Key || {Key, _Val} <- Result,
                            not intervals:in(Key, Interval)],
                    [])
    end,
    ?compare(fun erlang:'=<'/2, length(Result), ItemCount),
    ?equals(length(Result), length(lists:usort(Result))),
    ?implies(length(intervals:split(Interval, ItemCount)) == ItemCount,
             ?equals(length(Result), ItemCount)).

tester_get_db3(_Config) ->
    prop_get_db3(intervals:new(?MINUS_INFINITY), 844, random),
    tester:test(?MODULE, prop_get_db3, 3, 500, [{threads, 4}]).

tester_get_db4(_Config) ->
    prop_get_db4(intervals:new(?MINUS_INFINITY), 1,
                 {non_uniform, random_bias:binomial(50, 0.06755763133001705)},
                 [{output,list_key}]),
    tester:test(?MODULE, prop_get_db4, 4, 500, [{threads, 4}]).
