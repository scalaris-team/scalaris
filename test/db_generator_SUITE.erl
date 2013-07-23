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
%% @doc Unit tests for src/db_generator.erl.
%% @end
%% @version $Id$
-module(db_generator_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [tester_get_db3, tester_get_db4
     ].

suite() ->
    [
     {timetrap, {seconds, 90}}
    ].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    tester:register_type_checker({typedef, intervals, interval}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, continuous_interval}, intervals, is_continuous),
    tester:register_value_creator({typedef, random_bias, generator},
                                  random_bias, tester_create_generator, 3),
    tester:register_value_creator({typedef, intervals, interval}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval}, intervals, tester_create_continuous_interval, 4),
    Config2.

end_per_suite(Config) ->
    tester:unregister_value_creator({typedef, random_bias, generator}),
    tester:unregister_value_creator({typedef, intervals, interval}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval}),
    tester:unregister_type_checker({typedef, intervals, interval}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval}),
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_generator:get_db/3 and db_generator:get_db/3
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_get_db3(I::intervals:continuous_interval(), ItemCount::1..1000,
                   db_generator:db_distribution()) -> true.
prop_get_db3(Interval, ItemCount, Distribution0) ->
    Distribution = db_generator:feeder_fix_rangen(Distribution0, ItemCount),
    Result = db_generator:get_db(Interval, ItemCount, Distribution),
    ?equals([Key || Key <- Result,
                    not intervals:in(Key, Interval)],
            []),
    ?compare(fun erlang:'=<'/2, length(Result), ItemCount),
    ?implies(length(intervals:split(Interval, ItemCount)) == ItemCount,
             ?equals(length(Result), ItemCount)).

-spec prop_get_db4(I::intervals:continuous_interval(), ItemCount::1..1000,
                   db_generator:db_distribution(), Options::[db_generator:option()]) -> boolean().
prop_get_db4(Interval, ItemCount, Distribution0, Options) ->
    Distribution = db_generator:feeder_fix_rangen(Distribution0, ItemCount),
    Result = db_generator:get_db(Interval, ItemCount, Distribution, Options),
    case proplists:get_value(output, Options, list_key) of
        list_key ->
            ?equals([Key || Key <- Result,
                            not intervals:in(Key, Interval)],
                    []);
        list_key_val ->
            ?equals([Key || {Key, _Val} <- Result,
                            not intervals:in(Key, Interval)],
                    [])
    end,
    ?compare(fun erlang:'=<'/2, length(Result), ItemCount),
    ?implies(length(intervals:split(Interval, ItemCount)) == ItemCount,
             ?equals(length(Result), ItemCount)).

tester_get_db3(_Config) ->
    tester:test(?MODULE, prop_get_db3, 3, 500, [{threads, 4}]).

tester_get_db4(_Config) ->
    tester:test(?MODULE, prop_get_db4, 4, 500, [{threads, 4}]).
