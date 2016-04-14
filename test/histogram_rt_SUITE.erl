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

%% @author Maximilian Michels <michels@zib.de>
%% @doc    Unit tests specifically for histogram_rt in addition to the
%%         tests in histogram_SUITE.
%% @end
%% @version $Id$
-module(histogram_rt_SUITE).
-author('michels@zib.de').
-vsn('$Id$').

-include("unittest.hrl").
-include("scalaris.hrl").

-compile(export_all).

all()    ->
    [
     add_keys,
     merge_keys
    ].

suite() -> [ {timetrap, {seconds, 30}} ].

-define(EPSILON, 1.0e-7 * ?RT:n()).

init_per_suite(Config) ->
    rt_SUITE:register_value_creator(),
    unittest_helper:start_minimal_procs(Config, [], true).

end_per_suite(Config) ->
    rt_SUITE:unregister_value_creator(),
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.


-spec prop_add_keys(BaseKey::?RT:key(), Size::1..50, Values::[?RT:key(),...]) -> true.
prop_add_keys(BaseKey, Size, Values0) ->
    H = histogram_rt:create(Size, BaseKey),
    Values = lists:sublist(Values0, Size),
    H2 = lists:foldl(fun histogram_rt:add/2, H, Values),
    %% check results
    SortedValues = lists:sort(fun(Val, Val2) ->
                                      succ_ord_key(Val, Val2, BaseKey)
                              end, Values),
    %ct:pal("BaseKey: ~p SortedValues: ~p Result: ~p", [BaseKey, SortedValues, histogram_rt:get_data(H2)]),
    Result = lists:map(fun(Value) -> {Value, 1} end, SortedValues),
    ?compare(fun(Actual, Expected) ->
                     check_result(Actual, Expected, BaseKey)
             end, histogram_rt:get_data(H2), Result),
    true.

add_keys(_Config) ->
    prop_add_keys(?RT:hash_key("0"), 17, [?RT:hash_key("0")]),
    tester:test(?MODULE, prop_add_keys, 3, 250, [{threads, 2}]).

-spec prop_merge_keys(BaseKey::?RT:key(), Key1::?RT:key(), Key2::?RT:key()) -> true.
prop_merge_keys(BaseKey, Key1, Key2) ->
    %% insert two keys which will
    H = histogram_rt:create(1, BaseKey),
    H2 = histogram_rt:add(Key1, H),
    H3 = histogram_rt:add(Key2, H2),
    %% check results
    SplitKey =
        if Key1 =:= Key2 -> Key1;
           true ->
               case succ_ord_key(Key1, Key2, BaseKey) of
                   true  -> ?RT:get_split_key(Key1, Key2, {1,2});
                   false -> ?RT:get_split_key(Key2, Key1, {1,2})
               end
        end,
    %ct:pal("Key1: ~p (Range: ~p) Key2: ~p (Range: ~p) BaseKey: ~p SplitKey: ~p Result: ~p, Raw: ~p", [Key1, ?RT:get_range(Key1, BaseKey), Key2, ?RT:get_range(Key2, BaseKey), BaseKey, SplitKey, histogram_rt:get_data(H3), histogram:get_data(element(1, H3))]),
    ?compare(fun(Actual, Expected) ->
                     check_result(Actual, Expected, BaseKey)
             end, histogram_rt:get_data(H3), [{SplitKey, 2}]),
    true.

merge_keys(_Config) ->
    tester:test(?MODULE, prop_merge_keys, 3, 250, [{threads, 2}]).

check_result(Actual, Expected, BaseKey) ->
    case rt_SUITE:default_rt_has_chord_keys() of
        true  -> ?equals(Actual, Expected);
        false -> check_elements(Actual, Expected, BaseKey)
    end.

check_elements([], [], _BaseKey) ->
    true;
check_elements([{El1, Count1} | Rest], [{El2, Count2} | Rest2], BaseKey) ->
    %ct:pal("El: ~p, El2:~p", [El1, El2]),
    case nodelist:succ_ord_id(El1, El2, BaseKey) of
        true -> Range = ?RT:get_range(El1, El2);
        false -> Range = ?RT:get_range(El2, El1)
    end,
    %ct:pal("Range: ~p", [Range]),
    %ct:pal("Check: ~p", [Range < ?EPSILON orelse El1 =:= El2]),
    ?assert_w_note(Range < ?EPSILON orelse El1 =:= El2,
                   {Range, '>=', ?EPSILON, ';', El1, '=/=', El2}),
    ?equals(Count1, Count2),
    check_elements(Rest, Rest2, BaseKey).

%% @doc Like nodelist:succ_ord_id/3 but here the BaseKey is the biggest
%% possible key and not the smallest.
-spec succ_ord_key(K1::?RT:key(), K2::?RT:key(), BaseKey::?RT:key()) -> boolean().
succ_ord_key(K1, K2, BaseKey) ->
    (K1 > BaseKey andalso K2 > BaseKey andalso K1 =< K2) orelse
    (K1 < BaseKey andalso K2 =< BaseKey andalso K1 =< K2) orelse
    (K1 > BaseKey andalso K2 =< BaseKey).
