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
     {group, test_cases}
    ].

groups() ->
    [
     {test_cases, [{repeat, 50}], [add_keys, merge_keys]}
    ].

suite() -> [ {timetrap, {seconds, 30}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

add_keys(_Config) ->
    %% insert keys
    BaseKey = get_rnd_key(),
    H = histogram_rt:create(10, BaseKey),
    Values = [number_to_key(N) || N <- lists:seq(1, 10)],
    H2 = lists:foldl(fun histogram_rt:add/2, H, Values),
    %% check results
    SortedValues = lists:sort(fun(Val, Val2) ->
                                      nodelist:succ_ord_id(Val, Val2, BaseKey)
                              end, Values),
    Result = lists:map(fun(Value) -> {Value, 1} end, SortedValues),
    ?equals(histogram_rt:get_data(H2), Result),
    ok.

merge_keys(_Config) ->
    %% insert two keys which will
    BaseKey = get_rnd_key(),
    H = histogram_rt:create(1, BaseKey),
    Key1 = get_rnd_key(),
    Key2 = get_rnd_key(),
    H2 = histogram_rt:add(Key1, H),
    H3 = histogram_rt:add(Key2, H2),
    %% check results
    case nodelist:succ_ord_id(Key1, Key2, BaseKey) of
        true -> SplitKey = ?RT:get_split_key(Key1, Key2, {1,2});
        false -> SplitKey = ?RT:get_split_key(Key2, Key1, {1,2})
    end,
    ?equals(histogram_rt:get_data(H3), [{SplitKey, 2}]),
    ok.

get_rnd_key() ->
    ?RT:get_random_in_interval({'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ')'}).

number_to_key(Val) ->
    ?RT:hash_key(erlang:integer_to_list(Val)).