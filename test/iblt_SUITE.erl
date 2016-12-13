%  @copyright 2010-2016 Zuse Institute Berlin

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

%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%% @doc    Tests for iblt module (invertible bloom lookup table).
%% @end
-module(iblt_SUITE).
-author('mlange@informatik.hu-berlin.de').

-compile(export_all).

-include("scalaris.hrl").
-include("client_types.hrl").
-include("unittest.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() -> [
          tester_insert,
          tester_delete,
          %tester_prime_vs_noprime,
          tester_prime_option,
          tester_list_entries
         ].

suite() ->
    [
     {timetrap, {seconds, 120}}
    ].

init_per_suite(Config) ->
    rt_SUITE:register_value_creator(),
    unittest_helper:start_minimal_procs(Config, [], true).

end_per_suite(Config) ->
    rt_SUITE:unregister_value_creator(),
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_insert(10..100, ?RT:key(), client_version()) -> true.
prop_insert(CellCount, Key, Value) ->
    IBLT = iblt:new(get_hfs(), CellCount),
    IBLT2 = iblt:insert(IBLT, Key, Value),
    ?equals(iblt:get_prop(item_count, IBLT), 0),
    ?equals(iblt:get_prop(item_count, IBLT2), 1),
    ?equals(iblt:get(IBLT, Key), not_found),
    ?equals(iblt:get(IBLT2, Key), Value).

tester_insert(_) ->
    tester:test(?MODULE, prop_insert, 3, 1000, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_delete(10..100, ?RT:key(), client_version()) -> true.
prop_delete(CellCount, Key, Value) ->
    IBLT = iblt:new(get_hfs(), CellCount),
    IBLT2 = iblt:insert(IBLT, Key, Value),
    ?equals(iblt:get_prop(item_count, IBLT2), 1),
    ?equals(iblt:get(IBLT2, Key), Value),
    IBLT3 = iblt:delete(IBLT2, Key, Value),
    ?equals(iblt:get_prop(item_count, IBLT3), 0),
    ?equals(iblt:get(IBLT3, Key), not_found),
    %detail check: count of every cell must be zero
    {iblt, _, T, _, _, _} = IBLT3,
    Count = lists:foldl(fun({_, Col}, Acc) ->
                                Acc + lists:foldl(fun({C, _, _, _, _}, A) -> A+C end, 0, Col)
                        end, 0, T),
    ?equals(Count, 0).

tester_delete(_) ->
    tester:test(?MODULE, prop_delete, 3, 10000, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tester_prime_vs_noprime(_) ->
    Calls = 10000,
    Pid = spawn(?MODULE, collector, [{0, 0, 0, 0}, {0, 0}]),
    register(pvp_iblt_test, Pid),
    ct:pal("tester_prime_vs_noprime - Calls=~p", [Calls]),
    tester:test(?MODULE, prop_prime, 2, Calls, [{threads, 1}]),
    pvp_iblt_test ! {reset, "Prime-ColSize - COLLISIONS:"},
    tester:test(?MODULE, prop_prime, 2, Calls, [{threads, 1}]),
    pvp_iblt_test ! {reset, "Prime-ColSize - COLLISIONS:"},
    tester:test(?MODULE, prop_prime, 2, Calls, [{threads, 1}]),
    pvp_iblt_test ! {reset, "Prime-ColSize - COLLISIONS:"},

    tester:test(?MODULE, prop_noprime, 2, Calls, [{threads, 1}]),
    pvp_iblt_test ! {reset, "NON-Prime-ColSize - COLLISIONS:"},
    tester:test(?MODULE, prop_noprime, 2, Calls, [{threads, 1}]),
    pvp_iblt_test ! {reset, "NON-Prime-ColSize - COLLISIONS:"},
    tester:test(?MODULE, prop_noprime, 2, Calls, [{threads, 1}]),
    pvp_iblt_test ! {reset, "NON-Prime-ColSize - COLLISIONS:"},
    true.

-spec prop_prime(10..100, [{?RT:key(), client_version()}]) -> true.
prop_prime(Cells, Items) -> prop_get(Cells, Items, [prime]).

-spec prop_noprime(10..100, [{?RT:key(), client_version()}]) -> true.
prop_noprime(Cells, Items) -> prop_get(Cells, Items, []).

-spec prop_get(10..100, [{?RT:key(), client_version()}], iblt:options()) -> true.
prop_get(CellCount, Items, Options) ->
    IBLT = lists:foldl(fun({Key, Ver}, _IBLT) ->
                               iblt:insert(_IBLT, Key, Ver)
                       end,
                       iblt:new(get_hfs(), erlang:round(1.5 * CellCount), Options),
                       Items),
    ?equals(iblt:get_prop(item_count, IBLT), length(Items)),
    Found = lists:foldl(fun({Key, _}, Sum) ->
                                case iblt:get(IBLT, Key) of
                                    not_found -> Sum;
                                    _ -> Sum + 1
                                end
                        end, 0, Items),
    _ = if
            Found =:= 0 andalso length(Items) > 0 -> pvp_iblt_test ! count_zero;
            Found > 0 andalso Found =:= length(Items) -> pvp_iblt_test ! count_all;
            length(Items) > 0 -> pvp_iblt_test ! {some, Found, length(Items)};
            true -> ok
        end,
    _ = pvp_iblt_test ! {found_sum, Found},
    _ = pvp_iblt_test ! {items_sum, length(Items)},
    true.

collector({ZeroFound, SomeFound, SomeAll, AllFound} = D, {FoundSum, ItemsSum} = A) ->
    receive
        count_zero -> collector({ZeroFound + 1, SomeFound, SomeAll, AllFound}, A);
        count_all -> collector({ZeroFound, SomeFound, SomeAll, AllFound + 1}, A);
        {found_sum, FS} -> collector(D, {FoundSum + FS, ItemsSum});
        {items_sum, IS} -> collector(D, {FoundSum, ItemsSum + IS});
        {some, Found, All} -> collector({ZeroFound, SomeFound + Found, SomeAll + All, AllFound}, A);
        {reset, Title} -> ct:pal("~s: SumFound=~p/~p (~f%) | DETAILS:  ZeroFound=~p ; AllFound=~p ; Some=~p/~p (~f%)",
                                 [Title,
                                  FoundSum, ItemsSum, 100*(FoundSum / ItemsSum),
                                  ZeroFound, AllFound,
                                  SomeFound, SomeAll, 100*(SomeFound / SomeAll)]),
                          collector({0, 0, 0, 0}, {0, 0})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_prime_option(10..5000) -> true.
prop_prime_option(Cells) ->
    PIBLT = iblt:new(get_hfs(), Cells, [prime]),
    ?assert(prime:is_prime(iblt:get_prop(col_size, PIBLT))).

tester_prime_option(_) ->
    tester:test(?MODULE, prop_prime_option, 1, 20, [{threads, 2}]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_list_entries(?RT:key(), ?RT:key(), 10..200) -> true.
prop_list_entries(L, R, Count) ->
    I = intervals:new('[', L, R, ']'),
    DB = db_generator:get_db(I, Count, uniform, [{output, list_key_val}]),
    IBLT = lists:foldl(fun({Key, Val}, Acc) -> iblt:insert(Acc, Key, Val) end,
                       iblt:new(get_hfs(), erlang:round(1.5 * Count)),
                       DB),
    List = iblt:list_entries(IBLT),
    Found = length(List),
     ct:pal("--IBLT--
             ItemsInserted=~p/~p
             ListEntrySize=~p/~p (~f%)
             FPR=~p",
            [iblt:get_prop(item_count, IBLT), Count,
             Found, Count, (Found / Count) * 100,
             iblt:get_fpr(IBLT)]),
    ?assert(Found > 0 andalso Found =< Count).

tester_list_entries(_) ->
    tester:test(?MODULE, prop_list_entries, 3, 2, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_hfs() -> 5.
