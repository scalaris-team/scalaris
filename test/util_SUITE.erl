% @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc    Unit tests for src/util.erl.
%% @end
%% @version $Id$
-module(util_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("types.hrl").

-dialyzer([{[no_opaque, no_return], largest_smaller_than/1},
           {no_fail_call, lists_remove_at_indices/1}]).

all() ->
    [min_max, largest_smaller_than, gb_trees_foldl,
     repeat, repeat_collect, repeat_accumulate,
     repeat_p, repeat_p_collect, repeat_p_accumulate,
     random_subsets,
     tester_minus_all, tester_minus_all_sort,
     tester_minus_first, tester_minus_first_sort,
     tester_par_map2, tester_par_map3,
     lists_remove_at_indices,
     tester_timestamp,
     rrd_combine_timing_slots_handle_empty_rrd,
     rrd_combine_timing_slots_simple,
     rrd_combine_timing_slots_subset,
     rrd_combine_gauge_slots_handle_empty_rrd,
     rrd_combine_gauge_slots_simple,
     rrd_combine_gauge_slots_subset,
     sublist, tester_sublist3
 ].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.


min_max(_Config) ->
    ?equals(util:min(1, 2), 1),
    ?equals(util:min(2, 1), 1),
    ?equals(util:min(1, 1), 1),
    ?equals(util:max(1, 2), 2),
    ?equals(util:max(2, 1), 2),
    ?equals(util:max(1, 1), 1),
    ok.

largest_smaller_than(_Config) ->
    KVs = [{1, 1}, {2, 2}, {4, 4}, {8, 8}, {16, 16}, {32, 32}, {64, 64}],
    Tree = gb_trees:from_orddict(KVs),
    ?equals(util:gb_trees_largest_smaller_than(0, Tree), nil),
    ?equals(util:gb_trees_largest_smaller_than(1, Tree), nil),
    ?equals(util:gb_trees_largest_smaller_than(2, Tree), {value, 1, 1}),
    ?equals(util:gb_trees_largest_smaller_than(3, Tree), {value, 2, 2}),
    ?equals(util:gb_trees_largest_smaller_than(7, Tree), {value, 4, 4}),
    ?equals(util:gb_trees_largest_smaller_than(9, Tree), {value, 8, 8}),
    ?equals(util:gb_trees_largest_smaller_than(31, Tree), {value, 16, 16}),
    ?equals(util:gb_trees_largest_smaller_than(64, Tree), {value, 32, 32}),
    ?equals(util:gb_trees_largest_smaller_than(65, Tree), {value, 64, 64}),
    ?equals(util:gb_trees_largest_smaller_than(1000, Tree), {value, 64, 64}),
    ok.

gb_trees_foldl(_Config) ->
    KVs = [{1, 1}, {2, 2}, {4, 4}, {8, 8}, {16, 16}, {32, 32}, {64, 64}],
    Tree = gb_trees:from_orddict(KVs),
    ?assert(util:gb_trees_foldl(fun (K, K, Acc) ->
                                        Acc + K
                                end,
                                0,
                                Tree) =:= 127).

repeat(_) ->
    util:repeat(fun() -> io:format("#s_repeat#~n") end, [], 5),
    io:format("s_repeat_test successful if #s_repeat# was printed 5 times~n"),    
    ok.

repeat_collect(_) ->
    Times = 3,
    Result = util:repeat(fun(X) -> X * X end, [Times], Times, [collect]),
    ?equals(Result, [9, 9, 9]),
    ok.

repeat_accumulate(_) ->
    Times = 5,
    A = util:repeat(fun(X) -> X * X end, [Times], Times,
                    [{accumulate, fun(X, Y) -> X + Y end, 0}]),
    ?equals(A, Times*Times*Times),
    B = util:repeat(fun(X) -> X * X end, [Times], Times,
                    [{accumulate, fun(X, Y) -> X + Y end, 1000}]),
    ?equals(B, 1000 + Times*Times*Times),      
    ok.

repeat_p(_) ->
    Times = 5,
    util:repeat(
      fun(Caller) -> io:format("~w #p_repeat_test# called by ~w", [self(), Caller]) end, 
      [self()], 
      Times, [parallel]),
    io:format("p_repeat_test successful if ~B different pids printed #p_repeat#.", [Times]),
    ok.

repeat_p_collect(_) ->
    Times = 3,
    A = util:repeat(fun(X) -> X * X end, [Times], Times, [parallel, collect]),
    ?equals(A, [9, 9, 9]),
    ok.

repeat_p_accumulate(_) ->
    Times = 15,
    A = util:repeat(fun(X) -> X * X end, [Times], Times,
                    [parallel, {accumulate, fun(X, Y) -> X + Y end, 0}]),     
    ?equals(A, Times*Times*Times),
    B = util:repeat(fun(X) -> X * X end, [Times], Times,
                    [parallel, {accumulate, fun(X, Y) -> X + Y end, 1000}]),     
    ?equals(B, 1000 + Times*Times*Times),   
    ok.

-spec rand_subsets_check(Rand1::[integer()], Rand2::[integer()], Rand3::[integer()]) -> ok.
rand_subsets_check(Rand1, Rand2, Rand3) ->
    Rand1sort = lists:sort(Rand1),
    Rand2sort = lists:sort(Rand2),
    Rand3sort = lists:sort(Rand3),
    ?assert_w_note(Rand1 =/= Rand2 orelse Rand1 =/= Rand3 orelse Rand2 =/= Rand3,
                   {Rand1, Rand2, Rand3}),
    ?assert_w_note(Rand1sort =/= Rand2sort orelse Rand1sort =/= Rand3sort orelse Rand2sort =/= Rand3sort,
                   {Rand1sort, Rand2sort, Rand3sort}),
    ok.

random_subsets(_) ->
    % assume that when selecting 10 out of 1000 elements, at least one of 3
    % calls yields a result different to the others
    List = lists:seq(1, 1000),
    rand_subsets_check(util:random_subset(10, List),
                       util:random_subset(10, List),
                       util:random_subset(10, List)),
    rand_subsets_check(element(2, util:pop_randomsubset(10, List)),
                       element(2, util:pop_randomsubset(10, List)),
                       element(2, util:pop_randomsubset(10, List))),

    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% util:minus_all/2 and util:minus_first/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Checks that all intended items are deleted using util:minus_all/2.
%%      Note: this is kindof redundant to prop_minus_all_sort/2 but a cleaner
%%      approach avoiding a re-implementation of util:minus_all/2.
-spec prop_minus_all(List::[T], Excluded::[T]) -> true
    when is_subtype(T, any()).
prop_minus_all(List, Excluded) ->
    Result = util:minus_all(List, Excluded),
    _ = [begin
             case lists:member(L, Excluded) of
                 true  -> ?equals_w_note([R || R <- Result, R =:= L], [], io_lib:format("~.0p should have been deleted", [L]));
                 false -> ?equals_w_note([R || R <- Result, R =:= L], [R || R <- List, R =:= L], io_lib:format("Number of ~.0p should remain the same", [L]))
             end
         end || L <- List],
    true.

%% @doc Checks that the order of items stays the same using util:minus_all/2.
-spec prop_minus_all_sort(List::[T], Excluded::[T]) -> true
    when is_subtype(T, any()).
prop_minus_all_sort(List, Excluded) ->
    Result = util:minus_all(List, Excluded),
    prop_minus_all_sort_helper(Result, List, Excluded).

-spec prop_minus_all_sort_helper(Result::[T], List::[T], Excluded::[T]) -> true
    when is_subtype(T, any()).
prop_minus_all_sort_helper([], [], _) ->
    true;
prop_minus_all_sort_helper([_|_], [], _) ->
    false;
prop_minus_all_sort_helper([], [_|_], _) ->
    true;
prop_minus_all_sort_helper([RH | RT] = R, [LH | LT], Excluded) ->
    case lists:member(LH, Excluded) of
        true                 -> prop_minus_all_sort_helper(R, LT, Excluded);
        false when LH =:= RH -> prop_minus_all_sort_helper(RT, LT, Excluded);
        false                -> false
    end.

tester_minus_all(_Config) ->
    tester:test(?MODULE, prop_minus_all, 2, 5000, [{threads, 2}]).

tester_minus_all_sort(_Config) ->
    tester:test(?MODULE, prop_minus_all_sort, 2, 5000, [{threads, 2}]).

%% @doc Checks that all intended items are deleted once using util:minus_first/2.
%%      Note: this is kindof redundant to prop_minus_first_sort/2 but a cleaner
%%      approach avoiding a re-implementation of util:minus_first/2.
-spec prop_minus_first(List::[T], Excluded::[T]) -> true
    when is_subtype(T, any()).
prop_minus_first(List, Excluded) ->
    ?equals(util:minus_first(List, Excluded), lists:foldl(fun lists:delete/2, List, Excluded)).

%% @doc Checks that the order of items stays the same using util:minus_first/2.
-spec prop_minus_first_sort(List::[T], Excluded::[T]) -> true
    when is_subtype(T, any()).
prop_minus_first_sort(List, Excluded) ->
    Result = util:minus_first(List, Excluded),
    prop_minus_first_sort_helper(Result, List, Excluded).

-spec prop_minus_first_sort_helper(Result::[T], List::[T], Excluded::[T]) -> true
    when is_subtype(T, any()).
prop_minus_first_sort_helper([], [], _) ->
    true;
prop_minus_first_sort_helper([_|_], [], _) ->
    false;
prop_minus_first_sort_helper([], [_|_], _) ->
    true;
prop_minus_first_sort_helper([RH | RT] = R, [LH | LT], Excluded) ->
    case lists:member(LH, Excluded) of
        true                 -> prop_minus_first_sort_helper(R, LT, lists:delete(LH, Excluded));
        false when LH =:= RH -> prop_minus_first_sort_helper(RT, LT, Excluded);
        false                -> false
    end.

tester_minus_first(_Config) ->
    tester:test(?MODULE, prop_minus_first, 2, 5000, [{threads, 2}]).

tester_minus_first_sort(_Config) ->
    tester:test(?MODULE, prop_minus_first_sort, 2, 5000, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% util:par_map/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec try_fun(Module::module(), Fun::atom(), Args::[term()]) -> {ok | throw | error | exit, term()}.
try_fun(Module, Fun, Args) ->
    try {ok, apply(Module, Fun, Args)}
    catch Level:Reason -> {Level, Reason}
    end.

-spec compare__par_map2__lists_map(Fun::fun((A) -> term()), [A]) -> true | no_return().
compare__par_map2__lists_map(Fun, List) ->
    ParMapRes = try_fun(util, par_map, [Fun, List]),
    ListsMapRes = try_fun(lists, map, [Fun, List]),
    ?equals(ParMapRes, ListsMapRes).

-spec prop_par_map2([integer()]) -> ok.
prop_par_map2(List) ->
    compare__par_map2__lists_map(fun(X) -> X * X end, List),
    compare__par_map2__lists_map(fun(_) -> erlang:throw(failed) end, List),
    compare__par_map2__lists_map(fun(_) -> erlang:error(failed) end, List),
    compare__par_map2__lists_map(fun(_) -> erlang:exit(failed) end, List),
    ok.

tester_par_map2(_Config) ->
    tester:test(?MODULE, prop_par_map2, 1, 5000, [{threads, 2}]).

-spec compare__par_map3__lists_map(Fun::fun((A) -> term()), [A], MaxThreads::pos_integer()) -> true | no_return().
compare__par_map3__lists_map(Fun, List, MaxThreads) ->
    ParMapRes = try_fun(util, par_map, [Fun, List, MaxThreads]),
    ListsMapRes = try_fun(lists, map, [Fun, List]),
    ?equals(ParMapRes, ListsMapRes).

-spec prop_par_map3([integer()], 1..1000) -> ok.
prop_par_map3(List, MaxThreads) ->
    compare__par_map3__lists_map(fun(X) -> X * X end, List, MaxThreads),
    compare__par_map3__lists_map(fun(_) -> erlang:throw(failed) end, List, MaxThreads),
    compare__par_map3__lists_map(fun(_) -> erlang:error(failed) end, List, MaxThreads),
    compare__par_map3__lists_map(fun(_) -> erlang:exit(failed) end, List, MaxThreads),
    ok.

tester_par_map3(_Config) ->
    tester:test(?MODULE, prop_par_map3, 2, 5000, [{threads, 2}]).

lists_remove_at_indices(_Config) ->
    ?equals(util:lists_remove_at_indices([0,1,2,3,4,5], [0,2,4]), [1,3,5]),

    % both lists should be non-empty and all indices in the second list should point to
    % existing elements in the first list
    ?expect_exception(util:lists_remove_at_indices([1], []), error, function_clause),
    ?expect_exception(util:lists_remove_at_indices([], [0]), error, function_clause),
    ?expect_exception(util:lists_remove_at_indices([0,1,2,3], [5]), error, function_clause),
    ok.

sublist(_Config) ->
    L = [a,b,c,d,e,f,g],
    LLen = length(L),
    ?equals(util:sublist(L, 1, 0)    , {[], LLen}),
    ?equals(util:sublist(L, 1, 1)    , {[a], LLen}),
    ?equals(util:sublist(L, 1, 3)    , {[a,b,c], LLen}),
    ?equals(util:sublist(L, 1, 7)    , {[a,b,c,d,e,f,g], LLen}),
    ?equals(util:sublist(L, 1, 8)    , {[a,b,c,d,e,f,g], LLen}),
    ?equals(util:sublist(L, 1, 10)   , {[a,b,c,d,e,f,g], LLen}),
    ?equals(util:sublist(L, 2, 10)   , {[b,c,d,e,f,g], LLen}),
    ?equals(util:sublist(L, 3, 10)   , {[c,d,e,f,g], LLen}),
    ?equals(util:sublist(L, 7, 10)   , {[g], LLen}),
    ?equals(util:sublist(L, 8, 10)   , {[], LLen}),
    ?equals(util:sublist(L, 10, 10)  , {[], LLen}),
    
    ?equals(util:sublist(L, 1, -1)   , {[a], LLen}),
    ?equals(util:sublist(L, 1, -2)   , {[a], LLen}),
    ?equals(util:sublist(L, 1, -3)   , {[a], LLen}),
    ?equals(util:sublist(L, 2, -1)   , {[b], LLen}),
    ?equals(util:sublist(L, 3, -1)   , {[c], LLen}),
    ?equals(util:sublist(L, 4, -1)   , {[d], LLen}),
    ?equals(util:sublist(L, 5, -1)   , {[e], LLen}),
    ?equals(util:sublist(L, 6, -1)   , {[f], LLen}),
    ?equals(util:sublist(L, 7, -1)   , {[g], LLen}),
    ?equals(util:sublist(L, 8, -1)   , {[g], LLen}),
    ?equals(util:sublist(L, 3, -5)   , {[c,b,a], LLen}),
    
    ?equals(util:sublist(L, -1, 0)   , {[], LLen}),
    ?equals(util:sublist(L, -1, 1)   , {[g], LLen}),
    ?equals(util:sublist(L, -1, 3)   , {[g], LLen}),
    ?equals(util:sublist(L, -1, 10)  , {[g], LLen}),
    ?equals(util:sublist(L, -2, 10)  , {[f,g], LLen}),
    ?equals(util:sublist(L, -3, 10)  , {[e,f,g], LLen}),
    ?equals(util:sublist(L, -7, 10)  , {[a,b,c,d,e,f,g], LLen}),
    ?equals(util:sublist(L, -8, 10)  , {[a,b,c,d,e,f,g], LLen}),
    ?equals(util:sublist(L, -10, 10) , {[a,b,c,d,e,f,g], LLen}),
    
    ?equals(util:sublist(L, -1, -1)  , {[g], LLen}),
    ?equals(util:sublist(L, -1, -3)  , {[g,f,e], LLen}),
    ?equals(util:sublist(L, -1, -7)  , {[g,f,e,d,c,b,a], LLen}),
    ?equals(util:sublist(L, -1, -8)  , {[g,f,e,d,c,b,a], LLen}),
    ?equals(util:sublist(L, -1, -10) , {[g,f,e,d,c,b,a], LLen}),
    ?equals(util:sublist(L, -2, -10) , {[f,e,d,c,b,a], LLen}),
    ?equals(util:sublist(L, -3, -10) , {[e,d,c,b,a], LLen}),
    ?equals(util:sublist(L, -7, -10) , {[a], LLen}),
    ?equals(util:sublist(L, -8, -10) , {[], LLen}),
    ?equals(util:sublist(L, -10, -10), {[], LLen}),
    
    ok.

-spec prop_sublist3([any()], X::1..1000) -> true.
prop_sublist3(L, X) ->
    % last X elements (in different order)
    ?equals(lists:reverse(element(1, util:sublist(L, -1, -X))),
            element(1, util:sublist(L, -X, X))),
    % first X elements (in different order)
    ?equals(lists:reverse(element(1, util:sublist(L, 1, X))),
            element(1, util:sublist(L, X, -X))),
    true.

tester_sublist3(_Config) ->
    tester:test(?MODULE, prop_sublist3, 2, 5000, [{threads, 2}]).

-spec prop_timestamp1(erlang_timestamp()) -> true.
prop_timestamp1(TS) ->
    ?equals(TS, util:us2timestamp(util:timestamp2us(TS))),
    true.

-spec prop_timestamp2(util:us_timestamp()) -> true.
prop_timestamp2(Us) ->
    ?equals(Us, util:timestamp2us(util:us2timestamp(Us))),
    true.

tester_timestamp(_Config) ->
    tester:test(?MODULE, prop_timestamp1, 1, 5000, [{threads, 2}]),
    tester:test(?MODULE, prop_timestamp2, 1, 5000, [{threads, 2}]).

rrd_combine_timing_slots_handle_empty_rrd(_Config) ->
    DB0 = rrd:create(10, 10, {timing, us}, {0,0,0}),
    Dump = rrd:dump(DB0),
    ?equals(Dump, []),
    ?equals(util:rrd_combine_timing_slots(DB0, {0,0,0}, 10), undefined),
    ok
    .

rrd_combine_timing_slots_simple(_Config) ->
    Adds = [{20, 1}, {25, 3}, {30, 30}, {42, 42}],
    DB0 = rrd:create(10, 10, {timing, us}, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1),
            [{{0,0,40}, {0,0,50}, {42, 42*42, 1, 42, 42, {histogram,0,[],0,0}}},
             {{0,0,30}, {0,0,40}, {30, 30*30, 1, 30, 30, {histogram,0,[],0,0}}},
             {{0,0,20}, {0,0,30}, {1 + 3, 1*1 + 3*3, 2, 1, 3, {histogram,0,[],0,0}}}]),
    CurrentTS = {0,0,44}, % assume we are currently in the last slot

    Expected = {1 + 3 + 30 + 42, % sum
                1*1 + 3*3 + 30*30 + 42*42, % squares' sum
                2 + 1 + 1, % count
                1, % min
                42 % max
               },
    ?equals(util:rrd_combine_timing_slots(DB1, CurrentTS, 100), Expected),
    ?equals(util:rrd_combine_timing_slots(DB1, CurrentTS, 100, 10), Expected),
    ok
    .

rrd_combine_timing_slots_subset(_Config) ->
    % combine the newest two slots due to the interval
    Adds = [{20, 1}, {25, 3}, {30, 30}, {42, 42}],
    DB0 = rrd:create(10, 10, {timing, us}, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1),
            [{{0,0,40}, {0,0,50}, {42, 42*42, 1, 42, 42, {histogram,0,[],0,0}}},
             {{0,0,30}, {0,0,40}, {30, 30*30, 1, 30, 30, {histogram,0,[],0,0}}},
             {{0,0,20}, {0,0,30}, {1 + 3, 1*1 + 3*3, 2, 1, 3, {histogram,0,[],0,0}}}]),

    CurrentTS = {0,0,44}, % assume we are currently in the last slot
    Interval = 10, % overlap at most two slots

    ExpectedSmallEpsilon = {42 + 30, 42*42 + 30*30, 1 + 1, 30, 42},
    ?equals(util:rrd_combine_timing_slots(DB1, CurrentTS, Interval), ExpectedSmallEpsilon),
    ?equals(util:rrd_combine_timing_slots(DB1, CurrentTS, Interval, 5), ExpectedSmallEpsilon),

    % epsilon is big enough to do it only once
    ExpectedBigEpsilon = {42, 42*42, 1, 42, 42},
    ?equals(util:rrd_combine_timing_slots(DB1, CurrentTS, Interval, 10), ExpectedBigEpsilon),
    ?equals(util:rrd_combine_timing_slots(DB1, CurrentTS, Interval, 100), ExpectedBigEpsilon),
    ok
    .

rrd_combine_gauge_slots_handle_empty_rrd(_Config) ->
    DB0 = rrd:create(10, 10, gauge, {0,0,0}),
    Dump = rrd:dump(DB0),
    ?equals(Dump, []),
    ?equals(util:rrd_combine_gauge_slots(DB0, {0,0,0}, 10), undefined),
    ok
    .

rrd_combine_gauge_slots_simple(_Config) ->
    Adds = [{20, 1}, {25, 3}, {30, 30}, {42, 42}],
    DB0 = rrd:create(10, 10, gauge, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1),
            [{{0,0,40}, {0,0,50}, 42},
             {{0,0,30}, {0,0,40}, 30},
             {{0,0,20}, {0,0,30}, 3}]),
    CurrentTS = {0,0,44}, % assume we are currently in the last slot

    Expected = 75,
    ?equals(util:rrd_combine_gauge_slots(DB1, CurrentTS, 100), Expected),
    ?equals(util:rrd_combine_gauge_slots(DB1, CurrentTS, 100, 10), Expected),
    ok
    .

rrd_combine_gauge_slots_subset(_Config) ->
    % combine the newest two slots due to the interval
    Adds = [{20, 1}, {25, 3}, {30, 30}, {42, 42}],
    DB0 = rrd:create(10, 10, gauge, {0,0,0}),
    DB1 = lists:foldl(fun rrd_SUITE:apply/2, DB0, Adds),
    ?equals(rrd:dump(DB1),
            [{{0,0,40}, {0,0,50}, 42},
             {{0,0,30}, {0,0,40}, 30},
             {{0,0,20}, {0,0,30}, 3}]),

    CurrentTS = {0,0,44}, % assume we are currently in the last slot
    Interval = 10, % overlap at most two slots

    ?equals(util:rrd_combine_gauge_slots(DB1, CurrentTS, Interval), 72),
    ?equals(util:rrd_combine_gauge_slots(DB1, CurrentTS, Interval, 5), 72),
    ?equals(util:rrd_combine_gauge_slots(DB1, CurrentTS, Interval, 10), 42), % exits immediately
    ?equals(util:rrd_combine_gauge_slots(DB1, CurrentTS, Interval, 100), 42),
    ?equals(util:rrd_combine_gauge_slots(DB1, CurrentTS, 20, 1), 75),
    ok
    .
