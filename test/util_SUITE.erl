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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Unit tests for src/util.erl.
%%% @end
%% @version $Id$
-module(util_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [min_max, largest_smaller_than, gb_trees_foldl,
     repeat, repeat_collect, repeat_accumulate,
     repeat_p, repeat_p_collect, repeat_p_accumulate,
     tester_minus_all, tester_minus_all_sort,
     tester_minus_first, tester_minus_first_sort].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% util:minus_all/2 and util:minus_first/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Checks that all intended items are deleted using util:minus_all/2.
%%      Note: this is kindof redundant to prop_minus_all_sort/2 but a cleaner
%%      approach avoiding a re-implementation of util:minus_all/2.
-spec prop_minus_all(List::[T], Excluded::[T]) -> boolean().
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
-spec prop_minus_all_sort(List::[T], Excluded::[T]) -> boolean().
prop_minus_all_sort(List, Excluded) ->
    Result = util:minus_all(List, Excluded),
    prop_minus_all_sort_helper(Result, List, Excluded).

-spec prop_minus_all_sort_helper(Result::[T], List::[T], Excluded::[T]) -> boolean().
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
-spec prop_minus_first(List::[T], Excluded::[T]) -> boolean().
prop_minus_first(List, Excluded) ->
    ?equals(util:minus_first(List, Excluded), lists:foldl(fun lists:delete/2, List, Excluded)).

%% @doc Checks that the order of items stays the same using util:minus_first/2.
-spec prop_minus_first_sort(List::[T], Excluded::[T]) -> boolean().
prop_minus_first_sort(List, Excluded) ->
    Result = util:minus_first(List, Excluded),
    prop_minus_first_sort_helper(Result, List, Excluded).

-spec prop_minus_first_sort_helper(Result::[T], List::[T], Excluded::[T]) -> boolean().
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
