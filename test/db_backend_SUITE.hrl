% @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    Unit tests for db backends that fullfill src/db_backend_beh.erl.
%%         just define ?TEST_DB and include this file.
%%         The default to determine equality of keys is == (ets with ordered_set
%%         uses this). If a backend only considers matching keys equal (i.e.
%%         =:=) the EQ macro has to defined to =:= in the file that includes
%%         this one.
%% @end
%% @version $Id$

-include("scalaris.hrl").
-include("unittest.hrl").

%% default equality
-ifndef(EQ).
-define(EQ, ==).
-endif.

tests_avail() ->
    [tester_put,
     tester_get,
     tester_delete,
     tester_foldl,
     tester_foldr].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test put/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_put([db_backend_beh:entry()]) -> true.
prop_put(Data) ->
    {DB1, ScrubbedData} =
        write_scrubbed_to_db(?TEST_DB:new(randoms:getRandomString()),
                             Data),
    check_db(DB1, ScrubbedData, "check_db_put1"),
    ?TEST_DB:?CLOSE(DB1),
    true.

tester_put(_Config) ->
    tester:test(?MODULE, prop_put, 1, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test get/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get([db_backend_beh:entry()]) -> true.
prop_get(Data) ->
    {DB1, ScrubbedData} =
        write_scrubbed_to_db(?TEST_DB:new(randoms:getRandomString()), Data),
    GetData = lists:foldl(
            fun(Entry, AccIn) ->
                [?TEST_DB:get(DB1, element(1, Entry)) | AccIn]
            end, [], ScrubbedData),
    compare_lists(ScrubbedData, GetData, "check_db_put1"),
    check_db(DB1, GetData, "check_db_put1"),
    ?TEST_DB:?CLOSE(DB1),
    true.

tester_get(_Config) ->
    tester:test(?MODULE, prop_get, 1, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test delete/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete([db_backend_beh:entry()], [db_backend_beh:key()]) -> true.
prop_delete(Data, ToDelete) ->
    {DB1, ScrubbedData} =
        write_scrubbed_to_db(?TEST_DB:new(randoms:getRandomString()), Data),
    DB2 = lists:foldl(
            fun(Key, DBAcc) ->
                ?TEST_DB:delete(DBAcc, Key)
            end, DB1, ToDelete),
    ExpData = [El || El <- ScrubbedData,
                     not lists:any(fun(Key) -> element(1, El) ?EQ Key end,
                                   ToDelete)],
    check_db(DB2, ExpData, "check_db_delete"),
    ?TEST_DB:?CLOSE(DB2),
    true.

tester_delete(_Config) ->
    tester:test(?MODULE, prop_delete, 2, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test foldl/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_foldl([db_backend_beh:entry()], db_backend_beh:interval(),
                 non_neg_integer()) -> true.
prop_foldl(Data, Interval, MaxNum) ->
    {DB1, ScrubbedData} =
        write_scrubbed_to_db(?TEST_DB:new(randoms:getRandomString()), Data),
    ExpInInterval = [El || El <- ScrubbedData,
                           is_in(Interval, element(1, El))],
    ExpInIntervalCounted = lists:sublist(ExpInInterval, MaxNum),
    %% We have certain combination of keys (e.g. {0} and {0.0}]) which are returned
    %% in an unspecified order by certain DBs (lets call that a sequenze). If MaxNum
    %% is set to a value which would split such a sequenz the test might fail because
    %% the DBs returns unexpected keys. In such case all remaining elements of the sequenze
    %% are added as well (which effectivly increases MaxNum).
    ExpInIntervalCountedTailed =
        case MaxNum >= length(ExpInInterval) orelse MaxNum == 0 of
            true ->
                ExpInIntervalCounted;
            _ ->
                Tail = lists:takewhile(fun(E) ->
                                            element(1, lists:last(ExpInIntervalCounted)) ==
                                            element(1, E)
                                       end, lists:nthtail(MaxNum, ExpInInterval)),
                lists:append(ExpInIntervalCounted, Tail)
        end,

    AllFold = ?TEST_DB:foldl(DB1, fun(K, AccIn) -> [?TEST_DB:get(DB1, K) | AccIn] end, []),
    IntervalFold = ?TEST_DB:foldl(DB1,
                                 fun(K, AccIn) -> [?TEST_DB:get(DB1, K) | AccIn] end,
                                 [],
                                 Interval),
    IntervalCountFold = ?TEST_DB:foldl(DB1,
                            fun(K, AccIn) -> [?TEST_DB:get(DB1, K) | AccIn] end,
                            [],
                            Interval,
                            length(ExpInIntervalCountedTailed)),
    UnorderedFold = ?TEST_DB:foldl_unordered(DB1, fun(E, AccIn) -> [E | AccIn] end, []),
    %% we expect all data from fold to be in reversed order because of list
    %% accumulation. we need to reverse ScrubbedData, ExpInInterval and
    %% ExpInIntervalCounted.
    compare_lists(ScrubbedData, AllFold, "test_foldl1"),
    compare_lists(ExpInInterval, IntervalFold, "test_foldl2"),
    compare_lists(ExpInIntervalCountedTailed, IntervalCountFold, "test_foldl3"),
    compare_lists(ScrubbedData, UnorderedFold, "test_foldl4"),
    ?TEST_DB:?CLOSE(DB1),
    true.

tester_foldl(_Config) ->
    prop_foldl([{0.0,{{},[0.0],-3},[],one,{}}, {0,[{}]}], all, 1),
    prop_foldl([{0.0}, {0}, {0.0}], {'(', {'*'}, {0}, ']'}, 4),
    prop_foldl([{foo}, {"bar"}, {42},{-1},{{"foobar"}},{{{0.3820862051907793}}},{0.39125416350624936}], all, 2),
    prop_foldl([{42},{-1},{{{0.3820862051907793}}},{0.39125416350624936}], all, 2),
    prop_foldl([{{42,-5,{0,{},{-1,{{},[5],42},[[[]]]}}}},{[5]},{0.0}], all, 3),
    tester:test(?MODULE, prop_foldl, 3, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test foldr/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_foldr([db_backend_beh:entry()], db_backend_beh:interval(),
                 non_neg_integer()) -> true.
prop_foldr(Data, Interval, MaxNum) ->
    {DB1, ScrubbedData} =
        write_scrubbed_to_db(?TEST_DB:new(randoms:getRandomString()), Data),
    ExpInInterval = [El || El <- ScrubbedData,
                           is_in(Interval, element(1, El))],
    ExpInIntervalCounted =
        case MaxNum >= length(ExpInInterval) of
            true ->
                ExpInInterval;
            _ ->
                EndIndex = length(ExpInInterval) - MaxNum,
                Counted = lists:nthtail(EndIndex, ExpInInterval),
                %% The same reason as for prop_foldl, but instead of adding, drop
                %% all elements of the splitted sequence.
                case MaxNum == 0 of
                    true ->
                        Counted;
                    _ ->
                        lists:dropwhile(fun(E) ->
                                element(1, lists:nth(EndIndex, ExpInInterval)) ==
                                element(1, E)
                        end, Counted)
                end
        end,

    AllFold = ?TEST_DB:foldr(DB1, fun(K, AccIn) -> [?TEST_DB:get(DB1, K) | AccIn] end, []),
    IntervalFold = ?TEST_DB:foldr(DB1,
                                 fun(K, AccIn) -> [?TEST_DB:get(DB1, K) | AccIn] end,
                                 [],
                                 Interval),
    IntervalCountFold = ?TEST_DB:foldr(DB1,
                            fun(K, AccIn) -> [?TEST_DB:get(DB1, K) | AccIn] end,
                            [],
                            Interval,
                            length(ExpInIntervalCounted)),
    %% ct:pal("ExpInInterval: ~p~nIntervalFold: ~p~nInterval: ~p~n", [ExpInInterval,
    %%                                                  IntervalFold, Interval]),
    compare_lists(ScrubbedData, AllFold, "test_foldr1"),
    compare_lists(ExpInInterval, IntervalFold, "test_foldr2"),
    compare_lists(ExpInIntervalCounted, IntervalCountFold, "test_foldr3"),
    ?TEST_DB:?CLOSE(DB1),
    true.

tester_foldr(_Config) ->
    prop_foldr([{{0.0},[],[],42,42,[],one,{}},{{0},[{}]}], all, 1),
    tester:test(?MODULE, prop_foldr, 3, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
write_scrubbed_to_db(DB, Data) ->
    ScrubbedData = scrub_data(Data),
    DB1 = lists:foldl(
            fun(Entry, DBAcc) ->
                ?TEST_DB:put(DBAcc, Entry)
            end, DB, ScrubbedData),
    {DB1, ScrubbedData}.

scrub_data(Data) ->
    %% Entries should be unique
    SortFun = fun(A, B) -> ((element(1, A) < element(1, B)) orelse
                            (element(1, A) ?EQ element(1, B)))
                            orelse
                            (is_integer(element(1, A))
                            andalso is_float(element(1, B))
                            andalso element(1, A) == element(1, B))
              end,
    lists:usort(SortFun, lists:reverse(Data)).

check_db(DB, ExpData, Note) ->
    InDb = ?TEST_DB:foldl(DB, fun(K, AIn) -> [?TEST_DB:get(DB, K) | AIn] end, []),
    compare_lists(InDb, ExpData, Note),
    ?equals_w_note(?TEST_DB:get_load(DB), length(ExpData), Note).

compare_lists(List1, List2, Note) ->
    ?equals_w_note(length(List1), length(List2), "length of lists " ++ Note),
    ?compare_w_note(fun eq_lenient/2, scrub_data(List1), scrub_data(List2), "content of lists " ++
                   Note).

is_in({Key}, OtherKey) -> Key ?EQ OtherKey;
is_in(all, _Key) -> true;
is_in({'(', L, R, ')'}, Key) -> Key > L andalso Key < R;
is_in({'(', L, R, ']'}, Key) -> Key > L andalso ((Key < R) orelse (Key ?EQ R));
is_in({'[', L, R, ')'}, Key) -> ((Key > L) orelse (Key ?EQ L)) andalso Key < R;
is_in({'[', L, R, ']'}, Key) -> ((Key > L) orelse (Key ?EQ L)) andalso ((Key < R) orelse (Key ?EQ R)).

%% Test two lists L1 and L2 for equality while taking into consideration that elements E1, E2 might be
%% out of order iff E1 == E2 but E1 =/= E2 (eg. {0.0} and {0})
eq_lenient(L1, L2) -> eq_lenient(L1, [], L2).

eq_lenient([], [], []) -> true;
eq_lenient([H1|L1], [], [H2|L2]) when H1 ?EQ H2 ->
    eq_lenient(L1, [], L2);
eq_lenient([H1|L1], Pref, [H2|L2]) when H1 ?EQ H2 ->
    eq_lenient(L1, [], lists:append(Pref, L2));
eq_lenient([H1|L1], Pref, [H2|L2]) when element(1, H1) == element(1, H2) ->
    eq_lenient([H1|L1], [H2|Pref], L2);
eq_lenient(_L1, _Pref, _L2) -> false.
