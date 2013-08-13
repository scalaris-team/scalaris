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
%% @doc    Unit tests for db backends that fullfill src/backend_beh.erl.
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
-spec prop_put([backend_beh:entry()]) -> true.
prop_put(Data) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(?TEST_DB:new(randoms:getRandomString()),
                                             Data),
    check_db(DB1, ScrubedData, "check_db_put1"),
    ?TEST_DB:close(DB1),
    true.

tester_put(_Config) ->
    tester:test(?MODULE, prop_put, 1, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test get/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get([backend_beh:entry()]) -> true.
prop_get(Data) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(?TEST_DB:new(randoms:getRandomString()),
                                             Data),
    GetData = lists:foldl(
            fun(Entry, AccIn) ->
                [?TEST_DB:get(DB1, element(1, Entry)) | AccIn]
            end, [], ScrubedData),
    ?equals_w_note(lists:sort(ScrubedData), lists:sort(GetData), "check_db_put1"),
    check_db(DB1, GetData, "check_db_put1"),
    ?TEST_DB:close(DB1),
    true.

tester_get(_Config) ->
    tester:test(?MODULE, prop_get, 1, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test delete/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete([backend_beh:entry()], [backend_beh:key()]) -> true.
prop_delete(Data, ToDelete) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(?TEST_DB:new(randoms:getRandomString()),
                                             Data),
    DB2 = lists:foldl(
            fun(Key, DBAcc) ->
                ?TEST_DB:delete(DBAcc, Key)
            end, DB1, ToDelete),
    ExpData = lists:filter(
                fun(El) ->
                        not lists:any(fun(Key) ->
                                          element(1, El) ?EQ Key
                                  end, ToDelete)
                end, ScrubedData),
    check_db(DB2, ExpData, "check_db_delete"),
    ?TEST_DB:close(DB2),
    true.

tester_delete(_Config) ->
    tester:test(?MODULE, prop_delete, 2, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test foldl/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_foldl([backend_beh:entry()], backend_beh:interval(),
                 non_neg_integer()) -> true.
prop_foldl(Data, Interval, MaxNum) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(?TEST_DB:new(randoms:getRandomString()),
                                             Data),
    {ExpInInterval, _NotIn} = lists:partition(
                                fun(E) ->
                                        is_in(Interval, element(1, E))
                                end,
                                ScrubedData),
    ExpInIntervalCounted = lists:sublist(lists:keysort(1, ExpInInterval), MaxNum),
    AllFold = ?TEST_DB:foldl(DB1, fun(E, AccIn) -> [E | AccIn] end, []),
    IntervalFold = ?TEST_DB:foldl(DB1,
                                 fun(E, AccIn) -> [E | AccIn] end,
                                 [],
                                 Interval),
    IntervalCountFold = ?TEST_DB:foldl(DB1,
                            fun(E, AccIn) -> [E | AccIn] end,
                            [],
                            Interval,
                            MaxNum),
    ?equals_w_note(lists:sort(ScrubedData), lists:sort(AllFold), "test_foldl1"),
    ?equals_w_note(lists:sort(ExpInInterval), lists:sort(IntervalFold), "test_foldl2"),
    ?equals_w_note(lists:sort(ExpInIntervalCounted),
                   lists:sort(IntervalCountFold), "test_foldl3"),
    ?TEST_DB:close(DB1),
    true.

tester_foldl(_Config) ->
    tester:test(?MODULE, prop_foldl, 3, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test foldr/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_foldr([backend_beh:entry()], backend_beh:interval(),
                 non_neg_integer()) -> true.
prop_foldr(Data, Interval, MaxNum) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(?TEST_DB:new(randoms:getRandomString()),
                                             Data),
    {ExpInInterval, _NotIn} = lists:partition(
                                fun(E) ->
                                        is_in(Interval, element(1, E))
                                end,
                                ScrubedData),
    ExpInIntervalCounted = lists:sublist(lists:reverse(lists:keysort(1, ExpInInterval)), MaxNum),
    AllFold = ?TEST_DB:foldr(DB1, fun(E, AccIn) -> [E | AccIn] end, []),
    IntervalFold = ?TEST_DB:foldr(DB1,
                                 fun(E, AccIn) -> [E | AccIn] end,
                                 [],
                                 Interval),
    IntervalCountFold = ?TEST_DB:foldr(DB1,
                            fun(E, AccIn) -> [E | AccIn] end,
                            [],
                            Interval,
                            MaxNum),
    %% ct:pal("ExpInInterval: ~p~nIntervalFold: ~p~nInterval: ~p~n", [ExpInInterval,
    %%                                                  IntervalFold, Interval]),
    ?equals_w_note(lists:sort(ScrubedData), lists:sort(AllFold), "test_foldr1"),
    ?equals_w_note(lists:sort(ExpInInterval), lists:sort(IntervalFold), "test_foldr2"),
    ?equals_w_note(lists:sort(ExpInIntervalCounted),
                   lists:sort(IntervalCountFold), "test_foldr3"),
    ?TEST_DB:close(DB1),
    true.

tester_foldr(_Config) ->
    tester:test(?MODULE, prop_foldr, 3, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
write_scrubed_to_db(DB, Data) ->
    ScrubedData = scrub_data(Data),
    DB1 = lists:foldl(
            fun(Entry, DBAcc) ->
                ?TEST_DB:put(DBAcc, Entry)
            end, DB, ScrubedData),
    {DB1, ScrubedData}.

scrub_data(Data) ->
    %% Entries should be unique
    SortFun = fun(A, B) -> (element(1, A) < element(1, B)) or
                           (element(1, A) ?EQ element(1, B)) end,
    lists:usort(SortFun, [Entry || Entry <- Data]).

check_db(DB, ExpData, Note) ->
    InDb = ?TEST_DB:foldl(DB, fun(E, AIn) -> [E | AIn] end, []),
    ?equals_w_note(lists:sort(InDb), lists:sort(ExpData), Note),
    ?equals_w_note(?TEST_DB:get_load(DB), length(ExpData), Note).


is_in({element, Key}, OtherKey) -> Key ?EQ OtherKey;
is_in(all, _Key) -> true;
is_in({interval, '(', L, R, ')'}, Key) -> Key > L andalso Key < R;
is_in({interval, '(', L, R, ']'}, Key) -> Key > L andalso ((Key < R) or (Key ?EQ R));
is_in({interval, '[', L, R, ')'}, Key) -> ((Key > L) or (Key ?EQ L)) andalso Key < R;
is_in({interval, '[', L, R, ']'}, Key) -> ((Key > L) or (Key ?EQ L)) andalso ((Key < R) or (Key ?EQ R)).
