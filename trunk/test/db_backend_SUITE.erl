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
%%         Additional backends should be added to backends() to be tested.
%% @end
%% @version $Id$
-module(db_backend_SUITE).

-author('fajerski@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

all() ->
    [tester_put,
    tester_get,
    tester_delete,
    tester_foldl,
    tester_foldr].

suite() -> [ {timetrap, {seconds, 15}} ].

init_per_suite(Config) ->
    Config1 = unittest_helper:init_per_suite(Config),
    tester:register_type_checker({typedef, backend_beh, key}, backend_beh, tester_is_valid_db_key),
    tester:register_value_creator({typedef, backend_beh, key}, backend_beh, tester_create_db_key, 1),
    Config1.

end_per_suite(Config) ->
    tester:unregister_type_checker({typedef, backend_beh, key}),
    tester:unregister_value_creator({typedef, backend_beh, key}),
    unittest_helper:end_per_suite(Config).

%% add backend name to list
backends() ->
    [db_ets].

rw_suite_runs(N) ->
    erlang:min(N, 10000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test put/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_put([backend_beh:entry()]) -> true.
prop_put(Data) ->
    [test_put(Data, Backend) || Backend <- backends()],
    true.

test_put(Data, Backend) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(Backend:new(randoms:getRandomString()),
                                             Data, Backend),
    check_db(DB1, ScrubedData, Backend, "check_db_put1_" ++ atom_to_list(Backend)),
    Backend:close(DB1),
    true.

tester_put(_Config) ->
    tester:test(?MODULE, prop_put, 1, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test get/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get([backend_beh:entry()]) -> true.
prop_get(Data) ->
    [test_get(Data, Backend) || Backend <- backends()],
    true.

test_get(Data, Backend) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(Backend:new(randoms:getRandomString()),
                                             Data, Backend),
    GetData = lists:foldl(
            fun(Entry, AccIn) ->
                [Backend:get(DB1, element(1, Entry)) | AccIn]
            end, [], ScrubedData),
    ?equals_w_note(lists:sort(ScrubedData), lists:sort(GetData), "check_db_put1_"
                   ++ atom_to_list(Backend)),
    check_db(DB1, GetData, Backend, "check_db_put1_" ++ atom_to_list(Backend)),
    Backend:close(DB1),
    true.

tester_get(_Config) ->
    tester:test(?MODULE, prop_get, 1, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test delete/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete([backend_beh:entry()], [backend_beh:key()]) -> true.
prop_delete(Data, ToDelete) ->
    [test_delete(Data, ToDelete, Backend) || Backend <- backends()],
    true.

test_delete(Data, ToDelete, Backend) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(Backend:new(randoms:getRandomString()),
                                             Data, Backend),
    DB2 = lists:foldl(
            fun(Key, DBAcc) ->
                Backend:delete(DBAcc, Key)
            end, DB1, ToDelete),
    ExpData = lists:foldl(
            fun(Key, AccIn) ->
                lists:keydelete(Key, 1, AccIn)
            end, ScrubedData, ToDelete),
    check_db(DB2, ExpData, Backend, "check_db_put1_" ++ atom_to_list(Backend)),
    Backend:close(DB2),
    true.

tester_delete(_Config) ->
    tester:test(?MODULE, prop_delete, 2, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test foldl/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_foldl([backend_beh:entry()], backend_beh:interval(),
                 non_neg_integer()) -> true.
prop_foldl(Data, Interval, MaxNum) ->
    [test_foldl(Data, Interval, MaxNum, Backend) || Backend <- backends()],
    true.

test_foldl(Data, Interval, MaxNum, Backend) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(Backend:new(randoms:getRandomString()),
                                             Data, Backend),
    {ExpInInterval, _NotIn} = lists:partition(
            fun(E) -> 
                    case Interval of
                        all ->
                            true;
                        {element, Key} ->
                            element(1, E) == Key;
                        {interval, '(', L, R, ')'} ->
                            element(1, E) > L andalso
                            element(1, E) < R;
                        {interval, '(', L, R, ']'} ->
                            element(1, E) > L andalso
                            element(1, E) =< R;
                        {interval, '[', L, R, ')'} ->
                            element(1, E) >= L andalso
                            element(1, E) < R;
                        {interval, '[', L, R, ']'} ->
                            element(1, E) >= L andalso
                            element(1, E) =< R
                    end
            end,
            ScrubedData),
    ExpInIntervalCounted = lists:sublist(ExpInInterval, MaxNum),
    AllFold = Backend:foldl(DB1, fun(E, AccIn) -> [E | AccIn] end, []),
    IntervalFold = Backend:foldl(DB1,
                                 fun(E, AccIn) -> [E | AccIn] end,
                                 [],
                                 Interval),
    IntervalCountFold = Backend:foldl(DB1,
                            fun(E, AccIn) -> [E | AccIn] end,
                            [],
                            Interval,
                            MaxNum),
    ?equals_w_note(lists:sort(ScrubedData), lists:sort(AllFold), "test_foldl1_"
                   ++ atom_to_list(Backend)),
    ?equals_w_note(lists:sort(ExpInInterval), lists:sort(IntervalFold), "test_foldl2_"
                   ++ atom_to_list(Backend)),
    ?equals_w_note(lists:sort(ExpInIntervalCounted),
                   lists:sort(IntervalCountFold), "test_foldl3_" ++
                   atom_to_list(Backend)),
    Backend:close(DB1),
    true.

tester_foldl(_Config) ->
    tester:test(?MODULE, prop_foldl, 3, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test foldr/2 of available backends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_foldr([backend_beh:entry()], backend_beh:interval(),
                 non_neg_integer()) -> true.
prop_foldr(Data, Interval, MaxNum) ->
    [test_foldr(Data, Interval, MaxNum, Backend) || Backend <- backends()],
    true.

test_foldr(Data, Interval, MaxNum, Backend) ->
    {DB1, ScrubedData} =
                         write_scrubed_to_db(Backend:new(randoms:getRandomString()),
                                             Data, Backend),
    {ExpInInterval, _NotIn} = lists:partition(
            fun(E) -> 
                    case Interval of
                        all ->
                            true;
                        {element, Key} ->
                            element(1, E) == Key;
                        {interval, '(', L, R, ')'} ->
                            element(1, E) > L andalso
                            element(1, E) < R;
                        {interval, '(', L, R, ']'} ->
                            element(1, E) > L andalso
                            element(1, E) =< R;
                        {interval, '[', L, R, ')'} ->
                            element(1, E) >= L andalso
                            element(1, E) < R;
                        {interval, '[', L, R, ']'} ->
                            element(1, E) >= L andalso
                            element(1, E) =< R
                    end
            end,
            ScrubedData),
    ExpInIntervalCounted = lists:sublist(lists:reverse(ExpInInterval), MaxNum),
    AllFold = Backend:foldr(DB1, fun(E, AccIn) -> [E | AccIn] end, []),
    IntervalFold = Backend:foldr(DB1,
                                 fun(E, AccIn) -> [E | AccIn] end,
                                 [],
                                 Interval),
    IntervalCountFold = Backend:foldr(DB1,
                            fun(E, AccIn) -> [E | AccIn] end,
                            [],
                            Interval,
                            MaxNum),
    %% ct:pal("ExpInInterval: ~p~nIntervalFold: ~p~nInterval: ~p~n", [ExpInInterval,
    %%                                                  IntervalFold, Interval]),
    ?equals_w_note(lists:sort(ScrubedData), lists:sort(AllFold), "test_foldr1_"
                   ++ atom_to_list(Backend)),
    ?equals_w_note(lists:sort(ExpInInterval), lists:sort(IntervalFold), "test_foldr2_"
                   ++ atom_to_list(Backend)),
    ?equals_w_note(lists:sort(ExpInIntervalCounted),
                   lists:sort(IntervalCountFold), "test_foldr3_" ++
                   atom_to_list(Backend)),
    Backend:close(DB1),
    true.

tester_foldr(_Config) ->
    tester:test(?MODULE, prop_foldr, 3, rw_suite_runs(10000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
write_scrubed_to_db(DB, Data, Backend) ->
    ScrubedData = scrub_data(Data),
    DB1 = lists:foldl(
            fun(Entry, DBAcc) ->
                Backend:put(DBAcc, Entry)
            end, DB, ScrubedData),
    {DB1, ScrubedData}.

scrub_data(Data) ->
    %% Entries should be unique
    SortFun = fun(A, B) -> element(1, A) =< element(1, B) end,
    %% all tuples but {} are acceptable entries
    %% '$end_of_table' should not be used as key
    lists:usort(SortFun, [Entry || Entry <- Data, 
                                   Entry =/= {}, 
                                   element(1, Entry) =/= '$end_of_table']).

check_db(DB, ExpData, Backend, Note) ->
    InDb = Backend:foldl(DB, fun(E, AIn) -> [E | AIn] end, []),
    ?equals_w_note(lists:sort(InDb), lists:sort(ExpData), Note),
    ?equals_w_note(Backend:get_load(DB), length(ExpData), Note).
