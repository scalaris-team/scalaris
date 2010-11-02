% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Unit tests for src/db_ets.erl.
%%% @end
%% @version $Id$
-module(db_ets_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(TEST_DB, db_ets).

-include("db_SUITE.hrl").

all() -> lists:append(tests_avail(), [tester_get_chunk_precond, tester_get_chunk]).

%% @doc Specify how often a read/write suite can be executed in order not to
%%      hit a timeout (depending on the speed of the DB implementation).
-spec max_rw_tests_per_suite() -> pos_integer().
max_rw_tests_per_suite() ->
    10000.

-spec prop_get_chunk(Keys::[?RT:key()], BeginBr::intervals:left_bracket(), Begin::?RT:key(),
                     End::?RT:key(), EndBr::intervals:right_bracket(), ChunkSize::pos_integer()) -> true.
prop_get_chunk(Keys2, BeginBr, Begin, End, EndBr, ChunkSize) ->
    Interval = intervals:new(BeginBr, Begin, End, EndBr),
    case not intervals:is_empty(Interval) of
        true ->
            Keys = lists:usort(Keys2),
            DB = db_ets:new(),
            DB2 = fill_db(DB, Keys),
            {Next, Chunk} = db_ets:get_chunk(DB2, Interval, ChunkSize),
            db_ets:close(DB2),
            ?equals(lists:usort(Chunk), lists:sort(Chunk)), % check for duplicates
            ExpectedChunkSize = util:min(count_keys_in_range(Keys, Interval),
                                         ChunkSize),
            case ExpectedChunkSize =/= length(Chunk) of
                true ->
                    ?ct_fail("chunk has wrong size ~.0p ~.0p ~.0p, expected size: ~.0p",
                             [Chunk, Keys, Interval, ExpectedChunkSize]);
                false ->
                    ?equals([Entry || Entry <- Chunk,
                                      not intervals:in(db_entry:get_key(Entry), Interval)],
                            [])
            end,
            % Next if subset of Interval, no chunk entry is in Next:
            ?equals_w_note(intervals:is_subset(Next, Interval), true,
                           io_lib:format("Next ~.0p is not subset of ~.0p",
                                         [Next, Interval])),
            ?equals_w_note([Entry || Entry <- Chunk,
                                     intervals:in(db_entry:get_key(Entry), Next)],
                           [], io_lib:format("Next: ~.0p", [Next])),
            true;
        _ -> true
    end.

tester_get_chunk(_Config) ->
    prop_get_chunk([0, 4, 31], '[', 0, 4, ']', 2),
    prop_get_chunk([1, 5, 127, 13], '[', 3, 2, ']', 4),
    tester:test(?MODULE, prop_get_chunk, 6, rw_suite_runs(1000)).

-spec prop_delete_chunk(Keys::[?RT:key()], BeginBr::intervals:left_bracket(), Begin::?RT:key(),
                        End::?RT:key(), EndBr::intervals:right_bracket(), ChunkSize::pos_integer()) -> true.
prop_delete_chunk(Keys2, BeginBr, Begin, End, EndBr, ChunkSize) ->
    Interval = intervals:new(BeginBr, Begin, End, EndBr),
    case not intervals:is_empty(Interval) of
        true ->
            Keys = lists:usort(Keys2),
            DB = db_ets:new(),
            DB2 = fill_db(DB, Keys),
            {Next, Chunk} = db_ets:get_chunk(DB2, Interval, ChunkSize),
            Next = db_ets:delete_chunk(DB2, Interval, ChunkSize),
            PostDeleteChunkSize = db_ets:get_load(DB2),
            lists:foreach(fun (Entry) -> db_ets:delete_entry(DB2, Entry) end, Chunk),
            PostDeleteSize = db_ets:get_load(DB2),
            db_ets:close(DB2),
            ?equals(PostDeleteChunkSize, PostDeleteSize), % delete should have deleted all items in Chunk
            ?equals(length(Keys) - length(Chunk), PostDeleteSize), % delete should have deleted all items in Chunk
            true;
        _ -> true
    end.

tester_delete_chunk(_Config) ->
    tester:test(?MODULE, prop_delete_chunk, 6, rw_suite_runs(1000)).

-spec fill_db(DB::db_ets:db(), [?RT:key()]) -> db_ets:db().
fill_db(DB, []) -> DB;
fill_db(DB, [Key | Rest]) -> fill_db(db_ets:write(DB, Key, "Value", 1), Rest).

-spec count_keys_in_range(Keys::[?RT:key()], Interval::intervals:interval()) -> non_neg_integer().
count_keys_in_range(Keys, Interval) ->
    lists:foldl(fun(Key, Count) ->
                        case intervals:in(Key, Interval) of
                            true -> Count + 1;
                            _    -> Count
                        end
                end, 0, Keys).

tester_get_chunk_precond(_Config) ->
    Table = ets:new(ets_test_SUITE, [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
    ets:insert(Table, {5}),
    ets:insert(Table, {6}),
    ets:insert(Table, {7}),
    ?equals(ets:next(Table, 7), '$end_of_table'),
    ?equals(ets:next(Table, 6), 7),
    ?equals(ets:next(Table, 5), 6),
    ?equals(ets:next(Table, 4), 5),
    ?equals(ets:next(Table, 3), 5),
    ?equals(ets:next(Table, 2), 5),
    ?equals(ets:first(Table), 5).
