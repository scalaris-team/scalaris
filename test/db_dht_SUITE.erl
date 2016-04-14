% @copyright 2010-2015 Zuse Institute Berlin

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
%% @doc    Unit tests for src/db_dht.erl.
%% @end
%% @version $Id$
-module(db_dht_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("client_types.hrl").
-include("unittest.hrl").

groups() ->
    [{hand_written_tests, [sequence], [
                                       read,
                                       write,
                                       delete,
                                       get_load_and_middle,
                                       split_data,
                                       update_entries,
                                       changed_keys
                                      ]},
     {tester_tests, [sequence], [
                                 tester_new, tester_set_entry, tester_update_entry,
                                 tester_delete_entry1, tester_delete_entry2,
                                 tester_write,
                                 tester_delete, tester_add_data,
                                 tester_get_entries2, tester_get_entries3_1,
                                 tester_get_entries3_2,
                                 tester_get_load2,
                                 tester_split_data, tester_update_entries,
                                 tester_delete_entries1, tester_delete_entries2,
                                 tester_changed_keys_get_entry,
                                 tester_changed_keys_set_entry,
                                 tester_changed_keys_update_entry,
                                 tester_changed_keys_delete_entry,
                                 tester_changed_keys_read,
                                 tester_changed_keys_write,
                                 tester_changed_keys_delete,
                                 tester_changed_keys_get_entries2,
                                 tester_changed_keys_get_entries4,
                                 tester_get_chunk_precond,
                                 tester_get_chunk4,
                                 tester_get_split_key5,
                                 tester_changed_keys_update_entries,
                                 tester_changed_keys_delete_entries1,
                                 tester_changed_keys_delete_entries2,
                                 tester_changed_keys_get_load,
                                 tester_changed_keys_get_load2,
                                 tester_changed_keys_split_data1,
                                 tester_changed_keys_split_data2,
                                 tester_changed_keys_get_data,
                                 tester_changed_keys_add_data,
                                 tester_changed_keys_check_db,
                                 tester_changed_keys_mult_interval,
                                 tester_stop_record_changes]}].

all() ->
    [
     {group, hand_written_tests},
     {group, tester_tests}
    ].


suite() -> [ {timetrap, {seconds, 15}} ].

-define(KEY(Key), ?RT:hash_key(Key)).
-define(VALUE(Val), rdht_tx:encode_value(Val)).

%% @doc Specify how often a read/write suite can be executed in order not to
%%      hit a timeout (depending on the speed of the DB implementation).
-spec max_rw_tests_per_suite() -> pos_integer().
max_rw_tests_per_suite() ->
    10000.

%% @doc Returns the min of Desired and max_rw_tests_per_suite().
%%      Should be used to limit the number of executions of r/w suites.
-spec rw_suite_runs(Desired::pos_integer()) -> pos_integer().
rw_suite_runs(Desired) ->
    erlang:min(Desired, max_rw_tests_per_suite()).

init_per_suite(Config) ->
    Config3 = unittest_helper:start_minimal_procs(Config, [], false),
    tester:register_type_checker({typedef, intervals, interval, []}, intervals, is_well_formed),
    tester:register_value_creator({typedef, intervals, interval, []}, intervals, tester_create_interval, 1),
    Config3.

end_per_suite(Config) ->
    tester:unregister_type_checker({typedef, intervals, interval, []}),
    tester:unregister_value_creator({typedef, intervals, interval, []}),
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    pid_groups:join_as(ct_tests, ?MODULE),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

-define(db_equals_pattern(Actual, ExpectedPattern),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                case Actual of
                    {DB_EQUALS_PATTERN_DB, ExpectedPattern} -> DB_EQUALS_PATTERN_DB;
                    {DB_EQUALS_PATTERN_DB, Any} ->
                        ct:pal("Failed: Stacktrace ~p~n",
                               [util:get_stacktrace()]),
                        ?ct_fail("~p evaluated to \"~p\" which is "
                               "not the expected ~p",
                               [??Actual, Any, ??ExpectedPattern]),
                        DB_EQUALS_PATTERN_DB
                end
        end()).

read(_Config) ->
    prop_new(?KEY("Unknown")).

write(_Config) ->
    prop_write(?KEY("Key1"), ?VALUE("Value1"), 1, ?KEY("Key2")).

delete(_Config) ->
    prop_delete(?KEY("DeleteKey1"), ?VALUE("Value1"), false, 0, 1, ?KEY("DeleteKey2")),
    prop_delete(?KEY("DeleteKey1"), ?VALUE("Value1"), false, 1, 1, ?KEY("DeleteKey2")).

get_load_and_middle(_Config) ->
    DB = db_dht:new(db_dht),
    ?equals(db_dht:get_load(DB), 0),
    DB2 = db_dht:write(DB, rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1),
    ?equals(db_dht:get_load(DB2), 1),
    DB3 = db_dht:write(DB2, rt_SUITE:number_to_key(1), ?VALUE("Value1"), 2),
    ?equals(db_dht:get_load(DB3), 1),
    DB4 = db_dht:write(DB3, rt_SUITE:number_to_key(2), ?VALUE("Value2"), 1),
    ?equals(db_dht:get_load(DB4), 2),
    DB5 = db_dht:write(DB4, rt_SUITE:number_to_key(3), ?VALUE("Value3"), 1),
    DB6 = db_dht:write(DB5, rt_SUITE:number_to_key(4), ?VALUE("Value4"), 1),
    OrigFullList = db_dht:get_data(DB6),
    {DB7, HisList} = db_dht:split_data(DB6, node:mk_interval_between_ids(rt_SUITE:number_to_key(2), rt_SUITE:number_to_key(4))),
    ?equals(db_dht:read(DB7, rt_SUITE:number_to_key(3)), {ok, ?VALUE("Value3"), 1}),
    ?equals(db_dht:read(DB7, rt_SUITE:number_to_key(4)), {ok, ?VALUE("Value4"), 1}),
    ?equals(db_dht:get_load(DB7), 2),
    ?equals(length(HisList), 2),
    ?equals(length(db_dht:get_data(DB7)), 2),
    DB8 = db_dht:add_data(DB7, HisList),
    % lists could be in arbitrary order -> sort them
    ?equals(lists:sort(OrigFullList), lists:sort(db_dht:get_data(DB8))),
    db_dht:close(DB8).

%% @doc Some split_data tests using fixed values.
%% @see prop_split_data/2
split_data(_Config) ->
    prop_split_data([db_entry:new(rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1),
                     db_entry:new(rt_SUITE:number_to_key(2), ?VALUE("Value2"), 2)],
                    intervals:empty()),
    prop_split_data([db_entry:new(rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1),
                     db_entry:new(rt_SUITE:number_to_key(2), ?VALUE("Value2"), 2)],
                    intervals:all()),
    prop_split_data([db_entry:new(rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1),
                     db_entry:new(rt_SUITE:number_to_key(2), ?VALUE("Value2"), 2)],
                    intervals:new(rt_SUITE:number_to_key(2))),
    prop_split_data([db_entry:new(rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1),
                     db_entry:new(rt_SUITE:number_to_key(2), ?VALUE("Value2"), 2)],
                    intervals:new(rt_SUITE:number_to_key(5))),
    prop_split_data([db_entry:new(rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1),
                     db_entry:new(rt_SUITE:number_to_key(2), ?VALUE("Value2"), 2),
                     db_entry:new(rt_SUITE:number_to_key(3), ?VALUE("Value3"), 3),
                     db_entry:new(rt_SUITE:number_to_key(4), ?VALUE("Value4"), 4),
                     db_entry:new(rt_SUITE:number_to_key(5), ?VALUE("Value5"), 5)],
                    intervals:new('[', rt_SUITE:number_to_key(2), rt_SUITE:number_to_key(4), ')')),
    prop_split_data([db_entry:new(rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1),
                     db_entry:new(rt_SUITE:number_to_key(2), ?VALUE("Value2"), 2),
                     db_entry:new(rt_SUITE:number_to_key(3), ?VALUE("Value3"), 3),
                     db_entry:new(rt_SUITE:number_to_key(4), ?VALUE("Value4"), 4),
                     db_entry:new(rt_SUITE:number_to_key(5), ?VALUE("Value5"), 5)],
                    intervals:union(intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(3), ')'),
                                    intervals:new(rt_SUITE:number_to_key(4)))),
    prop_split_data([db_entry:set_writelock(db_entry:new(rt_SUITE:number_to_key(1), ?VALUE("Value1"), 1), 1),
                     db_entry:inc_readlock(db_entry:new(rt_SUITE:number_to_key(2), ?VALUE("Value2"), 2)),
                     db_entry:new(rt_SUITE:number_to_key(3), ?VALUE("Value3"), 3),
                     db_entry:new(rt_SUITE:number_to_key(4), ?VALUE("Value4"), 4),
                     db_entry:new(rt_SUITE:number_to_key(5), ?VALUE("Value5"), 5)],
                    intervals:union(intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(3), ')'),
                                    intervals:new(rt_SUITE:number_to_key(4)))).

%% @doc Some update_entries tests using fixed values.
%% @see prop_update_entries_helper/3
update_entries(_Config) ->
    prop_update_entries_helper([db_entry:new(?KEY("1"), ?VALUE("Value1"), 1),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 1),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 1),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 1),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 1)],
                               [db_entry:new(?KEY("1"), ?VALUE("Value1"), 2),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 2),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 2),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 2)],
                               [db_entry:new(?KEY("1"), ?VALUE("Value1"), 2),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 2),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 2),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 2)]),
    prop_update_entries_helper([db_entry:new(?KEY("1"), ?VALUE("Value1"), 1),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 1),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 1),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 1),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 1)],
                               [db_entry:new(?KEY("1"), ?VALUE("Value1"), 2),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 3)],
                               [db_entry:new(?KEY("1"), ?VALUE("Value1"), 2),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 1),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 1),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 3)]),
    prop_update_entries_helper([db_entry:new(?KEY("1"), ?VALUE("Value1"), 2),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 2),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 2),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 2)],
                               [db_entry:new(?KEY("1"), ?VALUE("Value1"), 1),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 1),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 1)],
                               [db_entry:new(?KEY("1"), ?VALUE("Value1"), 2),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 2),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 2),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 2)]),
    prop_update_entries_helper([db_entry:set_writelock(db_entry:new(?KEY("1"), ?VALUE("Value1"), 1), 1),
                                db_entry:inc_readlock(db_entry:new(?KEY("2"), ?VALUE("Value2"), 2)),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 1),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 1),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 1)],
                               [db_entry:new(?KEY("1"), ?VALUE("Value1"), 2),
                                db_entry:new(?KEY("2"), ?VALUE("Value2"), 2),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 2),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 2)],
                               [db_entry:set_writelock(db_entry:new(?KEY("1"), ?VALUE("Value1"), 1), 1),
                                db_entry:inc_readlock(db_entry:new(?KEY("2"), ?VALUE("Value2"), 2)),
                                db_entry:new(?KEY("3"), ?VALUE("Value3"), 2),
                                db_entry:new(?KEY("4"), ?VALUE("Value4"), 2),
                                db_entry:new(?KEY("5"), ?VALUE("Value5"), 2)]),
    prop_update_entry({?KEY("239309376718523519117394992299371645018"), empty_val, false, 0, -1},
                      <<6>>, false, 7, 4).

changed_keys(_Config) ->
    DB = db_dht:new(db_dht),

    ?equals(db_dht:get_changes(DB), {[], []}),

    DB2 = db_dht:stop_record_changes(DB),
    ?equals(db_dht:get_changes(DB2), {[], []}),

    DB3 = db_dht:record_changes(DB2, intervals:empty()),
    ?equals(db_dht:get_changes(DB3), {[], []}),

    db_dht:close(DB3).

% tester-based functions below:

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:new/0, db_dht getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_new(Key::?RT:key()) -> true.
prop_new(Key) ->
    DB = db_dht:new(db_dht),
    check_db(DB, {true, []}, 0, [], "check_db_new_1"),
    ?equals(db_dht:read(DB, Key), {ok, empty_val, -1}),
    check_entry(DB, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_new_1"),
    db_dht:close(DB),
    true.

tester_new(_Config) ->
    tester:test(?MODULE, prop_new, 1, rw_suite_runs(10), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:set_entry/2, db_dht getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_set_entry(DBEntry::db_entry:entry()) -> true.
prop_set_entry(DBEntry) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:set_entry(DB, DBEntry),
    IsNullEntry = db_entry:is_null(DBEntry),
    check_entry(DB2, db_entry:get_key(DBEntry), DBEntry,
                {ok, db_entry:get_value(DBEntry), db_entry:get_version(DBEntry)},
                not IsNullEntry, "check_entry_set_entry_1"),
    case not db_entry:is_empty(DBEntry) andalso
             not (db_entry:get_writelock(DBEntry) =/= false andalso db_entry:get_readlock(DBEntry) > 0) andalso
             db_entry:get_version(DBEntry) >= 0 of
        true -> check_db(DB2, {true, []}, 1, [DBEntry], "check_db_set_entry_0");
        _ when IsNullEntry ->
                check_db(DB2, {true, []}, 0, [], "check_db_set_entry_1");
        _    -> check_db(DB2, {false, [DBEntry]}, 1, [DBEntry], "check_db_set_entry_2")
    end,
    db_dht:close(DB2),
    true.

tester_set_entry(_Config) ->
    tester:test(?MODULE, prop_set_entry, 1, rw_suite_runs(10), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:update_entry/2, db_dht getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_update_entry(DBEntry1::db_entry:entry(), Value2::db_dht:value(), WriteLock2::boolean(),
                        ReadLock2::0..10, Version2::client_version()) -> true.
prop_update_entry(DBEntry1, Value2, WriteLock2, ReadLock2, Version2) ->
    DBEntry2 = create_db_entry(db_entry:get_key(DBEntry1), Value2, WriteLock2, ReadLock2, Version2),
    DB = db_dht:new(db_dht),
    DB2 = db_dht:set_entry(DB, DBEntry1),
    case db_entry:is_null(DBEntry1) of
        true -> % update not possible
            DB3 = DB2,
            ok;
        false ->
            DB3 = db_dht:update_entry(DB2, DBEntry2),
            IsNullEntry = db_entry:is_null(DBEntry2),
            check_entry(DB3, db_entry:get_key(DBEntry2), DBEntry2,
                        {ok, db_entry:get_value(DBEntry2), db_entry:get_version(DBEntry2)},
                        not IsNullEntry, "check_entry_update_entry_1"),
            case not db_entry:is_empty(DBEntry2) andalso
                     not (db_entry:get_writelock(DBEntry2) =/= false andalso db_entry:get_readlock(DBEntry2) > 0) andalso
                     db_entry:get_version(DBEntry2) >= 0 of
                true -> check_db(DB3, {true, []}, 1, [DBEntry2], "check_db_update_entry_0");
                _ when IsNullEntry ->
                    check_db(DB3, {true, []}, 0, [], "check_db_update_entry_1");
                _    -> check_db(DB3, {false, [DBEntry2]}, 1, [DBEntry2], "check_db_update_entry_2")
            end
    end,
    db_dht:close(DB3),
    true.

tester_update_entry(_Config) ->
    tester:test(?MODULE, prop_update_entry, 5, rw_suite_runs(10), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:delete_entry/2, db_dht getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete_entry1(DBEntry1::db_entry:entry()) -> true.
prop_delete_entry1(DBEntry1) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:set_entry(DB, DBEntry1),
    DB3 = db_dht:delete_entry(DB2, DBEntry1),
    check_entry(DB3, db_entry:get_key(DBEntry1), db_entry:new(db_entry:get_key(DBEntry1)),
                {ok, empty_val, -1}, false, "check_entry_delete_entry1_1"),
    check_db(DB3, {true, []}, 0, [], "check_db_delete_entry1_1"),
    db_dht:close(DB3),
    true.

-spec prop_delete_entry2(DBEntry1::db_entry:entry(), DBEntry2::db_entry:entry()) -> true.
prop_delete_entry2(DBEntry1, DBEntry2) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:set_entry(DB, DBEntry1),
    % note: DBEntry2 may not be the same
    DB3 = db_dht:delete_entry(DB2, DBEntry2),
    case db_entry:get_key(DBEntry1) =/= db_entry:get_key(DBEntry2) of
        true ->
            IsNullEntry = db_entry:is_null(DBEntry1),
            check_entry(DB3, db_entry:get_key(DBEntry1), DBEntry1,
                {ok, db_entry:get_value(DBEntry1), db_entry:get_version(DBEntry1)},
                not IsNullEntry, "check_entry_delete_entry2_1"),
            case not db_entry:is_empty(DBEntry1) andalso
                     not (db_entry:get_writelock(DBEntry1) =/= false andalso db_entry:get_readlock(DBEntry1) > 0) andalso
                     db_entry:get_version(DBEntry1) >= 0 of
                true -> check_db(DB3, {true, []}, 1, [DBEntry1], "check_db_delete_entry2_1a");
                _ when IsNullEntry ->
                        check_db(DB3, {true, []}, 0, [], "check_db_delete_entry2_1b");
                _    -> check_db(DB3, {false, [DBEntry1]}, 1, [DBEntry1], "check_db_delete_entry2_1c")
            end;
        _    ->
            check_entry(DB3, db_entry:get_key(DBEntry1), db_entry:new(db_entry:get_key(DBEntry1)),
                        {ok, empty_val, -1}, false, "check_entry_delete_entry2_2"),
            check_db(DB3, {true, []}, 0, [], "check_db_delete_entry2_2")
    end,
    db_dht:close(DB3),
    true.

tester_delete_entry1(_Config) ->
    tester:test(?MODULE, prop_delete_entry1, 1, rw_suite_runs(10), [{threads, 2}]).

tester_delete_entry2(_Config) ->
    tester:test(?MODULE, prop_delete_entry2, 2, rw_suite_runs(10), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:write/2, db_dht getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_write(Key::?RT:key(), Value::db_dht:value(), Version::client_version(), Key2::?RT:key()) -> true.
prop_write(Key, Value, Version, Key2) ->
    DBEntry = db_entry:new(Key, Value, Version),
    DB = db_dht:new(db_dht),
    DB2 = db_dht:write(DB, Key, Value, Version),
    check_entry(DB2, Key, DBEntry, {ok, Value, Version}, true, "check_entry_write_1"),
    check_db(DB2, {true, []}, 1, [DBEntry], "check_db_write_1"),
    case Key =/= Key2 of
        true -> check_entry(DB2, Key2, db_entry:new(Key2), {ok, empty_val, -1}, false, "write_2");
        _    -> ok
    end,
    db_dht:close(DB2),
    true.

tester_write(_Config) ->
    tester:test(?MODULE, prop_write, 4, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:delete/2, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete(Key::?RT:key(), Value::db_dht:value(), WriteLock::boolean(),
                  ReadLock::0..10, Version::client_version(), Key2::?RT:key()) -> true.
prop_delete(Key, Value, WriteLock, ReadLock, Version, Key2) ->
    DB = db_dht:new(db_dht),
    DBEntry = create_db_entry(Key, Value, WriteLock, ReadLock, Version),
    DB2 = db_dht:set_entry(DB, DBEntry),

    % delete DBEntry:
    DB3 =
        case db_entry:is_locked(DBEntry) of
            true ->
                DBA1 = ?db_equals_pattern(db_dht:delete(DB2, Key), locks_set),
                check_entry(DBA1, Key, DBEntry, {ok, Value, Version}, true, "check_entry_delete_1a"),
                case not db_entry:is_empty(DBEntry) andalso
                         not (db_entry:get_writelock(DBEntry) =/= false andalso db_entry:get_readlock(DBEntry) > 0) andalso
                         db_entry:get_version(DBEntry) >= 0 of
                    true -> check_db(DBA1, {true, []}, 1, [DBEntry], "check_db_delete_1ax");
                    _    -> check_db(DBA1, {false, [DBEntry]}, 1, [DBEntry], "check_db_delete_1ay")
                end,
                case Key =/= Key2 of
                    true ->
                        DBTmp = ?db_equals_pattern(db_dht:delete(DBA1, Key2), undef),
                        check_entry(DBTmp, Key, DBEntry, {ok, Value, Version}, true, "check_entry_delete_2a"),
                        case not db_entry:is_empty(DBEntry) andalso
                                 not (db_entry:get_writelock(DBEntry) =/= false andalso db_entry:get_readlock(DBEntry) > 0) andalso
                                 db_entry:get_version(DBEntry) >= 0 of
                            true -> check_db(DBTmp, {true, []}, 1, [DBEntry], "check_db_delete_2ax");
                            _    -> check_db(DBTmp, {false, [DBEntry]}, 1, [DBEntry], "check_db_delete_2ay")
                        end,
                        DBTmp;
                    _ -> DBA1
                end;
            _ ->
                DBA1 = ?db_equals_pattern(db_dht:delete(DB2, Key), ok),
                check_entry(DBA1, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_delete_1b"),
                check_db(DBA1, {true, []}, 0, [], "check_db_delete_1b"),
                DBA2 = ?db_equals_pattern(db_dht:delete(DBA1, Key), undef),
                check_entry(DBA2, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_delete_2b"),
                check_db(DBA2, {true, []}, 0, [], "check_db_delete_2b"),
                DBA2
        end,

    db_dht:close(DB3),
    true.

tester_delete(_Config) ->
    tester:test(?MODULE, prop_delete, 6, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:add_data/2, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_add_data(Data::db_dht:db_as_list()) -> true.
prop_add_data(Data) ->
    DB = db_dht:new(db_dht),

    UniqueCleanData = unittest_helper:scrub_data(Data),

    DB2 = db_dht:add_data(DB, Data),
    check_db2(DB2, length(UniqueCleanData), UniqueCleanData, "check_db_add_data_1"),

    db_dht:close(DB2),
    true.

tester_add_data(_Config) ->
    prop_add_data([create_db_entry(?KEY("3"), empty_val, false, 0, -1),
                   create_db_entry(?KEY("4"), <<"foo">>, false, 5, 230),
                   create_db_entry(?KEY("5"), empty_val, false, 3, -1)]),
    tester:test(?MODULE, prop_add_data, 1, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:get_entries/3 emulating the former get_range_kv/2 method
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_entries3_1(Data::db_dht:db_as_list(), Range::intervals:interval()) -> true.
prop_get_entries3_1(Data, Range) ->
    DB = db_dht:new(db_dht),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since db_dht:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    DB2 = db_dht:add_data(DB, UniqueData),

    FilterFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                              intervals:in(db_entry:get_key(A), Range)
                end,
    ValueFun = fun(DBEntry) -> {db_entry:get_key(DBEntry),
                                db_entry:get_value(DBEntry)}
               end,

    ?equals_w_note(lists:sort(db_dht:get_entries(DB2, FilterFun, ValueFun)),
                   lists:sort([ValueFun(A) || A <- lists:filter(FilterFun, UniqueData)]),
                   "check_get_entries3_1_1"),

    db_dht:close(DB2),
    true.

tester_get_entries3_1(_Config) ->
    tester:test(?MODULE, prop_get_entries3_1, 2, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:get_entries/3 emulating the former get_range_kvv/2 method
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_entries3_2(Data::db_dht:db_as_list(), Range::intervals:interval()) -> true.
prop_get_entries3_2(Data, Range) ->
    DB = db_dht:new(db_dht),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since db_dht:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    DB2 = db_dht:add_data(DB, UniqueData),

    FilterFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                              db_entry:get_writelock(A) =:= false andalso
                              intervals:in(db_entry:get_key(A), Range)
                end,
    ValueFun = fun(DBEntry) -> {db_entry:get_key(DBEntry),
                                db_entry:get_value(DBEntry),
                                db_entry:get_version(DBEntry)}
               end,

    ?equals_w_note(lists:sort(db_dht:get_entries(DB2, FilterFun, ValueFun)),
                   lists:sort([ValueFun(A) || A <- lists:filter(FilterFun, UniqueData)]),
                   "check_get_entries3_2_1"),

    db_dht:close(DB2),
    true.

tester_get_entries3_2(_Config) ->
    tester:test(?MODULE, prop_get_entries3_2, 2, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:get_entries/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_entries2(Data::db_dht:db_as_list(), Range::intervals:interval()) -> true.
prop_get_entries2(Data, Range) ->
    DB = db_dht:new(db_dht),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since db_dht:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    DB2 = db_dht:add_data(DB, UniqueData),

    InRangeFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                               intervals:in(db_entry:get_key(A), Range)
                 end,

    ?equals_w_note(lists:sort(db_dht:get_entries(DB2, Range)),
                   lists:sort(lists:filter(InRangeFun, UniqueData)),
                   "check_get_entries2_1"),

    db_dht:close(DB2),
    true.

tester_get_entries2(_Config) ->
    tester:test(?MODULE, prop_get_entries2, 2, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:get_load/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_load2(Data::db_dht:db_as_list(), LoadInterval::intervals:interval()) -> true.
prop_get_load2(Data, LoadInterval) ->
    DB = db_dht:new(db_dht),
    UniqueCleanData = unittest_helper:scrub_data(Data),

    DB2 = db_dht:add_data(DB, Data),

    FilterFun = fun(A) -> intervals:in(db_entry:get_key(A), LoadInterval) end,
    ValueFun = fun(_DBEntry) -> 1 end,

    ?equals_w_note(db_dht:get_load(DB2, LoadInterval),
                   length(lists:filter(FilterFun, UniqueCleanData)),
                   "check_get_load2_1"),
    ?equals_w_note(db_dht:get_load(DB2, LoadInterval),
                   length(db_dht:get_entries(DB2, FilterFun, ValueFun)),
                   "check_get_load2_2"),

    db_dht:close(DB2),
    true.

tester_get_load2(_Config) ->
    tester:test(?MODULE, prop_get_load2, 2, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:split_data/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_split_data(Data::db_dht:db_as_list(), Range::intervals:interval()) -> true.
prop_split_data(Data, Range) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    UniqueCleanData = unittest_helper:scrub_data(Data),

    InHisRangeFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                                  (not intervals:in(db_entry:get_key(A), Range))
                    end,
    InMyRangeFun = fun(A) -> intervals:in(db_entry:get_key(A), Range) end,

    {DB3, HisList} = db_dht:split_data(DB2, Range),
    ?equals_w_note(lists:sort(HisList),
                   lists:sort(lists:filter(InHisRangeFun, UniqueCleanData)),
                   "check_split_data_1"),
    ?equals_w_note(lists:sort(db_dht:get_data(DB3)),
                   lists:sort(lists:filter(InMyRangeFun, UniqueCleanData)),
                   "check_split_data_2"),

    db_dht:close(DB3),
    true.

tester_split_data(_Config) ->
    tester:test(?MODULE, prop_split_data, 2, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:update_entries/4
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_update_entries(Data::db_dht:db_as_list(), ItemsToUpdate::pos_integer()) -> true.
prop_update_entries(Data, ItemsToUpdate) ->
    UniqueCleanData = unittest_helper:scrub_data(Data),
    {UniqueUpdateData, UniqueOldData} =
    case length(UniqueCleanData) < ItemsToUpdate of
        true ->
            lists:split(length(UniqueCleanData), UniqueCleanData);
        _ ->
            lists:split(ItemsToUpdate, UniqueCleanData)
    end,

    ExpUpdatedData = UniqueUpdateData ++ UniqueOldData,

    prop_update_entries_helper(UniqueCleanData, UniqueUpdateData, ExpUpdatedData).

-spec prop_update_entries_helper(UniqueData::db_dht:db_as_list(), UniqueUpdateData::db_dht:db_as_list(), ExpUpdatedData::db_dht:db_as_list()) -> true.
prop_update_entries_helper(UniqueData, UniqueUpdateData, ExpUpdatedData) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, UniqueData),

    UpdatePred = fun(OldEntry, NewEntry) ->
                         db_entry:get_version(OldEntry) < db_entry:get_version(NewEntry)
                 end,
    UpdateVal = fun(_OldEntry, NewEntry) -> NewEntry end,

    DB3 = db_dht:update_entries(DB2, UniqueUpdateData, UpdatePred, UpdateVal),

    ?equals_w_note(lists:sort(db_dht:get_data(DB3)),
                   lists:sort(ExpUpdatedData),
                   "check_update_entries_1"),

    db_dht:close(DB3),
    true.

tester_update_entries(_Config) ->
    tester:test(?MODULE, prop_update_entries, 2, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:delete_entries/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete_entries1(Data::db_dht:db_as_list(), Range::intervals:interval()) -> true.
prop_delete_entries1(Data, Range) ->
    % use a range to delete entries
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),

    DB3 = db_dht:delete_entries(DB2, Range),

    UniqueCleanData = unittest_helper:scrub_data(Data),
    UniqueRemainingData = [DBEntry || DBEntry <- UniqueCleanData,
                                      not intervals:in(db_entry:get_key(DBEntry), Range)],
    check_db2(DB3, length(UniqueRemainingData), UniqueRemainingData, "check_db_delete_entries1_1"),

    db_dht:close(DB3),
    true.

-spec prop_delete_entries2(Data::db_dht:db_as_list(), Range::intervals:interval()) -> true.
prop_delete_entries2(Data, Range) ->
    % use a range to delete entries
    FilterFun = fun(DBEntry) -> not intervals:in(db_entry:get_key(DBEntry), Range) end,
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),

    DB3 = db_dht:delete_entries(DB2, FilterFun),

    UniqueCleanData = unittest_helper:scrub_data(Data),
    UniqueRemainingData = [DBEntry || DBEntry <- UniqueCleanData,
                                      intervals:in(db_entry:get_key(DBEntry), Range)],
    check_db2(DB3, length(UniqueRemainingData), UniqueRemainingData, "check_db_delete_entries2_1"),

    db_dht:close(DB3),
    true.

tester_delete_entries1(_Config) ->
    tester:test(?MODULE, prop_delete_entries1, 2, rw_suite_runs(1000), [{threads, 2}]).

tester_delete_entries2(_Config) ->
    tester:test(?MODULE, prop_delete_entries2, 2, rw_suite_runs(1000), [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_dht:record_changes/2, stop_record_changes/1 and get_changes/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_changed_keys_get_entry(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_get_entry(Data, ChangesInterval, Key) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    _ = db_dht:get_entry(DB3, Key),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_get_entry_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_get_entry_2"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_set_entry(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Entry::db_entry:entry()) -> true.
prop_changed_keys_set_entry(Data, ChangesInterval, Entry) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    Old = db_dht:get_entry(DB2, db_entry:get_key(Entry)),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    DB4 = db_dht:set_entry(DB3, Entry),
    check_changes(DB4, ChangesInterval, "changed_keys_set_entry_1"),
    check_entry_in_changes(DB4, ChangesInterval, Entry, Old, "changed_keys_set_entry_2"),

    DB5 = check_stop_record_changes(DB4, ChangesInterval, "changed_keys_set_entry_3"),

    db_dht:close(DB5),
    true.


-spec prop_changed_keys_update_entry(
        Data::[db_entry:entry(),...], ChangesInterval::intervals:interval(),
        UpdateVal::db_dht:value()) -> true.
prop_changed_keys_update_entry(Data, ChangesInterval, UpdateVal) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since db_dht:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    UpdateElement = util:randomelem(UniqueData),
    Old = db_dht:get_entry(DB2, db_entry:get_key(UpdateElement)),
    UpdatedElement = db_entry:set_value(UpdateElement, UpdateVal, db_entry:get_version(UpdateElement) + 1),

    case db_entry:is_null(Old) of
        true -> % element does not exist, i.e. was a null entry, -> cannot update
            DB5 = DB2;
        _ ->
            DB3 = db_dht:record_changes(DB2, ChangesInterval),
            DB4 = db_dht:update_entry(DB3, UpdatedElement),
            check_changes(DB4, ChangesInterval, "changed_update_entry_1"),
            check_entry_in_changes(DB4, ChangesInterval, UpdatedElement, Old, "changed_update_entry_2"),

            DB5 = check_stop_record_changes(DB4, ChangesInterval, "changed_update_entry_3")
    end,

    db_dht:close(DB5),
    true.

-spec prop_changed_keys_delete_entry(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Entry::db_entry:entry()) -> true.
prop_changed_keys_delete_entry(Data, ChangesInterval, Entry) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    Old = db_dht:get_entry(DB2, db_entry:get_key(Entry)),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    DB4 = db_dht:delete_entry(DB3, Entry),
    check_changes(DB4, ChangesInterval, "delete_entry_1"),
    check_key_in_deleted_no_locks(DB4, ChangesInterval, db_entry:get_key(Entry), Old, "delete_entry_2"),

    DB5 = check_stop_record_changes(DB4, ChangesInterval, "delete_entry_3"),

    db_dht:close(DB5),
    true.

-spec prop_changed_keys_read(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_read(Data, ChangesInterval, Key) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    _ = db_dht:read(DB3, Key),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_read_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_read_2"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_write(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key(), Value::db_dht:value(), Version::client_version()) -> true.
prop_changed_keys_write(Data, ChangesInterval, Key, Value, Version) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    Old = db_dht:get_entry(DB2, Key),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    DB4 = db_dht:write(DB3, Key, Value, Version),
    check_changes(DB4, ChangesInterval, "changed_keys_write_1"),
    ChangedEntry = db_dht:get_entry(DB4, Key),
    check_entry_in_changes(DB4, ChangesInterval, ChangedEntry, Old, "changed_keys_write_2"),

    DB5 = check_stop_record_changes(DB4, ChangesInterval, "changed_keys_write_3"),

    db_dht:close(DB5),
    true.

-spec prop_changed_keys_delete(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_delete(Data, ChangesInterval, Key) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    Old = db_dht:get_entry(DB2, Key),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    {DB4, _Status} = db_dht:delete(DB3, Key),
    check_changes(DB4, ChangesInterval, "delete_1"),
    check_key_in_deleted_no_locks(DB4, ChangesInterval, Key, Old, "delete_2"),

    DB5 = check_stop_record_changes(DB4, ChangesInterval, "delete_3"),

    db_dht:close(DB5),
    true.

-spec prop_changed_keys_get_entries2(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Interval::intervals:interval()) -> true.
prop_changed_keys_get_entries2(Data, ChangesInterval, Interval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    _ = db_dht:get_entries(DB3, Interval),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_get_entries2_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_get_entries2_2"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_get_entries4(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Interval::intervals:interval()) -> true.
prop_changed_keys_get_entries4(Data, ChangesInterval, Interval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    FilterFun = fun(E) -> (not db_entry:is_empty(E)) andalso
                              (not intervals:in(db_entry:get_key(E), Interval))
                end,
    ValueFun = fun(E) -> db_entry:get_key(E) end,

    _ = db_dht:get_entries(DB3, FilterFun, ValueFun),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_get_entries4_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_get_entries4_2"),

    db_dht:close(DB4),
    true.

-spec prop_get_chunk4(Keys::[?RT:key()], StartId::?RT:key(), Interval::intervals:interval(), ChunkSize::pos_integer() | all) -> true.
prop_get_chunk4(Keys2, StartId, Interval, ChunkSize) ->
    Keys = lists:usort(Keys2),
    %% ct:pal("prop_get_chunk4(~w, ~w, ~w, ~w)", [Keys2, StartId, Interval, ChunkSize]),
    DB = db_dht:new(db_dht),
    DB2 = lists:foldl(fun(Key, DBA) -> db_dht:write(DBA, Key, ?VALUE("Value"), 1) end, DB, Keys),
    {Next, Chunk} = db_dht:get_chunk(DB2, StartId, Interval, ChunkSize),
    % note: prevent erlang default printing from converting small int lists to strings:
    ChunkKeys = [{db_entry:get_key(C)} || C <- Chunk],
    %% ct:pal("-> ~.2p", [{Next, ChunkKeys}]),
    db_dht:close(DB2),
    ?equals(lists:usort(ChunkKeys), lists:sort(ChunkKeys)), % check for duplicates
    KeysInRange = [{Key} || Key <- Keys, intervals:in(Key, Interval)],
    KeysInRangeCount = length(KeysInRange),
    ExpectedChunkSize =
        case ChunkSize of
            all -> KeysInRangeCount;
            _   -> erlang:min(KeysInRangeCount, ChunkSize)
        end,
    if ExpectedChunkSize =/= length(ChunkKeys) ->
           ?ct_fail("chunk has wrong size ~w ~w ~w, expected size: ~w",
                    [ChunkKeys, Keys, Interval, ExpectedChunkSize]);
       true -> ok
    end,
    % elements in Chunk must be in Interval
    ?equals([X || X = {Key} <- ChunkKeys, not intervals:in(Key, Interval)],
            []),
    % Next is subset of Interval, no chunk entry is in Next:
    ?equals_w_note(intervals:is_subset(Next, Interval), true,
                   io_lib:format("Next ~.0p is not subset of ~.0p",
                                 [Next, Interval])),
    ?compare(fun(N, I) ->
                     ?implies(intervals:is_continuous(I) andalso (not intervals:in(StartId, I)),
                              intervals:is_continuous(N) orelse intervals:is_empty(N))
             end, Next, Interval),
    ?equals_w_note([X || X = {Key} <- ChunkKeys, intervals:in(Key, Next)],
                   [], io_lib:format("Next: ~.0p", [Next])),
    % if ChunkSize is all, Next must be empty!
    ?IIF(ChunkSize =:= all, ?equals(Next, intervals:empty()), true),
    % keys in Chunk plus keys in Next must be all keys in Interval
    KeysInChunkPlusNext = lists:usort(ChunkKeys
                                          ++ [{Key} || Key <- Keys, intervals:in(Key, Next)]),
    ?equals(KeysInChunkPlusNext, KeysInRange),
    % if Chunk not empty, first key must be first after StartId:
    if KeysInRangeCount > 0 ->
           FirstAfterStartId =
               case lists:partition(fun({Key}) -> Key >= StartId end, KeysInRange) of
                   {[], [H|_]} -> H;
                   {[H|_], _}  -> H
               end,
           ?compare(fun(ChunkKeysX, FirstAfterStartIdX) ->
                            hd(ChunkKeysX) =:= FirstAfterStartIdX
                    end, ChunkKeys, FirstAfterStartId);
       true -> true
    end.

-spec prop_get_split_key5(Keys::[?RT:key(),...], Begin::?RT:key(), End::?RT:key(), TargetLoad::pos_integer(), forward | backward) -> true.
prop_get_split_key5(Keys2, Begin, End, TargetLoad, ForwardBackward) ->
    Keys = lists:usort(Keys2),
    DB = db_dht:new(db_dht),
    DB2 = lists:foldl(fun(Key, DBA) -> db_dht:write(DBA, Key, ?VALUE("Value"), 1) end, DB, Keys),
    {SplitKey, TakenLoad} = db_dht:get_split_key(DB2, Begin, End, TargetLoad, ForwardBackward),
    SplitInterval = case ForwardBackward of
                        forward  ->
                            intervals:new('(', Begin, SplitKey, ']');
                        backward ->
                            intervals:new('(', SplitKey, Begin, ']')
                    end,
    {_Next, Chunk} = db_dht:get_chunk(DB2, Begin, SplitInterval, all),
    db_dht:close(DB2),

    ?compare(fun erlang:'=<'/2, TakenLoad, TargetLoad),
    ?compare(fun erlang:'=<'/2, length(Chunk), TargetLoad),
    case TakenLoad < TargetLoad of
        true ->
            ?equals(SplitKey, End);
        _ ->
            ok
    end,
    ?equals(length(Chunk), TakenLoad),
    true.

-spec prop_changed_keys_update_entries(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        Entry1::db_entry:entry(), Entry2::db_entry:entry()) -> true.
prop_changed_keys_update_entries(Data, ChangesInterval, Entry1, Entry2) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    Old1 = db_dht:get_entry(DB2, db_entry:get_key(Entry1)),
    Old2 = db_dht:get_entry(DB2, db_entry:get_key(Entry2)),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    UpdatePred = fun(OldEntry, NewEntry) ->
                         db_entry:get_version(OldEntry) < db_entry:get_version(NewEntry)
                 end,
    UpdateVal = fun(_OldEntry, NewEntry) -> NewEntry end,

    DB4 = db_dht:update_entries(DB3, [Entry1, Entry2], UpdatePred, UpdateVal),
    NewEntry1 = db_dht:get_entry(DB4, db_entry:get_key(Entry1)),
    NewEntry2 = db_dht:get_entry(DB4, db_entry:get_key(Entry2)),
    check_changes(DB4, ChangesInterval, "update_entries_1"),
    ?implies(db_entry:get_version(Old1) < db_entry:get_version(Entry1),
             check_entry_in_changes(DB4, ChangesInterval, NewEntry1, Old1, "update_entries_2")),
    ?implies(db_entry:get_version(Old2) < db_entry:get_version(Entry2),
             check_entry_in_changes(DB4, ChangesInterval, NewEntry2, Old2, "update_entries_3")),

    DB5 = check_stop_record_changes(DB4, ChangesInterval, "update_entries_4"),

    db_dht:close(DB5),
    true.

-spec prop_changed_keys_delete_entries1(
        Data::db_dht:db_as_list(), Range::intervals:interval(),
        ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_delete_entries1(Data, ChangesInterval, Range) ->
    % use a range to delete entries
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    DB4 = db_dht:delete_entries(DB3, Range),

    UniqueCleanData = unittest_helper:scrub_data(Data),
    DeletedKeys = [{db_entry:get_key(DBEntry), true}
                  || DBEntry <- UniqueCleanData,
                     intervals:in(db_entry:get_key(DBEntry), Range)],
    check_changes(DB4, ChangesInterval, "delete_entries1_1"),
    check_keys_in_deleted(DB4, ChangesInterval, DeletedKeys, "delete_entries1_2"),

    db_dht:close(DB3),
    true.

-spec prop_changed_keys_delete_entries2(
        Data::db_dht:db_as_list(), Range::intervals:interval(),
        ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_delete_entries2(Data, ChangesInterval, Range) ->
    % use a range to delete entries
    FilterFun = fun(DBEntry) -> not intervals:in(db_entry:get_key(DBEntry), Range) end,
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    DB4 = db_dht:delete_entries(DB3, FilterFun),

    UniqueCleanData = unittest_helper:scrub_data(Data),
    DeletedKeys = [{db_entry:get_key(DBEntry), true}
                  || DBEntry <- UniqueCleanData,
                     not intervals:in(db_entry:get_key(DBEntry), Range)],
    check_changes(DB4, ChangesInterval, "delete_entries2_1"),
    check_keys_in_deleted(DB4, ChangesInterval, DeletedKeys, "delete_entries2_2"),

    db_dht:close(DB3),
    true.

-spec prop_changed_keys_get_load(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_get_load(Data, ChangesInterval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    db_dht:get_load(DB3),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_get_load_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_get_load_2"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_get_load2(
        Data::db_dht:db_as_list(), LoadInterval::intervals:interval(),
        ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_get_load2(Data, LoadInterval, ChangesInterval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    db_dht:get_load(DB3, LoadInterval),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_get_load2_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_get_load2_2"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_split_data1(
        Data::db_dht:db_as_list(),
        ChangesInterval::intervals:interval(),
        MyNewInterval1::intervals:interval()) -> true.
prop_changed_keys_split_data1(Data, ChangesInterval, MyNewInterval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    {DB4, _HisList} = db_dht:split_data(DB3, MyNewInterval),
    ?equals_w_note(db_dht:get_changes(DB4), {[], []}, "split_data1_1"),

    DB5 = check_stop_record_changes(DB4, ChangesInterval, "split_data1_2"),

    db_dht:close(DB5),
    true.

-spec prop_changed_keys_split_data2(
        Data::db_dht:db_as_list(),
        ChangesInterval::intervals:interval(),
        MyNewInterval1::intervals:interval()) -> true.
prop_changed_keys_split_data2(Data, ChangesInterval, MyNewInterval) ->
    %% db_entries that are null won't be inserted into db anymore
    CleanData = unittest_helper:scrub_data(Data),
    DB = db_dht:new(db_dht),
    DB2 = db_dht:record_changes(DB, ChangesInterval),
    DB3 = db_dht:add_data(DB2, CleanData),

    {DB4, _HisList} = db_dht:split_data(DB3, MyNewInterval),

    check_changes(DB4, intervals:intersection(ChangesInterval, MyNewInterval), "split_data2_1"),

    DB5 = check_stop_record_changes(DB4, ChangesInterval, "split_data2_2"),

    db_dht:close(DB5),
    true.

-spec prop_changed_keys_get_data(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_get_data(Data, ChangesInterval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    UniqueCleanData = unittest_helper:scrub_data(Data),

    ?equals(lists:sort(db_dht:get_data(DB3)), lists:sort(UniqueCleanData)),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_get_data_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_get_data_2"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_add_data(
        Data::db_dht:db_as_list(),
        ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_add_data(Data, ChangesInterval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:record_changes(DB, ChangesInterval),

    DB3 = db_dht:add_data(DB2, Data),
    check_changes(DB3, ChangesInterval, "add_data_1"),

    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since db_dht:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    _ = [check_entry_in_changes(DB3, ChangesInterval, E, db_entry:new(db_entry:get_key(E)), "add_data_2")
           || E <- UniqueData],

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "add_data_3"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_check_db(
        Data::db_dht:db_as_list(), ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_check_db(Data, ChangesInterval) ->
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),
    DB3 = db_dht:record_changes(DB2, ChangesInterval),

    _ = db_dht:check_db(DB3),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, "changed_keys_check_db_1"),

    DB4 = check_stop_record_changes(DB3, ChangesInterval, "changed_keys_check_db_2"),

    db_dht:close(DB4),
    true.

-spec prop_changed_keys_mult_interval(
        Data::db_dht:db_as_list(), Entry1::db_entry:entry(),
        Entry2::db_entry:entry(), Entry3::db_entry:entry(),
        Entry4::db_entry:entry()) -> true.
prop_changed_keys_mult_interval(Data, Entry1, Entry2, Entry3, Entry4) ->
    CI1 = intervals:union(intervals:new(db_entry:get_key(Entry1)),
                          intervals:new(db_entry:get_key(Entry2))),
    CI2 = intervals:union(intervals:new(db_entry:get_key(Entry3)),
                          intervals:new(db_entry:get_key(Entry4))),
    CI1_2 = intervals:union(CI1, CI2),
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),

    DB3 = db_dht:record_changes(DB2, CI1),
    Old1 = db_dht:get_entry(DB3, db_entry:get_key(Entry1)),
    DB4 = db_dht:set_entry(DB3, Entry1),
    check_changes(DB4, CI1, "changed_keys_mult_interval_1"),
    check_entry_in_changes(DB4, CI1, Entry1, Old1, "changed_keys_mult_interval_2"),

    DB5 = db_dht:record_changes(DB4, CI2),
    Old2 = db_dht:get_entry(DB5, db_entry:get_key(Entry2)),
    DB6 = db_dht:set_entry(DB5, Entry2),
    check_changes(DB6, CI1_2, "changed_keys_mult_interval_3"),
    check_entry_in_changes(DB6, CI1_2, Entry2, Old2, "changed_keys_mult_interval_4"),

    DB7 = db_dht:record_changes(DB6, CI2),
    Old3 = db_dht:get_entry(DB7, db_entry:get_key(Entry3)),
    DB8 = db_dht:set_entry(DB7, Entry3),
    check_changes(DB8, CI1_2, "changed_keys_mult_interval_5"),
    check_entry_in_changes(DB8, CI1_2, Entry3, Old3, "changed_keys_mult_interval_6"),

    DB9 = db_dht:record_changes(DB8, CI2),
    Old4 = db_dht:get_entry(DB9, db_entry:get_key(Entry4)),
    DB10 = db_dht:set_entry(DB9, Entry4),
    check_changes(DB10, CI1_2, "changed_keys_mult_interval_7"),
    check_entry_in_changes(DB10, CI1_2, Entry4, Old4, "changed_keys_mult_interval_8"),

    DB11 = check_stop_record_changes(DB10, CI1_2, "changed_keys_mult_interval_9"),

    db_dht:close(DB11),
    true.

-spec prop_stop_record_changes(
        Data::db_dht:db_as_list(), Entry1::db_entry:entry(),
        Entry2::db_entry:entry(), Entry3::db_entry:entry(),
        Entry4::db_entry:entry()) -> true.
prop_stop_record_changes(Data, Entry1, Entry2, Entry3, Entry4) ->
    CI1 = intervals:union(intervals:new(db_entry:get_key(Entry1)),
                          intervals:new(db_entry:get_key(Entry2))),
    CI2 = intervals:union(intervals:new(db_entry:get_key(Entry3)),
                          intervals:new(db_entry:get_key(Entry4))),
    CI1_2 = intervals:union(CI1, CI2),
    CI1_wo2 = intervals:minus(CI1, CI2),
    DB = db_dht:new(db_dht),
    DB2 = db_dht:add_data(DB, Data),

    DB3 = db_dht:record_changes(DB2, CI1_2),
    Old1 = db_dht:get_entry(DB3, db_entry:get_key(Entry1)),
    DB4 = db_dht:set_entry(DB3, Entry1),
    check_changes(DB4, CI1_2, "stop_record_changes_1"),
    check_entry_in_changes(DB4, CI1_2, Entry1, Old1, "stop_record_changes_2"),

    Old3 = db_dht:get_entry(DB4, db_entry:get_key(Entry3)),
    DB5 = db_dht:set_entry(DB4, Entry3),
    check_changes(DB5, CI1_2, "stop_record_changes_3"),
    check_entry_in_changes(DB5, CI1_2, Entry3, Old3, "stop_record_changes_4"),

    DB6 = db_dht:stop_record_changes(DB5, CI2),
    check_changes(DB6, CI1_wo2, "stop_record_changes_5"),
    check_entry_in_changes(DB6, CI1_wo2, Entry1, Old1, "stop_record_changes_6"),

    Old2 = db_dht:get_entry(DB6, db_entry:get_key(Entry2)),
    DB7 = db_dht:set_entry(DB6, Entry2),
    check_changes(DB7, CI1_wo2, "stop_record_changes_7"),
    check_entry_in_changes(DB7, CI1_wo2, Entry2, Old2, "stop_record_changes_8"),

    Old4 = db_dht:get_entry(DB7, db_entry:get_key(Entry4)),
    DB8 = db_dht:set_entry(DB7, Entry4),
    check_changes(DB8, CI1_wo2, "stop_record_changes_9"),
    check_entry_in_changes(DB8, CI1_wo2, Entry4, Old4, "stop_record_changes_10"),

    DB9 = db_dht:stop_record_changes(DB8),
    ?equals_w_note(db_dht:get_changes(DB9), {[], []}, "stop_record_changes_11"),

    db_dht:close(DB9),
    true.

tester_changed_keys_get_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_entry, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_set_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_set_entry, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_update_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_update_entry, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_delete_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_delete_entry, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_read(_Config) ->
    tester:test(?MODULE, prop_changed_keys_read, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_write(_Config) ->
    tester:test(?MODULE, prop_changed_keys_write, 5, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_delete(_Config) ->
    tester:test(?MODULE, prop_changed_keys_delete, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_get_entries2(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_entries2, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_get_entries4(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_entries4, 3, rw_suite_runs(1000), [{threads, 2}]).

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

tester_get_chunk4(_Config) ->
    case rt_SUITE:default_rt_has_chord_keys() of
        true ->
            prop_get_chunk4([0, 4, 31], 0, intervals:new('[', 0, 4, ']'), 2),
            prop_get_chunk4([1, 5, 127, 13], 3, intervals:new('[', 3, 2, ']'), 4),
            prop_get_chunk4([30, 20, 8, 4], 9, intervals:union(intervals:new('[',0,10,']'), intervals:new('[',28,32,')')), all),
            prop_get_chunk4([321412035892863292970556376746395450950,178033137068077382596514331220271255735,36274679037320551674149151592760931654,24467032062604602002936599440583551943],
                            39662566533623950601697671725795532001,
                            intervals:union([intervals:new('[', 0, 117488216920678280505356111701746995698, ']'),
                                             intervals:new(161901968021578670353994653229245016552),
                                             intervals:new('[', 225156471921460939006161924022031177737, 340282366920938463463374607431768211456, ')')]),
                            all),
            prop_get_chunk4([12, 13, 14, 15, 16],0,intervals:new('[', 10, 0, ']'),2),
            prop_get_chunk4([12, 8, 6, 4], 4, intervals:new('[', 10, 0, ']'), 1),
            prop_get_chunk4([3, 4], 10, intervals:new('[', 0, 9, ']'), 1),
            prop_get_chunk4([2, 6, 7], 5, intervals:new(4), 3),
            prop_get_chunk4([2, 6, 7], 5, intervals:new('[',5,9,']'), 3);
        _ -> ok
    end,
    % TODO: fix this test
    prop_get_chunk4(
      [rt_SUITE:number_to_key(510),
       rt_SUITE:number_to_key(545)],
      rt_SUITE:number_to_key(530),
      intervals:new('(', rt_SUITE:number_to_key(532), rt_SUITE:number_to_key(530), ')'),
      all),

    tester:test(?MODULE, prop_get_chunk4, 4, rw_suite_runs(10000), [{threads, 2}]).

tester_get_split_key5(_Config) ->
    case rt_SUITE:default_rt_has_chord_keys() of
        true ->
            prop_get_split_key5([2], 6, 4, 12, backward),
            prop_get_split_key5([12, 10, 4], 6, 8, 1, backward),
            prop_get_split_key5([10, 9], 10, 6, 2, backward),
            prop_get_split_key5([10, 9, 8], 10, 6, 2, backward),
            prop_get_split_key5([10, 6, 5], 6, 9, 1, forward),
            prop_get_split_key5([10, 6, 5], 9, 6, 1, backward),
            prop_get_split_key5([10, 9, 8, 7], 10, 6, 2, backward),
            prop_get_split_key5([10, 9, 8, 7, 4], 10, 6, 2, backward),
            prop_get_split_key5([11, 10, 9, 8, 7, 4], 10, 6, 2, backward);
        _ -> ok
    end,
    % TODO: fix this test
    prop_get_split_key5([rt_SUITE:number_to_key(510),
                         rt_SUITE:number_to_key(520),
                         rt_SUITE:number_to_key(545)],
                        rt_SUITE:number_to_key(543),
                        rt_SUITE:number_to_key(520),
                        1, backward),

    tester:test(?MODULE, prop_get_split_key5, 5, rw_suite_runs(10000), [{threads, 2}]).

tester_changed_keys_update_entries(_Config) ->
    prop_changed_keys_update_entries(
      [{?KEY("200"),empty_val,false,0,-1}], intervals:all(),
      {?KEY("100"),empty_val,false,1,-1}, {?KEY("200"),empty_val,false,296,-1}),
    tester:test(?MODULE, prop_changed_keys_update_entries, 4, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_delete_entries1(_Config) ->
    tester:test(?MODULE, prop_changed_keys_delete_entries1, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_delete_entries2(_Config) ->
    tester:test(?MODULE, prop_changed_keys_delete_entries2, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_get_load(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_load, 2, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_get_load2(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_load2, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_split_data1(_Config) ->
    tester:test(?MODULE, prop_changed_keys_split_data1, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_split_data2(_Config) ->
    tester:test(?MODULE, prop_changed_keys_split_data2, 3, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_get_data(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_data, 2, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_add_data(_Config) ->
    tester:test(?MODULE, prop_changed_keys_add_data, 2, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_check_db(_Config) ->
    tester:test(?MODULE, prop_changed_keys_check_db, 2, rw_suite_runs(1000), [{threads, 2}]).

tester_changed_keys_mult_interval(_Config) ->
    tester:test(?MODULE, prop_changed_keys_mult_interval, 5, rw_suite_runs(1000), [{threads, 2}]).

tester_stop_record_changes(_Config) ->
    tester:test(?MODULE, prop_stop_record_changes, 5, rw_suite_runs(1000), [{threads, 2}]).


% helper functions:

-spec check_entry(DB::db_dht:db(), Key::?RT:key(), ExpDBEntry::db_entry:entry(),
                  ExpRead::{ok, Value::db_dht:value(), Version::client_version()} | {ok, empty_val, -1},
                  ExpExists::boolean(), Note::string()) -> true.
check_entry(DB, Key, ExpDBEntry, ExpRead, _ExpExists, Note) ->
    ?equals_w_note(db_dht:get_entry(DB, Key), ExpDBEntry, Note),
    ?equals_w_note(db_dht:read(DB, Key), ExpRead, Note).

% note: use manageable values for ReadLock!
-spec create_db_entry(Key::?RT:key(), Value::db_dht:value(), WriteLock::false | client_version(),
                      ReadLock::0..1000, Version::client_version()) -> db_entry:entry();
                     (Key::?RT:key(), Value::empty_val, WriteLock::false,
                      ReadLock::0..1000, Version::-1) -> db_entry:entry().
create_db_entry(Key, Value, WriteLock, ReadLock, Version) ->
    E1 = if Value =:= empty_val andalso Version =:= -1 -> db_entry:new(Key);
            true -> db_entry:new(Key, Value, Version)
         end,
    E2 = case WriteLock of
             true -> db_entry:set_writelock(E1, db_entry:get_version(E1));
             _    -> E1
         end,
    _E3 = inc_readlock(E2, ReadLock).

-spec inc_readlock(DBEntry::db_entry:entry(), Count::non_neg_integer()) -> db_entry:entry().
inc_readlock(DBEntry, 0) -> DBEntry;
inc_readlock(DBEntry, Count) -> inc_readlock(db_entry:inc_readlock(DBEntry), Count - 1).

-spec check_db(DB::db_dht:db(),
               ExpCheckDB::{true, []} | {false, InvalidEntries::db_dht:db_as_list()},
               ExpLoad::integer(),
               ExpData::db_dht:db_as_list(), Note::string()) -> true.
check_db(DB, ExpCheckDB, ExpLoad, ExpData, Note) ->
    check_db(DB, ExpCheckDB, ExpLoad, ExpData, {[], []}, Note).

-spec check_db(DB::db_dht:db(),
               ExpCheckDB::{true, []} | {false, InvalidEntries::db_dht:db_as_list()},
               ExpLoad::integer(),
               ExpData::db_dht:db_as_list(),
               ExpCKData::{UpdatedEntries::db_dht:db_as_list(), DeletedKeys::[?RT:key()]},
               Note::string()) -> true.
check_db(DB, ExpCheckDB, ExpLoad, ExpData, ExpCKData, Note) ->
    ?equals_w_note(db_dht:check_db(DB), ExpCheckDB, Note),
    ?equals_w_note(db_dht:get_load(DB), ExpLoad, Note),
    ?equals_w_note(lists:sort(db_dht:get_data(DB)), lists:sort(ExpData), Note),
    ?equals_w_note(db_dht:get_changes(DB), ExpCKData, Note).

%% @doc Like check_db/5 but do not check DB using db_dht:check_db.
-spec check_db2(DB::db_dht:db(), ExpLoad::integer(),
               ExpData::db_dht:db_as_list(), Note::string()) -> true.
check_db2(DB, ExpLoad, ExpData, Note) ->
    check_db2(DB, ExpLoad, ExpData, {[], []}, Note).

%% @doc Like check_db/5 but do not check DB using db_dht:check_db.
-spec check_db2(DB::db_dht:db(), ExpLoad::integer(),
                ExpData::db_dht:db_as_list(),
                ExpCKData::{UpdatedEntries::db_dht:db_as_list(), DeletedKeys::[?RT:key()]},
                Note::string()) -> true.
check_db2(DB, ExpLoad, ExpData, ExpCKData, Note) ->
    ?equals_w_note(db_dht:get_load(DB), ExpLoad, Note),
    ?equals_w_note(lists:sort(db_dht:get_data(DB)), lists:sort(ExpData), Note),
    ?equals_w_note(db_dht:get_changes(DB), ExpCKData, Note).

get_random_interval_from_changes(DB) ->
    {ChangedEntries, DeletedKeys} = db_dht:get_changes(DB),
    case ChangedEntries =/= [] orelse DeletedKeys =/= [] of
        true ->
            intervals:new(util:randomelem(
                            lists:append(
                              [db_entry:get_key(E) || E <- ChangedEntries],
                              DeletedKeys)));
        _    -> intervals:empty()
    end.

-spec check_stop_record_changes(DB::db_dht:db(), ChangesInterval::intervals:interval(), Note::string()) -> db_dht:db().
check_stop_record_changes(DB, ChangesInterval, Note) ->
    I1 = get_random_interval_from_changes(DB),
    DB2 = db_dht:stop_record_changes(DB, I1),
    check_changes(DB2, intervals:minus(ChangesInterval, I1), Note ++ "a"),

    DB3 = db_dht:stop_record_changes(DB2),
    ?equals_w_note(db_dht:get_changes(DB3), {[], []}, Note ++ "b"),

    DB3.


%% @doc Checks that all entries returned by db_dht:get_changes/1 are in the
%%      given interval.
-spec check_changes(DB::db_dht:db(), ChangesInterval::intervals:interval(), Note::string()) -> true.
check_changes(DB, ChangesInterval, Note) ->
    {ChangedEntries1, DeletedKeys1} = db_dht:get_changes(DB),
    case lists:all(fun(E) -> intervals:in(db_entry:get_key(E), ChangesInterval) end,
               ChangedEntries1) of
        false ->
            ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                     ["element(1, db_dht:get_changes(DB))", ChangedEntries1,
                      ChangesInterval, lists:flatten(Note)]);
        _ -> ok
    end,
    case lists:all(fun(E) -> intervals:in(E, ChangesInterval) end, DeletedKeys1) of
        false ->
            ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                     ["element(2, db_dht:get_changes(DB))", DeletedKeys1,
                      ChangesInterval, lists:flatten(Note)]);
        _ -> ok
    end,
    check_changes2(DB, ChangesInterval, ChangesInterval, Note),
    % select some random key from the changed entries and try get_changes/2
    % with an interval that does not contain this key
    case ChangedEntries1 =/= [] orelse DeletedKeys1 =/= [] of
        true ->
            SomeKey = util:randomelem(
                        lists:append(
                          [db_entry:get_key(E) || E <- ChangedEntries1],
                          DeletedKeys1)),
            check_changes2(DB, ChangesInterval, intervals:minus(ChangesInterval, intervals:new(SomeKey)), Note);
        _    -> true
    end.

%% @doc Checks that all entries returned by db_dht:get_changes/2 are in the
%%      given interval GetChangesInterval and also ChangesInterval.
-spec check_changes2(DB::db_dht:db(), ChangesInterval::intervals:interval(), GetChangesInterval::intervals:interval(), Note::string()) -> true.
check_changes2(DB, ChangesInterval, GetChangesInterval, Note) ->
    {ChangedEntries2, DeletedKeys2} = db_dht:get_changes(DB, GetChangesInterval),
    FinalInterval = intervals:intersection(ChangesInterval, GetChangesInterval),
    case lists:all(fun(E) -> intervals:in(db_entry:get_key(E), FinalInterval) end,
               ChangedEntries2) of
        false ->
            ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                     ["element(1, db_dht:get_changes(DB, FinalInterval))",
                      ChangedEntries2, FinalInterval, lists:flatten(Note)]);
        _ -> ok
    end,
    case lists:all(fun(E) -> intervals:in(E, FinalInterval) end, DeletedKeys2) of
        false ->
            ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                     ["element(2, db_dht:get_changes(DB, FinalInterval))",
                      DeletedKeys2, FinalInterval, lists:flatten(Note)]);
        _ -> ok
    end,
    true.

%% @doc Checks that a key is present exactly once in the list of deleted
%%      keys returned by db_dht:get_changes/1 if no lock is set on a
%%      previously existing entry.
-spec check_key_in_deleted_no_locks(
        DB::db_dht:db(), ChangesInterval::intervals:interval(), Key::?RT:key(),
        OldEntry::db_entry:entry(), Note::string()) -> true.
check_key_in_deleted_no_locks(DB, ChangesInterval, Key, Old, Note) ->
    case intervals:in(Key, ChangesInterval) andalso not db_entry:is_null(Old) andalso
             not db_entry:is_locked(Old) of
        true ->
            {_ChangedEntries, DeletedKeys} = db_dht:get_changes(DB),
            check_key_in_deleted_internal(DeletedKeys, ChangesInterval, Key,
                                          not db_entry:is_null(Old), Note);
        _    -> true
    end.

%% @doc Checks that all given keys are present exactly once in the list of
%%      deleted keys returned by db_dht:get_changes/1.
-spec check_keys_in_deleted(
        DB::db_dht:db(), ChangesInterval::intervals:interval(),
        Keys::[{?RT:key(), OldExists::boolean()}], Note::string()) -> true.
check_keys_in_deleted(DB, ChangesInterval, Keys, Note) ->
    {_ChangedEntries, DeletedKeys} = db_dht:get_changes(DB),
    [check_key_in_deleted_internal(DeletedKeys, ChangesInterval, Key, OldExists, Note)
    || {Key, OldExists} <- Keys],
    true.

-spec check_key_in_deleted_internal(
        DeletedKeys::[?RT:key()], ChangesInterval::intervals:interval(),
        Key::?RT:key(), OldExists::boolean(), Note::string()) -> true.
check_key_in_deleted_internal(DeletedKeys, ChangesInterval, Key, OldExists, Note) ->
    case intervals:in(Key, ChangesInterval) andalso OldExists of
        true ->
            case length([K || K <- DeletedKeys, K =:= Key]) of
                1 -> ok;
                _ -> ?ct_fail("element(2, db_dht:get_changes(DB)) evaluated "
                              "to \"~w\" and did not contain 1x deleted key ~w~n(~s)~n",
                              [DeletedKeys, Key, lists:flatten(Note)])
            end;
        _    -> true
    end.

%% @doc Checks that an entry is present exactly once in the list of changed
%%      entries returned by db_dht:get_changes/1.
-spec check_entry_in_changed_entries(
        DB::db_dht:db(), ChangesInterval::intervals:interval(), Entry::db_entry:entry(),
        OldEntry::db_entry:entry(), Note::string()) -> ok.
check_entry_in_changed_entries(DB, ChangesInterval, NewEntry, OldEntry, Note) ->
    {ChangedEntries, _DeletedKeys} = db_dht:get_changes(DB),
    check_entry_in_changed_entries_internal(ChangedEntries, ChangesInterval, NewEntry, OldEntry, Note).

-spec check_entry_in_changed_entries_internal(
        ChangedEntries::db_dht:db_as_list(), ChangesInterval::intervals:interval(),
        NewEntry::db_entry:entry(), OldEntry::db_entry:entry(), Note::string()) -> ok.
check_entry_in_changed_entries_internal(ChangedEntries, ChangesInterval, NewEntry, OldEntry, Note) ->
    case intervals:in(db_entry:get_key(NewEntry), ChangesInterval) andalso
             OldEntry =/= NewEntry of
        true ->
            case length([E || E <- ChangedEntries, E =:= NewEntry]) of
                1 -> ok;
                _ -> ?ct_fail("element(1, db_dht:get_changes(DB)) evaluated "
                              "to \"~w\" and did not contain 1x changed entry ~w~n(~s)~n",
                              [ChangedEntries, NewEntry, lists:flatten(Note)])
            end;
        _    -> ok
    end.

%% @doc Checks that an entry is present exactly once in the list of changed
%%      entries returned by db_dht:get_changes/1.
-spec check_entry_in_changes(
        DB::db_dht:db(), ChangesInterval::intervals:interval(), Entry::db_entry:entry(),
        OldEntry::db_entry:entry(), Note::string()) -> ok.
check_entry_in_changes(DB, ChangesInterval, NewEntry, OldEntry, Note) ->
    {ChangedEntries, DeletedKeys} = db_dht:get_changes(DB),
    case db_entry:is_null(NewEntry) of
        true ->
            check_key_in_deleted_internal(DeletedKeys, ChangesInterval,
                                          db_entry:get_key(NewEntry),
                                          not db_entry:is_null(OldEntry), Note);
        _ ->
            check_entry_in_changed_entries_internal(ChangedEntries, ChangesInterval, NewEntry, OldEntry, Note)
    end.

-spec count_keys_in_range(Keys::[?RT:key()], Interval::intervals:interval()) -> non_neg_integer().
count_keys_in_range(Keys, Interval) ->
    lists:foldl(fun(Key, Count) ->
                        case intervals:in(Key, Interval) of
                            true -> Count + 1;
                            _    -> Count
                        end
                end, 0, Keys).
