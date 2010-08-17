% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%%% @doc    Unit tests for database implementations. Define the TEST_DB macro
%%          to set the database module that is being tested.
%%% @end
%% @version $Id$

-include("../include/scalaris.hrl").

-include("unittest.hrl").

tests_avail() ->
    [read, write, write_lock, read_lock, read_write_lock, write_read_lock,
     delete, get_load_and_middle, split_data, update_entries,
     changed_keys,
     % random tester functions:
     tester_new, tester_set_entry, tester_update_entry,
     tester_delete_entry1, tester_delete_entry2,
     tester_write, tester_write_lock, tester_read_lock,
     tester_read_write_lock, tester_write_read_lock,
     tester_delete, tester_add_data,
     tester_get_entries2, tester_get_entries3_1, tester_get_entries3_2,
     tester_split_data, tester_update_entries,
     tester_changed_keys_get_entry,
     tester_changed_keys_set_entry,
     tester_changed_keys_update_entry,
     tester_changed_keys_delete_entry,
     tester_changed_keys_read,
     tester_changed_keys_write,
     tester_changed_keys_delete,
     tester_changed_keys_set_write_lock,
     tester_changed_keys_unset_write_lock,
     tester_changed_keys_set_read_lock,
     tester_changed_keys_unset_read_lock,
     tester_changed_keys_get_entries2,
     tester_changed_keys_get_entries4,
     tester_changed_keys_update_entries,
     tester_changed_keys_get_load,
     tester_changed_keys_split_data1,
     tester_changed_keys_split_data2,
     tester_changed_keys_get_data,
     tester_changed_keys_add_data,
     tester_changed_keys_check_db,
     tester_changed_keys_mult_interval
    ].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

-spec spawn_config_processes() -> pid().
spawn_config_processes() ->
    Owner = self(),
    Pid = spawn(fun() ->
                        crypto:start(),
                        pid_groups:start_link(),
                        config:start_link(["scalaris.cfg", "scalaris.local.cfg"]),
                        Owner ! {continue},
                        receive {done} -> ok
                        end
                end),
    receive {continue} -> ok
    end,
    Pid.

%% @doc Returns the min of Desired and max_rw_tests_per_suite().
%%      Should be used to limit the number of executions of r/w suites.
-spec rw_suite_runs(Desired::pos_integer()) -> pos_integer().
rw_suite_runs(Desired) ->
    erlang:min(Desired, max_rw_tests_per_suite()).

init_per_suite(Config) ->
    application:start(log4erl),
    crypto:start(),
    ct:pal("DB suite running with: ~p~n", [?TEST_DB]),
    file:set_cwd("../bin"),
    Pid = spawn_config_processes(),
    timer:sleep(100),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    case lists:keyfind(wrapper_pid, 1, Config) of
        false ->
            ok;
        {wrapper_pid, Pid} ->
            gen_component:kill(pid_groups),
            exit(Pid, kill)
    end,
    ok.

-define(db_equals_pattern(Actual, ExpectedPattern),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                case Actual of
                    {DB_EQUALS_PATTERN_DB, ExpectedPattern} -> DB_EQUALS_PATTERN_DB;
                    {DB_EQUALS_PATTERN_DB, Any} ->
                        ct:pal("Failed: Stacktrace ~p~n",
                               [erlang:get_stacktrace()]),
                        ?ct_fail("~p evaluated to \"~p\" which is "
                               "not the expected ~p",
                               [??Actual, Any, ??ExpectedPattern]),
                        DB_EQUALS_PATTERN_DB
                end
        end()).

read(_Config) ->
    prop_new(1, ?RT:hash_key("Unknown")).

write(_Config) ->
    prop_write(?RT:hash_key("Key1"), "Value1", 1, ?RT:hash_key("Key2")).

write_lock(_Config) ->
    prop_write_lock(?RT:hash_key("WriteLockKey1"), "Value1", 1, ?RT:hash_key("WriteLockKey2")).

read_lock(_Config) ->
    prop_read_lock(?RT:hash_key("ReadLockKey1"), "Value1", 1).

read_write_lock(_Config) ->
    prop_read_write_lock(?RT:hash_key("ReadWriteLockKey1"), "Value1", 1).

write_read_lock(_Config) ->
    prop_write_read_lock(?RT:hash_key("WriteReadLockKey1"), "Value1", 1).

delete(_Config) ->
    prop_delete(?RT:hash_key("DeleteKey1"), "Value1", false, 0, 1, ?RT:hash_key("DeleteKey2")),
    prop_delete(?RT:hash_key("DeleteKey1"), "Value1", false, 1, 1, ?RT:hash_key("DeleteKey2")).

get_load_and_middle(_Config) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    ?equals(?TEST_DB:get_load(DB), 0),
    DB2 = ?TEST_DB:write(DB, "Key1", "Value1", 1),
    ?equals(?TEST_DB:get_load(DB2), 1),
    DB3 = ?TEST_DB:write(DB2, "Key1", "Value1", 2),
    ?equals(?TEST_DB:get_load(DB3), 1),
    DB4 = ?TEST_DB:write(DB3, "Key2", "Value2", 1),
    ?equals(?TEST_DB:get_load(DB4), 2),
    DB5 = ?TEST_DB:write(DB4, "Key3", "Value3", 1),
    DB6 = ?TEST_DB:write(DB5, "Key4", "Value4", 1),
    OrigFullList = ?TEST_DB:get_data(DB6),
    {DB7, HisList} = ?TEST_DB:split_data(DB6, node:mk_interval_between_ids("Key2", "Key4")),
    ?equals(?TEST_DB:read(DB7, "Key3"), {ok, "Value3", 1}),
    ?equals(?TEST_DB:read(DB7, "Key4"), {ok, "Value4", 1}),
    ?equals(?TEST_DB:get_load(DB7), 2),
    ?equals(length(HisList), 2),
    ?equals(length(?TEST_DB:get_data(DB7)), 2),
    DB8 = ?TEST_DB:add_data(DB7, HisList),
    % lists could be in arbitrary order -> sort them
    ?equals(lists:sort(OrigFullList), lists:sort(?TEST_DB:get_data(DB8))),
    ?TEST_DB:close(DB8).

%% @doc Some split_data tests using fixed values.
%% @see prop_split_data/2
split_data(_Config) ->
    prop_split_data([db_entry:new(1, "Value1", 1),
                     db_entry:new(2, "Value2", 2)], intervals:empty()),
    prop_split_data([db_entry:new(1, "Value1", 1),
                     db_entry:new(2, "Value2", 2)], intervals:all()),
    prop_split_data([db_entry:new(1, "Value1", 1),
                     db_entry:new(2, "Value2", 2)], intervals:new(2)),
    prop_split_data([db_entry:new(1, "Value1", 1),
                     db_entry:new(2, "Value2", 2)], intervals:new(5)),
    prop_split_data([db_entry:new(1, "Value1", 1),
                     db_entry:new(2, "Value2", 2),
                     db_entry:new(3, "Value3", 3),
                     db_entry:new(4, "Value4", 4),
                     db_entry:new(5, "Value5", 5)],
                    intervals:new('[', 2, 4, ')')),
    prop_split_data([db_entry:new(1, "Value1", 1),
                     db_entry:new(2, "Value2", 2),
                     db_entry:new(3, "Value3", 3),
                     db_entry:new(4, "Value4", 4),
                     db_entry:new(5, "Value5", 5)],
                    intervals:union(intervals:new('[', 1, 3, ')'),
                                    intervals:new(4))),
    prop_split_data([db_entry:set_writelock(db_entry:new(1, "Value1", 1)),
                     db_entry:inc_readlock(db_entry:new(2, "Value2", 2)),
                     db_entry:new(3, "Value3", 3),
                     db_entry:new(4, "Value4", 4),
                     db_entry:new(5, "Value5", 5)],
                    intervals:union(intervals:new('[', 1, 3, ')'),
                                    intervals:new(4))).

%% @doc Some update_entries tests using fixed values.
%% @see prop_update_entries_helper/3
update_entries(_Config) ->
    prop_update_entries_helper([db_entry:new(1, "Value1", 1),
                                db_entry:new(2, "Value2", 1),
                                db_entry:new(3, "Value3", 1),
                                db_entry:new(4, "Value4", 1),
                                db_entry:new(5, "Value5", 1)],
                               [db_entry:new(1, "Value1", 2),
                                db_entry:new(2, "Value2", 2),
                                db_entry:new(3, "Value3", 2),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 2)],
                               [db_entry:new(1, "Value1", 2),
                                db_entry:new(2, "Value2", 2),
                                db_entry:new(3, "Value3", 2),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 2)]),
    prop_update_entries_helper([db_entry:new(1, "Value1", 1),
                                db_entry:new(2, "Value2", 1),
                                db_entry:new(3, "Value3", 1),
                                db_entry:new(4, "Value4", 1),
                                db_entry:new(5, "Value5", 1)],
                               [db_entry:new(1, "Value1", 2),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 3)],
                               [db_entry:new(1, "Value1", 2),
                                db_entry:new(2, "Value2", 1),
                                db_entry:new(3, "Value3", 1),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 3)]),
    prop_update_entries_helper([db_entry:new(1, "Value1", 2),
                                db_entry:new(2, "Value2", 2),
                                db_entry:new(3, "Value3", 2),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 2)],
                               [db_entry:new(1, "Value1", 1),
                                db_entry:new(4, "Value4", 1),
                                db_entry:new(5, "Value5", 1)],
                               [db_entry:new(1, "Value1", 2),
                                db_entry:new(2, "Value2", 2),
                                db_entry:new(3, "Value3", 2),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 2)]),
    prop_update_entries_helper([db_entry:set_writelock(db_entry:new(1, "Value1", 1)),
                                db_entry:inc_readlock(db_entry:new(2, "Value2", 2)),
                                db_entry:new(3, "Value3", 1),
                                db_entry:new(4, "Value4", 1),
                                db_entry:new(5, "Value5", 1)],
                               [db_entry:new(1, "Value1", 2),
                                db_entry:new(2, "Value2", 2),
                                db_entry:new(3, "Value3", 2),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 2)],
                               [db_entry:set_writelock(db_entry:new(1, "Value1", 1)),
                                db_entry:inc_readlock(db_entry:new(2, "Value2", 2)),
                                db_entry:new(3, "Value3", 2),
                                db_entry:new(4, "Value4", 2),
                                db_entry:new(5, "Value5", 2)]).

changed_keys(_Config) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    
    ?equals(?TEST_DB:get_changes(DB), {[], []}),
    
    DB2 = ?TEST_DB:stop_record_changes(DB),
    ?equals(?TEST_DB:get_changes(DB2), {[], []}),
    
    DB3 = ?TEST_DB:record_changes(DB2, intervals:empty()),
    ?equals(?TEST_DB:get_changes(DB3), {[], []}),
    
    ?TEST_DB:close(DB3).

% tester-based functions below:

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:new/1, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_new(NodeId::?RT:key(), Key::?RT:key()) -> true.
prop_new(NodeId, Key) ->
    DB = ?TEST_DB:new(NodeId),
    check_db(DB, {true, []}, 0, [], "check_db_new_1"),
    ?equals(?TEST_DB:read(DB, Key), {ok, empty_val, -1}),
    check_entry(DB, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_new_1"),
    ?TEST_DB:close(DB),
    true.

tester_new(_Config) ->
    tester:test(?MODULE, prop_new, 2, rw_suite_runs(10)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:set_entry/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_set_entry(DBEntry::db_entry:entry()) -> true.
prop_set_entry(DBEntry) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry),
    check_entry(DB2, db_entry:get_key(DBEntry), DBEntry,
                {ok, db_entry:get_value(DBEntry), db_entry:get_version(DBEntry)},
                true, "check_entry_set_entry_1"),
    case not db_entry:is_empty(DBEntry) andalso
             not (db_entry:get_writelock(DBEntry) andalso db_entry:get_readlock(DBEntry) > 0) andalso
             db_entry:get_version(DBEntry) >= 0 of
        true -> check_db(DB2, {true, []}, 1, [DBEntry], "check_db_set_entry_1");
        _    -> check_db(DB2, {false, [DBEntry]}, 1, [DBEntry], "check_db_set_entry_2")
    end,
    ?TEST_DB:close(DB2),
    true.

tester_set_entry(_Config) ->
    tester:test(?MODULE, prop_set_entry, 1, rw_suite_runs(10)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:update_entry/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_update_entry(DBEntry1::db_entry:entry(), Value2::?DB:value(), WriteLock2::boolean(),
                        ReadLock2::non_neg_integer(), Version2::?DB:version()) -> true.
prop_update_entry(DBEntry1, Value2, WriteLock2, ReadLock2, Version2) ->
    DBEntry2 = create_db_entry(db_entry:get_key(DBEntry1), Value2, WriteLock2, ReadLock2, Version2),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry1),
    DB3 = ?TEST_DB:update_entry(DB2, DBEntry2),
    check_entry(DB3, db_entry:get_key(DBEntry2), DBEntry2,
                {ok, db_entry:get_value(DBEntry2), db_entry:get_version(DBEntry2)},
                true, "check_entry_set_entry_1"),
    case not db_entry:is_empty(DBEntry2) andalso
             not (db_entry:get_writelock(DBEntry2) andalso db_entry:get_readlock(DBEntry2) > 0) andalso
             db_entry:get_version(DBEntry2) >= 0 of
        true -> check_db(DB3, {true, []}, 1, [DBEntry2], "check_db_set_entry_1");
        _    -> check_db(DB3, {false, [DBEntry2]}, 1, [DBEntry2], "check_db_set_entry_2")
    end,
    ?TEST_DB:close(DB3),
    true.

tester_update_entry(_Config) ->
    tester:test(?MODULE, prop_update_entry, 5, rw_suite_runs(10)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:delete_entry/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete_entry1(DBEntry1::db_entry:entry()) -> true.
prop_delete_entry1(DBEntry1) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry1),
    DB3 = ?TEST_DB:delete_entry(DB2, DBEntry1),
    check_entry(DB3, db_entry:get_key(DBEntry1), db_entry:new(db_entry:get_key(DBEntry1)),
                {ok, empty_val, -1}, false, "check_entry_delete_entry1_1"),
    check_db(DB3, {true, []}, 0, [], "check_db_delete_entry1_1"),
    ?TEST_DB:close(DB3),
    true.

-spec prop_delete_entry2(DBEntry1::db_entry:entry(), DBEntry2::db_entry:entry()) -> true.
prop_delete_entry2(DBEntry1, DBEntry2) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry1),
    % note: DBEntry2 may not be the same
    DB3 = ?TEST_DB:delete_entry(DB2, DBEntry2),
    case db_entry:get_key(DBEntry1) =/= db_entry:get_key(DBEntry2) of
        true ->
            check_entry(DB3, db_entry:get_key(DBEntry1), DBEntry1,
                {ok, db_entry:get_value(DBEntry1), db_entry:get_version(DBEntry1)},
                true, "check_entry_delete_entry2_1"),
            case not db_entry:is_empty(DBEntry1) andalso
                     not (db_entry:get_writelock(DBEntry1) andalso db_entry:get_readlock(DBEntry1) > 0) andalso
                     db_entry:get_version(DBEntry1) >= 0 of
                true -> check_db(DB3, {true, []}, 1, [DBEntry1], "check_db_delete_entry2_1a");
                _    -> check_db(DB3, {false, [DBEntry1]}, 1, [DBEntry1], "check_db_delete_entry2_1b")
            end;
        _    ->
            check_entry(DB3, db_entry:get_key(DBEntry1), db_entry:new(db_entry:get_key(DBEntry1)),
                        {ok, empty_val, -1}, false, "check_entry_delete_entry2_2"),
            check_db(DB3, {true, []}, 0, [], "check_db_delete_entry2_2")
    end,
    ?TEST_DB:close(DB3),
    true.

tester_delete_entry1(_Config) ->
    tester:test(?MODULE, prop_delete_entry1, 1, rw_suite_runs(10)).

tester_delete_entry2(_Config) ->
    tester:test(?MODULE, prop_delete_entry2, 2, rw_suite_runs(10)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:write/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_write(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version(), Key2::?RT:key()) -> true.
prop_write(Key, Value, Version, Key2) ->
    DBEntry = db_entry:new(Key, Value, Version),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:write(DB, Key, Value, Version),
    check_entry(DB2, Key, DBEntry, {ok, Value, Version}, true, "check_entry_write_1"),
    check_db(DB2, {true, []}, 1, [DBEntry], "check_db_write_1"),
    case Key =/= Key2 of
        true -> check_entry(DB2, Key2, db_entry:new(Key2), {ok, empty_val, -1}, false, "write_2");
        _    -> ok
    end,
    ?TEST_DB:close(DB2),
    true.

tester_write(_Config) ->
    tester:test(?MODULE, prop_write, 4, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_write_lock/2 in various scenarios, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_write_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version(), Key2::?RT:key()) -> true.
prop_write_lock(Key, Value, Version, Key2) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    
    % unlocking non-existing keys should fail and leave the DB in its previous state:
    DB1 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB, Key), failed),
    check_entry(DB1, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_write_lock_0"),
    check_db(DB1, {true, []}, 0, [], "check_db_write_lock_0"),
    
    % lock on existing entry:
    DB2 = ?TEST_DB:write(DB1, Key, Value, Version),
    check_entry(DB2, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_write_lock_1"),
    check_db(DB2, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_1"),
    DB3 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB2, Key), ok),
    check_entry(DB3, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, true, "check_entry_write_lock_2"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_lock_2"),
    % lock on locked key should fail
    DB4 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB3, Key), failed),
    check_entry(DB4, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, true, "check_entry_write_lock_3"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_lock_3"),
    % unlock key
    DB5 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB4, Key), ok),
    check_entry(DB5, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_write_lock_4"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_4"),
    % lockable again?
    DB6 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, true, "check_entry_write_lock_5"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_lock_5"),
    % unlock to finish
    DB7 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB6, Key), ok),
    check_entry(DB7, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_write_lock_6"),
    check_db(DB7, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_6"),
    % unlock unlocked key should fail
    DB8 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB7, Key), failed),
    check_entry(DB8, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_write_lock_7"),
    check_db(DB8, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_7"),

    % lock on non-existing key:
    FinalDB =
        case Key =/= Key2 of
            true ->
                LastEntry = create_db_entry(Key, Value, false, 0, Version),
                DB9 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB8, Key2), ok),
                check_entry(DB9, Key2, create_db_entry(Key2, empty_val, true, 0, -1), {ok, empty_val, -1}, true, "check_entry_write_lock_8"),
                check_db(DB9, {false, [create_db_entry(Key2, empty_val, true, 0, -1)]}, 2, [LastEntry, create_db_entry(Key2, empty_val, true, 0, -1)], "check_db_write_lock_8"),
                % lock on locked key should fail
                DB10 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB9, Key2), failed),
                check_entry(DB10, Key2, create_db_entry(Key2, empty_val, true, 0, -1), {ok, empty_val, -1}, true, "check_entry_write_lock_9"),
                check_db(DB10, {false, [create_db_entry(Key2, empty_val, true, 0, -1)]}, 2, [LastEntry, create_db_entry(Key2, empty_val, true, 0, -1)], "check_db_write_lock_9"),
                % unlock key should remove the empty_val
                DB11 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB10, Key2), ok),
                check_entry(DB11, Key2, db_entry:new(Key2), {ok, empty_val, -1}, false, "check_entry_write_lock_10"),
                check_db(DB11, {true, []}, 1, [LastEntry], "check_db_write_lock_10"),
                % lockable again?
                DB12 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB11, Key2), ok),
                check_entry(DB12, Key2, create_db_entry(Key2, empty_val, true, 0, -1), {ok, empty_val, -1}, true, "check_entry_write_lock_11"),
                check_db(DB12, {false, [create_db_entry(Key2, empty_val, true, 0, -1)]}, 2, [LastEntry, create_db_entry(Key2, empty_val, true, 0, -1)], "check_db_write_lock_11"),
                % unlock to finish (should remove the empty_val)
                DB13 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB12, Key2), ok),
                check_entry(DB13, Key2, db_entry:new(Key2), {ok, empty_val, -1}, false, "check_entry_write_lock_12"),
                check_db(DB13, {true, []}, 1, [LastEntry], "check_db_write_lock_12"),
                % unlock unlocked key should fail
                DB14 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB13, Key2), failed),
                check_entry(DB14, Key2, db_entry:new(Key2), {ok, empty_val, -1}, false, "check_entry_write_lock_13"),
                check_db(DB14, {true, []}, 1, [LastEntry], "check_db_write_lock_13"),
                DB14;
            _ -> DB8
        end,
    ?TEST_DB:close(FinalDB),
    true.

tester_write_lock(_Config) ->
    tester:test(?MODULE, prop_write_lock, 4, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_read_lock/2 in various scenarios, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_read_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version()) -> true.
prop_read_lock(Key, Value, Version) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    
    % (un)locking non-existing keys should fail and leave the DB in its previous state:
    DB1 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB, Key), failed),
    check_entry(DB1, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_read_lock_1"),
    check_db(DB1, {true, []}, 0, [], "check_db_read_lock_1"),
    DB2 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB1, Key), failed),
    check_entry(DB2, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_read_lock_2"),
    check_db(DB2, {true, []}, 0, [], "check_db_read_lock_2"),
    
    % lock on existing entry:
    DB3 = ?TEST_DB:write(DB2, Key, Value, Version),
    check_entry(DB3, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_read_lock_3"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_read_lock_3"),
    % read lock on existing key
    DB4 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB3, Key), ok),
    check_entry(DB4, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, true, "check_entry_read_lock_4"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_lock_4"),
    DB5 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB4, Key), ok),
    check_entry(DB5, Key, create_db_entry(Key, Value, false, 2, Version), {ok, Value, Version}, true, "check_entry_read_lock_5"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, false, 2, Version)], "check_db_read_lock_5"),
    % read unlock on existing key
    DB6 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, true, "check_entry_read_lock_6"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_lock_6"),
    % unlockable again?
    DB7 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB6, Key), ok),
    check_entry(DB7, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_read_lock_7"),
    check_db(DB7, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_read_lock_7"),
    % read unlock on non-read-locked key
    DB8 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB7, Key), failed),
    check_entry(DB8, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_read_lock_8"),
    check_db(DB8, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_read_lock_8"),
    
    ?TEST_DB:close(DB8),
    true.

tester_read_lock(_Config) ->
    tester:test(?MODULE, prop_read_lock, 3, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_write_lock/2 with a set read_lock, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_read_write_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version()) -> true.
prop_read_write_lock(Key, Value, Version) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:write(DB, Key, Value, Version),
    
    % write-(un)lock a read-locked key should fail
    DB3 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB2, Key), ok),
    check_entry(DB3, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, true, "check_entry_read_write_lock_1"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_write_lock_1"),
    DB4 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB3, Key), failed),
    check_entry(DB4, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, true, "check_entry_read_write_lock_2"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_write_lock_2"),
    DB5 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB4, Key), failed),
    check_entry(DB5, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, true, "check_entry_read_write_lock_3"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_write_lock_3"),
    DB6 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_read_write_lock_4"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_read_write_lock_4"),
    
    ?TEST_DB:close(DB6),
    true.

tester_read_write_lock(_Config) ->
    tester:test(?MODULE, prop_read_write_lock, 3, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_read_lock/2 in various scenarios, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_write_read_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version()) -> true.
prop_write_read_lock(Key, Value, Version) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:write(DB, Key, Value, Version),
    
    % read-(un)lock a write-locked key should fail
    DB3 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB2, Key), ok),
    check_entry(DB3, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, true, "check_entry_write_read_lock_1"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_read_lock_1"),
    DB4 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB3, Key), failed),
    check_entry(DB4, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, true, "check_entry_write_read_lock_2"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_read_lock_2"),
    DB5 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB4, Key), failed),
    check_entry(DB5, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, true, "check_entry_write_read_lock_3"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_read_lock_3"),
    DB6 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, true, "check_entry_write_read_lock_4"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_read_lock_4"),
    
    ?TEST_DB:close(DB6),
    true.

tester_write_read_lock(_Config) ->
    tester:test(?MODULE, prop_write_read_lock, 3, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:delete/2, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_delete(Key::?RT:key(), Value::?DB:value(), WriteLock::boolean(),
                  ReadLock::non_neg_integer(), Version::?DB:version(), Key2::?RT:key()) -> true.
prop_delete(Key, Value, WriteLock, ReadLock, Version, Key2) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DBEntry = create_db_entry(Key, Value, WriteLock, ReadLock, Version),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry),
    
    % delete DBEntry:
    DB3 =
        case db_entry:get_writelock(DBEntry) orelse db_entry:get_readlock(DBEntry) > 0 of
            true ->
                DBA1 = ?db_equals_pattern(?TEST_DB:delete(DB2, Key), locks_set),
                check_entry(DBA1, Key, DBEntry, {ok, Value, Version}, true, "check_entry_delete_1a"),
                case not db_entry:is_empty(DBEntry) andalso
                         not (db_entry:get_writelock(DBEntry) andalso db_entry:get_readlock(DBEntry) > 0) andalso
                         db_entry:get_version(DBEntry) >= 0 of
                    true -> check_db(DBA1, {true, []}, 1, [DBEntry], "check_db_delete_1ax");
                    _    -> check_db(DBA1, {false, [DBEntry]}, 1, [DBEntry], "check_db_delete_1ay")
                end,
                case Key =/= Key2 of
                    true ->
                        DBTmp = ?db_equals_pattern(?TEST_DB:delete(DBA1, Key2), undef),
                        check_entry(DBTmp, Key, DBEntry, {ok, Value, Version}, true, "check_entry_delete_2a"),
                        case not db_entry:is_empty(DBEntry) andalso
                                 not (db_entry:get_writelock(DBEntry) andalso db_entry:get_readlock(DBEntry) > 0) andalso
                                 db_entry:get_version(DBEntry) >= 0 of
                            true -> check_db(DBTmp, {true, []}, 1, [DBEntry], "check_db_delete_2ax");
                            _    -> check_db(DBTmp, {false, [DBEntry]}, 1, [DBEntry], "check_db_delete_2ay")
                        end,
                        DBTmp;
                    _ -> DBA1
                end;
            _ ->
                DBA1 = ?db_equals_pattern(?TEST_DB:delete(DB2, Key), ok),
                check_entry(DBA1, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_delete_1b"),
                check_db(DBA1, {true, []}, 0, [], "check_db_delete_1b"),
                DBA2 = ?db_equals_pattern(?TEST_DB:delete(DBA1, Key), undef),
                check_entry(DBA2, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_delete_2b"),
                check_db(DBA2, {true, []}, 0, [], "check_db_delete_2b"),
                DBA2
        end,
    
    ?TEST_DB:close(DB3),
    true.

tester_delete(_Config) ->
    tester:test(?MODULE, prop_delete, 6, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:add_data/2, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_add_data(Data::?TEST_DB:db_as_list()) -> true.
prop_add_data(Data) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    UniqueKeys = length(lists:usort([db_entry:get_key(DBEntry) || DBEntry <- Data])),
    
    DB2 = ?TEST_DB:add_data(DB, Data),
    check_db2(DB2, UniqueKeys, UniqueData, "check_db_add_data_1"),
    
    ?TEST_DB:close(DB2),
    true.

tester_add_data(_Config) ->
    tester:test(?MODULE, prop_add_data, 1, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:get_entries/3 emulating the former get_range_kv/2 method
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_entries3_1(Data::?TEST_DB:db_as_list(), Range::intervals:interval()) -> true.
prop_get_entries3_1(Data, Range_) ->
    Range = intervals:normalize(Range_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    DB2 = ?TEST_DB:add_data(DB, UniqueData),
    
    FilterFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                              intervals:in(db_entry:get_key(A), Range)
                end,
    ValueFun = fun(DBEntry) -> {db_entry:get_key(DBEntry),
                                db_entry:get_value(DBEntry)}
               end,
    
    ?equals_w_note(lists:sort(?TEST_DB:get_entries(DB2, FilterFun, ValueFun)),
                   lists:sort([ValueFun(A) || A <- lists:filter(FilterFun, UniqueData)]),
                   "check_get_entries3_1_1"),

    ?TEST_DB:close(DB2),
    true.

tester_get_entries3_1(_Config) ->
    tester:test(?MODULE, prop_get_entries3_1, 2, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:get_entries/3 emulating the former get_range_kvv/2 method
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_entries3_2(Data::?TEST_DB:db_as_list(), Range::intervals:interval()) -> true.
prop_get_entries3_2(Data, Range_) ->
    Range = intervals:normalize(Range_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    DB2 = ?TEST_DB:add_data(DB, UniqueData),
    
    FilterFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                              (not db_entry:get_writelock(A)) andalso
                              intervals:in(db_entry:get_key(A), Range)
                end,
    ValueFun = fun(DBEntry) -> {db_entry:get_key(DBEntry),
                                db_entry:get_value(DBEntry),
                                db_entry:get_version(DBEntry)}
               end,
    
    ?equals_w_note(lists:sort(?TEST_DB:get_entries(DB2, FilterFun, ValueFun)),
                   lists:sort([ValueFun(A) || A <- lists:filter(FilterFun, UniqueData)]),
                   "check_get_entries3_2_1"),

    ?TEST_DB:close(DB2),
    true.

tester_get_entries3_2(_Config) ->
    tester:test(?MODULE, prop_get_entries3_2, 2, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:get_entries/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_entries2(Data::?TEST_DB:db_as_list(), Range::intervals:interval()) -> true.
prop_get_entries2(Data, Range_) ->
    Range = intervals:normalize(Range_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    DB2 = ?TEST_DB:add_data(DB, UniqueData),
    
    InRangeFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                               intervals:in(db_entry:get_key(A), Range)
                 end,
    
    ?equals_w_note(lists:sort(?TEST_DB:get_entries(DB2, Range)),
                   lists:sort(lists:filter(InRangeFun, UniqueData)),
                   "check_get_entries2_1"),

    ?TEST_DB:close(DB2),
    true.

tester_get_entries2(_Config) ->
    tester:test(?MODULE, prop_get_entries2, 2, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:split_data/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_split_data(Data::?TEST_DB:db_as_list(), Range::intervals:interval()) -> true.
prop_split_data(Data, Range_) ->
    Range = intervals:normalize(Range_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    DB2 = ?TEST_DB:add_data(DB, UniqueData),
    
    InHisRangeFun = fun(A) -> (not db_entry:is_empty(A)) andalso
                                  (not intervals:in(db_entry:get_key(A), Range))
                    end,
    InMyRangeFun = fun(A) -> intervals:in(db_entry:get_key(A), Range) end,
    
    {DB3, HisList} = ?TEST_DB:split_data(DB2, Range),
    ?equals_w_note(lists:sort(HisList),
                   lists:sort(lists:filter(InHisRangeFun, UniqueData)),
                   "check_split_data_1"),
    ?equals_w_note(lists:sort(?TEST_DB:get_data(DB3)),
                   lists:sort(lists:filter(InMyRangeFun, UniqueData)),
                   "check_split_data_2"),

    ?TEST_DB:close(DB3),
    true.

tester_split_data(_Config) ->
    tester:test(?MODULE, prop_split_data, 2, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:update_entries/4
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_update_entries(Data::?TEST_DB:db_as_list(), ItemsToUpdate::pos_integer()) -> true.
prop_update_entries(Data, ItemsToUpdate) ->
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    UniqueUpdateData =
        [db_entry:inc_version(E) || E <- lists:sublist(UniqueData, ItemsToUpdate)],
    ExpUpdatedData =
        [begin
             case db_entry:get_writelock(E) orelse db_entry:get_readlock(E) =/= 0 of
                 true -> E;
                 _    ->
                     EUpd = [X || X <- UniqueUpdateData,
                                  db_entry:get_key(X) =:= db_entry:get_key(E),
                                  db_entry:get_version(X) > db_entry:get_version(E)],
                     case EUpd of
                         []  -> E;
                         [X] -> X
                     end
             end
         end || E <- UniqueData] ++
        [E || E <- UniqueUpdateData,
              not lists:any(fun(X) ->
                                    db_entry:get_key(X) =:= db_entry:get_key(E)
                            end, UniqueData)],
    
    prop_update_entries_helper(UniqueData, UniqueUpdateData, ExpUpdatedData).

-spec prop_update_entries_helper(UniqueData::?TEST_DB:db_as_list(), UniqueUpdateData::?TEST_DB:db_as_list(), ExpUpdatedData::?TEST_DB:db_as_list()) -> true.
prop_update_entries_helper(UniqueData, UniqueUpdateData, ExpUpdatedData) ->
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, UniqueData),
    
    UpdatePred = fun(OldEntry, NewEntry) ->
                         db_entry:get_version(OldEntry) < db_entry:get_version(NewEntry)
                 end,
    UpdateVal = fun(_OldEntry, NewEntry) -> NewEntry end,
    
    DB3 = ?TEST_DB:update_entries(DB2, UniqueUpdateData, UpdatePred, UpdateVal),
    
    ?equals_w_note(lists:sort(?TEST_DB:get_data(DB3)),
                   lists:sort(ExpUpdatedData),
                   "check_update_entries_1"),

    ?TEST_DB:close(DB3),
    true.

tester_update_entries(_Config) ->
    tester:test(?MODULE, prop_update_entries, 2, rw_suite_runs(1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:record_changes/2, stop_record_changes/1 and get_changes/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_changed_keys_get_entry(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_get_entry(Data, ChangesInterval_, Key) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),

    ?TEST_DB:get_entry(DB3, Key),
    ?equals_w_note(?TEST_DB:get_changes(DB3), {[], []}, "changed_keys_get_entry_1"),
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "changed_keys_get_entry_2"),
    
    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_set_entry(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Entry::db_entry:entry()) -> true.
prop_changed_keys_set_entry(Data, ChangesInterval_, Entry) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, db_entry:get_key(Entry)),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    DB4 = ?TEST_DB:set_entry(DB3, Entry),
    check_changes(DB4, ChangesInterval, "changed_keys_set_entry_1"),
    check_entry_in_changes(DB4, ChangesInterval, Entry, Old, "changed_keys_set_entry_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "changed_keys_set_entry_3"),

    ?TEST_DB:close(DB5),
    true.


-spec prop_changed_keys_update_entry(
        Data::[db_entry:entry(),...], ChangesInterval::intervals:interval(),
        UpdateVal::?TEST_DB:value()) -> true.
prop_changed_keys_update_entry(Data, ChangesInterval_, UpdateVal) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    UpdateElement = util:randomelem(UniqueData),
    Old = ?TEST_DB:get_entry2(DB2, db_entry:get_key(UpdateElement)),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    UpdatedElement = db_entry:inc_version(db_entry:set_value(UpdateElement, UpdateVal)),
    DB4 = ?TEST_DB:update_entry(DB3, UpdatedElement),
    check_changes(DB4, ChangesInterval, "changed_update_entry_1"),
    check_entry_in_changes(DB4, ChangesInterval, UpdatedElement, Old, "changed_update_entry_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "changed_update_entry_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_delete_entry(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Entry::db_entry:entry()) -> true.
prop_changed_keys_delete_entry(Data, ChangesInterval_, Entry) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, db_entry:get_key(Entry)),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    DB4 = ?TEST_DB:delete_entry(DB3, Entry),
    check_changes(DB4, ChangesInterval, "delete_entry_1"),
    check_key_in_deleted(DB4, ChangesInterval, db_entry:get_key(Entry), Old, "delete_entry_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "delete_entry_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_read(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_read(Data, ChangesInterval_, Key) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    ?TEST_DB:read(DB3, Key),
    ?equals_w_note(?TEST_DB:get_changes(DB3), {[], []}, "changed_keys_read_1"),
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "changed_keys_read_2"),

    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_write(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version()) -> true.
prop_changed_keys_write(Data, ChangesInterval_, Key, Value, Version) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, Key),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    DB4 = ?TEST_DB:write(DB3, Key, Value, Version),
    check_changes(DB4, ChangesInterval, "changed_keys_write_1"),
    ChangedEntry = ?TEST_DB:get_entry(DB4, Key),
    check_entry_in_changes(DB4, ChangesInterval, ChangedEntry, Old, "changed_keys_write_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "changed_keys_write_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_delete(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_delete(Data, ChangesInterval_, Key) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, Key),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    {DB4, _Status} = ?TEST_DB:delete(DB3, Key),
    check_changes(DB4, ChangesInterval, "delete_1"),
    check_key_in_deleted(DB4, ChangesInterval, Key, Old, "delete_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "delete_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_set_write_lock(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_set_write_lock(Data, ChangesInterval_, Key) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, Key),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    {DB4, _Status} = ?TEST_DB:set_write_lock(DB3, Key),
    NewEntry = ?TEST_DB:get_entry(DB4, Key),
    check_changes(DB4, ChangesInterval, "set_write_lock_1"),
    check_entry_in_changes(DB4, ChangesInterval, NewEntry, Old, "set_write_lock_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "set_write_lock_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_unset_write_lock(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_unset_write_lock(Data, ChangesInterval_, Key) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, Key),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    {DB4, _Status} = ?TEST_DB:unset_write_lock(DB3, Key),
    NewEntry = ?TEST_DB:get_entry(DB4, Key),
    check_changes(DB4, ChangesInterval, "unset_write_lock_1"),
    check_entry_in_changes(DB4, ChangesInterval, NewEntry, Old, "unset_write_lock_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "unset_write_lock_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_set_read_lock(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_set_read_lock(Data, ChangesInterval_, Key) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, Key),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    {DB4, _Status} = ?TEST_DB:set_read_lock(DB3, Key),
    NewEntry = ?TEST_DB:get_entry(DB4, Key),
    check_changes(DB4, ChangesInterval, "set_read_lock_1"),
    check_entry_in_changes(DB4, ChangesInterval, NewEntry, Old, "set_read_lock_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "set_read_lock_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_unset_read_lock(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Key::?RT:key()) -> true.
prop_changed_keys_unset_read_lock(Data, ChangesInterval_, Key) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old = ?TEST_DB:get_entry2(DB2, Key),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    {DB4, _Status} = ?TEST_DB:unset_read_lock(DB3, Key),
    NewEntry = ?TEST_DB:get_entry(DB4, Key),
    check_changes(DB4, ChangesInterval, "unset_read_lock_1"),
    check_entry_in_changes(DB4, ChangesInterval, NewEntry, Old, "unset_read_lock_2"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "unset_read_lock_3"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_get_entries2(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Interval::intervals:interval()) -> true.
prop_changed_keys_get_entries2(Data, ChangesInterval_, Interval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    Interval = intervals:normalize(Interval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    ?TEST_DB:get_entries(DB3, Interval),
    ?equals_w_note(?TEST_DB:get_changes(DB3), {[], []}, "changed_keys_get_entries2_1"),
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "changed_keys_get_entries2_2"),

    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_get_entries4(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Interval::intervals:interval()) -> true.
prop_changed_keys_get_entries4(Data, ChangesInterval_, Interval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    Interval = intervals:normalize(Interval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    FilterFun = fun(E) -> (not db_entry:is_empty(E)) andalso
                              (not intervals:in(db_entry:get_key(E), Interval))
                end,
    ValueFun = fun(E) -> db_entry:get_key(E) end,
    
    ?TEST_DB:get_entries(DB3, FilterFun, ValueFun),
    ?equals_w_note(?TEST_DB:get_changes(DB3), {[], []}, "changed_keys_get_entries4_1"),
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "changed_keys_get_entries4_2"),

    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_update_entries(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval(),
        Entry1::db_entry:entry(), Entry2::db_entry:entry()) -> true.
prop_changed_keys_update_entries(Data, ChangesInterval_, Entry1, Entry2) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    Old1 = ?TEST_DB:get_entry2(DB2, db_entry:get_key(Entry1)),
    Old2 = ?TEST_DB:get_entry2(DB2, db_entry:get_key(Entry2)),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    UpdatePred = fun(OldEntry, NewEntry) ->
                         db_entry:get_version(OldEntry) < db_entry:get_version(NewEntry)
                 end,
    UpdateVal = fun(_OldEntry, NewEntry) -> NewEntry end,

    DB4 = ?TEST_DB:update_entries(DB3, [Entry1, Entry2], UpdatePred, UpdateVal),
    NewEntry1 = ?TEST_DB:get_entry(DB4, db_entry:get_key(Entry1)),
    NewEntry2 = ?TEST_DB:get_entry(DB4, db_entry:get_key(Entry2)),
    check_changes(DB4, ChangesInterval, "update_entries_1"),
    check_entry_in_changes(DB4, ChangesInterval, NewEntry1, Old1, "update_entries_2"),
    check_entry_in_changes(DB4, ChangesInterval, NewEntry2, Old2, "update_entries_3"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "changed_keys_get_load_2"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_get_load(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_get_load(Data, ChangesInterval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    ?TEST_DB:get_load(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB3), {[], []}, "changed_keys_get_load_1"),
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "changed_keys_get_load_2"),

    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_split_data1(
        Data::?TEST_DB:db_as_list(),
        ChangesInterval::intervals:interval(),
        MyNewInterval1::intervals:interval()) -> true.
prop_changed_keys_split_data1(Data, ChangesInterval_, MyNewInterval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    MyNewInterval = intervals:normalize(MyNewInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),

    {DB4, _HisList} = ?TEST_DB:split_data(DB3, MyNewInterval),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "split_data1_1"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "split_data1_2"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_split_data2(
        Data::?TEST_DB:db_as_list(),
        ChangesInterval::intervals:interval(),
        MyNewInterval1::intervals:interval()) -> true.
prop_changed_keys_split_data2(Data, ChangesInterval_, MyNewInterval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    MyNewInterval = intervals:normalize(MyNewInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:record_changes(DB, ChangesInterval),
    DB3 = ?TEST_DB:add_data(DB2, Data),

    {DB4, _HisList} = ?TEST_DB:split_data(DB3, MyNewInterval),
    
    check_changes(DB4, intervals:intersection(ChangesInterval, MyNewInterval), "split_data2_1"),
    
    DB5 = ?TEST_DB:stop_record_changes(DB4),
    ?equals_w_note(?TEST_DB:get_changes(DB5), {[], []}, "split_data2_2"),

    ?TEST_DB:close(DB5),
    true.

-spec prop_changed_keys_get_data(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_get_data(Data, ChangesInterval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    ?TEST_DB:get_data(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB3), {[], []}, "changed_keys_get_data_1"),
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "changed_keys_get_data_2"),

    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_add_data(
        Data::?TEST_DB:db_as_list(),
        ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_add_data(Data, ChangesInterval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:record_changes(DB, ChangesInterval),
    
    DB3 = ?TEST_DB:add_data(DB2, Data),
    check_changes(DB3, ChangesInterval, "add_data_1"),

    % lists:usort removes all but first occurrence of equal elements
    % -> reverse list since ?TEST_DB:add_data will keep the last element
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, lists:reverse(Data)),
    [check_entry_in_changes(DB3, ChangesInterval, E, {false, db_entry:new(db_entry:get_key(E))}, "add_data_2")
        || E <- UniqueData],
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "add_data_3"),

    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_check_db(
        Data::?TEST_DB:db_as_list(), ChangesInterval::intervals:interval()) -> true.
prop_changed_keys_check_db(Data, ChangesInterval_) ->
    ChangesInterval = intervals:normalize(ChangesInterval_),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    DB3 = ?TEST_DB:record_changes(DB2, ChangesInterval),
    
    ?TEST_DB:check_db(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB3), {[], []}, "changed_keys_check_db_1"),
    
    DB4 = ?TEST_DB:stop_record_changes(DB3),
    ?equals_w_note(?TEST_DB:get_changes(DB4), {[], []}, "changed_keys_check_db_2"),

    ?TEST_DB:close(DB4),
    true.

-spec prop_changed_keys_mult_interval(
        Data::?TEST_DB:db_as_list(), Entry1::db_entry:entry(),
        Entry2::db_entry:entry(), Entry3::db_entry:entry(),
        Entry4::db_entry:entry()) -> true.
prop_changed_keys_mult_interval(Data, Entry1, Entry2, Entry3, Entry4) ->
    CI1 = intervals:union(intervals:new(db_entry:get_key(Entry1)),
                          intervals:new(db_entry:get_key(Entry2))),
    CI2 = intervals:union(intervals:new(db_entry:get_key(Entry3)),
                          intervals:new(db_entry:get_key(Entry4))),
    CI1_2 = intervals:union(CI1, CI2),
    DB = ?TEST_DB:new(?RT:hash_key(1)),
    DB2 = ?TEST_DB:add_data(DB, Data),
    
    DB3 = ?TEST_DB:record_changes(DB2, CI1),
    Old1 = ?TEST_DB:get_entry2(DB3, db_entry:get_key(Entry1)),
    DB4 = ?TEST_DB:set_entry(DB3, Entry1),
    check_changes(DB4, CI1, "changed_keys_mult_interval_1"),
    check_entry_in_changes(DB4, CI1, Entry1, Old1, "changed_keys_mult_interval_2"),
    
    DB5 = ?TEST_DB:record_changes(DB4, CI2),
    Old2 = ?TEST_DB:get_entry2(DB5, db_entry:get_key(Entry2)),
    DB6 = ?TEST_DB:set_entry(DB5, Entry2),
    check_changes(DB6, CI1_2, "changed_keys_mult_interval_3"),
    check_entry_in_changes(DB6, CI1_2, Entry2, Old2, "changed_keys_mult_interval_4"),
    
    DB7 = ?TEST_DB:record_changes(DB6, CI2),
    Old3 = ?TEST_DB:get_entry2(DB7, db_entry:get_key(Entry3)),
    DB8 = ?TEST_DB:set_entry(DB7, Entry3),
    check_changes(DB8, CI1_2, "changed_keys_mult_interval_5"),
    check_entry_in_changes(DB8, CI1_2, Entry3, Old3, "changed_keys_mult_interval_6"),
    
    DB9 = ?TEST_DB:record_changes(DB8, CI2),
    Old4 = ?TEST_DB:get_entry2(DB9, db_entry:get_key(Entry4)),
    DB10 = ?TEST_DB:set_entry(DB9, Entry4),
    check_changes(DB10, CI1_2, "changed_keys_mult_interval_7"),
    check_entry_in_changes(DB10, CI1_2, Entry4, Old4, "changed_keys_mult_interval_8"),
    
    DB11 = ?TEST_DB:stop_record_changes(DB10),
    ?equals_w_note(?TEST_DB:get_changes(DB11), {[], []}, "changed_keys_mult_interval_9"),

    ?TEST_DB:close(DB11),
    true.

%% @doc Checks that all entries returned by ?TEST_DB:get_changes/1 are in the
%%      given interval.
-spec check_changes(DB::?TEST_DB:db(), ChangesInterval::intervals:interval(), Note::string()) -> true.
check_changes(DB, ChangesInterval, Note) ->
    {ChangedEntries1, DeletedKeys1} = ?TEST_DB:get_changes(DB),
    (lists:all(fun(E) -> intervals:in(db_entry:get_key(E), ChangesInterval) end,
               ChangedEntries1) orelse
         ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                  ["element(1, ?TEST_DB:get_changes(DB))", ChangedEntries1,
                   ChangesInterval, lists:flatten(Note)])) andalso
    (lists:all(fun(E) -> intervals:in(E, ChangesInterval) end, DeletedKeys1) orelse
         ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                  ["element(2, ?TEST_DB:get_changes(DB))", DeletedKeys1,
                   ChangesInterval, lists:flatten(Note)])) andalso
    check_changes2(DB, ChangesInterval, ChangesInterval, Note) andalso
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

%% @doc Checks that all entries returned by ?TEST_DB:get_changes/2 are in the
%%      given interval GetChangesInterval and also ChangesInterval.
-spec check_changes2(DB::?TEST_DB:db(), ChangesInterval::intervals:interval(), GetChangesInterval::intervals:interval(), Note::string()) -> true.
check_changes2(DB, ChangesInterval, GetChangesInterval, Note) ->
    {ChangedEntries2, DeletedKeys2} = ?TEST_DB:get_changes(DB, GetChangesInterval),
    FinalInterval = intervals:intersection(ChangesInterval, GetChangesInterval),
    (lists:all(fun(E) -> intervals:in(db_entry:get_key(E), FinalInterval) end,
               ChangedEntries2) orelse
         ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                  ["element(1, ?TEST_DB:get_changes(DB, FinalInterval))",
                   ChangedEntries2, FinalInterval, lists:flatten(Note)])) andalso
    (lists:all(fun(E) -> intervals:in(E, FinalInterval) end, DeletedKeys2) orelse
         ?ct_fail("~s evaluated to \"~w\" and contains elements not in ~w~n(~s)~n",
                  ["element(2, ?TEST_DB:get_changes(DB, FinalInterval))",
                   DeletedKeys2, FinalInterval, lists:flatten(Note)])).

%% @doc Checks that a key is present exactly once in the list of deleted
%%      keys returned by ?TEST_DB:get_changes/1.
-spec check_key_in_deleted(
        DB::?TEST_DB:db(), ChangesInterval::intervals:interval(), Key::?RT:key(),
        {OldExists::boolean(), OldEntry::db_entry:entry()}, Note::string()) -> true.
check_key_in_deleted(DB, ChangesInterval, Key, {OldExists, OldEntry}, Note) ->
    case intervals:in(Key, ChangesInterval) andalso OldExists andalso
             (db_entry:get_writelock(OldEntry) =:= false andalso
                  db_entry:get_readlock(OldEntry) =:= 0) of
        true ->
            {_ChangedEntries, DeletedKeys} = ?TEST_DB:get_changes(DB),
            length([K || K <- DeletedKeys, K =:= Key]) =:= 1 orelse
                ?ct_fail("~s evaluated to \"~w\" and did not contain 1x key ~w~n(~s)~n",
                  ["element(2, ?TEST_DB:get_changes(DB))", DeletedKeys,
                   Key, lists:flatten(Note)]);
        _    -> true
    end.

%% @doc Checks that an entry is present exactly once in the list of changed
%%      entries returned by ?TEST_DB:get_changes/1.
-spec check_entry_in_changes(
        DB::?TEST_DB:db(), ChangesInterval::intervals:interval(), Entry::db_entry:entry(),
        {OldExists::boolean(), OldEntry::db_entry:entry()}, Note::string()) -> any().
check_entry_in_changes(DB, ChangesInterval, NewEntry, {_OldExists, OldEntry}, Note) ->
    case intervals:in(db_entry:get_key(NewEntry), ChangesInterval) andalso 
             OldEntry =/= NewEntry of
        true ->
            {ChangedEntries, _DeletedKeys} = ?TEST_DB:get_changes(DB),
            length([E || E <- ChangedEntries, E =:= NewEntry]) =:= 1  orelse
                ?ct_fail("~s evaluated to \"~w\" and did not contain 1x entry ~w~n(~s)~n",
                  ["element(1, ?TEST_DB:get_changes(DB))", ChangedEntries,
                   NewEntry, lists:flatten(Note)]);
        _    -> ok
    end.

tester_changed_keys_get_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_entry, 3, rw_suite_runs(1000)).

tester_changed_keys_set_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_set_entry, 3, rw_suite_runs(1000)).

tester_changed_keys_update_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_update_entry, 3, rw_suite_runs(1000)).

tester_changed_keys_delete_entry(_Config) ->
    tester:test(?MODULE, prop_changed_keys_delete_entry, 3, rw_suite_runs(1000)).

tester_changed_keys_read(_Config) ->
    tester:test(?MODULE, prop_changed_keys_read, 3, rw_suite_runs(1000)).

tester_changed_keys_write(_Config) ->
    tester:test(?MODULE, prop_changed_keys_write, 5, rw_suite_runs(1000)).

tester_changed_keys_delete(_Config) ->
    tester:test(?MODULE, prop_changed_keys_delete, 3, rw_suite_runs(1000)).

tester_changed_keys_set_write_lock(_Config) ->
    tester:test(?MODULE, prop_changed_keys_set_write_lock, 3, rw_suite_runs(1000)).

tester_changed_keys_unset_write_lock(_Config) ->
    tester:test(?MODULE, prop_changed_keys_unset_write_lock, 3, rw_suite_runs(1000)).

tester_changed_keys_set_read_lock(_Config) ->
    tester:test(?MODULE, prop_changed_keys_set_read_lock, 3, rw_suite_runs(1000)).

tester_changed_keys_unset_read_lock(_Config) ->
    tester:test(?MODULE, prop_changed_keys_unset_read_lock, 3, rw_suite_runs(1000)).

tester_changed_keys_get_entries2(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_entries2, 3, rw_suite_runs(1000)).

tester_changed_keys_get_entries4(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_entries4, 3, rw_suite_runs(1000)).

tester_changed_keys_update_entries(_Config) ->
    tester:test(?MODULE, prop_changed_keys_update_entries, 4, rw_suite_runs(1000)).

tester_changed_keys_get_load(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_load, 2, rw_suite_runs(1000)).

tester_changed_keys_split_data1(_Config) ->
    tester:test(?MODULE, prop_changed_keys_split_data1, 3, rw_suite_runs(1000)).

tester_changed_keys_split_data2(_Config) ->
    tester:test(?MODULE, prop_changed_keys_split_data2, 3, rw_suite_runs(1000)).

tester_changed_keys_get_data(_Config) ->
    tester:test(?MODULE, prop_changed_keys_get_data, 2, rw_suite_runs(1000)).

tester_changed_keys_add_data(_Config) ->
    tester:test(?MODULE, prop_changed_keys_add_data, 2, rw_suite_runs(1000)).

tester_changed_keys_check_db(_Config) ->
    tester:test(?MODULE, prop_changed_keys_check_db, 2, rw_suite_runs(1000)).

tester_changed_keys_mult_interval(_Config) ->
    tester:test(?MODULE, prop_changed_keys_mult_interval, 5, rw_suite_runs(1000)).


% helper functions:

-spec check_entry(DB::?TEST_DB:db(), Key::?RT:key(), ExpDBEntry::db_entry:entry(),
                  ExpRead::{ok, Value::?TEST_DB:value(), Version::?TEST_DB:version()} | {ok, empty_val, -1},
                  ExpExists::boolean(), Note::string()) -> true.
check_entry(DB, Key, ExpDBEntry, ExpRead, ExpExists, Note) ->
    ?equals_w_note(?TEST_DB:get_entry2(DB, Key), {ExpExists, ExpDBEntry}, Note),
    ?equals_w_note(?TEST_DB:get_entry(DB, Key), ExpDBEntry, Note),
    ?equals_w_note(?TEST_DB:read(DB, Key), ExpRead, Note),
    true.

-spec create_db_entry(Key::?RT:key(), Value::?DB:value(), WriteLock::boolean(),
                      ReadLock::non_neg_integer(), Version::?DB:version() | -1) -> db_entry:entry().
create_db_entry(Key, Value, WriteLock, ReadLock, Version) ->
    E1 = db_entry:new(Key, Value, Version),
    E2 = case WriteLock of
             true -> db_entry:set_writelock(E1);
             _    -> E1
         end,
    _E3 = case ReadLock of
              0 -> E2;
              _ -> lists:foldl(fun(_, ETmp) -> db_entry:inc_readlock(ETmp) end, E2, lists:seq(1, ReadLock))
          end.

-spec check_db(DB::?TEST_DB:db(),
               ExpCheckDB::{true, []} | {false, InvalidEntries::?TEST_DB:db_as_list()},
               ExpLoad::integer(),
               ExpData::?TEST_DB:db_as_list(), Note::string()) -> true.
check_db(DB, ExpCheckDB, ExpLoad, ExpData, Note) ->
    check_db(DB, ExpCheckDB, ExpLoad, ExpData, {[], []}, Note).

-spec check_db(DB::?TEST_DB:db(),
               ExpCheckDB::{true, []} | {false, InvalidEntries::?TEST_DB:db_as_list()},
               ExpLoad::integer(),
               ExpData::?TEST_DB:db_as_list(),
               ExpCKData::{UpdatedEntries::?TEST_DB:db_as_list(), DeletedKeys::[?RT:key()]},
               Note::string()) -> true.
check_db(DB, ExpCheckDB, ExpLoad, ExpData, ExpCKData, Note) ->
    ?equals_w_note(?TEST_DB:check_db(DB), ExpCheckDB, Note),
    ?equals_w_note(?TEST_DB:get_load(DB), ExpLoad, Note),
    ?equals_w_note(lists:sort(?TEST_DB:get_data(DB)), lists:sort(ExpData), Note),
    ?equals_w_note(?TEST_DB:get_changes(DB), ExpCKData, Note),
    true.

%% @doc Like check_db/5 but do not check DB using ?TEST_DB:check_db.
-spec check_db2(DB::?TEST_DB:db(), ExpLoad::integer(),
               ExpData::?TEST_DB:db_as_list(), Note::string()) -> true.
check_db2(DB, ExpLoad, ExpData, Note) ->
    check_db2(DB, ExpLoad, ExpData, {[], []}, Note).

%% @doc Like check_db/5 but do not check DB using ?TEST_DB:check_db.
-spec check_db2(DB::?TEST_DB:db(), ExpLoad::integer(),
                ExpData::?TEST_DB:db_as_list(),
                ExpCKData::{UpdatedEntries::?TEST_DB:db_as_list(), DeletedKeys::[?RT:key()]},
                Note::string()) -> true.
check_db2(DB, ExpLoad, ExpData, ExpCKData, Note) ->
    ?equals_w_note(?TEST_DB:get_load(DB), ExpLoad, Note),
    ?equals_w_note(lists:sort(?TEST_DB:get_data(DB)), lists:sort(ExpData), Note),
    ?equals_w_note(?TEST_DB:get_changes(DB), ExpCKData, Note),
    true.
