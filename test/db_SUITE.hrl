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
     delete, get_version, get_load_and_middle,
     % random tester functions:
     tester_new, tester_set_entry, tester_update_entry,
     tester_delete_entry1, tester_delete_entry2,
     tester_write, tester_write_lock, tester_read_lock,
     tester_read_write_lock, tester_write_read_lock,
     tester_delete, tester_add_data
    ].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

-spec spawn_config_processes() -> pid().
spawn_config_processes() ->
    Owner = self(),
    Pid =
        spawn(fun () ->
                       crypto:start(),
                       process_dictionary:start_link(),
                       config:start_link(["scalaris.cfg", "scalaris.local.cfg"]),
                       Owner ! {continue},
                       receive {done} -> ok
                       end
              end),
    receive {continue} -> ok
    end,
    Pid.

init_per_suite(Config) ->
    application:start(log4erl),
    crypto:start(),
    ct:pal("DB suite running with: ~p~n", [?TEST_DB]),
    file:set_cwd("../bin"),
    case ?TEST_DB of
        db_gb_trees ->
            Config;
        db_ets ->
            Config;
        db_toke ->
            Pid = spawn_config_processes(),
            timer:sleep(100),
            [{wrapper_pid, Pid} | Config];
        db_tcerl ->
            Pid = spawn_config_processes(),
            try db_tcerl:start_per_vm()
            catch
                error:Reason ->
                    gen_component:kill(process_dictionary),
                    exit(Pid, kill),
                    erlang:error(Reason)
            end,
            timer:sleep(100),
            [{wrapper_pid, Pid} | Config]
    end.

end_per_suite(Config) ->
    case lists:keyfind(wrapper_pid, 1, Config) of
        false ->
            ok;
        {wrapper_pid, Pid} ->
            gen_component:kill(process_dictionary),
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
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
    ?equals(?TEST_DB:read(DB, ?RT:hash_key("Unknown")), {ok, empty_val, -1}),
    ?TEST_DB:close(DB),
    ok.

write(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:write(DB, ?RT:hash_key("Key1"), "Value1", 1),
    ?equals(?TEST_DB:read(DB2, ?RT:hash_key("Key1")), {ok, "Value1", 1}),
    ?TEST_DB:close(DB2),
    ok.

write_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
    % lock on a key
    DB2 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB, ?RT:hash_key("WriteLockKey1")), ok),
    DB3 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB2, ?RT:hash_key("WriteLockKey2")), ok),
    % lock on locked key should fail
    DB4 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB3, ?RT:hash_key("WriteLockKey2")), failed),
    % unlock key
    DB5 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB4, ?RT:hash_key("WriteLockKey2")), ok),
    % lockable again?
    DB6 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB5, ?RT:hash_key("WriteLockKey2")), ok),
    DB7 = ?db_equals_pattern(?TEST_DB:get_locks(DB6, ?RT:hash_key("WriteLockKey2")), {true, 0, _Version}),
    % unlock to finish
    DB8 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB7, ?RT:hash_key("WriteLockKey2")), ok),
    % unlock non existing key
    DB9 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB8, ?RT:hash_key("WriteLockKey2")), failed),
    ?TEST_DB:close(DB9),
    ok.

read_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
    % read lock on new key should fail
    DB2 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB, ?RT:hash_key("ReadLockKey1")), failed),
    DB2b = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB2, ?RT:hash_key("ReadLockKey1")), failed),
    DB3           = ?TEST_DB:write(DB2b, ?RT:hash_key("ReadLockKey2"), "Value1", 1),
    % read lock on existing key
    DB4 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB3, ?RT:hash_key("ReadLockKey2")), ok),
    DB5 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB4, ?RT:hash_key("ReadLockKey2")), ok),
    % read unlock on existing key
    DB6 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB5, ?RT:hash_key("ReadLockKey2")), ok),
    DB7 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB6, ?RT:hash_key("ReadLockKey2")), ok),
    % read unlock on non read locked key
    DB8 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB7, ?RT:hash_key("ReadLockKey2")), failed),
    DB9 = ?db_equals_pattern(?TEST_DB:get_locks(DB8, ?RT:hash_key("ReadLockKey2")), {false,0,1}),
    DB10 = ?db_equals_pattern(?TEST_DB:get_locks(DB9, ?RT:hash_key("Unknown")), failed),
    % read on write locked new key should fail
    DB11 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB10, ?RT:hash_key("ReadLockKey1")), ok),
    %% never set a value, but DB Entry was created
    ?equals(?TEST_DB:read(DB11, ?RT:hash_key("ReadLockKey1")), {ok, empty_val, -1}),
    ?TEST_DB:close(DB11),
    ok.

read_write_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(2),
    DB2 = ?TEST_DB:write(DB, ?RT:hash_key("ReadWriteLockKey1"), "Value1", 1),
    ?equals(?TEST_DB:get_load(DB2), 1),
    DB3 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB2, ?RT:hash_key("ReadWriteLockKey1")), ok),
    % no write lock, when read locks exist
    DB4 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB3, ?RT:hash_key("ReadWriteLockKey1")), failed),
    ?TEST_DB:close(DB4),
    ok.

write_read_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:write(DB, ?RT:hash_key("WriteReadLockKey1"), "Value1", 1),
    DB3 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB2, ?RT:hash_key("WriteReadLockKey1")), ok),
    % no read lock, when a write lock exists
    DB4 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB3, ?RT:hash_key("WriteReadLockKey1")), failed),
    ?TEST_DB:close(DB4),
    ok.

delete(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:write(DB, ?RT:hash_key("Key1"), "Value1", 1),
    DB3 = ?db_equals_pattern(?TEST_DB:delete(DB2, ?RT:hash_key("Key1")), ok),
    ?equals(?TEST_DB:read(DB3, ?RT:hash_key("Key1")), {ok, empty_val, -1}),
    DB5 = ?db_equals_pattern(?TEST_DB:delete(DB3, ?RT:hash_key("Key1")), undef),
    DB6 = ?TEST_DB:write(DB5, ?RT:hash_key("Key1"), "Value1", 1),
    DB7 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB6, ?RT:hash_key("Key1")), ok),
    DB8 = ?db_equals_pattern(?TEST_DB:delete(DB7, ?RT:hash_key("Key1")), locks_set),
    ?TEST_DB:close(DB8),
    ok.

get_version(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
    ?equals(?TEST_DB:get_version(DB, ?RT:hash_key("Key1")), failed),
    DB2 = ?TEST_DB:write(DB, ?RT:hash_key("Key1"), "Value1", 10),
    ?equals(?TEST_DB:get_version(DB2, ?RT:hash_key("Key1")), {ok, 10}),
    DB3 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB2, ?RT:hash_key("Key2")), ok),
    ?equals(?TEST_DB:get_version(DB3, ?RT:hash_key("Key2")), {ok, -1}),
    ?TEST_DB:close(DB3),
    ok.

get_load_and_middle(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?TEST_DB:new(1),
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
    {DB7, HisList} = ?TEST_DB:split_data(DB6, intervals:mk_from_node_ids("Key2", "Key4")),
    ?equals(?TEST_DB:read(DB7, "Key3"), {ok, "Value3", 1}),
    ?equals(?TEST_DB:read(DB7, "Key4"), {ok, "Value4", 1}),
    ?equals(?TEST_DB:get_load(DB7), 2),
    ?equals(length(HisList), 2),
    ?equals(length(?TEST_DB:get_data(DB7)), 2),
    DB8 = ?TEST_DB:add_data(DB7, HisList),
    % lists could be in arbitrary order -> sort them
    ?equals(lists:sort(OrigFullList), lists:sort(?TEST_DB:get_data(DB8))),
    ?TEST_DB:close(DB8).

% tester-based functions below:

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:new/1, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_new(NodeId::?RT:key(), Key::?RT:key()) -> true).
prop_new(NodeId, Key) ->
    DB = ?TEST_DB:new(NodeId),
    check_db(DB, {true, []}, 0, [], "check_db_new_1"),
    ?equals(?TEST_DB:read(DB, Key), {ok, empty_val, -1}),
    check_entry(DB, Key, db_entry:new(Key), {ok, empty_val, -1}, failed, failed, "check_entry_new_1"),
    ?TEST_DB:close(DB),
    true.

tester_new(_Config) ->
    tester:test(?MODULE, prop_new, 2, 10).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:set_entry/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_set_entry(DBEntry::db_entry:entry()) -> true).
prop_set_entry(DBEntry) ->
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry),
    check_entry(DB2, db_entry:get_key(DBEntry), DBEntry,
                {ok, db_entry:get_value(DBEntry), db_entry:get_version(DBEntry)},
                {ok, db_entry:get_version(DBEntry)},
                {db_entry:get_writelock(DBEntry), db_entry:get_readlock(DBEntry), db_entry:get_version(DBEntry)},
                "check_entry_set_entry_1"),
    case not db_entry:is_empty(DBEntry) andalso
             not (db_entry:get_writelock(DBEntry) andalso db_entry:get_readlock(DBEntry) > 0) andalso
             db_entry:get_version(DBEntry) >= 0 of
        true -> check_db(DB2, {true, []}, 1, [DBEntry], "check_db_set_entry_1");
        _    -> check_db(DB2, {false, [DBEntry]}, 1, [DBEntry], "check_db_set_entry_2")
    end,
    ?TEST_DB:close(DB2),
    true.

tester_set_entry(_Config) ->
    tester:test(?MODULE, prop_set_entry, 1, 10).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:update_entry/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_update_entry(DBEntry1::db_entry:entry(), Value2::?DB:value(), WriteLock2::boolean(),
                        ReadLock2::non_neg_integer(), Version2::?DB:version()) -> true).
prop_update_entry(DBEntry1, Value2, WriteLock2, ReadLock2, Version2) ->
    DBEntry2 = create_db_entry(db_entry:get_key(DBEntry1), Value2, WriteLock2, ReadLock2, Version2),
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry1),
    DB3 = ?TEST_DB:update_entry(DB2, DBEntry2),
    check_entry(DB3, db_entry:get_key(DBEntry2), DBEntry2,
                {ok, db_entry:get_value(DBEntry2), db_entry:get_version(DBEntry2)},
                {ok, db_entry:get_version(DBEntry2)},
                {db_entry:get_writelock(DBEntry2), db_entry:get_readlock(DBEntry2), db_entry:get_version(DBEntry2)},
                "check_entry_set_entry_1"),
    case not db_entry:is_empty(DBEntry2) andalso
             not (db_entry:get_writelock(DBEntry2) andalso db_entry:get_readlock(DBEntry2) > 0) andalso
             db_entry:get_version(DBEntry2) >= 0 of
        true -> check_db(DB3, {true, []}, 1, [DBEntry2], "check_db_set_entry_1");
        _    -> check_db(DB3, {false, [DBEntry2]}, 1, [DBEntry2], "check_db_set_entry_2")
    end,
    ?TEST_DB:close(DB3),
    true.

tester_update_entry(_Config) ->
    tester:test(?MODULE, prop_update_entry, 5, 10).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:delete_entry/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_delete_entry1(DBEntry1::db_entry:entry()) -> true).
prop_delete_entry1(DBEntry1) ->
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry1),
    DB3 = ?TEST_DB:delete_entry(DB2, DBEntry1),
    check_entry(DB3, db_entry:get_key(DBEntry1), db_entry:new(db_entry:get_key(DBEntry1)),
                {ok, empty_val, -1}, failed, failed, "check_entry_delete_entry1_1"),
    check_db(DB3, {true, []}, 0, [], "check_db_delete_entry1_1"),
    ?TEST_DB:close(DB3),
    true.

-spec(prop_delete_entry2(DBEntry1::db_entry:entry(), DBEntry2::db_entry:entry()) -> true).
prop_delete_entry2(DBEntry1, DBEntry2) ->
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry1),
    % note: DBEntry2 may not be the same
    DB3 = ?TEST_DB:delete_entry(DB2, DBEntry2),
    case db_entry:get_key(DBEntry1) =/= db_entry:get_key(DBEntry2) of
        true ->
            check_entry(DB3, db_entry:get_key(DBEntry1), DBEntry1,
                {ok, db_entry:get_value(DBEntry1), db_entry:get_version(DBEntry1)},
                {ok, db_entry:get_version(DBEntry1)},
                {db_entry:get_writelock(DBEntry1), db_entry:get_readlock(DBEntry1), db_entry:get_version(DBEntry1)},
                "check_entry_delete_entry2_1"),
            case not db_entry:is_empty(DBEntry1) andalso
                     not (db_entry:get_writelock(DBEntry1) andalso db_entry:get_readlock(DBEntry1) > 0) andalso
                     db_entry:get_version(DBEntry1) >= 0 of
                true -> check_db(DB3, {true, []}, 1, [DBEntry1], "check_db_delete_entry2_1a");
                _    -> check_db(DB3, {false, [DBEntry1]}, 1, [DBEntry1], "check_db_delete_entry2_1b")
            end;
        _    ->
            check_entry(DB3, db_entry:get_key(DBEntry1), db_entry:new(db_entry:get_key(DBEntry1)),
                        {ok, empty_val, -1}, failed, failed, "check_entry_delete_entry2_2"),
            check_db(DB3, {true, []}, 0, [], "check_db_delete_entry2_2")
    end,
    ?TEST_DB:close(DB3),
    true.

tester_delete_entry1(_Config) ->
    tester:test(?MODULE, prop_delete_entry1, 1, 10).

tester_delete_entry2(_Config) ->
    tester:test(?MODULE, prop_delete_entry2, 2, 10).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:write/2, ?TEST_DB getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_write(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version(), Key2::?RT:key()) -> true).
prop_write(Key, Value, Version, Key2) ->
    DBEntry = db_entry:new(Key, Value, Version),
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:write(DB, Key, Value, Version),
    check_entry(DB2, Key, DBEntry, {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_1"),
    check_db(DB2, {true, []}, 1, [DBEntry], "check_db_write_1"),
    case Key =/= Key2 of
        true -> check_entry(DB2, Key2, db_entry:new(Key2), {ok, empty_val, -1}, failed, failed, "write_2");
        _    -> ok
    end,
    ?TEST_DB:close(DB2),
    true.

tester_write(_Config) ->
    tester:test(?MODULE, prop_write, 4, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_write_lock/2 in various scenarios, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_write_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version(), Key2::?RT:key()) -> true).
prop_write_lock(Key, Value, Version, Key2) ->
    DB = ?TEST_DB:new(1),
    
    % unlocking non-existing keys should fail and leave the DB in its previous state:
    DB1 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB, Key), failed),
    check_entry(DB1, Key, db_entry:new(Key), {ok, empty_val, -1}, failed, failed, "check_entry_write_lock_0"),
    check_db(DB1, {true, []}, 0, [], "check_db_write_lock_0"),
    
    % lock on existing entry:
    DB2 = ?TEST_DB:write(DB, Key, Value, Version),
    check_entry(DB2, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_lock_1"),
    check_db(DB2, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_1"),
    DB3 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB2, Key), ok),
    check_entry(DB3, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, {ok, Version}, {true, 0, Version}, "check_entry_write_lock_2"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_lock_2"),
    % lock on locked key should fail
    DB4 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB3, Key), failed),
    check_entry(DB4, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, {ok, Version}, {true, 0, Version}, "check_entry_write_lock_3"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_lock_3"),
    % unlock key
    DB5 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB4, Key), ok),
    check_entry(DB5, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_lock_4"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_4"),
    % lockable again?
    DB6 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, {ok, Version}, {true, 0, Version}, "check_entry_write_lock_5"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_lock_5"),
    % unlock to finish
    DB7 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB6, Key), ok),
    check_entry(DB7, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_lock_6"),
    check_db(DB7, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_6"),
    % unlock unlocked key should fail
    DB8 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB7, Key), failed),
    check_entry(DB8, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_lock_7"),
    check_db(DB8, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_7"),

    % lock on non-existing key:
    FinalDB =
        case Key =/= Key2 of
            true ->
                LastEntry = create_db_entry(Key, Value, false, 0, Version),
                DB9 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB8, Key2), ok),
                check_entry(DB9, Key2, create_db_entry(Key2, empty_val, true, 0, -1), {ok, empty_val, -1}, {ok, -1}, {true, 0, -1}, "check_entry_write_lock_8"),
                check_db(DB9, {false, [create_db_entry(Key2, empty_val, true, 0, -1)]}, 2, [LastEntry, create_db_entry(Key2, empty_val, true, 0, -1)], "check_db_write_lock_8"),
                % lock on locked key should fail
                DB10 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB9, Key2), failed),
                check_entry(DB10, Key2, create_db_entry(Key2, empty_val, true, 0, -1), {ok, empty_val, -1}, {ok, -1}, {true, 0, -1}, "check_entry_write_lock_9"),
                check_db(DB10, {false, [create_db_entry(Key2, empty_val, true, 0, -1)]}, 2, [LastEntry, create_db_entry(Key2, empty_val, true, 0, -1)], "check_db_write_lock_9"),
                % unlock key should remove the empty_val
                DB11 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB10, Key2), ok),
                check_entry(DB11, Key2, db_entry:new(Key2), {ok, empty_val, -1}, failed, failed, "check_entry_write_lock_10"),
                check_db(DB11, {true, []}, 1, [LastEntry], "check_db_write_lock_10"),
                % lockable again?
                DB12 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB11, Key2), ok),
                check_entry(DB12, Key2, create_db_entry(Key2, empty_val, true, 0, -1), {ok, empty_val, -1}, {ok, -1}, {true, 0, -1}, "check_entry_write_lock_11"),
                check_db(DB12, {false, [create_db_entry(Key2, empty_val, true, 0, -1)]}, 2, [LastEntry, create_db_entry(Key2, empty_val, true, 0, -1)], "check_db_write_lock_11"),
                % unlock to finish (should remove the empty_val)
                DB13 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB12, Key2), ok),
                check_entry(DB11, Key2, db_entry:new(Key2), {ok, empty_val, -1}, failed, failed, "check_entry_write_lock_12"),
                check_db(DB11, {true, []}, 1, [LastEntry], "check_db_write_lock_12"),
                % unlock unlocked key should fail
                DB14 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB13, Key2), failed),
                check_entry(DB13, Key2, db_entry:new(Key2), {ok, empty_val, -1}, failed, failed, "check_entry_write_lock_13"),
                check_db(DB13, {true, []}, 1, [LastEntry], "check_db_write_lock_13"),
                DB14;
            _ -> DB8
        end,
    ?TEST_DB:close(FinalDB),
    true.

tester_write_lock(_Config) ->
    tester:test(?MODULE, prop_write_lock, 4, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_read_lock/2 in various scenarios, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_read_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version()) -> true).
prop_read_lock(Key, Value, Version) ->
    DB = ?TEST_DB:new(1),
    
    % (un)locking non-existing keys should fail and leave the DB in its previous state:
    DB1 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB, Key), failed),
    check_entry(DB1, Key, db_entry:new(Key), {ok, empty_val, -1}, failed, failed, "check_entry_read_lock_1"),
    check_db(DB1, {true, []}, 0, [], "check_db_read_lock_1"),
    DB2 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB1, Key), failed),
    check_entry(DB2, Key, db_entry:new(Key), {ok, empty_val, -1}, failed, failed, "check_entry_read_lock_2"),
    check_db(DB2, {true, []}, 0, [], "check_db_read_lock_2"),
    
    % lock on existing entry:
    DB3 = ?TEST_DB:write(DB, Key, Value, Version),
    check_entry(DB3, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_lock_3"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_3"),
    % read lock on existing key
    DB4 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB3, Key), ok),
    check_entry(DB4, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, {ok, Version}, {false, 1, Version}, "check_entry_write_lock_4"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_write_lock_4"),
    DB5 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB4, Key), ok),
    check_entry(DB5, Key, create_db_entry(Key, Value, false, 2, Version), {ok, Value, Version}, {ok, Version}, {false, 2, Version}, "check_entry_write_lock_5"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, false, 2, Version)], "check_db_write_lock_5"),
    % read unlock on existing key
    DB6 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, {ok, Version}, {false, 1, Version}, "check_entry_write_lock_6"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_write_lock_6"),
    % unlockable again?
    DB7 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB6, Key), ok),
    check_entry(DB7, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_lock_7"),
    check_db(DB7, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_7"),
    % read unlock on non-read-locked key
    DB8 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB7, Key), failed),
    check_entry(DB8, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_lock_8"),
    check_db(DB8, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_lock_8"),
    
    ?TEST_DB:close(DB8),
    true.

tester_read_lock(_Config) ->
    tester:test(?MODULE, prop_read_lock, 3, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_write_lock/2 with a set read_lock, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_read_write_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version()) -> true).
prop_read_write_lock(Key, Value, Version) ->
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:write(DB, Key, Value, Version),
    
    % write-(un)lock a read-locked key should fail
    DB3 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB2, Key), ok),
    check_entry(DB3, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, {ok, Version}, {false, 1, Version}, "check_entry_read_write_lock_1"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_write_lock_1"),
    DB4 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB3, Key), failed),
    check_entry(DB4, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, {ok, Version}, {false, 1, Version}, "check_entry_read_write_lock_2"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_write_lock_2"),
    DB5 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB4, Key), failed),
    check_entry(DB5, Key, create_db_entry(Key, Value, false, 1, Version), {ok, Value, Version}, {ok, Version}, {false, 1, Version}, "check_entry_read_write_lock_3"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, false, 1, Version)], "check_db_read_write_lock_3"),
    DB6 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_read_write_lock_4"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_read_write_lock_4"),
    
    ?TEST_DB:close(DB6),
    true.

tester_read_write_lock(_Config) ->
    tester:test(?MODULE, prop_read_write_lock, 3, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:(un)set_read_lock/2 in various scenarios, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_write_read_lock(Key::?RT:key(), Value::?TEST_DB:value(), Version::?TEST_DB:version()) -> true).
prop_write_read_lock(Key, Value, Version) ->
    DB = ?TEST_DB:new(1),
    DB2 = ?TEST_DB:write(DB, Key, Value, Version),
    
    % read-(un)lock a write-locked key should fail
    DB3 = ?db_equals_pattern(?TEST_DB:set_write_lock(DB2, Key), ok),
    check_entry(DB3, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, {ok, Version}, {true, 0, Version}, "check_entry_write_read_lock_1"),
    check_db(DB3, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_read_lock_1"),
    DB4 = ?db_equals_pattern(?TEST_DB:set_read_lock(DB3, Key), failed),
    check_entry(DB4, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, {ok, Version}, {true, 0, Version}, "check_entry_write_read_lock_2"),
    check_db(DB4, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_read_lock_2"),
    DB5 = ?db_equals_pattern(?TEST_DB:unset_read_lock(DB4, Key), failed),
    check_entry(DB5, Key, create_db_entry(Key, Value, true, 0, Version), {ok, Value, Version}, {ok, Version}, {true, 0, Version}, "check_entry_write_read_lock_3"),
    check_db(DB5, {true, []}, 1, [create_db_entry(Key, Value, true, 0, Version)], "check_db_write_read_lock_3"),
    DB6 = ?db_equals_pattern(?TEST_DB:unset_write_lock(DB5, Key), ok),
    check_entry(DB6, Key, create_db_entry(Key, Value, false, 0, Version), {ok, Value, Version}, {ok, Version}, {false, 0, Version}, "check_entry_write_read_lock_4"),
    check_db(DB6, {true, []}, 1, [create_db_entry(Key, Value, false, 0, Version)], "check_db_write_read_lock_4"),
    
    ?TEST_DB:close(DB6),
    true.

tester_write_read_lock(_Config) ->
    tester:test(?MODULE, prop_write_read_lock, 3, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:delete/2, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_delete(Key::?RT:key(), Value::?DB:value(), WriteLock::boolean(),
                  ReadLock::non_neg_integer(), Version::?DB:version(), Key2::?RT:key()) -> true).
prop_delete(Key, Value, WriteLock, ReadLock, Version, Key2) ->
    DB = ?TEST_DB:new(1),
    DBEntry = create_db_entry(Key, Value, WriteLock, ReadLock, Version),
    DB2 = ?TEST_DB:set_entry(DB, DBEntry),
    
    % delete DBEntry:
    DB3 =
        case db_entry:get_writelock(DBEntry) orelse db_entry:get_readlock(DBEntry) > 0 of
            true ->
                DBA1 = ?db_equals_pattern(?TEST_DB:delete(DB2, Key), locks_set),
                check_entry(DBA1, Key, DBEntry, {ok, Value, Version}, {ok, Version}, {WriteLock, ReadLock, Version}, "check_entry_delete_1a"),
                case not db_entry:is_empty(DBEntry) andalso
                         not (db_entry:get_writelock(DBEntry) andalso db_entry:get_readlock(DBEntry) > 0) andalso
                         db_entry:get_version(DBEntry) >= 0 of
                    true -> check_db(DBA1, {true, []}, 1, [DBEntry], "check_db_delete_1ax");
                    _    -> check_db(DBA1, {false, [DBEntry]}, 1, [DBEntry], "check_db_delete_1ay")
                end,
                case Key =/= Key2 of
                    true ->
                        DBTmp = ?db_equals_pattern(?TEST_DB:delete(DBA1, Key2), undef),
                        check_entry(DBTmp, Key, DBEntry, {ok, Value, Version}, {ok, Version}, {WriteLock, ReadLock, Version}, "check_entry_delete_2a"),
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
                check_entry(DBA1, Key, db_entry:new(Key), {ok, empty_val, -1}, failed, failed, "check_entry_delete_1b"),
                check_db(DBA1, {true, []}, 0, [], "check_db_delete_1b"),
                DBA2 = ?db_equals_pattern(?TEST_DB:delete(DBA1, Key), undef),
                check_entry(DBA2, Key, db_entry:new(Key), {ok, empty_val, -1}, failed, failed, "check_entry_delete_2b"),
                check_db(DBA2, {true, []}, 0, [], "check_db_delete_2b"),
                DBA2
        end,
    
    ?TEST_DB:close(DB3),
    true.

tester_delete(_Config) ->
    tester:test(?MODULE, prop_delete, 6, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ?TEST_DB:add_data/2, also validate using different getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_add_data(Data::?TEST_DB:db_as_list()) -> true).
prop_add_data(Data) ->
    DB = ?TEST_DB:new(1),
    
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
    tester:test(?MODULE, prop_add_data, 1, 100).

%TODO: ?TEST_DB:split_data
%TODO: ?TEST_DB:get_range*
%TODO: ?TEST_DB:update_if_newer

% helper functions:

-spec check_entry(DB::?TEST_DB:db(), Key::?RT:key(), ExpDBEntry::db_entry:entry(),
                  ExpRead::{ok, Value::?TEST_DB:value(), Version::?TEST_DB:version()} | {ok, empty_val, -1},
                  ExpVersion::{ok, Version::?TEST_DB:version() | -1} | failed,
                  ExpLocks::{WriteLock::boolean(), ReadLock::non_neg_integer(), Version::?TEST_DB:version()} | {true, 0, -1} | failed,
                  Note::string()) -> true.
check_entry(DB, Key, ExpDBEntry, ExpRead, ExpVersion, ExpLocks, Note) ->
    ?equals_w_note(?TEST_DB:get_entry(DB, Key), ExpDBEntry, Note),
    ?equals_w_note(?TEST_DB:read(DB, Key), ExpRead, Note),
    ?equals_w_note(?TEST_DB:get_version(DB, Key), ExpVersion, Note),
    ?equals_w_note(?TEST_DB:get_locks(DB, Key), {DB, ExpLocks}, Note),
    true.

-spec create_db_entry(Key::?RT:key(), Value::?DB:value(), WriteLock::boolean(),
                      ReadLock::non_neg_integer(), Version::?DB:version()) -> db_entry:entry().
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
    ?equals_w_note(?TEST_DB:check_db(DB), ExpCheckDB, Note),
    ?equals_w_note(?TEST_DB:get_load(DB), ExpLoad, Note),
    ?equals_w_note(lists:sort(?TEST_DB:get_data(DB)), lists:sort(ExpData), Note),
    true.

%% @doc Like check_db/5 but do not check DB using ?TEST_DB:check_db.
-spec check_db2(DB::?TEST_DB:db(), ExpLoad::integer(),
               ExpData::?TEST_DB:db_as_list(), Note::string()) -> true.
check_db2(DB, ExpLoad, ExpData, Note) ->
    ?equals_w_note(?TEST_DB:get_load(DB), ExpLoad, Note),
    ?equals_w_note(lists:sort(?TEST_DB:get_data(DB)), lists:sort(ExpData), Note),
    true.
