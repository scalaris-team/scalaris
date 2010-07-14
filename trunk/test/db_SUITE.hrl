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
    [read,
     write,
     write_lock,
     read_lock,
     read_write_lock,
     write_read_lock,
     delete,
     get_version,
     get_load_and_middle
    ].

suite() ->
    [
     {timetrap, {seconds, 5}}
    ].

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
        db_tcerl ->
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
    ?equals(?TEST_DB:get_middle_key(DB4), failed),
    DB5 = ?TEST_DB:write(DB4, "Key3", "Value3", 1),
    DB6 = ?TEST_DB:write(DB5, "Key4", "Value4", 1),
    ?equals(?TEST_DB:get_middle_key(DB6), {ok, "Key2"}),
    OrigFullList = ?TEST_DB:get_data(DB6),
    {DB7, HisList} = ?TEST_DB:split_data(DB6, intervals:mk_from_node_ids("Key2", "Key4")),
    ?equals(?TEST_DB:read(DB7, "Key3"), {ok, "Value3", 1}),
    ?equals(?TEST_DB:read(DB7, "Key4"), {ok, "Value4", 1}),
    ?equals(?TEST_DB:get_load(DB7), 2),
    ?equals(length(HisList), 2),
    ?equals(length(?TEST_DB:get_data(DB7)), 2),
    DB8 = ?TEST_DB:add_data(DB7, HisList),
    ?equals(OrigFullList, ?TEST_DB:get_data(DB8)),
    ?TEST_DB:close(DB8).
