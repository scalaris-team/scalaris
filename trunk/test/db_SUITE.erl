%  Copyright 2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : db_gb_trees_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/db_gb_trees.erl
%%%
%%% Created :  19 Dec 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(db_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("../include/scalaris.hrl").

-import(?DB).

-include("unittest.hrl").

all() ->
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
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    application:start(log4erl),
    crypto:start(),
    ct:pal("DB suite running with: ~p~n", [?DB]),
    file:set_cwd("../bin"),
    case ?DB of
	cs_db_otp ->
	    Pid = spawn(fun () ->
				process_dictionary:start_link_for_unittest(),
				?DB:start_link("db_SUITE.erl"),
				timer:sleep(30000)
			end),
	    timer:sleep(100),
	    [{wrapper_pid, Pid} | Config];
	db_gb_trees ->
	    Config;
	db_ets ->
	    Config;
        db_tcerl ->
	    Pid = spawn(fun () ->
                                config:start_link(["scalaris.cfg"]),
				timer:sleep(10000)
			end),
            tcerl:start(),
	    timer:sleep(100),
	    [{wrapper_pid, Pid} | Config]
    end.

end_per_suite(_Config) ->
    crypto:stop(),
    ok.

read(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    ?assert(?DB:read(DB, ?RT:hash_key("Unknown")) == failed),
    ?DB:close(DB),
    ok.

write(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    DB2 = ?DB:write(DB, ?RT:hash_key("Key1"), "Value1", 1),
    ?assert(?DB:read(DB2, ?RT:hash_key("Key1")) == {ok, "Value1", 1}),
    ?DB:close(DB),
    ok.

write_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    % lock on a key
    {DB2, ok}     = ?DB:set_write_lock(DB, ?RT:hash_key("WriteLockKey1")),
    {DB3, ok}     = ?DB:set_write_lock(DB2, ?RT:hash_key("WriteLockKey2")),
    % lock on locked key should fail
    {DB4, failed} = ?DB:set_write_lock(DB3, ?RT:hash_key("WriteLockKey2")),
    % unlock key
    {DB5, ok}     = ?DB:unset_write_lock(DB4, ?RT:hash_key("WriteLockKey2")),
    % lockable again?
    {DB6, ok}     = ?DB:set_write_lock(DB5, ?RT:hash_key("WriteLockKey2")),
    {DB7, {true,0,_Version}} = ?DB:get_locks(DB6, ?RT:hash_key("WriteLockKey2")),
    % unlock to finish
    {DB8, ok}     = ?DB:unset_write_lock(DB7, ?RT:hash_key("WriteLockKey2")),
    % unlock non existing key
    {_DB9, failed}     = ?DB:unset_write_lock(DB8, ?RT:hash_key("WriteLockKey2")),
    ?DB:close(DB),
    ok.

read_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    % read lock on new key should fail
    {DB2, failed} = ?DB:set_read_lock(DB, ?RT:hash_key("ReadLockKey1")),
    {DB2b, failed} = ?DB:unset_read_lock(DB2, ?RT:hash_key("ReadLockKey1")),
    DB3           = ?DB:write(DB2b, ?RT:hash_key("ReadLockKey2"), "Value1", 1),
    % read lock on existing key
    {DB4, ok}     = ?DB:set_read_lock(DB3, ?RT:hash_key("ReadLockKey2")),
    {DB5, ok}     = ?DB:set_read_lock(DB4, ?RT:hash_key("ReadLockKey2")),
    % read unlock on existing key
    {DB6, ok}     = ?DB:unset_read_lock(DB5, ?RT:hash_key("ReadLockKey2")),
    {DB7, ok}     = ?DB:unset_read_lock(DB6, ?RT:hash_key("ReadLockKey2")),
    % read unlock on non read locked key
    {DB8, failed}     = ?DB:unset_read_lock(DB7, ?RT:hash_key("ReadLockKey2")),
    {DB9, {false,0,1}} = ?DB:get_locks(DB8, ?RT:hash_key("ReadLockKey2")),
    {DB10, failed} = ?DB:get_locks(DB9, ?RT:hash_key("Unknown")),
    % read on write locked new key should fail
    {DB11, ok}     = ?DB:set_write_lock(DB10, ?RT:hash_key("ReadLockKey1")),
    failed     = ?DB:read(DB11, ?RT:hash_key("ReadLockKey1")),
    ?DB:close(DB),
    ok.

read_write_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(2),
    DB2 = ?DB:write(DB, ?RT:hash_key("ReadWriteLockKey1"), "Value1", 1),
    1 = ?DB:get_load(DB2),
    {DB3, ok}     = ?DB:set_read_lock(DB2, ?RT:hash_key("ReadWriteLockKey1")),
    % no write lock, when read locks exist
    {_DB4, failed} = ?DB:set_write_lock(DB3, ?RT:hash_key("ReadWriteLockKey1")),
    ?DB:close(DB),
    ok.

write_read_lock(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    DB2 = ?DB:write(DB, ?RT:hash_key("WriteReadLockKey1"), "Value1", 1),
    {DB3, ok} = ?DB:set_write_lock(DB2, ?RT:hash_key("WriteReadLockKey1")),
    % no read lock, when a write lock exists
    {_DB4, failed} = ?DB:set_read_lock(DB3, ?RT:hash_key("WriteReadLockKey1")),
    ?DB:close(DB),
    ok.

delete(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    DB2 = ?DB:write(DB, ?RT:hash_key("Key1"), "Value1", 1),
    {DB3, ok} = ?DB:delete(DB2, ?RT:hash_key("Key1")),
    ?assert(?DB:read(DB3, ?RT:hash_key("Key1")) == failed),
    {DB5, undef} = ?DB:delete(DB3, ?RT:hash_key("Key1")),
    DB6 = ?DB:write(DB5, ?RT:hash_key("Key1"), "Value1", 1),
    {DB7, ok} = ?DB:set_read_lock(DB6, ?RT:hash_key("Key1")),
    {_DB8, locks_set} = ?DB:delete(DB7, ?RT:hash_key("Key1")),
    ?DB:close(DB),
    ok.

get_version(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    failed = ?DB:get_version(DB, ?RT:hash_key("Key1")),
    DB2 = ?DB:write(DB, ?RT:hash_key("Key1"), "Value1", 10),
    {ok, 10} = ?DB:get_version(DB2, ?RT:hash_key("Key1")),
    {DB3, ok} = ?DB:set_write_lock(DB2, ?RT:hash_key("Key2")),
    {ok, -1} = ?DB:get_version(DB3, ?RT:hash_key("Key2")),
    ?DB:close(DB),
    ok.

get_load_and_middle(_Config) ->
    erlang:put(instance_id, "db_SUITE.erl"),
    DB = ?DB:new(1),
    ?assert(?DB:get_load(DB) == 0),
    DB2 = ?DB:write(DB, "Key1", "Value1", 1),
    ?assert(?DB:get_load(DB2) == 1),
    DB3 = ?DB:write(DB2, "Key1", "Value1", 2),
    ?assert(?DB:get_load(DB3) == 1),
    DB4 = ?DB:write(DB3, "Key2", "Value2", 1),
    ?assert(?DB:get_load(DB4) == 2),
    ?assert(?DB:get_middle_key(DB4) == failed),
    DB5 = ?DB:write(DB4, "Key3", "Value3", 1),
    DB6 = ?DB:write(DB5, "Key4", "Value4", 1),
    ?assert(?DB:get_middle_key(DB6) == {ok, "Key2"}),
    OrigFullList = ?DB:get_data(DB6),
    {DB7, HisList} = ?DB:split_data(DB6, "Key4", "Key2"),
    ?assert(?DB:read(DB7, "Key3") == {ok, "Value3", 1}),
    ?assert(?DB:read(DB7, "Key4") == {ok, "Value4", 1}),
    ?assert(?DB:get_load(DB7) == 2),
    ?assert(length(HisList) == 2),
    ?assert(length(?DB:get_data(DB7)) == 2),
    DB8 = ?DB:add_data(DB7, HisList),
    ?assert(OrigFullList == ?DB:get_data(DB8)),
    ?DB:close(DB).


