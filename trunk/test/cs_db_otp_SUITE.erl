%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : cs_db_otp_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/cs_db_otp.erl
%%%
%%% Created :  20 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(cs_db_otp_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [read, write, write_lock, read_lock, read_write_lock, write_read_lock].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    process_dictionary:start(),
    _Srv = cs_db_otp:start(test_instance_id),
    Config.

end_per_suite(_Config) ->
    cs_db_otp:stop(),
    process_dictionary:stop(),
    ok.

read(_Config) ->
    erlang:put(instance_id, test_instance_id),
    DB = cs_db_otp:new(1),
    ?assert(cs_db_otp:read(DB, "Unknown") == failed),
    ok.

write(_Config) ->
    erlang:put(instance_id, test_instance_id),
    DB = cs_db_otp:new(1),
    DB2 = cs_db_otp:write(DB, "Key1", "Value1", 1),
    ?assert(cs_db_otp:read(DB2, "Key1") == {ok, "Value1", 1}),
    ok.

write_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    DB = cs_db_otp:new(1),
    {DB2, ok}     = cs_db_otp:set_write_lock(DB, "WriteLockKey1"),
    {DB3, ok}     = cs_db_otp:set_write_lock(DB2, "WriteLockKey2"),
    {DB4, failed} = cs_db_otp:set_write_lock(DB3, "WriteLockKey2"),
    {_DB5, ok}     = cs_db_otp:unset_write_lock(DB4, "WriteLockKey2"),
    ok.

read_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    DB = cs_db_otp:new(1),
    {DB2, failed} = cs_db_otp:set_read_lock(DB, "ReadLockKey1"),
    DB3           = cs_db_otp:write(DB2, "ReadLockKey2", "Value1", 1),
    {DB4, ok}     = cs_db_otp:set_read_lock(DB3, "ReadLockKey2"),
    {DB5, ok}     = cs_db_otp:set_read_lock(DB4, "ReadLockKey2"),
    {_DB6, ok}     = cs_db_otp:unset_read_lock(DB5, "ReadLockKey2"),
    ok.

read_write_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    DB = cs_db_otp:new(1),
    DB2           = cs_db_otp:write(DB, "ReadWriteLockKey1", "Value1", 1),
    {DB3, ok}     = cs_db_otp:set_read_lock(DB2, "ReadWriteLockKey1"),
    {_DB4, failed} = cs_db_otp:set_write_lock(DB3, "ReadWriteLockKey1"),
    ok.

write_read_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    DB = cs_db_otp:new(1),
    DB2           = cs_db_otp:write(DB, "WriteReadLockKey1", "Value1", 1),
    {DB3, ok}     = cs_db_otp:set_write_lock(DB2, "WriteReadLockKey1"),
    {_DB4, failed} = cs_db_otp:set_read_lock(DB3, "WriteReadLockKey1"),
    ok.

