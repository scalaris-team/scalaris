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
    ?assert(cs_db_otp:read("Unknown") == failed),
    ok.

write(_Config) ->
    erlang:put(instance_id, test_instance_id),
    ?assert(cs_db_otp:write("Key1", "Value1", 1) == ok),
    ?assert(cs_db_otp:read("Key1") == {ok, "Value1", 1}),
    ok.

write_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    ?assert(cs_db_otp:set_write_lock("WriteLockKey1") == ok),
    ?assert(cs_db_otp:set_write_lock("WriteLockKey2") == ok),
    ?assert(cs_db_otp:set_write_lock("WriteLockKey2") == failed),
    ?assert(cs_db_otp:unset_write_lock("WriteLockKey2") == ok),
    ok.

read_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    ?assert(cs_db_otp:set_read_lock("ReadLockKey1") == failed),
    ?assert(cs_db_otp:write("ReadLockKey2", "Value1", 1) == ok),
    ?assert(cs_db_otp:set_read_lock("ReadLockKey2") == ok),
    ?assert(cs_db_otp:set_read_lock("ReadLockKey2") == ok),
    ?assert(cs_db_otp:unset_read_lock("ReadLockKey2") == ok),
    ok.

read_write_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    ?assert(cs_db_otp:write("ReadWriteLockKey1", "Value1", 1) == ok),
    ?assert(cs_db_otp:set_read_lock("ReadWriteLockKey1") == ok),
    ?assert(cs_db_otp:set_write_lock("ReadWriteLockKey1") == failed),
    ok.

write_read_lock(_Config) ->
    erlang:put(instance_id, test_instance_id),
    ?assert(cs_db_otp:write("WriteReadLockKey1", "Value1", 1) == ok),
    ?assert(cs_db_otp:set_write_lock("WriteReadLockKey1") == ok),
    ?assert(cs_db_otp:set_read_lock("WriteReadLockKey1") == failed),
    ok.

