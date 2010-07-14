%  Copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
-module(db_gb_trees_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [read, write, write_lock, read_lock, read_write_lock, write_read_lock].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

read(_Config) ->
    DB = db_gb_trees:new(1),
    ?equals(db_gb_trees:read(DB, "Unknown"), {ok, empty_val, -1}),
    ok.

write(_Config) ->
    DB = db_gb_trees:new(1),
    DB2 = db_gb_trees:write(DB, "Key1", "Value1", 1),
    ?equals(db_gb_trees:read(DB2, "Key1"), {ok, "Value1", 1}),
    ok.

write_lock(_Config) ->
    DB = db_gb_trees:new(1),
    {DB2, ok}     = db_gb_trees:set_write_lock(DB, "WriteLockKey1"),
    {DB3, ok}     = db_gb_trees:set_write_lock(DB2, "WriteLockKey2"),
    {DB4, failed} = db_gb_trees:set_write_lock(DB3, "WriteLockKey2"),
    {_DB5, ok}     = db_gb_trees:unset_write_lock(DB4, "WriteLockKey2"),
    ok.

read_lock(_Config) ->
    DB = db_gb_trees:new(1),
    {DB2, failed} = db_gb_trees:set_read_lock(DB, "ReadLockKey1"),
    DB3           = db_gb_trees:write(DB2, "ReadLockKey2", "Value1", 1),
    {DB4, ok}     = db_gb_trees:set_read_lock(DB3, "ReadLockKey2"),
    {DB5, ok}     = db_gb_trees:set_read_lock(DB4, "ReadLockKey2"),
    {_DB6, ok}     = db_gb_trees:unset_read_lock(DB5, "ReadLockKey2"),
    ok.

read_write_lock(_Config) ->
    DB = db_gb_trees:new(1),
    DB2           = db_gb_trees:write(DB, "ReadWriteLockKey1", "Value1", 1),
    {DB3, ok}     = db_gb_trees:set_read_lock(DB2, "ReadWriteLockKey1"),
    {_DB4, failed} = db_gb_trees:set_write_lock(DB3, "ReadWriteLockKey1"),
    ok.

write_read_lock(_Config) ->
    DB = db_gb_trees:new(1),
    DB2           = db_gb_trees:write(DB, "WriteReadLockKey1", "Value1", 1),
    {DB3, ok}     = db_gb_trees:set_write_lock(DB2, "WriteReadLockKey1"),
    {_DB4, failed} = db_gb_trees:set_read_lock(DB3, "WriteReadLockKey1"),
    ok.
