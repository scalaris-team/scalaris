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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Common types and function specs for database implementations.
%% @end
%% @version $Id$

-type(value() :: any()).
-type(version() :: non_neg_integer()).
-type(kv_list() :: [{Key::?RT:key(), Value::value()}]).
-type(kvv_list() :: [{Key::?RT:key(), Value::value(), Version::version()}]).
-type(db_as_list() :: [db_entry:entry()]).

-ifdef(with_export_type_support).
-export_type([db/0, value/0, version/0, kv_list/0, kvv_list/0,
              db_as_list/0]).
-endif.

-export([new/1, close/1]).
-export([get_entry/2, set_entry/2, update_entry/2, delete_entry/2]).
-export([read/2, write/4, get_version/2]).
-export([delete/2]).
-export([set_write_lock/2, unset_write_lock/2,
         set_read_lock/2, unset_read_lock/2, get_locks/2]).
-export([get_range_kv/2, get_range_kvv/2, get_range_entry/2]).
-export([get_load/1, split_data/2, get_data/1, add_data/2]).
-export([update_if_newer/2]).
-export([check_db/1]).
-export([record_changes/2, stop_record_changes/1, get_changes/1]).

-spec new(NodeId::?RT:key()) -> db().
-spec close(DB::db()) -> any().

-spec get_entry(DB::db(), Key::?RT:key()) -> db_entry:entry().
-spec set_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
-spec update_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
-spec delete_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().

-spec read(DB::db(), Key::?RT:key()) ->
         {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.
-spec write(DB::db(), Key::?RT:key(), Value::value(), Version::version()) ->
         NewDB::db().
-spec get_version(DB::db(), Key::?RT:key()) ->
         {ok, Version::version() | -1} | failed.

-spec delete(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | locks_set | undef}.

-spec set_write_lock(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | failed}.
-spec unset_write_lock(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | failed}.
-spec set_read_lock(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | failed}.
-spec unset_read_lock(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | failed}.
-spec get_locks(DB::db(), Key::?RT:key()) ->
         {DB::db(), {WriteLock::boolean(), ReadLock::non_neg_integer(), Version::version()} | {true, 0, -1} | failed}.

-spec get_range_kv(DB::db(), Range::intervals:interval()) -> kv_list().
-spec get_range_kvv(DB::db(), Range::intervals:interval()) -> kvv_list().
-spec get_range_entry(DB::db(), Range::intervals:interval()) -> db_as_list().

-spec get_load(DB::db()) -> Load::integer().
-spec split_data(DB::db(), MyNewInterval::intervals:interval()) ->
         {NewDB::db(), db_as_list()}.
-spec get_data(DB::db()) -> db_as_list().
-spec add_data(DB::db(), db_as_list()) -> NewDB::db().

-spec update_if_newer(OldDB::db(), KVs::kvv_list()) -> NewDB::db().

-spec record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
-spec stop_record_changes(OldDB::db()) -> NewDB::db().
-spec get_changes(DB::db()) -> {db_as_list(), [?RT:key()]}.

-spec check_db(DB::db()) -> {true, []} | {false, InvalidEntries::db_as_list()}.
