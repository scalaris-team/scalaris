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
-type(kvv_list() :: [{Key::?RT:key(), Value::value(), Version::version()}]).
-type(db_as_list() :: [db_entry:entry()]).

-ifdef(with_export_type_support).
-export_type([db/0, value/0, version/0, kvv_list/0,
              db_as_list/0]).
-endif.

-export([new/1, close/1]).
-export([get_entry/2, get_entry2/2, set_entry/2, update_entry/2, delete_entry/2]).
-export([read/2, write/4, delete/2]).
-export([set_write_lock/2, unset_write_lock/2,
         set_read_lock/2, unset_read_lock/2]).
-export([get_entries/2, get_entries/3]).
-export([update_entries/4]).
-export([delete_entries/2]).
-export([get_load/1, split_data/2, get_data/1, add_data/2]).
-export([check_db/1]).
-export([record_changes/2, stop_record_changes/1, stop_record_changes/2,
         get_changes/1, get_changes/2]).

-spec new(NodeId::?RT:key()) -> db().
-spec close(DB::db()) -> any().

-spec get_entry(DB::db(), Key::?RT:key()) -> db_entry:entry().
-spec get_entry2(DB::db(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.
-spec set_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
-spec update_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
-spec delete_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().

-spec read(DB::db(), Key::?RT:key()) ->
         {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.
-spec write(DB::db(), Key::?RT:key(), Value::value(), Version::version()) ->
         NewDB::db().

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

-spec get_entries(DB::db(), Range::intervals:interval()) -> db_as_list().
-spec get_entries(DB::db(),
                  FilterFun::fun((DBEntry::db_entry:entry()) -> boolean()),
                  ValueFun::fun((DBEntry::db_entry:entry()) -> Value))
        -> [Value].
-spec update_entries(DB::db(), Values::[db_entry:entry()],
                     Pred::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> boolean()),
                     UpdateFun::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> UpdatedEntry::db_entry:entry()))
        -> NewDB::db().
-spec delete_entries(DB::db(),
                     RangeOrFun::intervals:interval() |
                                 fun((DBEntry::db_entry:entry()) -> boolean()))
        -> NewDB::db().

-spec get_load(DB::db()) -> Load::integer().
-spec split_data(DB::db(), MyNewInterval::intervals:interval()) ->
         {NewDB::db(), db_as_list()}.
-spec get_data(DB::db()) -> db_as_list().
-spec add_data(DB::db(), db_as_list()) -> NewDB::db().

-spec record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
-spec stop_record_changes(OldDB::db()) -> NewDB::db().
-spec stop_record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
-spec get_changes(DB::db()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
-spec get_changes(DB::db(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.

-spec check_db(DB::db()) -> {true, []} | {false, InvalidEntries::db_as_list()}.
