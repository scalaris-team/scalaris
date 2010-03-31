%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    database.hrl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Common types and function specs for database implementations.
%%% @end
%%% Created : 31 Mar 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-type(key()::integer() | string()).
-type(value()::any()).
-type(version()::pos_integer()).

-spec new(NodeId::?RT:key()) -> db().

-spec close(DB::db()) -> any().

-spec set_write_lock(DB::db(), Key::key()) ->
         {NewDB::db(), Status::ok | failed}.

-spec unset_write_lock(DB::db(), Key::key()) ->
         {NewDB::db(), Status::ok | failed}.

-spec set_read_lock(DB::db(), Key::key()) ->
         {NewDB::db(), Status::ok | failed}.

-spec unset_read_lock(DB::db(), Key::key()) ->
         {NewDB::db(), Status::ok | failed}.

-spec get_locks(DB::db(), Key::key()) ->
         {DB::db(), {WriteLock::boolean(), ReadLock::integer(), Version::version()} | failed}.

-spec read(DB::db(), Key::key()) ->
         {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.

-spec write(DB::db(), Key::key(), Value::value(), Version::version()) ->
         NewDB::db().

-spec get_version(DB::db(), Key::key()) ->
         {ok, Version::version()} | failed.

-spec delete(DB::db(), Key::key()) ->
         {NewDB::db(), Status::ok | locks_set | undef}.

-spec get_load(DB::db()) -> Load::integer().

-spec get_middle_key(DB::db()) -> {ok, MiddleKey::key()} | failed.

-spec split_data(DB::db(), MyKey::key(), HisKey::key()) ->
         {NewDB::db(), [{Key::key(), {Value::value(), WriteLock::boolean(), ReadLock::integer(), Version::version()}}]}.

-spec get_data(DB::db()) ->
         [{Key::key(), {Value::value(), WriteLock::boolean(), ReadLock::integer(), Version::version()}}].

-spec add_data(DB::db(), [{Key::key(), {Value::value(), WriteLock::boolean(), ReadLock::integer(), Version::version()}}]) ->
         NewDB::db().

-spec get_range_with_version(DB::db(), Range::intervals:interval()) ->
         [{Key::key(), Value::value(), WriteLock::boolean(), ReadLock::integer(), Version::version()}].

-spec get_range_only_with_version(DB::db(), Range::intervals:interval()) ->
         [{Key::key(), Value::value(), Version::version()}].

-spec build_merkle_tree(DB::db(), Range::intervals:interval()) ->
         MerkerlTree::merkerl:tree().
