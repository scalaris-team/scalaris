% @copyright 2010-2011 Zuse Institute Berlin

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

-type value() :: atom() | boolean() | number() | binary().
-type version() :: non_neg_integer().
-type kvv_list() :: [{Key::?RT:key(), Value::value(), Version::version()}].
-type db_as_list() :: [db_entry:entry()].
-type db_name() :: string().
-opaque db() :: db_t(). % define db_t in the DB-implementation!

-type subscr_action_t() :: write | delete | split.
-type subscr_element() :: close_db | {Operation::subscr_action_t(), Key::?RT:key()}.
-type subscr_changes_fun_t() :: fun((DB::db_t(), Operation::subscr_action_t(), Key::?RT:key()) -> any()).
-type subscr_remove_fun_t() :: fun(() -> any()).
-type subscr_t() :: {Tag::any(), intervals:interval(), ChangesFun::subscr_changes_fun_t(), CloseDBFun::subscr_remove_fun_t()}.

-ifdef(with_export_type_support).
-export_type([db/0, value/0, version/0, kvv_list/0, db_as_list/0,
              subscr_action_t/0, subscr_changes_fun_t/0, subscr_remove_fun_t/0,
              subscr_t/0]).
-endif.

-export([new/0, open/1, close/1, close/2]).
-export([get_name/1]).
-export([get_entry/2, get_entry2/2, set_entry/2, update_entry/2, delete_entry/2]).
-export([read/2, write/4, delete/2]).
-export([get_entries/2, get_entries/3]).
-export([update_entries/4]).
-export([delete_entries/2]).
-export([get_load/1, get_load/2, split_data/2, get_data/1, add_data/2]).
-export([check_db/1]).
-export([set_subscription/5, set_subscription/2,
         get_subscription/2, remove_subscription/2]).
-export([record_changes/2, stop_record_changes/1, stop_record_changes/2,
         get_changes/1, get_changes/2]).

%% public methods:
% note: these wrapper methods need to be used in order for dialyzer to cope
% with the opaque db/0 type

-spec new() -> db().
new() -> new_().

-spec open(DBName::db_name()) -> db().
open(DBName) -> open_(DBName).

-spec close(DB::db()) -> any().
close(DB) -> close_(DB).

-spec close(DB::db(), Delete::boolean()) -> any().
close(DB, Delete) -> close_(DB, Delete).

-spec get_name(DB::db()) -> db_name().
get_name(DB) -> get_name_(DB).

-spec get_load(DB::db()) -> Load::integer().
get_load(DB) -> get_load_(DB).

-spec get_load(DB::db(), Interval::intervals:interval()) -> Load::integer().
get_load(DB, Interval) -> get_load_(DB, Interval).

% entry-based methods:
-spec get_entry(DB::db(), Key::?RT:key()) -> db_entry:entry().
get_entry(DB, Key) -> get_entry_(DB, Key).

-spec get_entry2(DB::db(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.
get_entry2(DB, Key) -> get_entry2_(DB, Key).

-spec set_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
set_entry(DB, Entry) -> set_entry_(DB, Entry).

-spec update_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
update_entry(DB, Entry) -> update_entry_(DB, Entry).

-spec delete_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
delete_entry(DB, Entry) -> delete_entry_(DB, Entry).

% convenience methods
% (should not be used inside the DB implementation and thus do not need a wrapper):
-spec read(DB::db(), Key::?RT:key()) ->
         {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.
-spec write(DB::db(), Key::?RT:key(), Value::value(), Version::version()) ->
         NewDB::db().

-spec delete(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | locks_set | undef}.

% operations on / with multiple DB entries:
-spec get_entries(DB::db(), Range::intervals:interval()) -> db_as_list().
get_entries(DB, Range) -> get_entries_(DB, Range).

-spec get_entries(DB::db(),
                  FilterFun::fun((DBEntry::db_entry:entry()) -> boolean()),
                  ValueFun::fun((DBEntry::db_entry:entry()) -> Value))
        -> [Value].
get_entries(DB, FilterFun, ValueFun) -> get_entries_(DB, FilterFun, ValueFun).

-spec update_entries(DB::db(), Values::[db_entry:entry()],
                     Pred::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> boolean()),
                     UpdateFun::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> UpdatedEntry::db_entry:entry()))
        -> NewDB::db().
update_entries(DB, Values, Pred, UpdateFun) -> update_entries_(DB, Values, Pred, UpdateFun).

-spec delete_entries(DB::db(),
                     RangeOrFun::intervals:interval() |
                                 fun((DBEntry::db_entry:entry()) -> boolean()))
        -> NewDB::db().
delete_entries(DB, RangeOrFun) -> delete_entries_(DB, RangeOrFun).

-spec split_data(DB::db(), MyNewInterval::intervals:interval()) ->
         {NewDB::db(), db_as_list()}.
split_data(DB, MyNewInterval) -> split_data_(DB, MyNewInterval).

-spec get_data(DB::db()) -> db_as_list().
get_data(DB) -> get_data_(DB).

-spec add_data(DB::db(), db_as_list()) -> NewDB::db().
add_data(DB, Data) -> add_data_(DB, Data).

% subscriptions:
-spec set_subscription(DB::db(), Tag::any(), I::intervals:interval(), ChangesFun::subscr_changes_fun_t(), RemSubscrFun::subscr_remove_fun_t()) -> db().
set_subscription(DB, Tag, I, ChangesFun, RemSubscrFun) ->
    set_subscription_(DB, Tag, I, ChangesFun, RemSubscrFun).

-spec set_subscription(DB::db(), subscr_t()) -> db().
set_subscription(DB, SubscrTuple) ->
    set_subscription_(DB, SubscrTuple).

-spec get_subscription(DB::db(), Tag::any()) -> [subscr_t()].
get_subscription(DB, Tag) -> get_subscription_(DB, Tag).

-spec remove_subscription(DB::db(), Tag::any()) -> db().
remove_subscription(DB, Tag) -> remove_subscription_(DB, Tag).

% recording changes to the DB:
-spec record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
record_changes(DB, Interval) -> record_changes_(DB, Interval).

-spec stop_record_changes(OldDB::db()) -> NewDB::db().
stop_record_changes(DB) -> stop_record_changes_(DB).

-spec stop_record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
stop_record_changes(DB, Interval) -> stop_record_changes_(DB, Interval).

-spec get_changes(DB::db()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes(DB) -> get_changes_(DB).

-spec get_changes(DB::db(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes(DB, Interval) -> get_changes_(DB, Interval).

% note: no need for a wrapper here:
-spec check_db(DB::db()) -> {true, []} | {false, InvalidEntries::db_as_list()}.

%% private methods (must be implemented by the DB)
-spec new_() -> db_t().
-spec open_(DBName::db_name()) -> db_t().
-spec close_(DB::db_t()) -> any().
-spec close_(DB::db_t(), Delete::boolean()) -> any().
-spec get_name_(DB::db_t()) -> db_name().
-spec get_load_(DB::db_t(), Interval::intervals:interval()) -> Load::integer().
-spec get_load_(DB::db_t()) -> Load::integer().

-spec get_entry_(DB::db_t(), Key::?RT:key()) -> db_entry:entry().
-spec get_entry2_(DB::db_t(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.
-spec set_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
-spec update_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
-spec delete_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().

-spec get_entries_(DB::db_t(), Range::intervals:interval()) -> db_as_list().
-spec get_entries_(DB::db_t(),
                   FilterFun::fun((DBEntry::db_entry:entry()) -> boolean()),
                   ValueFun::fun((DBEntry::db_entry:entry()) -> Value))
        -> [Value].
-spec update_entries_(DB::db_t(), Values::[db_entry:entry()],
                      Pred::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> boolean()),
                      UpdateFun::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> UpdatedEntry::db_entry:entry()))
        -> NewDB::db_t().
-spec delete_entries_(DB::db_t(),
                      RangeOrFun::intervals:interval() |
                                  fun((DBEntry::db_entry:entry()) -> boolean()))
        -> NewDB::db_t().

-spec split_data_(DB::db_t(), MyNewInterval::intervals:interval()) ->
         {NewDB::db_t(), db_as_list()}.
-spec get_data_(DB::db_t()) -> db_as_list().
-spec add_data_(DB::db_t(), db_as_list()) -> NewDB::db_t().

-spec record_changes_(OldDB::db_t(), intervals:interval()) -> NewDB::db_t().
-spec stop_record_changes_(OldDB::db_t()) -> NewDB::db_t().
-spec stop_record_changes_(OldDB::db_t(), intervals:interval()) -> NewDB::db_t().
-spec get_changes_(DB::db_t()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
-spec get_changes_(DB::db_t(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
