%  @copyright 2008-2011 Zuse Institute Berlin

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
%%% @doc    database behaviour
%%% @end
%% @version $Id$
-module(db_beh).
-author('schuett@zib.de').
-vsn('$Id$').

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-include("scalaris.hrl").
-type value() :: atom() | boolean() | number() | binary().
-type version() :: non_neg_integer().
-type db() :: term().
-type db_as_list() :: [db_entry:entry()].
-type db_name() :: string().

-type subscr_op_t() :: {write, db_entry:entry()} | {delete | split, ?RT:key()}.
-type subscr_changes_fun_t() :: fun((DB::db(), Tag::any(), Operation::subscr_op_t()) -> db()).
-type subscr_remove_fun_t() :: fun((Tag::any()) -> any()).
-type subscr_t() :: {Tag::any(), intervals:interval(), ChangesFun::subscr_changes_fun_t(), CloseDBFun::subscr_remove_fun_t()}.

-callback new() -> db().
-callback open(DBName::db_name()) -> db().
-callback close(DB::db()) -> any().
-callback close(DB::db(), Delete::boolean()) -> any().

-callback get_name(DB::db()) -> db_name().
-callback get_load(DB::db()) -> Load::integer().
-callback get_load(DB::db(), Interval::intervals:interval()) -> Load::integer().

% entry-based methods:
-callback get_entry(DB::db(), Key::?RT:key()) -> db_entry:entry().
-callback get_entry2(DB::db(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.
-callback set_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
-callback update_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
-callback delete_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().

% convenience methods:
-callback read(DB::db(), Key::?RT:key())
        -> {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.
-callback write(DB::db(), Key::?RT:key(), Value::value(), Version::version())
        -> NewDB::db().
-callback delete(DB::db(), Key::?RT:key())
        -> {NewDB::db(), Status::ok | locks_set | undef}.

% operations on / with multiple DB entries:
-callback get_entries(DB::db(), Range::intervals:interval()) -> db_as_list().
-callback get_entries(DB::db(),
                      FilterFun::fun((DBEntry::db_entry:entry()) -> boolean()),
                      ValueFun::fun((DBEntry::db_entry:entry()) -> Value))
        -> [Value].
-callback get_chunk(DB::db(), Interval::intervals:interval(), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), db_as_list()}.
-callback get_chunk(DB::db(), Interval::intervals:interval(),
                    FilterFun::fun((db_entry:entry()) -> boolean()),
                    ValueFun::fun((db_entry:entry()) -> V), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [V]}.
-callback update_entries(DB::db(), Values::[db_entry:entry()],
                         Pred::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> boolean()),
                         UpdateFun::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> UpdatedEntry::db_entry:entry()))
        -> NewDB::db().
-callback delete_entries(DB::db(),
                         RangeOrFun::intervals:interval() |
                                     fun((DBEntry::db_entry:entry()) -> boolean()))
        -> NewDB::db().
-callback delete_chunk(DB::db(), Interval::intervals:interval(), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), db()}.

-callback split_data(DB::db(), MyNewInterval::intervals:interval())
        -> {NewDB::db(), db_as_list()}.
-callback get_split_key(DB::db(), Begin::?RT:key(), TargetLoad::pos_integer(), forward | backward)
        -> {?RT:key(), TakenLoad::pos_integer()}.

-callback get_data(DB::db()) -> db_as_list().
-callback add_data(DB::db(), db_as_list()) -> NewDB::db().

% subscriptions:
-callback set_subscription(DB::db(), subscr_t()) -> db().
-callback get_subscription(DB::db(), Tag::any()) -> [subscr_t()].
-callback remove_subscription(DB::db(), Tag::any()) -> db().

% recording changes to the DB:
-callback record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
-callback stop_record_changes(OldDB::db()) -> NewDB::db().
-callback stop_record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
-callback get_changes(DB::db()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
-callback get_changes(DB::db(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.

-callback check_db(DB::db()) -> {true, []} | {false, InvalidEntries::db_as_list()}.

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     % init
     {new, 0}, {open, 1},
     {get_name, 1},
     % close, delete?
     {close, 1},
     % standard calls
     {read, 2}, {write, 4},
     {get_entry, 2}, {get_entry2, 2}, {set_entry, 2}, {update_entry, 2},
     {get_chunk, 3}, {get_chunk, 5},
     % dangerous calls
     {delete, 2}, {delete_entry, 2},
     {delete_chunk, 3},
     % load balancing
     {get_load, 1}, {get_load, 2}, {split_data, 2}, {get_split_key, 4},
     % operations on multiple entries
     {get_data, 1}, {add_data, 2},
     {get_entries, 2}, {get_entries, 3},
     {update_entries, 4},
     {delete_entries, 2},
     % record changes
     {record_changes, 2}, {stop_record_changes, 1}, {stop_record_changes, 2},
     {get_changes, 1}, {get_changes, 2},
     % debugging
     {check_db, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.

