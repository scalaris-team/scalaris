%  @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc Database based on db_ets that verifies the correct use of the DB
%%      interface.
%% @end
%% @version $Id$
-module(db_verify_use).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).

-define(BASE_DB, db_ets).

-type db_t()::{?BASE_DB:db(), Counter::non_neg_integer()}.

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

%% -define(TRACE0(Fun), io:format("~w: ~w()~n", [self(), Fun])).
%% -define(TRACE1(Fun, Par1), io:format("~w: ~w(~w)~n", [self(), Fun, Par1])).
%% -define(TRACE2(Fun, Par1, Par2), io:format("~w: ~w(~w, ~w)~n", [self(), Fun, Par1, Par2])).
%% -define(TRACE3(Fun, Par1, Par2, Par3), io:format("~w: ~w(~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3])).
%% -define(TRACE4(Fun, Par1, Par2, Par3, Par4), io:format("~w: ~w(~w, ~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3, Par4])).
%% -define(TRACE5(Fun, Par1, Par2, Par3, Par4, Par5), io:format("~w: ~w(~w, ~w, ~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3, Par4, Par5])).
-define(TRACE0(Fun), ok).
-define(TRACE1(Fun, Par1), ok).
-define(TRACE2(Fun, Par1, Par2), ok).
-define(TRACE3(Fun, Par1, Par2, Par3), ok).
-define(TRACE4(Fun, Par1, Par2, Par3, Par4), ok).
-define(TRACE5(Fun, Par1, Par2, Par3, Par4, Par5), ok).
-spec verify_counter(Counter::non_neg_integer()) -> ok.
verify_counter(Counter) ->
    case erlang:get(?MODULE) of
        Counter   -> ok;
        undefined -> erlang:error({counter_undefined, Counter});
        _         -> erlang:error({counter_mismatch, Counter, erlang:get(?MODULE)})
    end.

-spec update_counter(Counter::non_neg_integer()) -> NewCounter::non_neg_integer().
update_counter(Counter) ->
    NewCounter = Counter + 1,
    erlang:put(?MODULE, NewCounter),
    NewCounter.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc initializes a new database.
-spec new_() -> db_t().
new_() ->
    ?TRACE0(new),
    case erlang:get(?MODULE) of
        undefined -> ok;
        _         -> erlang:error({counter_defined, erlang:get(?MODULE)})
    end,
    erlang:put(?MODULE, 0),
    {?BASE_DB:new(), 0}.

%% @doc Re-opens a previously existing database.
-spec open_(DBName::db_name()) -> db_t().
open_(FileName) ->
    ?TRACE1(open, FileName),
    case erlang:get(?MODULE) of
        undefined -> ok;
        _         -> erlang:error({counter_defined, erlang:get(?MODULE)})
    end,
    erlang:put(?MODULE, 0),
    {?BASE_DB:open(FileName), 0}.

%% @doc Closes and deletes the DB.
-spec close_(DB::db_t()) -> any().
close_({DB, _Counter} = _DB_) ->
    ?TRACE1(close, _DB_),
    erlang:erase(?MODULE),
    ?BASE_DB:close(DB).

%% @doc Closes and (optionally) deletes the DB.
close_({DB, _Counter} = _DB_, Delete) ->
    ?TRACE2(close, _DB_, Delete),
    erlang:erase(?MODULE),
    ?BASE_DB:close(DB, Delete).

%% @doc Returns the name of the DB which can be used with open/1.
-spec get_name_(DB::db_t()) -> db_name().
get_name_({DB, Counter} = _DB_) ->
    ?TRACE1(get_name, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_name(DB).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
-spec get_entry_(DB::db_t(), Key::?RT:key()) -> db_entry:entry().
get_entry_({DB, Counter} = _DB_, Key) ->
    ?TRACE2(get_entry, _DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:get_entry(DB, Key).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
-spec get_entry2_(DB::db_t(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.
get_entry2_({DB, Counter} = _DB_, Key) ->
    ?TRACE2(get_entry2, _DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:get_entry2(DB, Key).

%% @doc Inserts a complete entry into the DB.
-spec set_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
set_entry_({DB, Counter} = _DB_, Entry) ->
    ?TRACE2(set_entry, _DB_, Entry),
    verify_counter(Counter),
    {?BASE_DB:set_entry(DB, Entry), update_counter(Counter)}.

%% @doc Updates an existing (!) entry in the DB.
-spec update_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
update_entry_({DB, Counter} = _DB_, Entry) ->
    ?TRACE2(update_entry, _DB_, Entry),
    verify_counter(Counter),
    {true, _} = ?BASE_DB:get_entry2(DB, db_entry:get_key(Entry)),
    {?BASE_DB:update_entry(DB, Entry), update_counter(Counter)}.

%% @doc Removes all values with the given entry's key from the DB.
-spec delete_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
delete_entry_({DB, Counter} = _DB_, Entry) ->
    ?TRACE2(delete_entry, _DB_, Entry),
    verify_counter(Counter),
    {?BASE_DB:delete_entry(DB, Entry), update_counter(Counter)}.

%% @doc Returns the number of stored keys.
-spec get_load_(DB::db_t()) -> Load::integer().
get_load_({DB, Counter} = _DB_) ->
    ?TRACE1(get_load, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_load(DB).

%% @doc Returns the number of stored keys in the given Interval.
-spec get_load_(DB::db_t(), Interval::intervals:interval()) -> Load::integer().
get_load_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(get_load, _DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_load(DB, Interval).

%% @doc Adds all db_entry objects in the Data list.
-spec add_data_(DB::db_t(), db_as_list()) -> NewDB::db_t().
add_data_({DB, Counter} = _DB_, Data) ->
    ?TRACE2(add_data, _DB_, Data),
    verify_counter(Counter),
    {?BASE_DB:add_data(DB, Data), update_counter(Counter)}.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
-spec split_data_(DB::db_t(), MyNewInterval::intervals:interval()) ->
         {NewDB::db_t(), db_as_list()}.
split_data_({DB, Counter} = _DB_, MyNewInterval) ->
    ?TRACE2(split_data, _DB_, MyNewInterval),
    verify_counter(Counter),
    {MyNewDB, HisList} = ?BASE_DB:split_data(DB, MyNewInterval),
    {{MyNewDB, update_counter(Counter)}, HisList}.

%% @doc Returns the key that would remove not more than TargetLoad entries
%%      from the DB when starting at the key directly after Begin.
-spec get_split_key_(DB::db_t(), Begin::?RT:key(), TargetLoad::pos_integer(), forward | backward)
        -> {?RT:key(), TakenLoad::pos_integer()}.
get_split_key_({DB, Counter} = _DB_, Begin, TargetLoad, Direction) ->
    ?TRACE4(get_load, _DB_, Begin, TargetLoad, Direction),
    verify_counter(Counter),
    ?BASE_DB:get_split_key(DB, Begin, TargetLoad, Direction).

%% @doc Gets (non-empty) db_entry objects in the given range.
-spec get_entries_(DB::db_t(), Range::intervals:interval()) -> db_as_list().
get_entries_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(get_entries, _DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_entries(DB, Interval).

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%%      which FilterFun returns true.
-spec get_entries_(DB::db_t(),
                   FilterFun::fun((DBEntry::db_entry:entry()) -> boolean()),
                   ValueFun::fun((DBEntry::db_entry:entry()) -> Value))
        -> [Value].
get_entries_({DB, Counter} = _DB_, FilterFun, ValueFun) ->
    ?TRACE3(get_entries, _DB_, FilterFun, ValueFun),
    verify_counter(Counter),
    ?BASE_DB:get_entries(DB, FilterFun, ValueFun).

%% @doc Returns all key-value pairs of the given DB which are in the given
%%      interval but at most ChunkSize elements.
-spec get_chunk_(DB::db_t(), Interval::intervals:interval(), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), db_as_list()}.
get_chunk_({DB, Counter} = _DB_, Interval, ChunkSize) ->
    ?TRACE3(get_chunk, _DB_, Interval, ChunkSize),
    verify_counter(Counter),
    ?BASE_DB:get_chunk(DB, Interval, ChunkSize).

%% @doc Returns all key-value pairs of the given DB which are in the given
%%      interval but at most ChunkSize elements.
-spec get_chunk_(DB::db_t(), Interval::intervals:interval(),
                 FilterFun::fun((db_entry:entry()) -> boolean()),
                 ValueFun::fun((db_entry:entry()) -> V), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [V]}.
get_chunk_({DB, Counter} = _DB_, Interval, FilterFun, ValueFun, ChunkSize) ->
    ?TRACE5(get_chunk, _DB_, Interval, FilterFun, ValueFun, ChunkSize),
    verify_counter(Counter),
    ?BASE_DB:get_chunk(DB, Interval, FilterFun, ValueFun, ChunkSize).

%% @doc Returns all DB entries.
-spec get_data_(DB::db_t()) -> db_as_list().
get_data_({DB, Counter} = _DB_) ->
    ?TRACE1(get_data, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_data(DB).

%% doc Adds a subscription for the given interval under Tag (overwrites an
%%     existing subscription with that tag).
-spec set_subscription_(State::db_t(), subscr_t()) -> db_t().
set_subscription_({DB, Counter} = _DB_, SubscrTuple) ->
    ?TRACE2(set_subscription, _DB_, SubscrTuple),
    verify_counter(Counter),
    {?BASE_DB:set_subscription(DB, SubscrTuple), update_counter(Counter)}.

%% doc Gets a subscription stored under Tag (empty list if there is none).
-spec get_subscription_(State::db_t(), Tag::any()) -> [subscr_t()].
get_subscription_({DB, Counter} = _DB_, Tag) ->
    ?TRACE2(get_data, _DB_, Tag),
    verify_counter(Counter),
    ?BASE_DB:get_subscription(DB, Tag).

%% doc Removes a subscription stored under Tag (if there is one).
-spec remove_subscription_(State::db_t(), Tag::any()) -> db_t().
remove_subscription_({DB, Counter} = _DB_, Tag) ->
    ?TRACE2(get_data, _DB_, Tag),
    verify_counter(Counter),
    {?BASE_DB:remove_subscription(DB, Tag), update_counter(Counter)}.

%% @doc Adds the new interval to the interval to record changes for.
%%      Changed entries can then be gathered by get_changes/1.
-spec record_changes_(OldDB::db_t(), intervals:interval()) -> NewDB::db_t().
record_changes_({DB, Counter} = _DB_, NewInterval) ->
    ?TRACE2(record_changes, _DB_, NewInterval),
    verify_counter(Counter),
    {?BASE_DB:record_changes(DB, NewInterval), update_counter(Counter)}.

%% @doc Stops recording changes and removes all entries from the table of
%%      changed keys.
-spec stop_record_changes_(OldDB::db_t()) -> NewDB::db_t().
stop_record_changes_({DB, Counter} = _DB_) ->
    ?TRACE1(stop_record_changes, _DB_),
    verify_counter(Counter),
    {?BASE_DB:stop_record_changes(DB), update_counter(Counter)}.

%% @doc Stops recording changes in the given interval and removes all such
%%      entries from the table of changed keys.
-spec stop_record_changes_(OldDB::db_t(), intervals:interval()) -> NewDB::db_t().
stop_record_changes_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(stop_record_changes, _DB_, Interval),
    verify_counter(Counter),
    {?BASE_DB:stop_record_changes(DB, Interval), update_counter(Counter)}.

%% @doc Gets all db_entry objects which have been changed or deleted.
-spec get_changes_(DB::db_t()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes_({DB, Counter} = _DB_) ->
    ?TRACE1(get_changes, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_changes(DB).

%% @doc Gets all db_entry objects in the given interval which have been
%%      changed or deleted.
-spec get_changes_(DB::db_t(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(get_changes, _DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_changes(DB, Interval).

-spec read(DB::db(), Key::?RT:key()) ->
         {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.
read({DB, Counter} = _DB_, Key) ->
    ?TRACE2(read, _DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:read(DB, Key).

-spec write(DB::db(), Key::?RT:key(), Value::value(), Version::version()) ->
         NewDB::db().
write({DB, Counter} = _DB_, Key, Value, Version) ->
    ?TRACE4(write, _DB_, Key, Value, Version),
    verify_counter(Counter),
    {?BASE_DB:write(DB, Key, Value, Version), update_counter(Counter)}.

-spec delete(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | locks_set | undef}.
delete({DB, Counter} = _DB_, Key) ->
    ?TRACE2(delete, _DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:delete(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

-spec update_entries_(DB::db_t(), Values::[db_entry:entry()],
                      Pred::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> boolean()),
                      UpdateFun::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> UpdatedEntry::db_entry:entry()))
        -> NewDB::db_t().
update_entries_({DB, Counter} = _DB_, NewEntries, Pred, UpdateFun) ->
    ?TRACE4(update_entries, _DB_, NewEntries, Pred, UpdateFun),
    verify_counter(Counter),
    {?BASE_DB:update_entries(DB, NewEntries, Pred, UpdateFun), update_counter(Counter)}.

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
-spec delete_entries_(DB::db_t(),
                      RangeOrFun::intervals:interval() |
                                  fun((DBEntry::db_entry:entry()) -> boolean()))
        -> NewDB::db_t().
delete_entries_({DB, Counter} = _DB_, RangeOrFilterFun) ->
    ?TRACE2(get_entries, _DB_, RangeOrFilterFun),
    verify_counter(Counter),
    {?BASE_DB:delete_entries(DB, RangeOrFilterFun), update_counter(Counter)}.

-spec delete_chunk_(DB::db_t(), Interval::intervals:interval(), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), db_t()}.
delete_chunk_({DB, Counter} = _DB_, Interval, ChunkSize) ->
    ?TRACE3(delete_chunk, _DB_, Interval, ChunkSize),
    verify_counter(Counter),
    {RestInterval, NewDB} = ?BASE_DB:delete_chunk(DB, Interval, ChunkSize),
    {RestInterval, {NewDB, update_counter(Counter)}}.

-spec check_db(DB::db()) -> {true, []} | {false, InvalidEntries::db_as_list()}.
check_db({DB, Counter} = _DB_) ->
    ?TRACE1(check_db, _DB_),
    verify_counter(Counter),
    ?BASE_DB:check_db(DB).
