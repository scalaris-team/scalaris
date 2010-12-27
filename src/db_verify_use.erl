%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc Database based on db_gb_trees that verifies the correct use of the DB
%%      interface.
%% @end
%% @version $Id$
-module(db_verify_use).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).

-define(BASE_DB, db_gb_trees).

-type db_t()::{?BASE_DB:db(), Counter::non_neg_integer()}.

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

%% -define(TRACE0(Fun), io:format("~w: ~w()~n", [self(), Fun])).
%% -define(TRACE1(Fun, Par1), io:format("~w: ~w(~w)~n", [self(), Fun, Par1])).
%% -define(TRACE2(Fun, Par1, Par2), io:format("~w: ~w(~w, ~w)~n", [self(), Fun, Par1, Par2])).
%% -define(TRACE3(Fun, Par1, Par2, Par3), io:format("~w: ~w(~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3])).
%% -define(TRACE4(Fun, Par1, Par2, Par3, Par4), io:format("~w: ~w(~w, ~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3, Par4])).
-define(TRACE0(Fun), ok).
-define(TRACE1(Fun, Par1), ok).
-define(TRACE2(Fun, Par1, Par2), ok).
-define(TRACE3(Fun, Par1, Par2, Par3), ok).
-define(TRACE4(Fun, Par1, Par2, Par3, Par4), ok).
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
new_() ->
    ?TRACE0(new),
    case erlang:get(?MODULE) of
        undefined -> ok;
        _         -> erlang:error({counter_defined, erlang:get(?MODULE)})
    end,
    erlang:put(?MODULE, 0),
    {?BASE_DB:new(), 0}.

%% @doc Re-opens a previously existing database.
open_(FileName) ->
    ?TRACE1(open, FileName),
    case erlang:get(?MODULE) of
        undefined -> ok;
        _         -> erlang:error({counter_defined, erlang:get(?MODULE)})
    end,
    erlang:put(?MODULE, 0),
    {?BASE_DB:open(FileName), 0}.

%% @doc Closes and deletes the DB.
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
get_name_({DB, Counter} = _DB_) ->
    ?TRACE1(get_name, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_name(DB).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
get_entry_({DB, Counter} = _DB_, Key) ->
    ?TRACE2(get_entry, _DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:get_entry(DB, Key).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2_({DB, Counter} = _DB_, Key) ->
    ?TRACE2(get_entry2, _DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:get_entry2(DB, Key).

%% @doc Inserts a complete entry into the DB.
set_entry_({DB, Counter} = _DB_, Entry) ->
    ?TRACE2(set_entry, _DB_, Entry),
    verify_counter(Counter),
    {?BASE_DB:set_entry(DB, Entry), update_counter(Counter)}.

%% @doc Updates an existing (!) entry in the DB.
update_entry_({DB, Counter} = _DB_, Entry) ->
    ?TRACE2(update_entry, _DB_, Entry),
    verify_counter(Counter),
    {true, _} = ?BASE_DB:get_entry2(DB, db_entry:get_key(Entry)),
    {?BASE_DB:update_entry(DB, Entry), update_counter(Counter)}.

%% @doc Removes all values with the given entry's key from the DB.
delete_entry_({DB, Counter} = _DB_, Entry) ->
    ?TRACE2(delete_entry, _DB_, Entry),
    verify_counter(Counter),
    {?BASE_DB:delete_entry(DB, Entry), update_counter(Counter)}.

%% @doc Returns the number of stored keys.
get_load_({DB, Counter} = _DB_) ->
    ?TRACE1(get_load, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_load(DB).

%% @doc Returns the number of stored keys in the given Interval.
get_load_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(get_load, _DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_load(DB, Interval).

%% @doc Adds all db_entry objects in the Data list.
add_data_({DB, Counter} = _DB_, Data) ->
    ?TRACE2(add_data, _DB_, Data),
    verify_counter(Counter),
    {?BASE_DB:add_data(DB, Data), update_counter(Counter)}.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
split_data_({DB, Counter} = _DB_, MyNewInterval) ->
    ?TRACE2(split_data, _DB_, MyNewInterval),
    verify_counter(Counter),
    {MyNewDB, HisList} = ?BASE_DB:split_data(DB, MyNewInterval),
    {{MyNewDB, update_counter(Counter)}, HisList}.

%% @doc Gets (non-empty) db_entry objects in the given range.
get_entries_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(get_entries, _DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_entries(DB, Interval).

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%%      which FilterFun returns true.
get_entries_({DB, Counter} = _DB_, FilterFun, ValueFun) ->
    ?TRACE3(get_entries, _DB_, FilterFun, ValueFun),
    verify_counter(Counter),
    ?BASE_DB:get_entries(DB, FilterFun, ValueFun).

%% @doc Returns all DB entries.
get_data_({DB, Counter} = _DB_) ->
    ?TRACE1(get_data, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_data(DB).

%% @doc Adds the new interval to the interval to record changes for.
%%      Changed entries can then be gathered by get_changes/1.
record_changes_({DB, Counter} = _DB_, NewInterval) ->
    ?TRACE2(record_changes, _DB_, NewInterval),
    verify_counter(Counter),
    {?BASE_DB:record_changes(DB, NewInterval), update_counter(Counter)}.

%% @doc Stops recording changes and removes all entries from the table of
%%      changed keys.
stop_record_changes_({DB, Counter} = _DB_) ->
    ?TRACE1(stop_record_changes, _DB_),
    verify_counter(Counter),
    {?BASE_DB:stop_record_changes(DB), update_counter(Counter)}.

%% @doc Stops recording changes in the given interval and removes all such
%%      entries from the table of changed keys.
stop_record_changes_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(stop_record_changes, _DB_, Interval),
    verify_counter(Counter),
    {?BASE_DB:stop_record_changes(DB, Interval), update_counter(Counter)}.

%% @doc Gets all db_entry objects which have been changed or deleted.
get_changes_({DB, Counter} = _DB_) ->
    ?TRACE1(get_changes, _DB_),
    verify_counter(Counter),
    ?BASE_DB:get_changes(DB).

%% @doc Gets all db_entry objects in the given interval which have been
%%      changed or deleted.
get_changes_({DB, Counter} = _DB_, Interval) ->
    ?TRACE2(get_changes, _DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_changes(DB, Interval).

set_write_lock({DB, Counter} = _DB_, Key) ->
    ?TRACE2(set_write_lock, _DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:set_write_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

unset_write_lock({DB, Counter} = _DB_, Key) ->
    ?TRACE2(unset_write_lock, _DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:unset_write_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

set_read_lock({DB, Counter} = _DB_, Key) ->
    ?TRACE2(set_read_lock, _DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:set_read_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

unset_read_lock({DB, Counter} = _DB_, Key) ->
    ?TRACE2(unset_read_lock, _DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:unset_read_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

read({DB, Counter} = _DB_, Key) ->
    ?TRACE2(read, _DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:read(DB, Key).

write({DB, Counter} = _DB_, Key, Value, Version) ->
    ?TRACE4(write, _DB_, Key, Value, Version),
    verify_counter(Counter),
    {?BASE_DB:write(DB, Key, Value, Version), update_counter(Counter)}.

delete({DB, Counter} = _DB_, Key) ->
    ?TRACE2(delete, _DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:delete(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

update_entries_({DB, Counter} = _DB_, NewEntries, Pred, UpdateFun) ->
    ?TRACE4(update_entries, _DB_, NewEntries, Pred, UpdateFun),
    verify_counter(Counter),
    {?BASE_DB:update_entries(DB, NewEntries, Pred, UpdateFun), update_counter(Counter)}.

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
delete_entries_({DB, Counter} = _DB_, RangeOrFilterFun) ->
    ?TRACE2(get_entries, _DB_, RangeOrFilterFun),
    verify_counter(Counter),
    {?BASE_DB:delete_entries(DB, RangeOrFilterFun), update_counter(Counter)}.

check_db({DB, Counter} = _DB_) ->
    ?TRACE1(check_db, _DB_),
    verify_counter(Counter),
    ?BASE_DB:check_db(DB).
