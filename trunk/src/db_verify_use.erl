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

-opaque(db()::{?BASE_DB:db(), Counter::non_neg_integer()}).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

%% -define(TRACE0(Fun), io:format("~w: ~w()~n", [self(), Fun])).
%% -define(TRACE1(Fun, Par1), io:format("~w: ~w(~w)~n", [self(), Fun, Par1])).
%% -define(TRACE2(Fun, Par1, Par2), io:format("~w: ~w(~w, ~w)~n", [self(), Fun, Par1, Par2])).
%% -define(TRACE3(Fun, Par1, Par2, Par3), io:format("~w: ~w(~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3])).
%% -define(TRACE4(Fun, Par1, Par2, Par3, Par4), io:format("~w: ~w(~w, ~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3, Par4])).
-define(TRACE0(Fun), Fun, ok).
-define(TRACE1(Fun, Par1), Fun, Par1, ok).
-define(TRACE2(Fun, Par1, Par2), Fun, Par1, Par2, ok).
-define(TRACE3(Fun, Par1, Par2, Par3), Fun, Par1, Par2, Par3, ok).
-define(TRACE4(Fun, Par1, Par2, Par3, Par4), Fun, Par1, Par2, Par3, Par4, ok).
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

%% @doc initializes a new database
new(NodeId) ->
    ?TRACE1(new, NodeId),
    case erlang:get(?MODULE) of
        undefined -> ok;
        _         -> erlang:error({counter_defined, erlang:get(?MODULE)})
    end,
    erlang:put(?MODULE, 0),
    {?BASE_DB:new(NodeId), 0}.

%% delete DB (missing function)
close({DB, _Counter} = DB_) ->
    ?TRACE1(close, DB_),
    erlang:erase(?MODULE),
    ?BASE_DB:close(DB).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
get_entry({DB, Counter} = DB_, Key) ->
    ?TRACE2(get_entry, DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:get_entry(DB, Key).

%% @doc Inserts a complete entry into the DB.
set_entry({DB, Counter} = DB_, Entry) ->
    ?TRACE2(set_entry, DB_, Entry),
    verify_counter(Counter),
    {?BASE_DB:set_entry(DB, Entry), update_counter(Counter)}.

%% @doc Updates an existing (!) entry in the DB.
update_entry({DB, Counter} = DB_, Entry) ->
    ?TRACE2(update_entry, DB_, Entry),
    verify_counter(Counter),
    {?BASE_DB:update_entry(DB, Entry), update_counter(Counter)}.

%% @doc Removes all values with the given entry's key from the DB.
delete_entry({DB, Counter} = DB_, Entry) ->
    ?TRACE2(delete_entry, DB_, Entry),
    verify_counter(Counter),
    {?BASE_DB:delete_entry(DB, Entry), update_counter(Counter)}.

%% @doc Returns the number of stored keys.
get_load({DB, Counter} = DB_) ->
    ?TRACE1(get_load, DB_),
    verify_counter(Counter),
    ?BASE_DB:get_load(DB).

%% @doc Adds all db_entry objects in the Data list.
add_data({DB, Counter} = DB_, Data) ->
    ?TRACE2(add_data, DB_, Data),
    verify_counter(Counter),
    {?BASE_DB:add_data(DB, Data), update_counter(Counter)}.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
split_data({DB, Counter} = DB_, MyNewInterval) ->
    ?TRACE2(split_data, DB_, MyNewInterval),
    verify_counter(Counter),
    {MyNewDB, HisList} = ?BASE_DB:split_data(DB, MyNewInterval),
    {{MyNewDB, update_counter(Counter)}, HisList}.

%% @doc Get key/value pairs in the given range.
get_range_kv({DB, Counter} = DB_, Interval) ->
    ?TRACE2(get_range_kv, DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_range_kv(DB, Interval).

%% @doc Get key/value/version triples of non-write-locked entries in the given range.
get_range_kvv({DB, Counter} = DB_, Interval) ->
    ?TRACE2(get_range_kvv, DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_range_kvv(DB, Interval).

%% @doc Gets db_entry objects in the given range.
get_range_entry({DB, Counter} = DB_, Interval) ->
    ?TRACE2(get_range_entry, DB_, Interval),
    verify_counter(Counter),
    ?BASE_DB:get_range_entry(DB, Interval).

%% @doc Returns all DB entries.
get_data({DB, Counter} = DB_) ->
    ?TRACE1(get_data, DB_),
    verify_counter(Counter),
    ?BASE_DB:get_data(DB).

%% @doc Adds the new interval to the interval to record changes for.
%%      Changed entries can then be gathered by get_changes/1.
record_changes({DB, Counter} = DB_, NewInterval) ->
    ?TRACE2(record_changes, DB_, NewInterval),
    verify_counter(Counter),
    {?BASE_DB:record_changes(DB, NewInterval), update_counter(Counter)}.

%% @doc Stops recording changes and deletes all entries of the table of changed
%%      keys. 
stop_record_changes({DB, Counter} = DB_) ->
    ?TRACE1(stop_record_changes, DB_),
    verify_counter(Counter),
    {?BASE_DB:stop_record_changes(DB), update_counter(Counter)}.

%% @doc Gets all db_entry objects which have been changed or deleted.
get_changes({DB, Counter} = DB_) ->
    ?TRACE1(get_changes, DB_),
    verify_counter(Counter),
    ?BASE_DB:get_changes(DB).

set_write_lock({DB, Counter} = DB_, Key) ->
    ?TRACE2(set_write_lock, DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:set_write_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

unset_write_lock({DB, Counter} = DB_, Key) ->
    ?TRACE2(unset_write_lock, DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:unset_write_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

set_read_lock({DB, Counter} = DB_, Key) ->
    ?TRACE2(set_read_lock, DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:set_read_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

unset_read_lock({DB, Counter} = DB_, Key) ->
    ?TRACE2(unset_read_lock, DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:unset_read_lock(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

get_locks({DB, Counter} = DB_, Key) ->
    ?TRACE2(get_locks, DB_, Key),
    verify_counter(Counter),
    {NewDB, Locks} = ?BASE_DB:get_locks(DB, Key),
    {{NewDB, update_counter(Counter)}, Locks}.

read({DB, Counter} = DB_, Key) ->
    ?TRACE2(read, DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:read(DB, Key).

write({DB, Counter} = DB_, Key, Value, Version) ->
    ?TRACE4(write, DB_, Key, Value, Version),
    verify_counter(Counter),
    {?BASE_DB:write(DB, Key, Value, Version), update_counter(Counter)}.

delete({DB, Counter} = DB_, Key) ->
    ?TRACE2(delete, DB_, Key),
    verify_counter(Counter),
    {NewDB, Status} = ?BASE_DB:delete(DB, Key),
    {{NewDB, update_counter(Counter)}, Status}.

get_version({DB, Counter} = DB_, Key) ->
    ?TRACE2(get_version, DB_, Key),
    verify_counter(Counter),
    ?BASE_DB:get_version(DB, Key).

update_if_newer({DB, Counter} = DB_, KVs) ->
    ?TRACE2(update_if_newer, DB_, KVs),
    verify_counter(Counter),
    {?BASE_DB:update_if_newer(DB, KVs), update_counter(Counter)}.

check_db({DB, Counter} = DB_) ->
    ?TRACE1(check_db, DB_),
    verify_counter(Counter),
    ?BASE_DB:check_db(DB).
