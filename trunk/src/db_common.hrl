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
%% @doc Common functions for database implementations.
%%      Note: include from a DB implementation!
%%      TODO: Most of them are only provided for convenience - check if
%%      they are still needed (they all are based on the new
%%      ?DB:get_entry/2, ?DB:get_entry2/2, ?DB:set_entry/2,
%%      ?DB:update_entry/2 and ?DB:delete_entry/2 functions)
%% @end
%% @version $Id$

% Beed this variant of get_entry to determine whether an entry is stored in the
% DB or not. Implement in DB-specific files!
-spec get_entry2(DB::db(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.

%% @doc Sets a write lock on a key. If the key does not exist, an empty_val
%%      will be stored for this key.
%%      The write lock is a boolean value per key.
set_write_lock(DB, Key) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    Lockable = (false =:= db_entry:get_writelock(DBEntry)) andalso
                   (0 =:= db_entry:get_readlock(DBEntry)),
    NewEntry = db_entry:set_writelock(DBEntry),
    case Lockable of
        true when not Exists ->
            {set_entry(DB, NewEntry), ok};
        true ->
            {update_entry(DB, NewEntry), ok};
        _ ->
            {DB, failed}
    end.

%% @doc Unsets the write lock of a key. An empty_val value previously stored by
%%      set_write_lock on the same key will be deleted from DB. If there is no
%%      matching value, this function will fail.
%%      The write lock is a boolean value per key.
unset_write_lock(DB, Key) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    IsEmpty = db_entry:is_empty(DBEntry),
    case Exists of
        false ->
            {DB, failed};
        _ when IsEmpty ->
            {delete_entry(DB, DBEntry), ok};
        _ ->
            case db_entry:get_writelock(DBEntry) of
                true ->
                    NewEntry = db_entry:unset_writelock(DBEntry),
                    {update_entry(DB, NewEntry), ok};
                _ ->
                    {DB, failed}
            end
    end.

%% @doc Sets a read lock on an existing key.
%%      The read lock is an integer value per key.
set_read_lock(DB, Key) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    case Exists of
        false ->
            {DB, failed};
        _ ->
            case db_entry:get_writelock(DBEntry) of
                false ->
                    NewEntry = db_entry:inc_readlock(DBEntry),
                    {update_entry(DB, NewEntry), ok};
                _ ->
                    {DB, failed}
            end
    end.

%% @doc unsets a read lock on a key
%%      the read lock is an integer value per key
unset_read_lock(DB, Key) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    case Exists of
        false ->
            {DB, failed};
        _ ->
            case db_entry:get_readlock(DBEntry) of
                0 ->
                    {DB, failed};
                _ ->
                    NewEntry = db_entry:dec_readlock(DBEntry),
                    {update_entry(DB, NewEntry), ok}
            end
    end.

%% @doc get the locks and version of a key
get_locks(DB, Key) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    case Exists of
        false ->
            {DB, failed};
        _ ->
            {DB, {db_entry:get_writelock(DBEntry),
                  db_entry:get_readlock(DBEntry),
                  db_entry:get_version(DBEntry)}}
    end.

%% @doc Reads the version and value of a key.
read(DB, Key) ->
    DBEntry = get_entry(DB, Key),
    {ok, db_entry:get_value(DBEntry), db_entry:get_version(DBEntry)}.

%% @doc Updates the value of the given key.
write(DB, Key, Value, Version) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    case Exists of
        false ->
            NewEntry = db_entry:new(Key, Value, Version),
            set_entry(DB, NewEntry);
        _ ->
            NewEntry = db_entry:set_value(
                         db_entry:set_version(DBEntry, Version), Value),
            update_entry(DB, NewEntry)
    end.

%% @doc Deletes the key. Returns {DB, undef} if the key does not exist in the
%%      DB, {DB, locks_set} if read or write locks are still set and {DB, ok}
%%      if the operation was successfully performed.
delete(DB, Key) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    case Exists of
        false ->
            {DB, undef};
        _ ->
            case db_entry:get_writelock(DBEntry) =:= false andalso
                     db_entry:get_readlock(DBEntry) =:= 0 of
                true ->
                    {delete_entry(DB, DBEntry), ok};
                _ ->
                    {DB, locks_set}
            end
    end.

%% @doc Reads the version of a key.
get_version(DB, Key) ->
    {Exists, DBEntry} = get_entry2(DB, Key),
    case Exists of
        false -> failed;
        _     -> {ok, db_entry:get_version(DBEntry)}
    end.

%% @doc Updates all entries in the DB that are listed in KVs with versions
%%      higher than existing entries and with no locks set on existing entries.
update_if_newer(OldDB, KVs) ->
    F = fun ({Key, Value, Version}, DB) ->
                 {Exists, DBEntry} = get_entry2(DB, Key),
                 IsUpdatable = db_entry:get_writelock(DBEntry) =:= false andalso
                                   db_entry:get_readlock(DBEntry) =:= 0 andalso
                                   db_entry:get_version(DBEntry) < Version,
                 case Exists of
                     false ->
                         set_entry(DB, db_entry:new(Key, Value, Version));
                     _ when IsUpdatable ->
                         update_entry(DB, db_entry:new(Key, Value, Version));
                     _ ->
                         DB
                 end
        end,
    lists:foldl(F, OldDB, KVs).

%% @doc Checks whether all entries in the DB are valid, i.e.
%%      - no writelocks and readlocks at the same time
%%      - no empty_val values (these should only be in the DB temporarily)
%%      - version is greater than or equal to 0
%%      Returns the result of the check and a list of invalid entries.
check_db(DB) ->
    Data = get_data(DB),
    ValidFun = fun(DBEntry) ->
                       not db_entry:is_empty(DBEntry) andalso
                           not (db_entry:get_writelock(DBEntry) andalso
                                    db_entry:get_readlock(DBEntry) > 0) andalso
                           db_entry:get_version(DBEntry) >= 0
               end,
    {_Valid, Invalid} = lists:partition(ValidFun, Data),
    case Invalid of
        [] -> {true, []};
        _  -> {false, Invalid}
    end.

%% @doc Adds the new interval to the interval to record changes for.
%%      Changed entries can then be gathered by get_changes/1.
record_changes({DB, CKInt, CKDB}, NewInterval) ->
    {DB, intervals:union(CKInt, NewInterval), CKDB}.

%% @doc Stops recording changes and deletes all entries of the table of changed
%%      keys. 
stop_record_changes({DB, _CKInt, CKDB}) ->
    ?CKETS:delete_all_objects(CKDB),
    {DB, intervals:empty(), CKDB}.

%% @doc Get the changed keys database from the state (seperate function to make
%%      dialyzer happy with get_changes/1 calling get_changes_helper/4).
-spec get_ckdb(State::db()) -> tid() | atom().
get_ckdb({_DB, _CKInt, CKDB}) -> CKDB.

%% @doc Gets all db_entry objects which have been changed or deleted.
get_changes(State) ->
    CKDB = get_ckdb(State),
    get_changes_helper(State, ?CKETS:tab2list(CKDB), [], []).

%% @doc Helper for get_changes/2 that adds the entry of a changed key either to
%%      the list of changed entries or to the list of deleted entries.
-spec get_changes_helper(State::db(), ChangedKeys::[{?RT:key()}], ChangedEntries::[db_entry:entry()], DeletedKeys::[?RT:key()]) -> {ChangedEntries::[db_entry:entry()], DeletedKeys::[?RT:key()]}.
get_changes_helper(_State, [], ChangedEntries, DeletedKeys) ->
    {ChangedEntries, DeletedKeys};
get_changes_helper(State, [{CurKey} | RestKeys], ChangedEntries, DeletedKeys) ->
    {Existing, Entry} = get_entry2(State, CurKey),
    case Existing of
        true -> get_changes_helper(State, RestKeys, [Entry | ChangedEntries], DeletedKeys);
        _    -> get_changes_helper(State, RestKeys, ChangedEntries, [CurKey | DeletedKeys])
    end.
