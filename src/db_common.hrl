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
%% @doc Common functions for database implementations.
%%      Requires an implementation of Module:delete_entry_at_key_/2.
%%      Note: include from a DB implementation!
%%      TODO: Most of them are only provided for convenience - check if
%%      they are still needed (they all are based on the new
%%      ?DB:get_entry/2, ?DB:get_entry2/2, ?DB:set_entry/2,
%%      ?DB:update_entry/2 and ?DB:delete_entry/2 functions)
%% @end
%% @version $Id$

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
%% -define(TRACE_SNAP(X, Y), ct:pal(ct_error_notify, X, Y)).
-define(TRACE_SNAP(X, Y), ?TRACE(X, Y)).

%% @doc Closes the given DB and deletes all contents (this DB can thus not be
%%      re-opened using open/1).
-spec close_(DB::db_t()) -> any().
close_(State) ->
    close_(State, true).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
-spec get_entry_(DB::db_t(), Key::?RT:key()) -> db_entry:entry().
get_entry_(State, Key) ->
    {_Exists, Result} = get_entry2_(State, Key),
    Result.

%% @doc Reads the version and value of a key.
-spec read(DB::db(), Key::?RT:key()) ->
         {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.
read(DB, Key) ->
    DBEntry = get_entry_(DB, Key),
    {ok, db_entry:get_value(DBEntry), db_entry:get_version(DBEntry)}.

%% @doc Updates the value of the given key.
-spec write(DB::db(), Key::?RT:key(), Value::value(), Version::version()) ->
         NewDB::db().
write(DB, Key, Value, Version) ->   
    {Exists, DBEntry} = get_entry2_(DB, Key),
    case Exists of
        false ->
            NewEntry = db_entry:new(Key, Value, Version),
            set_entry_(DB, NewEntry);
        _ ->
            NewEntry = db_entry:set_value(DBEntry, Value, Version),
            update_entry_(DB, NewEntry)
    end.

%% @doc Deletes the key. Returns {DB, undef} if the key does not exist in the
%%      DB, {DB, locks_set} if read or write locks are still set and {DB, ok}
%%      if the operation was successfully performed.
-spec delete(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | locks_set | undef}.
delete(DB, Key) ->
    {Exists, DBEntry} = get_entry2_(DB, Key),
    case Exists of
        false ->
            {DB, undef};
        _ ->
            case db_entry:is_locked(DBEntry) of
                false ->
                    {delete_entry_(DB, DBEntry), ok};
                _ ->
                    {DB, locks_set}
            end
    end.

%% @doc Removes all values with the given entry's key from the DB.
-spec delete_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
delete_entry_(State, Entry) ->
    Key = db_entry:get_key(Entry),
    delete_entry_at_key_(State, Key).

%% @doc Gets (non-empty) db_entry objects in the given range.
-spec get_entries_(DB::db_t(), Range::intervals:interval()) -> db_as_list().
get_entries_(State, Interval) ->
    {Elements, RestInterval} = intervals:get_elements(Interval),
    case intervals:is_empty(RestInterval) of
        true ->
            [E || Key <- Elements, E <- [get_entry_(State, Key)], not db_entry:is_empty(E)];
        _ ->
            {_, Data} =
                get_chunk_(State, Interval,
                           fun(DBEntry) ->
                                   (not db_entry:is_empty(DBEntry)) andalso
                                       intervals:in(db_entry:get_key(DBEntry), Interval)
                           end,
                           fun(E) -> E end, all),
            Data
    end.

%% @doc Returns all key-value pairs of the given DB which are in the given
%%      interval but at most ChunkSize elements.
%%      Assumes the ets-table is an ordered_set,
%%      may return data from "both ends" of the DB-range if the interval is
%%      ""wrapping around", i.e. its begin is larger than its end.
%%      Returns the chunk and the remaining interval for which the DB may still
%%      have data (a subset of I).
%%      Precond: Interval is a subset of the range of the dht_node and continuous!
-spec get_chunk_(DB::db_t(), Interval::intervals:interval(), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), db_as_list()}.
get_chunk_(DB, Interval, ChunkSize) ->
    get_chunk_(DB, Interval, fun(_) -> true end, fun(E) -> E end, ChunkSize).

%% @doc Updates all (existing or non-existing) non-locked entries from
%%      NewEntries for which Pred(OldEntry, NewEntry) returns true with
%%      UpdateFun(OldEntry, NewEntry).
-spec update_entries_(DB::db_t(), Values::[db_entry:entry()],
                      Pred::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> boolean()),
                      UpdateFun::fun((OldEntry::db_entry:entry(), NewEntry::db_entry:entry()) -> UpdatedEntry::db_entry:entry()))
        -> NewDB::db_t().
update_entries_(OldDB, NewEntries, Pred, UpdateFun) ->
    F = fun(NewEntry, DB) ->
                {Exists, OldEntry} = get_entry2_(DB, db_entry:get_key(NewEntry)),
                IsNotLocked = not db_entry:is_locked(OldEntry),
                IsUpdatable = IsNotLocked andalso Pred(OldEntry, NewEntry),
                case Exists of
                    false when IsUpdatable ->
                        set_entry_(DB, UpdateFun(OldEntry, NewEntry));
                    _ when IsUpdatable ->
                        update_entry_(DB, UpdateFun(OldEntry, NewEntry));
                    _ ->
                        DB
                end
        end,
    lists:foldl(F, OldDB, NewEntries).

%% @doc Checks whether all entries in the DB are valid, i.e.
%%      - no writelocks and readlocks at the same time
%%      - no empty_val values (these should only be in the DB temporarily)
%%      - version is greater than or equal to 0
%%      Returns the result of the check and a list of invalid entries.
-spec check_db(DB::db()) -> {true, []} | {false, InvalidEntries::db_as_list()}.
check_db(DB) ->
    Data = get_data(DB),
    ValidFun = fun(DBEntry) ->
                       not db_entry:is_empty(DBEntry) andalso
                           not (db_entry:get_writelock(DBEntry) =/= false andalso
                                    db_entry:get_readlock(DBEntry) > 0) andalso
                           db_entry:get_version(DBEntry) >= 0
               end,
    {_Valid, Invalid} = lists:partition(ValidFun, Data),
    case Invalid of
        [] -> {true, []};
        _  -> {false, Invalid}
    end.

%% subscriptions

%% doc Adds a subscription for the given interval under Tag (overwrites an
%%     existing subscription with that tag).
-spec set_subscription_(State::db_t(), subscr_t()) -> db_t().
set_subscription_(State = {_DB, Subscr, _SnapState}, SubscrTuple) ->
    ets:insert(Subscr, SubscrTuple),
    State.

%% doc Gets a subscription stored under Tag (empty list if there is none).
-spec get_subscription_(State::db_t(), Tag::any()) -> [subscr_t()].
get_subscription_({_DB, Subscr, _SnapState}, Tag) ->
    ets:lookup(Subscr, Tag).

%% doc Removes a subscription stored under Tag (if there is one).
-spec remove_subscription_(State::db_t(), Tag::any()) -> db_t().
remove_subscription_(State = {_DB, Subscr, _SnapState}, Tag) ->
    case ets:lookup(Subscr, Tag) of
        [] -> ok;
        [{Tag, _I, _ChangesFun, RemSubscrFun}] -> RemSubscrFun(Tag)
    end,
    ets:delete(Subscr, Tag),
    State.

%% @doc Go through all subscriptions and perform the given operation if
%%      matching.
-spec call_subscribers(State::db_t(), Operation::close_db | subscr_op_t()) -> db_t().
call_subscribers(State = {_DB, Subscr, _SnapState}, Operation) ->
    call_subscribers_iter(State, Operation, ets:first(Subscr)).

%% @doc Iterates over all susbcribers and calls their subscribed functions.
-spec call_subscribers_iter(State::db_t(), Operation::close_db | subscr_op_t(),
        CurrentKey::subscr_t() | '$end_of_table') -> db_t().
call_subscribers_iter(State, _Operation, '$end_of_table') ->
    State;
call_subscribers_iter(State = {_DB, Subscr, _SnapState}, Op, CurrentKey) ->
    % assume the key exists (it should since we are iterating over the table!)
    [{Tag, I, ChangesFun, RemSubscrFun}] = ets:lookup(Subscr, CurrentKey),
    NewState =
        case Op of
            close_db ->
                RemSubscrFun(Tag),
                State;
            Operation ->
                case Operation of
                    {write, Entry} -> Key = db_entry:get_key(Entry), ok;
                    {delete, Key}  -> ok;
                    {split, Key}  -> ok
                end,
                case intervals:in(Key, I) of
                    false -> State;
                    _     -> ChangesFun(State, Tag, Operation)
                end
        end,
    call_subscribers_iter(NewState, Op, ets:next(Subscr, CurrentKey)).

%% subscriptions for changed keys

%% @doc Check that the table storing changed keys exists and creates it if
%%      necessary.
-spec subscr_delta_check_table(State::db_t()) -> tid() | atom().
subscr_delta_check_table(State) ->
    case erlang:get('$delta_tab') of
        undefined ->
            CKDBname = list_to_atom(get_name_(State) ++ "_ck"), % changed keys
            CKDB = ?CKETS:new(CKDBname, [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
            erlang:put('$delta_tab', CKDB);
        CKDB -> ok
    end,
    CKDB.

%% @doc Cleans up, i.e. deletes, the table with changed keys (called on
%%      subscription removal).
-spec subscr_delta_close_table(Tag::any()) -> ok | true.
subscr_delta_close_table(_Tag) ->
    case erlang:erase('$delta_tab') of
        undefined -> ok;
        CKDB -> ?CKETS:delete(CKDB)
    end.

%% @doc Inserts/removes the key into the table of changed keys depending on the
%%      operation (called whenever the DB is changed).
-spec subscr_delta(State::db_t(), Tag::any(), Operation::subscr_op_t()) -> db_t().
subscr_delta(State, _Tag, Operation) ->
    CKDB = subscr_delta_check_table(State),
    case Operation of
        {write, Entry} -> ?CKETS:insert(CKDB, {db_entry:get_key(Entry)});
        {delete, Key}  -> ?CKETS:insert(CKDB, {Key});
        {split, Key}   -> ?CKETS:delete(CKDB, Key)
    end,
    State.

%% @doc Removes any changed key in interval I (called when some (sub-)interval
%%      is unsubscribed).
-spec subscr_delta_remove(State::db_t(), I::intervals:interval()) -> ok.
subscr_delta_remove(State, Interval) ->
    CKDB = subscr_delta_check_table(State),
    F = fun (DBEntry, _) ->
                 Key = db_entry:get_key(DBEntry),
                 case intervals:in(Key, Interval) of
                     true -> ?CKETS:delete(CKDB, Key);
                     _    -> true
                 end
        end,
    ?CKETS:foldl(F, true, CKDB),
    ok.

%% @doc Adds the new interval to the interval to record changes for. Entries
%%      which have (potentially) changed can then be gathered by get_changes/1.
-spec record_changes_(OldDB::db_t(), intervals:interval()) -> NewDB::db_t().
record_changes_(State, NewInterval) ->
    RecChanges = get_subscription_(State, record_changes),
    NewSubscr =
        case RecChanges of
            [] -> {record_changes, NewInterval,
                   fun subscr_delta/3, fun subscr_delta_close_table/1};
            [{Tag, I, ChangesFun, RemSubscrFun}] ->
                {Tag, intervals:union(I, NewInterval), ChangesFun, RemSubscrFun}
        end,
    set_subscription_(State, NewSubscr).

%% @doc Stops recording changes and removes all entries from the table of
%%      changed keys.
-spec stop_record_changes_(OldDB::db_t()) -> NewDB::db_t().
stop_record_changes_(State) ->
    remove_subscription_(State, record_changes).

%% @doc Stops recording changes in the given interval and removes all such
%%      entries from the table of changed keys.
-spec stop_record_changes_(OldDB::db_t(), intervals:interval()) -> NewDB::db_t().
stop_record_changes_(State, Interval) ->
    RecChanges = get_subscription_(State, record_changes),
    case RecChanges of
        [] -> State;
        [{Tag, I, ChangesFun, RemSubscrFun}] ->
            subscr_delta_remove(State, Interval),
            NewI = intervals:minus(I, Interval),
            case intervals:is_empty(NewI) of
                true -> remove_subscription_(State, Tag);
                _ -> set_subscription_(State, {Tag, NewI, ChangesFun, RemSubscrFun})
            end
    end.

%% @doc Gets all db_entry objects which have (potentially) been changed or
%%      deleted (might return objects that have not changed but have been
%%      touched by one of the DB setters).
-spec get_changes_(DB::db_t()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes_(State) ->
    get_changes_(State, intervals:all()).

%% @doc Gets all db_entry objects in the given interval which have
%%      (potentially) been changed or deleted (might return objects that have
%%      not changed but have been touched by one of the DB setters).
-spec get_changes_(DB::db_t(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes_(State, Interval) ->
    case erlang:get('$delta_tab') of
        undefined -> get_changes_helper(State, [], Interval, [], []);
        CKDB -> get_changes_helper(State, ?CKETS:tab2list(CKDB), Interval, [], [])
    end.

%% @doc Helper for get_changes/2 that adds the entry of a changed key either to
%%      the list of changed entries or to the list of deleted entries.
-spec get_changes_helper(State::db_t(), ChangedKeys::[{?RT:key()}],
        Interval::intervals:interval(), ChangedEntries::[db_entry:entry()],
        DeletedKeys::[?RT:key()])
            -> {ChangedEntries::[db_entry:entry()], DeletedKeys::[?RT:key()]}.
get_changes_helper(_State, [], _Interval, ChangedEntries, DeletedKeys) ->
    {ChangedEntries, DeletedKeys};
get_changes_helper(State, [{CurKey} | RestKeys], Interval, ChangedEntries, DeletedKeys) ->
    case intervals:in(CurKey, Interval) of
        true ->
            {Existing, Entry} = get_entry2_(State, CurKey),
            case Existing of
                true -> get_changes_helper(State, RestKeys, Interval, [Entry | ChangedEntries], DeletedKeys);
                _    -> get_changes_helper(State, RestKeys, Interval, ChangedEntries, [CurKey | DeletedKeys])
            end;
        _ -> get_changes_helper(State, RestKeys, Interval, ChangedEntries, DeletedKeys)
    end.

%% Snapshot-related functions

%% @doc Copy existing entry to snapshot table
-spec copy_value_to_snapshot_table_(DB::db_t(), Key::?RT:key()) -> NewDB::db_t().
copy_value_to_snapshot_table_(State = {DB, Subscr, {SnapTable, LiveLC, SnapLC}}, Key) ->
    NewSnapLC = case get_entry2_(State, Key) of
        {true, Entry}   ->
            {_, OldSnapEntry} = get_snapshot_entry_(State, db_entry:get_key(Entry)),
            ets:insert(SnapTable, Entry),
            TmpLC = db_entry:update_lockcount(OldSnapEntry, Entry, SnapLC),
            ?TRACE_SNAP("copy_value_to_snapshot_table: ~p~nfrom ~p to ~p~n~p",
                        [comm:this(), SnapLC, TmpLC, Entry]),
            TmpLC;
        _ ->
            SnapLC
    end,
    {DB, Subscr, {SnapTable, LiveLC, NewSnapLC}}.

%% @doc Returns snapshot data as is
-spec get_snapshot_data_(DB::db_t()) -> db_as_list().
get_snapshot_data_({_DB, _Subscr, {SnapTable, _, _}}) ->
    case SnapTable of
        false -> [];
        _     -> ets:tab2list(SnapTable)
    end.

%% @doc Join snapshot and primary db such that all tuples in the primary db are replaced
%%      if there is a matching tuple available in the snapshot set. The other tuples are
%%      returned as is.
-spec join_snapshot_data_(DB::db_t()) -> db_as_list().
join_snapshot_data_(State) ->
    PrimaryDB = lists:keysort(1, get_data_(State)),
    SnapshotDB = lists:keysort(1, get_snapshot_data_(State)),
    join_snapshot_data_helper(SnapshotDB, PrimaryDB).

-spec join_snapshot_data_helper(SnapshotDB::db_as_list(), PrimaryDB::db_as_list())
        -> db_as_list().
join_snapshot_data_helper([], Result) -> Result;
join_snapshot_data_helper([{Key, _, _, _, _} = Tuple | More], List2) ->
    Newlist = lists:keyreplace(Key, 1, List2, Tuple), 
    join_snapshot_data_helper(More, Newlist).

-spec set_snapshot_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
set_snapshot_entry_(State = {DB, Subscr, {SnapTable, LiveLC, SnapLC}}, Entry) ->
    case db_entry:is_null(Entry) of
        true -> delete_snapshot_entry_(State, Entry);
        _    ->
            % if there is a snapshot entry for this key, we base our lock calculation on that,
            % if not, we have to consider the live db because of the copy-on-write logic
            NewSnapLC = case get_snapshot_entry_(State, db_entry:get_key(Entry)) of
                {true, OldEntry} ->
                    db_entry:update_lockcount(OldEntry, Entry, SnapLC);
                {false, _} ->
                    {_, LiveEntry} = get_entry2_(State, db_entry:get_key(Entry)),
                    db_entry:update_lockcount(LiveEntry, Entry, SnapLC)
            end,
            ?TRACE_SNAP("set_snapshot_entry: ~p~n~p~n~p", 
                        [comm:this(), NewSnapLC, Entry]),
            ets:insert(SnapTable, Entry),
            {DB, Subscr, {SnapTable, LiveLC, NewSnapLC}}
    end.


-spec get_snapshot_entry_(DB::db_t(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.
get_snapshot_entry_({_DB, _Subscr, {SnapTable, _LiveLC, _SnapLC}}, Key) ->
    case ets:lookup(SnapTable, Key) of
        [Entry] -> {true, Entry};
        []      -> {false, db_entry:new(Key)}
    end.

-spec delete_snapshot_entry_at_key_(DB::db_t(), Key::?RT:key()) -> NewDB::db_t().
delete_snapshot_entry_at_key_(State = {DB, Subscr, {SnapTable, LiveLC, SnapLC}}, Key) ->
    case get_snapshot_entry_(State, Key) of
        {true, OldEntry} ->
            NewSnapLC = db_entry:update_lockcount(OldEntry, db_entry:new(Key), SnapLC);
        {false, _} ->
            {_, LiveEntry} = get_entry2_(State, Key),
            NewSnapLC = db_entry:update_lockcount(LiveEntry, db_entry:new(Key), SnapLC)
    end,
    ets:delete(SnapTable, Key),
    {DB, Subscr, {SnapTable, LiveLC, NewSnapLC}}.

%% @doc Removes all values with the given entry's key from the Snapshot DB.
-spec delete_snapshot_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
delete_snapshot_entry_(State, Entry) ->
    Key = db_entry:get_key(Entry),
    delete_snapshot_entry_at_key_(State, Key).

-spec init_snapshot_(DB::db_t()) -> NewDB::db_t().
init_snapshot_({DB, Subscr, {SnapTable, LiveLC, _SnapLC}}) ->
    case SnapTable of
        false -> ok;
        _     -> ets:delete(SnapTable)
    end,
    SnapDBName = "db_" ++ randoms:getRandomString() ++ ":snapshot",
    % copy live db lock count to new snapshot db
    {DB, Subscr, {ets:new(list_to_atom(SnapDBName), [ordered_set, private]), LiveLC, LiveLC}}.

-spec snapshot_is_lockfree_(DB::db_t()) -> boolean().
snapshot_is_lockfree_({_DB, _Subscr, {_SnapTable, _LiveLC, SnapLC}}) ->
    SnapLC =:= 0.

-spec decrease_snapshot_lockcount_(DB::db_t()) -> NewDB::db_t().
decrease_snapshot_lockcount_({DB, Subscr, {SnapTable, LiveLC, SnapLC}}) ->
    {DB, Subscr, {SnapTable, LiveLC, SnapLC - 1}}.

-spec snapshot_is_running_(DB::db_t()) -> boolean().
snapshot_is_running_({_DB, _Subscr, {SnapTable, _LiveLC, _SnapLC}}) ->
    case SnapTable of
        false -> false;
        _     -> true
    end.

-spec delete_snapshot_(DB::db_t()) -> NewDB::db_t().
delete_snapshot_({_DB, _Subscr, {false, _LiveLC, _SnapLC}} = State) ->
    State;
delete_snapshot_({DB, Subscr, {SnapTable, LiveLC, _SnapLC}}) ->
    ets:delete(SnapTable),
    {DB, Subscr, {false, LiveLC, 0}}.

%% End snapshot-related functions
