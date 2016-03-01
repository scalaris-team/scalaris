% @copyright 2009-2015 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Nico Kruber <kruber@zib.de>
%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    DB back-end for DHTs storing db_entry:entry() objects.
%% @end
%% @version $Id$
-module(db_dht).
-author('schintke@zib.de').
-author('kruber@zib.de').
-author('fajerski@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("client_types.hrl").

%% -define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
%% -define(TRACE_SNAP(X, Y), ct:pal(X, Y)).
%% -define(TRACE_SNAP(X, Y), ?TRACE(X, Y)).
-define(TRACE_SNAP(X, Y), ok).
%% -define(TRACE_CHUNK(X, Y), ct:pal(X, Y)).
-define(TRACE_CHUNK(X, Y), ok).
-define(DB, (config:read(db_backend))). %% DB backend
-define(CKETS, ets). %% changed keys database

%% whole DB management
-export([new/1, close/1, close_and_delete/1]).
-export([get_load/1, get_load/2]).
-export([get_data/1, get_data/3]).

%% raw whole db_entry operations
-export([get_entry/2, set_entry/2, set_entry/4]).
-export([delete_entry/2, delete_entry_at_key/2]).
-export([update_entry/2]).

%% access as used in tx and quorum reads (value-version-based)
-export([read/2, write/4, delete/2]).

%% bulk entry operations on intervals or filter funs for replica
%% repair and local use (for data slide)
-export([get_chunk/4, get_chunk/6]).

%% slide: slide DB (see dht_node_state.erl)
-export([get_split_key/5]).
-export([split_data/2, add_data/2]).
-export([get_entries/2, get_entries/3]).

%% slide: delta recording (see dht_node_state.erl)
-export([record_changes/2]).
-export([stop_record_changes/1, stop_record_changes/2]).
-export([get_changes/1, get_changes/2]).
-export([delete_entries/2]).

%% slide: slide snapshot data (see dht_node_state.erl)
-export([snapshot_is_running/1]).
-export([add_snapshot_data/2]).
-export([get_snapshot_data/2]).

%% subscriptions to DB changes (called locally by delta recording)
%%-export([set_subscription/2, get_subscription/2, remove_subscription/2]).

%% snapshots: business logic
-export([init_snapshot/1]).
-export([delete_snapshot/1]).
-export([join_snapshot_data/1]).
-export([snapshot_is_lockfree/1]).
-export([get_live_lc/1, get_snap_lc/1]).

%% debugging, diagnostic outputs
-export([get_snapshot_data/1]).
-export([set_snapshot_entry/2, get_snapshot_entry/2]).
-export([delete_snapshot_entry/2, delete_snapshot_entry_at_key/2]).

%% for unittests
-export([check_db/1]).
-export([copy_value_to_snapshot_table/2]).
-export([update_entries/4]).


-type db() :: {KeyValueDB  :: term(),
               Subscribers :: db_ets:db(), %% for delta recording
               SnaphotInfo :: {term() | false,
                               LiveLockCount :: non_neg_integer(),
                               SnapLockCount :: non_neg_integer()}}.
-type version() :: client_version().
-type value() :: rdht_tx:encoded_value().
-type db_as_list() :: [db_entry:entry()].

-type subscr_op_t() :: {write, db_entry:entry()} | {delete | split, ?RT:key()}.
-type subscr_changes_fun_t() :: fun((DB::db(), Tag::any(), Operation::subscr_op_t()) -> db()).
-type subscr_remove_fun_t() :: fun((Tag::any()) -> any()).
-type subscr_t() :: {Tag::any(), intervals:interval(), ChangesFun::subscr_changes_fun_t(), CloseDBFun::subscr_remove_fun_t()}.


-export_type([db/0, value/0, version/0, db_as_list/0]).

%%%%%%
%%% whole DB management
%%%%%%

%% @doc Initializes a new database.
-spec new(nonempty_string() | atom()) -> db().
new(DBName) ->
    DBNameNew = db_util:get_name(DBName),
    SubscrName = db_util:get_subscriber_name(DBNameNew),
    {?DB:new(DBNameNew), db_ets:new(SubscrName), {false, 0, 0}}.

%% @doc Closes the given DB (it may be recoverable using open/1 depending on
%%      the DB back-end).
-spec close(db()) -> true.
close(State) ->
    close(State, close).

%% @doc Closes the given DB and deletes all contents (this DB can thus not be
%%      re-opened using open/1).
-spec close_and_delete(db()) -> true.
close_and_delete(State) ->
    close(State, close_and_delete).

%% @doc Helper for close/1 and close_and_delete/1.
-spec close(db(), CloseFn::close | close_and_delete) -> true.
close({KVStore, Subscribers, {SnapDB, _LLC, _SLC}} = State, CloseFn) ->
    _ = call_subscribers(State, close_db),
    ?DB:CloseFn(KVStore),
    db_ets:CloseFn(Subscribers),
    case SnapDB of
        false ->
            ok;
        ETSTable ->
            ?DB:CloseFn(ETSTable)
    end.

%% @doc Returns the number of stored keys.
-spec get_load(DB::db()) -> Load::non_neg_integer().
get_load({DB, _Subscr, _SnapState}) ->
    ?DB:get_load(DB).

%% @doc Returns the number of stored keys in the given interval.
-spec get_load(DB::db(), Interval::intervals:interval()) -> Load::integer().
get_load(State, [all]) -> get_load(State);
get_load(_State, []) -> 0;
get_load({DB, _Subscr, _Snap}, Interval) ->
    ?DB:foldl(
            DB,
            fun(Key, AccIn) ->
                case intervals:in(Key, Interval) of
                    true -> AccIn + 1;
                    _ -> AccIn
                end
            end, 0).

%% @doc Returns all (including empty, but not null) DB entries.
-spec get_data(DB::db()) -> db_as_list().
get_data(State) ->
    get_data(State, fun(_) -> true end, fun(E) -> E end).

-spec get_data(DB::db(), 
               FilterFun::fun((db_entry:entry()) -> boolean()),
               ValueFun::fun((db_entry:entry()) -> V)) 
        -> [V].
get_data({DB, _Subscr, _Snap}, FilterFun, ValueFun) ->
    FoldlFun = fun(Entry, Acc) ->
                       case FilterFun(Entry) of
                            true -> [ValueFun(Entry) | Acc];
                            _  -> Acc
                       end
               end,
    ?DB:foldl_unordered(DB, FoldlFun, []).

%%%%%%
%%% raw whole db_entry operations
%%%%%%

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
-spec get_entry(db(), Key::?RT:key()) -> db_entry:entry().
get_entry({KVStore, _Subscr, _Snap}, Key) ->
    %% report read to process rrd and check for report to monitor
    lb_stats:update_db_monitor(db_reads, Key),
    case ?DB:get(KVStore, Key) of
        {} ->
            db_entry:new(Key);
        Entry ->
            Entry
    end.

-spec set_entry(db(), Entry::db_entry:entry()) -> db().
set_entry(DB, Entry) ->
    set_entry(DB, Entry, 1, 2).

-spec set_entry(db(), Entry::db_entry:entry(), non_neg_integer(),
                 non_neg_integer()) -> db().
set_entry(State, Entry, TLogSnapNo, OwnSnapNo) ->
    %% report write to process rrd and check for report to monitor
    lb_stats:update_db_monitor(db_writes, db_entry:get_key(Entry)),
    case db_entry:is_null(Entry) of
        true ->
            delete_entry(State, Entry);
        _ ->
            %% do lockcounting and copy-on-write logic
            {KVStore, Subscr, Snap} = snaps(State, Entry, TLogSnapNo, OwnSnapNo),
            %% set actual entry in DB
            call_subscribers({?DB:put(KVStore, Entry), Subscr, Snap},
                             {write, Entry})
    end.

%% @doc Removes all values with the given entry's key from the DB.
-spec delete_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
delete_entry(State, Entry) ->
    delete_entry_at_key(State, db_entry:get_key(Entry)).

-spec delete_entry_at_key(DB::db(), ?RT:key()) -> NewDB::db().
delete_entry_at_key(State, Key) ->
    delete_entry_at_key(State, Key, delete).

%% @doc Removes all values with the given key from the DB with specified reason.
-spec delete_entry_at_key(DB::db(), ?RT:key(), delete | split) ->  NewDB::db().
delete_entry_at_key({DB, Subscr, {Snap, LiveLC, SnapLC}} = State,  Key, Reason) ->
    %% TODO count locks
    OldEntry = get_entry(State, Key),
    Delta = -db_entry:lockcount(OldEntry),
    NewLiveLC = LiveLC + Delta,
    NewSnapLC = case Snap of
        false -> SnapLC;
        _ -> SnapLC + Delta
    end,
    %% @doc removes all entries from the DB that correspond to Key
    call_subscribers({?DB:delete(DB, Key), Subscr, {Snap, NewLiveLC, NewSnapLC}}, {Reason, Key}).

%% @doc Updates an existing (!) entry in the DB.
%% TODO: necessary?
-spec update_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
update_entry(DB, Entry) ->
    set_entry(DB, Entry).


%%%%%%
%%% DB access as used in tx and quorum reads (value-version-based)
%%%%%%

%% @doc Reads the version and value of a key.
-spec read(DB::db(), Key::?RT:key()) ->
         {ok, Value::value(), Version::version()} | {ok, empty_val, -1}.
read(DB, Key) ->
    DBEntry = get_entry(DB, Key),
    {ok, db_entry:get_value(DBEntry), db_entry:get_version(DBEntry)}.

%% @doc Updates the value of the given key.
-spec write(DB::db(), Key::?RT:key(), Value::value(), Version::version()) ->
         NewDB::db().
write(DB, Key, Value, Version) ->
    DBEntry = get_entry(DB, Key),
    case db_entry:is_null(DBEntry) of
        true ->
            NewEntry = db_entry:new(Key, Value, Version),
            set_entry(DB, NewEntry);
        _ ->
            NewEntry = db_entry:set_value(DBEntry, Value, Version),
            update_entry(DB, NewEntry)
    end.

%% @doc Deletes the key. Returns {DB, undef} if the key does not exist in the
%%      DB, {DB, locks_set} if read or write locks are still set and {DB, ok}
%%      if the operation was successfully performed.
-spec delete(DB::db(), Key::?RT:key()) ->
         {NewDB::db(), Status::ok | locks_set | undef}.
delete(DB, Key) ->
    DBEntry = get_entry(DB, Key),
    case db_entry:is_null(DBEntry) of
        true ->
            {DB, undef};
        _ ->
            case db_entry:is_locked(DBEntry) of
                false ->
                    {delete_entry(DB, DBEntry), ok};
                _ ->
                    {DB, locks_set}
            end
    end.

%% @doc Returns all key-value pairs of the given DB which are in the given
%%      interval but at most ChunkSize elements.
%%      Assumes the ets-table is an ordered_set,
%%      may return data from "both ends" of the DB-range if the interval is
%%      ""wrapping around", i.e. its begin is larger than its end.
%%      Returns the chunk and the remaining interval for which the DB may still
%%      have data (a subset of I).
%%      Precond: Interval is a subset of the range of the dht_node and continuous!
-spec get_chunk(DB::db(), StartId::?RT:key(), Interval::intervals:interval(), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), db_as_list()}.
get_chunk(DB, StartId, Interval, ChunkSize) ->
    get_chunk(DB, StartId, Interval, fun(_) -> true end, fun(E) -> E end, ChunkSize).

-spec get_chunk(DB::db(), StartId::?RT:key(), Interval::intervals:interval(),
                FilterFun::fun((db_entry:entry()) -> boolean()),
                ValueFun::fun((db_entry:entry()) -> V),
                ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [V]}.
get_chunk(_State, _StartId, [], _FilterFun, _ValueFun, _ChunkSize) ->
    {intervals:empty(), []};
get_chunk(State, StartId, Interval, FilterFun, ValueFun, all) ->
    {_Next, Chunk} =
        get_chunk(State, StartId, Interval, FilterFun, ValueFun, get_load(State)),
    {intervals:empty(), Chunk};


%%%%%%
%% bulk entry operations on intervals or filter funs for replica
%% repair and local use (for data slide)
%%%%%%

get_chunk({DB, _Subscr, _Snap}, StartId, Interval, FilterFun, ValueFun, ChunkSize) ->
    %% split intervals in a way so that the first simple interval of After
    %% either contains StartId or is the closest following after StartId
    ?TRACE_CHUNK("get_chunk:~nStartID: ~p~nInterval:~p~nChunksize: ~p~n",
                 [StartId, Interval, ChunkSize]),
    %% rotate and split intervals so that StartId is the first element looked at
    {Before, After} = lists:splitwith(
            fun(all) -> false;
               ({Key}) ->
                    StartId > Key;
               ({_LBr, _L, R, ']'}) ->
                StartId > R;
               ({_LBr, _L, R, ')'}) ->
                StartId >= R
            end, intervals:get_simple_intervals(Interval)),
    ?TRACE_CHUNK("split: ~p~n~p~n", [Before, After]),
    RotatedInterval = case After of
        [] -> Before;
        [all] ->
            [{'[', StartId, ?PLUS_INFINITY, ')'},
             {'[', ?MINUS_INFINITY, StartId, ')'}];
        [{_K} | _Rest] ->
            After ++ Before;
        [{LBr, L, R, RBr} | Tail] ->
            case intervals:in(StartId, intervals:new(LBr, L, R, RBr)) of
                true ->
                    lists:append([[{'[', StartId, R, RBr}],
                                  Tail, Before,
                                  [{LBr, L, StartId, ')'}]]);
                _ ->
                    After ++ Before
            end
    end,
    AddDataFun = fun(Key, {Acc, RemainingChunkSize}) ->
                         Entry = ?DB:get(DB, Key),
                         case FilterFun(Entry) of
                             true -> {[Entry | Acc],
                                      RemainingChunkSize - 1};
                             _    -> {Acc, RemainingChunkSize}
                         end
                 end,
    ?TRACE_CHUNK("get_chunk2: asking db:foldl to look in those intervals ~p~n",
                 [RotatedInterval]),
    {Chunk, Remaining} = lists:foldl(
        fun(I, {Acc, RemainingChunkSize}) ->
            ?DB:foldl(DB, AddDataFun, {Acc, RemainingChunkSize}, I,
                      RemainingChunkSize)
        end, {[], ChunkSize}, RotatedInterval),
    %% calculate the leftover interval and return
    ?TRACE_CHUNK("StartId: ~p~nInterval: ~p~nChunkSize: ~p~nChunk: ~p~nRemaining: ~p~nOpen: ~p~n",
           [StartId, Interval, ChunkSize, Chunk, Remaining, calc_remaining_interval(StartId, Remaining, Chunk, Interval)]),
    {calc_remaining_interval(StartId, Remaining, Chunk, Interval),
     lists:foldl(
            fun(E, AccIn) ->
                [ValueFun(E) | AccIn] end,
            [], Chunk)}.

%%%%%%
%%% slide: slide DB (see dht_node_state.erl)
%%%%%%

%% @doc Returns the key that would remove not more than TargetLoad entries
%%      from the DB when starting at the key directly after Begin in case of
%%      forward searches and directly at Begin in case of backward searches,
%%      respectively.
%%      Precond: a load larger than 0
%%      Note: similar to get_chunk/2.
-spec get_split_key(DB::db(), Begin::?RT:key(), End::?RT:key(), TargetLoad::pos_integer(), forward | backward)
        -> {?RT:key(), TakenLoad::non_neg_integer()}.
get_split_key({DB, _Subscr, _Snap}, Begin, End, TargetLoad, forward)
        when Begin > End ->
    %% when Begin and End wrap around do two folds
    {Key, Taken1} = ?DB:foldl(DB,
              fun(E, {_El, Taken}) -> {E, Taken + 1} end,
              {End, 0},
              {'(', Begin, ?PLUS_INFINITY, ')'},
              TargetLoad),
    Split = ?DB:foldl(DB,
              fun(E, {_El, Taken}) -> {E, Taken + 1} end,
              {Key, Taken1},
              {'[', ?MINUS_INFINITY, End, ']'},
              TargetLoad - Taken1),
    normalize_split_key(Split, TargetLoad, End);
get_split_key({DB, _Subscr, _Snap}, Begin, End, TargetLoad, forward) ->
    Split = ?DB:foldl(DB,
              fun(E, {_El, Taken}) -> {E, Taken + 1} end,
              {End, 0},
              {'(', Begin, End, ']'},
              TargetLoad),
    normalize_split_key(Split, TargetLoad, End);

get_split_key({DB, _Subscr, _Snap}, Begin, End, TargetLoad, backward)
        when Begin < End ->
    %% when Begin and End wrap around do two folds
    {Key, Taken1} = ?DB:foldr(DB,
              fun(E, {_El, Taken}) -> {E, Taken + 1} end,
              {End, 0},
              {'[', ?MINUS_INFINITY, Begin, ']'},
              TargetLoad + 1),
    ?TRACE("first fold done~nnew target:~p~nstart: ~p~nend: ~p~nacc: ~p",
           [TargetLoad - Taken1, ?PLUS_INFINITY, End, {Key, Taken1}]),
    Split = ?DB:foldr(DB,
              fun(E, {_El, Taken}) -> {E, Taken + 1} end,
              {Key, Taken1},
              {'(', End, ?PLUS_INFINITY, ')'},
              TargetLoad - Taken1 + 1),
    normalize_split_key_b(Split, TargetLoad, End);
get_split_key({DB, _Subscr, _Snap}, Begin, End, TargetLoad, backward) ->
    Split = ?DB:foldr(DB,
              fun(E, {_El, Taken}) -> {E, Taken + 1} end,
              {End, 0},
              {'(', End, Begin, ']'},
              TargetLoad + 1),
    normalize_split_key_b(Split, TargetLoad, End).

normalize_split_key({_Key, TakenLoad}, TargetLoad, End)
  when TakenLoad < TargetLoad ->
    {End, TakenLoad};
normalize_split_key({Key, TakenLoad}, _TargetLoad, _End) ->
    {Key, TakenLoad}.

normalize_split_key_b({Key, TakenLoad}, TargetLoad, End)
  when TakenLoad > TargetLoad ->
    normalize_split_key({Key, TakenLoad - 1}, TargetLoad, End);
normalize_split_key_b({_Key, TakenLoad}, TargetLoad, End)
  when TakenLoad == TargetLoad ->
    normalize_split_key({End, TakenLoad}, TargetLoad, End);
normalize_split_key_b(Split, TargetLoad, End) ->
    normalize_split_key(Split, TargetLoad, End).


%% @doc Splits the database into a database (first element) which contains all
%% keys in MyNewInterval and a list of the other values (second element).
%% Note: removes all keys not in MyNewInterval from the list of changed
%% keys!
-spec split_data(DB::db(), MyNewInterval::intervals:interval()) -> {NewDB::db(), db_as_list()}.
split_data(State = {DB, _Subscr, _SnapState}, MyNewInterval) ->
    F = fun (Key, {StateAcc, HisList}) ->
            DBEntry = ?DB:get(DB, Key),
            case intervals:in(Key, MyNewInterval) of
                true -> {StateAcc, HisList};
                _ -> NewHisList = case db_entry:is_empty(DBEntry) of
                                      false -> [DBEntry | HisList];
                                      _ -> HisList
                                  end,
                    {delete_entry_at_key(StateAcc, Key, split), NewHisList}
            end
    end,
    ?DB:foldl(DB, F, {State, []}).

%% @doc Adds all db_entry objects in the Data list.
-spec add_data(DB::db(), db_as_list()) -> NewDB::db().
add_data(DB, Data) ->
    lists:foldl(fun(Entry, DBAcc) ->
                        set_entry(DBAcc, Entry)
                    end, DB, Data).

%% @doc Gets (non-empty) db_entry objects in the given range.
-spec get_entries(DB::db(), Range::intervals:interval()) -> db_as_list().
get_entries(State, Interval) ->
    {Elements, RestInterval} = intervals:get_elements(Interval),
    case intervals:is_empty(RestInterval) of
        true ->
            [E || Key <- Elements, not db_entry:is_empty(E = get_entry(State, Key))];
        _ ->
            {_, Data} =
                get_chunk(State, ?RT:hash_key("0"), % any key will work, here!
                           Interval,
                           fun(DBEntry) -> not db_entry:is_empty(DBEntry) end,
                           fun(E) -> E end, all),
            Data
    end.

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%% which FilterFun returns true.
%% TODO only for legacy compatability; get_chunk should be used
-spec get_entries(DB::db(),
                  FilterFun::fun((DBEntry::db_entry:entry()) ->
                                        boolean()),
                  ValueFun::fun((DBEntry::db_entry:entry())
                                -> Value)) -> [Value].
get_entries(State, FilterFun, ValueFun) ->
    element(2, get_chunk(State, ?RT:hash_key("0"),
                         intervals:all(), FilterFun, ValueFun, all)).


%%%%%%
%%% slide: delta recording (see dht_node_state.erl)
%%%%%%

%% @doc Adds the new interval to the interval to record changes for. Entries
%%      which have (potentially) changed can then be gathered by get_changes/1.
-spec record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
record_changes(State, NewInterval) ->
    RecChanges = get_subscription(State, record_changes),
    ?TRACE("Old Subscription is ~p~n", [RecChanges]),
    NewSubscr =
        case RecChanges of
            [] -> {record_changes, NewInterval,
                   fun subscr_delta/3, fun subscr_delta_close_table/1};
            [{Tag, I, ChangesFun, RemSubscrFun}] ->
                {Tag, intervals:union(I, NewInterval), ChangesFun, RemSubscrFun}
        end,
    ?TRACE("setting new subscription ~p~n", [NewSubscr]),
    set_subscription(State, NewSubscr).

%% @doc Stops recording changes and removes all entries from the table of
%%      changed keys.
-spec stop_record_changes(OldDB::db()) -> NewDB::db().
stop_record_changes(State) ->
    remove_subscription(State, record_changes).

%% @doc Stops recording changes in the given interval and removes all such
%%      entries from the table of changed keys.
-spec stop_record_changes(OldDB::db(), intervals:interval()) -> NewDB::db().
stop_record_changes(State, Interval) ->
    RecChanges = get_subscription(State, record_changes),
    case RecChanges of
        [] -> State;
        [{Tag, I, ChangesFun, RemSubscrFun}] ->
            subscr_delta_remove(State, Interval),
            NewI = intervals:minus(I, Interval),
            case intervals:is_empty(NewI) of
                true -> remove_subscription(State, Tag);
                _ -> set_subscription(State, {Tag, NewI, ChangesFun, RemSubscrFun})
            end
    end.

%% @doc Gets all db_entry objects which have (potentially) been changed or
%%      deleted (might return objects that have not changed but have been
%%      touched by one of the DB setters).
-spec get_changes(DB::db()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes(State) ->
    get_changes(State, intervals:all()).

%% @doc Gets all db_entry objects in the given interval which have
%%      (potentially) been changed or deleted (might return objects that have
%%      not changed but have been touched by one of the DB setters).
-spec get_changes(DB::db(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes(State, Interval) ->
    case erlang:get('$delta_tab') of
        undefined -> get_changes_helper(State, [], Interval, [], []);
        CKDB -> get_changes_helper(State, ?CKETS:tab2list(CKDB), Interval, [], [])
    end.

%% @doc Helper for get_changes/2 that adds the entry of a changed key either to
%%      the list of changed entries or to the list of deleted entries.
-spec get_changes_helper(State::db(), ChangedKeys::[{?RT:key()}],
        Interval::intervals:interval(), ChangedEntries::[db_entry:entry()],
        DeletedKeys::[?RT:key()])
            -> {ChangedEntries::[db_entry:entry()], DeletedKeys::[?RT:key()]}.
get_changes_helper(_State, [], _Interval, ChangedEntries, DeletedKeys) ->
    {ChangedEntries, DeletedKeys};
get_changes_helper(State, [{CurKey} | RestKeys], Interval, ChangedEntries, DeletedKeys) ->
    case intervals:in(CurKey, Interval) of
        true ->
            Entry = get_entry(State, CurKey),
            case db_entry:is_null(Entry) of
                false -> ?TRACE("~p get_changes: ~p was changed~n", [self(), CurKey]),
                    get_changes_helper(State, RestKeys, Interval, [Entry | ChangedEntries], DeletedKeys);
                _    -> ?TRACE("~p get_changes: ~p was deleted~n", [self(), CurKey]),
                    get_changes_helper(State, RestKeys, Interval, ChangedEntries, [CurKey | DeletedKeys])
            end;
        _ -> ?TRACE("~p get_changes: key ~p is not in ~p~n", [self(), CurKey, Interval]),
            get_changes_helper(State, RestKeys, Interval, ChangedEntries, DeletedKeys)
    end.

%% @doc Inserts/removes the key into the table of changed keys depending on the
%%      operation (called whenever the DB is changed).
-spec subscr_delta(State::db(), Tag::any(), Operation::subscr_op_t()) -> db().
subscr_delta(State, _Tag, Operation) ->
    CKDB = subscr_delta_check_table(State),
    ?TRACE("subscr_delta is called for op ~p~n", [Operation]),
    case Operation of
        {write, Entry} -> ?CKETS:insert(CKDB, {db_entry:get_key(Entry)});
        {delete, Key}  -> ?CKETS:insert(CKDB, {Key});
        {split, Key}   -> ?CKETS:delete(CKDB, Key)
    end,
    State.

%% @doc Cleans up, i.e. deletes, the table with changed keys (called on
%%      subscription removal).
-spec subscr_delta_close_table(Tag::any()) -> ok | true.
subscr_delta_close_table(_Tag) ->
    case erlang:erase('$delta_tab') of
        undefined -> ok;
        CKDB -> ?CKETS:delete(CKDB)
    end.

%% @doc Check that the table storing changed keys exists and create it if
%%      necessary.
-spec subscr_delta_check_table(State::db()) -> ets:tid() | atom().
subscr_delta_check_table(_State) ->
    DeltaDB = case erlang:get('$delta_tab') of
        undefined ->
            CKDB = ?CKETS:new(dht_node_db_ck, [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
            erlang:put('$delta_tab', CKDB),
            CKDB;
        CKDB -> CKDB
    end,
    DeltaDB.

%% @doc Removes any changed key in interval I (called when some (sub-)interval
%%      is unsubscribed).
-spec subscr_delta_remove(State::db(), I::intervals:interval()) -> ok.
subscr_delta_remove(State, Interval) ->
    CKDB = subscr_delta_check_table(State),
    F = fun(DBEntry, _) ->
                Key = db_entry:get_key(DBEntry),
                case intervals:in(Key, Interval) of
                    true -> ?CKETS:delete(CKDB, Key);
                    _    -> true
                end
        end,
    ?CKETS:foldl(F, true, CKDB),
    ok.

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
-spec delete_entries(DB::db(),
                     RangeOrFun::intervals:interval() |
                                 fun((DBEntry::db_entry:entry()) -> boolean()))
        -> NewDB::db().
delete_entries(State = {DB, _Subscr, _SnapState}, FilterFun)
  when is_function(FilterFun) ->
    F = fun(Key, StateAcc) ->
                DBEntry = ?DB:get(DB, Key),
                case FilterFun(DBEntry) of
                    false -> StateAcc;
                    _     -> delete_entry(StateAcc, DBEntry)
                end
        end,
    ?DB:foldl(DB, F, State);
delete_entries({DB, _Subscr, _SnapState} = State, Interval) ->
    {Elements, RestInterval} = intervals:get_elements(Interval),
    case intervals:is_empty(RestInterval) of
        true ->
            lists:foldl(fun(Key, State1) ->
                                delete_entry_at_key(State1, Key)
                        end, State, Elements);
        _ ->
            F = fun(Key, StateAcc) ->
                        DBEntry = ?DB:get(DB, Key),
                        delete_entry(StateAcc, DBEntry)
                end,
            SimpleI = intervals:get_simple_intervals(Interval),
            lists:foldl(fun(I, AccIn) ->
                                ?DB:foldl(DB, F, AccIn, I)
                        end, State, SimpleI)
    end.

%%%%%%
%%% end slide: delta recording (see dht_node_state.erl)
%%%%%%


%%%%%%
%%% slide: slide snapshot data (see dht_node_state.erl)
%%%%%%

-spec snapshot_is_running(DB::db()) -> boolean().
snapshot_is_running({_DB, _Subscr, {SnapTable, _LiveLC, _SnapLC}}) ->
    case SnapTable of
        false -> false;
        _     -> true
    end.


-spec add_snapshot_data(DB::db(), db_as_list()) -> NewDB::db().
add_snapshot_data(State, Entries) ->
    lists:foldl(
        fun(Entry, StateAcc) ->
                set_snapshot_entry(StateAcc, Entry)
        end, State, Entries).

%% @doc Returns snapshot data as is for a specific interval
-spec get_snapshot_data(DB::db(), intervals:interval()) -> db_as_list().
get_snapshot_data({_DB, _Subscr, {false, _, _}}, _Interval) ->
    [];
get_snapshot_data({_DB, _Subscr, {SnapTable, _, _}}, [all]) ->
    ?DB:foldl(SnapTable, fun(K, AccIn) -> [?DB:get(SnapTable, K) | AccIn] end, []);
get_snapshot_data({_DB, _Subscr, {SnapTable, _, _}}, Interval) ->
    %% TODO usort is only to make test suite happy since it thinks [all, all] is
    %% a sensible interval and double entries where returned
    lists:usort(fun({K1,_,_,_,_}, {K2,_,_,_,_}) -> K1 =< K2 end,
                    lists:foldl(
        fun(I, Acc) ->
            ?DB:foldl(SnapTable,
                      fun(Key, AccIn) ->
                            Entry = ?DB:get(SnapTable, Key),
                            ?TRACE("get_snapshot_data: adding ~p to data",
                                   [Entry]),
                            [Entry | AccIn]
                      end,
                      Acc,
                      I)
                    end, [], intervals:get_simple_intervals(Interval))).


%%%%%%
%%% subscriptions to DB changes (called locally by delta recording)
%%%%%%

%% @doc Adds a subscription for the given interval under Tag (overwrites an
%%     existing subscription with that tag).
-spec set_subscription(State::db(), subscr_t()) -> db().
set_subscription({DB, Subscr, SnapState}, Subscription) ->
    {DB, db_ets:put(Subscr, Subscription), SnapState}.

%% @doc Gets a subscription stored under Tag (empty list if there is none).
-spec get_subscription(State::db(), Tag::any()) -> [subscr_t()].
get_subscription({_DB, Subscr, _SnapState}, Tag) ->
    case db_ets:get(Subscr, Tag) of
        {} ->
            [];
        SubsT ->
            [SubsT]
    end.

%% @doc Removes a subscription stored under Tag (if there is one).
-spec remove_subscription(State::db(), Tag::any()) -> db().
remove_subscription({DB, Subscr, SnapState}, Tag) ->
    case db_ets:get(Subscr, Tag) of
        {} -> ok;
        {Tag, _I, _ChangesFun, RemSubscrFun} -> RemSubscrFun(Tag)
    end,
    {DB, db_ets:delete(Subscr, Tag), SnapState}.

%% @doc Go through all subscriptions and perform the given operation if
%%      matching.
-spec call_subscribers(State::db(), Operation::close_db | subscr_op_t()) -> db().
call_subscribers(State = {_DB, Subscr, _SnapState}, Operation) ->
    {NewState, _Op} = db_ets:foldl(Subscr,
              fun call_subscribers_iter/2,
              {State, Operation}),
    NewState.

%% @doc Iterates over all susbcribers and calls their subscribed functions.
-spec call_subscribers_iter(subscr_t(), {State::db(), Operation::close_db |
                                         subscr_op_t()}) -> {db(),
                                                             Operation::close_db
                                                             | subscr_op_t()}.
call_subscribers_iter(Tag, {{_DB, Subscr, _SnapState} = State, Op}) ->
    % assume the key exists (it should since we are iterating over the table!)
    {Tag, I, ChangesFun, RemSubscrFun} = db_ets:get(Subscr, Tag),
    NewState =
        case Op of
            close_db ->
                RemSubscrFun(Tag),
                State;
            Operation ->
                Key = case Operation of
                    {write, Entry} -> db_entry:get_key(Entry);
                    {delete, K}  -> K;
                    {split, K}  -> K
                end,
                case intervals:in(Key, I) of
                    false ->
                        ?TRACE("not calling subscribers...~p not in interval ~p~n",
                               [Key, I]),
                        State;
                    _     ->
                        ?TRACE("calling subscriber for tag ~p and op ~p~n", [Tag,
                                                                             Operation]),
                        ChangesFun(State, Tag, Operation)
                end
        end,
    {NewState, Op}.



%%%%%%
%%% snapshots: business logic
%%%%%%

-spec init_snapshot(DB::db()) -> NewDB::db().
init_snapshot({DB, Subscr, {SnapTable, LiveLC, _SnapLC}}) ->
    case SnapTable of
        false -> ok;
        _     -> ?DB:close(SnapTable)
    end,
    SnapDBName = "db_" ++ randoms:getRandomString() ++ ":snapshot",
    % copy live db lock count to new snapshot db
    {DB, Subscr, {?DB:new(SnapDBName), LiveLC, LiveLC}}.

-spec delete_snapshot(DB::db()) -> NewDB::db().
delete_snapshot({_DB, _Subscr, {false, _LiveLC, _SnapLC}} = State) ->
    State;
delete_snapshot({DB, Subscr, {SnapTable, LiveLC, _SnapLC}}) ->
    ?DB:close(SnapTable),
    {DB, Subscr, {false, LiveLC, 0}}.

%% @doc Join snapshot and primary db such that all tuples in the
%%      primary db are replaced if there is a matching tuple available
%%      in the snapshot set. The other tuples are returned as is.
-spec join_snapshot_data(DB::db()) -> db_as_list().
join_snapshot_data(State) ->
    PrimaryDB = lists:keysort(1, get_entries(State, intervals:all())),
    SnapshotDB = lists:keysort(1, get_snapshot_data(State)),
    join_snapshot_data_helper(SnapshotDB, PrimaryDB).

-spec join_snapshot_data_helper(SnapshotDB::db_as_list(),
                                PrimaryDB::db_as_list())
                               -> db_as_list().
join_snapshot_data_helper([], Result) -> Result;
join_snapshot_data_helper([{Key, _, _, _, _} = Tuple | More], List2) ->
    Newlist = lists:keyreplace(Key, 1, List2, Tuple),
    join_snapshot_data_helper(More, Newlist).

-spec snapshot_is_lockfree(DB::db()) -> boolean().
snapshot_is_lockfree({_DB, _Subscr, {_SnapTable, _LiveLC, SnapLC}}) ->
    SnapLC =:= 0.

-spec get_live_lc(DB::db()) -> non_neg_integer().
get_live_lc({_DB, _Subscr, {_SnapTable, LiveLC, _SnapLC}}) ->
    LiveLC.

-spec get_snap_lc(DB::db()) -> non_neg_integer().
get_snap_lc({_DB, _Subscr, {_SnapTable, _LiveLC, SnapLC}}) ->
    SnapLC.

%%-spec decrease_snapshot_lockcount(DB::db()) -> NewDB::db().
%%decrease_snapshot_lockcount({DB, Subscr, {SnapTable, LiveLC, SnapLC}}) ->
%%    {DB, Subscr, {SnapTable, LiveLC, SnapLC - 1}}.

%% do all the necessary things to maintain possibly running
%% snapshots. called in set_entry()
snaps({DB, Subscr, {false, LiveLC, SnapLC}} = State, Entry, _OpSnapNum,
      _OwnSnapNo) ->
    %% no snapshot running, just update the live lockcount
    LC_new = db_entry:lockcount(Entry),
    % NOTE: only retrieve OldEntry if needed
    LC_old =
        if LC_new =:= 0 andalso LiveLC =:= 0 andalso SnapLC =:= 0 ->
               0; % must be 0, otherwise DB is in erroneous state!
           true ->
               db_entry:lockcount(get_entry(State, db_entry:get_key(Entry)))
        end,
    {DB, Subscr, {false, LiveLC + LC_new - LC_old, SnapLC}};
snaps({DB, Subscr, {SnapDB, LiveLC, SnapLC}} = State, Entry, OpSnapNum,
      OwnSnapNo) when OpSnapNum >= OwnSnapNo ->
    OldEntry = get_entry(State, db_entry:get_key(Entry)),
    %% This case hints at the validate phase of a
    %% transaction. Lockcounts should only increase and in this case
    %% we should do copy-on-write. There is a special case to handle:
    %% when a write transaction is aborted but we voted prepare we
    %% need to correct the lockcounts.
    case db_entry:lockcount(Entry) - db_entry:lockcount(OldEntry) of
        Delta when Delta < 0 ->
            %% check if LiveLC and SnapLC are > 1 in old db in this
            %% case a transaction got through validation just before a
            %% new snapshot begun on this node.  On other nodes the
            %% snapshots was triggered before the transaction...hence
            %% the abort.  lockcount needs to be decreased so
            %% snapshots can advance
            if LiveLC > 0 andalso SnapLC >= LiveLC ->
                    %% in case the tx was validated before new
                    %% snapshot we need to decrease the copied
                    %% lockcount
                    ?TRACE_SNAP("db:snaps ~p
                                snapnumbers not ok but snaplocks exist
                                ~p   ~p~n~p",
                                [self(), OpSnapNum, OwnSnapNo,
                                 {DB, Subscr, {SnapDB, LiveLC + Delta, SnapLC +
                                               Delta}}]),
                    {DB, Subscr, {SnapDB, LiveLC + Delta, SnapLC + Delta}};
                true ->
                    ?TRACE_SNAP("db:snaps ~p
                    snapnumbers not ok but locks are ok
                                ~p   ~p~n~p~n~p",
                                [self(), OpSnapNum,
                                 OwnSnapNo, DB, {DB, Subscr, {SnapDB, LiveLC +
                                                              Delta, SnapLC +
                                                              Delta}}]),
                    {DB, Subscr, {SnapDB, LiveLC + Delta, SnapLC}}
            end;
        0 ->
            %% not sure if this can happen....but it just does nothing
            State;
        Delta ->
            %% new transaction validated. this one should not belong to the
            %% snapshot so do copy-on-write
            copy_value_to_snapshot_table({DB, Subscr, {SnapDB, LiveLC + Delta,
                                                        SnapLC}}, db_entry:get_key(Entry))

    end;
snaps({DB, Subscr, {SnapDB, LiveLC, SnapLC}} = State, Entry, OpSnapNum,
      OwnSnapNo) when OpSnapNum < OwnSnapNo ->
    OldEntry = get_entry(State, db_entry:get_key(Entry)),
    %% this case hints at commit or abort of a transaction that belongs into the
    %% snapshot. Since this is the finishing phase of the transaction locks
    %% should decrease. If locks increase log a warning since thsi should not
    %% happen,
    case db_entry:lockcount(Entry) - db_entry:lockcount(OldEntry) of
        Delta when Delta < 0 ->
            SnapEntry = get_snapshot_entry(State, db_entry:get_key(Entry)),
            case db_entry:is_null(SnapEntry) of
                false ->
                    ?TRACE_SNAP("db:snaps:~p~nkey in snapdb...reducing lockcount",
                                [self()]),
                    % in this case there was an entry with this key in the snapshot table
                    % so it might have different locks than the one in the live db.
                    % we're applying the lock decrease on the snapshot table entry
                    set_snapshot_entry({DB, Subscr, {SnapDB, LiveLC + Delta,
                                                         SnapLC}}, Entry);
                _ ->
                    ?TRACE_SNAP("db:snaps ~p~nkey not in snapdb~n~p",
                                [self(), db_entry:get_key(Entry)]),
                    % key was not found in snapshot table -> both dbs are in sync for this key
                    {DB, Subscr, {SnapDB, LiveLC + Delta, SnapLC + Delta}}
            end;
        0 ->
            %% not sure if this can happen....but it just does nothing
            State;
        _Delta ->
            log:log(warn, "db_common:snaps(): ~p~nlockcount increase but op has old
                    snapnumber...should not happen~p  ~p~n~p~n~p~n~p",
                    [self(), OpSnapNum, OwnSnapNo, OldEntry, Entry, State]),
            State
    end.


%%%%%%
%%% debugging, diagnostic outputs
%%%%%%

%% @doc Returns snapshot data as is for whole interval
-spec get_snapshot_data(DB::db()) -> db_as_list().
get_snapshot_data(DB) ->
    get_snapshot_data(DB, intervals:all()).

-spec set_snapshot_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
set_snapshot_entry(State = {DB, Subscr, {SnapTable, LiveLC, SnapLC}}, Entry) ->
    case db_entry:is_null(Entry) of
        true -> delete_snapshot_entry(State, Entry);
        _    ->
            % if there is a snapshot entry for this key, we base our lock calculation on that,
            % if not, we have to consider the live db because of the copy-on-write logic
            OldEntry = get_snapshot_entry(State, db_entry:get_key(Entry)),
            NewSnapLC = case db_entry:is_null(OldEntry) of
                false ->
                    SnapLC + db_entry:lockcount(Entry) - db_entry:lockcount(OldEntry);
                _ ->
                    LiveEntry = get_entry(State, db_entry:get_key(Entry)),
                    SnapLC + db_entry:lockcount(Entry) - db_entry:lockcount(LiveEntry)
            end,
            ?TRACE_SNAP("set_snapshot_entry: ~p~n~p~n~p",
                        [self(), NewSnapLC, Entry]),
            {DB, Subscr, {?DB:put(SnapTable, Entry), LiveLC, NewSnapLC}}
    end.

-spec get_snapshot_entry(DB::db(), Key::?RT:key()) -> db_entry:entry().
get_snapshot_entry({_DB, _Subscr, {SnapTable, _LiveLC, _SnapLC}}, Key) ->
    case ?DB:get(SnapTable, Key) of
        {} ->
            db_entry:new(Key);
        Entry ->
            Entry
    end.

%% @doc Removes all values with the given entry's key from the Snapshot DB.
-spec delete_snapshot_entry(DB::db(), Entry::db_entry:entry()) -> NewDB::db().
delete_snapshot_entry(State, Entry) ->
    Key = db_entry:get_key(Entry),
    delete_snapshot_entry_at_key(State, Key).

-spec delete_snapshot_entry_at_key(DB::db(), Key::?RT:key()) -> NewDB::db().
delete_snapshot_entry_at_key(State = {DB, Subscr, {SnapTable, LiveLC, SnapLC}}, Key) ->
    OldEntry = get_snapshot_entry(State, Key),
    NewSnapLC = case db_entry:is_null(OldEntry) of
        false ->
            SnapLC - db_entry:lockcount(OldEntry);
        _ ->
            LiveEntry = get_entry(State, Key),
            SnapLC - db_entry:lockcount(LiveEntry)
    end,
    ?TRACE("deleting key ~p", [Key]),
    {DB, Subscr, {?DB:delete(SnapTable, Key), LiveLC, NewSnapLC}}.


%%%%%%
%%% for unittests
%%%%%%

%% @doc Checks whether all entries in the DB are valid, i.e.
%%      - no writelocks and readlocks at the same time
%%      - no empty_val values (these should only be in the DB temporarily)
%%      - version is greater than or equal to 0
%%      Returns the result of the check and a list of invalid entries.
%%      Used in unittests.
-spec check_db(DB::db()) -> {true, []} | {false, InvalidEntries::db_as_list()}.
check_db({DB, _Subscr, _Snap}) ->
    Data = ?DB:foldl(DB, fun(K, A) -> [?DB:get(DB, K) | A] end, []),
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

%% @doc Copy existing entry to snapshot table
-spec copy_value_to_snapshot_table(DB::db(), Key::?RT:key()) -> NewDB::db().
copy_value_to_snapshot_table(State = {DB, Subscr, {SnapTable, LiveLC, SnapLC}}, Key) ->
    Entry = get_entry(State, Key),
    NewSnap = case db_entry:is_null(Entry) of
        false   ->
            OldSnapEntry = get_snapshot_entry(State, db_entry:get_key(Entry)),
            ?TRACE_SNAP("copy_value_to_snapshot_table: ~p~nfrom ~p to ~p~n~p",
                        [self(), SnapLC, TmpLC, Entry]),
            {?DB:put(SnapTable, Entry), LiveLC, SnapLC +
             db_entry:lockcount(Entry) - db_entry:lockcount(OldSnapEntry)};
        _ ->
            {SnapTable, LiveLC, SnapLC}
    end,
    {DB, Subscr, NewSnap}.

%% @doc Updates all (existing or non-existing) non-locked entries from
%%      NewEntries for which Pred(OldEntry, NewEntry) returns true with
%%      UpdateFun(OldEntry, NewEntry).
-spec update_entries(DB::db(),
                     Values::[db_entry:entry()],
                     Pred::fun((OldEntry::db_entry:entry(),
                                NewEntry::db_entry:entry()) -> boolean()),
                     UpdateFun::fun((OldEntry::db_entry:entry(),
                                     NewEntry::db_entry:entry()) ->
                                           UpdatedEntry::db_entry:entry()))
                    -> NewDB::db().
update_entries(OldDB, NewEntries, Pred, UpdateFun) ->
    F = fun(NewEntry, DB) ->
                OldEntry = get_entry(DB, db_entry:get_key(NewEntry)),
                IsNotLocked = not db_entry:is_locked(OldEntry),
                IsUpdatable = IsNotLocked andalso Pred(OldEntry, NewEntry),
                case db_entry:is_null(OldEntry) of
                    true when IsUpdatable ->
                        set_entry(DB, UpdateFun(OldEntry, NewEntry));
                    _ when IsUpdatable ->
                        update_entry(DB, UpdateFun(OldEntry, NewEntry));
                    _ ->
                        DB
                end
        end,
    lists:foldl(F, OldDB, NewEntries).


%%%%%%
%%% locally used helper functions to long to place inside
%%% corresponding context block
%%%%%%

-spec calc_remaining_interval(?RT:key(), non_neg_integer(), db_as_list(),
                              intervals:interval()) -> intervals:interval().
%% if there are less elements in Chunk that ChunkSize allows, the whole interval
%% was covered
calc_remaining_interval(_StartId, Remaining, _Chunk, _Interval)
        when Remaining > 0 -> intervals:empty();
%% if Chunk is empty the whole Interval was covered
calc_remaining_interval(_StartId, _Remaining, [], _Interval) ->
    intervals:empty();
calc_remaining_interval(StartId, _Remaining, Chunk, Interval) ->
    %% the interval covered by chunk is either the biggest key left of startid
    %% or if there are no keys left of startid simply the biggest key in chunk
    Last = calc_last_key_rem_int(Chunk, StartId),
    intervals:minus(Interval, intervals:new('[', StartId, Last, ']')).

%% @doc Gets the largest key in Chunk left of StartId if there is one,
%%      otherwise gets the largest key of all items.
-spec calc_last_key_rem_int(Chunk::[db_entry:entry(),...], StartId::?RT:key()) -> ?RT:key().
calc_last_key_rem_int([{Key, _, _, _, _} | Rest], StartId) ->
    calc_last_key_rem_int(Rest, StartId, Key, Key < StartId).

%% @doc Helper for calc_last_key_rem_int/2.
-spec calc_last_key_rem_int(db_as_list(), StartId::?RT:key(), Max::?RT:key(),
                            OnlySmallerThanStart::boolean()) -> ?RT:key().
calc_last_key_rem_int([], _StartId, Max, _OnlySmallerThanStart) -> Max;
calc_last_key_rem_int([{Key, _, _, _, _} | Rest], StartId, Max, true) when Key < StartId ->
    calc_last_key_rem_int(Rest, StartId, ?IIF(Key > Max, Key, Max), true);
calc_last_key_rem_int([_ | Rest], StartId, Max, true) ->
    calc_last_key_rem_int(Rest, StartId, Max, true);
calc_last_key_rem_int([{Key, _, _, _, _} | Rest], StartId, _Max, false) when Key < StartId ->
    calc_last_key_rem_int(Rest, StartId, Key, true);
calc_last_key_rem_int([{Key, _, _, _, _} | Rest], StartId, Max, false) ->
    calc_last_key_rem_int(Rest, StartId, ?IIF(Key > Max, Key, Max), false).


