% @copyright 2013-2016 Zuse Institute Berlin,

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
%% @doc    DB back-end for DHT nodes storing prbr objects.
%% @end
%% @version $Id$
-module(db_prbr).
-author('schintke@zib.de').
-author('kruber@zib.de').
-author('fajerski@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

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
-export([check_config/0]).
-export([new/1, open/1]).
-export([close/1, close_and_delete/1]).
-export([get_load/1, get_load/2]).
-export([tab2list/1]).
-export([get_chunk2/7]).

%% raw whole db entry operations
-export([get/2]).
-export([set/2]).

-export([add_data/2]).
-export([get_entries/2]).

%% slide: delta recording (see dht_node_state.erl)
-export([record_changes/2]).
-export([stop_record_changes/2]).
-export([get_changes/2]).
-export([delete_entries/2]).
-export([delete_entry/2]).

-type db() :: {KeyValueDB  :: term(),
               Subscribers :: db_ets:db(), %% for delta recording
               SnaphotInfo :: {term() | false,
                               LiveLockCount :: non_neg_integer(),
                               SnapLockCount :: non_neg_integer()}}.
-type version() :: non_neg_integer().
-type value() :: rdht_tx:encoded_value().
-type entry() :: tuple(). %% first element must be the key().
-type db_as_list() :: [entry()].

-type subscr_op_t() :: {write, entry()} | {delete | split, ?RT:key()}.
-type subscr_changes_fun_t() :: fun((DB::db(), Tag::any(), Operation::subscr_op_t()) -> db()).
-type subscr_remove_fun_t() :: fun((Tag::any()) -> any()).
-type subscr_t() :: {Tag::any(), intervals:interval(), ChangesFun::subscr_changes_fun_t(), CloseDBFun::subscr_remove_fun_t()}.


-export_type([db/0, value/0, version/0, db_as_list/0]).

%%%%%%
%%% whole DB management
%%%%%%

%% @doc Initializes a new database.
-spec new(nonempty_string() | atom() | tuple()) -> db().
new(DBName) ->
  DBNameNew = db_util:get_name(DBName),
  SubscrName = db_util:get_subscriber_name(DBNameNew),
  {?DB:new(DBNameNew), db_ets:new(SubscrName), {false, 0, 0}}.

%% @doc Re-opens an existing database.
-spec open(DB::nonempty_string()) -> db().
open(DBName) ->
  DB = ?DB:open(DBName),
  SubscrName = ?DB:get_name(DB) ++ ":subscribers",
  {DB, db_ets:new(SubscrName), {false, 0, 0}}.

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
-spec get_load(DB::db()) -> Load::integer().
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

-spec tab2list(Table_name::db()) -> [Entries::entry()].
tab2list({DB, _Subscr, _Snap}) ->
    ?DB:tab2list(DB).

%%%%%%
%%% raw whole db entry operations
%%%%%%

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
-spec get(db(), Key::?RT:key()) -> entry() | {}.
get({KVStore, _Subscr, _Snap}, Key) ->
    ?DB:get(KVStore, Key).

-spec set(db(), entry()) -> db().
set(DB, Entry) ->
    set_entry(DB, Entry, 1, 2).

-spec set_entry(db(), entry(), non_neg_integer(),
                 non_neg_integer()) -> db().
set_entry(State, Entry, _TLogSnapNo, _OwnSnapNo) ->
    {KVStore, Subscr, Snap} = State,
    %% set actual entry in DB
    NewDB = {?DB:put(KVStore, Entry), Subscr, Snap},
    call_subscribers(NewDB, {write, Entry}).

%% @doc Removes all values with the given entry's key from the DB.
-spec delete_entry(DB::db(), entry()) -> NewDB::db().
delete_entry(State, Entry) ->
    delete_entry_at_key(State, entry_key(Entry)).

-spec delete_entry_at_key(DB::db(), ?RT:key()) -> NewDB::db().
delete_entry_at_key(State, Key) ->
    delete_entry_at_key(State, Key, delete).

%% @doc Removes all values with the given key from the DB with specified reason.
-spec delete_entry_at_key(DB::db(), ?RT:key(), delete | split) ->  NewDB::db().
delete_entry_at_key({DB, Subscr, {Snap, LiveLC, SnapLC}} = State,  Key, Reason) ->
    %% TODO count locks
    _OldEntry = get(State, Key),
    %% @doc removes all entries from the DB that correspond to Key
    call_subscribers({?DB:delete(DB, Key), Subscr, {Snap, LiveLC, SnapLC}}, {Reason, Key}).



%% @doc Returns all key-value pairs of the given DB which are in the given
%%      interval but at most ChunkSize elements.
%%      Assumes the ets-table is an ordered_set,
%%      may return data from "both ends" of the DB-range if the interval is
%%      ""wrapping around", i.e. its begin is larger than its end.
%%      Returns the chunk and the remaining interval for which the DB may still
%%      have data (a subset of I).
%%      Precond: Interval is a subset of the range of the dht_node and continuous!
-spec get_chunk(DB::db(), StartId::?RT:key(), Interval::intervals:interval(),
                FilterFun::fun((entry()) -> boolean()),
                ValueFun::fun((entry()) -> V),
                ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [V]}.
get_chunk(State, StartId, Interval, FilterFun, ValueFun, ChunkSize) ->
    case intervals:is_empty(Interval) of
        true -> {intervals:empty(), []};
        false ->
            case config:read(db_prbr_chunker) of
                db_prbr ->
                    get_chunk2(State, StartId, Interval,
                               FilterFun, ValueFun, ChunkSize);
                Module -> Module:get_chunk2(State, StartId, Interval,
                                            FilterFun, ValueFun, ChunkSize)
            end
    end.

%% @doc Helper for get_chunk/6.
-spec get_chunk2(DB::db(), StartId::?RT:key(), Interval::intervals:interval(),
                FilterFun::fun((entry()) -> boolean()),
                ValueFun::fun((entry()) -> V),
                ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [V]}.
get_chunk2(State, StartId, Interval, FilterFun, ValueFun, all) ->
    {_Next, Chunk} =
        get_chunk2(State, StartId, Interval, FilterFun, ValueFun, get_load(State)),
    {intervals:empty(), Chunk};

get_chunk2({DB, Subscr, Snap}, StartId, Interval, FilterFun, ValueFun, ChunkSize) ->
    AddDataFun = fun(Key, {Acc, RemainingChunkSize}) ->
                     Entry = ?DB:get(DB, Key),
                     case FilterFun(Entry) of
                         true -> {[Entry | Acc],
                                  RemainingChunkSize - 1};
                         _    -> {Acc, RemainingChunkSize}
                     end
             end,
    get_chunk2({DB, Subscr, Snap}, StartId, Interval, FilterFun, ValueFun, AddDataFun, ChunkSize).

%%%%%%
%% bulk entry operations on intervals or filter funs for replica
%% repair and local use (for data slide)
%%%%%%

-spec get_chunk2(DB::db(), StartId::?RT:key(), Interval::intervals:interval(),
                 FilterFun::fun((entry()) -> boolean()),
                 ValueFun::fun((entry()) -> V),
                 AddDataFun::fun((Key::?RT:key(), {Acc::[V], RemainingChunkSize::pos_integer()}) ->
                                        {Acc::[V], RemainingChunkSize::pos_integer()}),
                ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [V]}.
get_chunk2({DB, _Subscr, _Snap}, StartId, Interval, _FilterFun, ValueFun, AddDataFun, ChunkSize) ->
    %% split intervals in a way so that the first simple interval of After
    %% either contains StartId or is the closest following after StartId
    ?TRACE_CHUNK("get_chunk:~nStartID: ~p~nInterval:~p~nChunksize: ~p~n",
                 [StartId, Interval, ChunkSize]),
    %% rotate and split intervals so that StartId is the first element looked at
    {Before, After} = lists:splitwith(
            fun(all) -> false;
               ({Key}) ->
                    StartId > Key;
               ({_LBr, _L, R, _RBr}) ->
                StartId > R
            end, intervals:get_simple_intervals(Interval)),
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

%% @doc Adds all db entry objects in the Data list.
-spec add_data(DB::db(), db_as_list()) -> NewDB::db().
add_data(DB, Data) ->
    lists:foldl(fun(Entry, DBAcc) ->
                        case Entry of
                            {} -> DBAcc;
                            _ -> set(DBAcc, Entry)
                        end
                end, DB, Data).

%% @doc Gets (non-empty) db entry objects in the given range.
-spec get_entries(DB::db(), Range::intervals:interval()) -> db_as_list().
get_entries(State, Interval) ->
    {Elements, RestInterval} = intervals:get_elements(Interval),
    case intervals:is_empty(RestInterval) of
        true ->
            [E || Key <- Elements, {} =/= (E = get(State, Key))];
        _ ->
            {_, Data} =
                get_chunk(State, ?RT:hash_key("0"), % any key will work, here!
                           Interval,
                           fun(DBEntry) -> {} =/= DBEntry end,
                           fun(E) -> E end, all),
            Data
    end.


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


%% @doc Gets all db entries in the given interval which have
%% (potentially) been changed or deleted (might return objects that
%% have not changed but have been touched by one of the DB setters).
-spec get_changes(DB::db(), intervals:interval()) -> {Changed::db_as_list(), Deleted::[?RT:key()]}.
get_changes(State, Interval) ->
    case erlang:get('$delta_tab') of
        undefined -> get_changes_helper(State, [], Interval, [], []);
        CKDB -> get_changes_helper(State, ?CKETS:tab2list(CKDB), Interval, [], [])
    end.

%% @doc Helper for get_changes/2 that adds the entry of a changed key either to
%%      the list of changed entries or to the list of deleted entries.
-spec get_changes_helper(State::db(), ChangedKeys::[{?RT:key()}],
        Interval::intervals:interval(), ChangedEntries::[entry()],
        DeletedKeys::[?RT:key()])
            -> {ChangedEntries::[entry()], DeletedKeys::[?RT:key()]}.
get_changes_helper(_State, [], _Interval, ChangedEntries, DeletedKeys) ->
    {ChangedEntries, DeletedKeys};
get_changes_helper(State, [{CurKey} | RestKeys], Interval, ChangedEntries, DeletedKeys) ->
    case intervals:in(CurKey, Interval) of
        true ->
            Entry = get(State, CurKey),
            case Entry of
                {} ->
                    ?TRACE("~p get_changes: ~p was deleted~n", [self(), CurKey]),
                    get_changes_helper(State, RestKeys, Interval, ChangedEntries, [CurKey | DeletedKeys]);
                _ ->
                    ?TRACE("~p get_changes: ~p was changed~n", [self(), CurKey]),
                    get_changes_helper(State, RestKeys, Interval, [Entry | ChangedEntries], DeletedKeys)
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
        {write, Entry} -> ?CKETS:insert(CKDB, {entry_key(Entry)});
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

%% @doc Check that the table storing changed keys exists and creates it if
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
                Key = entry_key(DBEntry),
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
                                 fun((entry()) -> boolean()))
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
                        Entry = ?DB:get(DB, Key),
                        delete_entry(StateAcc, Entry)
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
-spec call_subscribers_iter(subscr_t(),
                            {State::db(), Operation::close_db | subscr_op_t()})
        -> {db(), Operation::close_db | subscr_op_t()}.
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
                    {write, Entry} -> entry_key(Entry);
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
%%% for unittests
%%%%%%

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
    ?TRACE_CHUNK("left: ~p~nright: ~p~ncalc_remaining:~n~p~nNext is ~p minus ~p",
                 [Left, Right, Chunk, Interval, intervals:new('[', StartId, Last, ']')]),
    intervals:minus(Interval, intervals:new('[', StartId, Last, ']')).

%% @doc Gets the largest key in Chunk left of StartId if there is one,
%%      otherwise gets the largest key of all items.
-spec calc_last_key_rem_int(Chunk::[entry(),...], StartId::?RT:key()) -> ?RT:key().
calc_last_key_rem_int([X | Rest], StartId) ->
    Key = entry_key(X),
    calc_last_key_rem_int(Rest, StartId, Key, Key < StartId).

%% @doc Helper for calc_last_key_rem_int/2.
-spec calc_last_key_rem_int(db_as_list(), StartId::?RT:key(), Max::?RT:key(),
                            OnlySmallerThanStart::boolean()) -> ?RT:key().
calc_last_key_rem_int([], _StartId, Max, _OnlySmallerThanStart) -> Max;
calc_last_key_rem_int([X | Rest], StartId, Max, true) ->
    Key = entry_key(X),
    case Key < StartId of
        true ->
            calc_last_key_rem_int(Rest, StartId,
                                  ?IIF(Key > Max, Key, Max), true);
        false ->
            calc_last_key_rem_int(Rest, StartId, Max, true)
    end;
calc_last_key_rem_int([X | Rest], StartId, Max, false) ->
    Key = entry_key(X),
    case Key < StartId of
        true ->
            calc_last_key_rem_int(Rest, StartId, Key, true);
        false ->
            calc_last_key_rem_int(Rest, StartId,
                                  ?IIF(Key > Max, Key, Max), false)
    end.

entry_key(E) -> element(1, E).

-spec check_config() -> boolean().
check_config() ->
    All_DBs = [db_ets, db_mnesia, db_toke, db_hanoidb, db_bitcask],
    Current_DB = config:read(db_backend),

    config:cfg_is_module(db_backend) and
    config:cfg_is_in(db_backend, All_DBs) and
    case Current_DB:is_available() of
        true ->
            true;
        Missing ->
            error_logger:error_msg("Modules ~p for selected DB backend ~p are missing.~n",
                                   [Missing, Current_DB]),
            false
    end and
    case config:read(ensure_recover) of
        true ->
            % ensure_recovery is enabled which means we must check if
            % leases are enabled and the chosen DB supports recovery
            case config:read(leases) of
                true ->
                    true;
                _ ->
                    error_logger:error_msg(
                      "Ensure_recover enabled but option leases not true "
                      "(see scalaris.cfg and scalaris.local.cfg).~n"
                      " Leases must be defined and set to true.~n"),
                    false
            end and
            case Current_DB:supports_feature(recover) of
                true ->
                   true;
                _ ->
                    % get DBs which have recovery and are installed
                    Rec_DBs = [DB || DB <- All_DBs,
                                     DB:supports_feature(recover),
                                     DB:is_available()],
                    error_logger:error_msg(
                      "Ensure_recover enabled but selected DB backend (~p) does not "
                      "support recovery.~n Try one of the following: ~p "
                      "(see scalaris.cfg and scalaris.local.cfg).~n",
                      [Current_DB, Rec_DBs]),
                    false
            end;
        _ ->
            % ensure recovery is not enabled and thus no checks are needed
            true
    end.
