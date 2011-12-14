% @copyright 2009-2011 Zuse Institute Berlin,
%            2009 onScale solutions

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc    In-process database using ets
%% @end
%% @version $Id$
-module(db_ets).
-author('schintke@onscale.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).
-type db_t() :: {Table::tid() | atom(), SubscrTable::tid() | atom()}.

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-define(CKETS, ets).

-include("db_common.hrl").

%% @doc Initializes a new database.
new_() ->
    % ets prefix: DB_ + random name
    RandomName = randoms:getRandomString(),
    DBName = "db_" ++ RandomName,
    SubscrName = DBName ++ ":subscribers",
    % better protected? All accesses would have to go to DB-process
    {ets:new(list_to_atom(DBName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
     ets:new(list_to_atom(SubscrName), [ordered_set, private])}.

%% @doc Re-opens a previously existing database (not supported by ets
%%      -> create new DB).
open_(_FileName) ->
    log:log(warn, "[ Node ~w:db_ets ] open/1 not supported, executing new/0 instead", [self()]),
    new().

%% @doc Closes and deletes the DB.
close_(State = {DB, Subscr}, _Delete) ->
    _ = call_subscribers(State, close_db),
    ets:delete(DB),
    ets:delete(Subscr).

%% @doc Returns the name of the table for open/1.
get_name_({DB, _Subscr}) ->
    erlang:atom_to_list(ets:info(DB, name)).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2_({DB, _Subscr}, Key) ->
    case ets:lookup(DB, Key) of
        [Entry] -> {true, Entry};
        []      -> {false, db_entry:new(Key)}
    end.

%% @doc Inserts a complete entry into the DB.
%%      Note: is the Entry is a null entry, it will be deleted!
set_entry_(State = {DB, _Subscr}, Entry) ->
    case db_entry:is_null(Entry) of
        true -> delete_entry_(State, Entry);
        _    -> ets:insert(DB, Entry),
                call_subscribers(State, {write, Entry})
    end.

%% @doc Updates an existing (!) entry in the DB.
%%      TODO: use ets:update_element here?
update_entry_(State, Entry) ->
    set_entry_(State, Entry).

%% @doc Removes all values with the given key from the DB.
delete_entry_at_key_(State = {DB, _Subscr}, Key) ->
    ets:delete(DB, Key),
    call_subscribers(State, {delete, Key}).

%% @doc Returns the number of stored keys.
get_load_({DB, _Subscr}) ->
    ets:info(DB, size).

%% @doc Returns the number of stored keys in the given interval.
get_load_(State = {DB, _Subscr}, Interval) ->
    IsEmpty = intervals:is_empty(Interval),
    IsAll = intervals:is_all(Interval),
    if
        IsEmpty -> 0;
        IsAll   -> get_load_(State);
        true    ->
            F = fun(DBEntry, Load) ->
                        case intervals:in(db_entry:get_key(DBEntry), Interval) of
                            true -> Load + 1;
                            _    -> Load
                        end
                end,
            ets:foldl(F, 0, DB)
    end.

%% @doc adds keys
add_data_(State = {DB, _Subscr}, Data) ->
    ets:insert(DB, Data),
    _ = lists:foldl(fun(Entry, _) ->
                        call_subscribers(State, {write, Entry})
                    end, ok, Data),
    State.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
%%      Note: removes all keys not in MyNewInterval from the list of changed
%%      keys!
split_data_(State = {DB, _Subscr}, MyNewInterval) ->
    F = fun (DBEntry, HisList) ->
                Key = db_entry:get_key(DBEntry),
                case intervals:in(Key, MyNewInterval) of
                    true -> HisList;
                    _    -> ets:delete(DB, Key),
                            _ = call_subscribers(State, {split, Key}),
                            case db_entry:is_empty(DBEntry) of
                                false -> [DBEntry | HisList];
                                _     -> HisList
                            end
                end
        end,
    HisList = ets:foldl(F, [], DB),
    {State, HisList}.

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%%      which FilterFun returns true.
get_entries_({DB, _Subscr}, FilterFun, ValueFun) ->
    F = fun (DBEntry, Data) ->
                 case FilterFun(DBEntry) of
                     true -> [ValueFun(DBEntry) | Data];
                     _    -> Data
                 end
        end,
    ets:foldl(F, [], DB).

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
delete_entries_(State = {DB, _Subscr}, FilterFun) when is_function(FilterFun) ->
    F = fun(DBEntry, _) ->
                case FilterFun(DBEntry) of
                    false -> ok;
                    _     -> Key = db_entry:get_key(DBEntry),
                             ets:delete(DB, Key),
                             _ = call_subscribers(State, {delete, Key}),
                             ok
                end
        end,
    ets:foldl(F, ok, DB),
    State;
delete_entries_(State, Interval) ->
    {Elements, RestInterval} = intervals:get_elements(Interval),
    case intervals:is_empty(RestInterval) of
        true ->
            lists:foldl(fun(Key, State1) -> delete_entry_at_key_(State1, Key) end, State, Elements);
        _ ->
            delete_entries_(State,
                            fun(E) -> intervals:in(db_entry:get_key(E), Interval) end)
    end.

%% @doc Returns all DB entries.
get_data_({DB, _Subscr}) ->
    ets:tab2list(DB).

get_chunk_(DB, Interval, FilterFun, ValueFun, all) ->
    get_chunk_(DB, Interval, FilterFun, ValueFun, get_load_(DB));
get_chunk_(DB, Interval, FilterFun, ValueFun, ChunkSize) ->
    AddDataFun = fun(DB1, Key, Data) ->
                         DBEntry = get_entry_(DB1, Key),
                         case FilterFun(DBEntry) of
                             true -> [ValueFun(DBEntry) | Data];
                             _    -> []
                         end
                 end,
    get_chunk_helper(DB, Interval, AddDataFun, ChunkSize).

get_chunk_helper({ETSDB, _Subscr} = DB, Interval, AddDataFun, ChunkSize) ->
    % assert ChunkSize > 0, see ChunkSize type
    case get_load_(DB) of
        0 -> {intervals:empty(), []};
        _ ->
            %ct:pal("0: ~p ~p", [intervals:get_bounds(Interval), Interval]),
            {_BeginBr, Begin, End, EndBr} = intervals:get_bounds(Interval),
            % get first key which is in the interval and in the ets table:
            case first_key_in_interval(ETSDB, Begin, Interval) of
                '$end_of_table' ->
                    {intervals:empty(), []};
                FirstKey ->
                    %ct:pal("first key: ~.0p~n", [FirstKey]),
                    Chunk0 = AddDataFun(DB, FirstKey, []),
                    {Next, Chunk} =
                        get_chunk_inner(DB, ets:next(ETSDB, FirstKey), FirstKey,
                                        Interval, AddDataFun, ChunkSize - 1, Chunk0),
                    case Next of
                        '$end_of_table' ->
                            {intervals:empty(), Chunk};
                        '$end_of_interval' ->
                            {intervals:empty(), Chunk};
                        _ ->
                            case intervals:in(Next, Interval) of
                                false ->
                                    {intervals:empty(), Chunk};
                                _ ->
                                    {intervals:new('[', Next, End, EndBr), Chunk}
                            end
                    end
            end
    end.

%% @doc Find first key in range (assume a continuous interval), start at
%%      Current which does not have to exist in the table.
-spec first_key_in_interval(
        DB::tid() | atom(), Next::?RT:key(), Interval::intervals:interval())
            -> ?RT:key() | '$end_of_table'.
first_key_in_interval(ETSDB, Current, Interval) ->
    case intervals:in(Current, Interval) andalso ets:member(ETSDB, Current) of
        true -> Current;
        _    ->
            Next = case ets:next(ETSDB, Current) of
                       '$end_of_table' -> ets:first(ETSDB);
                       X               -> X
                   end,
            case intervals:in(Next, Interval) of
                true -> Next;
                _    -> '$end_of_table' % found nothing in range
            end
    end.

%% @doc inner loop for get_chunk
%% pre: Current is in ets table, ets table is not empty
-spec get_chunk_inner(DB::db_t(), Current::?RT:key() | '$end_of_table',
                      RealStart::?RT:key(), Interval::intervals:interval(),
                      AddDataFun::fun((DB::db_t(), Key::?RT:key(), [V]) -> [V]),
                      ChunkSize::pos_integer(), Chunk::[V])
        -> {?RT:key() | '$end_of_table' | '$end_of_interval', [V]}.
get_chunk_inner(_DB, RealStart, RealStart, _Interval, _AddDataFun, _ChunkSize, Chunk) ->
    %ct:pal("inner: 0: ~p", [RealStart]),
    % we hit the start element, i.e. our whole data set has been traversed
    {'$end_of_interval', Chunk};
get_chunk_inner(_DB, Current, _RealStart, _Interval, _AddDataFun, 0, Chunk) ->
    %ct:pal("inner: 1: ~p", [Current]),
    % we hit the chunk size limit
    {Current, Chunk};
get_chunk_inner({ETSDB, _Subscr} = DB, '$end_of_table', RealStart, Interval, AddDataFun, ChunkSize, Chunk) ->
    %ct:pal("inner: 2: ~p", ['$end_of_table']),
    % reached end of table - start at beginning (may be a wrapping interval)
    get_chunk_inner(DB, ets:first(ETSDB), RealStart, Interval, AddDataFun, ChunkSize, Chunk);
get_chunk_inner({ETSDB, _Subscr} = DB, Current, RealStart, Interval, AddDataFun, ChunkSize, Chunk) ->
    %ct:pal("inner: 3: ~p", [Current]),
    case intervals:in(Current, Interval) of
        true ->
            Chunk1 = AddDataFun(DB, Current, Chunk),
            Next = ets:next(ETSDB, Current),
            get_chunk_inner(DB, Next, RealStart, Interval, AddDataFun, ChunkSize - 1, Chunk1);
        _ ->
            {'$end_of_interval', Chunk}
    end.

%% @doc Returns the key that would remove not more than TargetLoad entries
%%      from the DB when starting at the key directly after Begin.
%%      Precond: a load larger than 0
%%      Note: similar to get_chunk/2.
get_split_key_(DB, Begin, TargetLoad, forward) ->
    get_split_key_(DB, Begin, TargetLoad, fun ets:first/1, fun ets:next/2);
get_split_key_(DB, Begin, TargetLoad, backward) ->
    get_split_key_(DB, Begin, TargetLoad, fun ets:last/1, fun ets:prev/2).

-spec get_split_key_(DB::db_t(), Begin::?RT:key(), TargetLoad::pos_integer(),
        ETS_first::fun((DB::tid() | atom()) -> ?RT:key() | '$end_of_table'),
        ETS_next::fun((DB::tid() | atom(), Key::?RT:key()) -> ?RT:key() | '$end_of_table'))
        -> {?RT:key(), TakenLoad::pos_integer()}.
get_split_key_({ETSDB, _Subscr} = DB, Begin, TargetLoad,
               ETS_first, ETS_next) ->
    % assert ChunkSize > 0, see ChunkSize type
    case get_load_(DB) of
        0 -> throw('empty_db');
        _ ->
            % get first key in the ets table which is larger than Begin:
            case first_key(ETSDB, Begin, ETS_first, ETS_next) of
                '$end_of_table' ->
                    throw('empty_db');
                FirstKey ->
                    %ct:pal("first key: ~.0p~n", [FirstKey]),
                    {SplitKey, RestLoad} =
                        get_split_key_inner(DB, ETS_next(ETSDB, FirstKey),
                                            FirstKey, TargetLoad - 1, FirstKey,
                                            ETS_first, ETS_next),
                    {SplitKey, TargetLoad - RestLoad}
            end
    end.

%% @doc Find the first key in the DB which is larger/smaller than Key.
%%      Note: Key does not have to exist in the table.
-spec first_key(DB::tid() | atom(), Key::?RT:key(),
        ETS_first::fun((DB::tid() | atom()) -> ?RT:key() | '$end_of_table'),
        ETS_next::fun((DB::tid() | atom(), Key::?RT:key()) -> ?RT:key() | '$end_of_table'))
            -> ?RT:key() | '$end_of_table'.
first_key(ETSDB, Key, ETS_first, ETS_next) ->
    case ETS_next(ETSDB, Key) of
        '$end_of_table' -> ETS_first(ETSDB);
        X               -> X
    end.

%% @doc inner loop for get_split_key
%% pre: Current is in ets table, ets table is not empty
-spec get_split_key_inner(
        DB::db_t(), Current::?RT:key() | '$end_of_table',
        RealStart::?RT:key(), TargetLoad::pos_integer(), SplitKey::?RT:key(),
        ETS_first::fun((DB::tid() | atom()) -> ?RT:key() | '$end_of_table'),
        ETS_next::fun((DB::tid() | atom(), Key::?RT:key()) -> ?RT:key() | '$end_of_table'))
            -> {?RT:key(), TakenLoad::pos_integer()}.
get_split_key_inner(_DB, RealStart, RealStart, TargetLoad, SplitKey, _ETS_first, _ETS_next) ->
    % we hit the start element, i.e. our whole data set has been traversed
    {SplitKey, TargetLoad};
get_split_key_inner(_DB, _Current, _RealStart, 0, SplitKey, _ETS_first, _ETS_next) ->
    % we hit the chunk size limit
    {SplitKey, 0};
get_split_key_inner({ETSDB, _Subscr} = DB, '$end_of_table', RealStart, TargetLoad, SplitKey, ETS_first, ETS_next) ->
    % reached end of table - start at beginning (may be a wrapping interval)
    get_split_key_inner(DB, ETS_first(ETSDB), RealStart, TargetLoad, SplitKey, ETS_first, ETS_next);
get_split_key_inner({ETSDB, _Subscr} = DB, Current, RealStart, TargetLoad, _SplitKey, ETS_first, ETS_next) ->
    Next = ETS_next(ETSDB, Current),
    get_split_key_inner(DB, Next, RealStart, TargetLoad - 1, Current, ETS_first, ETS_next).

delete_chunk_(DB, Interval, ChunkSize) ->
    AddDataFun = fun(_DB1, Key, Data) -> [Key | Data] end,
    {Next, Chunk} = get_chunk_helper(DB, Interval, AddDataFun, ChunkSize),
    DB2 = lists:foldl(fun(Key, DB1) -> delete_entry_at_key_(DB1, Key) end, DB, Chunk),
    {Next, DB2}.
