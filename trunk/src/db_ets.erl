% @copyright 2009-2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
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
% TODO: make db() opaque again once dialyzer doesn't complain about get_db/1 anymore ("matching against tuple breaks opaqueness")
-type db_t() :: {Table::tid() | atom(), RecordChangesInterval::intervals:interval(), ChangedKeysTable::tid() | atom()}.

% TODO: move these functions to the behaviour and implement them in the other DBs!
-export([get_chunk/3, delete_chunk/3, get_split_key/4]).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").


-define(CKETS, ets).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initializes a new database.
new_() ->
    % ets prefix: DB_ + random name
    RandomName = randoms:getRandomId(),
    DBname = list_to_atom("db_" ++ RandomName),
    CKDBname = list_to_atom("db_ck_" ++ RandomName), % changed keys
    % better protected? All accesses would have to go to DB-process
    {ets:new(DBname, [ordered_set | ?DB_ETS_ADDITIONAL_OPS]), intervals:empty(), ?CKETS:new(CKDBname, [ordered_set | ?DB_ETS_ADDITIONAL_OPS])}.

%% @doc Re-opens a previously existing database (not supported by ets
%%      -> create new DB).
open_(_FileName) ->
    log:log(warn, "[ Node ~w:db_ets ] open/1 not supported, executing new/0 instead", [self()]),
    new().

%% @doc Closes and deletes the DB.
close_({DB, _CKInt, CKDB}, _Delete) ->
    ets:delete(DB),
    ?CKETS:delete(CKDB).

%% @doc Returns an empty string.
%%      Note: should return the name of the DB for open/1 which is not
%%      supported by ets though).
get_name_(_State) ->
    "".

%% @doc Returns all DB entries.
get_data_({DB, _CKInt, _CKDB}) ->
    ets:tab2list(DB).

%% @doc Returns all key-value pairs of the given DB which are in the given
%%      interval but at most ChunkSize elements.
%%      Assumes the ets-table is an ordered_set,
%%      may return data from "both ends" of the DB-range if the interval is
%%      ""wrapping around", i.e. its begin is larger than its end.
%%      Returns the chunk and the remaining interval for which the DB may still
%%      have data (a subset of I).
%%      Precond: Interval is a subset of the range of the dht_node and thus, in
%%      particular, continuous
-spec get_chunk(DB::db(), Interval::intervals:interval(), ChunkSize::pos_integer()) ->
    {intervals:interval(), db_as_list()}.
get_chunk(DB, Interval, ChunkSize) -> get_chunk_(DB, Interval, ChunkSize).

-spec get_chunk_(DB::db_t(), Interval::intervals:interval(), ChunkSize::pos_integer()) ->
    {intervals:interval(), db_as_list()}.
get_chunk_({ETSDB, _CKInt, _CKDB} = DB, Interval, ChunkSize) ->
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
                    {Next, Chunk} =
                        get_chunk_inner(DB, ets:next(ETSDB, FirstKey), FirstKey,
                                        Interval, ChunkSize - 1, [get_entry_(DB, FirstKey)]),
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
                      ChunkSize::pos_integer(), Chunk::db_as_list())
        -> {?RT:key() | '$end_of_table' | '$end_of_interval', db_as_list()}.
get_chunk_inner(_DB, RealStart, RealStart, _Interval, _ChunkSize, Chunk) ->
    %ct:pal("inner: 0: ~p", [RealStart]),
    % we hit the start element, i.e. our whole data set has been traversed
    {'$end_of_interval', Chunk};
get_chunk_inner(_DB, Current, _RealStart, _Interval, 0, Chunk) ->
    %ct:pal("inner: 1: ~p", [Current]),
    % we hit the chunk size limit
    {Current, Chunk};
get_chunk_inner({ETSDB, _CKInt, _CKDB} = DB, '$end_of_table', RealStart, Interval, ChunkSize, Chunk) ->
    %ct:pal("inner: 2: ~p", ['$end_of_table']),
    % reached end of table - start at beginning (may be a wrapping interval)
    get_chunk_inner(DB, ets:first(ETSDB), RealStart, Interval, ChunkSize, Chunk);
get_chunk_inner({ETSDB, _CKInt, _CKDB} = DB, Current, RealStart, Interval, ChunkSize, Chunk) ->
    %ct:pal("inner: 3: ~p", [Current]),
    case intervals:in(Current, Interval) of
        true ->
            Entry = get_entry_(DB, Current),
            Next = ets:next(ETSDB, Current),
            get_chunk_inner(DB, Next, RealStart, Interval, ChunkSize - 1, [Entry | Chunk]);
        _ ->
            {'$end_of_interval', Chunk}
    end.

%% @doc Returns the key that would remove not more than TargetLoad entries
%%      from the DB when starting at the key directly after Begin.
%%      Precond: a load larger than 0
%%      Note: similar to get_chunk/2.
-spec get_split_key(DB::db(), Begin::?RT:key(), TargetLoad::pos_integer(), forward | backward)
        -> {?RT:key(), TakenLoad::pos_integer()}.
get_split_key(DB, Begin, TargetLoad, forward) ->
    get_split_key_(DB, Begin, TargetLoad, fun ets:first/1, fun ets:next/2);
get_split_key(DB, Begin, TargetLoad, backward) ->
    get_split_key_(DB, Begin, TargetLoad, fun ets:last/1, fun ets:prev/2).

-spec get_split_key_(DB::db_t(), Begin::?RT:key(), TargetLoad::pos_integer(),
        ETS_first::fun((DB::tid() | atom()) -> ?RT:key() | '$end_of_table'),
        ETS_next::fun((DB::tid() | atom(), Key::?RT:key()) -> ?RT:key() | '$end_of_table'))
        -> {?RT:key(), TakenLoad::pos_integer()}.
get_split_key_({ETSDB, _CKInt, _CKDB} = DB, Begin, TargetLoad,
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
get_split_key_inner({ETSDB, _CKInt, _CKDB} = DB, '$end_of_table', RealStart, TargetLoad, SplitKey, ETS_first, ETS_next) ->
    % reached end of table - start at beginning (may be a wrapping interval)
    get_split_key_inner(DB, ETS_first(ETSDB), RealStart, TargetLoad, SplitKey, ETS_first, ETS_next);
get_split_key_inner({ETSDB, _CKInt, _CKDB} = DB, Current, RealStart, TargetLoad, _SplitKey, ETS_first, ETS_next) ->
    Next = ETS_next(ETSDB, Current),
    get_split_key_inner(DB, Next, RealStart, TargetLoad - 1, Current, ETS_first, ETS_next).

-spec delete_chunk(DB::db(), Interval::intervals:interval(), ChunkSize::pos_integer()) ->
    {intervals:interval(), db()}.
delete_chunk(DB, Interval, ChunkSize) -> delete_chunk_(DB, Interval, ChunkSize).

-spec delete_chunk_(DB::db_t(), Interval::intervals:interval(), ChunkSize::pos_integer()) ->
    {intervals:interval(), db_t()}.
delete_chunk_(DB, Interval, ChunkSize) ->
    {Next, Chunk} = get_chunk_(DB, Interval, ChunkSize),
    DB2 = lists:foldl(fun (Entry, DB1) -> delete_entry(DB1, Entry) end, DB, Chunk),
    {Next, DB2}.

-define(ETS, ets).
-include("db_generic_ets.hrl").
