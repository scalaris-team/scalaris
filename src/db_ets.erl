% @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
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
-type(db() :: {Table::tid() | atom(), RecordChangesInterval::intervals:interval(), ChangedKeysTable::tid() | atom()}).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-export([get_chunk/3]).

-define(CKETS, ets).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initializes a new database.
new() ->
    % ets prefix: DB_ + random name
    RandomName = randoms:getRandomId(),
    DBname = list_to_atom(string:concat("db_", RandomName)),
    CKDBname = list_to_atom(string:concat("db_ck_", RandomName)), % changed keys
    % better protected? All accesses would have to go to DB-process
    {ets:new(DBname, [ordered_set | ?DB_ETS_ADDITIONAL_OPS]), intervals:empty(), ?CKETS:new(CKDBname, [ordered_set | ?DB_ETS_ADDITIONAL_OPS])}.

%% @doc Re-opens a previously existing database (not supported by ets
%%      -> create new DB).
open(_FileName) ->
    log:log(warn, "[ Node ~w:db_ets ] open/1 not supported, executing new/0 instead", [self()]),
    new().

%% @doc Closes and deletes the DB.
close({DB, _CKInt, CKDB}, _Delete) ->
    ets:delete(DB),
    ?CKETS:delete(CKDB).

%% @doc Returns an empty string.
%%      Note: should return the name of the DB for open/1 which is not
%%      supported by ets though).
get_name(_State) ->
    "".

%% @doc Returns all DB entries.
get_data({DB, _CKInt, _CKDB}) ->
    ets:tab2list(DB).

%% @doc Gets the ets database from the state (seperate function to
%%      make dialyzer happy with get_chunk/1 using ets' methods and methods
%%      with an opaque db() state concurrently).
-spec get_db(State::db()) -> tid() | atom().
get_db({DB, _CKInt, _CKDB}) -> DB.

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
get_chunk(DB, Interval, ChunkSize) ->
    % assert ChunkSize > 0, see ChunkSize type
    case get_load(DB) of
        0 -> {intervals:empty(), []};
        _ ->
            %ct:pal("0: ~p ~p", [intervals:get_bounds(Interval), Interval]),
            {_BeginBr, Begin, End, EndBr} = intervals:get_bounds(Interval),
            % get first key which is in the interval and in the ets table:
            ETSDB = get_db(DB),
            case first_key_in_interval(ETSDB, Begin, Interval) of
                '$end_of_table' ->
                    {intervals:empty(), []};
                FirstKey ->
                    %ct:pal("first key: ~.0p~n", [FirstKey]),
                    {Next, Chunk} =
                        get_chunk_inner(DB, ets:next(ETSDB, FirstKey), FirstKey,
                                        Interval, ChunkSize - 1, [get_entry(DB, FirstKey)]),
                    case Next of
                        '$end_of_table' ->
                            {intervals:empty(), Chunk};
                        '$end_of_interval' ->
                            {intervals:empty(), Chunk};
                        _ ->
                            case intervals:in(Next, Interval) of
                                false ->
                                    {intervals:empty(), Chunk};
                                true when Next =:= End ->
                                    {intervals:new(Next), Chunk};
                                _ ->
                                    {intervals:new('[', Next, End, EndBr), Chunk}
                            end
                    end
            end
    end.

%% @doc Find first key in range (assume a continuous interval), start at
%%      Current which does not have to exist in the table.
-spec first_key_in_interval(
        DB::tid() | atom(), Next::?RT:key() | '$end_of_table',
        Interval::intervals:interval()) -> ?RT:key() | '$end_of_table'.
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
-spec get_chunk_inner(DB::{tid() | atom(), intervals:interval(), tid() | atom()},
                      Current::?RT:key() | '$end_of_table', RealStart::?RT:key(),
                      Interval::intervals:interval(), ChunkSize::pos_integer(),
                      Chunk::db_as_list())
        -> {?RT:key() | '$end_of_table' | '$end_of_interval', db_as_list()}.
get_chunk_inner(_DB, RealStart, RealStart, _Interval, _ChunkSize, Chunk) ->
    %ct:pal("inner: 0: ~p", [RealStart]),
    % we hit the start element, i.e. our whole data set has been traversed
    {'$end_of_interval', Chunk};
get_chunk_inner(_DB, Current, _RealStart, _Interval, 0, Chunk) ->
    %ct:pal("inner: 1: ~p", [Current]),
    % we hit the chunk size limit
    {Current, Chunk};
get_chunk_inner(DB, '$end_of_table', RealStart, Interval, ChunkSize, Chunk) ->
    %ct:pal("inner: 2: ~p", ['$end_of_table']),
    % reached end of table - start at beginning (may be a wrapping interval)
    ETSDB = get_db(DB),
    get_chunk_inner(DB, ets:first(ETSDB), RealStart, Interval, ChunkSize, Chunk);
get_chunk_inner(DB, Current, RealStart, Interval, ChunkSize, Chunk) ->
    %ct:pal("inner: 3: ~p", [Current]),
    case intervals:in(Current, Interval) of
        true ->
            Entry = get_entry(DB, Current),
            ETSDB = get_db(DB),
            Next = ets:next(ETSDB, Current),
            get_chunk_inner(DB, Next, RealStart, Interval, ChunkSize - 1, [Entry | Chunk]);
        _ ->
            {'$end_of_interval', Chunk}
    end.

-define(ETS, ets).
-include("db_generic_ets.hrl").
