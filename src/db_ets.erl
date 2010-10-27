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
-opaque(db() :: {Table::tid() | atom(), RecordChangesInterval::intervals:interval(), ChangedKeysTable::tid() | atom()}).

-type(chunk() :: list(db_entry:entry())).

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

% @doc tries to return all key-value pairs of the given DB which are in the
% given interval but at most ChunkSize elements
% assumes the ets-table is an ordered_set
% may return data from "both ends" of the DB-range if Begin(interval) > End(Interval)
% returns the chunk and the remaining interval
-spec get_chunk(DB::db(), Interval::intervals:interval(), ChunkSize::pos_integer()) ->
    {intervals:interval(), chunk()}.
get_chunk(DB, Interval, ChunkSize) ->
    % assert ChunkSize > 0, see ChunkSize type
    case get_load(DB) of
        0 -> {intervals:empty(), []};
        _ ->
            %ct:pal("0: ~p ~p", [intervals:get_bounds(Interval), Interval]),
            {'[', Begin, End, ')'} = intervals:get_bounds(Interval),
            {Next, Chunk} = get_chunk_inner_start(DB, Begin, Interval, ChunkSize),
            case Next of
                '$end_of_table' ->
                    {intervals:empty(), Chunk};
                _ ->
                    {intervals:new({'[', Begin, End, ')'}), Chunk}
            end
    end.

% @doc find first real item in range
-spec get_chunk_inner_start(DB::db(),
                            Next::?RT:key() | '$end_of_table',
                            Interval::intervals:interval(),
                            ChunkSize::pos_integer()) ->
    {?RT:key() | '$end_of_table', chunk()}.
get_chunk_inner_start({ETSDB, _CKInt, _CKDB} = DB, '$end_of_table', Interval, ChunkSize) ->
    %ct:pal("start: 0: ~p", ['$end_of_table']),
    get_chunk_inner_start(DB, ets:first(ETSDB), Interval, ChunkSize);
get_chunk_inner_start({ETSDB, _CKInt, _CKDB} = DB, Current, Interval, ChunkSize) ->
    %ct:pal("start: 1: ~p", [Current]),
    case intervals:in(Current, Interval) of
        true ->
            case get_entry2(DB, Current) of
                {true, Entry} ->
                    % Current is the first real entry and it is in the given range
                    Next = ets:next(ETSDB, Current),
                    case intervals:in(Next, Interval) of
                        true ->
                            get_chunk_inner(DB, Next, Current, Interval,
                                            ChunkSize - 1, [Entry]);
                        false ->
                            {'$end_of_table', [Entry]}
                    end;
                {false, _} ->
                    Next = ets:next(ETSDB, Current),
                    get_chunk_inner_start(DB, Next, Interval, ChunkSize)
            end;
        false ->
            % found nothing in range
            {'$end_of_table', []}
    end.


% @doc inner loop for get_chunk
-spec get_chunk_inner(DB::db(),
                      Next::?RT:key() | '$end_of_table',
                      RealStart::?RT:key(),
                      Interval::intervals:interval(),
                      ChunkSize::pos_integer(),
                      Chunk::chunk()) ->
    {?RT:key() | '$end_of_table', chunk()}.
get_chunk_inner(_DB, Next, _RealStart, _Interval, 0, Chunk) ->
    %ct:pal("inner: 0: ~p", [Next]),
    % we hit the chunk size limit
    {Next, Chunk};
get_chunk_inner(_DB, RealStart, RealStart, _Interval, _ChunkSize, Chunk) ->
    %ct:pal("inner: 1: ~p", [RealStart]),
    % we hit the start element
    {RealStart, Chunk};
get_chunk_inner({ETSDB, _CKInt, _CKDB} = DB, '$end_of_table', RealStart, Interval, ChunkSize, Chunk) ->
    %ct:pal("inner: 2: ~p", ['$end_of_table']),
    First = ets:first(ETSDB),
    case intervals:in(First, Interval) of
        true ->
            get_chunk_inner(DB, First, RealStart, Interval, ChunkSize, Chunk); %continue
        false ->
            {'$end_of_table', Chunk} %done
    end;
get_chunk_inner({ETSDB, _CKInt, _CKDB} = DB, Current, RealStart, Interval, ChunkSize, Chunk) ->
    %ct:pal("inner: 3: ~p", [Current]),
    % pre: Current is in Interval
    case get_entry2(DB, Current) of
        {true, Entry} ->
            Next = ets:next(ETSDB, Current),
            case intervals:in(Next, Interval) of
                true ->
                    get_chunk_inner(DB, Next, RealStart, Interval, ChunkSize - 1, [Entry | Chunk]);
                false ->
                    {'$end_of_table', [Entry | Chunk]}
            end;
        {false, _} ->
            Next = ets:next(ETSDB, Current),
            get_chunk_inner(DB, Next, RealStart, Interval, ChunkSize, Chunk)
    end.

-define(ETS, ets).
-include("db_generic_ets.hrl").
