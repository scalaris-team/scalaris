% @copyright 2009-2013 Zuse Institute Berlin,

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
%% @doc    In-process database using ets
%% @end
%% @version $Id$
-module(db_ets).
-author('schintke@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).
-type db_t() :: {Table::tid() | atom(), SubscrTable::tid() | atom(), {SnapTable::tid() | atom() | boolean(), non_neg_integer(), non_neg_integer()}}.

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-define(CKETS, ets).

-include("db_common.hrl").

%% @doc Initializes a new database.
-spec new_() -> db_t().
new_() ->
    % ets prefix: DB_ + random name
    RandomName = randoms:getRandomString(),
    DBName = "db_" ++ RandomName,
    SubscrName = DBName ++ ":subscribers",
    % better protected? All accesses would have to go to DB-process
    {ets:new(list_to_atom(DBName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
     ets:new(list_to_atom(SubscrName), [ordered_set, private]),
     {false,0,0}}.

%% @doc Re-opens a previously existing database (not supported by ets
%%      -> create new DB).
-spec open_(DBName::db_name()) -> db_t().
open_(_FileName) ->
    log:log(warn, "[ Node ~w:db_ets ] open/1 not supported, executing new/0 instead", [self()]),
    new().

%% @doc Closes and deletes the DB.
-spec close_(DB::db_t(), Delete::boolean()) -> any().
close_(State = {DB, Subscr, {SnapTable, _LiveLC, _SnapLC}}, _Delete) ->
    _ = call_subscribers(State, close_db),
    ets:delete(DB),
    ets:delete(Subscr),
    case SnapTable of
        false -> ok;
        _ -> ets:delete(SnapTable)
    end.

%% @doc Returns the name of the table for open/1.
-spec get_name_(DB::db_t()) -> db_name().
get_name_({DB, _Subscr, _SnapState}) ->
    erlang:atom_to_list(ets:info(DB, name)).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
-spec get_entry2_(DB::db_t(), Key::?RT:key()) -> {Exists::boolean(), db_entry:entry()}.
get_entry2_({DB, _Subscr, _SnapState}, Key) ->
    case ets:lookup(DB, Key) of
        [Entry] -> {true, Entry};
        []      -> {false, db_entry:new(Key)}
    end.

%% @doc Inserts a complete entry into the DB.
%%      Note: is the Entry is a null entry, it will be deleted!
-spec set_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
set_entry_(State = {DB, _Subscr, {_SnapTable, LiveLC, _SnapLC}}, Entry) ->
    case db_entry:is_null(Entry) of
        true -> 
                delete_entry_(State, Entry);
        _    -> {_, OldEntry} = get_entry2_(State,db_entry:get_key(Entry)),
                NewLiveLC = db_entry:update_lockcount(OldEntry,Entry,LiveLC),
                ets:insert(DB, Entry),
                call_subscribers({DB,_Subscr,{_SnapTable,NewLiveLC,_SnapLC}}, {write, Entry})
    end.

%% @doc Updates an existing (!) entry in the DB.
%%      TODO: use ets:update_element here?
-spec update_entry_(DB::db_t(), Entry::db_entry:entry()) -> NewDB::db_t().
update_entry_(State, Entry) ->
    set_entry_(State, Entry).

%% @doc Removes all values with the given key from the DB with reason delete.
-spec delete_entry_at_key_(DB::db_t(), ?RT:key()) -> NewDB::db_t().
delete_entry_at_key_(State, Key) ->
    delete_entry_at_key_(State, Key, delete).

%% @doc Removes all values with the given key from the DB with specified reason.
-spec delete_entry_at_key_(DB::db_t(), ?RT:key(), delete | split) -> NewDB::db_t().
delete_entry_at_key_(State = {DB, _Subscr, {_SnapTable, LiveLC, _SnapLC}}, Key, Reason) ->
    {_, OldEntry} = get_entry2_(State,Key),
    NewLiveLC = db_entry:update_lockcount(OldEntry,db_entry:new(Key),LiveLC),
    ets:delete(DB, Key),
    call_subscribers({DB,_Subscr,{_SnapTable,NewLiveLC,_SnapLC}}, {Reason, Key}).

%% @doc Returns the number of stored keys.
-spec get_load_(DB::db_t()) -> Load::integer().
get_load_({DB, _Subscr, _SnapState}) ->
    ets:info(DB, size).

%% @doc Returns the number of stored keys in the given interval.
-spec get_load_(DB::db_t(), Interval::intervals:interval()) -> Load::integer().
get_load_(State = {DB, _Subscr, _SnapState}, Interval) ->
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

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
%%      Note: removes all keys not in MyNewInterval from the list of changed
%%      keys!
-spec split_data_(DB::db_t(), MyNewInterval::intervals:interval()) ->
         {NewDB::db_t(), db_as_list()}.
split_data_(State = {DB, _Subscr, _SnapState}, MyNewInterval) ->
    F = fun (DBEntry, {StateAcc, HisList}) ->
                Key = db_entry:get_key(DBEntry),
                case intervals:in(Key, MyNewInterval) of
                    true -> {StateAcc, HisList};
                    _    -> NewHisList = case db_entry:is_empty(DBEntry) of
                                false -> [DBEntry | HisList];
                                _     -> HisList
                            end,
                            {delete_entry_at_key_(StateAcc, Key, split), NewHisList}
                end
        end,
    ets:foldl(F, {State, []}, DB).

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%%      which FilterFun returns true.
-spec get_entries_(DB::db_t(),
                   FilterFun::fun((DBEntry::db_entry:entry()) -> boolean()),
                   ValueFun::fun((DBEntry::db_entry:entry()) -> Value))
        -> [Value].
get_entries_({DB, _Subscr, _SnapState}, FilterFun, ValueFun) ->
    F = fun (DBEntry, Data) ->
                 case FilterFun(DBEntry) of
                     true -> [ValueFun(DBEntry) | Data];
                     _    -> Data
                 end
        end,
    ets:foldl(F, [], DB).

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
-spec delete_entries_(DB::db_t(),
                      RangeOrFun::intervals:interval() |
                                  fun((DBEntry::db_entry:entry()) -> boolean()))
        -> NewDB::db_t().
delete_entries_(State = {DB, _Subscr, _SnapState}, FilterFun) when is_function(FilterFun) ->
    F = fun(DBEntry, StateAcc) ->
                case FilterFun(DBEntry) of
                    false -> StateAcc;
                    _     -> delete_entry_at_key_(StateAcc, db_entry:get_key(DBEntry))
                end
        end,
    ets:foldl(F, State, DB);
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
-spec get_data_(DB::db_t()) -> db_as_list().
get_data_({DB, _Subscr, _SnapState}) ->
    ets:tab2list(DB).

-spec get_chunk_(DB::db_t(), StartId::?RT:key(), Interval::intervals:interval(),
                 FilterFun::fun((db_entry:entry()) -> boolean()),
                 ValueFun::fun((db_entry:entry()) -> V), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [V]}.
get_chunk_(DB, StartId, Interval, FilterFun, ValueFun, all) ->
    get_chunk_(DB, StartId, Interval, FilterFun, ValueFun, get_load_(DB));
get_chunk_(DB, StartId, Interval, FilterFun, ValueFun, ChunkSize) ->
    AddDataFun = fun(DB1, Key, Data) ->
                         DBEntry = get_entry_(DB1, Key),
                         case FilterFun(DBEntry) of
                             true -> [ValueFun(DBEntry) | Data];
                             _    -> Data
                         end
                 end,
    {Next, Data} =
        get_chunk_helper(DB, StartId, Interval, AddDataFun, ChunkSize, forward),
    {Next, lists:reverse(Data)}.

-type add_data_fun(V) :: fun((DB::db_t(), Key::?RT:key(), Data::[] | V) -> [] | V).
-type ets_start_fun() :: fun((Table::tid() | atom()) -> ?RT:key() | '$end_of_table').
-type ets_cont_fun()  :: fun((Table::tid() | atom(), Key::?RT:key()) -> ?RT:key() | '$end_of_table').

-spec get_chunk_helper(DB::db_t(), StartId::?RT:key(), Interval::intervals:interval(), AddDataFun::add_data_fun(V),
                       ChunkSize::pos_integer(), ForwardBackward::forward | backward)
        -> {intervals:interval(), [] | V}.
get_chunk_helper({ETSDB, _Subscr, _SnapStates} = DB, StartId, Interval,
                 AddDataFun, ChunkSize, ForwardBackward) ->
    Load = get_load_(DB),
    case intervals:is_empty(Interval) of
        true ->
            {intervals:empty(), []};
        _ when Load =:= 0 ->
            {intervals:empty(), []};
        _ ->
            % assert ChunkSize > 0, see ChunkSize type
            %log:pal("0: ~p ~p", [intervals:get_bounds(Interval), Interval]),
            {_BeginBr, Begin, End, _EndBr} = intervals:get_bounds(Interval),
            case ForwardBackward of
                forward  ->
                    ETS_first = fun ets:first/1, ETS_next = fun ets:next/2,
                    ETS_last  = fun ets:last/1,  ETS_prev = fun ets:prev/2,
                    StartFromI_first = Begin,    StartFromI_last = End;
                backward ->
                    ETS_first = fun ets:last/1,  ETS_next = fun ets:prev/2,
                    ETS_last  = fun ets:first/1, ETS_prev = fun ets:next/2,
                    StartFromI_first = End,      StartFromI_last = Begin
            end,
            % get first key which is in the interval and in the ets table:
            IsContinuous = intervals:is_continuous(Interval),
            {FirstKey, ChunkF, ChunkFProcessed} =
                first_key_in_interval(DB, ETS_first, ETS_next, StartId, Interval,
                                      StartFromI_first, AddDataFun, IsContinuous),
            {LastKey, _ChunkL, ChunkLProcessed} =
                first_key_in_interval(DB, ETS_last, ETS_prev, StartId, Interval,
                                      StartFromI_last, AddDataFun, IsContinuous),
%%             log:pal("~nfirst key (InI?): ~.0p (~p)~nlast key (InI?): ~.0p (~p)~n",
%%                     [FirstKey, ChunkFProcessed =:= 1, LastKey, ChunkLProcessed =:= 1]),
            
            case FirstKey of
                LastKey when FirstKey =/= StartId ->
                    % only one element in DB or interval
                    {intervals:empty(), ChunkF};
                _ ->
                    % note: the value from Chunk1 is not added by get_chunk_inner!
%%                     log:pal("get_chunk:~nCSize: ~.2p~nFSize: ~.2p~nRSize: ~.2p",
%%                             [ChunkSize, ChunkFProcessed, ChunkSize - ChunkFProcessed]),
                    % speed up common case:
                    % Interval is continuous, StartId is not in Interval, FirstKey and LastKey are in Interval
                    % or Interval is all (also continuous)
                    % -> FirstKey is first in DB and first in interval order (similar for LastKey)
                    % -> all elements up to LastKey are in Interval
                    %    (no further intervals:in/2 checks necessary)
                    NoInIntCheck = IsContinuous andalso
                                       ChunkFProcessed =:= 1 andalso
                                       ChunkLProcessed =:= 1  andalso
                                       ((not intervals:in(StartId, Interval)) orelse
                                            intervals:is_all(Interval)),
                    {Next, Chunk, RestChunkSize} =
                        get_chunk_inner(DB, ETS_first, ETS_next, ETS_next(ETSDB, FirstKey), LastKey,
                                        Interval, AddDataFun, ChunkSize - ChunkFProcessed,
                                        ChunkF, NoInIntCheck),
%%                     log:pal("get_chunk_inner:~nNext:  ~.2p~nChunk: ~.2p~nRSize: ~.2p",
%%                             [Next, Chunk, RestChunkSize]),
                    
                    % if data from LastKey was not added (if FirstKey =:= LastKey) and
                    % we have space left, add it here:
                    Chunk1 = if ChunkLProcessed =/= 0 andalso FirstKey =/= LastKey
                                    andalso RestChunkSize =/= 0 ->
                                    % use AddDataFun to process the item as the user intended:
                                    AddDataFun(DB, LastKey, Chunk);
                                true -> Chunk
                             end,
                    
                    RestInterval = 
                        case Next of
                            '$end_of_interval' when ChunkLProcessed =/= 0
                                                        andalso FirstKey =/= LastKey
                                                        andalso RestChunkSize =:= 0 ->
                                % would have added LastKey, but no space in chunk
                                % -> have taken everything except LastKey
                                intervals:new(LastKey);
                            '$end_of_interval' ->
%%                                 log:pal("get_chunk:~nNextI: ~.2p~nChunk: ~.2p",
%%                                         [intervals:empty(), Chunk1]),
                                intervals:empty();
                            _ ->
                                TakenInterval =
                                    case ForwardBackward of
                                        forward when FirstKey =:= LastKey ->
                                            intervals:new('[', LastKey, Next, ')');
                                        forward ->
                                            intervals:new('(', LastKey, Next, ')');
                                        backward when FirstKey =:= LastKey ->
                                            intervals:new('(', Next, LastKey, ']');
                                        backward ->
                                            intervals:new('(', Next, LastKey, ')')
                                    end,
%%                                 log:pal("get_chunk:~nNextI: ~.2p~nChunk: ~.2p",
%%                                         [intervals:minus(Interval, TakenInterval), Chunk1]),
                                intervals:minus(Interval, TakenInterval)
                        end,
                    {RestInterval, Chunk1}
            end
    end.

%% @doc Find first key in range (assume a continuous interval), start at
%%      Current which does not have to exist in the table.
-spec first_key_in_interval(
        DB::db_t(), ETS_first::ets_start_fun(), ETS_next::ets_cont_fun(),
        Start::?RT:key(), Interval::intervals:interval(), StartFromI::?RT:key(),
        AddDataFun::add_data_fun(V), TryFromI::boolean())
        -> {?RT:key(), [] | V, Processed::non_neg_integer()}.
first_key_in_interval({ETSDB, _Subscr, _SnapState} = DB, ETS_first, ETS_next,
                      Start, Interval, StartFromI, AddDataFun, TryFromI) ->
    case intervals:in(Start, Interval) andalso ets:member(ETSDB, Start) of
        true ->
            {Start, AddDataFun(DB, Start, []), 1};
        _    ->
            Next = case ETS_next(ETSDB, Start) of
                       '$end_of_table' -> ETS_first(ETSDB);
                       X               -> X
                   end,
            case intervals:in(Next, Interval) of
                true ->
                    {Next, AddDataFun(DB, Next, []), 1};
                _ when not TryFromI ->
                    {Next, [], 0};
                _ ->
                    % try interval bounds
                    first_key_in_interval(DB, ETS_first, ETS_next, StartFromI,
                                          Interval, StartFromI, AddDataFun, false)
            end
    end.

%% @doc Inner loop for get_chunk. Will stop at the first value equal to End.
%% pre: Current is in ets table, ets table is not empty
-spec get_chunk_inner(DB::db_t(), ETS_first::ets_start_fun(), ETS_next::ets_cont_fun(),
                      Current::?RT:key() | '$end_of_table',
                      End::?RT:key(), Interval::intervals:interval(),
                      AddDataFun::add_data_fun(V), ChunkSize::non_neg_integer(),
                      Chunk::[] | V, NoInIntCheck::boolean())
        -> {?RT:key() | '$end_of_interval', [] | V, Rest::non_neg_integer()}.
get_chunk_inner(_DB, _ETS_first, _ETS_next, End, End, _Interval, _AddDataFun,
                ChunkSize, Chunk, _NoInIntCheck) ->
    %log:pal("inner: 0: ~p", [End]),
    % we hit the start element, i.e. our whole data set has been traversed
    {'$end_of_interval', Chunk, ChunkSize};
get_chunk_inner({ETSDB, _Subscr, _SnapState} = _DB, ETS_first, _ETS_next, Current,
                End, _Interval, _AddDataFun, 0 = ChunkSize, Chunk, _NoInIntCheck) ->
    %log:pal("inner: 1: ~p", [Current]),
    % we hit the chunk size limit
    case Current of
        '$end_of_table' ->
            % note: should not be '$end_of_table' (table is not empty!)
            First = ETS_first(ETSDB),
            case First of
                End -> {'$end_of_interval', Chunk, ChunkSize};
                _   -> {First, Chunk, ChunkSize}
            end;
        _ ->
            {Current, Chunk, ChunkSize}
    end;
get_chunk_inner({ETSDB, _Subscr, _SnapState} = DB, ETS_first, ETS_next, '$end_of_table',
                End, Interval, AddDataFun, ChunkSize, Chunk, NoInIntCheck) ->
    %log:pal("inner: 2: ~p", ['$end_of_table']),
    % reached end of table - start at beginning (may be a wrapping interval)
    get_chunk_inner(DB, ETS_first, ETS_next, ETS_first(ETSDB), End, Interval,
                    AddDataFun, ChunkSize, Chunk, NoInIntCheck);
get_chunk_inner({ETSDB, _Subscr, _SnapState} = DB, ETS_first, ETS_next, Current,
                End, Interval, AddDataFun, ChunkSize, Chunk, NoInIntCheck) ->
    %log:pal("inner: 3: ~p", [Current]),
    case NoInIntCheck orelse intervals:in(Current, Interval) of
        true -> NewChunk = AddDataFun(DB, Current, Chunk),
                NewChunkSize = ChunkSize - 1;
        _    -> NewChunk = Chunk,
                NewChunkSize = ChunkSize
    end,
    Next = ETS_next(ETSDB, Current),
    get_chunk_inner(DB, ETS_first, ETS_next, Next, End, Interval,
                    AddDataFun, NewChunkSize, NewChunk, NoInIntCheck).

%% @doc Returns the key that would remove not more than TargetLoad entries
%%      from the DB when starting at the key directly after Begin in case of
%%      forward searches and directly at Begin in case of backward searches,
%%      respectively.
%%      Precond: a load larger than 0
%%      Note: similar to get_chunk/2.
-spec get_split_key_(DB::db_t(), Begin::?RT:key(), End::?RT:key(), TargetLoad::pos_integer(), forward | backward)
        -> {?RT:key(), TakenLoad::non_neg_integer()}.
get_split_key_(DB, Begin, End, TargetLoad, forward) ->
    case get_chunk_helper(DB, Begin, intervals:new('(', Begin, End, ']'),
                          fun get_split_key_data_fun/3, TargetLoad, forward) of
        {_Next, []}           -> {End, 0};
        {_Next, {[H], Taken}} -> {H, Taken}
    end;
get_split_key_(DB, Begin, End, TargetLoad, backward) ->
    case get_chunk_helper(DB, Begin, intervals:new('(', End, Begin, ']'),
                          fun get_split_key_data_fun/3, TargetLoad + 1, backward) of
        {_Next, []} ->
            {End, 0};
        {_Next, {[H], Taken}}  when Taken =:= (TargetLoad + 1) ->
            {H, TargetLoad};
        {_Next, {[_H], Taken}} when Taken =< TargetLoad ->
            {End, Taken}
    end.

-spec get_split_key_data_fun(DB::db_t(), Key::?RT:key(), Data::[] | Res) -> Res
        when is_subtype(Res, {[Key::?RT:key(),...], Count::pos_integer()}).
get_split_key_data_fun(_DB1, Key, []) -> {[Key], 1};
get_split_key_data_fun(_DB1, Key, {_Keys, Count}) -> {[Key], Count + 1}.
