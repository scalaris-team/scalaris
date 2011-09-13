%  @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc In-process Database using toke
%% @end
-module(db_toke).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).

-type db_t() :: {{DB::pid(), FileName::string()}, SubscrTable::tid() | atom()}.

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-define(CKETS, ets).

-include("db_common.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initializes a new database (will launch a process for it).
new_() ->
    Dir = util:make_filename(atom_to_list(node())),
    FullDir = lists:flatten([config:read(db_directory), "/", Dir]),
    _ = case file:make_dir(FullDir) of
            ok -> ok;
            {error, eexist} -> ok;
            {error, Error} -> exit({db_toke, 'cannot create dir', FullDir, Error})
        end,
    {_Now_Ms, _Now_s, Now_us} = Now = erlang:now(),
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_local_time(Now),
    FileBaseName = util:make_filename(
                     io_lib:format("db_~B~B~B-~B~B~B\.~B.tch",
                                   [Year, Month, Day, Hour, Minute, Second, Now_us])),
    FullFileName = lists:flatten([FullDir, "/", FileBaseName]),
    new_db(FullFileName, [read, write, create, truncate]).

%% @doc Re-opens an existing database (will launch a process for it).
%%      BEWARE: use with caution in order to preserve consistency!
open_(FileName) ->
    new_db(FileName, [read, write]).

-spec new_db(FileName::string(),
             TokeOptions::[read | write | create | truncate | no_lock |
                           lock_no_block | sync_on_transaction]) -> db_t().
new_db(FileName, TokeOptions) ->
    DB = case toke_drv:start_link() of
             {ok, Pid} -> Pid;
             ignore ->
                 log:log(error, "[ Node ~w:db_toke ] process start returned 'ignore'", [self()]),
                 erlang:error({toke_failed, drv_start_ignore});
             {error, Error} ->
                 log:log(error, "[ Node ~w:db_toke ] ~.0p", [self(), Error]),
                 erlang:error({toke_failed, Error})
         end,
    case toke_drv:new(DB) of
        ok ->
            RandomName = randoms:getRandomId(),
            SubscrName = "db_" ++ RandomName ++ ":subscribers",
            case toke_drv:open(DB, FileName, TokeOptions) of
                ok     -> {{DB, FileName},
                           ets:new(list_to_atom(SubscrName), [ordered_set, private])};
                Error2 -> log:log(error, "[ Node ~w:db_toke ] ~.0p", [self(), Error2]),
                          erlang:error({toke_failed, Error2})
            end;
        Error1 ->
            log:log(error, "[ Node ~w:db_toke ] ~.0p", [self(), Error1]),
            erlang:error({toke_failed, Error1})
    end.

%% @doc Deletes all contents of the given DB.
close_(State = {{DB, FileName}, _Subscr}, Delete) ->
    call_subscribers(State, close_db),
    toke_drv:close(DB),
    toke_drv:delete(DB),
    toke_drv:stop(DB),
    case Delete of
        true ->
            case file:delete(FileName) of
                ok -> ok;
                {error, Reason} -> log:log(error, "[ Node ~w:db_toke ] deleting ~.0p failed: ~.0p",
                                           [self(), FileName, Reason])
            end;
        _ -> ok
    end.

%% @doc Returns the name of the DB, i.e. the path to its file, which can be
%%      used with open/1.
get_name_({{_DB, FileName}, _Subscr}) ->
    FileName.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2_({{DB, _FileName}, _Subscr}, Key) ->
    case toke_drv:get(DB, erlang:term_to_binary(Key, [{minor_version, 1}])) of
        not_found -> {false, db_entry:new(Key)};
        Entry     -> {true, erlang:binary_to_term(Entry)}
    end.

%% @doc Inserts a complete entry into the DB.
set_entry_(State = {{DB, _FileName}, _Subscr}, Entry) ->
    case db_entry:is_null(Entry) of
        true -> delete_entry_(State, Entry);
        _    -> 
            Key = db_entry:get_key(Entry),
            ok = toke_drv:insert(DB, erlang:term_to_binary(Key, [{minor_version, 1}]),
                                 erlang:term_to_binary(Entry, [{minor_version, 1}])),
            call_subscribers(State, {write, Entry}),
            State
    end.

%% @doc Updates an existing (!) entry in the DB.
update_entry_(State, Entry) ->
    set_entry_(State, Entry).

%% @doc Removes all values with the given key from the DB.
delete_entry_at_key_(State, Key) ->
    delete_entry_at_key_(State, Key, erlang:term_to_binary(Key, [{minor_version, 1}])).

delete_entry_at_key_(State = {{DB, _FileName}, _Subscr}, Key, Key_) ->
    toke_drv:delete(DB, Key_),
    call_subscribers(State, {delete, Key}),
    State.

%% @doc Returns the number of stored keys.
get_load_({{DB, _FileName}, _Subscr}) ->
    % TODO: not really efficient (maybe store the load in the DB?)
    toke_drv:fold(fun (_K, _V, Load) -> Load + 1 end, 0, DB).

%% @doc Returns the number of stored keys in the given interval.
get_load_(State = {{DB, _FileName}, _Subscr}, Interval) ->
    IsEmpty = intervals:is_empty(Interval),
    IsAll = intervals:is_all(Interval),
    if
        IsEmpty -> 0;
        IsAll   -> get_load_(State);
        true    ->
            toke_drv:fold(fun(Key_, _V, Load) ->
                                  Key = erlang:binary_to_term(Key_),
                                  case intervals:in(Key, Interval) of
                                      true -> Load + 1;
                                      _    -> Load
                                  end
                          end, 0, DB)
    end.

%% @doc Adds all db_entry objects in the Data list.
add_data_(State = {{DB, _FileName}, _Subscr}, Data) ->
    % -> do not use set_entry (no further checks for changed keys necessary)
    lists:foldl(
      fun(DBEntry, _) ->
              ok = toke_drv:insert(DB,
                                   erlang:term_to_binary(db_entry:get_key(DBEntry), [{minor_version, 1}]),
                                   erlang:term_to_binary(DBEntry, [{minor_version, 1}]))
      end, null, Data),
    _ = [call_subscribers(State, {write, Entry}) || Entry <- Data],
    State.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
%%      Note: removes all keys not in MyNewInterval from the list of changed
%%      keys!
split_data_(State = {{DB, _FileName}, _Subscr}, MyNewInterval) ->
    % first collect all toke keys to remove from my db (can not delete while doing fold!)
    F = fun(_K, DBEntry_, HisList) ->
                DBEntry = erlang:binary_to_term(DBEntry_),
                case intervals:in(db_entry:get_key(DBEntry), MyNewInterval) of
                    true -> HisList;
                    _    -> [DBEntry | HisList]
                end
        end,
    HisList = toke_drv:fold(F, [], DB),
    % delete empty entries from HisList and remove all entries in HisList from the DB
    HisListFilt =
        lists:foldl(
          fun(DBEntry, L) ->
                  Key = db_entry:get_key(DBEntry),
                  toke_drv:delete(DB, erlang:term_to_binary(Key, [{minor_version, 1}])),
                  call_subscribers(State, {split, Key}),
                  case db_entry:is_empty(DBEntry) of
                      false -> [DBEntry | L];
                      _     -> L
                  end
          end, [], HisList),
    {State, HisListFilt}.

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%%      which FilterFun returns true.
get_entries_({{DB, _FileName}, _Subscr}, FilterFun, ValueFun) ->
    F = fun (_Key, DBEntry_, Data) ->
                 DBEntry = erlang:binary_to_term(DBEntry_),
                 case FilterFun(DBEntry) of
                     true -> [ValueFun(DBEntry) | Data];
                     _    -> Data
                 end
        end,
    toke_drv:fold(F, [], DB).

%% @doc Returns all ValueFun(DBEntry) objects of the given DB which are in the
%%      given interval and satisfy FilterFun but at most ChunkSize elements.
%%      See get_chunk/3 for more details.
get_chunk_(State, Interval, FilterFun, ValueFun, ChunkSize) ->
    AddDataFun = fun(_Key_, _Key, DBEntry_, Data) ->
                         DBEntry = erlang:binary_to_term(DBEntry_),
                         case FilterFun(DBEntry) of
                             true -> [ValueFun(DBEntry) | Data];
                             _    -> Data
                         end
                 end,
    get_chunk_helper(State, Interval, AddDataFun, fun db_entry:get_key/1, ChunkSize).

-spec get_chunk_helper(DB::db_t(), Interval::intervals:interval(),
                       AddDataFun::fun((Key_::binary(), Key::?RT:key(), db_entry:entry(), [T]) -> [T]),
                       GetKeyFromDataFun::fun((T) -> ?RT:key()), ChunkSize::pos_integer() | all)
        -> {intervals:interval(), [T]}.
get_chunk_helper({{DB, _FileName}, _Subscr}, Interval, AddDataFun, GetKeyFromDataFun, ChunkSize) ->
    {BeginBr, Begin, End, EndBr} = intervals:get_bounds(Interval),
    % try to find the first existing key in the interval, starting at Begin:
    MInfToBegin = intervals:minus(intervals:all(),
                                  intervals:new(BeginBr, Begin, ?PLUS_INFINITY, ')')),
    F = fun (Key_, DBEntry_, {N, Data} = Acc) ->
                 Key = erlang:binary_to_term(Key_),
                 case intervals:in(Key, Interval) of
                     true when ChunkSize =:= all ->
                         AddDataFun(Key_, Key, DBEntry_, Data);
                     true ->
                         Data1 = AddDataFun(Key_, Key, DBEntry_, Data),
                         % filter out every (2 * ChunkSize) elements
                         case N rem 2 * ChunkSize of
                             0 ->
                                 {0, get_chunk_helper_filter(Data1, MInfToBegin, GetKeyFromDataFun, ChunkSize)};
                             _ ->
                                 {N + 1, Data1}
                         end;
                     _    -> Acc
                 end
        end,
    {_, Data} = toke_drv:fold(F, {0, []}, DB),
    SortedData = get_chunk_helper_sort(Data, MInfToBegin, GetKeyFromDataFun),
    case ChunkSize of
        all -> {intervals:empty(), SortedData};
        _   -> {Chunk, Rest} = util:safe_split(ChunkSize, SortedData),
               case Rest of
                   []      -> {intervals:empty(), Chunk};
                   [H | _] -> {intervals:new('[', GetKeyFromDataFun(H), End, EndBr), Chunk}
               end
    end.

-spec get_chunk_helper_sort(Data::[T], MInfToBegin::intervals:interval(),
                            GetKeyFromDataFun::fun((T) -> ?RT:key())) -> SortedData::[T].
get_chunk_helper_sort(Data, MInfToBegin, GetKeyFromDataFun) ->
    {SecondPart, FirstPart} =
        lists:partition(fun(E) ->
                                intervals:in(GetKeyFromDataFun(E), MInfToBegin)
                        end, Data),
    lists:append(lists:usort(FirstPart), lists:usort(SecondPart)).

-spec get_chunk_helper_filter(Data::[T], MInfToBegin::intervals:interval(),
                              GetKeyFromDataFun::fun((T) -> ?RT:key()),
                              ChunkSize::pos_integer()) -> SortedData::[T].
get_chunk_helper_filter(Data, MInfToBegin, GetKeyFromDataFun, ChunkSize) ->
    SortedData = get_chunk_helper_sort(Data, MInfToBegin, GetKeyFromDataFun),
    % note: leave one extra to be able to find the next available key
    {Chunk, _Rest} = util:safe_split(ChunkSize + 1, SortedData),
    Chunk.

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
delete_entries_(State = {{DB, _FileName}, _Subscr}, FilterFun) when is_function(FilterFun) ->
    % first collect all toke keys to delete (can not delete while doing fold!)
    F = fun(KeyToke, DBEntry_, ToDelete) ->
                DBEntry = erlang:binary_to_term(DBEntry_),
                case FilterFun(DBEntry) of
                    false -> ToDelete;
                    _     -> [{KeyToke, db_entry:get_key(DBEntry)} | ToDelete]
                end
        end,
    KeysToDelete = toke_drv:fold(F, [], DB),
    % delete all entries with these keys
    _ = [begin
             toke_drv:delete(DB, KeyToke),
             call_subscribers(State, {delete, Key})
         end || {KeyToke, Key} <- KeysToDelete],
    State;
delete_entries_(State, Interval) ->
    {Elements, RestInterval} = intervals:get_elements(Interval),
    case intervals:is_empty(RestInterval) of
        true ->
            lists:foldl(fun(Key, State1) -> delete_entry_at_key_(State1, Key) end, State, Elements);
        _ ->
            delete_entries_(State,
                            fun(E) ->
                                    intervals:in(db_entry:get_key(E), Interval)
                            end)
    end.

delete_chunk_(DB, Interval, ChunkSize) ->
    AddDataFun = fun(Key_, Key, _DBEntry_, Data) -> [{Key, Key_} | Data] end,
    {Next, Chunk} = get_chunk_helper(DB, Interval, AddDataFun, fun({K, _K_}) -> K end, ChunkSize),
    DB2 = lists:foldl(fun({Key, Key_}, DB1) -> delete_entry_at_key_(DB1, Key, Key_) end, DB, Chunk),
    {Next, DB2}.

%% @doc Returns all DB entries.
get_data_({{DB, _FileName}, _Subscr}) ->
    toke_drv:fold(fun (_K, DBEntry, Acc) ->
                           [erlang:binary_to_term(DBEntry) | Acc]
                  end, [], DB).

%% @doc Returns the key that would remove not more than TargetLoad entries
%%      from the DB when starting at the key directly after Begin.
%%      Precond: a load larger than 0
%%      Note: similar to get_chunk/2.
get_split_key_(DB, Begin, TargetLoad, Direction) ->
    % assert ChunkSize > 0, see ChunkSize type
    case get_load_(DB) of
        0 -> throw('empty_db');
        _ ->
            % first need to get all keys, then sort them and filter out the split key
            F = fun (Key_, _DBEntry_, Data) -> [erlang:binary_to_term(Key_) | Data] end,
            Keys = toke_drv:fold(F, [], DB),
            % try to find the first existing key in the interval, starting at Begin (exclusive):
            MInfToBegin = intervals:minus(intervals:all(),
                                          intervals:new('(', Begin, ?PLUS_INFINITY, ')')),
            {SecondPart, FirstPart} =
                lists:partition(fun(E) -> intervals:in(E, MInfToBegin) end, Keys),
            SortedKeys = lists:append(lists:usort(FirstPart), lists:usort(SecondPart)),
            
            case Direction of
                forward  ->
                    {Chunk, _Rest} = util:safe_split(TargetLoad, SortedKeys),
                    {lists:last(Chunk), erlang:length(Chunk)};
                backward ->
                    {Chunk, _Rest} = util:safe_split(TargetLoad, lists:reverse(SortedKeys)),
                    {hd(Chunk), erlang:length(Chunk)}
            end
    end.
