%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    generic db code for ets
%%% @end
%% @version $Id$

% Note: this include must be included in files including this file!
%% -include("../include/scalaris.hrl").

get_entry(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
%% for new storage format
%%        [Entry] -> Entry;
%% for compatibility with old DB format:
        [{Key, {A,B,C,D}}] -> {Key,A,B,C,D};
        [] -> db_entry:new(Key)
    end.

set_entry(DB, Entry) ->
%%  for new storage format
%%   ets:insert(DB, Entry).
%% for compatibility with old DB format:
    {Key, A,B,C,D} = Entry,
    ets:insert(DB, {Key, {A,B,C,D}}),
    DB.

%% @doc sets a write lock on a key.
%%      the write lock is a boolean value per key
set_write_lock(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {Value, false, 0, Version}}] ->
            ?ETS:insert(DB, {Key, {Value, true, 0, Version}}),
            {DB, ok};
        [{Key, {_Value, _WriteLock, _ReadLock, _Version}}] ->
            {DB, failed};
        [] ->
            % no value stored yet
            ?ETS:insert(DB, {Key, {empty_val, true, 0, -1}}),
            {DB, ok}
    end.

%% @doc unsets the write lock of a key
%%      the write lock is a boolean value per key
unset_write_lock(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {empty_val, true, 0, -1}}] ->
            ?ETS:delete(DB, Key),
            {DB, ok};
        [{Key, {Value, true, ReadLock, Version}}] ->
            ?ETS:insert(DB, {Key, {Value, false, ReadLock, Version}}),
            {DB, ok};
        [{Key, {_Value, false, _ReadLock, _Version}}] ->
            {DB, failed};
        [] ->
            {DB, failed}
    end.

%% @doc sets a read lock on a key
%%      the read lock is an integer value per key
set_read_lock(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {Value, false, ReadLock, Version}}] ->
            ?ETS:insert(DB, {Key, {Value, false, ReadLock + 1, Version}}),
            {DB, ok};
        [{Key, {_Value, _WriteLock, _ReadLock, _Version}}] ->
            {DB, failed};
        [] ->
            {DB, failed}
    end.

%% @doc unsets a read lock on a key
%%      the read lock is an integer value per key
unset_read_lock(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {_Value, _WriteLock, 0, _Version}}] ->
            {DB, failed};
        [{Key, {Value, WriteLock, ReadLock, Version}}] ->
            ?ETS:insert(DB, {Key, {Value, WriteLock, ReadLock - 1, Version}}),
            {DB, ok};
        [] ->
            {DB, failed}
    end.

%% @doc get the locks and version of a key
get_locks(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {_Value, WriteLock, ReadLock, Version}}] ->
            {DB, {WriteLock, ReadLock, Version}};
        [] ->
            {DB, failed}
    end.

%% @doc reads the version and value of a key
read(DB, Key) ->
%%    Start = erlang:now(),
    Res = case ?ETS:lookup(DB, Key) of
              [{Key, {Value, _WriteLock, _ReadLock, Version}}] ->
                  {ok, Value, Version};
              [] ->
                  {ok, empty_val, -1}
          end,
%%     Stop = erlang:now(),
%%     Span = timer:now_diff(Stop, Start),
%%     case ets:lookup(profiling, db_read_lookup) of
%%         [] ->
%%             ets:insert(profiling, {db_read_lookup, Span});
%%         [{_, Sum}] ->
%%             ets:insert(profiling, {db_read_lookup, Sum + Span})
%%     end,
    Res.

%% @doc updates the value of key
write(DB, Key, Value, Version) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {_Value, WriteLock, ReadLock, _Version}}] ->
            % better use ets:update_element?
            ?ETS:insert(DB, {Key, {Value, WriteLock, ReadLock, Version}});
        [] ->
            ?ETS:insert(DB, {Key, {Value, false, 0, Version}})
    end,
    DB.

%% @doc deletes the key
delete(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {_Value, false, 0, _Version}}] ->
            ?ETS:delete(DB, Key),
            {DB, ok};
        [{Key, _Value}] ->
            {DB, locks_set};
        [] ->
            {DB, undef}
    end.

%% @doc reads the version of a key
get_version(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
        [{Key, {_Value, _WriteLock, _ReadLock, Version}}] ->
            {ok, Version};
        [] ->
            failed
    end.

%% @doc returns the number of stored keys
get_load(DB) ->
    ?ETS:info(DB, size).

%% @doc adds keys
add_data(DB, Data) ->
    ?ETS:insert(DB, Data),
    DB.

%% @doc returns all keys (and removes them from the db) which belong 
%%      to a new node with id HisKey
split_data(DB, MyKey, HisKey) ->
    F = fun (KV = {Key, _}, HisList) ->
                case util:is_between(HisKey, Key, MyKey) of
                    true ->
                        HisList;
                    false ->
                        [KV | HisList]
                end
        end,
    HisList = ?ETS:foldl(F, [], DB),
    [ ?ETS:delete(DB, AKey) || {AKey, _} <- HisList],
    {DB, HisList}.

% update only if no locks are taken and version number is higher
update_if_newer(OldDB, KVs) ->
    F = fun ({Key, Value, Version}, DB) ->
                case ?ETS:lookup(DB, Key) of
                    [] ->
                        ?ETS:insert(DB, {Key, {Value, false, 0, Version}}),
                        DB;
                    [{_Value, WriteLock, ReadLock, OldVersion}] ->
                        case not WriteLock andalso
                            ReadLock == 0 andalso
                            OldVersion < Version of
                            true ->
                                ?ETS:insert(DB, {Key, {Value, WriteLock, ReadLock, Version}}), 
                                DB;
                            false ->
                                DB
                        end
                end
        end,
    lists:foldl(F, OldDB, KVs).

%% @doc get keys in a range
-spec(get_range/3 :: (db(), key(), key()) -> [{key(), value()}]).
get_range(DB, From, To) ->
    F = fun ({Key, {Value, _, _, _}}, Data) ->
                case util:is_between(From, Key, To) andalso Value =/= empty_val of
                    true ->
                        [{Key, Value} | Data];
                    false ->
                        Data
                end
        end,
    ?ETS:foldl(F, [], DB).

%% @doc get keys and versions in a range
get_range_with_version(DB, Interval) ->
    F = fun ({Key, {Value, WriteLock, ReadLock, Version}}, Data) ->
                case intervals:in(Key, Interval) andalso Value =/= empty_val of
                    true ->
                        [{Key, Value, WriteLock, ReadLock, Version} | Data];
                    false ->
                        Data
                end
        end,
    ?ETS:foldl(F, [], DB).

get_range_only_with_version(DB, Interval) ->
    F = fun ({Key, {Value, WLock, _, Version}}, Data) ->
                case WLock == false andalso intervals:in(Key, Interval) andalso Value =/= empty_val of
                    true ->
                        [{Key, Value, Version} | Data];
                    false ->
                        Data
                end
        end,
    ?ETS:foldl(F, [], DB).

%% @doc returns the key, which splits the data into two equally
%%      sized groups
get_middle_key(DB) ->
    case (Length = ?ETS:info(DB, size)) < 3 of
        true ->
            failed;
        false ->
            {ok, nth_key(DB, Length div 2 - 1)}
    end.

nth_key(DB, N) ->
    First = ?ETS:first(DB),
    nth_key_iter(DB, First, N).

nth_key_iter(_DB, Key, 0) ->
    Key;
nth_key_iter(DB, Key, N) ->
    nth_key_iter(DB, ?ETS:next(DB, Key), N - 1).
