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
%% -include("scalaris.hrl").

-include("db_common.hrl").

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
get_entry(DB, Key) ->
    {_Exists, Result} = get_entry2(DB, Key),
    Result.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2(DB, Key) ->
%%    Start = erlang:now(),
    Result = case ?ETS:lookup(DB, Key) of
                 [Entry] -> {true, Entry};
                 []      -> {false, db_entry:new(Key)}
             end,
%%     Stop = erlang:now(),
%%     Span = timer:now_diff(Stop, Start),
%%     case ?ETS:lookup(profiling, db_read_lookup) of
%%         [] ->
%%             ?ETS:insert(profiling, {db_read_lookup, Span});
%%         [{_, Sum}] ->
%%             ?ETS:insert(profiling, {db_read_lookup, Sum + Span})
%%     end,
    Result.

%% @doc Inserts a complete entry into the DB.
set_entry(DB, Entry) ->
    ?ETS:insert(DB, Entry),
    DB.

%% @doc Updates an existing (!) entry in the DB.
%%      TODO: use ets:update_element here?
update_entry(DB, Entry) ->
    ?ETS:insert(DB, Entry),
    DB.

%% @doc Removes all values with the given entry's key from the DB.
delete_entry(DB, Entry) ->
    ?ETS:delete(DB, db_entry:get_key(Entry)),
    DB.

%% @doc returns the number of stored keys
get_load(DB) ->
    ?ETS:info(DB, size).

%% @doc adds keys
add_data(DB, Data) ->
    ?ETS:insert(DB, Data),
    DB.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
split_data(DB, MyNewInterval) ->
    F = fun (DBEntry, HisList) ->
                case not db_entry:is_empty(DBEntry) andalso
                         intervals:in(db_entry:get_key(DBEntry), MyNewInterval) of
                    true -> HisList;
                    _    ->
                        delete_entry(DB, DBEntry),
                        [DBEntry | HisList]
                end
        end,
    HisList = ?ETS:foldl(F, [], DB),
    {DB, HisList}.

%% @doc Get key/value pairs in the given range.
get_range(DB, Interval) ->
    F = fun (DBEntry, Data) ->
                case (not db_entry:is_empty(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    ?ETS:foldl(F, [], DB).

%% @doc Gets db_entry objects in the given range.
get_range_with_version(DB, Interval) ->
    F = fun (DBEntry, Data) ->
                 case (not db_entry:is_empty(DBEntry)) andalso
                          intervals:in(db_entry:get_key(DBEntry), Interval) of
                     true -> [DBEntry | Data];
                     _    -> Data
                 end
        end,
    ?ETS:foldl(F, [], DB).

%% @doc Get key/value/version triples in the given range.
get_range_only_with_version(DB, Interval) ->
    F = fun (DBEntry, Data) ->
                case (not db_entry:is_empty(DBEntry)) andalso
                         db_entry:get_writelock(DBEntry) =:= false andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry),
                              db_entry:get_version(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    ?ETS:foldl(F, [], DB).

%% @doc Returns the key, which splits the data into two equally sized groups.
get_middle_key(DB) ->
    case (Length = ?ETS:info(DB, size)) < 3 of
        true -> failed;
        _    -> {ok, nth_key(DB, Length div 2 - 1)}
    end.

nth_key(DB, N) ->
    First = ?ETS:first(DB),
    nth_key_iter(DB, First, N).

nth_key_iter(_DB, Key, 0) ->
    Key;
nth_key_iter(DB, Key, N) ->
    nth_key_iter(DB, ?ETS:next(DB, Key), N - 1).
