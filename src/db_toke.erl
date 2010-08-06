%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-type(db()::atom()).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-include("db_common.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initializes a new database (will launch a process for it); returns the
%%      a reference to the DB.
new(Id) ->
    Dir = lists:flatten(io_lib:format("~s/~s", [config:read(db_directory),
                                  atom_to_list(node())])),
    file:make_dir(Dir),
    FileName = lists:flatten(io_lib:format("~s/db_~p.tch", [Dir, Id])),
    {ok, DB} = toke_drv:start_link(),
    case toke_drv:new(DB) of
        ok ->
            case toke_drv:open(DB, FileName, [read,write,create,truncate]) of
                ok     -> DB;
                Error2 -> log:log(error, "[ TOKE ] ~p", [Error2])
            end;
        Error1 ->
            log:log(error, "[ TOKE ] ~p", [Error1])
    end.

%% @doc Deletes all contents of the given DB.
close(DB) ->
    toke_drv:close(DB),
    toke_drv:delete(DB),
    toke_drv:stop(DB).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
get_entry(DB, Key) ->
    {_Exists, Result} = get_entry2(DB, Key),
    Result.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2(DB, Key) ->
    case toke_drv:get(DB, erlang:term_to_binary(Key)) of
        not_found -> {false, db_entry:new(Key)};
        Entry     -> {true, erlang:binary_to_term(Entry)}
    end.

%% @doc Inserts a complete entry into the DB.
set_entry(DB, Entry) ->
    ok = toke_drv:insert(DB, erlang:term_to_binary(db_entry:get_key(Entry)),
                         erlang:term_to_binary(Entry)),
    DB.

%% @doc Updates an existing (!) entry in the DB.
update_entry(DB, Entry) ->
    ok = toke_drv:insert(DB, erlang:term_to_binary(db_entry:get_key(Entry)),
                         erlang:term_to_binary(Entry)),
    DB.

%% @doc Removes all values with the given entry's key from the DB.
delete_entry(DB, Entry) ->
    toke_drv:delete(DB, erlang:term_to_binary(db_entry:get_key(Entry))),
    DB.

%% @doc Returns the number of stored keys.
get_load(DB) ->
    % TODO: not really efficient (maybe store the load in the DB?)
    toke_drv:fold(fun (_K, _V, Acc) -> Acc + 1 end, 0, DB).

%% @doc Adds all db_entry objects in the Data list.
add_data(DB, Data) ->
    lists:foldl(fun(DBEntry, _) -> set_entry(DB, DBEntry) end, DB, Data).

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
split_data(DB, MyNewInterval) ->
    F = fun(_K, DBEntry_, HisList) ->
                DBEntry = erlang:binary_to_term(DBEntry_),
                case intervals:in(db_entry:get_key(DBEntry), MyNewInterval) of
                    true -> HisList;
                    _    -> [DBEntry | HisList]
                end
        end,
    HisList = toke_drv:fold(F, [], DB),
    % delete empty entries from HisList and remove all entries in HisList from the DB
    HisListFilt = lists:foldl(fun(DBEntry, L) ->
                                      delete_entry(DB, DBEntry),
                                      case db_entry:is_empty(DBEntry) of
                                          false -> [DBEntry | L];
                                          _     -> L
                                      end
                              end, [], HisList),
    {DB, HisListFilt}.

%% @doc Get key/value pairs in the given range.
get_range_kv(DB, Interval) ->
    F = fun(_K, DBEntry_, Data) ->
                DBEntry = erlang:binary_to_term(DBEntry_),
                case (not db_entry:is_empty(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    toke_drv:fold(F, [], DB).

%% @doc Get key/value/version triples of non-write-locked entries in the given range.
get_range_kvv(DB, Interval) ->
    F = fun(_K, DBEntry_, Data) ->
                DBEntry = erlang:binary_to_term(DBEntry_),
                case (not db_entry:is_empty(DBEntry)) andalso
                         (not db_entry:get_writelock(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry),
                              db_entry:get_version(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    toke_drv:fold(F, [], DB).

%% @doc Gets db_entry objects in the given range.
get_range_entry(DB, Interval) ->
    F = fun(_K, DBEntry_, Data) ->
                DBEntry = erlang:binary_to_term(DBEntry_),
                case (not db_entry:is_empty(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [DBEntry | Data];
                    _    -> Data
                end
        end,
    toke_drv:fold(F, [], DB).

%% @doc Returns all DB entries.
get_data(DB) ->
    toke_drv:fold(fun (_K, DBEntry, Acc) ->
                           [erlang:binary_to_term(DBEntry) | Acc]
                  end, [], DB).
