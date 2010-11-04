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

-type db_t() :: {{DB::pid(), FileName::string()}, RecordChangesInterval::intervals:interval(), ChangedKeysTable::tid() | atom()}.

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
    file:make_dir(FullDir),
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
            CKDBname = list_to_atom(string:concat("db_ck_", RandomName)), % changed keys
            case toke_drv:open(DB, FileName, TokeOptions) of
                ok     -> {{DB, FileName}, intervals:empty(), ?CKETS:new(CKDBname, [ordered_set, private | ?DB_ETS_ADDITIONAL_OPS])};
                Error2 -> log:log(error, "[ Node ~w:db_toke ] ~.0p", [self(), Error2]),
                          erlang:error({toke_failed, Error2})
            end;
        Error1 ->
            log:log(error, "[ Node ~w:db_toke ] ~.0p", [self(), Error1]),
            erlang:error({toke_failed, Error1})
    end.

%% @doc Deletes all contents of the given DB.
close_({{DB, FileName}, _CKInt, CKDB}, Delete) ->
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
    end,
    ?CKETS:delete(CKDB).

%% @doc Returns the name of the DB, i.e. the path to its file, which can be
%%      used with open/1.
get_name_({{_DB, FileName}, _CKInt, _CKDB}) ->
    FileName.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2_({{DB, _FileName}, _CKInt, _CKDB}, Key) ->
    case toke_drv:get(DB, erlang:term_to_binary(Key)) of
        not_found -> {false, db_entry:new(Key)};
        Entry     -> {true, erlang:binary_to_term(Entry)}
    end.

%% @doc Inserts a complete entry into the DB.
set_entry_(State = {{DB, _FileName}, CKInt, CKDB}, Entry) ->
    Key = db_entry:get_key(Entry),
    case intervals:in(Key, CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {Key})
    end,
    ok = toke_drv:insert(DB, erlang:term_to_binary(Key),
                         erlang:term_to_binary(Entry)),
    State.

%% @doc Updates an existing (!) entry in the DB.
update_entry_(State, Entry) ->
    set_entry_(State, Entry).

%% @doc Removes all values with the given entry's key from the DB.
delete_entry_(State = {{DB, _FileName}, CKInt, CKDB}, Entry) ->
    Key = db_entry:get_key(Entry),
    case intervals:in(Key, CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {Key})
    end,
    toke_drv:delete(DB, erlang:term_to_binary(Key)),
    State.

%% @doc Returns the number of stored keys.
get_load_({{DB, _FileName}, _CKInt, _CKDB}) ->
    % TODO: not really efficient (maybe store the load in the DB?)
    toke_drv:fold(fun (_K, _V, Acc) -> Acc + 1 end, 0, DB).

%% @doc Adds all db_entry objects in the Data list.
add_data_(State = {{DB, _FileName}, CKInt, CKDB}, Data) ->
    % check once for the 'common case'
    case intervals:is_empty(CKInt) of
        true -> ok;
        _    -> [?CKETS:insert(CKDB, {db_entry:get_key(Entry)}) ||
                   Entry <- Data,
                   intervals:in(db_entry:get_key(Entry), CKInt)]
    end,
    % -> do not use set_entry (no further checks for changed keys necessary)
    lists:foldl(
      fun(DBEntry, _) ->
              ok = toke_drv:insert(DB,
                                   erlang:term_to_binary(db_entry:get_key(DBEntry)),
                                   erlang:term_to_binary(DBEntry))
      end, null, Data),
    State.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
%%      Note: removes all keys not in MyNewInterval from the list of changed
%%      keys!
split_data_(State = {{DB, _FileName}, _CKInt, CKDB}, MyNewInterval) ->
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
                  toke_drv:delete(DB, erlang:term_to_binary(db_entry:get_key(DBEntry))),
                  ?CKETS:delete(CKDB, db_entry:get_key(DBEntry)),
                  case db_entry:is_empty(DBEntry) of
                      false -> [DBEntry | L];
                      _     -> L
                  end
          end, [], HisList),
    {State, HisListFilt}.

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%%      which FilterFun returns true.
get_entries_({{DB, _FileName}, _CKInt, _CKDB}, FilterFun, ValueFun) ->
    F = fun (_Key, DBEntry_, Data) ->
                 DBEntry = erlang:binary_to_term(DBEntry_),
                 case FilterFun(DBEntry) of
                     true -> [ValueFun(DBEntry) | Data];
                     _    -> Data
                 end
        end,
    toke_drv:fold(F, [], DB).

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
delete_entries_(State = {{DB, _FileName}, CKInt, CKDB}, FilterFun) when is_function(FilterFun) ->
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
    [begin
         toke_drv:delete(DB, KeyToke),
         case intervals:in(Key, CKInt) of
             true -> ?CKETS:insert(CKDB, {Key});
             _    -> ok
         end
     end || {KeyToke, Key} <- KeysToDelete],
    State;
delete_entries_(State, Interval) ->
    delete_entries_(State,
                    fun(E) ->
                            intervals:in(db_entry:get_key(E), Interval)
                    end).

%% @doc Returns all DB entries.
get_data_({{DB, _FileName}, _CKInt, _CKDB}) ->
    toke_drv:fold(fun (_K, DBEntry, Acc) ->
                           [erlang:binary_to_term(DBEntry) | Acc]
                  end, [], DB).
