%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%%% @doc    In-process Database using gb_trees
%%% @end
%%% @version $Id$
-module(db_gb_trees).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).

-type db_t() :: {Table::gb_tree(), RecordChangesInterval::intervals:interval(), ChangedKeysTable::tid() | atom()}.

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-define(CKETS, ets).

-include("db_common.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initializes a new database.
new_() ->
    RandomName = randoms:getRandomId(),
    CKDBname = list_to_atom(string:concat("db_ck_", RandomName)), % changed keys
    {gb_trees:empty(), intervals:empty(), ?CKETS:new(CKDBname, [ordered_set, private | ?DB_ETS_ADDITIONAL_OPS])}.

%% @doc Re-opens a previously existing database (not supported by gb_trees
%%      -> create new DB).
open_(_FileName) ->
    log:log(warn, "[ Node ~w:db_gb_trees ] open/1 not supported, executing new/0 instead", [self()]),
    new().

%% @doc Closes and deletes the DB.
close_({_DB, _CKInt, CKDB}, _Delete) ->
    ?CKETS:delete(CKDB).

%% @doc Returns an empty string.
%%      Note: should return the name of the DB for open/1 which is not
%%      supported by gb_trees though).
get_name_(_State) ->
    "".

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2_({DB, _CKInt, _CKDB}, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, Entry} -> {true, Entry};
        none           -> {false, db_entry:new(Key)}
    end.

%% @doc Inserts a complete entry into the DB.
set_entry_({DB, CKInt, CKDB}, Entry) ->
    Key = db_entry:get_key(Entry),
    case intervals:in(Key, CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {Key})
    end,
    {gb_trees:enter(Key, Entry, DB), CKInt, CKDB}.

%% @doc Updates an existing (!) entry in the DB.
update_entry_({DB, CKInt, CKDB}, Entry) ->
    Key = db_entry:get_key(Entry),
    case intervals:in(Key, CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {Key})
    end,
    {gb_trees:update(Key, Entry, DB), CKInt, CKDB}.

%% @doc Removes all values with the given entry's key from the DB.
delete_entry_({DB, CKInt, CKDB}, Entry) ->
    Key = db_entry:get_key(Entry),
    case intervals:in(Key, CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {Key})
    end,
    {gb_trees:delete_any(Key, DB), CKInt, CKDB}.

%% @doc Returns the number of stored keys.
get_load_({DB, _CKInt, _CKDB}) ->
    gb_trees:size(DB).

%% @doc Adds all db_entry objects in the Data list.
add_data_({DB, CKInt, CKDB}, Data) ->
    % check once for the 'common case'
    case intervals:is_empty(CKInt) of
        true -> ok;
        _    -> [?CKETS:insert(CKDB, {db_entry:get_key(Entry)}) ||
                   Entry <- Data,
                   intervals:in(db_entry:get_key(Entry), CKInt)]
    end,
    % -> do not use set_entry (no further checks for changed keys necessary)
    NewDB = lists:foldl(
              fun(DBEntry, Tree) ->
                      gb_trees:enter(db_entry:get_key(DBEntry), DBEntry, Tree)
              end, DB, Data),
    {NewDB, CKInt, CKDB}.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
%%      Note: removes all keys not in MyNewInterval from the list of changed
%%      keys!
split_data_({DB, CKInt, CKDB}, MyNewInterval) ->
    % depending on the number of removed entries, a complete re-construction of
    % the tree using gb_trees:from_orddict could be faster than continuously
    % deleting entries - we do not know this beforehand though
    % -> do single deletes here
    F = fun(DBEntry, {HisList, MyTree}) ->
                Key = db_entry:get_key(DBEntry),
                case intervals:in(Key, MyNewInterval) of
                    true -> {HisList, MyTree};
                    _    -> NewTree = gb_trees:delete(Key, MyTree),
                            ?CKETS:delete(CKDB, Key),
                            case db_entry:is_empty(DBEntry) of
                                false -> {[DBEntry | HisList], NewTree};
                                _     -> {HisList, NewTree}
                            end
               end
        end,
    {HisList, MyNewTree} = lists:foldr(F, {[], DB}, gb_trees:values(DB)),
    {{MyNewTree, CKInt, CKDB}, HisList}.

%% @doc Gets all custom objects (created by ValueFun(DBEntry)) from the DB for
%%      which FilterFun returns true.
get_entries_({DB, _CKInt, _CKDB}, FilterFun, ValueFun) ->
    F = fun(_Key, DBEntry, Data) ->
                case FilterFun(DBEntry) of
                    true -> [ValueFun(DBEntry) | Data];
                    _    -> Data
                end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Deletes all objects in the given Range or (if a function is provided)
%%      for which the FilterFun returns true from the DB.
delete_entries_({DB, CKInt, CKDB}, FilterFun) when is_function(FilterFun) ->
    % depending on the number of removed entries, a complete re-construction of
    % the tree using gb_trees:from_orddict could be faster than continuously
    % deleting entries - we do not know this beforehand though
    % -> do single deletes here
    F = fun(DBEntry, AccDB) ->
                case FilterFun(DBEntry) of
                    false -> AccDB;
                    _     -> Key = db_entry:get_key(DBEntry),
                             case intervals:in(Key, CKInt) of
                                 true -> ?CKETS:insert(CKDB, {Key});
                                 _    -> ok
                             end,
                             gb_trees:delete(Key, AccDB)
                end
        end,
    NewDB = lists:foldr(F, DB, gb_trees:values(DB)),
    {NewDB, CKInt, CKDB};
delete_entries_(State, Interval) ->
    delete_entries_(State,
                    fun(E) ->
                            intervals:in(db_entry:get_key(E), Interval)
                    end).

%% @doc Returns all DB entries.
get_data_({DB, _CKInt, _CKDB}) ->
    gb_trees:values(DB).
