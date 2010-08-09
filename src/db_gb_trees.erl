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

-opaque(db() :: {Table::gb_tree(), RecordChangesInterval::intervals:interval(), ChangedKeysTable::tid() | atom()}).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-define(CKETS, ets).

-include("db_common.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc initializes a new database
new(_NodeId) ->
    RandomName = randoms:getRandomId(),
    CKDBname = list_to_atom(string:concat("db_ck_", RandomName)), % changed keys
    {gb_trees:empty(), intervals:empty(), ?CKETS:new(CKDBname, [ordered_set, private | ?DB_ETS_ADDITIONAL_OPS])}.

%% delete DB (missing function)
close({_DB, _CKInt, CKDB}) ->
    ?CKETS:delete(CKDB).

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
get_entry(State, Key) ->
    {_Exists, Result} = get_entry2(State, Key),
    Result.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2({DB, _CKInt, _CKDB}, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, Entry} -> {true, Entry};
        none           -> {false, db_entry:new(Key)}
    end.

%% @doc Inserts a complete entry into the DB.
set_entry({DB, CKInt, CKDB}, Entry) ->
    case intervals:in(db_entry:get_key(Entry), CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {db_entry:get_key(Entry)})
    end,
    {gb_trees:enter(db_entry:get_key(Entry), Entry, DB), CKInt, CKDB}.

%% @doc Updates an existing (!) entry in the DB.
update_entry({DB, CKInt, CKDB}, Entry) ->
    case intervals:in(db_entry:get_key(Entry), CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {db_entry:get_key(Entry)})
    end,
    {gb_trees:update(db_entry:get_key(Entry), Entry, DB), CKInt, CKDB}.

%% @doc Removes all values with the given entry's key from the DB.
delete_entry({DB, CKInt, CKDB}, Entry) ->
    case intervals:in(db_entry:get_key(Entry), CKInt) of
        false -> ok;
        _     -> ?CKETS:insert(CKDB, {db_entry:get_key(Entry)})
    end,
    {gb_trees:delete_any(db_entry:get_key(Entry), DB), CKInt, CKDB}.

%% @doc Returns the number of stored keys.
get_load({DB, _CKInt, _CKDB}) ->
    gb_trees:size(DB).

%% @doc Adds all db_entry objects in the Data list.
add_data({DB, CKInt, CKDB}, Data) ->
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
split_data({DB, CKInt, CKDB}, MyNewInterval) ->
    {MyListTmp, HisListTmp} =
        lists:partition(fun(DBEntry) ->
                                intervals:in(db_entry:get_key(DBEntry),
                                             MyNewInterval)
                        end, gb_trees:values(DB)),
    % note: building [{Key, DBEntry}] from MyList should be more memory efficient
    % than using gb_trees:to_list(DB) above and removing Key from the tuples in
    % HisList
    MyNewList = [{db_entry:get_key(DBEntry), DBEntry} || DBEntry <- MyListTmp],
    HisList = [begin
                   ?CKETS:delete(CKDB, db_entry:get_key(DBEntry)),
                   DBEntry
               end || DBEntry <- HisListTmp, not db_entry:is_empty(DBEntry)],
    {{gb_trees:from_orddict(MyNewList), CKInt, CKDB}, HisList}.

%% @doc Get key/value pairs in the given range.
get_range_kv({DB, _CKInt, _CKDB}, Interval) ->
    F = fun (_Key, DBEntry, Data) ->
                case (not db_entry:is_empty(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Get key/value/version triples of non-write-locked entries in the given range.
get_range_kvv({DB, _CKInt, _CKDB}, Interval) ->
    F = fun (_Key, DBEntry, Data) ->
                case (not db_entry:is_empty(DBEntry)) andalso
                         (not db_entry:get_writelock(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry),
                              db_entry:get_version(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Gets db_entry objects in the given range.
get_range_entry({DB, _CKInt, _CKDB}, Interval) ->
    F = fun (_Key, DBEntry, Data) ->
                 case (not db_entry:is_empty(DBEntry)) andalso
                          intervals:in(db_entry:get_key(DBEntry), Interval) of
                     true -> [DBEntry | Data];
                     _    -> Data
                 end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Returns all DB entries.
get_data({DB, _CKInt, _CKDB}) ->
    gb_trees:values(DB).
