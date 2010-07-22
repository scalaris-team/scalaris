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

-type(db()::gb_tree()).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-include("db_common.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc initializes a new database
new(_) ->
    gb_trees:empty().

%% delete DB (missing function)
close(_) ->
    ok.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
get_entry(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, Entry} -> Entry;
        none           -> db_entry:new(Key)
    end.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, Entry} -> {true, Entry};
        none           -> {false, db_entry:new(Key)}
    end.

%% @doc Inserts a complete entry into the DB.
set_entry(DB, Entry) ->
    gb_trees:enter(db_entry:get_key(Entry), Entry, DB).

%% @doc Updates an existing (!) entry in the DB.
update_entry(DB, Entry) ->
    gb_trees:update(db_entry:get_key(Entry), Entry, DB).

%% @doc Removes all values with the given entry's key from the DB.
delete_entry(DB, Entry) ->
    gb_trees:delete_any(db_entry:get_key(Entry), DB).

%% @doc Returns the number of stored keys.
get_load(DB) ->
    gb_trees:size(DB).

%% @doc Adds all db_entry objects in the Data list.
add_data(DB, Data) ->
    lists:foldl(fun(DBEntry, Tree) -> set_entry(Tree, DBEntry) end, DB, Data).

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
split_data(DB, MyNewInterval) ->
    {MyList, HisList} =
        lists:partition(fun(DBEntry) ->
                                (not db_entry:is_empty(DBEntry)) andalso
                                    intervals:in(db_entry:get_key(DBEntry), MyNewInterval)
                        end,
                        gb_trees:values(DB)),
    % note: building [{Key, Val}] from MyList should be more memory efficient
    % than using gb_trees:to_list(DB) above and removing Key from the tuples in
    % HisList
    {gb_trees:from_orddict([ {db_entry:get_key(DBEntry), DBEntry} ||
                                DBEntry <- MyList]), HisList}.

%% @doc Get key/value pairs in the given range.
get_range(DB, Interval) ->
    F = fun (_Key, DBEntry, Data) ->
                case (not db_entry:is_empty(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Gets db_entry objects in the given range.
get_range_with_version(DB, Interval) ->
    F = fun (_Key, DBEntry, Data) ->
                 case (not db_entry:is_empty(DBEntry)) andalso
                          intervals:in(db_entry:get_key(DBEntry), Interval) of
                     true -> [DBEntry | Data];
                     _    -> Data
                 end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Get key/value/version triples of non-write-locked entries in the given range.
get_range_only_with_version(DB, Interval) ->
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

%% @doc Returns all DB entries.
get_data(DB) ->
    gb_trees:values(DB).
