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
-vsn('$Id').

-include("scalaris.hrl").

-behaviour(db_beh).

-type(db()::gb_tree()).

% note: must include this file AFTER the type definitions for erlang < R13B04
% to work 
-include("db.hrl").

-export([start_link/1]).
-export([new/1, close/1]).
%% @TODO -export([get_entry/2, set_entry/2]).
-export([read/2, write/4, get_version/2]).
-export([delete/2]).
-export([set_write_lock/2, unset_write_lock/2,
         set_read_lock/2, unset_read_lock/2, get_locks/2]).
-export([get_range/3, get_range_with_version/2]).
-export([get_load/1, get_middle_key/1, split_data/3, get_data/1,
         add_data/2]).
-export([get_range_only_with_version/2,
         build_merkle_tree/2,
         update_if_newer/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(instanceid()) -> ignore.
start_link(_InstanceId) ->
    ignore.

%% @doc initializes a new database
new(_) ->
    gb_trees:empty().

%% delete DB (missing function)
close(_) ->
    ok.

%% @doc sets a write lock on a key.
%%      the write lock is a boolean value per key
set_write_lock(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {Value, false, 0, Version}} ->
            NewDB = gb_trees:update(Key,
                                    {Value, true, 0, Version},
                                    DB),
            {NewDB, ok};
        {value, {_Value, _WriteLock, _ReadLock, _Version}} ->
            {DB, failed};
        none ->
            % no value stored yet
            NewDB = gb_trees:enter(Key,
                                   {empty_val, true, 0, -1},
                                   DB),
            {NewDB, ok}
    end.

%% @doc unsets the write lock of a key
%%      the write lock is a boolean value per key
unset_write_lock(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {empty_val, true, 0, -1}} ->
            NewDB = gb_trees:delete(Key, DB),
            {NewDB, ok};
        {value, {Value, true, ReadLock, Version}} ->
            NewDB = gb_trees:update(Key,
                                    {Value, false, ReadLock, Version},
                                    DB),
            {NewDB, ok};
        {value, {_Value, false, _ReadLock, _Version}} ->
            {DB, failed};
        none ->
            {DB, failed}
    end.

%% @doc sets a read lock on a key
%%      the read lock is an integer value per key
set_read_lock(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {Value, false, ReadLock, Version}} ->
            NewDB = gb_trees:update(Key,
                                    {Value, false, ReadLock + 1, Version},
                                    DB),
            {NewDB, ok};
        {value, {_Value, _WriteLock, _ReadLock, _Version}} ->
            {DB, failed};
        none ->
            {DB, failed}
    end.

%% @doc unsets a read lock on a key
%%      the read lock is an integer value per key
unset_read_lock(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {_Value, _WriteLock, 0, _Version}} ->
            {DB, failed};
        {value, {Value, WriteLock, ReadLock, Version}} ->
            NewDB = gb_trees:update(Key,
                                    {Value, WriteLock, ReadLock - 1, Version},
                                    DB),
            {NewDB, ok};
        none ->
            {DB, failed}
    end.

%% @doc get the locks and version of a key
get_locks(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {_Value, WriteLock, ReadLock, Version}} ->
            {DB, {WriteLock, ReadLock, Version}};
        none ->
            {DB, failed}
    end.

%% @doc reads the version and value of a key
read(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {Value, _WriteLock, _ReadLock, Version}} ->
            {ok, Value, Version};
        none ->
            {ok, empty_val, -1}
    end.

%% @doc updates the value of key
write(DB, Key, Value, Version) ->
    case gb_trees:lookup(Key, DB) of
        {value, {_Value, WriteLock, ReadLock, _Version}} ->
            gb_trees:enter(Key,
                           {Value, WriteLock, ReadLock, Version},
                           DB);
        none ->
            gb_trees:enter(Key,
                           {Value, false, 0, Version},
                           DB)
    end.

%% @doc deletes the key
delete(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {_Value, false, 0, _Version}} ->
            {gb_trees:delete(Key, DB), ok};
        {value, _Value} ->
            {DB, locks_set};
        none ->
            {DB, undef}
    end.

%% @doc reads the version of a key
get_version(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
        {value, {_Value, _WriteLock, _ReadLock, Version}} ->
            {ok, Version};
        none ->
            failed
    end.

%% @doc returns the number of stored keys
get_load(DB) ->
    gb_trees:size(DB).

%% @doc returns the key, which splits the data into two equally
%%      sized groups
get_middle_key(DB) ->
    case (Length = gb_trees:size(DB)) < 3 of
        true ->
            failed;
        false ->
            Keys = gb_trees:keys(DB),
            Middle = Length div 2,
            MiddleKey = lists:nth(Middle, Keys),
            {ok, MiddleKey}
    end.

%% @doc returns all keys (and removes them from the db) which belong
%%      to a new node with id HisKey
split_data(DB, MyKey, HisKey) ->
    DataList = gb_trees:to_list(DB),
    {MyList, HisList} = lists:partition(fun ({Key, _}) -> util:is_between(HisKey, Key, MyKey) end, DataList),
    {gb_trees:from_orddict(MyList), HisList}.

%% @doc returns all keys
get_data(DB) ->
    gb_trees:to_list(DB).

%% @doc adds keys
add_data(DB, Data) ->
    lists:foldl(fun ({Key, Value}, Tree) -> gb_trees:enter(Key, Value, Tree) end, DB, Data).

%% @doc get keys in a range
%% @spec get_range(db(), string(), string()) -> [{string(), string()}]
get_range(DB, From, To) ->
    [ {Key, Value} || {Key, {Value, _WLock, _RLock, _Vers}} <- gb_trees:to_list(DB),
                      util:is_between(From, Key, To), Value =/= empty_val ].

%% @doc get keys and versions in a range
get_range_with_version(DB, Interval) ->
    {From, To} = intervals:unpack(Interval),
    [ {Key, Value, WriteLock, ReadLock, Version}
      || {Key, {Value, WriteLock, ReadLock, Version}} <- gb_trees:to_list(DB),
         util:is_between(From, Key, To), Value =/= empty_val ].

% get_range_with_version
%@private

get_range_only_with_version(DB, Interval) ->
    {From, To} = intervals:unpack(Interval),
    [ {Key, Value, Vers}
      || {Key, {Value, WLock, _RLock, Vers}} <- gb_trees:to_list(DB),
         WLock == false andalso util:is_between(From, Key, To), Value =/= empty_val ].


build_merkle_tree(DB, Range) ->
    {From, To} = intervals:unpack(Range),
    MerkleTree = lists:foldl(fun ({Key, {_, _, _, _Version}}, Tree) ->
                                     case util:is_between(From, Key, To) of
                                         true ->
                                             merkerl:insert({Key, 0}, Tree);
                                         false ->
                                             Tree
                                     end
                             end,
                undefined, gb_trees:to_list(DB)),
    MerkleTree.

% update only if no locks are taken and version number is higher
update_if_newer(OldDB,  KVs) ->
    F = fun ({Key, Value, Version}, DB) ->
                 case gb_trees:lookup(Key, DB) of
                     none ->
                         gb_trees:insert(Key, {Value, false, 0, Version}, DB);
                     {value, {_Value, false, 0, OldVersion}} when OldVersion < Version ->
                         gb_trees:update(Key, {Value, false, 0, Version}, DB);
                     _ -> DB
                end
        end,
    lists:foldl(F, OldDB, KVs).
