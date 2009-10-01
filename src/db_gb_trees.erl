%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : db_gb_trees.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : In-process Database using gb_trees
%%%
%%% Created : 19 Dec 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(db_gb_trees).

-author('schuett@zib.de').
-vsn('$Id').

-behaviour(database).

-include("chordsharp.hrl").

-import(ct).

-type(key()::integer() | string()).

-ifdef(types_are_builtin).
-type(db()::gb_tree()).
-else.
-type(db()::gb_trees:gb_tree()).
-endif.

-export([start_link/1,
	 set_write_lock/2, unset_write_lock/2, set_read_lock/2, 
	 unset_read_lock/2, get_locks/2,

	 read/2, write/4, get_version/2, 

	 delete/2,

	 get_range/3, get_range_with_version/2,

	 get_load/1, get_middle_key/1, split_data/3, get_data/1, 
	 add_data/2,
	 get_range_only_with_version/2,
	 build_merkle_tree/2,
	 update_if_newer/2,
	 new/0]).

%%====================================================================
%% public functions
%%====================================================================

start_link(_InstanceId) ->
    ignore.

%% @doc initializes a new database
new() ->
    gb_trees:empty().

%% @doc sets a write lock on a key.
%%      the write lock is a boolean value per key
%% @spec set_write_lock(db(), string()) -> {db(), ok | failed}
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
%% @spec unset_write_lock(db(), string()) -> {db(), ok | failed}
unset_write_lock(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
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
%% @spec set_read_lock(db(), string()) -> {db(), ok | failed}
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
%% @spec unset_read_lock(db(), string()) -> {db(), ok | failed}
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
%% @spec get_locks(db(), string()) -> {bool(), int(), int()}| failed
get_locks(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
	{value, {_Value, WriteLock, ReadLock, Version}} ->
	    {DB, {WriteLock, ReadLock, Version}};
	none ->
	    {DB, failed}
    end.

%% @doc reads the version and value of a key
%% @spec read(db(), string()) -> {ok, string(), integer()} | failed
read(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
	{value, {Value, _WriteLock, _ReadLock, Version}} ->
	    {ok, Value, Version};
	none ->
	    failed
    end.

%% @doc updates the value of key
%% @spec write(db(), string(), string(), integer()) -> db()
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
-spec(delete/2 :: (db(), key()) -> {db(), ok | locks_set
				    | undef}).
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
%% @spec get_version(db(), string()) -> {ok, integer()} | failed
get_version(DB, Key) ->
    case gb_trees:lookup(Key, DB) of
	{value, {_Value, _WriteLock, _ReadLock, Version}} ->
	    {ok, Version};
	none ->
	    failed
    end.

%% @doc returns the number of stored keys
%% @spec get_load(db()) -> integer()
get_load(DB) ->
    gb_trees:size(DB).

%% @doc returns the key, which splits the data into two equally 
%%      sized groups
%% @spec get_middle_key(db()) -> {ok, string()} | failed
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
-spec(split_data/3 :: (db(), key(), key()) -> {db(), [{key(), {key(), bool(), integer(), integer()}}]}).
split_data(DB, MyKey, HisKey) ->
    DataList = gb_trees:to_list(DB),
    {MyList, HisList} = lists:partition(fun ({Key, _}) -> util:is_between(HisKey, Key, MyKey) end, DataList),
    {gb_trees:from_orddict(MyList), HisList}.

%% @doc returns all keys
%% @spec get_data(db()) -> [{string(), {string(), bool(), integer(), integer()}}]
get_data(DB) ->
    gb_trees:to_list(DB).

%% @doc adds keys
%% @spec add_data(db(), [{string(), {string(), bool(), integer(), integer()}}]) -> any()
add_data(DB, Data) ->
    lists:foldl(fun ({Key, Value}, Tree) -> gb_trees:enter(Key, Value, Tree) end, DB, Data).

%% @doc get keys in a range
%% @spec get_range(db(), string(), string()) -> [{string(), string()}]
get_range(DB, From, To) ->
    [ {Key, Value} || {Key, {Value, _WLock, _RLock, _Vers}} <- gb_trees:to_list(DB),
                      util:is_between(From, Key, To) ].

%% @doc get keys and versions in a range
%% @spec get_range_with_version(db(), intervals:interval()) -> [{Key::term(),
%%       Value::term(), Version::integer(), WriteLock::bool(), ReadLock::integer()}]
get_range_with_version(DB, Interval) ->
    {From, To} = intervals:unpack(Interval),
    [ {Key, Value, Version, WriteLock, ReadLock}
      || {Key, {Value, WriteLock, ReadLock, Version}} <- gb_trees:to_list(DB),
         util:is_between(From, Key, To) ].

% get_range_with_version
%@private

get_range_only_with_version(DB, Interval) ->
    {From, To} = intervals:unpack(Interval),
    [ {Key, Value, Vers}
      || {Key, {Value, WLock, _RLock, Vers}} <- gb_trees:to_list(DB),
         WLock == false andalso util:is_between(From, Key, To) ].


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
		    {value, {_Value, WriteLock, ReadLock, OldVersion}} ->
			case not WriteLock andalso ReadLock == 0 andalso OldVersion < Version of
			    true ->
				gb_trees:update(Key, 
						{Value, WriteLock, ReadLock, Version}, 
						DB);
			    false ->
				DB
			end
		end
	end, 
    lists:foldl(F, OldDB, KVs).
