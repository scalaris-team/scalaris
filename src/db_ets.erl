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
%%% File    : db_ets.erl
%%% Author  : Florian Schintke <schintke@onscale.de>
%%% Description : In-process Database using ets
%%%
%%% Created : 21 Mar 2009 by Florian Schintke <schintke@onscale.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schintke@onscale.de>
%% @copyright 2009 onScale solutions
%% @version $Id $
-module(db_ets).

-author('schintke@onscale.de').
-vsn('$Id').

-behaviour(database).

-include("chordsharp.hrl").

-import(ct).
-import(randoms).
-import(string).

-type(key()::integer() | string()).

-ifdef(types_are_builtin).
-type(db()::atom()).
-else.
-type(db()::atom()).
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

%% @doc initializes a new database; returns the DB name.
new() ->
    % ets prefix: DB_ + random name
    DBname = list_to_atom(string:concat("db_", randoms:getRandomId())),
    % better protected? All accesses would have to go to DB-process
    % ets:new(DBname, [ordered_set, protected, named_table]).
    ets:new(DBname, [ordered_set, public, named_table]).

%% delete DB (missing function)

%% @doc sets a write lock on a key.
%%      the write lock is a boolean value per key
%% @spec set_write_lock(db(), string()) -> {db(), ok | failed}
set_write_lock(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {Value, false, 0, Version}}] ->
	    ets:insert(DB, {Key, {Value, true, 0, Version}}),
	    {DB, ok};
	[{Key, {_Value, _WriteLock, _ReadLock, _Version}}] ->
	    {DB, failed};
	[] ->
	    % no value stored yet
	    ets:insert(DB, {Key, {empty_val, true, 0, -1}}),
	    {DB, ok}
    end.

%% @doc unsets the write lock of a key
%%      the write lock is a boolean value per key
%% @spec unset_write_lock(db(), string()) -> {db(), ok | failed}
unset_write_lock(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {Value, true, ReadLock, Version}}] ->
	    ets:insert(DB, {Key, {Value, false, ReadLock, Version}}),
	    {DB, ok};
	[{Key, {_Value, false, _ReadLock, _Version}}] ->
	    {DB, failed};
	[] ->
	    {DB, failed}
    end.

%% @doc sets a read lock on a key
%%      the read lock is an integer value per key
%% @spec set_read_lock(db(), string()) -> {db(), ok | failed}
set_read_lock(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {Value, false, ReadLock, Version}}] ->
	    ets:insert(DB, {Key, {Value, false, ReadLock + 1, Version}}),
	    {DB, ok};
	[{Key, {_Value, _WriteLock, _ReadLock, _Version}}] ->
	    {DB, failed};
	[] ->
	    {DB, failed}
    end.

%% @doc unsets a read lock on a key
%%      the read lock is an integer value per key
%% @spec unset_read_lock(db(), string()) -> {db(), ok | failed}
unset_read_lock(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {_Value, _WriteLock, 0, _Version}}] ->
	    {DB, failed};
	[{Key, {Value, WriteLock, ReadLock, Version}}] ->
	    ets:insert(DB, {Key, {Value, WriteLock, ReadLock - 1, Version}}),
	    {DB, ok};
	[] ->
	    {DB, failed}
    end.

%% @doc get the locks and version of a key
%% @spec get_locks(db(), string()) -> {bool(), int(), int()}| failed
get_locks(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {_Value, WriteLock, ReadLock, Version}}] ->
	    {DB, {WriteLock, ReadLock, Version}};
	[] ->
	    {DB, failed}
    end.

%% @doc reads the version and value of a key
%% @spec read(db(), string()) -> {ok, string(), integer()} | failed
read(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {Value, _WriteLock, _ReadLock, Version}}] ->
	    {ok, Value, Version};
	[] ->
	    failed
    end.

%% @doc updates the value of key
%% @spec write(db(), string(), string(), integer()) -> db()
write(DB, Key, Value, Version) ->
    case ets:lookup(DB, Key) of
	[{Key, {_Value, WriteLock, ReadLock, _Version}}] ->
            % better use ets:update_element?
	    ets:insert(DB, {Key, {Value, WriteLock, ReadLock, Version}});
	[] ->
	    ets:insert(DB, {Key, {Value, false, 0, Version}})
    end,
    DB.

%% @doc deletes the key
-spec(delete/2 :: (db(), key()) -> {db(), ok | locks_set
				    | undef}).
delete(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {_Value, false, 0, _Version}}] ->
	    ets:delete(DB, Key),
	    {DB, ok};
	[{Key, _Value}] ->
	    {DB, locks_set};
	[] ->
	    {DB, undef}
    end.

%% @doc reads the version of a key
%% @spec get_version(db(), string()) -> {ok, integer()} | failed
get_version(DB, Key) ->
    case ets:lookup(DB, Key) of
	[{Key, {_Value, _WriteLock, _ReadLock, Version}}] ->
	    {ok, Version};
	[] ->
	    failed
    end.




%% @doc returns the number of stored keys
%% @spec get_load(db()) -> integer()
get_load(DB) ->
    case ets:info(DB, size) of
        undefined -> 0;
        Result -> Result
    end.

%% @doc returns the key, which splits the data into two equally 
%%      sized groups
%% @spec get_middle_key(db()) -> {ok, string()} | failed
get_middle_key(DB) ->
    case (Length = ets:info(DB, size)) < 3 of
	true -> failed;
	false ->
	    Keys = ets:tab2list(DB),
            Middle = Length div 2,
	    {MiddleKey, _Val} = lists:nth(Middle, Keys),
	    {ok, MiddleKey}
    end.

%% @doc returns all keys (and removes them from the db) which belong 
%%      to a new node with id HisKey
-spec(split_data/3 :: (db(), key(), key()) -> {db(), [{key(), {key(), bool(), integer(), integer()}}]}).
split_data(DB, MyKey, HisKey) ->
    DataList = ets:tab2list(DB),
    {_MyList, HisList} = lists:partition(fun ({Key, _}) -> util:is_between(HisKey, Key, MyKey) end, DataList),
    [ ets:delete(DB, AKey) || {AKey, _} <- HisList],
    {DB, HisList}.

%% @doc returns all keys
%% @spec get_data(db()) -> [{string(), {string(), bool(), integer(), integer()}}]
get_data(DB) ->
    ets:tab2list(DB).

%% @doc adds keys
%% @spec add_data(db(), [{string(), {string(), bool(), integer(), integer()}}]) -> any()
add_data(DB, Data) ->
    ets:insert(DB, Data),
    DB.

%% @doc get keys in a range
%% @spec get_range(db(), string(), string()) -> [{string(), string()}]
get_range(_DB, _From, _To) ->
    ct:pal("db_ets: get range not implemented yet.~n").

%% @doc get keys and versions in a range
%% @spec get_range_with_version(db(), intervals:interval()) -> [{Key::term(), 
%%       Value::term(), Version::integer(), WriteLock::bool(), ReadLock::integer()}]
get_range_with_version(_DB, _Interval) ->    
    ct:pal("db_ets: get range with version not implemented yet.~n").

% get_range_with_version
%@private

get_range_only_with_version(_DB, _Interval) ->
    ct:pal("db_ets: get range only with version not implemented yet.~n").

build_merkle_tree(_DB, _Range) ->
    ct:pal("db_ets: build merkle tree not implemented yet.~n").

% update only if no locks are taken and version number is higher
update_if_newer(_OldDB, _KVs) ->
    ct:pal("db_ets: update_if_newer not implemented yet.~n").

