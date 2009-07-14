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
%%% File    : db_generic_ets.hrl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : generic db code for ets
%%%
%%% Created : 13 Jul 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum f<FC>r Informationstechnik Berlin
%% @version $Id $

%% @doc sets a write lock on a key.
%%      the write lock is a boolean value per key
-spec(set_write_lock/2 :: (db(), string()) -> {db(), ok | failed}).
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
%% @spec unset_write_lock(db(), string()) -> {db(), ok | failed}
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
%% @spec set_read_lock(db(), string()) -> {db(), ok | failed}
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
%% @spec unset_read_lock(db(), string()) -> {db(), ok | failed}
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
%% @spec get_locks(db(), string()) -> {bool(), int(), int()}| failed
get_locks(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
	[{Key, {_Value, WriteLock, ReadLock, Version}}] ->
	    {DB, {WriteLock, ReadLock, Version}};
	[] ->
	    {DB, failed}
    end.

%% @doc reads the version and value of a key
%% @spec read(db(), string()) -> {ok, string(), integer()} | failed
read(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
	[{Key, {empty_val, true, 0, -1}}] ->
            failed;
	[{Key, {Value, _WriteLock, _ReadLock, Version}}] ->
	    {ok, Value, Version};
	[] ->
	    failed
    end.

%% @doc updates the value of key
%% @spec write(db(), string(), string(), integer()) -> db()
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
-spec(delete/2 :: (db(), key()) -> {db(), ok | locks_set
				    | undef}).
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
%% @spec get_version(db(), string()) -> {ok, integer()} | failed
get_version(DB, Key) ->
    case ?ETS:lookup(DB, Key) of
	[{Key, {_Value, _WriteLock, _ReadLock, Version}}] ->
	    {ok, Version};
	[] ->
	    failed
    end.




%% @doc returns the number of stored keys
%% @spec get_load(db()) -> integer()
get_load(DB) ->
    ?ETS:info(DB, size).

%% @doc adds keys
%% @spec add_data(db(), [{string(), {string(), bool(), integer(), integer()}}]) -> any()
add_data(DB, Data) ->
    ?ETS:insert(DB, Data),
    DB.


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
