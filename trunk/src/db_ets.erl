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
	 new/1, close/1]).

%%====================================================================
%% public functions
%%====================================================================

start_link(_InstanceId) ->
    ignore.

%% @doc initializes a new database; returns the DB name.
new(_) ->
    % ets prefix: DB_ + random name
    DBname = list_to_atom(string:concat("db_", randoms:getRandomId())),
    % better protected? All accesses would have to go to DB-process
    % ets:new(DBname, [ordered_set, protected, named_table]).
    ets:new(DBname, [ordered_set, public, named_table]).

%% delete DB (missing function)
close(DB) ->
    ets:delete(DB).

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

%% @doc get keys in a range
%% @spec get_range(db(), string(), string()) -> [{string(), string()}]
get_range(DB, From, To) ->
    [ {Key, Value} || {Key, {Value, _WLock, _RLock, _Vers}} <- ets:tab2list(DB),
                      util:is_between(From, Key, To), Value =/= empty_val ].

%% @doc get keys and versions in a range
%% @spec get_range_with_version(db(), intervals:interval()) -> [{Key::term(),
%%       Value::term(), Version::integer(), WriteLock::bool(), ReadLock::integer()}]
get_range_with_version(DB, Interval) ->
    {From, To} = intervals:unpack(Interval),
    [ {Key, Value, Version, WriteLock, ReadLock}
      || {Key, {Value, WriteLock, ReadLock, Version}} <- ets:tab2list(DB),
         util:is_between(From, Key, To), Value =/= empty_val ].

% get_range_with_version
%@private

get_range_only_with_version(DB, Interval) ->
    {From, To} = intervals:unpack(Interval),
    [ {Key, Value, Vers}
      || {Key, {Value, WLock, _RLock, Vers}} <- ets:tab2list(DB),
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
		undefined, ets:tab2list(DB)),
    MerkleTree.

-define(ETS, ets).
-include("db_generic_ets.hrl").
