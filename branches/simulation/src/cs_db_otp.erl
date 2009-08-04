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
%%% File    : cs_db_otp.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Database functions
%%%
%%% Created : 26 Mar 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(cs_db_otp).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_server).
-behaviour(database).

-include("chordsharp.hrl").

-type(key()::database:key()).

-type(db()::ok).

-export([start_link/1, start/1, init/1, handle_call/3, handle_cast/2, 
	 handle_info/2, code_change/3, terminate/2, stop/0,
	 
	 set_write_lock/2, unset_write_lock/2, set_read_lock/2, 
	 unset_read_lock/2, get_locks/2,

	 read/2, write/4, delete/2, get_version/2, 

	 get_range/3, get_range_with_version/2, get_range_only_with_version/2,

	 get_load/1, get_middle_key/1, split_data/3, get_data/1, 
	 add_data/2,
	new/1, close/1,

	build_merkle_tree/2,
	update_if_newer/2]).

%% for testing purpose
-export([print_locked_items/0]).

%%====================================================================
%% public functions
%%====================================================================

get_pid() ->
    InstanceId = erlang:get(instance_id),
    if
	InstanceId == undefined ->
	   log:log(error,"~p", [util:get_stacktrace()]);
	true ->
	    ok
    end,
    process_dictionary:lookup_process(InstanceId, cs_db_otp).

%% @doc initializes a new database
new(_) ->
    gen_server:call(get_pid(), {drop_everything}, 20000),
    ok.

%% @todo
close(_) ->
    ok.
%% @doc sets a write lock on a key.
%%      the write lock is a boolean value per key
%% @spec set_write_lock(db(), string()) -> {db(), ok | failed}
set_write_lock(ok = DB, Key) ->
    {DB, gen_server:call(get_pid(), {set_write_lock, Key}, 20000)}.

%% @doc unsets the write lock of a key
%%      the write lock is a boolean value per key
%% @spec unset_write_lock(db(), string()) -> {db(), ok | failed}
unset_write_lock(ok = DB, Key) ->
    {DB, gen_server:call(get_pid(), {unset_write_lock, Key}, 20000)}.

%% @doc sets a read lock on a key
%%      the read lock is an integer value per key
%% @spec set_read_lock(db(), string()) -> {db(), ok | failed}
set_read_lock(ok = DB, Key) ->
    {DB, gen_server:call(get_pid(), {set_read_lock, Key}, 20000)}.

%% @doc unsets a read lock on a key
%%      the read lock is an integer value per key
%% @spec unset_read_lock(db(), string()) -> {db(), ok | failed}
unset_read_lock(ok = DB, Key) ->
    {DB, gen_server:call(get_pid(), {unset_read_lock, Key}, 20000)}.

%% @doc get the locks and version of a key
%% @spec get_locks(db(), string()) -> {bool(), int(), int()}| failed
get_locks(ok = _DB, Key) ->
    gen_server:call(get_pid(), {get_locks, Key}, 20000).

%% @doc reads the version and value of a key
%% @spec read(db(), string()) -> {ok, string(), integer()} | failed
read(ok = _DB, Key) ->
    gen_server:call(get_pid(), {read, Key}, 20000).

%% @doc updates the value of key
%% @spec write(db(), string(), string(), integer()) -> db()
write(ok = DB, Key, Value, Version) ->
    gen_server:call(get_pid(), {write, Key, Value, Version}, 20000),
    DB.

%% @doc deletes the key
-spec(delete/2 :: (db(), key()) -> {db(), ok | locks_set
				    | undef}).
delete(ok = _DB, Key) ->
    gen_server:call(get_pid(), {delete, Key}, 20000).

%% @doc reads the version of a key
%% @spec get_version(db(), string()) -> {ok, integer()} | failed
get_version(ok = _DB, Key) ->
    gen_server:call(get_pid(), {get_version, Key}, 20000).

%% @doc returns the number of stored keys
%% @spec get_load(db()) -> integer()
get_load(ok = _DB) ->
    gen_server:call(get_pid(), {get_load}, 20000).

%% @doc returns the key, which splits the data into two equally 
%%      sized groups
%% @spec get_middle_key(db()) -> {ok, string()} | failed
get_middle_key(ok = _DB) ->
    gen_server:call(get_pid(), {get_middle_key}, 20000).

%% @doc returns all keys (and removes them from the db) which belong 
%%      to a new node with id HisKey
-spec(split_data/3 :: (db(), key(), key()) -> {db(), [{key(), {key(), bool(), integer(), integer()}}]}).
split_data(ok = DB, MyKey, HisKey) ->
    {DB, gen_server:call(get_pid(), {split_data, MyKey, HisKey}, 20000)}.

%% @doc returns all keys
%% @spec get_data(db()) -> [{string(), {string(), bool(), integer(), integer()}}]
get_data(ok = _DB) ->
    gen_server:call(get_pid(), {get_data}, 20000).
    
%% @doc adds keys
%% @spec add_data(db(), [{string(), {string(), bool(), integer(), integer()}}]) -> any()
add_data(ok = DB, Data) ->
    gen_server:call(get_pid(), {add_data, Data}, 20000),
    DB.
    
%% @doc get keys in a range
%% @spec get_range(db(), string(), string()) -> [{string(), string()}]
get_range(ok = _DB, From, To) ->
    gen_server:call(get_pid(), {get_range, From, To}, 20000).

%% @doc get keys, locks, and versions in a range
%% @spec get_range_with_version(db(), intervals:interval()) -> [{Key::term(), 
%%       Value::term(), Version::integer(), WriteLock::bool(), ReadLock::integer()}]
get_range_with_version(ok = _DB, Interval) ->    
    gen_server:call(get_pid(), {get_range_with_version, Interval}, 20000).

%% @doc get keys and versions in a range
%% @spec get_range_only_with_version(db(), intervals:interval()) -> [{Key::term(), 
%%       Value::term(), Version::integer()}]
get_range_only_with_version(ok = _DB, Interval) ->    
    gen_server:call(get_pid(), {get_range_only_with_version, Interval}, 20000).

%% @doc build merkle tree for data, key as is and value will be the version number
-spec(build_merkle_tree/2 :: (db(), intervals:interval()) -> merkerl:tree()).
build_merkle_tree(ok = _DB, Range) ->
    gen_server:call(get_pid(), {build_merkle_tree, Range}, 20000).

%% @doc update key-value pairs if newer than the ones in the db
-spec(update_if_newer/2 :: (db(), list({any(), any(), integer()})) -> ok).
update_if_newer(ok = _DB, KVs) ->
    gen_server:call(get_pid(), {update_if_newer, KVs}, 20000).
    
%%===============================================================================
%% for testing purpose 
%%===============================================================================
print_locked_items()->
    LI = gen_server:call(get_pid(), {get_locked_items}, 2000),
    LIlength = length(LI),
    if
	LIlength > 0 ->
	    io:format("LockedItems: ~n~p~n", [LI]);
	true ->
	    nothing_locked
    end.

%%===============================================================================
%% gen_server setup
%%===============================================================================

%% @doc Starts the server
start_link(InstanceId) ->
    gen_server:start_link(?MODULE, [InstanceId], []).

%% @doc Starts the server; for use with the test framework
start(InstanceId) ->
    gen_server:start(?MODULE, [InstanceId], []).

%@private
init([InstanceId]) ->
    process_dictionary:register_process(InstanceId, cs_db_otp, self()),
    {ok, db_gb_trees:new(1)}.


%@private
stop() ->
    gen_server:cast(?MODULE, stop).

%%===============================================================================
%% gen_server callbacks
%%===============================================================================

% drop_everything
%@private
handle_call({drop_everything}, _From, _DB) ->
    {reply, ok, db_gb_trees:new(1)};

% set write lock
%@private
handle_call({set_write_lock, Key}, _From, DB) ->
    {NewDB, Res} = db_gb_trees:set_write_lock(DB, Key),
    {reply, Res, NewDB};

% unset write lock
%@private
handle_call({unset_write_lock, Key}, _From, DB) ->
    {NewDB, Res} = db_gb_trees:unset_write_lock(DB, Key),
    {reply, Res, NewDB};

% set read lock
%@private
handle_call({set_read_lock, Key}, _From, DB) ->
    {NewDB, Res} = db_gb_trees:set_read_lock(DB, Key),
    {reply, Res, NewDB};

% unset read lock
%@private
handle_call({unset_read_lock, Key}, _From, DB) ->
    {NewDB, Res} = db_gb_trees:unset_read_lock(DB, Key),
    {reply, Res, NewDB};

% get locks
%@private
handle_call({get_locks, Key}, _From, DB) ->
    {_DropDB, Res} = db_gb_trees:get_locks(DB, Key),
    {reply, {ok, Res}, DB};

% read
%@private
handle_call({read, Key}, _From, DB) ->
    {reply, db_gb_trees:read(DB, Key), DB};

% write
%@private
handle_call({write, Key, Value, Version}, _From, DB) ->
    {reply, ok, db_gb_trees:write(DB, Key, Value, Version)};

% delete
%@private
handle_call({delete, Key}, _From, DB) ->
    {NewDB, Res} = db_gb_trees:delete(DB, Key),
    {reply, {ok, Res}, NewDB};

% get_version
%@private
handle_call({get_version, Key}, _From, DB) ->
    {reply, db_gb_trees:get_version(DB, Key), DB};

% get_load
%@private
handle_call({get_load}, _From, DB) ->
    {reply, db_gb_trees:get_load(DB), DB};

% get_middle_key
%@private
handle_call({get_middle_key}, _From, DB) ->
    {reply, db_gb_trees:get_middle_key(DB), DB};

% split_data
%@private
handle_call({split_data, MyKey, HisKey}, _From, DB) ->
    {MyDB, HisList} = db_gb_trees:split_data(DB, MyKey, HisKey),
    {reply, HisList, MyDB};

% get_data
%@private
handle_call({get_data}, _From, DB) ->
    {reply, db_gb_trees:get_data(DB), DB};

% add_data
%@private
handle_call({add_data, Keys}, _From, DB) ->
    {reply, ok, db_gb_trees:add_data(DB, Keys)};

% get_range
%@private
handle_call({get_range, From, To}, _From, DB) ->
    {reply, db_gb_trees:get_range(DB, From, To), DB};

handle_call({get_range_with_version, Interval}, _From, DB) ->
    {reply, db_gb_trees:get_range_with_version(DB, Interval), DB};

% get_range_with_version
%@private
handle_call({get_range_only_with_version, Interval}, _From, DB) ->
    {reply, db_gb_trees:get_range_only_with_version(DB, Interval), DB};

handle_call({build_merkle_tree, Range}, _From, DB) ->
    {reply, db_gb_trees:build_merkle_tree(DB, Range), DB};

% update only if no locks are taken and version number is higher
handle_call({update_if_newer, KVs}, _From, DB) ->
    {reply, ok, db_gb_trees:update_if_newer(DB, KVs)};

%%===============================================================================
%% for testing purpose 
%%===============================================================================
handle_call({get_locked_items}, _From, DB)->
    Items = lists:filter(fun({_Key, {_Value, WriteLock, ReadLock, _Version}})->
				 if
				     WriteLock == true->
					 true;
				     ReadLock > 0 ->
					 true;
				     true ->
					 false
				 end
			 end,
			 gb_trees:to_list(DB)),
    {reply, Items, DB}.


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%@private
handle_cast(stop, DB) ->
    {stop, normal, DB};
%@private
handle_cast({debug_info, Requestor}, DB) ->
    cs_send:send_local(Requestor , {debug_info_response, [{"db_items", gb_trees:size(DB)}]}),
    {noreply, DB};
%@private
handle_cast(_Msg, DB) ->
    {noreply, DB}.

%@private
handle_info(_Info, DB) ->
    {noreply, DB}.


%@private
code_change(_OldVsn, DB, _Extra) ->
    {ok, DB}.

%@private
terminate(_Reason, _DB) ->
    ok.

