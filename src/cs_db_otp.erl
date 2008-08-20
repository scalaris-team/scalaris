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
-vsn('$Id: cs_db_otp.erl 463 2008-05-05 11:14:22Z schuett $ ').

-behaviour(gen_server).
-behaviour(database).

-export([start_link/1, start/1, init/1, handle_call/3, handle_cast/2, 
	 handle_info/2, code_change/3, terminate/2, stop/0,
	 
	 set_write_lock/1, unset_write_lock/1, set_read_lock/1, 
	 unset_read_lock/1, get_locks/1,

	 read/1, write/3, get_version/1, 

	 get_range/2, get_range_with_version/2,

	 get_load/0, get_middle_key/0, split_data/2, get_data/0, 
	 add_data/1]).

%% for testing purpose
-export([print_locked_items/0]).

%%====================================================================
%% public functions
%%====================================================================

get_pid() ->
    InstanceId = erlang:get(instance_id),
    if
	InstanceId == undefined ->
	    io:format("~p~n", [util:get_stacktrace()]);
	true ->
	    ok
    end,
    process_dictionary:lookup_process(InstanceId, cs_db_otp).

%% @doc sets a write lock on a key.
%%      the write lock is a boolean value per key
%% @spec set_write_lock(string()) -> ok | failed
set_write_lock(Key) ->
    gen_server:call(get_pid(), {set_write_lock, Key}, 20000).

%% @doc unsets the write lock of a key
%%      the write lock is a boolean value per key
%% @spec unset_write_lock(string()) -> ok | failed
unset_write_lock(Key) ->
    gen_server:call(get_pid(), {unset_write_lock, Key}, 20000).

%% @doc sets a read lock on a key
%%      the read lock is an integer value per key
%% @spec set_read_lock(string()) -> ok | failed
set_read_lock(Key) ->
    gen_server:call(get_pid(), {set_read_lock, Key}, 20000).

%% @doc unsets a read lock on a key
%%      the read lock is an integer value per key
%% @spec unset_read_lock(string()) -> ok | failed
unset_read_lock(Key) ->
    gen_server:call(get_pid(), {unset_read_lock, Key}, 20000).

%% @doc get the locks and version of a key
%% @spec get_locks(string()) -> {bool(), int(), int()}| failed
get_locks(Key) ->
    gen_server:call(get_pid(), {get_locks, Key}, 20000).

%% @doc reads the version and value of a key
%% @spec read(string()) -> {ok, string(), integer()} | failed
read(Key) ->
    gen_server:call(get_pid(), {read, Key}, 20000).

%% @doc updates the value of key
%% @spec write(string(), string(), integer()) -> ok
write(Key, Value, Version) ->
    gen_server:call(get_pid(), {write, Key, Value, Version}, 20000).

%% @doc reads the version of a key
%% @spec get_version(string()) -> {ok, integer()} | failed
get_version(Key) ->
    gen_server:call(get_pid(), {get_version, Key}, 20000).

%% @doc returns the number of stored keys
%% @spec get_load() -> integer()
get_load() ->
    gen_server:call(get_pid(), {get_load}, 20000).

%% @doc returns the key, which splits the data into two equally 
%%      sized groups
%% @spec get_middle_key() -> {ok, string()} | failed
get_middle_key() ->
    gen_server:call(get_pid(), {get_middle_key}, 20000).

%% @doc returns all keys (and removes them from the db) which belong 
%%      to a new node with id HisKey
%% @spec split_data(string(), string()) -> [{string(), {string(), bool(), integer(), integer()}}]
split_data(MyKey, HisKey) ->
    gen_server:call(get_pid(), {split_data, MyKey, HisKey}, 20000).

%% @doc returns all keys
%% @spec get_data() -> [{string(), {string(), bool(), integer(), integer()}}]
get_data() ->
    gen_server:call(get_pid(), {get_data}, 20000).
    
%% @doc adds keys
%% @spec add_data([{string(), {string(), bool(), integer(), integer()}}]) -> ok
add_data(Data) ->
    gen_server:call(get_pid(), {add_data, Data}, 20000).
    
%% @doc get keys in a range
%% @spec get_range(string(), string()) -> [{string(), string()}]
get_range(From, To) ->
    gen_server:call(get_pid(), {get_range, From, To}, 20000).

%% @doc get keys and versions in a range
%% @spec get_range_with_version(string(), string()) -> [{string(), string(), integer()}]
get_range_with_version(From, To) ->    
    gen_server:call(get_pid(), {get_range_with_version, From, To}, 20000).
    

%%====================================================================
%% for testing purpose 
%%====================================================================
print_locked_items()->
    LI = gen_server:call(get_pid(), {get_locked_items}, 2000),
    LIlength = length(LI),
    if
	LIlength > 0 ->
	    io:format("LockedItems: ~n~p~n", [LI]);
	true ->
	    nothing_locked
    end.

%%====================================================================
%% gen_server setup
%%====================================================================

%% @doc Starts the server
start_link(InstanceId) ->
    gen_server:start_link(?MODULE, [InstanceId], []).

%% @doc Starts the server; for use with the test framework
start(InstanceId) ->
    gen_server:start(?MODULE, [InstanceId], []).

%@private
init([InstanceId]) ->
    process_dictionary:register_process(InstanceId, cs_db_otp, self()),
    {ok, gb_trees:empty()}.


%@private
stop() ->
    gen_server:cast(?MODULE, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

% set write lock
%@private
handle_call({set_write_lock, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, {Value, false, 0, Version}} ->
	    NewDB = gb_trees:update(Key, 
				    {Value, true, 0, Version}, 
				    DB),
	    {reply, ok, NewDB};
	{value, {_Value, _WriteLock, _ReadLock, _Version}} ->
	    {reply, failed, DB};
	none ->
	    % no value stored yet
	    NewDB = gb_trees:enter(Key, 
				   {empty_val, true, 0, -1},
				   DB),
	    {reply, ok, NewDB}
    end;

% unset write lock
%@private
handle_call({unset_write_lock, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, {Value, true, ReadLock, Version}} ->
	    NewDB = gb_trees:update(Key, 
				    {Value, false, ReadLock, Version}, 
				    DB),
	    {reply, ok, NewDB};
	{value, {_Value, false, _ReadLock, _Version}} ->
	    {reply, failed, DB};
	none ->
	    {reply, failed, DB}
    end;

% set read lock
%@private
handle_call({set_read_lock, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, {Value, false, ReadLock, Version}} ->
	    NewDB = gb_trees:update(Key, 
				    {Value, false, ReadLock + 1, Version}, 
				    DB),
	    {reply, ok, NewDB};
	{value, {_Value, _WriteLock, _ReadLock, _Version}} ->
	    {reply, failed, DB};
	none ->
	    {reply, failed, DB}
    end;

% unset read lock
%@private
handle_call({unset_read_lock, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, {_Value, _WriteLock, 0, _Version}} ->
	    {reply, failed, DB};
	{value, {Value, WriteLock, ReadLock, Version}} ->
	    NewDB = gb_trees:update(Key, 
				    {Value, WriteLock, ReadLock - 1, Version}, 
				    DB),
	    {reply, ok, NewDB};
	none ->
	    {reply, failed, DB}
    end;

% get locks
%@private
handle_call({get_locks, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, {_Value, WriteLock, ReadLock, Version}} ->
	    {reply, {WriteLock, ReadLock, Version}, DB};
	none ->
	    {reply, failed, DB}
    end;

% read
%@private
handle_call({read, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, {Value, _WriteLock, _ReadLock, Version}} ->
	    {reply, {ok, Value, Version}, DB};
	none ->
	    {reply, failed, DB}
    end;

% write
%@private
handle_call({write, Key, Value, Version}, _From, DB) ->
    NewDB = case gb_trees:lookup(Key, DB) of
		{value, {_Value, WriteLock, ReadLock, _Version}} ->
		    gb_trees:enter(Key, 
				   {Value, WriteLock, ReadLock, Version}, 
				   DB);
		none ->
		    gb_trees:enter(Key, 
				   {Value, false, 0, Version}, 
				   DB)
	    end,
    {reply, ok, NewDB};

% get_version
%@private
handle_call({get_version, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, {_Value, _WriteLock, _ReadLock, Version}} ->
	    {reply, {ok, Version}, DB};
	none ->
	    {reply, failed, DB}
    end;

% get_load
%@private
handle_call({get_load}, _From, DB) ->
    {reply, gb_trees:size(DB), DB};

% get_middle_key
%@private
handle_call({get_middle_key}, _From, DB) ->
    Size = gb_trees:size(DB),
    if
	Size < 3 ->
	    {reply, failed, DB};
	true ->
	    Keys = gb_trees:keys(DB),
	    Middle = util:lengthX(Keys) div 2 + 1,
	    MiddleKey = lists:nth(Middle, Keys),
	    {reply, MiddleKey, DB}
    end;

% split_data
%@private
handle_call({split_data, MyKey, HisKey}, _From, DB) ->
    DataList = gb_trees:to_list(DB),
    {MyList, HisList} = lists:partition(fun ({Key, _}) -> util:is_between(HisKey, Key, MyKey) end, DataList),
    {reply, HisList, gb_trees:from_orddict(MyList)};

% get_data
%@private
handle_call({get_data}, _From, DB) ->
    {reply, gb_trees:to_list(DB), DB};

% add_data
%@private
handle_call({add_data, Keys}, _From, DB) ->
    NewDB = lists:foldl(fun ({Key, Value}, Tree) -> gb_trees:enter(Key, Value, Tree) end, DB, Keys),
    {reply, ok, NewDB};

% get_range
%@private
handle_call({get_range, From, To}, _From, DB) ->
    Items = lists:foldl(fun ({Key, {Value, _WriteLock, _ReadLock, _Version}}, List) -> 
				case util:is_between(From, Key, To) of
				    true ->
					[{Key, Value} | List];
				    false ->
					List
				end
			end, 
		[], gb_trees:to_list(DB)),
    {reply, Items, DB};

% get_range_with_version
%@private
handle_call({get_range_with_version, From, To}, _From, DB) ->
    Items = lists:foldl(fun ({Key, {Value, _WriteLock, _ReadLock, Version}}, List) -> 
				case util:is_between(From, Key, To) of
				    true ->
					[{Key, Value, Version} | List];
				    false ->
					List
				end
			end, 
		[], gb_trees:to_list(DB)),
    {reply, Items, DB};


%%====================================================================
%% for testing purpose 
%%====================================================================
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
    Requestor ! {debug_info_response, [{"db_items", gb_trees:size(DB)}]},
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

