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
%%% File    : process_dictionary.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : process dictionary
%%%
%%% Created : 17 Aug 2007 by Thorsten Schuett <schuett@csr-pc11.zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$

%@doc This module provides a mechanism to implement process
%     groups. Within a process group, the names of processes have to
%     be unique, but the same name can be used in different
%     groups. The motivation for this module was to run several Chord#
%     nodes in one erlang vm. But for the processes forming a Chord#
%     node being able to talk to each other, they have to now their
%     names (cs_node, config, etc.). This module allows the processes
%     to keep their names. 
%
%     When a new process group is created, a unique "instance_id" is
%     created, which has to be shared by all nodes in this
%     group. 
%     
%     {@link register_process/3} registers the name of a process in
%     his group and stores the instance_id in the calling processes'
%     environment using {@link erlang:put/2}.
%
%     {@link lookup_process/2} will lookup in the process group for a
%     process with the given name.

-module(process_dictionary).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_server).

%% API
-export([start_link/0, start/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3,

	 register_process/3, 
	 lookup_process/2,
	 lookup_process/1,
	 find_cs_node/0, 
	 find_all_cs_nodes/0, 
	 find_all_processes/1, 
	 find_group/1, 

	 get_groups/0,
	 get_processes_in_group/1, 
	 get_info/2,

	 %for fprof
	 get_all_pids/0]).

%% for unit testing
-export([start_link_for_unittest/0]).

%%====================================================================
%% public functions
%%====================================================================

%% @doc register a process with InstanceId and Name
%% @spec register_process(term(), term(), pid()) -> ok
register_process(InstanceId, Name, Pid) ->
    erlang:put(instance_id, InstanceId),
    erlang:put(instance_name, Name),
    gen_server:call(?MODULE, {register_process, InstanceId, Name, Pid}, 20000).

%% @doc looks up a process with InstanceId and Name in the dictionary
%% @spec lookup_process(term(), term()) -> term()
lookup_process(InstanceId, Name) ->
    case ets:lookup(?MODULE, {InstanceId, Name}) of
        [{{InstanceId, Name}, Value}] ->
            Value;
        [] ->
            failed
    end.
    %gen_server:call(?MODULE, {lookup_process, InstanceId, Name}, 20000).

%% @doc find the process group and name of a process by pid
-spec(lookup_process/1 :: (pid()) -> {any(), any()} | failed).
lookup_process(Pid) ->
    case ets:match(?MODULE, {'$1',Pid}) of
	[[{Group, Name}]] ->
	    {Group, Name};
	[] ->
	    failed
    end.

%% @doc tries to find a cs_node process
%% @spec find_cs_node() -> pid()
find_cs_node() ->
    gen_server:call(?MODULE, {find_process, cs_node}, 20000).

%% @doc tries to find all cs_node processes
-spec(find_all_cs_nodes/0 :: () -> list()).
find_all_cs_nodes() ->
    find_all_processes(cs_node).

-spec(find_all_processes/1 :: (any()) -> list()).
find_all_processes(Name) ->
    gen_server:call(?MODULE, {find_all_processes, Name}, 20000).

%% @doc tries to find a process group with a specific process inside
%% @spec find_group(term()) -> term()
find_group(Process) ->
    gen_server:call(?MODULE, {find_group, Process}, 20000).

%% @doc find groups for web interface
%% @spec get_groups() -> term()
get_groups() ->
    gen_server:call(?MODULE, {get_groups}, 20000).
 
%% @doc find processes in a group (for web interface)
%% @spec get_processes_in_group(term()) -> term()
get_processes_in_group(InstanceId) ->   
    gen_server:call(?MODULE, {get_processes_in_group, InstanceId}, 20000).
    
%% @doc get info about process (for web interface)
%% @spec get_info(term(), term()) -> term()
get_info(InstanceId, Name) ->   
    KVs = case gen_server:call(?MODULE, {lookup_process2, InstanceId, list_to_atom(Name)}, 20000) of
	      failed ->
		  [{"process", "unknown"}];
	      {ok, Pid} ->
		  Pid ! {'$gen_cast', {debug_info, self()}},
		  {memory, Memory} = process_info(Pid, memory),
		  {reductions, Reductions} = process_info(Pid, reductions),
		  {message_queue_len, QueueLen} = process_info(Pid, message_queue_len),
		  AddInfo = receive
				{debug_info_response, LocalKVs} ->
				    LocalKVs
			    after 1000 ->
				    []
			    end,
		  [{"memory", Memory}, {"reductions", Reductions}, {"message_queue_len", QueueLen} | AddInfo]
	  end,
    JsonKVs = lists:map(fun({K, V}) -> {struct, [{key, K}, {value, toString(V)}]} end, KVs),
    {struct, [{pairs, {array, JsonKVs}}]}.

%% @doc get all pids (for fprof)
%% @spec get_all_pids() -> [pid()]
get_all_pids() ->   
    gen_server:call(?MODULE, {get_all_pids}, 20000).
    
%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
%@doc Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%@doc Starts the server for unit testing
start_link_for_unittest() ->
    case whereis(process_dictionary) of
	undefined ->
	    gen_server:start({local, ?MODULE}, ?MODULE, [], []);
	_ ->
	    gen_server:call(?MODULE, {drop_state}, 20000),
	    already_running
    end.
    
%%--------------------------------------------------------------------
%% Function: start() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server; for use with the test framework
%%--------------------------------------------------------------------
%@doc Starts the server; for use with the test framework
start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: stop() -> ok
%% Description: Stops the server
%%--------------------------------------------------------------------
%@doc Stops the server
stop() ->
    gen_server:cast(?MODULE, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
%@private
init([]) ->
    ets:new(?MODULE, [set, protected, named_table]),
    {ok, ok}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------

%@private
handle_call({get_all_pids}, _From, State) ->
    {reply, [X || [X]<- ets:match(?MODULE, {'_','$1'})], State};
handle_call({register_process, InstanceId, Name, Pid}, _From, State) ->
    ets:insert(?MODULE, {{InstanceId, Name}, Pid}),
    {reply, ok, State};

handle_call({lookup_process2, InstanceId, Name}, _From, State) ->
    Result = case ets:lookup(?MODULE, {InstanceId, Name}) of
        [{{InstanceId, Name}, Value}] ->
            {ok, Value};
        [] ->
            failed
    end,
    {reply, Result, State};

handle_call({find_process, Name}, _From, State) ->
    Result = case ets:match(?MODULE, {{'_', Name}, '$1'}) of
		 [[Value] | _] ->
		     {ok, Value};
		 [] ->
		     failed
	     end,
    {reply, Result, State};

handle_call({find_all_processes, Name}, _From, State) ->
    Result = ets:match(?MODULE, {{'_', Name}, '$1'}),
    {reply, lists:flatten(Result), State};

handle_call({find_group, Name}, _From, State) ->
    Result = case ets:match(?MODULE, {{'$1', Name}, '_'}) of
	[[Value] | _] ->
	    Value;
	[] ->
	    failed
    end,
    {reply, Result, State};

handle_call({get_groups}, _From, State) ->
    AllGroups = find_all_groups(ets:tab2list(?MODULE), gb_sets:new()),
    GroupsAsJson = {array, lists:foldl(fun(El, Rest) -> [{struct, [{id, El}, {text, El}, {leaf, false}]} | Rest] end, [], gb_sets:to_list(AllGroups))},
    {reply, GroupsAsJson, State};

handle_call({get_processes_in_group, Group}, _From, State) ->
    AllProcesses = find_processes_in_group(ets:tab2list(?MODULE), gb_sets:new(), Group),
    ProcessesAsJson = {array, lists:foldl(fun(El, Rest) -> [{struct, [{id, toString(El)}, {text, toString(El)}, {leaf, true}]} | Rest] end, [], gb_sets:to_list(AllProcesses))},
    {reply, ProcessesAsJson, State};

handle_call({drop_state}, _From, State) ->
    ets:delete_all_objects(?MODULE),
    {reply, ok, State}.

find_all_groups([], Set) ->
    Set;
find_all_groups([{{InstanceId, _}, _} | Rest], Set) ->
    find_all_groups(Rest, gb_sets:add_element(InstanceId, Set)).

find_processes_in_group([], Set, _Group) ->
    Set;
find_processes_in_group([{{InstanceId, TheName}, _} | Rest], Set, Group) ->
    if
	InstanceId == Group ->
	    find_processes_in_group(Rest, gb_sets:add_element(TheName, Set), Group);
	true ->
	    find_processes_in_group(Rest, Set, Group)
    end.

toString(X) when is_atom(X) ->
    atom_to_list(X);
toString(X) ->
    X.
    

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
%@private
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
%@private
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
%@private
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
%@private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
