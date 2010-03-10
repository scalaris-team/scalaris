%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$

%@doc This module provides a mechanism to implement process
%     groups. Within a process group, the names of processes have to
%     be unique, but the same name can be used in different
%     groups. The motivation for this module was to run several scalaris
%     nodes in one erlang vm. But for the processes forming a scalaris
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

-behaviour(gen_component).

%% API
-export([start_link/0, start/0]).

%% gen_server callbacks
-export([init/1,on/2,
         register_process/3,
         lookup_process/2,
         lookup_process/1,
         find_cs_node/0,
         find_all_cs_nodes/0,
         find_all_processes/1,
         find_group/1,
         find_all_groups/1,

         get_groups/0,
         get_processes_in_group/1,
		 get_group_member/1,
         %get_info/2,

         %for fprof
         get_all_pids/0]).

%%====================================================================
%% public functions
%%====================================================================

%% @doc register a process with InstanceId and Name
-spec(register_process/3 :: (term(), term(), pid()) -> ok).
register_process(InstanceId, Name, Pid) ->
    erlang:put(instance_id, InstanceId),
    erlang:put(instance_name, Name),
    cs_send:send_local(get_pid() , {register_process, InstanceId, Name, Pid}),
    gen_component:wait_for_ok().

%% @doc looks up a process with InstanceId and Name in the dictionary
-spec(lookup_process/2 :: (term(), term()) -> pid() | failed).
lookup_process(InstanceId, Name) ->
    case ets:lookup(?MODULE, {InstanceId, Name}) of
        [{{InstanceId, Name}, Value}] ->
            Value;
        [] ->
            log:log(error, "[ PD ] lookup_process failed: InstanceID:  ~p  For: ~p",[InstanceId, Name]),
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
-spec(find_cs_node/0 :: () -> {ok, pid()} | failed).
find_cs_node() ->
    find_process(cs_node).

%% @doc tries to find all cs_node processes
-spec(find_all_cs_nodes/0 :: () -> list()).
find_all_cs_nodes() ->
    find_all_processes(cs_node).

-spec(find_all_processes/1 :: (any()) -> list()).
find_all_processes(Name) ->
    %ct:pal("ets:info: ~p~n",[ets:info(?MODULE)]),
    Result = ets:match(?MODULE, {{'_', Name}, '$1'}),
    lists:flatten(Result).

%% @doc tries to find a process group with a specific process inside
%% @spec find_group(term()) -> term()
find_group(ProcessName) ->
    case ets:match(?MODULE, {{'$1', ProcessName}, '_'}) of
        [[Value] | _] ->
            Value;
        [] ->
            failed
    end.

find_all_groups(ProcessName) ->
    case ets:match(?MODULE, {{'$1', ProcessName}, '_'}) of
        [] ->
            failed;
        List ->
            lists:append(List)
    end.

%% @doc find groups for web interface
%% @spec get_groups() -> term()
get_groups() ->
    AllGroups = find_all_groups(ets:tab2list(?MODULE), gb_sets:new()),
    GroupsAsJson = {array, lists:foldl(fun(El, Rest) -> [{struct, [{id, El}, {text, El}, {leaf, false}]} | Rest] end, [], gb_sets:to_list(AllGroups))},
    GroupsAsJson.

%% @doc find processes in a group (for web interface)
%% @spec get_processes_in_group(term()) -> term()
get_processes_in_group(Group) ->
    AllProcesses = find_processes_in_group(ets:tab2list(?MODULE), gb_sets:new(), Group),
    ProcessesAsJson = {array, lists:foldl(fun(El, Rest) -> [{struct, [{id, toString(El)}, {text, toString(El)}, {leaf, true}]} | Rest] end, [], gb_sets:to_list(AllProcesses))},
    ProcessesAsJson.

%% @doc Gets the Pid of the current process' group member with the given name.
-spec(get_group_member/1 :: (atom()) -> pid() | failed).
get_group_member(Name) ->
    InstanceId = erlang:get(instance_id),
    if
        InstanceId =:= undefined ->
            log:log(error,"[ Node | ~w ] instance ID undefined: ~p", [self(),util:get_stacktrace()]);
        true ->
            ok
    end,
    Pid = process_dictionary:lookup_process(InstanceId, Name),
    case Pid of
        failed ->
            log:log(error,"[ Node | ~w ] process ~w not found: ~p", [self(),Name,util:get_stacktrace()]),
            failed;
        _ -> Pid
    end.

get_all_pids() ->
    [X || [X]<- ets:match(?MODULE, {'_','$1'})].

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
%@doc Starts the server
start_link() ->
    gen_component:start_link(?MODULE, [], [{register_native, process_dictionary}]).

%%--------------------------------------------------------------------
%% Function: start() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server; for use with the test framework
%%--------------------------------------------------------------------
%@doc Starts the server; for use with the test framework
start() ->
    gen_component:start_link(?MODULE, [], [{register_native, process_dictionary}]).



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
init(_Args) ->
    ets:new(?MODULE, [set, protected, named_table]),
    %process_flag(trap_exit, true),
    State = null,
    State.

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

-spec(on/2 :: (any(), null) -> null).
on({register_process, InstanceId, Name, Pid}, State) ->
    case ets:insert_new(?MODULE, {{InstanceId, Name}, Pid}) of
        true ->
            link(Pid);
        false ->
            OldPid = ets:lookup_element(?MODULE, {InstanceId, Name}, 2),
            unlink(OldPid),
            link(Pid),
            ets:insert_new(?MODULE, {{InstanceId, Name}, Pid})
    end,
    cs_send:send_local(Pid , {ok}),
    State;
on({drop_state}, State) ->
    % only for unit tests
    Links = ets:match(?MODULE, {'_', '$1'}),
    [unlink(Pid) || [Pid] <- Links],
    ets:delete_all_objects(?MODULE),
    State;
on({'EXIT', FromPid, _Reason}, State) ->
    Processes = ets:match(?MODULE, {'$1', FromPid}),
    [ets:delete(?MODULE, {InstanceId, Name}) || [{InstanceId, Name}] <- Processes],
    State;
on(_, _State) ->
    unknown_event.

%% lookup_process2(InstanceId, Name) ->
%%     Result = case ets:lookup(?MODULE, {InstanceId, Name}) of
%%         [{{InstanceId, Name}, Value}] ->
%%             {ok, Value};
%%         [] ->
%%             failed
%%     end,
%%     Result.

find_process(Name) ->
    case ets:match(?MODULE, {{'_', Name}, '$1'}) of
         [[Value] | _] ->
             {ok, Value};
         [] ->
             failed
         end.

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
%%% Internal functions
%%--------------------------------------------------------------------

get_pid() ->
    case whereis(process_dictionary) of
        undefined ->
            log:log(error, "[ PD ] call of get_pid undefined");
        PID ->
            %log:log(info, "[ PD ] find right pid"),
            PID
    end.
