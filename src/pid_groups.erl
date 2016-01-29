% @copyright 2007-2016 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @author Florian Schintke <schintke@zib.de>
%% @doc Process groups.
%%
%%      This module provides a mechanism to implement process
%%      groups. Within a process group, the names of processes have to
%%      be unique, but the same name can be used in different
%%      groups. The motivation for this module was to run several
%%      scalaris nodes in one erlang vm for debugging. But for the
%%      processes forming a scalaris node being able to talk to each
%%      other, they have to know their names (dht_node, config,
%%      etc.). This module allows the processes to keep their names.
%%
%%      When a new process group is created, a unique "groupname" is
%%      created, which has to be shared by all nodes in this group.
%% @end
%% @version $Id$
-module(pid_groups).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,on/2]).

%% group creation
-export([new/0,     %% () -> GrpName
         new/1]).   %% (PrefixAtom) -> GrpName

%% group membership management
-export([join/1,          %% (GrpName) -> ok               deprecated
         join_as/2,       %% (GrpName, PidName) -> ok
         add/3]).         %% (GrpName, PidName, Pid) -> ok

%% operations inside a group (PRE: caller is group member):
-export([my_groupname/0,  %% () -> GrpName
         my_pidname/0,    %% () -> PidName
         get_my/1,        %% (PidName) -> Pid
         my_members/0,    %% () -> [Pids]
         my_tab2list/0]). %% () -> [{GrpName, PidName, Pid}]

%% get information on groups for non-members
-export([pid_of/2,            %% (GrpName, PidName) -> Pid
         group_and_name_of/1, %% (Pid) -> {GrpName, PidName}
         group_of/1,          %% (Pid) -> GrpName
         group_with/1,        %% (PidName) -> GrpName
         groups_with/1,       %% (PidName) -> [GrpName]

         find_a/1,            %% (PidName) -> Pid
         find_all/1,          %% (PidName) -> [Pid]

         members/1,           %% (GrpName) -> [Pids]
         members_by_name/1,   %% (GrpName) -> [pidname()]

         pid_to_name/1,       %% translation of pids to strings for debugging
         pids_to_names/2]).

%% global information
-export([processes/0,     %% () -> [Pid]    %% for fprof
         groups/0,        %% () -> [GrpName]
         tab2list/0]).    %% () -> [{GrpName, PidName, Pid}]

%% for persistency
-export([group_to_filename/1,   %% (GrpName) -> string()
         filename_to_group/1]). %% (string()) -> GrpName

%% for the monitoring in the web interface
-export([get_web_debug_info/2,    %% (Group, PidName) -> any()
         groups_as_json/0,
         members_by_name_as_json/1,
         group_to_string/1,
         string_to_group/1]).

%% for unittests when group is in breakpoint
-export([hide/1, unhide/1]).

-export_type([pidname/0, groupname/0]).

-type(pidname() :: nonempty_string()                       % vivaldi and fd
                 | atom()
                 | {atom(), atom()}
                 | {atom(), pos_integer()}                 % the nth lease_db
                 | {{atom(), pos_integer()}, atom()}).     % the acceptor of the nths tm
-type(groupname() :: atom()
                   | pos_integer()
                   | {atom(), pos_integer() | atom()}).

-type(message() ::
    {pid_groups_add, groupname(), pidname(), pid()} |
    {drop_state} |
    {group_and_name_of, Pid::comm:mypid(), Source::comm:mypid()} |
    {'EXIT', pid(), Reason::any()}).

-include("gen_component.hrl").

%%% group creation
%% @doc create a new group with a random name.
-spec new() -> groupname().
new() ->
    randoms:getRandomInt().

%% @doc create a new group with a given prefix.
-spec new(atom()) -> groupname().
new(Prefix) ->
    {Prefix, new()}.

%%% group membership management
%% @doc Current process joins the group GrpName, but has no process name.
%%
%% DEPRECATED! client processes should not join groups.  Other
%% processes should have a unique role name and join via join_as or
%% add.
-spec join(groupname()) -> ok.
join(GrpName) ->
    %% @todo eliminate this function
    erlang:put('$@groupname', GrpName), ok.

%% @doc Current process joins the group GrpName with process name Pidname.
-spec join_as(groupname(), pidname()) -> ok.
join_as(GrpName, PidName) ->
    erlang:put('$@groupname', GrpName), %% for my_groupname()
    erlang:put('$@pidname', PidName),   %% for my_pidname()
    add(GrpName, PidName, self()).

%% @doc Third party register: add a pid with a given pidname (role) to a group.
-spec add(groupname(), pidname(), pid()) -> ok.
add(GrpName, PidName, Pid) ->
    comm:send_local(pid_groups_manager(),
                    {pid_groups_add, GrpName, PidName, Pid, self()}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({pid_groups_add_done}, ok);
        ?SCALARIS_RECV(
            {pid_groups_add_done, OldGroup, OldName}, %% ->
            throw({Pid, already_registered_as, OldGroup, OldName, was_trying, GrpName, PidName})
          )
    end.


%%% operations inside a group (PRE_COND: caller is group member):
-spec my_groupname() -> groupname() | undefined.
my_groupname() -> erlang:get('$@groupname').

-spec my_pidname() -> pidname() | undefined.
my_pidname() -> erlang:get('$@pidname').

%% @doc Gets the Pid of the current process' group member with the given name.
-spec get_my(pidname()) -> pid() | failed.
get_my(PidName) ->
    case my_groupname() of
        undefined -> failed;
        GrpName   -> cached_lookup(GrpName, PidName)
    end.

-spec my_members() -> [pid()].
my_members() -> members(my_groupname()).

-spec my_tab2list() -> [{{groupname(), pidname()}, pid()}].
my_tab2list() ->
    GrpName = my_groupname(),
    [ X || X <- tab2list(), GrpName =:= element(1, element(1, X)) ].

%% get information on groups for non-members

%% @doc lookup a pid via its group name and pid name.
-spec pid_of(groupname(), pidname()) -> pid() | failed.
pid_of(GrpName, PidName) ->
    cached_lookup(GrpName, PidName).

%% @doc lookup group and pid name of a process via its pid.
-spec group_and_name_of(pid()) -> {groupname(), pidname()} | failed.
%% maybe used with reg_name() but '_' would violates result type.
group_and_name_of('_') -> failed;
group_and_name_of(Pid) ->
    case ets:match(?MODULE, {'$1',Pid}) of
        [[{GrpName, PidName}]] -> {GrpName, PidName};
        []                -> failed
    end.

-spec group_of(pid()) -> groupname() | failed.
group_of(Pid) ->
    case group_and_name_of(Pid) of
        failed -> failed;
        {GrpName, _PidName} -> GrpName
    end.

%% @doc find a process group with a given process name inside
-spec group_with(pidname()) -> groupname() | failed.
group_with(PidName) ->
    case ets:match(?MODULE, {{'$1', PidName}, '_'}) of
        [[GrpName] | _] -> GrpName;
        []            -> failed
    end.

%% @doc find all process groups with a given process name inside
-spec groups_with(pidname()) -> [groupname()] | failed.
groups_with(PidName) ->
    case ets:match(?MODULE, {{'$1', PidName}, '_'}) of
        [_|_] = Groups -> lists:append(Groups);
        []   -> failed
    end.

%% @doc find a pid with the given name (search in own group first), otherwise
%% allow round-robin returns of all the pids with this name in any group
-spec find_a(pidname()) -> pid() | failed.
find_a(PidName) ->
    cached_lookup('_', PidName).

%% @doc perform a lookup with caching and cheking whether the cache is up to date.
%% Commonly used by find_a/1 and get_my/1. get_my should only search in the own
%% group, while find_a/1 first searches the own group and then searches in other
%% groups (parameter SearchGrp =:= '_' instead of a groupname()). For find_a/1,
%% if several matching pidnames are found, we use a round-robin cache per client
%% process to spread the load.
%% @private
-spec cached_lookup('_' | groupname(), pidname()) -> pid() | failed.
cached_lookup(SearchGrp, PidName) ->
    CachedName = {'$?scalaris_pid_groups_cache', SearchGrp, PidName},
    case erlang:get(CachedName) of
        undefined ->
            %% try in my own group first
            X = case my_groupname() of
                    undefined -> failed;
                    SearchGrp ->
                        %% lookup in own group
                        case ets:lookup(?MODULE, {SearchGrp, PidName}) of
                            [{{SearchGrp, PidName}, FPid}] -> FPid;
                            [] -> failed
                        end;
                    GrpName ->
                        %% generic search, but lookup in own group first.
                        %% recursive call to ask cache of own group before doing a lookup
                        cached_lookup(GrpName, PidName)
                end,
            %% when (failed =:= X) -> search in own group did not help
            case X of
                failed ->
                    %% search others (when SearchGrp =:= '_' as in find_a/1)
                    case ets:match(?MODULE, {{SearchGrp, PidName}, '$1'}) of
                        [[FoundPid]] ->
                            erlang:put(CachedName, FoundPid),
                            FoundPid;
                        [[_TPid] | _] = Pids ->
                            %% fill process local cache
                            ?DBG_ASSERT(SearchGrp =:= '_'),
                            %% This should only happen for client
                            %% processes not registered in a pid_group (failed)
                            %% io:format("Ring is created to find a ~p in ~p ~p~n",
                            %% [PidName, self(), pid_groups:group_and_name_of(self())]),
                            Ring = ring_new([PidX || [PidX] <- util:shuffle(Pids),
                                                     erlang:is_process_alive(PidX),
                                                     erlang:process_info(PidX, priority) =/= {priority, low}]),
                            {Pid, NewPids} = ring_get(Ring),
                            erlang:put(CachedName, NewPids),
                            Pid;
                        [] ->
                            log:log(info, "***No pid registered for ~p ~p~n",
                                    [PidName, util:get_stacktrace()]),
                            failed
                    end;
                Pid ->
                    erlang:put(CachedName, Pid),
                    Pid
            end;
        Pid when is_pid(Pid) ->
            %% clean process local cache if entry is outdated
            case erlang:is_process_alive(Pid) andalso
                     erlang:process_info(Pid, priority) =/= {priority, low} of
                true ->
                    Pid;
                false -> erlang:erase(CachedName),
                         cached_lookup(SearchGrp, PidName)
            end;
        Pids ->
            %% clean process local cache if entry is outdated
            {Pid, NewPids} = ring_get(Pids),
            case erlang:is_pid(Pid)
                andalso erlang:is_process_alive(Pid)
                andalso
                erlang:process_info(Pid, priority) =/= {priority, low} of
                true ->
                    erlang:put(CachedName, NewPids),
                    Pid;
                false -> erlang:erase(CachedName),
                         cached_lookup(SearchGrp, PidName)
            end
    end.

-spec ring_get({list(), list()}) -> {any(), {list(), list()}}.
ring_get({[], []})        -> {'$dead_code', {[], []}};
ring_get({[E], []} = Q)   -> {E, Q};
ring_get({[], L}) -> ring_get({lists:reverse(L), []});
ring_get({[ E | R ], L})  -> {E, {R, [E|L]}}.

-spec ring_new(list()) -> {list(), list()}.
ring_new(L) -> {L, []}.

-spec find_all(pidname()) -> [pid()].
find_all(PidName) ->
    PidList = ets:match(?MODULE, {{'_', PidName}, '$1'}),
    lists:append(PidList).

-spec members(groupname()) -> [pid()].
members(GrpName) ->
    PidList = ets:match(?MODULE, {{GrpName, '_'}, '$1'}),
    lists:append(PidList).

-spec members_by_name(groupname()) -> [pidname()].
members_by_name(GrpName) ->
    MatchList = ets:match(?MODULE, {{GrpName, '$1'}, '_'}),
    PidNameList = [ X || [X] <- MatchList ],
    lists:sort(PidNameList).

%% global information
-spec processes() -> [pid()]. %% for fprof for example
processes() ->
    %% alternative implementation, when each pid is only in one group:
    %% [X || [X]<- ets:match(?MODULE, {'_','$1'})].
    lists:usort([element(2, X) || X <- tab2list()]).

-spec groups() -> [groupname()].
groups() ->
    lists:usort([element(1, element(1,X)) || X <- tab2list()]).

-spec tab2list() -> [{{groupname(), pidname()}, pid()}].
tab2list() -> ets:tab2list(?MODULE).

%% @doc only supports the form {atom(), pos_integer()} | atom(). Not
%%      deeper nested structures.
-spec group_to_filename(groupname()) -> nonempty_string().
group_to_filename('') ->
    ?ASSERT(util:is_unittest()),
    "''"; %% make tester happy by not delivering an empty string
group_to_filename(GrpName) when is_atom(GrpName) ->
    atom_to_list(GrpName);
group_to_filename(GrpName) when is_integer(GrpName) ->
    integer_to_list(GrpName);
group_to_filename({Prefix, Postfix}) ->
    group_to_filename(Prefix) ++ "@" ++ group_to_filename(Postfix).

%% @doc only supports the form {atom(), pos_integer()} | atom(). Not
%%      deeper nested structures.
-spec filename_to_group(nonempty_string()) -> groupname().
%% filename_to_group([]) -> {'',''};
filename_to_group(GrpName) ->
    case string:sub_word(GrpName, 1, $@) of
        GrpName ->
            list_to_atom(GrpName);
        Part1 ->
            {list_to_atom(Part1), list_to_integer(string:sub_word(GrpName, 2, $@))}
    end.

%% @doc Resolve a local pid to its name.
-spec pid_to_name(pid() | {groupname(), pidname()}) -> string().
pid_to_name(Pid) when is_pid(Pid) ->
    case pid_groups:group_and_name_of(Pid) of
        failed -> erlang:pid_to_list(Pid);
        X      -> pid_to_name(X)
    end;
pid_to_name({GrpName, PidName}) ->
    lists:flatten(io_lib:format("~.0p:~.0p", [GrpName, PidName])).

%% @doc Resolve (local and remote) pids to names.
-spec pids_to_names(Pids::[comm:mypid()], Timeout::pos_integer()) -> [string()].
pids_to_names(Pids, Timeout) ->
    Refs = [begin
                case comm:is_local(Pid) of
                    true -> comm:make_local(Pid);
                    _    -> comm:send(comm:get(pid_groups, Pid),
                                      {group_and_name_of, Pid,
                                       comm:reply_as(comm:this(), 2, {ok, '_', Pid})}),
                            comm:send_local_after(Timeout, self(), {timeout, Pid})
                end
            end || Pid <- Pids],
    [begin
         case erlang:is_reference(Ref) of
             false -> pid_to_name(Ref);
             _     -> trace_mpath:thread_yield(),
                      receive
                          ?SCALARIS_RECV(
                              {ok, {group_and_name_of_response, Name}, Pid}, %% ->
                              begin
                                  _ = erlang:cancel_timer(Ref),
                                  receive {timeout, Pid} -> ok
                                      after 0 -> ok
                                  end,
                                  pid_to_name(Name)
                              end
                            );
                          {timeout, Pid} ->
                              lists:flatten(
                                io_lib:format("~.0p (timeout)", [Pid]))
                      end
         end
     end || Ref <- Refs].

%% for the monitoring in the web interface
%% @doc get info about a process
-spec get_web_debug_info(string(), string()) ->
   {struct, [{pairs, {array, [{struct, [{key | value, nonempty_string()}]}]}}]}.
get_web_debug_info([], []) ->
    {struct, [{pairs, {array, [ {struct, [{key, "process"}, {value, "unknown"}]} ]}}]};
get_web_debug_info(GrpStr, PidNameString) ->
    GrpName = string_to_group(GrpStr),
    KVs = case pid_of_string(GrpName, PidNameString) of
              failed -> [{"process", "unknown"}];
              Pid    -> util:debug_info(Pid)
          end,
    JsonKVs = [ {struct, [{key, K}, {value, V}]} || {K, V} <- KVs],
    {struct, [{pairs, {array, JsonKVs}}]}.

%% for web interface
-spec groups_as_json() ->
   {array, [{struct, [{id | text, nonempty_string()} | {leaf, false}]}]}.
groups_as_json() ->
    GroupList = groups(),
    GroupListAsStructs =
        [ {struct, [{id, group_to_string(El)}, {text, group_to_string(El)}, {leaf, false}]} || El <- GroupList ],
    {array, GroupListAsStructs}.

%% @doc find processes in a group (for web interface)
-spec members_by_name_as_json(nonempty_string()) ->
   {array, [{struct, [{id | text, nonempty_string()} | {leaf, true}]}]}.
members_by_name_as_json(GrpString) ->
    GrpName = string_to_group(GrpString),
    PidList = members_by_name(GrpName),
    PidListAsJson =
        [ begin
              ElStr = pidname_to_string(El),
              {struct, [{id, GrpString ++ "." ++ ElStr},
                        {text, ElStr}, {leaf, true}]}
          end || El <- PidList ],
    {array, PidListAsJson}.

-spec group_to_string(groupname()) -> string().
group_to_string(Group) when is_atom(Group) ->
    atom_to_list(Group);
group_to_string(Group) when is_integer(Group) ->
    integer_to_list(Group);
group_to_string(Group) when is_tuple(Group) ->
    lists:flatten(io_lib:format("~p", [Group])).

-spec string_to_group(nonempty_string()) -> groupname() | failed.
string_to_group(GrpStr) ->
    case [Name || Name <- groups(),
                  group_to_string(Name) =:= GrpStr] of
        [GrpName] -> GrpName;
        [] -> failed
    end.

%% @doc hide a group of processes temporarily (for paused groups in
%% unit tests)
-spec hide(groupname()) -> ok.
hide(GrpName) ->
    comm:send_local(pid_groups_manager(),
                    {pid_groups_hide, GrpName, self()}),
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({pid_groups_hide_done, GrpName}, ok) end.

-spec unhide(groupname()) -> ok.
unhide(GrpName) ->
    comm:send_local(pid_groups_manager(),
                    {pid_groups_unhide, GrpName, self()}),
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({pid_groups_unhide_done, GrpName}, ok) end.

%%
%% pid_groups manager process (gen_component)
%%

%% @doc Starts the server
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{erlang_register, ?MODULE},
                              %% we provide a protected ets table,
                              %% that should be available when we
                              %% started..., so we better wait for its
                              %% creation.
                              {wait_for_init}]).

%% @doc Initiates the server
%% @private
-spec init([]) -> null.
init(_Args) ->
%%  ets:new(call_counter, [set, public, named_table]),
%%  ets:insert(call_counter, {lookup_pointer, 0}),
    _ = ets:new(?MODULE, [set, protected, named_table]),
    _ = ets:new(pid_groups_hidden, [set, protected, named_table]),
    %% required to gracefully eliminate dead, but registered processes
    %% from the ets-table
    process_flag(trap_exit, true),
    _State = null.

%% @doc Handling call messages
%% @private
-spec on(message(), null) -> null.
on({pid_groups_add, GrpName, PidName, Pid, ReplyTo}, State) ->
    case group_and_name_of(Pid) of
        failed ->
            case ets:insert_new(?MODULE, {{GrpName, PidName}, Pid}) of
                true -> ok;
                false ->
                    OldPid = ets:lookup_element(?MODULE, {GrpName, PidName}, 2),
                    unlink(OldPid),
                    ets:insert(?MODULE, {{GrpName, PidName}, Pid})
            end,
            link(Pid),
            comm:send_local(ReplyTo, {pid_groups_add_done});
        {GrpName, PidName} ->
            comm:send_local(ReplyTo, {pid_groups_add_done});
        {OldGroup, OldName} ->
            comm:send_local(ReplyTo, {pid_groups_add_done, OldGroup, OldName})
    end,
    State;

on({drop_state}, State) ->
    % only for unit tests
    Links = ets:match(?MODULE, {'_', '$1'}),
    _ = [unlink(Pid) || [Pid] <- Links],
    ets:delete_all_objects(?MODULE),
    State;

on({group_and_name_of, Pid, Source}, State) ->
    % only for web debug interface
    Name = pid_groups:group_and_name_of(comm:make_local(Pid)),
    comm:send(Source, {group_and_name_of_response, Name}),
    State;

on({pid_groups_hide, GrpName, Source}, State) ->
    _ = [ begin
              ets:delete_object(?MODULE, X),
              ets:insert(pid_groups_hidden, X)
          end || X <- tab2list(), GrpName =:= element(1, element(1, X)) ],
    comm:send_local(Source, {pid_groups_hide_done, GrpName}),
    State;

on({pid_groups_unhide, GrpName, Source}, State) ->
    _ = [ begin
              ets:delete_object(pid_groups_hidden, X),
              ets:insert(?MODULE, X)
          end || X <- ets:tab2list(pid_groups_hidden), GrpName =:= element(1, element(1, X)) ],
    ets:delete(pid_groups_hidden, GrpName),
    comm:send_local(Source, {pid_groups_unhide_done, GrpName}),
    State;

on({'EXIT', FromPid, _Reason}, State) ->
    Processes = ets:match(?MODULE, {'$1', FromPid}),
    _ = [ets:delete(?MODULE, {InstanceId, Name}) || [{InstanceId, Name}] <- Processes],
    State.


%% Internal functions
%% @doc Helper for the web debug interface to convert a pidname.
-spec pidname_to_string(pidname()) -> nonempty_string().
pidname_to_string('') ->
    ?ASSERT(util:is_unittest()),
    "''"; %% make tester happy by not delivering an empty string
pidname_to_string(X) when is_atom(X) ->
    atom_to_list(X);
pidname_to_string(Name) when is_tuple(Name) ->
    lists:flatten(io_lib:format("~p", [Name]));
pidname_to_string(Name) when is_list(Name) ->
    Name.

-spec pid_of_string(groupname() | failed, nonempty_string()) -> pid() | failed.
pid_of_string(failed, _) -> failed;
pid_of_string(GrpName, [${ | _] = PidNameStr) ->
    case [Name || Name <- members_by_name(GrpName),
                  pidname_to_string(Name) =:= PidNameStr] of
        [PidName] -> pid_of(GrpName, PidName);
        [] -> failed
    end;
pid_of_string(GrpName, "''") ->
    pid_of(GrpName, '');
pid_of_string(GrpName, PidNameStr) ->
    PidName = try erlang:list_to_existing_atom(PidNameStr)
              catch error:badarg -> PidNameStr
              end,
    case pid_of(GrpName, PidName) of
        failed when is_atom(PidName) ->
            % fallback for string pidnames with matching atoms somewhere
            pid_of(GrpName, PidNameStr);
        X -> X
    end.

%% @doc Returns the pid of the pid_groups manager process (a gen_component).
-spec pid_groups_manager() -> pid() | failed.
pid_groups_manager() ->
    case whereis(?MODULE) of
        undefined ->
            log:log(error, "[ PG ] no pid_groups gen_component process found"),
            failed;
        PID ->
            %log:log(info, "[ PG ] pid_groups gen_component process found"),
            PID
    end.
