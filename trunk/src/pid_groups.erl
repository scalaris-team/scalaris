% @copyright 2007-2012 Zuse Institute Berlin,

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
%%      other, they have to now their names (dht_node, config,
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

%% for the monitoring in the web interface
-export([get_web_debug_info/2,    %% (Group, PidName) -> any()
        groups_as_json/0,
        members_by_name_as_json/1]).

%% for unittests when group is in breakpoint
-export([hide/1, unhide/1]).

-ifdef(with_export_type_support).
-export_type([pidname/0, groupname/0]).
-endif.

-type(pidname() :: atom() | nonempty_string()).
-type(groupname() :: nonempty_string()).

-type(message() ::
    {pid_groups_add, groupname(), pidname(), pid()} |
    {drop_state} |
    {group_and_name_of, Pid::comm:mypid(), Source::comm:mypid()} |
    {'EXIT', pid(), Reason::any()}).

%%% group creation
%% @doc create a new group with a random name.
-spec new() -> groupname().
new() ->
    randoms:getRandomString().

%% @doc create a new group with a given prefix.
-spec new(string()) -> groupname().
new(Prefix) ->
    Prefix ++ new().

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
    receive {pid_groups_add_done} -> ok end.


%%% operations inside a group (PRE_COND: caller is group member):
-spec my_groupname() -> groupname() | undefined.
my_groupname() -> erlang:get('$@groupname').

-spec my_pidname() -> pidname().
my_pidname() -> erlang:get('$@pidname').

%% @doc Gets the Pid of the current process' group member with the given name.
-spec get_my(pidname()) -> pid() | failed.
get_my(PidName) ->
    case my_groupname() of
        undefined -> failed;
        GrpName   -> pid_of(GrpName, PidName)
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
    case ets:lookup(?MODULE, {GrpName, PidName}) of
        [{{GrpName, PidName}, Pid}] -> Pid;
        []                          -> failed
    end.

%% @doc lookup group and pid name of a process via its pid.
-spec group_and_name_of(pid()) -> {groupname(), pidname()} | failed.
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
        []   -> failed;
        Groups -> lists:append(Groups)
    end.

-spec find_a(pidname()) -> pid() | failed.
find_a(PidName) ->
    % try in my own group first
    case get_my(PidName) of
        failed ->
            %% use process local cache
            CachedName = {'$?scalaris_pid_groups_cache', PidName},
            case erlang:get(CachedName) of
                undefined ->
                    %% search others
                    case ets:match(?MODULE, {{'_', PidName}, '$1'}) of
                        [[Pid] | _] ->
                            %% fill process local cache
                            erlang:put(CachedName, Pid),
                            Pid;
                        [] ->
                            io:format("***No pid registered for ~p~n",
                                      [PidName]),
                            failed
                    end;
                Pid ->
                    %% clean process local cache if entry is outdated
                    case erlang:is_process_alive(Pid) of
                        true -> Pid;
                        false -> erlang:erase(CachedName),
                                 find_a(PidName)
                    end
            end;
        Pid -> Pid
    end.

-spec find_all(pidname()) -> [pid()].
find_all(PidName) ->
    PidList = ets:match(?MODULE, {{'_', PidName}, '$1'}),
    lists:flatten(PidList).

-spec members(groupname()) -> [pid()].
members(GrpName) ->
    PidList = ets:match(?MODULE, {{GrpName, '_'}, '$1'}),
    lists:flatten(PidList).

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
             _     -> receive
                          {ok, {group_and_name_of_response, Name}, Pid} ->
                              _ = erlang:cancel_timer(Ref),
                              receive {timeout, Pid} -> ok
                                  after 0 -> ok
                              end,
                              pid_to_name(Name);
                          {timeout, Pid} ->
                              lists:flatten(
                                io_lib:format("~.0p (timeout)", [Pid]))
                      end
         end
     end || Ref <- Refs].

%% for the monitoring in the web interface
%% @doc get info about a process
-spec get_web_debug_info(groupname(), nonempty_string()) ->
   {struct, [{pairs, {array, [{struct, [{key | value, nonempty_string()}]}]}}]}.
get_web_debug_info(GrpName, PidNameString) ->
    PidName = try erlang:list_to_existing_atom(PidNameString)
              catch error:badarg -> PidNameString
              end,
    Pid = case pid_of(GrpName, PidName) of
              failed -> pid_of(GrpName, PidNameString);
              X -> X
          end,
    KVs =
        case Pid of
            failed -> [{"process", "unknown"}];
            _      -> util:debug_info(Pid)
        end,
    JsonKVs = [ {struct, [{key, K}, {value, toString(V)}]} || {K, V} <- KVs],
    {struct, [{pairs, {array, JsonKVs}}]}.

%% for web interface
-spec groups_as_json() ->
   {array, [{struct, [{id | text, nonempty_string()} | {leaf, false}]}]}.
groups_as_json() ->
    GroupList = groups(),
    GroupListAsStructs =
        [ {struct, [{id, El}, {text, El}, {leaf, false}]} || El <- GroupList ],
    {array, GroupListAsStructs}.

%% @doc find processes in a group (for web interface)
-spec members_by_name_as_json(groupname()) ->
   {array, [{struct, [{id | text, nonempty_string()} | {leaf, true}]}]}.
members_by_name_as_json(GrpName) ->
    PidList = members_by_name(GrpName),
    PidListAsJson =
        [ {struct, [{id, toString(GrpName) ++ "." ++ toString(El)},
                    {text, toString(El)}, {leaf, true}]}
          || El <- PidList ],
    {array, PidListAsJson}.

%% @doc hide a group of processes temporarily (for paused groups in
%% unit tests)
-spec hide(groupname()) -> ok.
hide(GrpName) ->
    comm:send_local(pid_groups_manager(),
                    {pid_groups_hide, GrpName, self()}),
    receive {pid_groups_hide_done, GrpName} -> ok end.

-spec unhide(groupname()) -> ok.
unhide(GrpName) ->
    comm:send_local(pid_groups_manager(),
                    {pid_groups_unhide, GrpName, self()}),
    receive {pid_groups_unhide_done, GrpName} -> ok end.

%%
%% pid_groups manager process (gen_component)
%%

%% @doc Starts the server
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [], [{erlang_register, ?MODULE}]).

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
    case ets:insert_new(?MODULE, {{GrpName, PidName}, Pid}) of
        true -> ok;
        false ->
            OldPid = ets:lookup_element(?MODULE, {GrpName, PidName}, 2),
            unlink(OldPid),
            ets:insert(?MODULE, {{GrpName, PidName}, Pid})
    end,
    link(Pid),
    comm:send_local(ReplyTo, {pid_groups_add_done}),
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
-spec toString(atom() | nonempty_string()) -> nonempty_string().
%%toString(X) -> io_lib:write_string(X).
toString(X) when is_atom(X) -> atom_to_list(X);
toString(X)                 -> X.

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
