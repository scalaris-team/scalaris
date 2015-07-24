%  @copyright 2007-2015 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Supervisor helper functions for better control of startup phase.
%%
%%      Start the supervisor separately and then add its childs one
%%      after the other.
%% @end
%% @version $Id$
-module(sup).
-author('schintke@zib.de').
-vsn('$Id$').

-include("types.hrl").

-export([sup_start/3,
         sup_get_all_children/1,
         sup_terminate/1,
         sup_terminate_childs/1]).
-export([worker_desc/3,
         worker_desc/4,
         supervisor_desc/3,
         supervisor_desc/4]).

%% for admin:add_node and unittests
-export([start_sup_as_child/3]).

%% Typical call structure is:
%% sup_start(SupName, Module, Options)
%% Module:supspec(Options)
%% Erlang supervisor:start_link(SupName, Module, Options) is called
%% Erlamg supervisor calls Module:init(Options) in spawned process
%%   which return the supervisor spec
%%   and in normal Erlang its childs
%%   when childs are given, supervisor:start_link also start the childs
%%   with sup it is intended to add the childs later...
%% Module:childs(Options) is called to get the childs
%% for each child, the given spec is used to start it.

%% when a child is a supervisor: how is the group info passed from
%% Module:init to the childs? Only via the Options, so the group cannot be created in Module:init anymore...

-type prefix() :: nonempty_maybe_improper_list(string(), string()).

%% taken from supervisor.erl (not exported there)
-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | {'error', startlink_err()}.
-type sup_name() :: {'local', Name :: atom()} | {'global', Name :: atom()}.
-type sup_ref()  :: (Name :: atom())
                  | {Name :: atom(), Node :: node()}
                  | {'global', Name :: atom()}
                  | pid().

-spec sup_start(sup_name(), module(), term()) -> startlink_ret().
sup_start(Supervisor, Module, Options) ->
    InitialPrefix = "+",
    sup_start([InitialPrefix], Supervisor, Module, Options).

-spec sup_start(prefix(),
                sup_name() | no_name, module(), term())
               -> startlink_ret().
sup_start(Prefix, Supervisor, Module, Options) ->
    progress(Prefix ++ "~p ~p.erl~n", [Supervisor, Module]),
    SupSpec = trycall(Prefix, Module, supspec, Options, unknown_supspec),
    Res = case Supervisor of
              no_name -> supervisor:start_link(Module, Options);
              _ -> supervisor:start_link(Supervisor, Module, Options)
          end,
    progress(Prefix ++ "~p done~n", [Supervisor]),
    case Res of
        {ok, SupRef} ->
            progress(Prefix ++ "`-~p ~p~n", [SupSpec, SupRef]),
            Childs = trycall(Prefix, Module, childs, Options, []),
            ChildPrefix = last_prefix_to_space(Prefix),
            ChildsRes = add_childs(ChildPrefix ++ ["  +-"], SupRef, Childs),
            if is_tuple(ChildsRes) andalso element(1, ChildsRes) =:= ok ->
                   Res; %% return pid of supervisor as it may be linked to externally;
               true ->
                   io:format("Startup raised ~p.~n", [ChildsRes]),
                   sup_terminate(SupRef),
                   ChildsRes
            end;
        Error ->
            progress(Prefix ++ "~.0p", [Error]),
            Error
    end.

-spec start_sup_as_child(prefix(),
                         sup_ref() | no_name,
                         supervisor:child_spec())
               -> supervisor:startchild_ret().
start_sup_as_child(Prefix, AtSup, SupAsChild) ->
    progress(Prefix ++ "supervisor ~.0p ~.0p~n",
             [element(1, SupAsChild), element(2, SupAsChild)]),
    Module = element(1, element(2, SupAsChild)),
    Args = element(3, element(2, SupAsChild)),
    PipePrefix = last_prefix_to_pipe(Prefix),
    SupSpec = trycall(PipePrefix,
                      Module, supspec, Args, unknown_supspec),
    case SupSpec of
        unknown_supspec ->
            progress(PipePrefix ++ "childs are: ~.0p", [element(6, SupAsChild)]);
        _ -> ok
    end,
    Res = supervisor:start_child(AtSup, SupAsChild),
%%    io:format("Res is ~.0p", [Res]),
    case Res of
        %% {ok, SupRef} | {ok, SupRef, _GroupInfo}:
        X when is_tuple(X) andalso element(1, X) =:= ok ->
            SupRef = element(2, X),
            case SupSpec of
                unknown_supspec ->
                    progress(PipePrefix ++ "started childs at ~p:~n",
                             [SupRef]),
                    show_started_childs(PipePrefix ++ ["+-"], SupRef),
                    Res;
                _ ->
                    progress(PipePrefix
                             ++ "~.0p ~.0p~n", [SupSpec, SupRef]),
                    Childs = trycall(PipePrefix, Module, childs, Args, []),
                    ChildsRes = add_childs(PipePrefix ++ ["+-"], SupRef, Childs),
                    if is_tuple(ChildsRes) andalso element(1, ChildsRes) =:= ok ->
                           Res; %% return pid of supervisor as it may be linked to externally;
                       true ->
                           io:format("Startup raised ~p.~n", [ChildsRes]),
                           SupName = element(1, SupAsChild),
                           sup_terminate_childs(SupRef),
                           _ = supervisor:terminate_child(AtSup, SupName),
                           _ = supervisor:delete_child(AtSup, SupName),
                           ChildsRes
                    end
               end;
        Error ->
            progress(Prefix ++ " ~p~n", [Error]),
            Error
    end.

trycall(Prefix, Module, Func, Args, DefaultReturnValue) ->
    try Module:Func(Args)
    catch error:undef ->
            FlatPrefix = lists:flatten(Prefix),
            progress(FlatPrefix ++ "~p provides no ~p/1 function. Fall back to normal supervisor startup here.~n", [Module, Func]),
            DefaultReturnValue;
          X:Y ->
            io:format("~p:~p failed with ~p:~p ~p~n",
                      [Module, Func, X, Y, erlang:get_stacktrace()]),
            DefaultReturnValue
    end.

add_childs(_Prefix, SupRef, []) -> {ok, SupRef};
add_childs(Prefix, SupRef, [Child]) ->
    LastChildPrefix = last_prefix_to_backslash(Prefix),
    add_child(LastChildPrefix, SupRef, Child);

add_childs(Prefix, SupRef, [Child | Tail]) ->
    Res = add_child(Prefix, SupRef, Child),
    case Res of
        {ok, _Pid} ->
            add_childs(Prefix, SupRef, Tail);
        {ok, _Pid, _GroupInfo} ->
            add_childs(Prefix, SupRef, Tail);
        Error ->
            Error
    end.

add_child(Prefix, SupRef, Child) ->
    case Child of
        {Name, MFA, _Kind, _Kill, worker, _Opts} ->
            progress(Prefix ++ "~.0p ~.0p ", [Name, MFA]),
            Res = supervisor:start_child(SupRef, Child),
            _ = case Res of
                {ok, undefined} ->
                    progress("not started~n");
                {ok, Pid} -> progress("~p.~n", [Pid]);
                {ok, Pid, GroupInfo} ->
                        progress("~.0p ~.0p.", [Pid, GroupInfo]);
                Error ->
                    progress("~nFailed to start ~p reason ~p~n", [Child, Error]),
                    progress("Supervisor ~p has childs: ~p~n",
                              [SupRef, sup_which_children(SupRef)]),
                    Error
            end,
            Res;
        {_Name, _MFA, _Kind, _Kill, supervisor, _Opts} ->
            %% output is done by sup_start
            start_sup_as_child(Prefix, SupRef, Child)
    end.

show_started_childs(Prefix, SupRef) ->
    Childs = sup_which_children(SupRef),
    show_childs(Prefix, Childs).

show_childs(_Prefix, []) -> ok;
show_childs(Prefix, [LastChild]) ->
    show_child(last_prefix_to_backslash(Prefix), LastChild);
show_childs(Prefix, [X | Tail]) ->
    show_child(Prefix, X),
    show_childs(Prefix, Tail).

show_child(Prefix, Child) ->
    case element(3, Child) of
        worker ->
            progress(Prefix ++ "~.0p~n", [Child]);
        supervisor ->
            progress(Prefix ++ "~.0p~n", [Child]),
            show_started_childs(last_prefix_to_pipe(Prefix) ++ ["+-"], element(2, Child))
    end.


last_prefix_to_space([LastElem]) ->
    [[case X of
         $+ -> $
     end || X <- LastElem]];
last_prefix_to_space([X | T]) ->
    [X | last_prefix_to_space(T)].

last_prefix_to_pipe([LastElem]) ->
    [[case X of
         $+ -> $|;
         $- -> $ ;
         $` -> $ ;
         _ -> X
     end || X <- LastElem]];
last_prefix_to_pipe([X | T]) ->
    [X | last_prefix_to_pipe(T)].

last_prefix_to_backslash([LastElem]) ->
    [[case X of
         $+ -> $`;
         _ -> X
     end || X <- LastElem]];
last_prefix_to_backslash([X | T]) ->
    [X | last_prefix_to_backslash(T)].

-spec progress(prefix()) -> ok.
progress(Fmt) ->
    util:if_verbose(lists:flatten(Fmt)),
    ok.
-spec progress(prefix(), list()) -> ok.
progress(Fmt, Args) ->
    util:if_verbose(lists:flatten(Fmt), Args),
    ok.

%% @doc Creates a worker description for a supervisor.
-spec worker_desc(Name::atom() | string() | {atom(), pos_integer()}, Module::module(),
                  Function::atom())
        -> {Name::atom() | string() | {atom(), pos_integer()}, {Module::module(), Function::atom(), Options::[]},
            permanent, brutal_kill, worker, []}.
worker_desc(Name, Module, Function) ->
    worker_desc(Name, Module, Function, []).

%% @doc Creates a worker description for a supervisor.
-spec worker_desc(Name::atom() | string() | {atom(), pos_integer()}, Module::module(),
                  Function::atom(), Options::list())
        -> {Name::atom() | string() | {atom(), pos_integer()}, {Module::module(), Function::atom(), Options::list()},
            permanent, brutal_kill, worker, []}.
worker_desc(Name, Module, Function, Options) ->
    {Name, {Module, Function, Options}, permanent, brutal_kill, worker, []}.

%% @doc Creates a supervisor description for a supervisor.
-spec supervisor_desc(Name::atom() | string() | {atom(), pos_integer()}, Module::module(),
                      Function::atom())
        -> {Name::atom() | string() | {atom(), pos_integer()},
            {Module::module(), Function::atom(), Options::[]},
            permanent, brutal_kill, supervisor, []}.
supervisor_desc(Name, Module, Function) ->
    supervisor_desc(Name, Module, Function, []).

%% @doc Creates a supervisor description for a supervisor.
-spec supervisor_desc(Name::atom() | string() | {atom(), pos_integer()}, Module::module(),
                      Function::atom(), Options::list())
        -> {Name::atom() | string() | {atom(), pos_integer()},
            {Module::module(), Function::atom(), Options::list()},
            permanent, brutal_kill, supervisor, []}.
supervisor_desc(Name, Module, Function, Args) ->
    {Name, {Module, Function, Args}, permanent, brutal_kill, supervisor, []}.

-spec sup_terminate(Supervisor::pid() | atom()) -> ok.
sup_terminate(SupPid) ->
    sup_terminate_childs(SupPid),
    case is_pid(SupPid) of
        true -> exit(SupPid, kill);
        false ->
            case whereis(SupPid) of
                SupPid2 when is_pid(SupPid2) -> exit(SupPid2, kill);
                _ -> ok
            end
    end,
    util:wait_for_process_to_die(SupPid),
    ok.

%% @doc Terminates all children of the given supervisor(s) gracefully, i.e. first
%%      stops all gen_component processes and then terminates all children
%%      recursively.
%%      Note: the children beneath the given supervisor pids MUST NOT overlap!
-spec sup_terminate_childs(Supervisor::Proc | [Proc]) -> ok
    when is_subtype(Proc, pid() | atom()).
sup_terminate_childs(SupPid) when is_pid(SupPid) orelse is_atom(SupPid) ->
    sup_terminate_childs([SupPid]);
sup_terminate_childs(SupPids) when is_list(SupPids) ->
    _ = [sup_pause_childs(SupPid) || SupPid <- SupPids],
    _ = [sup_kill_childs(SupPid) || SupPid <- SupPids],
    ok.

%% @doc Pauses all children of the given supervisor (recursively) by setting
%%      an appropriate breakpoint.
-spec sup_pause_childs(Supervisor::pid() | atom()) -> ok.
sup_pause_childs(SupPid) ->
    ChildSpecs = sup_which_children(SupPid),
    Self = self(),
    _ = [ begin
              case Type of
                  supervisor ->
                      sup_pause_childs(Pid);
                  worker ->
                      case gen_component:is_gen_component(Pid) of
                          true -> gen_component:bp_about_to_kill(Pid);
                          _    -> ok
                      end
              end
          end ||  {_Id, Pid, Type, _Module} <- ChildSpecs,
                  Pid =/= undefined, Pid =/= Self, is_process_alive(Pid) ],
    ok.

-spec get_tables_of(pid()) -> {MNesia::[atom()], Ets::[ets:tid() | atom()]} | [ets:tid() | atom()].
get_tables_of(Pid)->
    case config:read(db_backend) of
        db_mnesia ->
            {db_mnesia:mnesia_tables_of(pid_groups:my_groupname()),
             util:ets_tables_of(Pid)};
        _ ->
            util:ets_tables_of(Pid)
    end.

-spec delete_tables(pid(), {MNesia::[atom()], Ets::[ets:tid() | atom()]} | [ets:tid() | atom()] ) -> ok.
delete_tables(Pid, Db) ->
    case config:read(db_backend) of
        db_mnesia ->
            {MNesia, Ets} = Db,
            _ = db_mnesia:delete_mnesia_tables(MNesia),
            ok;
        _ ->
            Ets = Db,
            ok
    end,
    _ = [ util:wait_for_ets_table_to_disappear(Pid, Tab) || Tab <- Ets ],
    ok.

%% @doc Kills all children of the given supervisor (recursively) after they
%%      have been paused by sup_pause_childs/1.
-spec sup_kill_childs(Supervisor::pid() | atom()) -> ok.
sup_kill_childs(SupPid) ->
    ChildSpecs = sup_which_children(SupPid),
    _ = [ try
              case Type of
                  supervisor -> sup_kill_childs(Pid);
                  worker     -> ok
              end,
              Tables = get_tables_of(Pid),
              _ = supervisor:terminate_child(SupPid, Id),
              _ = supervisor:delete_child(SupPid, Id),
              util:wait_for_process_to_die(Pid),
              delete_tables(Pid, Tables),
              ok
          catch
              % child may not exist any more due to a parallel process terminating it
              exit:{killed, _} -> ok;
              exit:{noproc, _} -> ok;
              % exit reason may encapsulate a previous failure
              exit:{{killed, _}, _} -> ok;
              exit:{{noproc, _}, _} -> ok
          end ||  {Id, Pid, Type, _Module} <- ChildSpecs,
                  Pid =/= undefined, is_process_alive(Pid) ],
    ok.

-spec sup_get_all_children(Supervisor::pid() | atom()) -> [pid()].
sup_get_all_children(Supervisor) ->
    AllChilds = [X || X = {_, Pid, _, _} <- sup_which_children(Supervisor),
                      Pid =/= undefined],
    WorkerChilds = [Pid ||  {_Id, Pid, worker, _Modules} <- AllChilds],
    SupChilds = [Pid || {_Id, Pid, supervisor, _Modules} <- AllChilds],
    lists:flatten([WorkerChilds | [sup_get_all_children(S) || S <- SupChilds]]).

%% @doc Hardened version of supervisor:which_children/1 which returns an empty
%%      list of childs if the supervisor has been killed, e.g. by another
%%      process.
-spec sup_which_children(Supervisor::pid() | atom()) ->
          [{Id::term(), Pid::pid() | undefined, Type::worker | supervisor,
            Modules::[module()] | dynamic}].
sup_which_children(Supervisor) ->
    try supervisor:which_children(Supervisor)
    catch exit:{killed, _} -> [];
          exit:{noproc, _} -> []
    end.
