%  @copyright 2007-2012 Zuse Institute Berlin

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
%% @version $Id: $
-module(sup).
-author('schintke@zib.de').
-vsn('$Id: ').

-export([sup_start/3]).

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
            TotalRes = add_childs(ChildPrefix ++ ["  +-"], SupRef, Childs),
            case TotalRes of
                {ok, _} ->
                    io:format("Scalaris started successfully."
                              " Hit <return> to see the erlang shell prompt.~n");
                Err ->
                    io:format("Startup raised ~p.~n", [Err])
            end,
            TotalRes;
        Error ->
            progress(Prefix ++ "~.0p", [Error]),
            Error
    end.

-spec start_sup_as_child(prefix(),
                         sup_ref() | no_name,
                         supervisor:child_spec())
               -> startlink_ret().
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
        {ok, SupRef} ->
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
                    add_childs(PipePrefix ++ ["+-"], SupRef, Childs)
               end;
        {ok, SupRef, _GroupInfo} ->
            case SupSpec of
                unknown_supspec ->
                    progress(PipePrefix ++ "started childs at ~p:~n",
                             [SupRef]),
                    show_started_childs(PipePrefix ++ ["+-"], SupRef),
                    Res;
                _ ->
                    progress(PipePrefix
                             ++ "~.0p ~.0p~n", [SupSpec, SupRef]),
                    Childs = trycall(PipePrefix, Module, childs, Args, _Default = []),
                    add_childs(PipePrefix ++ ["+-"], SupRef, Childs)
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
                      [Module, Func, X, Y, util:get_stacktrace()]),
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
                {ok, _Pid} -> ok;
                {ok, _Pid, _GroupInfo} -> ok;
                Error ->
                    progress("~nFailed to start ~p reason ~p~n", [Child, Error]),
                    progress("Supervisor ~p has childs: ~p~n",
                              [SupRef, supervisor:which_children(SupRef)]),
                    Error
            end,
            Res;
        {_Name, _MFA, _Kind, _Kill, supervisor, _Opts} ->
            %% output is done by sup_start
            start_sup_as_child(Prefix, SupRef, Child)
    end.

show_started_childs(Prefix, SupRef) ->
    Childs = supervisor:which_children(SupRef),
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
