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

%% @author Roger Critchlow
%% @doc Extracting data from the environment of an application.
%% @end
%% @version $Id$
-module(preconfig).
-author('Roger Critchlow').
-vsn('$Id$').

-export([get_env/2]).

-export([log_path/0, cs_log_file/0, mem_log_file/0, docroot/0, config/0,
         local_config/0, cs_port/0, cs_instances/0, yaws_port/0]).

%% @doc path to the log directory
-spec log_path() -> string().
log_path() ->
    get_env(log_path, "../log", "../log").

%% @doc path to the scalaris log file
-spec cs_log_file() -> string().
cs_log_file() ->
    filename:join(log_path(), "scalaris_error_logger.txt").

%% @doc path to the mem log file
-spec mem_log_file() -> string().
mem_log_file() ->
    filename:join(log_path(), "mem.txt").

%% @doc document root for the application yaws server
-spec docroot() -> string().
docroot() ->
    get_env(docroot, "../docroot", "../docroot").

%% @doc path to the scalaris config file
-spec config() -> string().
config() ->
    get_env(config, "scalaris.cfg", "scalaris.cfg").

%% @doc path to the scalaris local config file
-spec local_config() -> string().
local_config() ->
    get_env(local_config, "scalaris.local.cfg", "scalaris.local.cfg").

%% @doc internet port for scalaris
-spec cs_port() -> Ports::integer() | [integer()] | {FromPort::integer(), ToPort::integer()}.
cs_port() ->
    Port = get_env(cs_port, 14195, 14196),
    case Port of
        X when is_integer(X) -> X;
        X when is_list(X) -> X;
        X = {From, To} when is_integer(From) andalso is_integer(To) -> X;
        X ->
            error_logger:error_msg("get_env(~w) returned ~w (expected integer(), [integer()] or {integer(), integer()})~n", [cs_port, X]),
            erlang:exit(unsupported_type)
    end.

%% @doc number of cloned instances of scalaris to run
-spec cs_instances() -> integer().
cs_instances() ->
    get_int_from_env(cs_instances, 1, 1).

%% @doc yaws http port to serve
-spec yaws_port() -> integer().
yaws_port() ->
    get_int_from_env(yaws_port, 8000, 8001).

%% @doc get an application environment with defaults
-spec get_env(Env::atom(), Default::T) -> T.
get_env(Env, Def) ->
    get_env(Env, Def, Def).

%% @doc get an application environment with defaults
-spec get_env(Env::atom(), BootDef::T, NodeDef::T) -> T.
get_env(Env, Boot_Def, Scalaris_Def) ->
    %% io:format("preconfig:get_env(~p,~p,~p) -> ~p~n", [Env, Boot_Def, Scalaris_Def, application:get_env(Env)]),
    case application:get_env(Env) of
        {ok, Val} -> Val;
        _ ->
            case application:get_application() of
                {ok, boot_cs} -> Boot_Def;
                {ok, scalaris } -> Scalaris_Def;
                undefined -> Boot_Def;
                App ->
                    error_logger:error_msg("application:get_application() returned ~w~n", [App]),
                    erlang:exit(unknown_application)
            end
    end.

%% @doc get a port number from the environment with defaults
-spec get_int_from_env(Env::atom(), BootDef::integer(), NodeDef::integer()) -> integer().
get_int_from_env(Env, Boot_Def, Scalaris_Def) ->
    %% io:format("preconfig:get_env(~p,~p,~p) -> ~p~n", [Env, Boot_Def, Scalaris_Def, application:get_env(Env)]),
    case application:get_env(Env) of
        {ok, Val} when is_list(Val) -> list_to_integer(Val);
        {ok, Val} when is_integer(Val) -> Val;
        {ok, Val} when not is_integer(Val) ->
            error_logger:error_msg("application:get_env(~w) returned ~w (expected string or integer)~n", [Env, Val]),
            erlang:exit(unsupported_type);
        _ ->
            case application:get_application() of
                {ok, boot_cs} -> Boot_Def;
                {ok, scalaris } -> Scalaris_Def;
                undefined -> Boot_Def;
                App ->
                    error_logger:error_msg("application:get_application() returned ~w~n", [App]),
                    erlang:exit(unknown_application)
            end
    end.
