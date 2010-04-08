-module(preconfig).

-export([get_env/2]).

-export([log_path/0, cs_log_file/0, mem_log_file/0, docroot/0, config/0,
         local_config/0, cs_port/0, cs_instances/0, yaws_port/0]).

%% @doc path to the log directory
%% @spec log_path() -> string()
log_path() ->
    get_env(log_path, "../log", "../log", "../log").

%% @doc path to the scalaris log file
%% @spec cs_log_file() -> string()
cs_log_file() ->
    filename:join(log_path(), "cs_log.txt").

%% @doc path to the mem log file
%% @spec mem_log_file() -> string()
mem_log_file() ->
    filename:join(log_path(), "mem.txt").

%% @doc document root for the application yaws server
%% @spec docroot() -> string()
docroot() ->
    get_env(docroot, "../docroot", "../docroot_node", "../docroot_node").

%% @doc path to the scalaris config file
%% @spec config() -> string()
config() ->
    get_env(config, "scalaris.cfg", "scalaris.cfg", "scalaris.cfg").

%% @doc path to the scalaris local config file
%% @spec local_config() -> string()
local_config() ->
    get_env(local_config, "scalaris.local.cfg", "scalaris.local.cfg", "scalaris.local.cfg").

%% @doc internet port for scalaris
%% @spec cs_port() -> string()
cs_port() ->
    get_int_from_env(cs_port, "14195", "14196", "14197").

%% @doc number of cloned instances of scalaris to run
%% @spec cs_instances() -> string()
cs_instances() ->
    get_int_from_env(cs_instances, "1", "1", "1").

%% @doc yaws http port to serve
%% @spec yaws_port() -> int()
yaws_port() ->
    get_int_from_env(yaws_port, 8000, 8001, 8002).

%% @doc get an application environment with defaults
%% @spec get_env(env, default) -> string()
get_env(Env, Def) ->
    get_env(Env, Def, Def, Def).

%% @doc get an application environment with defaults
%% @spec get_env(env, boot, node, client) -> string()
get_env(Env, Boot_Def, Scalaris_Def, Client_Def) ->
    %% io:format("preconfig:get_env(~p,~p,~p) -> ~p~n", [Env, Boot_Def, Scalaris_Def, application:get_env(Env)]),
    case application:get_env(Env) of
        {ok, Val} -> Val;
        _Else ->
            case application:get_application() of
                {ok, boot_cs} -> Boot_Def;
                {ok, scalaris } -> Scalaris_Def;
                {ok, client_cs } -> Client_Def;
		undefined -> Boot_Def
            end
    end.

%% @doc get a port number from the environment with defaults
%% @spec get_int_from_env(env, boot, node, client) -> string()
get_int_from_env(Env, Boot_Def, Scalaris_Def, Client_Def) ->
    %% io:format("preconfig:get_env(~p,~p,~p) -> ~p~n", [Env, Boot_Def, Scalaris_Def, application:get_env(Env)]),
    Int = fun(Value) ->
                  case is_list(Value) of
                      true -> list_to_integer(Value);
                      false -> Value
                  end
          end,
    case application:get_env(Env) of
        {ok, Val} -> Int(Val);
        _ ->
            case application:get_application() of
                {ok, boot_cs} -> Int(Boot_Def);
                {ok, scalaris } -> Int(Scalaris_Def);
                {ok, client_cs } -> Int(Client_Def);
		undefined -> Int(Boot_Def);
                Else ->
                    io:format("application:get_application() returned ~p~n", [Else]),
                    Else
            end
    end.
