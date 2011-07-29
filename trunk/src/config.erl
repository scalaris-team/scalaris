%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc    Config file parser for scalaris.
%% @end
%% @version $Id$
-module(config).
-author('schuett@zib.de').
-vsn('$Id$').
-include("scalaris.hrl").

-export([
         start_link2/1, start_link2/0, start_link/1, start_link/2, start/2,
         read/1, write/2,
         check_config/0,

         exists/1, is_atom/1, is_bool/1, is_mypid/1,
         is_ip/1, is_port/1,
         is_integer/1, is_float/1,
         is_tuple/2, is_tuple/4, is_list/1, is_list/3, is_string/1,
         is_in_range/3, is_greater_than/2, is_greater_than_equal/2,
         is_less_than/2, is_less_than_equal/2, is_in/2, is_module/1,
         test_and_error/3
        ]).

%% public functions

%% @doc Reads a config parameter. If it is not found, the application's
%%      environment is checked or failed is returned. The result will be
%%      cached in the config.
-spec read(Key::atom()) -> any() | failed.
read(Key) ->
    % If an environment variable sets a config parameter that is present in the
    % config, it will override the config (see populate_db/1, process_term/1).
    % We can thus first check the ets table and fall back to the environment
    % check afterwards.
    case ets:lookup(config_ets, Key) of
        [{Key, Value}] -> Value;
        [] -> Value = util:app_get_env(Key, failed),
              case self() =:= erlang:whereis(config) of
                  true -> ets:insert(config_ets, {Key, Value});
                  _    -> write(Key, Value)
              end,
              Value
    end.

%% @doc Writes a config parameter.
-spec write(atom(), any()) -> ok.
write(Key, Value) ->
    comm:send_local(config, {write, self(), Key, Value}),
    receive
        {write_done} -> ok
    end.

%% gen_server setup

%% @doc Short for start_link/2 with no options and the given files.
-spec start_link(Files::[file:name()]) -> {ok, pid()}.
start_link(Files) ->
    start_link(Files, []).

%% @doc Starts the config process. If Options contains a
%%      {config, [{Key1, Value1},...]} tuple, each Key is set to its Value in
%%      the config.
-spec start_link(Files::[file:name()], Options::[tuple()]) -> {ok, pid()}.
start_link(Files, Options) ->
    case whereis(config) of
        Pid when is_pid(Pid) ->
            %% ct:pal("There is already a Config process:~n"),
            {ok, Pid};
        _ ->
            TheFiles = case util:app_get_env(add_config, []) of
                           []         -> Files;
                           ConfigFile -> lists:append(Files, [ConfigFile])
                       end,
%%             error_logger:info_msg("Config files: ~p~n", [TheFiles]),
            Owner = self(),
            Link = spawn_link(?MODULE, start, [TheFiles, Owner]),
            receive
                done -> ok;
                X    -> error_logger:error_msg("unknown config message  ~p", [X])
            end,
            ConfigParameters = case lists:keyfind(config, 1, Options) of
                                   {config, ConfPars} -> ConfPars;
                                   _ -> []
                               end,
            _ = [write(K, V) || {K, V} <- ConfigParameters],
            {ok, Link}
    end.

%% @doc Short for start_link2/1 with no options.
-spec start_link2() -> {ok, pid()}.
start_link2() ->
    start_link2([]).

%% @doc Starts the config process and determines the config files from the
%%      application's environment. If there is no application, "scalaris.cfg"
%%      and "scalaris.local.cfg" are used. If Options contains a
%%      {config, [{Key1, Value1},...]} tuple, each Key is set to its Value in
%%      the config.
-spec start_link2(Options::[tuple()]) -> {ok, pid()}.
start_link2(Options) ->
    Files = [util:app_get_env(config, "scalaris.cfg"),
             util:app_get_env(local_config, "scalaris.local.cfg")],
    start_link(Files, Options).

%@private
-spec start(Files::[file:name()], Owner::pid()) -> no_return().
start(Files, Owner) ->
    erlang:register(config, self()),
    _ = ets:new(config_ets, [set, protected, named_table]),
    _ = [ populate_db(File) || File <- Files],
    case check_config() of
        true -> ok;
        _    -> % wait so the error output can be written:
            init:stop(1),
            receive nothing -> ok end
    end,
    Owner ! done,
    loop().

loop() ->
    receive
        {write, Pid, Key, Value} ->
            ets:insert(config_ets, {Key, Value}),
            comm:send_local(Pid, {write_done}),
            loop();
        _ ->
            loop()
    end.

%@private
-spec populate_db(File::file:name()) -> ok | fail.
populate_db([]) -> ok;
populate_db(File) ->
    %% note: log4erl may not be available -> use error_logger instead of log
    case file:consult(File) of
        {ok, Terms} ->
            _ = lists:map(fun process_term/1, Terms),
            ok;
        {error, enoent} ->
            error_logger:info_msg("Can't load config file ~p: File does not exist. "
                                  " Ignoring.\n", [File]),
            fail;
        {error, Reason} ->
            error_logger:error_msg("Can't load config file ~p: ~p. Exiting.\n",
                                   [File, Reason]),
            init:stop(1),
            receive nothing -> ok end
            %fail
    end.

-spec process_term({Key::atom(), Value::term()}) -> true.
process_term({Key, Value}) ->
    ets:insert(config_ets, {Key, util:app_get_env(Key, Value)}).

%% check config methods

%% @doc Checks whether config parameters of all processes exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    Checks =
        [ case X() of
              true -> true;
              false ->
                  ct:pal(error, "check_config ~p failed.~n", [X]),
                  false
          end || X  <- [ fun log:check_config/0,
                         fun sup_scalaris:check_config/0,
                         fun sup_dht_node_core:check_config/0,
                         fun cyclon:check_config/0,
                         fun acceptor:check_config/0,
                         fun gossip:check_config/0,
                         fun learner:check_config/0,
                         fun rdht_tx:check_config/0,
                         fun rdht_tx_read:check_config/0,
                         fun rdht_tx_write:check_config/0,
                         fun ?RT:check_config/0,
                         fun rt_loop:check_config/0,
                         fun tx_tm_rtm:check_config/0,
                         fun vivaldi:check_config/0,
                         fun vivaldi_latency:check_config/0,
                         fun ?RM:check_config/0,
                         fun fd_hbs:check_config/0,
                         fun dht_node_move:check_config/0,
                         fun dht_node_join:check_config/0,
                         %% note: need to check all passive load
                         %%       balancing algorithm's parameters
                         %%       (another node may ask us to provide
                         %%       a candidate for any of them)
                         fun lb_psv_simple:check_config/0,
                         fun lb_psv_split:check_config/0,
                         fun lb_psv_gossip:check_config/0,
                         fun comm_acceptor:check_config/0,
                         fun monitor:check_config/0,
                         fun monitor_perf:check_config/0,
                         fun rep_upd:check_config/0,
                         fun sup_yaws:check_config/0 ]],
    lists:foldl(fun(A,B) -> A and B end, true, Checks).

-spec exists(Key::atom()) -> boolean().
exists(Key) ->
    case read(Key) of
        failed ->
            error_logger:error_msg("~p not defined (see scalaris.cfg and scalaris.local.cfg)~n", [Key]),
            false;
        _X -> true
    end.

%% @doc Tests the config parameter stored under atom Key with function Pred and
%%      prints an error message if not, also returns the result.
-spec test_and_error(Key::atom(), Pred::fun((any()) -> boolean()), Msg::list()) -> boolean().
test_and_error(Key, Pred, Msg) ->
    Value = read(Key),
    case exists(Key) andalso Pred(Value) of
        true -> true;
        false -> error_logger:error_msg("~p = ~p ~s (see scalaris.cfg and scalaris.local.cfg)~n",
                                            [Key, Value, lists:flatten(Msg)]),
                 false
    end.

-spec is_atom(Key::atom()) -> boolean().
is_atom(Key) ->
    Pred = fun erlang:is_atom/1,
    Msg = "is not an atom",
    test_and_error(Key, Pred, Msg).

-spec is_module(Key::atom()) -> boolean().
is_module(Key) ->
    Pred = fun(Value) ->
                   erlang:is_atom(Value) andalso
                       code:which(Value) =/= non_existing
           end,
    Msg = "is not an existing module",
    test_and_error(Key, Pred, Msg).

-spec is_bool(Key::atom()) -> boolean().
is_bool(Key) ->
    Pred = fun erlang:is_boolean/1,
    Msg = "is not a boolean",
    test_and_error(Key, Pred, Msg).

-spec is_mypid(Key::atom()) -> boolean().
is_mypid(Key) ->
    Pred = fun comm:is_valid/1,
    Msg = "is not a valid pid",
    test_and_error(Key, Pred, Msg).

-spec is_ip(Key::atom()) -> boolean().
is_ip(Key) ->
    IsIp = fun(Value) ->
                   case Value of
                       {IP1, IP2, IP3, IP4} ->
                           ((IP1 >= 0) andalso (IP1 =< 255)
                            andalso (IP2 >= 0) andalso (IP2 =< 255)
                            andalso (IP3 >= 0) andalso (IP3 =< 255)
                            andalso (IP4 >= 0) andalso (IP4 =< 255));
                       _X -> false
                   end
           end,
    Msg = "is not a valid IPv4 address",
    test_and_error(Key, IsIp, Msg).

-spec is_port(Key::atom()) -> boolean().
is_port(Key) ->
    IsPort = fun(Value) ->
                     case Value of
                         X when erlang:is_integer(X) ->
                             true;
                         X when erlang:is_list(X) ->
                             lists:all(fun erlang:is_integer/1, X);
                         {From, To} ->
                             erlang:is_integer(From) andalso
                                 erlang:is_integer(To);
                         _ -> false
                     end
             end,
    Msg = "is not a valid Port address",
    test_and_error(Key, IsPort, Msg).

-spec is_integer(Key::atom()) -> boolean().
is_integer(Key) ->
    Pred = fun erlang:is_integer/1,
    Msg = "is not a valid integer",
    test_and_error(Key, Pred, Msg).

-spec is_float(Key::atom()) -> boolean().
is_float(Key) ->
    Pred = fun erlang:is_float/1,
    Msg = "is not a valid float",
    test_and_error(Key, Pred, Msg).

-spec is_tuple(Key::atom(), TupleSize::pos_integer()) -> boolean().
is_tuple(Key, Size) ->
    Pred = fun(Value) ->
                   erlang:is_tuple(Value) and
                       (erlang:tuple_size(Value) =:= Size)
           end,
    Msg = io_lib:format("is not a valid tuple of size ~p", [Size]),
    test_and_error(Key, Pred, Msg).

-spec is_tuple(Key::atom(), TupleSize::pos_integer(), Pred::fun((any()) -> boolean()), PredDescr::string()) -> boolean().
is_tuple(Key, Size, Pred, PredDescr) ->
    CompletePred = fun(Value) ->
                           erlang:is_tuple(Value) and
                               (erlang:tuple_size(Value) =:= Size) and
                               Pred(Value)
                   end,
    Msg = io_lib:format("is not a valid tuple of size ~p satisfying ~p", [Size, PredDescr]),
    test_and_error(Key, CompletePred, Msg).

-spec is_list(Key::atom()) -> boolean().
is_list(Key) ->
    Pred = fun erlang:is_list/1,
    Msg = "is not a valid list",
    test_and_error(Key, Pred, Msg).

-spec is_list(Key::atom(), Pred::fun((any()) -> boolean()), PredDescr::string()) -> boolean().
is_list(Key, Pred, PredDescr) ->
    IsListWithPred = fun(Value) ->
                             case Value of
                                 X when erlang:is_list(X) ->
                                     lists:all(Pred, X);
                                 _X -> false
                             end
                     end,
    Msg = io_lib:format("is not a valid list with elements satisfying ~p", [PredDescr]),
    test_and_error(Key, IsListWithPred, Msg).

-spec is_string(Key::atom()) -> boolean().
is_string(Key) ->
    IsChar = fun(X) -> (X >= 0) andalso (X =< 255) end,
    IsString = fun(Value) ->
                   case Value of
                       X when erlang:is_list(X) ->
                           lists:all(IsChar, X);
                       _X -> false
                   end
           end,
    Msg = "is not a (printable) string",
    test_and_error(Key, IsString, Msg).

-spec is_in_range(Key::atom(), Min::number(), Max::number()) -> boolean().
is_in_range(Key, Min, Max) ->
    IsInRange = fun(Value) -> (Value >= Min) andalso (Value =< Max) end,
    Msg = io_lib:format("is not between ~p and ~p (both inclusive)",
                        [Min, Max]),
    test_and_error(Key, IsInRange, Msg).

-spec is_greater_than(Key::atom(), Min::number() | atom()) -> boolean().
is_greater_than(Key, Min) when erlang:is_atom(Min) ->
    is_greater_than(Key, read(Min));
is_greater_than(Key, Min) ->
    IsGreaterThan = fun(Value) -> (Value > Min) end,
    Msg = io_lib:format("is not larger than ~p", [Min]),
    test_and_error(Key, IsGreaterThan, Msg).

-spec is_greater_than_equal(Key::atom(), Min::number() | atom()) -> boolean().
is_greater_than_equal(Key, Min) when erlang:is_atom(Min) ->
    is_greater_than_equal(Key, read(Min));
is_greater_than_equal(Key, Min) ->
    IsGreaterThanEqual = fun(Value) -> (Value >= Min) end,
    Msg = io_lib:format("is not larger than or equal to ~p", [Min]),
    test_and_error(Key, IsGreaterThanEqual, Msg).

-spec is_less_than(Key::atom(), Max::number() | atom()) -> boolean().
is_less_than(Key, Max) when erlang:is_atom(Max) ->
    is_less_than(Key, read(Max));
is_less_than(Key, Max) ->
    IsLessThan = fun(Value) -> (Value < Max) end,
    Msg = io_lib:format("is not less than ~p", [Max]),
    test_and_error(Key, IsLessThan, Msg).

-spec is_less_than_equal(Key::atom(), Max::number() | atom()) -> boolean().
is_less_than_equal(Key, Max) when erlang:is_atom(Max) ->
    is_less_than_equal(Key, read(Max));
is_less_than_equal(Key, Max) ->
    IsLessThanEqual = fun(Value) -> (Value =< Max) end,
    Msg = io_lib:format("is not less than or equal to ~p", [Max]),
    test_and_error(Key, IsLessThanEqual, Msg).

-spec is_in(Key::atom(), ValidValues::[any(),...]) -> boolean().
is_in(Key, ValidValues) ->
    IsIn = fun(Value) -> lists:any(fun(X) -> X =:= Value end,
                                   ValidValues) end,
    Msg = io_lib:format("is not allowed (valid values: ~p)",
                        [ValidValues]),
    test_and_error(Key, IsIn, Msg).
