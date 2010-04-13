%  Copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : config.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : config file parser for scalaris
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(config).
-author('schuett@zib.de').
-vsn('$Id$ ').
-include("scalaris.hrl").

-export([
         start_link/1, start/2,
         read/1, write/2,
         check_config/0,

         exists/1, is_atom/1, is_bool/1, is_mypid/1, is_ip/1, is_integer/1,
         is_float/1, is_string/1, is_in_range/3,
         is_greater_than/2, is_greater_than_equal/2,
         is_less_than/2, is_less_than_equal/2, is_in/2
        ]).

%% public functions

%% @doc Reads config parameter.
-spec(read/1 :: (atom()) -> any() | failed).
read(Key) ->
    case ets:lookup(config_ets, Key) of
        [{Key, Value}] ->
            %% allow values defined as application environments to override
            Value;
        [] ->
            case preconfig:get_env(Key, failed) of
                failed ->
                    failed;
                X ->
                    ets:insert(config_ets, {Key, X}),
                    X
            end
    end.

%% @doc Writes a config parameter.
-spec write(atom(), any()) -> ok.
write(Key, Value) ->
    cs_send:send_local(config, {write, self(), Key, Value}),
    receive
        {write_done} -> ok
    end.

%% gen_server setup

start_link(Files) ->
    TheFiles = case application:get_env(add_config) of
                   {ok, ConfigFile} ->
                       lists:append(Files, [ConfigFile]);
                   _Else ->
                       Files
               end,
    io:format("Config files: ~p~n", [TheFiles]),
    Owner = self(),
    Link = spawn_link(?MODULE, start, [TheFiles, Owner]),
    receive
        done ->
            ok;
        X ->
            io:format("unknown config message  ~p", [X])
    end,
    {ok, Link}.

%@private
start(Files, Owner) ->
    register(?MODULE, self()),
    ets:new(config_ets, [set, protected, named_table]),
    [ populate_db(File) || File <- Files],
    check_config() orelse halt(1),
    Owner ! done,
    loop().

loop() ->
    receive
        {write, Pid, Key, Value} ->
            ets:insert(config_ets, {Key, Value}),
            cs_send:send_local(Pid, {write_done}),
            loop();
        _ ->
            loop()
    end.

%@private
populate_db(File) ->
    case file:consult(File) of
        {ok, Terms} ->
            lists:map(fun process_term/1, Terms),
            eval_environment(os:getenv("CS_PORT"));
        {error, enoent} ->
            %% note: log4erl may not be available
            io:format("Can't load config file ~p: File does not exist. Ignoring.\n", [File]),
            fail;
        {error, Reason} ->
            %% note: log4erl may not be available
            io:format("Can't load config file ~p: ~p. Exiting.\n", [File, Reason]),
            erlang:halt(1),
            fail
    end.

eval_environment(false) ->
    ok;
eval_environment(Port) ->
    {PortInt, []} = string:to_integer(Port),
    ets:insert(config_ets, {listen_port, PortInt}).

process_term({Key, Value}) ->
    ets:insert(config_ets, {Key, preconfig:get_env(Key, Value)}).


%% check config methods

%% @doc Checks whether config parameters of all processes exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    cyclon:check_config() and
        acceptor:check_config() and
        gossip:check_config() and
        learner:check_config() and
        rdht_tx:check_config() and
        rdht_tx_read:check_config() and
        rdht_tx_write:check_config() and
        tx_tm_rtm:check_config() and
        vivaldi:check_config() and
        vivaldi_latency:check_config() and
        ?RM:check_config().

-spec exists(Key::atom()) -> boolean().
exists(Key) ->
    case read(Key) of
        failed ->
            io:format("~p not defined (see scalaris.cfg and scalaris.local.cfg)~n",
                      [Key]),
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
        false -> io:format("~p = ~p ~s (see scalaris.cfg and scalaris.local.cfg)~n",
                           [Key, Value, lists:flatten(Msg)]),
                 false
    end.

-spec is_atom(Key::atom()) -> boolean().
is_atom(Key) ->
    Pred = fun erlang:is_atom/1,
    Msg = "is not an atom",
    test_and_error(Key, Pred, Msg).

-spec is_bool(Key::atom()) -> boolean().
is_bool(Key) ->
    Pred = fun erlang:is_boolean/1,
    Msg = "is not a boolean",
    test_and_error(Key, Pred, Msg).

-spec is_mypid(Key::atom()) -> boolean().
is_mypid(Key) ->
    Pred = fun cs_send:is_valid/1,
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

-spec is_string(Key::atom()) -> boolean().
is_string(Key) ->
    IsChar = fun(X) -> (X >= 0) andalso (X =< 255) end,
    IsString = fun(Value) ->
                   case Value of
                       X when is_list(X) ->
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
is_greater_than(Key, Min) when is_atom(Min) ->
    is_greater_than(Key, read(Min));
is_greater_than(Key, Min) ->
    IsGreaterThan = fun(Value) -> (Value > Min) end,
    Msg = io_lib:format("is not larger than ~p", [Min]),
    test_and_error(Key, IsGreaterThan, Msg).

-spec is_greater_than_equal(Key::atom(), Min::number() | atom()) -> boolean().
is_greater_than_equal(Key, Min) when is_atom(Min) ->
    is_greater_than_equal(Key, read(Min));
is_greater_than_equal(Key, Min) ->
    IsGreaterThanEqual = fun(Value) -> (Value >= Min) end,
    Msg = io_lib:format("is not larger than or equal to ~p", [Min]),
    test_and_error(Key, IsGreaterThanEqual, Msg).

-spec is_less_than(Key::atom(), Max::number() | atom()) -> boolean().
is_less_than(Key, Max) when is_atom(Max) ->
    is_less_than(Key, read(Max));
is_less_than(Key, Max) ->
    IsLessThan = fun(Value) -> (Value < Max) end,
    Msg = io_lib:format("is not less than ~p", [Max]),
    test_and_error(Key, IsLessThan, Msg).

-spec is_less_than_equal(Key::atom(), Max::number() | atom()) -> boolean().
is_less_than_equal(Key, Max) when is_atom(Max) ->
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
