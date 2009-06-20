%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : transaction_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/transstore/*.erl
%%%
%%% Created :  14 Mar 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(transaction_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [read, write, write_read, write2_read2, tfuns, multi_write].

suite() ->
    [
     {timetrap, {seconds, 40}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    Pid = unittest_helper:make_ring(4),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    %error_logger:tty(false),
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    unittest_helper:stop_ring(Pid),
    ok.

read(_Config) ->
    ?equals(transaction_api:quorum_read("UnknownKey"), {fail, not_found}),
    ok.

write(_Config) ->
    ?equals(transaction_api:single_write("WriteKey", "Value"), commit),
    ok.

write_read(_Config) ->
    ?equals(transaction_api:single_write("Key", "Value"), commit),
    ?equals(transaction_api:quorum_read("Key"), {"Value", 0}),
    ok.

write2_read2(_Config) ->
    KeyA = "KeyA",
    KeyB = "KeyB",
    ValueA = "Value1",
    ValueB = "Value2",
    SuccessFun = fun(X) -> {success, X} end,
    FailureFun = fun(Reason)-> {failure, Reason} end,

    TWrite2 = 
        fun(TransLog)->
                {ok, TransLog1} = transaction_api:write(KeyA, ValueA, TransLog),
                {ok, TransLog2} = transaction_api:write(KeyB, ValueB, TransLog1),
                {{ok, ok}, TransLog2}
        end,
    TRead2 = 
        fun(X)->
                Res1 = transaction_api:read(KeyA, X),
                ct:pal("Res1: ~p~n", [Res1]),
                {{value, ValA}, Y} = Res1,
                Res2 = transaction_api:read(KeyB, Y),
                ct:pal("Res2: ~p~n", [Res2]),
                {{value, ValB}, TransLog2} = Res2,
                {{ok, ok}, TransLog2}
        end,

    {ResultW, TLogW} = transaction_api:do_transaction(TWrite2, SuccessFun, FailureFun),
    ct:pal("Write TLOG: ~p~n", [TLogW]),
    ?equals(ResultW, success),
    {ResultR, TLogR} = transaction_api:do_transaction(TRead2, SuccessFun, FailureFun),
    ct:pal("Read TLOG: ~p~n", [TLogR]),
    ?equals(ResultR, success),
    ok.

multi_write(_Config) ->
    Key = "MultiWrite",
    Value1 = "Value1",
    Value2 = "Value2",
    TFun = fun(TransLog)->
                   {ok, TransLog1} = transaction_api:write(Key, Value1, TransLog),
                   {ok, TransLog2} = transaction_api:write(Key, Value2, TransLog1),
		   {{ok, ok}, TransLog2}
           end,
    SuccessFun = fun(X) ->
                         {success, X}
                 end,
    FailureFun = fun(Reason)->
                         {failure, Reason}
                 end,

    {Result, _} = transaction_api:do_transaction(TFun, SuccessFun, FailureFun),
    ?equals(Result, success),
    ok.

tfuns(_Config) ->
    ok.
