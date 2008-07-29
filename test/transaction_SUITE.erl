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
    [read, write, write_read, tfuns, multi_write].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    Pid = spawn(fun () ->
			ct:pal(ct, "registered names ~p", [erlang:registered()]),
			process_dictionary:start_link_for_unittest(), 
			boot_sup:start_link(), 
			timer:sleep(25000) 
		end),
    timer:sleep(15000),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    exit(Pid, kill),
    ok.

read(_Config) ->
    ?equals(transstore.transaction_api:quorum_read("UnknownKey"), {fail, not_found}),
    ok.

write(_Config) ->
    ?equals(transstore.transaction_api:single_write("WriteKey", "Value"), commit),
    ok.

write_read(_Config) ->
    ?equals(transstore.transaction_api:single_write("Key", "Value"), commit),
    ?equals(transstore.transaction_api:quorum_read("Key"), {"Value", 0}),
    ok.

multi_write(_Config) ->
    Key = "MultiWrite",
    Value1 = "Value1",
    Value2 = "Value2",
    TFun = fun(TransLog)->
                   {ok, TransLog1} = transstore.transaction_api:write(Key, Value1, TransLog),
                   {ok, TransLog2} = transstore.transaction_api:write(Key, Value2, TransLog1),
		   {{ok, ok}, TransLog2}
           end,
    SuccessFun = fun(X) ->
                         {success, X}
                 end,
    FailureFun = fun(Reason)->
                         {failure, Reason}
                 end,

    {Result, _} = transstore.transaction_api:do_transaction(TFun, SuccessFun, FailureFun),
    ?equals(Result, success),
    ok.


tfuns(_Config) ->
    ok.
