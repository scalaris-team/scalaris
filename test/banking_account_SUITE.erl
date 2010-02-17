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
%%% File    : banking_account_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/transstore/*.erl
%%%
%%% Created :  18 Aug 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(banking_account_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-import(transaction_api, [read2/2, write2/3]).

-include("unittest.hrl").

all() ->
    [banking_account].

suite() ->
    [
     {timetrap, {seconds, 120}}
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

make_tfun(MyAccount, OtherAccount) ->
    fun (TransLog)->
	    {MyBalance, TransLog1}    = read2(TransLog,  MyAccount),
            %ct:pal("~p ~p~n", [MyAccount, MyBalance]),
	    {OtherBalance, TransLog2} = read2(TransLog1, OtherAccount),
            %ct:pal("~p ~p~n", [OtherAccount, OtherBalance]),
	    if
		OtherBalance > 500 ->
		    TransLog3 = write2(TransLog2, MyAccount,    MyBalance + 400),
		    TransLog4 = write2(TransLog3, OtherAccount, OtherBalance - 400),
		    {{ok, ok}, TransLog4};
		OtherBalance > 100 ->
		    TransLog3 = write2(TransLog2, MyAccount,    MyBalance + 100),
		    TransLog4 = write2(TransLog3, OtherAccount, OtherBalance - 100),
		    {{ok, ok}, TransLog4};
		true ->
		    {{ok, ok}, TransLog2}
	    end
    end.

process(Parent, MyAccount, OtherAccount, Count) ->
    SuccessFun = fun(X) ->
                         {success, X}
                 end,
    FailureFun = fun(Reason)->
                         {failure, Reason}
                 end,
    process_iter(Parent, make_tfun(MyAccount, OtherAccount), Count, SuccessFun, FailureFun, 0).

process_iter(Parent, _Key, 0, _SuccessFun, _FailureFun, AbortCount) ->
    Parent ! {done, AbortCount};
process_iter(Parent, TFun, Count, SuccessFun, FailureFun, AbortCount) ->
    case transaction_api:do_transaction(TFun, SuccessFun, FailureFun) of
	{success, {commit, _Y}} ->
	    process_iter(Parent, TFun, Count - 1, SuccessFun, FailureFun, AbortCount);
	{failure, abort} ->
	    process_iter(Parent, TFun, Count, SuccessFun, FailureFun, AbortCount + 1);
	X ->
	    ct:pal("do_transaction failed: ~p~n", [X])
    end.

banking_account(_Config) ->
    ?equals(transaction_api:single_write("a", 1000), commit),
    ?equals(transaction_api:single_write("b", 0), commit),
    ?equals(transaction_api:single_write("c", 0), commit),
    Self = self(),
    Count = 100,
    spawn(banking_account_SUITE, process, [Self, "a", "c", Count]),
    spawn(banking_account_SUITE, process, [Self, "b", "a", Count]),
    spawn(banking_account_SUITE, process, [Self, "c", "b", Count]),
    _Aborts = wait_for_done(3),
    {A, _} = transaction_api:quorum_read("a"),
    {B, _} = transaction_api:quorum_read("b"),
    {C, _} = transaction_api:quorum_read("c"),
    ct:pal("balance: ~p ~p ~p~n", [A, B, C]),
    ?equals(A + B + C, 1000),
    ok.

wait_for_done(0) ->
    [];
wait_for_done(Count) ->
    receive
	{done, Aborts} ->
	    [Aborts |wait_for_done(Count - 1)]
    end.
