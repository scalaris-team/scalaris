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
%%% File    : concurrent_transaction_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/transstore/*.erl
%%%
%%% Created :  18 Aug 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(concurrent_transaction_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [increment_test_2, increment_test_4, increment_test_8].

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

make_tfun(Key) ->
    fun (TransLog)->
	    {Result, TransLog1} = transstore.transaction_api:read(Key, TransLog),
	    {Result2, TransLog2} =
		if
		    Result == fail ->
			Value = 0,
			transstore.transaction_api:write(Key, Value, TransLog);
		    true ->
			{value, Val} = Result,
			Value = Val + 1,
			transstore.transaction_api:write(Key, Value, TransLog1)
		end,
	    if
		Result2 == ok ->
		    {{ok, Value}, TransLog2};
		true ->
		    {{fail, abort}, TransLog2}
	    end
    end.

process(Parent, Key, Count) ->
    SuccessFun = fun(X) ->
                         {success, X}
                 end,
    FailureFun = fun(Reason)->
                         {failure, Reason}
                 end,
    process_iter(Parent, make_tfun(Key), Count, SuccessFun, FailureFun, 0).

process_iter(Parent, _Key, 0, _SuccessFun, _FailureFun, AbortCount) ->
    Parent ! {done, AbortCount};
process_iter(Parent, TFun, Count, SuccessFun, FailureFun, AbortCount) ->
    case transstore.transaction_api:do_transaction(TFun, SuccessFun, FailureFun) of
	{success, {commit, Y}} ->
	    process_iter(Parent, TFun, Count - 1, SuccessFun, FailureFun, AbortCount);
	{failure, abort} ->
	    process_iter(Parent, TFun, Count, SuccessFun, FailureFun, AbortCount + 1);
	X ->
	    ct:pal("error in process_iter: ~p~n", [X])
    end.
    

increment_test_8(_Config) ->
    % init: i = 0
    Key = "i",
    ?equals(transstore.transaction_api:single_write("i", 0), commit),

    Self = self(),
    Count = 100,
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),

    Aborts = wait_for_done(8),
    ct:pal("aborts: ~p~n", [Aborts]),
    Foo = transstore.transaction_api:quorum_read(Key),
    {Total, _} = Foo,
    ?equals(Total, 8 * Count),
    ok.

increment_test_4(_Config) ->
    % init: i = 0
    Key = "i",
    ?equals(transstore.transaction_api:single_write("i", 0), commit),

    Self = self(),
    Count = 100,
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),

    Aborts = wait_for_done(4),
    ct:pal("aborts: ~p~n", [Aborts]),
    Foo = transstore.transaction_api:quorum_read(Key),
    {Total, _} = Foo,
    ?equals(Total, 4 * Count),
    ok.

increment_test_2(_Config) ->
    % init: i = 0
    Key = "i",
    ?equals(transstore.transaction_api:single_write("i", 0), commit),

    Self = self(),
    Count = 100,
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_SUITE, process, [Self, Key, Count]),

    Aborts = wait_for_done(2),
    ct:pal("aborts: ~p~n", [Aborts]),
    Foo = transstore.transaction_api:quorum_read(Key),
    {Total, _} = Foo,
    ?equals(Total, 2 * Count),
    ok.

wait_for_done(0) ->
    [];
wait_for_done(Count) ->
    receive
	{done, Aborts} ->
	    [Aborts |wait_for_done(Count - 1)]
    end.

tfuns(_Config) ->
    ok.
