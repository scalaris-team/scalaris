%  Copyright 2008-2011 Zuse Institute Berlin
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
-module(concurrent_transaction_v2_SUITE).

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
    Config2 = unittest_helper:init_per_suite(Config),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    Config2.

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

inc(Key) ->
    {TLog1, [{read, Key, ReadResult}]} =
        cs_api_v2:process_request_list([], [{read, Key}]),
    case ReadResult of
        {value, Value} ->
            {_TLog, [{write, Key, {value, _Written}}, CommitResult]} =
                cs_api_v2:process_request_list(
                  TLog1, [{write, Key, Value + 1}, {commit}]),
            case CommitResult of
                commit -> ok;
                Reason -> {failure, Reason}
            end;
        {fail, Reason} ->
            {failure, Reason}
    end.

process(Parent, Key, Count) ->
    SuccessFun = fun(X) ->
                         {success, X}
                 end,
    FailureFun = fun(Reason)->
                         {failure, Reason}
                 end,
    process_iter(Parent, Key, Count, SuccessFun, FailureFun, 0).

process_iter(Parent, _Key, 0, _SuccessFun, _FailureFun, AbortCount) ->
    Parent ! {done, AbortCount};
process_iter(Parent, Key, Count, SuccessFun, FailureFun, AbortCount) ->
    Result = inc(Key),
    case Result of
	ok ->
	    process_iter(Parent, Key, Count - 1, SuccessFun, FailureFun, AbortCount);
	{failure, abort} ->
	    process_iter(Parent, Key, Count, SuccessFun, FailureFun, AbortCount + 1);
	{failure, timeout} ->
	    process_iter(Parent, Key, Count, SuccessFun, FailureFun, AbortCount + 1);
	{failure, failed} ->
	    process_iter(Parent, Key, Count, SuccessFun, FailureFun, AbortCount + 1)
    end.

increment_test_8(_Config) ->
    % init: i = 0
    Key = "i",
    ?equals(cs_api_v2:write("i", 0), ok),

    Self = self(),
    Count = 100,
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),

    Aborts = wait_for_done(8),
    ct:pal("aborts: ~w~n", [Aborts]),
    Total = cs_api_v2:read(Key),
    ?equals(8*Count, Total),
    ok.

increment_test_4(_Config) ->
    % init: i = 0
    Key = "i",
    ?equals(cs_api_v2:write("i", 0), ok),

    Self = self(),
    Count = 100,
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),

    Aborts = wait_for_done(4),
    ct:pal("aborts: ~w~n", [Aborts]),
    Total = cs_api_v2:read(Key),
    ?equals(4 * Count, Total),
    ok.

increment_test_2(_Config) ->
    % init: i = 0
    Key = "i",
    ?equals(cs_api_v2:write("i", 0), ok),

    Self = self(),
    Count = 100,
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),
    spawn(concurrent_transaction_v2_SUITE, process, [Self, Key, Count]),

    Aborts = wait_for_done(2),
    ct:pal("aborts: ~w~n", [Aborts]),
    Total = cs_api_v2:read(Key),
    ?equals(2* Count, Total),
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
