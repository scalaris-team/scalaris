% Copyright 2008-2011 Zuse Institute Berlin
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
%%% File    : bench_increment.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : i++ benchmark
%%%
%%% Created :  25 Aug 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(bench_increment).

-author('schuett@zib.de').
-vsn('$Id$').

-export([bench/0, bench_raw/0, process/3]).

-spec inc(Key::string()) -> api_tx:commit_result() | api_tx:read_result().
inc(Key) ->
    {TLog1, [ReadResult]} =
        api_tx:req_list([{read, Key}]),
    case ReadResult of
        {ok, Value} ->
            {_TLog, [{ok}, CommitResult]} =
                api_tx:req_list(TLog1, [{write, Key, Value + 1}, {commit}]),
            CommitResult;
        Fail -> Fail
    end.

-spec process(Parent::comm:erl_local_pid(), Key::string(), Count::non_neg_integer()) -> ok.
process(Parent, Key, Count) ->
    process_iter(Parent, Key, Count, 0).

-spec process_iter(Parent::comm:erl_local_pid(), Key::string(), Count::non_neg_integer(),
                   AbortCount::non_neg_integer()) -> ok.
process_iter(Parent, _Key, 0, AbortCount) ->
    comm:send_local(Parent , {done, AbortCount});
process_iter(Parent, Key, Count, AbortCount) ->
    Result = inc(Key),
    case Result of
        {ok}              -> process_iter(Parent, Key, Count - 1, AbortCount);
        {fail, abort}     -> process_iter(Parent, Key, Count, AbortCount + 1);
        {fail, timeout}   -> process_iter(Parent, Key, Count, AbortCount + 1);
        {fail, not_found} -> process_iter(Parent, Key, Count, AbortCount + 1);
        X -> log:log(warn, "~p", [X])
    end.

-spec bench() -> pos_integer().
bench() ->
    bench_raw().

-spec bench_raw() -> pos_integer().
bench_raw() ->
    Self = self(),
    Count = 1000,
    Key = "i",
    spawn(fun () -> process(Self, Key, Count) end),
    spawn(fun () -> process(Self, Key, Count) end),
    spawn(fun () -> process(Self, Key, Count) end),
    spawn(fun () -> process(Self, Key, Count) end),
    spawn(fun () -> process(Self, Key, Count) end),
    spawn(fun () -> process(Self, Key, Count) end),
    _ = wait_for_done(6),
    Count.

-spec bench_cprof() -> pos_integer().
bench_cprof() ->
    Self = self(),
    Count = 300,
    Key = "i",
    cprof:start(),
    spawn(fun () -> process(Self, Key, Count) end),
    _ = wait_for_done(1),
    cprof:pause(),
    io:format("~p~n", [cprof:analyse()]),
    Count.

-spec bench_fprof() -> pos_integer().
bench_fprof() ->
    Count = fprof:apply(bench_increment, bench_raw, [], [{procs, pid_groups:processes()}]),
    fprof:profile(),
    %fprof:analyse(),
    fprof:analyse([{cols, 140}, details, callers, totals, {dest, []}]), % , totals, no_details, no_callers, {sort, acc},
    Count.

-spec increment_test() -> ok.
increment_test() ->
    % init: i = 0
    Key = "i",
    {ok} = api_tx:write(Key, 0),

    {Time, Value} = util:tc(bench_increment, bench, []),
    io:format("executed ~p transactions in ~p us: ~p~n", [Value, Time, Value / Time * 1000000]),
    %error_logger:tty(false),
    ok.

-spec wait_for_done(non_neg_integer()) -> [integer()].
wait_for_done(0) ->
    [];
wait_for_done(Count) ->
    receive
        {done, Aborts} ->
            io:format("aborts: ~p~n", [Aborts]),
            [Aborts | wait_for_done(Count - 1)]
    end.
