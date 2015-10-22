%  @copyright 2008-2015 Zuse Institute Berlin

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
%% @doc    i++ benchmark
%% @end
%% @version $Id$
-module(bench_fun).

-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([increment/1, increment_with_histo/1, increment_with_key/2, quorum_read/1, read_read/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% public API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec increment(integer()) -> fun().
increment(Iterations) ->
    fun(Parent) ->
            Key = get_and_init_key(),
            {Diff, Aborts} = util:tc(fun increment_iter/3, [Key, Iterations, 0]),
            comm:send_local(Parent, {done, Diff, Aborts})
    end.

-spec increment_with_histo(integer()) -> fun().
increment_with_histo(Iterations) ->
    fun(Parent) ->
            Key = get_and_init_key(),
            {Diff, {Aborts, H2}} =
                util:tc(fun() ->
                                H = histogram:create(20),
                                increment_with_histo_iter(H, Key, Iterations, 0)
                        end),
            io:format("~p~n", [histogram:get_data(H2)]),
            comm:send_local(Parent, {done, Diff, Aborts})
    end.

-spec increment_with_key(integer(), client_key()) -> fun().
increment_with_key(Iterations, Key) ->
    fun(Parent) ->
            {Diff, Aborts} = util:tc(fun increment_iter/3, [Key, Iterations, 0]),
            comm:send_local(Parent, {done, Diff, Aborts})
    end.

-spec quorum_read(integer()) -> fun().
quorum_read(Iterations) ->
    fun (Parent) ->
            Key = get_and_init_key(),
            {Diff, Aborts} = util:tc(fun read_iter/3, [Key, Iterations, 0]),
            comm:send_local(Parent, {done, Diff, Aborts})
    end.

-spec read_read(integer()) -> fun().
read_read(Iterations) ->
    fun (Parent) ->
            Key1 = get_and_init_key(),
            Key2 = get_and_init_key(),
            {Diff, Aborts} = util:tc(fun read_read_iter/4, [Key1, Key2, Iterations, 0]),
            comm:send_local(Parent, {done, Diff, Aborts})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% increment
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

-spec increment_iter(string(), integer(), non_neg_integer()) -> non_neg_integer().
increment_iter(_Key, 0, Aborts) ->
    Aborts;
increment_iter(Key, Iterations, Aborts) ->
    Result = inc(Key),
    case Result of
        {ok}              -> increment_iter(Key, Iterations - 1, Aborts);
        {fail, abort, [Key]}     ->
            timer:sleep(randoms:rand_uniform(1, erlang:max(2, 10 * Aborts + 1))),
            increment_iter(Key, Iterations, Aborts + 1);
        {fail, not_found} ->
            timer:sleep(randoms:rand_uniform(1, erlang:max(2, 10 * Aborts + 1))),
            increment_iter(Key, Iterations, Aborts + 1);
        X ->
            log:log(warn, "bench_fun:increment_iter unexpected return: ~p", [X]),
            increment_iter(Key, Iterations, Aborts + 1)
    end.

-spec increment_with_histo_iter(histogram:histogram(), string(), integer(), non_neg_integer()) -> {non_neg_integer(), histogram:histogram()}.
increment_with_histo_iter(H, _Key, 0, Aborts) ->
    {Aborts, H};
increment_with_histo_iter(H, Key, Iterations, Aborts) ->
    Before = os:timestamp(),
    Result = inc(Key),
    After = os:timestamp(),
    case Result of
        {ok}              -> increment_with_histo_iter(
                               histogram:add(timer:now_diff(After, Before) / 1000, H),
                               Key, Iterations - 1, Aborts);
        {fail, abort, [Key]}     ->
            timer:sleep(randoms:rand_uniform(1, erlang:max(2, 10 * Aborts + 1))),
            increment_with_histo_iter(H, Key, Iterations, Aborts + 1);
        {fail, not_found} ->
            timer:sleep(randoms:rand_uniform(1, erlang:max(2, 10 * Aborts + 1))),
            increment_with_histo_iter(H, Key, Iterations, Aborts + 1);
        X ->
            log:log(warn, "bench_fun:increment_with_histo_iter unexpected return: ~p", [X]),
            increment_with_histo_iter(H, Key, Iterations, Aborts + 1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% quorum_read
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_iter(_Key, 0, Aborts) ->
    Aborts;
read_iter(Key, Iterations, Aborts) ->
    case api_tx:read(Key) of
        {ok, _Value}    -> read_iter(Key, Iterations - 1, Aborts);
        {fail, _Reason} -> read_iter(Key, Iterations, Aborts + 1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% read_read (reads two items in one transaction)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read_read_tx(Key1, Key2) ->
    {_, Result} = api_tx:req_list([{read, Key1}, {read, Key2}, {commit}]),
    Result.

read_read_iter(_Key1, _Key2, 0, Aborts) ->
    Aborts;
read_read_iter(Key1, Key2, Iterations, Aborts) ->
    case read_read_tx(Key1, Key2) of
        [{ok, 0},{ok,0},{ok}] -> read_read_iter(Key1, Key2, Iterations - 1, Aborts);
        _ -> read_read_iter(Key1, Key2, Iterations, Aborts + 1)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% initialize keys
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec retries() -> pos_integer().
retries() -> 10.

-spec get_and_init_key() -> string().
get_and_init_key() ->
    Key = randoms:getRandomString(),
    get_and_init_key(Key, retries(), _TriedKeys = 1).

-spec get_and_init_key(Key::string(), Retries::non_neg_integer(),
                       TriedKeys::pos_integer()) -> FinalKey::string().
get_and_init_key(_Key, 0, TriedKeys) ->
    NewKey = randoms:getRandomString(),
    io:format("geT_and_init_key choosing new key and retrying~n"),
    timer:sleep(randoms:rand_uniform(1, 10000 * TriedKeys)),
    get_and_init_key(NewKey, retries(), TriedKeys + 1);

get_and_init_key(Key, Count, TriedKeys) ->
    case api_tx:write(Key, 0) of
        {ok} ->
            Key;
        {fail, abort, [Key]} ->
            SleepTime = randoms:rand_uniform(1, TriedKeys * 2000 * 11 - Count),
            io:format("geT_and_init_key 1 failed, retrying in ~p ms~n",
                      [SleepTime]),
            timer:sleep(SleepTime),
            get_and_init_key(Key, Count - 1, TriedKeys)
    end.
