%  @copyright 2008-2014 Zuse Institute Berlin
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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Unit tests for src/api_tx
%% @end
%% @version $Id$
-module(api_tx_concurrent_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [increment_test_2, increment_test_4, increment_test_8].

suite() -> [ {timetrap, {seconds, 620}} ].

init_per_suite(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    Config.

end_per_suite(_Config) ->
    ok.

inc(Key) ->
    {TLog1, [ReadResult]} = api_tx:req_list([{read, Key}]),
    case ReadResult of
        {ok, Value} ->
            {_TLog, [{ok}, CommitResult]} =
                api_tx:req_list(TLog1, [{write, Key, Value + 1}, {commit}]),
            CommitResult;
        Fail -> Fail
    end.

process(Parent, Key, Count) ->
    process_iter(Parent, Key, Count, 0).

process_iter(Parent, _Key, 0, AbortCount) ->
    Parent ! {done, AbortCount};
process_iter(Parent, Key, Count, AbortCount) ->
    Result = inc(Key),
    case Result of
        {ok}             -> process_iter(Parent, Key, Count - 1, AbortCount);
        {fail, abort, _} -> process_iter(Parent, Key, Count, AbortCount + 1)
    end.

increment_test_8(_Config) -> increment_test_n(_Config, 8).
increment_test_4(_Config) -> increment_test_n(_Config, 4).
increment_test_2(_Config) -> increment_test_n(_Config, 2).

increment_test_n(_Config, N) ->
    Key = "i",
    ?equals(api_tx:write("i", 0), {ok}),

    Self = self(),
    Count = 200 div N,
    _ = [ spawn(api_tx_concurrent_SUITE, process, [Self, Key, Count])
          || _ <- lists:seq(1, N) ],

    Aborts = wait_for_done(N),
    ct:pal("aborts: ~w~n", [Aborts]),
    {ok, Total} = api_tx:read(Key),
    ?equals(N*Count, Total),
    ok.

wait_for_done(0) ->
    [];
wait_for_done(Count) ->
    receive
        {done, Aborts} ->
            [Aborts |wait_for_done(Count - 1)]
    end.
