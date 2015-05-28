%  @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc Unit tests testing banking transactions
%% @end
%% @version $Id$
-module(banking_account_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').
-compile(export_all).

-include("unittest.hrl").

all()   -> [banking_account].
suite() -> [ {timetrap, {seconds, 120}} ].

init_per_suite(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    Config.

end_per_suite(_Config) ->
    ok.

money_transfer(A, B) ->
    TLog0 = api_tx:new_tlog(),
    Req1 = [{read, A},
            {read, B}],
    {TLog1, [{ok, ABalance}, {ok, BBalance}]} = api_tx:req_list(TLog0, Req1),
    AtoB = case ABalance > BBalance of
               true  ->  1;
               false -> -1
           end,
    Req2 = [{write, A, ABalance - AtoB},
            {write, B, BBalance + AtoB},
            {commit}],
    {_TLog2, [_, _, ResCommit]} = api_tx:req_list(TLog1, Req2),
    ResCommit.

process(Parent, A, B, Count) ->
    process_iter(Parent, A, B, Count, 0).

process_iter(Parent, _A, _B, 0, AbortCount) ->
    Parent ! {done, AbortCount};
process_iter(Parent, A, B, Count, AbortCount) ->
    case money_transfer(A, B) of
        {ok} -> process_iter(Parent, A, B, Count - 1, AbortCount);
        _ -> process_iter(Parent, A, B, Count, AbortCount + 1)
    end.

banking_account(_Config) ->
    Total = 1000,
    ?equals(api_tx:write("a", Total), {ok}),
    ?equals(api_tx:write("b", 0), {ok}),
    ?equals(api_tx:write("c", 0), {ok}),
    Self = self(),
    Count = 1000,
    spawn(banking_account_SUITE, process, [Self, "a", "b", Count]),
    spawn(banking_account_SUITE, process, [Self, "b", "c", Count]),
    spawn(banking_account_SUITE, process, [Self, "c", "a", Count]),
    Aborts = wait_for_done(3),
    ct:pal("Committed Tx: ~p~n", [3 * Count]),
    ct:pal("Aborted and restarted Tx due to concurrent conflicting Txs: ~p~n",
           [lists:sum(Aborts)]),
    {ok, A} = api_tx:read("a"),
    {ok, B} = api_tx:read("b"),
    {ok, C} = api_tx:read("c"),
    ct:pal("balance: ~p ~p ~p~n", [A, B, C]),
    ?equals(A + B + C, Total),
    ok.

wait_for_done(0) ->
    [];
wait_for_done(Count) ->
    receive
        {done, Aborts} -> [Aborts | wait_for_done(Count - 1)]
    end.
