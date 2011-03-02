%% @copyright 2011 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @version $Id: api_tx.erl 1457 2011-02-28 15:08:00Z schintke $
-module(api_tx_SUITE).
-author('schintke@zib.de').
-vsn('$Id: api_tx.erl 1457 2011-02-28 15:08:00Z schintke $').
-compile(export_all).
-include("unittest.hrl").

all()   -> [new_tlog_0,
            req_list_2,
            read_2,
            write_3,
            commit_1,
            read_1,
            write_2,
            test_and_set_3,
            conflicting_tx
           ].
suite() -> [ {timetrap, {seconds, 40}} ].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    Config2.

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

new_tlog_0(_Config) ->
    ?equals(api_tx:new_tlog(), []),
    ok.

req_list_2(_Config) ->
    EmptyTLog = api_tx:new_tlog(),

    %% execute empty request list
    ?equals(api_tx:req_list(EmptyTLog, []), {[], []}),

    %% write new item
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{write, "req_list_2_B", 7}, {commit}]),
                    {_TLog, [_WriteRes = {ok}, _CommitRes = {ok}]}),
    %% read existing item
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{read, "req_list_2_B"}, {commit}]),
                    {_TLog, [_ReadRes = {ok, _ReadVal=7}, _CommitRes = {ok}]}),
    %% read non-existing item
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{read, "non-existing"}, {commit}]),
                    {_TLog, [_ReadRes = {fail, not_found},
                             _CommitRes = {ok}]}), %% or {fail, abort}?
    %% exec more complex transaction with repeated requests
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{read, "B"}, {read, "B"},
                                     {write, "A", 8}, {read, "A"}, {read, "A"},
                                     {read, "A"}, {write, "B", 9},
                                     {commit}]),
                    {_TLog, [{fail,not_found}, {fail,not_found},
                             {ok}, {ok, 8}, {ok, 8},
                             {ok, 8}, {ok},
                             {ok}]}),

    %% exec empty commit
    ?equals_pattern(api_tx:req_list(EmptyTLog, [{commit}]),
                    {_TLog, [{ok}]}),

    %% exec empty double commit
    ?equals_pattern(api_tx:req_list(EmptyTLog, [{commit}, {commit}]),
                    {_TLog, [{fail, abort}, {fail, abort}]}),

    %% try commit not as last operation in request list
    ?equals_pattern(api_tx:req_list(EmptyTLog, [{commit}, {read, "A"}]),
                    {_TLog, [{fail, abort}, {ok, 8}]}),

    %% try commit not as last operation in request list with longer list
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{commit}, {read, "A"}, {read, "B"}]),
                    {_TLog, [{fail, abort}, {ok, 8}, {ok,9}]}),

    %% ops based on tlog
    {NonExistReadTLog, _Res1} = api_tx:read(EmptyTLog, "req_list_2_C"),
    %% write new item which is already in tlog
    ?equals_pattern(api_tx:req_list(NonExistReadTLog,
                                    [{write, "req_list_2_C", 42}, {commit}]),
                    {_TLog, [_WriteRes = {ok}, _CommitRes = {ok}]}),
    %% read existing item which is already in tlog
    {ExistReadTLog, _Res2} = api_tx:read(EmptyTLog, "req_list_2_C"),
    ?equals_pattern(api_tx:req_list(ExistReadTLog,
                                    [{read, "req_list_2_C"}, {commit}]),
                    {_TLog, [_ReadRes = {ok, _ReadVal=42}, _CommitRes = {ok}]}),
    %% read non-existing item
    {NonExistReadTLog2, _Res3} = api_tx:read(EmptyTLog, "non-existing"),
    ?equals_pattern(api_tx:req_list(NonExistReadTLog2,
                                    [{read, "non-existing"}, {commit}]),
                    {_TLog, [_ReadRes = {fail, not_found},
                             _CommitRes = {ok}]}), %% or {fail, abort}?
    ok.

read_2(_Config) ->
    _ = api_tx:write("A", 7),
    %% read existing key
    ?equals_pattern(api_tx:read(api_tx:new_tlog(), "A"),
                    {_, {ok, 7}}),
    %% read non existing key
    ?equals_pattern(api_tx:read(api_tx:new_tlog(), "non-existing"),
                    {_, {fail, not_found}}),
    ok.

write_3(_Config) ->
    %% write new key
    ?equals_pattern(api_tx:write(api_tx:new_tlog(), "write_3_newkey", 7),
                    {_, {ok}}),
    %% modify existing key
    ?equals_pattern(api_tx:write(api_tx:new_tlog(), "write_3_newkey", 8),
                    {_, {ok}}),
    %% write a key that is already in tlog
    {TLogA, _} = api_tx:read(api_tx:new_tlog(), "write_3_newkey"),
    ?equals_pattern(api_tx:write(TLogA, "write_3_newkey", 9), {_, {ok}}),
    %% write key that does not exist and the read in tlog failed
    {TLogB, {fail, not_found}} =
        api_tx:read(api_tx:new_tlog(), "write_3_newkey2"),
    ?equals_pattern(api_tx:write(TLogB, "write_3_newkey2", 9), {_, {ok}}),
    ok.

commit_1(_Config) ->
    EmptyTLog = api_tx:new_tlog(),
    %% commit empty tlog
    ?equals(api_tx:commit(EmptyTLog), {ok}),

    %% commit a tlog
    {TLog, _} = api_tx:write(api_tx:new_tlog(), "commit_1_A", 7),
    ?equals(api_tx:commit(TLog), {ok}),
    ok.

read_1(_Config) ->
    ?equals(api_tx:read("non-existing"), {fail, not_found}),
    ok.

write_2(_Config) ->
    ?equals(api_tx:write("WriteKey", "Value"), {ok}),

    %% invalid key
    ?equals_pattern(catch api_tx:write([a,b,c], "Value"), {'EXIT',{badarg,_}}),
    ok.

test_and_set_3(_Config) ->
    ?equals(api_tx:write("test_and_set_3", "Value"), {ok}),

    ?equals(api_tx:test_and_set("test_and_set_3", "Value", "NextValue"), {ok}),

    ?equals(api_tx:test_and_set("test_and_set_3", "wrong", "NewValue"),
            {fail, {key_changed, "NextValue"}}),

    ok.

conflicting_tx(_Config) ->
    EmptyTLog = api_tx:new_tlog(),
    %% ops with other interleaving tx
    %% prepare an account
    api_tx:write("Account A", 100),

    ct:pal("INIT done~n"),
    %% Tx1 reads the balance and then sleeps a bit
    {Tx1TLog, {ok, Bal1}} = api_tx:read(EmptyTLog, "Account A"),

    ct:pal("tx2 start~n"),
    %% Tx2 reads the balance and increases it
    {Tx2TLog, {ok, Bal2}} = api_tx:read(EmptyTLog, "Account A"),
    ?equals_pattern(
       api_tx:req_list(Tx2TLog, [{write, "Account A", Bal2 + 100}, {commit}]),
       {_, [_WriteRes = {ok}, _CommitRes = {ok}]}),
    ct:pal("tx2 done~n"),

    %% Tx1 tries to increases it atomically and fails
    ?equals_pattern(
       api_tx:req_list(Tx1TLog, [{write, "Account A", Bal1 + 100}, {commit}]),
       {_, [_WriteRes = {ok}, _CommitRes = {fail, abort}]}),

    ok.
