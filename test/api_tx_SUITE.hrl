%% @copyright 2014 Zuse Institute Berlin

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
%% @author Thorsten Schuett <schuett@zib.de>
%% @author Nico Kruber <kruber@zib.de>

-dialyzer({no_fail_call, write_2/1}).

%% deliver list of all test cases defined in here:
proto_sched_ready_tests() ->
    [ new_tlog_0,
      req_list_2,
      read_2,
      write_3,
      commit_1,
      read_1,
      write_2,
      test_and_set_3,
      conflicting_tx,
      conflicting_tx2,
      write2_read2,
      multi_write,
      ops_on_not_found,
      random_write_read
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

new_tlog_0(_Config) ->
    ?proto_sched(start),
    ?equals(api_tx:new_tlog(), []),
    ?proto_sched(stop).
req_list_2(_Config) ->
    ?proto_sched(start),
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
                             %% allow test for existance of a key to be ok
                             _CommitRes = {ok}]}),
    %% read non-existing item and write to that item afterwards
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{read, "non-existing1"},
                                     {write, "non-existing1", "value"},
                                     {commit}]),
                    {_TLog, [_ReadRes = {fail, not_found},
                             _WriteRes = {ok},
                             _CommitRes = {ok}]}),
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
                    {_TLog, [{fail, abort, []}, {fail, abort, []}]}),

    %% try commit not as last operation in request list
    ?equals_pattern(api_tx:req_list(EmptyTLog, [{commit}, {read, "A"}]),
                    {_TLog, [{fail, abort, []}, _]}),

    %% try commit not as last operation in request list with longer list
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{commit}, {read, "A"}, {read, "B"}]),
                    {_TLog, [{fail, abort, []}, _, _]}),

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
                             %% allow test for existance of a key to be ok
                             _CommitRes = {ok}]}),
    ?proto_sched(stop),
    ok.

read_2(_Config) ->
    ?proto_sched(start),
    _ = api_tx:write("A", 7),
    %% read existing key
    ?equals_pattern(api_tx:read(api_tx:new_tlog(), "A"),
                    {_, {ok, 7}}),
    %% read non existing key
    ?equals_pattern(api_tx:read(api_tx:new_tlog(), "non-existing"),
                    {_, {fail, not_found}}),

    ?proto_sched(stop),
    ok.

write_3(_Config) ->
    ?proto_sched(start),
    %% write new key
    {_, {ok}} = api_tx:write(api_tx:new_tlog(), "write_3_newkey", 7),
    %% modify existing key
    {_, {ok}} = api_tx:write(api_tx:new_tlog(), "write_3_newkey", 8),
    %% write a key that is already in tlog
    {TLogA, _} = api_tx:read(api_tx:new_tlog(), "write_3_newkey"),
    {_, {ok}} = api_tx:write(TLogA, "write_3_newkey", 9),
    %% write key that does not exist and the read in tlog failed
    {TLogB, {fail, not_found}} =
        api_tx:read(api_tx:new_tlog(), "write_3_newkey2"),
    {_, {ok}} = api_tx:write(TLogB, "write_3_newkey2", 9),
    ?proto_sched(stop),
    ok.

commit_1(_Config) ->
    ?proto_sched(start),
    EmptyTLog = api_tx:new_tlog(),
    %% commit empty tlog
    ?equals(api_tx:commit(EmptyTLog), {ok}),

    %% commit a tlog
    {WriteTLog, _} = api_tx:write(api_tx:new_tlog(), "commit_1_A", 7),
    ?equals(api_tx:commit(WriteTLog), {ok}),

    _ = api_tx:write("commit_1_B", 7),
    {ReadTLog, _} = api_tx:read(api_tx:new_tlog(), "commit_1_B"),
    ?equals(api_tx:commit(ReadTLog), {ok}),

    %% commit a failed TLog
    TimeoutReadTLog =
        [ tx_tlog:set_entry_status(X, ?fail) || X <- ReadTLog ],
    ?equals(api_tx:commit(TimeoutReadTLog), {fail, abort, ["commit_1_B"]}),

    {WriteTLog2, _} = api_tx:write(api_tx:new_tlog(), "commit_1_C", 7),
    TimeoutWriteTLog =
        [ tx_tlog:set_entry_status(X, ?fail) || X <- WriteTLog2 ],
    ?equals(api_tx:commit(TimeoutWriteTLog), {fail, abort, ["commit_1_C"]}),

    %% commit a non-existing tlog
    {NonExistReadTLog, _} = api_tx:read(EmptyTLog, "non-existing"),
    %% allow test for existance of a key to be ok
    ?equals(api_tx:commit(NonExistReadTLog), {ok}),

    ?proto_sched(stop),
    ok.

read_1(_Config) ->
    ?proto_sched(start),
    ?equals(api_tx:read("non-existing"), {fail, not_found}),
    ?equals(api_tx:read("read_1_ReadKey"), {fail, not_found}),
    ?equals(api_tx:write("read_1_ReadKey", "IsSet"), {ok}),
    ?equals(api_tx:read("read_1_ReadKey"), {ok, "IsSet"}),
    ?proto_sched(stop),
    ok.

write_2(_Config) ->
    ?proto_sched(start),
    ?equals(api_tx:write("write_2_WriteKey", "Value"), {ok}),
    ?equals(api_tx:read("write_2_WriteKey"), {ok, "Value"}),
    ?equals(api_tx:write("write_2_WriteKey", "Value2"), {ok}),
    ?equals(api_tx:read("write_2_WriteKey"), {ok, "Value2"}),

    %% invalid key
    try ?RT:hash_key([a,b,c]) of
        _ -> ?equals(catch api_tx:write([a,b,c], "Value"), {ok})
    catch
        error:badarg ->
            ?equals_pattern(catch api_tx:write([a,b,c], "Value"), {'EXIT',{badarg, _}})
    end,
    ?proto_sched(stop),
    ok.

test_and_set_3(_Config) ->
    ?proto_sched(start),
    ?equals(api_tx:test_and_set("test_and_set_3", "Value", "NextValue"),
            {fail, not_found}),
    ?equals(api_tx:write("test_and_set_3", "Value"), {ok}),
    ?equals(api_tx:test_and_set("test_and_set_3", "Value", "NextValue"), {ok}),
    ?equals(api_tx:test_and_set("test_and_set_3", "wrong", "NewValue"),
            {fail, {key_changed, "NextValue"}}),
    ?proto_sched(stop),
    ok.

conflicting_tx(_Config) ->
    ?proto_sched(start),
    EmptyTLog = api_tx:new_tlog(),
    %% ops with other interleaving tx
    %% prepare an account
    _ = api_tx:write("Account A", 100),

    %% Tx1: read the balance and later try to modify it
    {Tx1TLog, {ok, Bal1}} = api_tx:read(EmptyTLog, "Account A"),

    %% Tx3: read the balance and later try to commit the read
    {Tx3TLog, {ok, _Bal3}} = api_tx:read(EmptyTLog, "Account A"),

    %% Tx2 reads the balance and increases it
    {Tx2TLog, {ok, Bal2}} = api_tx:read(EmptyTLog, "Account A"),
    ?equals_pattern(
       api_tx:req_list(Tx2TLog, [{write, "Account A", Bal2 + 100}, {commit}]),
       {_, [_WriteRes = {ok}, _CommitRes = {ok}]}),

    %% Tx1 tries to increases it atomically and fails
    ?equals_pattern(
       api_tx:req_list(Tx1TLog, [{write, "Account A", Bal1 + 100}, {commit}]),
       {_, [_WriteRes = {ok}, _CommitRes = {fail, abort, ["Account A"]}]}),
    io:format("DOne~n"),
    %% Tx3: try to commit the read and fail (value changed in the meantime)
    ?equals_pattern(api_tx:commit(Tx3TLog), {fail, abort, ["Account A"]}),

    %% check that two reading transactions can coexist
    %% Tx4: read the balance and later try to commit the read
    {Tx4TLog, {ok, _Bal4}} = api_tx:read(EmptyTLog, "Account A"),

    %% Tx5: read the balance and commit the read
    {Tx5TLog, {ok, _Bal5}} = api_tx:read(EmptyTLog, "Account A"),
    ?equals_pattern(api_tx:commit(Tx5TLog), {ok}),

    %% Tx4: try to commit a read and succeed (no updates in the meantime)
    ?equals_pattern(api_tx:commit(Tx4TLog), {ok}),
    ?proto_sched(stop),
    ok.

conflicting_tx2(_Config) ->
    ?proto_sched(start),
    %% read non-existing item
%%% log:log("1 doing read~n"),
    {TLog1a, [ReadRes1a]} =
        api_tx:req_list([{read, "conflicting_tx2_non-existing"}]),
    ?equals(ReadRes1a, {fail, not_found}),
%%% log:log("2 doing commit for read TLog~n"),
    ?equals(api_tx:commit(TLog1a), {ok}),

%%% log:log("3 doing single write for key (creating it)~n"),
    _ = api_tx:write("conflicting_tx2_non-existing", "Value"),
    %% verify not_found of tlog in commit phase? key now exists!
%%% log:log("4 doing commit for outdated 'not_found' TLog~n"),
    ?equals(api_tx:commit(TLog1a),
            {fail, abort, ["conflicting_tx2_non-existing"]}),

%%% log:log("5 doing write and commit on outdated 'not_found' TLog~n"),
    ?equals_pattern(api_tx:req_list(TLog1a,
                                    [{write, "conflicting_tx2_non-existing", "NewValue"},
                                     {commit}]),
                    {_TLog, [_WriteRes = {ok},
                             _CommitRes = {fail, abort, ["conflicting_tx2_non-existing"]}]}),
%%% log:log("6 reading same key again, should return initially written value~n"),
    ?equals(api_tx:read("conflicting_tx2_non-existing"), {ok, "Value"}),

    ?proto_sched(stop),
    ok.

write2_read2(_Config) ->
    ?proto_sched(start),
    KeyA = "KeyA",
    KeyB = "KeyB",
    ValueA = "Value1",
    ValueB = "Value2",

    log:log("Write KeyA"),
    {TLog1, _} = api_tx:write(api_tx:new_tlog(), KeyA, ValueA),
    log:log("Write KeyB"),
    {TLog2, _} = api_tx:write(TLog1, KeyB, ValueB),
    log:log("commit tlog ~p", [TLog2]),
    {ok} = api_tx:commit(TLog2),
    log:log("reading A and B"),

    ?equals_pattern(api_tx:req_list([{read, KeyA}, {read, KeyB}, {commit}]),
                    {_TLog4, [{ok, ValueA}, {ok, ValueB}, {ok}]}),
    ?proto_sched(stop),
    ok.

multi_write(_Config) ->
    ?proto_sched(start),
    Key = "MultiWrite",
    Value1 = "Value1",
    Value2 = "Value2",
    {TLog1, _} = api_tx:write(api_tx:new_tlog(), Key, Value1),
    {TLog2, _} = api_tx:write(TLog1, Key, Value2),
    ?equals(api_tx:commit(TLog2), {ok}),
    ?equals(api_tx:read(Key), {ok, Value2}),
    ?proto_sched(stop),
    ok.

-spec ops_on_not_found(Config::[tuple()]) -> ok.
ops_on_not_found(_Config) ->
    ?proto_sched(start),
    %% perform operations on non existing key (TLog of that) and
    %% check for the return values
    %% also documents expected behaviour a bit
    {NotFoundTLog,_} = api_tx:read(api_tx:new_tlog(), "a"),

    [ ?equals_w_note(api_tx:req_list(NotFoundTLog, [Req]),
                     {ExpectedTLog, ExpectedRes},
                     {'NotFoundTLog: ', NotFoundTLog, ' Req: ', Req})
      || {Req, {ExpectedTLog, ExpectedRes}} <-
             [{ {read, "a"},
                % {[{76,"a",-1,84,0,78,78}],[{fail,not_found}]} }
                {[{?read,"a",-1,?ok,0,?value_dropped,?value_dropped}],
                 [{fail,not_found}]}
              },
              { {read, "a", random_from_list},
                {[{?read,"a",-1,?fail,0,?value_dropped,?value_dropped}],
                 [{fail,not_found}]}
              },
              { {read, "a", {sublist, 1, 2}},
                {[{?read,"a",-1,?fail,0,?value_dropped,?value_dropped}],
                 [{fail,not_found}]}
              },
              { {write, "a", 7},
                %% {77,"a",-1,84,0,75,7}
                {[{?write,"a",-1,?ok,0,?value,7}],
                 [{ok}]}
              },
              { {add_del_on_list, "a", [7], [8]},
                %% {77,\"a\",-1,84,0,75,<<131,107,0,1,7>>}]
                {[{?write,"a",-1,?ok,0,?value,term_to_binary([7])}],
                 [{ok}]}
              },
              { {add_del_on_list, "a", 7, 8},
                {[{?read,"a",-1,?fail,0,?value_dropped,?value_dropped}],
                 [{fail, not_a_list}]}
              },
              { {add_on_nr, "a", 7},
                {[{?write,"a",-1,?ok,0,?value,7}],
                 [{ok}]}
              },
              { {add_on_nr, "a", [7]},
                {[{?read,"a",-1,?fail,0,?value_dropped,?value_dropped}],
                 [{fail, not_a_number}]}
              },
              { {test_and_set, "a", 0, 7},
                {[{?read,"a",-1,?fail,0,?value_dropped,?value_dropped}],
                 [{fail,not_found}]}
              }
             ]
    ],
    ?proto_sched(stop),
    ok.

random_write_read2(0) -> ok;
random_write_read2(Count) ->
    ?proto_sched(start),
    Key = io_lib:format("~p", [Count]),
    ?equals_w_note(api_tx:write(Key, Count), {ok}, Key),
    ?equals_w_note(api_tx:read(Key), {ok, Count}, Key),
    ?proto_sched(stop),
    random_write_read2(Count -1).

random_write_read(_) ->
    random_write_read2(adapt_tx_runs(10000)).
