%% @copyright 2012 Zuse Institute Berlin

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
%% @version $Id$
-module(api_tx_SUITE).
-author('schintke@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [new_tlog_0,
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
            write_test_race_mult_rings,
            tester_encode_decode,
            random_write_read,
            tester_read_not_existing,
            tester_write_read_not_existing,
            tester_write_read,
            tester_add_del_on_list_not_existing,
            tester_add_del_on_list,
            tester_add_del_on_list_maybe_invalid,
            tester_add_on_nr_not_existing,
            tester_add_on_nr,
            tester_add_on_nr_maybe_invalid,
            tester_test_and_set_not_existing,
            tester_test_and_set,
            tester_tlog_add_del_on_list_not_existing,
            tester_tlog_add_del_on_list,
            tester_tlog_add_del_on_list_maybe_invalid,
            tester_tlog_add_on_nr_not_existing,
            tester_tlog_add_on_nr,
            tester_tlog_add_on_nr_maybe_invalid
           ].
suite() -> [ {timetrap, {seconds, 200}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        write_test_race_mult_rings -> %% this case creates its own ring
            Config;
        tester_encode_decode -> %% this case does not need a ring
            Config;
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

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
                    {_TLog, [{fail, abort}, {fail, abort}]}),

    %% try commit not as last operation in request list
    ?equals_pattern(api_tx:req_list(EmptyTLog, [{commit}, {read, "A"}]),
                    {_TLog, [{fail, abort}, _]}),

    %% try commit not as last operation in request list with longer list
    ?equals_pattern(api_tx:req_list(EmptyTLog,
                                    [{commit}, {read, "A"}, {read, "B"}]),
                    {_TLog, [{fail, abort}, _, _]}),

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
    {WriteTLog, _} = api_tx:write(api_tx:new_tlog(), "commit_1_A", 7),
    ?equals(api_tx:commit(WriteTLog), {ok}),

    _ = api_tx:write("commit_1_B", 7),
    {ReadTLog, _} = api_tx:read(api_tx:new_tlog(), "commit_1_B"),
    ?equals(api_tx:commit(ReadTLog), {ok}),

    %% commit a timedout TLog
    TimeoutReadTLog =
        [ tx_tlog:set_entry_status(X, {fail, timeout}) || X <- ReadTLog ],
    ?equals(api_tx:commit(TimeoutReadTLog), {fail, abort}),

    {WriteTLog2, _} = api_tx:write(api_tx:new_tlog(), "commit_1_C", 7),
    TimeoutWriteTLog =
        [ tx_tlog:set_entry_status(X, {fail, timeout}) || X <- WriteTLog2 ],
    ?equals(api_tx:commit(TimeoutWriteTLog), {fail, abort}),

    %% commit a non-existing tlog
    {NonExistReadTLog, _} = api_tx:read(EmptyTLog, "non-existing"),
    %% allow test for existance of a key to be ok
    ?equals(api_tx:commit(NonExistReadTLog), {ok}),

    ok.

read_1(_Config) ->
    ?equals(api_tx:read("non-existing"), {fail, not_found}),
    ?equals(api_tx:read("read_1_ReadKey"), {fail, not_found}),
    ?equals(api_tx:write("read_1_ReadKey", "IsSet"), {ok}),
    ?equals(api_tx:read("read_1_ReadKey"), {ok, "IsSet"}),
    ok.

write_2(_Config) ->
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
    ok.

test_and_set_3(_Config) ->
    ?equals(api_tx:test_and_set("test_and_set_3", "Value", "NextValue"),
            {fail, not_found}),
    ?equals(api_tx:write("test_and_set_3", "Value"), {ok}),
    ?equals(api_tx:test_and_set("test_and_set_3", "Value", "NextValue"), {ok}),
    ?equals(api_tx:test_and_set("test_and_set_3", "wrong", "NewValue"),
            {fail, {key_changed, "NextValue"}}),
    ok.

conflicting_tx(_Config) ->
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
       {_, [_WriteRes = {ok}, _CommitRes = {fail, abort}]}),

    %% Tx3: try to commit the read and fail (value changed in the meantime)
    ?equals_pattern(api_tx:commit(Tx3TLog), {fail, abort}),

    %% check that two reading transactions can coexist
    %% Tx4: read the balance and later try to commit the read
    {Tx4TLog, {ok, _Bal4}} = api_tx:read(EmptyTLog, "Account A"),

    %% Tx5: read the balance and commit the read
    {Tx5TLog, {ok, _Bal5}} = api_tx:read(EmptyTLog, "Account A"),
    ?equals_pattern(api_tx:commit(Tx5TLog), {ok}),

    %% Tx4: try to commit a read and succeed (no updates in the meantime)
    ?equals_pattern(api_tx:commit(Tx4TLog), {ok}),
    ok.

conflicting_tx2(_Config) ->
    %% read non-existing item
    {TLog1a, [ReadRes1a]} =
        api_tx:req_list([{read, "conflicting_tx2_non-existing"}]),
    ?equals(ReadRes1a, {fail, not_found}),
    ?equals(api_tx:commit(TLog1a), {ok}),

    _ = api_tx:write("conflicting_tx2_non-existing", "Value"),
    %% verify not_found of tlog in commit phase? key now exists!
    ?equals(api_tx:commit(TLog1a), {fail, abort}),

    ?equals_pattern(api_tx:req_list(TLog1a,
                                    [{write, "conflicting_tx2_non-existing", "NewValue"},
                                     {commit}]),
                    {_TLog, [_WriteRes = {ok},
                             _CommitRes = {fail, abort}]}),
    ?equals(api_tx:read("conflicting_tx2_non-existing"), {ok, "Value"}),


    ok.

write2_read2(_Config) ->
    KeyA = "KeyA",
    KeyB = "KeyB",
    ValueA = "Value1",
    ValueB = "Value2",

    {TLog1, _} = api_tx:write(api_tx:new_tlog(), KeyA, ValueA),
    {TLog2, _} = api_tx:write(TLog1, KeyB, ValueB),
    {ok} = api_tx:commit(TLog2),

    ?equals_pattern(api_tx:req_list([{read, KeyA}, {read, KeyB}, {commit}]),
                    {_TLog4, [{ok, ValueA}, {ok, ValueB}, {ok}]}),
    ok.

multi_write(_Config) ->
    Key = "MultiWrite",
    Value1 = "Value1",
    Value2 = "Value2",
    {TLog1, _} = api_tx:write(api_tx:new_tlog(), Key, Value1),
    {TLog2, _} = api_tx:write(TLog1, Key, Value2),
    ?equals(api_tx:commit(TLog2), {ok}),
    ?equals(api_tx:read(Key), {ok, Value2}),
    ok.


%% @doc Test for api_tx:write taking at least 2s after stopping a ring
%%      and starting a new one.
write_test_race_mult_rings(Config) ->
    % first ring:
    write_test(Config),
    % second ring and more:
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config),
    write_test(Config).

-spec write_test(Config::[tuple()]) -> ok.
write_test(Config) ->
    OldRegistered = erlang:registered(),
    OldProcesses = unittest_helper:get_processes(),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}, {monitor_perf_interval, 0}]}]),
    Self = self(),
    BenchPid1 = erlang:spawn(fun() ->
                                     {Time, _} = util:tc(api_tx, write, ["1", 1]),
                                     comm:send_local(Self, {time, Time}),
                                     ct:pal("~.0pus~n", [Time])
                             end),
    receive {time, FirstWriteTime} -> ok
    end,
    util:wait_for_process_to_die(BenchPid1),
    BenchPid2 = erlang:spawn(fun() ->
                                     {Time, _} = util:tc(api_tx, write, ["2", 2]),
                                     comm:send_local(Self, {time, Time}),
                                     ct:pal("~.0pus~n", [Time])
                             end),
    receive {time, SecondWriteTime} -> ok
    end,
    util:wait_for_process_to_die(BenchPid2),
    unittest_helper:check_ring_load(4  * 2),
    unittest_helper:check_ring_data(),
    unittest_helper:stop_ring(),
%%     randoms:stop(), %doesn't matter
    _ = inets:stop(),
    unittest_helper:kill_new_processes(OldProcesses),
    {_, _, OnlyNewReg} =
        util:split_unique(OldRegistered, erlang:registered()),
    ct:pal("NewReg: ~.0p~n", [OnlyNewReg]),
    ?equals_pattern(FirstWriteTime, X when X =< 1000000),
    ?equals_pattern(SecondWriteTime, X when X =< 1000000).

-spec prop_encode_decode(Value::client_value()) -> boolean().
prop_encode_decode(Value) ->
    Value =:= rdht_tx:decode_value(rdht_tx:encode_value(Value)).

tester_encode_decode(_Config) ->
    tester:test(?MODULE, prop_encode_decode, 1, 10000).

random_write_read2(0) -> ok;
random_write_read2(Count) ->
    Key = lists:flatten(io_lib:format("~p", [Count])),
    ?equals_w_note(api_tx:write(Key, Count), {ok}, Key),
    ?equals_w_note(api_tx:read(Key), {ok, Count}, Key),
    random_write_read2(Count -1).

random_write_read(_) ->
    random_write_read2(10000).

-spec prop_read_not_existing(Key::client_key()) -> boolean().
prop_read_not_existing(Key) ->
    case api_tx:read(Key) of
        {fail, not_found} -> true;
        {ok, _Value} -> true; % may happen as we do not clear the ring after every op
        _ -> false
    end.

tester_read_not_existing(_Config) ->
    tester:test(?MODULE, prop_read_not_existing, 1, 10000).

-spec prop_write_read_not_existing(Key::client_key(), Value::client_value()) -> true | no_return().
prop_write_read_not_existing(Key, Value) ->
    ?equals(api_tx:write(Key, Value), {ok}),
    ?equals(api_tx:read(Key), {ok, Value}).

tester_write_read_not_existing(_Config) ->
    tester:test(?MODULE, prop_write_read_not_existing, 2, 10000).

-spec prop_write_read(Key::client_key(), Value1::client_value(), Value2::client_value()) -> true | no_return().
prop_write_read(Key, Value1, Value2) ->
    ?equals(api_tx:write(Key, Value1), {ok}),
    ?equals(api_tx:write(Key, Value2), {ok}),
    ?equals(api_tx:read(Key), {ok, Value2}).

tester_write_read(_Config) ->
    tester:test(?MODULE, prop_write_read, 3, 10000).

-spec prop_add_del_on_list2(Key::client_key(), Initial::client_value(), OldExists::boolean(), ToAdd::client_value(), ToRemove::client_value()) -> true | no_return().
prop_add_del_on_list2(Key, Initial, OldExists, ToAdd, ToRemove) ->
    if (not erlang:is_list(Initial)) orelse
           (not erlang:is_list(ToAdd)) orelse
           (not erlang:is_list(ToRemove)) ->
           ?equals(api_tx:add_del_on_list(Key, ToAdd, ToRemove), {fail, not_a_list}),
           Result = api_tx:read(Key),
           if OldExists -> ?equals(Result, {ok, Initial});
              true      -> ?equals(Result, {fail, not_found})
           end;
       true ->
           ?equals(api_tx:add_del_on_list(Key, ToAdd, ToRemove), {ok}),
           Result = api_tx:read(Key),
           ?equals_pattern(Result, {ok, _List}),
           {ok, List} = Result,
           SortedList = lists:sort(fun util:'=:<'/2, List),
           ?equals(SortedList, lists:sort(fun util:'=:<'/2, util:minus_first(lists:append(Initial, ToAdd), ToRemove)))
    end.

-spec prop_add_del_on_list_not_existing(Key::client_key(), ToAdd::[client_value()], ToRemove::[client_value()]) -> true | no_return().
prop_add_del_on_list_not_existing(Key, ToAdd, ToRemove) ->
    case api_tx:read(Key) of
        {ok, OldValue} -> OldExists = true;
        _ -> OldValue = [], OldExists = false
    end,
    prop_add_del_on_list2(Key, OldValue, OldExists, ToAdd, ToRemove).

tester_add_del_on_list_not_existing(_Config) ->
    tester:test(?MODULE, prop_add_del_on_list_not_existing, 3, 10000).

-spec prop_add_del_on_list(Key::client_key(), Initial::client_value(), ToAdd::[client_value()], ToRemove::[client_value()]) -> true | no_return().
prop_add_del_on_list(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_del_on_list2(Key, Initial, true, ToAdd, ToRemove).

tester_add_del_on_list(_Config) ->
    tester:test(?MODULE, prop_add_del_on_list, 4, 10000).

-spec prop_add_del_on_list_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value(), ToRemove::client_value()) -> true | no_return().
prop_add_del_on_list_maybe_invalid(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_del_on_list2(Key, Initial, true, ToAdd, ToRemove).

tester_add_del_on_list_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_add_del_on_list_maybe_invalid, 4, 10000).

-spec prop_add_on_nr2(Key::client_key(), Existing::boolean(), Initial::client_value(), ToAdd::client_value()) -> true | no_return().
prop_add_on_nr2(Key, Existing, Initial, ToAdd) ->
    if (not erlang:is_number(Initial)) orelse
           (not erlang:is_number(ToAdd)) ->
           ?equals(api_tx:add_on_nr(Key, ToAdd), {fail, not_a_number}),
           Result = api_tx:read(Key),
           if Existing -> ?equals(Result, {ok, Initial});
              true     -> ?equals(Result, {fail, not_found})
           end;
       true ->
           ?equals(api_tx:add_on_nr(Key, ToAdd), {ok}),
           Result = api_tx:read(Key),
           ?equals_pattern(Result, {ok, _Number}),
           {ok, Number} = Result,
           case Existing of
               false -> ?equals(Number, ToAdd);
               % note: Initial+ToAdd could be float when Initial is not and thus Number is neither
               true when ToAdd == 0 -> ?equals(Number, Initial);
               _     -> ?equals(Number, (Initial + ToAdd))
           end
    end.

-spec prop_add_on_nr_not_existing(Key::client_key(), ToAdd::number()) -> true | no_return().
prop_add_on_nr_not_existing(Key, ToAdd) ->
    {Existing, OldValue} = case api_tx:read(Key) of
                               {ok, Value} -> {true, Value};
                               _ -> {false, 0}
                           end,
    prop_add_on_nr2(Key, Existing, OldValue, ToAdd).

tester_add_on_nr_not_existing(_Config) ->
    tester:test(?MODULE, prop_add_on_nr_not_existing, 2, 10000).

-spec prop_add_on_nr(Key::client_key(), Initial::client_value(), ToAdd::number()) -> true | no_return().
prop_add_on_nr(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_on_nr2(Key, true, Initial, ToAdd).

tester_add_on_nr(_Config) ->
    tester:test(?MODULE, prop_add_on_nr, 3, 10000).

-spec prop_add_on_nr_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value()) -> true | no_return().
prop_add_on_nr_maybe_invalid(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_on_nr2(Key, true, Initial, ToAdd).

tester_add_on_nr_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_add_on_nr_maybe_invalid, 3, 10000).

-spec prop_test_and_set2(Key::client_key(), Existing::boolean(), RealOldValue::client_value(), OldValue::client_value(), NewValue::client_value()) -> true | no_return().
prop_test_and_set2(Key, Existing, RealOldValue, OldValue, NewValue) ->
    if not Existing ->
           ?equals(api_tx:test_and_set(Key, OldValue, NewValue), {fail, not_found}),
           ?equals(api_tx:read(Key), {fail, not_found});
       RealOldValue =:= OldValue ->
           ?equals(api_tx:test_and_set(Key, OldValue, NewValue), {ok}),
           ?equals(api_tx:read(Key), {ok, NewValue});
       true ->
           ?equals(api_tx:test_and_set(Key, OldValue, NewValue), {fail, {key_changed, RealOldValue}}),
           ?equals(api_tx:read(Key), {ok, RealOldValue})
    end.

-spec prop_test_and_set_not_existing(Key::client_key(), OldValue::client_value(), NewValue::client_value()) -> true | no_return().
prop_test_and_set_not_existing(Key, OldValue, NewValue) ->
    {Existing, RealOldValue} = case api_tx:read(Key) of
                                   {ok, Value} -> {true, Value};
                                   _ -> {false, unknown}
                               end,
    prop_test_and_set2(Key, Existing, RealOldValue, OldValue, NewValue).

tester_test_and_set_not_existing(_Config) ->
    tester:test(?MODULE, prop_test_and_set_not_existing, 3, 10000).

-spec prop_test_and_set(Key::client_key(), RealOldValue::client_value(), OldValue::client_value(), NewValue::client_value()) -> true | no_return().
prop_test_and_set(Key, RealOldValue, OldValue, NewValue) ->
    ?equals(api_tx:write(Key, RealOldValue), {ok}),
    prop_test_and_set2(Key, true, RealOldValue, OldValue, NewValue).

tester_test_and_set(_Config) ->
    tester:test(?MODULE, prop_test_and_set, 4, 10000).

%%% operations with TLOG:

-spec prop_tlog_add_del_on_list2(TLog::tx_tlog:tlog(), Key::client_key(), Initial::client_value(), OldExists::boolean(), ToAdd::client_value(), ToRemove::client_value()) -> true | no_return().
prop_tlog_add_del_on_list2(TLog0, Key, Initial, OldExists, ToAdd, ToRemove) ->
    {TLog1, Result1} = api_tx:add_del_on_list(TLog0, Key, ToAdd, ToRemove),
    {TLog2, Result2} = api_tx:read(TLog1, Key),
    if (not erlang:is_list(Initial)) orelse
           (not erlang:is_list(ToAdd)) orelse
           (not erlang:is_list(ToRemove)) ->
           ?equals(Result1, {fail, not_a_list}),
           if OldExists -> ?equals(Result2, {ok, Initial});
              true      -> ?equals(Result2, {fail, not_found})
           end,
           Result3 = api_tx:commit(TLog2),
           ?equals(Result3, {fail, abort}),
           if OldExists -> ?equals(api_tx:read(Key), {ok, Initial});
              true      -> ?equals(api_tx:read(Key), {fail, not_found})
           end;
       true ->
           ?equals(Result1, {ok}),
           ?equals_pattern(Result2, {ok, _List}),
           {ok, List} = Result2,
           SortedList = lists:sort(fun util:'=:<'/2, List),
           ?equals(SortedList, lists:sort(fun util:'=:<'/2, util:minus_first(lists:append(Initial, ToAdd), ToRemove))),
           Result3 = api_tx:commit(TLog2),
           ?equals(Result3, {ok}),
           ?equals(api_tx:read(Key), Result2)
    end.

-spec prop_tlog_add_del_on_list_not_existing(Key::client_key(), ToAdd::[client_value()], ToRemove::[client_value()]) -> true | no_return().
prop_tlog_add_del_on_list_not_existing(Key, ToAdd, ToRemove) ->
    case api_tx:read(Key) of
        {ok, OldValue} -> OldExists = true;
        _ -> OldValue = [], OldExists = false
    end,
    prop_tlog_add_del_on_list2(api_tx:new_tlog(), Key, OldValue, OldExists, ToAdd, ToRemove).

tester_tlog_add_del_on_list_not_existing(_Config) ->
    tester:test(?MODULE, prop_tlog_add_del_on_list_not_existing, 3, 10000).

-spec prop_tlog_add_del_on_list(Key::client_key(), Initial::client_value(), ToAdd::[client_value()], ToRemove::[client_value()]) -> true | no_return().
prop_tlog_add_del_on_list(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_del_on_list2(api_tx:new_tlog(), Key, Initial, true, ToAdd, ToRemove).

tester_tlog_add_del_on_list(_Config) ->
    tester:test(?MODULE, prop_tlog_add_del_on_list, 4, 10000).

-spec prop_tlog_add_del_on_list_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value(), ToRemove::client_value()) -> true | no_return().
prop_tlog_add_del_on_list_maybe_invalid(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_del_on_list2(api_tx:new_tlog(), Key, Initial, true, ToAdd, ToRemove).

tester_tlog_add_del_on_list_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_tlog_add_del_on_list_maybe_invalid, 4, 10000).

-spec prop_tlog_add_on_nr2(TLog::tx_tlog:tlog(), Key::client_key(), Existing::boolean(), Initial::client_value(), ToAdd::client_value()) -> true | no_return().
prop_tlog_add_on_nr2(TLog0, Key, Existing, Initial, ToAdd) ->
    {TLog1, Result1} = api_tx:add_on_nr(TLog0, Key, ToAdd),
    {TLog2, Result2} = api_tx:read(TLog1, Key),
    if (not erlang:is_number(Initial)) orelse
           (not erlang:is_number(ToAdd)) ->
           ?equals(Result1, {fail, not_a_number}),
           if Existing -> ?equals(Result2, {ok, Initial});
              true     -> ?equals(Result2, {fail, not_found})
           end,
           Result3 = api_tx:commit(TLog2),
           ?equals(Result3, {fail, abort}),
           if Existing -> ?equals(api_tx:read(Key), {ok, Initial});
              true     -> ?equals(api_tx:read(Key), {fail, not_found})
           end;
       true ->
           ?equals(Result1, {ok}),
           ?equals_pattern(Result2, {ok, _Number}),
           {ok, Number} = Result2,
           case Existing of
               false -> ?equals(Number, ToAdd);
               % note: Initial+ToAdd could be float when Initial is not and thus Number is neither
               true when ToAdd == 0 -> ?equals(Number, Initial);
               _     -> ?equals(Number, (Initial + ToAdd))
           end,
           Result3 = api_tx:commit(TLog2),
           ?equals(Result3, {ok}),
           ?equals(api_tx:read(Key), Result2)
    end.

-spec prop_tlog_add_on_nr_not_existing(Key::client_key(), ToAdd::number()) -> true | no_return().
prop_tlog_add_on_nr_not_existing(Key, ToAdd) ->
    {Existing, OldValue} = case api_tx:read(Key) of
                               {ok, Value} -> {true, Value};
                               _ -> {false, 0}
                           end,
    prop_tlog_add_on_nr2(api_tx:new_tlog(), Key, Existing, OldValue, ToAdd).

tester_tlog_add_on_nr_not_existing(_Config) ->
    tester:test(?MODULE, prop_tlog_add_on_nr_not_existing, 2, 10000).

-spec prop_tlog_add_on_nr(Key::client_key(), Initial::client_value(), ToAdd::number()) -> true | no_return().
prop_tlog_add_on_nr(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_on_nr2(api_tx:new_tlog(), Key, true, Initial, ToAdd).

tester_tlog_add_on_nr(_Config) ->
    tester:test(?MODULE, prop_tlog_add_on_nr, 3, 10000).

-spec prop_tlog_add_on_nr_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value()) -> true | no_return().
prop_tlog_add_on_nr_maybe_invalid(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_on_nr2(api_tx:new_tlog(), Key, true, Initial, ToAdd).

tester_tlog_add_on_nr_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_tlog_add_on_nr_maybe_invalid, 3, 10000).
