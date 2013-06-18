%% @copyright 2012-2013 Zuse Institute Berlin

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

all()   -> [
            new_tlog_0,
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
            read_write_2old,
            read_write_2old_locked,
            read_write_notfound,
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
            tester_tlog_add_on_nr_maybe_invalid,
            tester_tlog_test_and_set_not_existing,
            tester_tlog_test_and_set,
            tester_random_from_list,
            tester_req_list,
            tester_req_list_on_same_key,
            req_list_parallelism
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
    ?equals(api_tx:new_tlog(), []).

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
    ok.

conflicting_tx2(_Config) ->
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

-spec write_test(Config::[tuple()]) -> true.
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
    ?equals_pattern_w_note(FirstWriteTime, X when X =< 1000000,
       "We need more than a second to become operational?!"),
    ?equals_pattern(SecondWriteTime, X when X =< 1000000).

-spec read_write_2old(Config::[tuple()]) -> ok.
read_write_2old(_Config) ->
    Key = "read_write_2old_a",
    GSelf = comm:make_global(self()),
    ?equals_w_note(api_tx:write(Key, 1), {ok}, "write_1_a"),
    wait_for_dht_entries(4),
    [HK1, HK2, _HK3, _HK4] = ?RT:get_replica_keys(?RT:hash_key(Key)),
    _ = [comm:send_local(DhtNode, {delete_keys, GSelf, [HK1, HK2]}) || DhtNode <- pid_groups:find_all(dht_node)],
    receive {delete_keys_reply} -> ok end,
    receive {delete_keys_reply} -> ok end,
    
    ?equals(api_tx:write(Key, 2), {ok}),
    ok.

-spec read_write_2old_locked(Config::[tuple()]) -> ok.
read_write_2old_locked(_Config) ->
    Key = "read_write_2old_a",
    GSelf = comm:make_global(self()),
    ?equals_w_note(api_tx:write(Key, 1), {ok}, "write_1_a"),
    wait_for_dht_entries(4),
    [HK1, HK2, _HK3, _HK4] = ?RT:get_replica_keys(?RT:hash_key(Key)),

    % get HK1, HK2 entries
    api_dht_raw:unreliable_lookup(HK1, {get_key_entry, GSelf, HK1}),
    api_dht_raw:unreliable_lookup(HK2, {get_key_entry, GSelf, HK2}),
    receive {get_key_entry_reply, Entry1} ->
                ?assert_w_note(not db_entry:is_empty(Entry1), io_lib:format("~p", [Entry1]))
    end,
    receive {get_key_entry_reply, Entry2} ->
                ?assert_w_note(not db_entry:is_empty(Entry2), io_lib:format("~p", [Entry2]))
    end,
    ?equals(db_entry:get_version(Entry1), db_entry:get_version(Entry2)),
    OldVersion = db_entry:get_version(Entry1),
    
    % write new value
    ?equals_w_note(api_tx:write(Key, 2), {ok}, "write_2_a"),
    util:wait_for(
      fun() ->
              {Status, Values} = api_dht_raw:range_read(0, 0),
              Status =:= ok andalso
                  lists:all(fun(E) ->
                                    db_entry:get_version(E) =:= (OldVersion + 1)
                            end, Values)
      end),
    
    % set two outdated, locked entries:
    Entry1L = db_entry:set_writelock(Entry1, OldVersion - 1),
    Entry2L = db_entry:set_writelock(Entry2, OldVersion - 1),
    api_dht_raw:unreliable_lookup(db_entry:get_key(Entry1L), {set_key_entry, GSelf, Entry1L}),
    api_dht_raw:unreliable_lookup(db_entry:get_key(Entry2L), {set_key_entry, GSelf, Entry2L}),
    receive {set_key_entry_reply, Entry1L} -> ok end,
    receive {set_key_entry_reply, Entry2L} -> ok end,
    
    % now try to write
    ?equals_w_note(api_tx:write(Key, 3), {ok}, "write_3_a"),
    ok.

-spec read_write_notfound(Config::[tuple()]) -> ok.
read_write_notfound(_Config) ->
    Key = "read_write_notfound_test",
    _ = [read_write_notfound_test(Key ++ lists:flatten(io_lib:format("_~p_~p", [X, M])), X, M)
           || X <- [none | lists:seq(1,4)],
              M <- [single, req_list]],
    ok.

-spec read_write_notfound_test(Key::client_key(), HashedKeyToExclude::1..4 | none, Mode::single | req_list) -> ok.
read_write_notfound_test(Key, HashedKeyToExclude, Mode) ->
    [HK1, HK2, _HK3, _HK4] = HashedKeys = ?RT:get_replica_keys(?RT:hash_key(Key)),
    Note = io_lib:format("Key: ~p, Hashed: ~p, Excl.: ~p, Mode: ~p",
                         [Key, HashedKeys, HashedKeyToExclude, Mode]),
    % init 
    ?equals_w_note(api_tx:write(Key, 1), {ok}, Note ++ " (write_0_a)"),
    wait_for_dht_entries(HashedKeys),
    _ = [begin
             comm:send_local(DhtNode, {delete_keys, comm:make_global(self()), [HK1, HK2]}),
             receive {delete_keys_reply} -> ok end
         end || DhtNode <- pid_groups:find_all(dht_node)],
    
    % test
    case HashedKeyToExclude of
        none -> ok;
        _    -> drop_read_op_on_key(HashedKeyToExclude)
    end,
    case Mode of
        single ->
            ct:pal("read"),
            {T1, R1} = api_tx:read(api_tx:new_tlog(), Key),
            ct:pal("write ~p", [T1]),
            {T2, R2} = api_tx:write(T1, Key, 2),
            ct:pal("commit ~p", [T2]),
            R3 = api_tx:commit(T2),
            ok;
        req_list ->
            ct:pal("req_list"),
            {_T1, [R1, R2, R3]} = api_tx:req_list(api_tx:new_tlog(), [{read, Key}, {write, Key, 2}, {commit}]),
            ok
    end,
    
    ?equals_w_note(R2, {ok}, Note ++ " (write result)"),
    % the following should be true but is not at the moment:
    case R1 of
        {fail, not_found} ->
            ?equals_pattern_w_note(R3, {fail, abort, _}, Note ++ " (commit result)");
        {ok, 1} ->
            ?equals_pattern_w_note(R3, {ok}, Note ++ " (commit result)")
    end,
    
    % cleanup
    stop_drop_read_op_on_key(HashedKeyToExclude),
    ok.

drop_read_op_on_key(HashedKey) ->
    ct:pal("Silencing key ~p~n", [HashedKey]),
    Self = self(),
    SkipHashedKeyFun =
        fun (Message, _State) ->
                 case Message of
                     {?read_op, _Source_PID, _SourceId, HashedKey, _Op} ->
                         ct:pal("Detected read, dropping it ~p, key ~p~n",
                                [self(), HashedKey]),
                         comm:send_local(Self, {drop_read_op_on_key, HashedKey, done}),
                         drop_single;
                     {?read_op, _Source_PID, _SourceId, HashedKey2, _Op} ->
                         ct:pal("Detected read ~p, key ~p~n",
                                [self(), HashedKey2]),
                         false;
                     _ ->
%%                          ct:pal("Let pass ~p~n", [Message]),
                         false
                 end
        end,
    
    _ = [gen_component:bp_set_cond(DhtNode, SkipHashedKeyFun, drop_read_op_on_key)
        || DhtNode <- pid_groups:find_all(dht_node)],
    ok.

stop_drop_read_op_on_key(HashedKey) ->
    ct:pal("Reactivating ~p~n", [HashedKey]),
    _ = [gen_component:bp_del(DhtNode, drop_read_op_on_key)
        || DhtNode <- pid_groups:find_all(dht_node)],
    cleanup_drop_read_op_on_key(HashedKey).

cleanup_drop_read_op_on_key(HashedKey) ->
    receive {drop_read_op_on_key, HashedKey, done} -> cleanup_drop_read_op_on_key(HashedKey)
    after 0 -> ok end.

-spec prop_encode_decode(Value::client_value()) -> boolean().
prop_encode_decode(Value) ->
    Value =:= rdht_tx:decode_value(rdht_tx:encode_value(Value)).

tester_encode_decode(_Config) ->
    tester:test(?MODULE, prop_encode_decode, 1, 10000).

random_write_read2(0) -> ok;
random_write_read2(Count) ->
    Key = io_lib:format("~p", [Count]),
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
    tester:test(?MODULE, prop_add_del_on_list_not_existing, 3, 5000).

-spec prop_add_del_on_list(Key::client_key(), Initial::client_value(), ToAdd::[client_value()], ToRemove::[client_value()]) -> true | no_return().
prop_add_del_on_list(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_del_on_list2(Key, Initial, true, ToAdd, ToRemove).

tester_add_del_on_list(_Config) ->
    tester:test(?MODULE, prop_add_del_on_list, 4, 5000).

-spec prop_add_del_on_list_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value(), ToRemove::client_value()) -> true | no_return().
prop_add_del_on_list_maybe_invalid(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_del_on_list2(Key, Initial, true, ToAdd, ToRemove).

tester_add_del_on_list_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_add_del_on_list_maybe_invalid, 4, 5000).

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
    tester:test(?MODULE, prop_add_on_nr_not_existing, 2, 5000).

-spec prop_add_on_nr(Key::client_key(), Initial::client_value(), ToAdd::number()) -> true | no_return().
prop_add_on_nr(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_on_nr2(Key, true, Initial, ToAdd).

tester_add_on_nr(_Config) ->
    tester:test(?MODULE, prop_add_on_nr, 3, 5000).

-spec prop_add_on_nr_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value()) -> true | no_return().
prop_add_on_nr_maybe_invalid(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_add_on_nr2(Key, true, Initial, ToAdd).

tester_add_on_nr_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_add_on_nr_maybe_invalid, 3, 5000).

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
    tester:test(?MODULE, prop_test_and_set_not_existing, 3, 5000).

-spec prop_test_and_set(Key::client_key(), RealOldValue::client_value(), OldValue::client_value(), NewValue::client_value()) -> true | no_return().
prop_test_and_set(Key, RealOldValue, OldValue, NewValue) ->
    ?equals(api_tx:write(Key, RealOldValue), {ok}),
    prop_test_and_set2(Key, true, RealOldValue, OldValue, NewValue).

tester_test_and_set(_Config) ->
    tester:test(?MODULE, prop_test_and_set, 4, 5000).

-spec prop_random_from_list(Key::client_key(), Value::client_value()) -> true.
prop_random_from_list(Key, Value) ->
    _ = api_tx:write(Key,  [Value]),
    ?equals_pattern(
        api_tx:req_list([{read, Key, random_from_list}]),
        {[{?read, Key, Version, ?ok, SnapNumber, ?value_dropped, ?value_dropped}],
         [{ok, { Value, 1 } }]} when is_integer(Version) 
                                    andalso Version >= 0
                                    andalso is_integer(SnapNumber)
                                    andalso SnapNumber >= 0),
    ValueEnc = rdht_tx:encode_value({Value, 1}),
    ?equals_pattern(
        api_txc:req_list([{read, Key, random_from_list}]),
        {[{?read, Key, Version, ?ok, SnapNumber, ?value_dropped, ?value_dropped}],
         [{ok, ValueEnc}]} when is_integer(Version)
                                    andalso Version >= 0
                                    andalso is_integer(SnapNumber)
                                    andalso SnapNumber >= 0),
    true.

tester_random_from_list(_Config) ->
    tester:test(?MODULE, prop_random_from_list, 2, 5000).

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
           ?equals(Result3, {fail, abort, [Key]}),
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
    tester:test(?MODULE, prop_tlog_add_del_on_list_not_existing, 3, 5000).

-spec prop_tlog_add_del_on_list(Key::client_key(), Initial::client_value(), ToAdd::[client_value()], ToRemove::[client_value()]) -> true | no_return().
prop_tlog_add_del_on_list(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_del_on_list2(api_tx:new_tlog(), Key, Initial, true, ToAdd, ToRemove).

tester_tlog_add_del_on_list(_Config) ->
    tester:test(?MODULE, prop_tlog_add_del_on_list, 4, 5000).

-spec prop_tlog_add_del_on_list_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value(), ToRemove::client_value()) -> true | no_return().
prop_tlog_add_del_on_list_maybe_invalid(Key, Initial, ToAdd, ToRemove) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_del_on_list2(api_tx:new_tlog(), Key, Initial, true, ToAdd, ToRemove).

tester_tlog_add_del_on_list_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_tlog_add_del_on_list_maybe_invalid, 4, 5000).

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
           ?equals(Result3, {fail, abort, [Key]}),
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
    tester:test(?MODULE, prop_tlog_add_on_nr_not_existing, 2, 5000).

-spec prop_tlog_add_on_nr(Key::client_key(), Initial::client_value(), ToAdd::number()) -> true | no_return().
prop_tlog_add_on_nr(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_on_nr2(api_tx:new_tlog(), Key, true, Initial, ToAdd).

tester_tlog_add_on_nr(_Config) ->
    tester:test(?MODULE, prop_tlog_add_on_nr, 3, 5000).

-spec prop_tlog_add_on_nr_maybe_invalid(Key::client_key(), Initial::client_value(), ToAdd::client_value()) -> true | no_return().
prop_tlog_add_on_nr_maybe_invalid(Key, Initial, ToAdd) ->
    ?equals(api_tx:write(Key, Initial), {ok}),
    prop_tlog_add_on_nr2(api_tx:new_tlog(), Key, true, Initial, ToAdd).

tester_tlog_add_on_nr_maybe_invalid(_Config) ->
    tester:test(?MODULE, prop_tlog_add_on_nr_maybe_invalid, 3, 5000).

-spec prop_tlog_test_and_set2(TLog::tx_tlog:tlog(), Key::client_key(), Existing::boolean(), RealOldValue::client_value(), OldValue::client_value(), NewValue::client_value()) -> true | no_return().
prop_tlog_test_and_set2(TLog0, Key, Existing, RealOldValue, OldValue, NewValue) ->
    {TLog1, Result1} = api_tx:test_and_set(TLog0, Key, OldValue, NewValue),
    {TLog2, Result2} = api_tx:read(TLog1, Key),
    if not Existing ->
           ?equals(Result1, {fail, not_found}),
           ?equals(Result2, {fail, not_found}),
           Result3 = api_tx:commit(TLog2),
           ?equals(Result3, {fail, abort, [Key]}),
           ?equals(api_tx:read(Key), {fail, not_found});
       RealOldValue =:= OldValue ->
           ?equals(Result1, {ok}),
           ?equals(Result2, {ok, NewValue}),
           Result3 = api_tx:commit(TLog2),
           ?equals(Result3, {ok}),
           ?equals(api_tx:read(Key), Result2);
       true ->
           ?equals(Result1, {fail, {key_changed, RealOldValue}}),
           ?equals(Result2, {ok, RealOldValue}),
           Result3 = api_tx:commit(TLog2),
           ?equals(Result3, {fail, abort, [Key]}),
           ?equals(api_tx:read(Key), {ok, RealOldValue})
    end.

-spec prop_tlog_test_and_set_not_existing(Key::client_key(), OldValue::client_value(), NewValue::client_value()) -> true | no_return().
prop_tlog_test_and_set_not_existing(Key, OldValue, NewValue) ->
    {Existing, RealOldValue} = case api_tx:read(Key) of
                                   {ok, Value} -> {true, Value};
                                   _ -> {false, unknown}
                               end,
    prop_tlog_test_and_set2(api_tx:new_tlog(), Key, Existing, RealOldValue, OldValue, NewValue).

tester_tlog_test_and_set_not_existing(_Config) ->
    tester:test(?MODULE, prop_tlog_test_and_set_not_existing, 3, 5000).

-spec prop_tlog_test_and_set(Key::client_key(), RealOldValue::client_value(), OldValue::client_value(), NewValue::client_value()) -> true | no_return().
prop_tlog_test_and_set(Key, RealOldValue, OldValue, NewValue) ->
    ?equals(api_tx:write(Key, RealOldValue), {ok}),
    prop_tlog_test_and_set2(api_tx:new_tlog(), Key, true, RealOldValue, OldValue, NewValue).

tester_tlog_test_and_set(_Config) ->
    tester:test(?MODULE, prop_tlog_test_and_set, 4, 5000).

%% @doc Checks that the same result is returned when executing a req_list in a
%%      bunch or as sequential single requests (partial reads with random data
%%      are not supported).
-spec prop_tester_req_list([api_tx:read_request() | api_tx:write_request() |
                            api_tx:add_del_on_list_request() |
                            api_tx:add_on_nr_request() |
                            api_tx:test_and_set_request()]) -> true | no_return().
prop_tester_req_list(ReqList) ->
    {TLogAll, ResultAll} = api_tx:req_list(ReqList),
    {TLogSeq, ResultSeq} =
        lists:foldl(fun(Req, {TLog, Res}) ->
                            {NTlog, NRes} = api_tx:req_list(TLog, [Req]),
                            {NTlog, lists:append(Res, NRes)}
                    end, {api_tx:new_tlog(), []}, ReqList),
    ?equals(ResultAll, ResultSeq),
    ?equals(TLogAll, TLogSeq).

tester_req_list(_Config) ->
    prop_tester_req_list([{add_del_on_list,[354334],{},[-1]},
                          {test_and_set,[677315],{[]},3},
                          {read,[677315]}]),
    prop_tester_req_list([{add_del_on_list,[354334],{},[-1]},
                          {read,[677315]},
                          {read,[677315]}]),
    tester:test(?MODULE, prop_tester_req_list, 1, 5000).

%% @doc Compares Res with ExpRes for the given request Req. Takes care of not
%%      comparing results from reading random values, e.g. random_from_list.
%%      If ORes (1-element list) has failed, Res (1|2-element list) should also
%%      be the same as ExpRes (1|2-element list).
-spec same_result_if_not_random(Req::api_tx:request_on_key(), ORes::[api_tx:result(),...],
                                Res::[api_tx:result(),...], ExpRes::[api_tx:result(),...],
                                Note::iolist()) -> true.
same_result_if_not_random(Req, [ORes], Res, ExpRes, Note) ->
    case Req of
        {read, _Key0, ReadOp}
          when ReadOp =:= random_from_list orelse
                   (is_tuple(ReadOp) andalso element(1, ReadOp) =:= sublist) ->
            case ORes of
                {fail, _} ->
                    ?equals_w_note(Res, ExpRes, Note);
                {ok, {_RandomVal, ListLength}} ->
                    % can only guarantee the list length here
                    case length(ExpRes) of
                        2 -> ?equals_pattern_w_note(Res, [{ok}, {ok, {_, ListLength}}], Note);
                        1 -> ?equals_pattern_w_note(Res, [{ok, {_, ListLength}}], Note)
                    end
            end;
        _ ->
            ?equals_w_note(Res, ExpRes, Note)
    end.

-spec check_op_on_tlog(tx_tlog:tlog(), api_tx:request_on_key(), tx_tlog:tlog(),
                       [api_tx:result(),...], none | client_value) -> true | no_return().
check_op_on_tlog(TLog, Req, NTLog, NRes, RingVal) ->
    ReqKey = element(2, Req),
    case tx_tlog:find_entry_by_key(TLog, ReqKey) of
        false ->
            % TODO: implement some checks here, e.g. create OldEntry as read op and continue with the checks below
            true;
        OldEntry ->
            NewEntry = tx_tlog:find_entry_by_key(NTLog, ReqKey),
            ?assert_w_note(NewEntry =/= false, io_lib:format("NewEntry: ~.0p", [NewEntry])),
            Note = io_lib:format("Entry: ~.0p, Res: ~.0p, Req: ~.0p, RingVal: ~.0p", [OldEntry, NRes, Req, RingVal]),
            case tx_tlog:get_entry_status(OldEntry) of
                ?fail ->
                    % despite the status 'failed', the operation's result
                    % should be the same as if there was no failure!
                    TLog2 = [tx_tlog:set_entry_status(OldEntry, ?ok)],
                    {NTLog2, NRes2} = api_tx:req_list(TLog2, [Req]),
                    same_result_if_not_random(Req, NRes, NRes2, NRes, Note),
                    % while we are at it, check the op on an ok TLog, too:
                    check_op_on_tlog(TLog2, Req, NTLog2, NRes2, RingVal),
                    % status must not change between OldEntry and NewEntry!
                    ?equals(?fail, tx_tlog:get_entry_status(NewEntry));
                ?ok ->
                    
                    % result must be the same as if executed alone
                    % note: previous write may have changed the value!
                    {ExpResAlone, ReqsAlone} =
                        case tx_tlog:get_entry_operation(OldEntry) of
                            ?read -> {NRes, [Req]};
                            ?write ->
                                ValueAfterWrite = rdht_tx:decode_value(element(2, tx_tlog:get_entry_value(OldEntry))),
                                {[{ok}, hd(NRes)], [{write, ReqKey, ValueAfterWrite}, Req]}
                        end,
                    {_, NRes2} = api_tx:req_list(ReqsAlone),
                    same_result_if_not_random(Req, NRes, NRes2, ExpResAlone, Note),
                    
                    {_, OReadRes} = api_tx:read(TLog, ReqKey),
                    case OReadRes of
                        {fail, not_found} ->
                            % no write can result in not_found:
                            ?equals(tx_tlog:get_entry_operation(OldEntry), ?read),
                            ?assert(RingVal =:= none),
                            case Req of
                                {write, _Key, Value} ->
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                    ?equals(tx_tlog:get_entry_value(NewEntry),
                                            {?value, rdht_tx:encode_value(Value)}),
                                    ?equals(NRes, [{ok}]);
                                {read, _Key, _} -> % random_from_list | {sublist, _Start, Len}
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?fail),
                                    ?equals(tx_tlog:get_entry_value_type(NewEntry), ?value_dropped),
                                    ?equals(NRes, [{fail, not_found}]);
                                {read, _Key} ->
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                    ?equals(tx_tlog:get_entry_value_type(NewEntry), ?value_dropped),
                                    ?equals(NRes, [{fail, not_found}]);
                                {test_and_set, _Key, _Old, _New} ->
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?fail), 
                                    ?equals(tx_tlog:get_entry_value_type(NewEntry), ?value_dropped),
                                    ?equals(NRes, [{fail, not_found}]);
                                {add_on_nr, _Key, X} when NRes =:= [{ok}] ->
                                    % check value content (op will create the value)
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                    ?equals(tx_tlog:get_entry_value(NewEntry),
                                            {?value, rdht_tx:encode_value(X)}),
                                    ?equals(NRes, [{ok}]);
                                {add_on_nr, _Key, _X} when NRes =:= [{fail, not_a_number}] ->
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?fail),
                                    ?equals(tx_tlog:get_entry_value_type(NewEntry), ?value_dropped);
                                {add_del_on_list, _Key, ToAdd, ToDel} when NRes =:= [{ok}] ->
                                    % check value content (op will create the value)
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                    ?equals(tx_tlog:get_entry_value(NewEntry),
                                            {?value, rdht_tx:encode_value(util:minus_first(ToAdd, ToDel))}),
                                    ?equals(NRes, [{ok}]);
                                {add_del_on_list, _Key, _ToAdd, _ToRemove} when NRes =:= [{fail, not_a_list}] ->
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?fail),
                                    ?equals(tx_tlog:get_entry_value_type(NewEntry), ?value_dropped)
                            end;
                        {ok, OValue} ->
                            case tx_tlog:get_entry_operation(OldEntry) of
                                ?read ->
                                    % ?assert(RingVal =/= none) % 'none' may be the value used
                                    % (anyway, we check the value below so this check is not important)
                                    ?equals(OValue, RingVal);
                                ?write ->
                                    ?equals({?value, rdht_tx:encode_value(OValue)},
                                            tx_tlog:get_entry_value(OldEntry))
                            end,
                            case NRes of
                                [OkTpl] when is_tuple(OkTpl) andalso element(1, OkTpl) =:= ok ->
                                    case Req of
                                        {write, _Key, Value} ->
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            ?equals(tx_tlog:get_entry_value(NewEntry),
                                                    {?value, rdht_tx:encode_value(Value)}),
                                            ?equals(NRes, [{ok}]);
                                        {read, _Key} ->
                                            ?equals(OldEntry, NewEntry),
                                            ?equals_pattern(tx_tlog:get_entry_value_type(NewEntry),
                                                            XType when XType =:= ?value_dropped orelse XType =:= ?value),
                                            ?equals(NRes, [{ok, OValue}]);
                                        {test_and_set, _Key, _Old, New} ->
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            ?equals(tx_tlog:get_entry_value(NewEntry),
                                                    {?value, rdht_tx:encode_value(New)}),
                                            ?equals(NRes, [{ok}]);
                                        {add_on_nr, _Key, X} when X == 0 -> % no-op (int or float)
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            ?equals(tx_tlog:get_entry_value(NewEntry),
                                                    tx_tlog:get_entry_value(OldEntry)),
                                            ?equals(NRes, [{ok}]);
                                        {add_on_nr, _Key, X} ->
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            ?equals(tx_tlog:get_entry_value(NewEntry),
                                                    {?value, rdht_tx:encode_value(OValue + X)}),
                                            ?equals(NRes, [{ok}]);
                                        {add_del_on_list, _Key, ToAdd, ToDel} when ToAdd =:= [] andalso ToDel =:= [] -> % no-op
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            ?equals(tx_tlog:get_entry_value(NewEntry),
                                                    tx_tlog:get_entry_value(OldEntry)),
                                            ?equals(NRes, [{ok}]);
                                        {add_del_on_list, _Key, ToAdd, ToDel} ->
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            NValue = util:minus_first(lists:append(ToAdd, OValue), ToDel),
                                            ?equals(tx_tlog:get_entry_value(NewEntry),
                                                    {?value, rdht_tx:encode_value(NValue)}),
                                            ?equals(NRes, [{ok}]);
                                        {read, _Key, random_from_list} ->
                                            ?equals_pattern(NRes, [{ok, {_RandomValX, _ListLengthX}}]),
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            [{ok, {RandomVal, ListLengthX}}] = NRes,
                                            Note2 = io_lib:format("RandomVal: ~p (~p), StoredVal: ~p (~p)",
                                                                 [RandomVal, ListLengthX, OValue, length(OValue)]),
                                            ?assert_w_note(lists:member(RandomVal, OValue), Note2),
                                            ?equals_w_note(ListLengthX, length(OValue), Note2),
                                            ?equals_pattern(tx_tlog:get_entry_value_type(NewEntry),
                                                            XType when XType =:= ?value_dropped orelse XType =:= ?value);
                                        {read, _Key, {sublist, _Start, Len}} ->
                                            ?equals_pattern(NRes, [{ok, {_SubListX, _ListLengthX}}]),
                                            ?equals(tx_tlog:get_entry_status(NewEntry), ?ok),
                                            [{ok, {SubList, ListLengthX}}] = NRes,
                                            Note2 = io_lib:format("SubList: ~p (~p), StoredVal: ~p (~p)",
                                                                 [SubList, ListLengthX, OValue, length(OValue)]),
                                            ?assert_w_note(length(SubList) =< erlang:abs(Len), Note2),
                                            ?equals_w_note(lists:subtract(SubList, OValue), [], Note2),
                                            ?equals_w_note(ListLengthX, length(OValue), Note2),
                                            ?equals_pattern(tx_tlog:get_entry_value_type(NewEntry),
                                                            XType when XType =:= ?value_dropped orelse XType =:= ?value)
                                    end;
                                [{fail, Reason}] ->
                                    case Req of
                                        {write, _Key, _Value} ->
                                            ?ct_fail("a write should never fail without a commit", []);
                                        {read, _Key} ->
                                            ?ct_fail("a read should not fail on an existing value", []);
                                        {test_and_set, _Key, _Old, _New} ->
                                            ?equals(Reason, {key_changed, OValue});
                                        {add_on_nr, _Key, _X} ->
                                            ?equals(Reason, not_a_number);
                                        {add_del_on_list, _Key, _ToAdd, _ToDel} ->
                                            ?equals(Reason, not_a_list);
                                        {read, _Key, random_from_list} ->
                                            ?equals_pattern(Reason, X when X =:= empty_list orelse X =:= not_a_list);
                                        {read, _Key, {sublist, _Start, _Len}} ->
                                            ?equals(Reason, not_a_list)
                                    end,
                                    % note: all ops except read fail the transaction if the op fails
                                    %       a read never gets to this point though so we can check it here
                                    ?equals(tx_tlog:get_entry_status(NewEntry), ?fail),
                                    ?equals(tx_tlog:get_entry_value(NewEntry),
                                            tx_tlog:get_entry_value(OldEntry))
                            end
                    end
            end
    end.


check_commit([], {ok}, _RingVal) -> true;
check_commit(TLog, CommitRes, RingVal) ->
    TEntry = hd(TLog),
    Key = tx_tlog:get_entry_key(TEntry),
    case CommitRes of
        {ok} ->
            ?equals(?ok, tx_tlog:get_entry_status(TEntry)),
            case tx_tlog:get_entry_operation(TEntry) of
                ?read ->
                    NewRingVal = case api_tx:read(Key) of
                                     {fail, not_found} -> none;
                                     {ok, NewVal} -> NewVal
                                 end,
                    ?equals(RingVal, NewRingVal);
                ?write ->
                    {ok, NewRingVal} = api_tx:read(Key),
                    ?equals(tx_tlog:get_entry_value(TEntry),
                            {?value, rdht_tx:encode_value(NewRingVal)})
            end;
        {fail, abort, _} ->
            % no concurrency in the test case, so the commit result should depend on the entry's tx status!
            ?equals(?fail, tx_tlog:get_entry_status(TEntry)),
            NewRingVal = case api_tx:read(Key) of
                             {fail, not_found} -> none;
                             {ok, NewVal} -> NewVal
                         end,
            ?equals(RingVal, NewRingVal)
    end.

%% same result when executing a req_list in a bunch or as sequential
%% single requests.
-spec prop_tester_req_list_on_same_key(client_key(), [api_tx:request_on_key()]) -> true | no_return().
prop_tester_req_list_on_same_key(Key, InReqList) ->
    ReqList = [ setelement(2, Req, Key) || Req <- InReqList ],

    RingVal = case api_tx:read(Key) of
                  {fail, not_found} -> none;
                  {ok, Val} -> Val
              end,
    %% perform on key not in DHT
    {TLogSeqE, _ResultSeqE} =
        lists:foldl(fun(Req, {TLog, Res}) ->
                            {NTLog, NRes} = api_tx:req_list(TLog, [Req]),
                            check_op_on_tlog(TLog, Req, NTLog, NRes, RingVal),
                            {NTLog, lists:append(Res, NRes)}
                    end, {api_tx:new_tlog(), []}, ReqList),

    CommitE = api_tx:commit(TLogSeqE),
    check_commit(TLogSeqE, CommitE, RingVal),

    %% perform on key as int
    {ok} = api_tx:write(Key, 42),
    {TLogSeqI, _ResultSeqI} =
        lists:foldl(fun(Req, {TLog, Res}) ->
                            {NTLog, NRes} = api_tx:req_list(TLog, [Req]),
                            check_op_on_tlog(TLog, Req, NTLog, NRes, 42),
                            {NTLog, lists:append(Res, NRes)}
                    end, {api_tx:new_tlog(), []}, ReqList),

    CommitI = api_tx:commit(TLogSeqI),
    check_commit(TLogSeqI, CommitI, 42),

    %% perform on key as list
    {ok} = api_tx:write(Key, [42]),
    {TLogSeqL, _ResultSeqL} =
        lists:foldl(fun(Req, {TLog, Res}) ->
                            {NTLog, NRes} = api_tx:req_list(TLog, [Req]),
                            check_op_on_tlog(TLog, Req, NTLog, NRes, [42]),
                            {NTLog, lists:append(Res, NRes)}
                    end, {api_tx:new_tlog(), []}, ReqList),

    CommitL = api_tx:commit(TLogSeqL),
    check_commit(TLogSeqL, CommitL, [42]),

    true.

tester_req_list_on_same_key(_Config) ->
    prop_tester_req_list_on_same_key("a", [{read,"a"},{add_on_nr,"a","*"},{test_and_set,"a",[],{42}}]),
    tester:test(?MODULE, prop_tester_req_list_on_same_key, 2, 5000).

req_list_parallelism(_Config) ->
    Partitions = 25,
    WriteReqsPart = [{write, lists:flatten(io_lib:format("articles:count:~B", [X])), 200}
                      || X <- lists:seq(1, Partitions)],
    ReadReqsPart = [{read, lists:flatten(io_lib:format("articles:count:~B", [X]))}
                     || X <- lists:seq(1, Partitions)],

    api_tx:req_list_commit_each(WriteReqsPart),
    {ok} = api_tx:write("articles:count", 200 * Partitions),

    Iters = 500,

    ReadResPart = lists:sum(util:for_to_ex(1, Iters, fun(_) -> element(1, util:tc(api_tx, req_list_commit_each, [ReadReqsPart])) end)),
    ReadRes = lists:sum(util:for_to_ex(1, Iters, fun(_) -> element(1, util:tc(api_tx, req_list_commit_each, [[{read, "articles:count"}]])) end)),
    AvgReadResPart = ReadResPart / Iters,
    AvgReadRes = ReadRes / Iters,
    ct:pal("api_tx:req_list_commit_each~n  1 key : ~.2f~n 25 keys: ~.2f~n", [AvgReadRes, AvgReadResPart]),

    TxReadResPart = lists:sum(util:for_to_ex(1, Iters, fun(_) -> element(1, util:tc(api_tx, req_list, [ReadReqsPart])) end)),
    TxReadRes = lists:sum(util:for_to_ex(1, Iters, fun(_) -> element(1, util:tc(api_tx, req_list, [[{read, "articles:count"}]])) end)),
    AvgTxReadResPart = TxReadResPart / Iters,
    AvgTxReadRes = TxReadRes / Iters,
    ct:pal("api_tx:req_list~n  1 key : ~.2f~n 25 keys: ~.2f~n", [AvgTxReadRes, AvgTxReadResPart]),

    % parallel reads should not be much slower than a single read (tolerate (Partitions / 2) * time)
    if AvgReadResPart >= (Partitions / 2) * AvgReadRes ->
           {comment, lists:flatten(
              io_lib:format(
                "api_tx:req_list_commit_each/1: 1 key: ~.2fus, ~B keys: ~.2fus~n",
                [AvgReadRes, Partitions, AvgReadResPart]))};
       AvgTxReadResPart >= (Partitions / 2) * AvgTxReadRes ->
           {comment, lists:flatten(
              io_lib:format(
                "api_tx:req_list/1: 1 key: ~.2fus, ~B keys: ~.2fus~n",
                [AvgTxReadRes, Partitions, AvgTxReadResPart]))};
        true -> ok
    end.

%% @doc Wait until (exactly) the given number of DHT entries are stored.
%%      This may be necessary, to make sure (late) write messages have arrived
%%      at the original nodes.
%%      Note: DHT entries = 4 * client entries!
-spec wait_for_dht_entries(Count::non_neg_integer() | [?RT:key()]) -> ok.
wait_for_dht_entries(Count) when is_integer(Count) andalso Count >= 0 ->
    util:wait_for(
      fun() ->
              {Status, Values} = api_dht_raw:range_read(0, 0),
              Status =:= ok andalso erlang:length(Values) =:= Count
      end);
wait_for_dht_entries(HashedKeys) when is_list(HashedKeys) ->
    GSelf = comm:make_global(self()),
    util:wait_for(
      fun() ->
              Entries = [begin
                             api_dht_raw:unreliable_lookup(HK, {get_key_entry, GSelf, HK}),
                             receive {get_key_entry_reply, Entry} -> Entry end
                         end || HK <- HashedKeys],
              NonEmptyEntries = [E || E <- Entries, not db_entry:is_empty(E)],
              length(NonEmptyEntries) =:= length(HashedKeys)
      end).
