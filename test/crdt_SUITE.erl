%% @copyright 2013-2018 Zuse Institute Berlin

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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Unit tests for crdt-paxos.
%% @end
-module(crdt_SUITE).
-author('skrzypczak.de').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

-define(NUM_REPEATS, 10).
-define(RANDOMIZE_RING, true).

all()   -> [
            tester_type_check_crdt,
            {group, gcounter_group},
            {group, pncounter_group},
            {group, proto_sched_group}
           ].

groups() -> [
        {gcounter_group, [sequence, {repeat, ?NUM_REPEATS}],
         [
          crdt_gcounter_inc,
          crdt_gcounter_read_your_write,
          crdt_gcounter_read_monotonic,
          crdt_gcounter_read_monotonic2,
          crdt_gcounter_concurrent_read_monotonic,
          crdt_gcounter_ordered_concurrent_read
         ]},
        {pncounter_group, [sequence, {repeat, ?NUM_REPEATS}],
         [
          crdt_pncounter_banking
         ]},
        {proto_sched_group, [sequence, {repeat, ?NUM_REPEATS}],
         [
          crdt_proto_sched_write,
          crdt_proto_sched_concurrent_read_monotonic,
          crdt_proto_sched_concurrent_read_ordered,
          crdt_proto_sched_read_your_write
        ]}
    ].

suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) ->
    [{batching, Group =/= proto_sched_group} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    EnableBatching =
        case lists:keyfind(batching, 1, Config) of
            false -> false;
            {batching, Enabled} -> Enabled
        end,
    Size = randoms:rand_uniform(1, 25),

    unittest_helper:make_ring(Size, [{config, [{log_path, PrivDir},
                                               {ordered_links, false},
                                               {read_batching, EnableBatching}]}]),

    ct:pal("Start test with ringsize ~p", [Size]),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

crdt_gcounter_inc(_Config) ->
    %% start random number of writers
    %% select a key to operate on
    %% start random number of increments (non transactional)
    %% check if number of increments = value in key
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    Parallel = randoms:rand_uniform(1, 50),
    Count = 10000 div Parallel,
    WriteFun = fun(_I) -> ok = gcounter_on_cseq:inc(Key) end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),

    {ok, Result} = gcounter_on_cseq:read(Key),
    ct:pal("gcounter state is: ~p", [gcounter_on_cseq:read_state(Key)]),
    ct:pal("Planned ~p increments, done ~p - discrepancy is NOT ok~n", [Count*Parallel, Result]),
    ?equals(Count*Parallel, Result),
    ok.

crdt_pncounter_banking(_Config) ->
    %% Emulation of banking account suite (without transaction...i.e. no atomicity
    %% of a money transfer).
    %% 1. Create multiple accounts
    %% 2. Spawn multiple worker that transfer money between accounts
    %% 3. Check if all the money is still there
    UnitTestPid = self(),

    AccountNum = randoms:rand_uniform(2, 10),
    Accounts = [randoms:getRandomString() || _ <- lists:seq(1, AccountNum)],
    %% init all accounts with some money
    StartMoney = randoms:rand_uniform(100, 10000),
    TotalMoney = StartMoney * AccountNum,
    _ = [pncounter_on_cseq:add(Account, StartMoney) || Account <- Accounts],

    %% spawn worker
    Parallel = randoms:rand_uniform(1, 50),
    Count = 10000 div Parallel,
    WriteFun =  fun(_I) ->
                    TransactAmount = randoms:rand_uniform(1, StartMoney),
                    From = lists:nth(randoms:rand_uniform(1, AccountNum), Accounts),
                    To = lists:nth(randoms:rand_uniform(1, AccountNum), Accounts),
                    %% allows From =:= To, but shouldn't be a problem
                    ok = pncounter_on_cseq:subtract(From, TransactAmount),
                    ok = pncounter_on_cseq:add(To, TransactAmount)
                end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),

    %% check if nothing is lost or gained
    Balances = [begin
                    {ok, Money} = pncounter_on_cseq:read(Account),
                    Money
                end || Account <- Accounts],
    EndMoney = lists:sum(Balances),
    ct:pal("Start balance: ~p~nEnd balance: ~p~ndiscrepancy is NOT ok!~n",
           [TotalMoney, EndMoney]),
    ?equals(TotalMoney, EndMoney),
    ct:pal("The individual account balances are: ~n~p", [Balances]),
    ok.

crdt_gcounter_read_your_write(_Config) ->
    %% starts concurrent worker writing/reading repeateadly
    %% each update should be visibile immediatly when using the same worker
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    %% Start writer
    Parallel = randoms:rand_uniform(1, 50),
    Count = 1000 div Parallel,
    WriteFun = fun (_) ->
                    %% note: by desing, when mixing a strong read with a eventual write
                    %% or vice verca, a process must not necessarly observer the write.
                    %% this is because eventual ops send their
                    %% request to a specific replica. the quorum ops waits for
                    %% an arbitrary quorum, meaning that the replica used for the eventual
                    %% op might not be included...
                    {O, N} = case randoms:rand_uniform(1, 2) of
                        1 ->
                            {ok, Old} = gcounter_on_cseq:read(Key),
                            ok = gcounter_on_cseq:inc(Key),
                            {ok, New} = gcounter_on_cseq:read(Key),
                            {Old, New};
                        2 ->
                            %% TODO: if there is more than one local dht node
                            %% request will be sent to a random one. Thus, one might
                            %% not see the effects of an eventual write
                            {ok, Old} = gcounter_on_cseq:read_eventual(Key),
                            ok = gcounter_on_cseq:inc_eventual(Key),
                            {ok, New} = gcounter_on_cseq:read_eventual(Key),
                            {Old, New}
                    end,
                    ?equals(true, O < N)
               end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),

    ok.

crdt_gcounter_read_monotonic(_Config) ->
    %% starts random number of (strong and eventual) writers
    %% start one reader
    %% values read should increase monotonic
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    %% read for infinity
    ReadFun = fun() -> {ok, Val} = gcounter_on_cseq:read_state(Key), Val end,
    CmpFun = fun gcounter:lteq/2,
    Reader = spawn_monotonic_reader(UnitTestPid, ReadFun, CmpFun),

    %% Start writer
    Parallel = randoms:rand_uniform(1, 50),
    Count = 10000 div Parallel,
    WriteFun = fun
                    (I) when I div 2 == 0 -> ok = gcounter_on_cseq:inc(Key);
                    (_)                   -> ok = gcounter_on_cseq:inc_eventual(Key)
               end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),

    %% stop reader
    stop_readers(Reader),
    check_monotonic_reader_failure(),

    ok.

crdt_gcounter_read_monotonic2(_Config) ->
    %% starts random number of (strong and eventual) writers
    %% start multiple reader, which submit reads sequentially
    %% values read should increase monotonic
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    ReaderCount = randoms:rand_uniform(2, 50),
    ct:pal("Start ~p readers", [ReaderCount]),
    Reader = [spawn(fun() ->
                        Loop =
                            fun(F) ->
                                receive
                                    {read, Pids, Prev} ->
                                        {ok, Result} = gcounter_on_cseq:read_state(Key),
                                        case gcounter:lteq(Prev, Result) of
                                            true -> ok;
                                            false ->
                                                ct:pal("~n~w ~nis not smaller or equals than ~n~w", [Prev, Result]),
                                                UnitTestPid ! {compare_failed, Prev, Result},
                                                ok
                                        end,
                                        NextReader = lists:nth(randoms:rand_uniform(1, length(Pids)+1), Pids),
                                        NextReader ! {read, Pids, Result},
                                        F(F);
                                    {reader_done} ->
                                        UnitTestPid ! {reader_terminated}
                                end
                            end,
                        Loop(Loop)
                    end)
              || _ <- lists:seq(1, ReaderCount)],
    {ok, Init} = gcounter_on_cseq:read_state(Key),
    hd(Reader) ! {read, Reader, Init},

    %% do all the writes
    Parallel = randoms:rand_uniform(1, 50),
    Count = 10000 div Parallel,
    WriteFun = fun
                    (I) when I div 2 == 0 -> ok = gcounter_on_cseq:inc(Key);
                    (_)                   -> ok = gcounter_on_cseq:inc_eventual(Key)
               end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),
    stop_readers(Reader),

    receive {compare_failed, A, B} ->
        ?ct_fail("~n~w ~nis not smaller or equals than ~n~w", [A, B])
    after 100 ->
        ok
    end,
    ok.



crdt_gcounter_concurrent_read_monotonic(_Config) ->
    %% starts random number of (strong and eventual) writers
    %% start multiple reader
    %% for each reader, the sequence of seen values should increase
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    %% read for infinity
    ReaderCount = randoms:rand_uniform(2, 10),
    ct:pal("Start ~p readers", [ReaderCount]),
    ReadFun = fun() -> {ok, Val} = gcounter_on_cseq:read_state(Key), Val end,
    CmpFun = fun gcounter:lteq/2,
    Readers = [spawn_monotonic_reader(UnitTestPid, ReadFun, CmpFun) || _ <- lists:seq(1, ReaderCount)],

    %% Start writer
    Parallel = randoms:rand_uniform(1, 50),
    Count = 10000 div Parallel,
    WriteFun = fun
                    (I) when I div 2 == 0 -> ok = gcounter_on_cseq:inc(Key);
                    (_)                   -> ok = gcounter_on_cseq:inc_eventual(Key)
               end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),
    stop_readers(Readers),

    check_monotonic_reader_failure(),

    ok.

crdt_gcounter_ordered_concurrent_read(_Config) ->
    %% Starts random number of writer and two readers
    %% For two concurrent reads returning r1 and r2, it must alsways hold that
    %% r1 <= r2 or r2 <= r1. (of course, the same must hold for non-concurrent reads)
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    %% start two readers which will report their read results back to main process
    ReaderCount = randoms:rand_uniform(2, 5),
    ct:pal("Start ~p readers", [ReaderCount]),
    ReaderPids =
        [spawn(
           fun() ->
                   Loop = fun(F) ->
                            receive
                                {reader_done} ->
                                    UnitTestPid ! {reader_terminated}
                            after 0 ->
                                {ok, Result} = gcounter_on_cseq:read_state(Key),
                                UnitTestPid ! {testreturn, Id, Result},
                                F(F)
                            end
                          end,
                   Loop(Loop)
           end)
        || Id <- lists:seq(1, ReaderCount)],

    %% Start writers
    WriterCount = randoms:rand_uniform(1, 20),
    Count = 5000 div WriterCount,
    WriteFun = fun
                    (I) when I div 2 == 0 -> ok = gcounter_on_cseq:inc(Key);
                    (_)                   -> ok = gcounter_on_cseq:inc_eventual(Key)
               end,
    _ = spawn_writers(UnitTestPid, WriterCount, Count, WriteFun),
    wait_writers_completion(WriterCount),
    stop_readers(ReaderPids),

    ReadResults = [
                    begin
                        Loop = fun(F, Collected) ->
                                    receive {testreturn, Id, Result} ->
                                        F(F, [Result | Collected])
                                    after 0 ->
                                        Collected
                                    end
                               end,
                        lists:reverse(Loop(Loop, []))
                    end
                  || Id <- lists:seq(1, ReaderCount)],
    %% check each pair of returns... it should be enough to only check a subset
    %% of pairs but this is good enough for now
    ct:pal("Check if all ~p reads preformend can be ordered...", [length(lists:flatten(ReadResults))]),
    _ = [begin
            {L1, L2} = {lists:nth(L1Idx, ReadResults), lists:nth(L2Idx, ReadResults)},
            [
             ?equals_w_note(gcounter:lteq(E1, E2) orelse gcounter:lteq(E2, E1), true,
                            lists:flatten(io_lib:format("(E1: ~p, E2: ~p)", [E1, E2])))
            || E1 <- L1, E2 <- L2]
         end
        || L1Idx <- lists:seq(1, ReaderCount), L2Idx <- lists:seq(1, ReaderCount), L1Idx =< L2Idx],

    ok.

crdt_proto_sched_write(_Config) ->
    Key = randoms:getRandomString(),
    TraceId = default,
    UnitTestPid = self(),

    Parallel = randoms:rand_uniform(1, 10),
    Count = 500 div Parallel,
    WriteFun = fun(_I) -> ok = gcounter_on_cseq:inc(Key) end,

    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun, TraceId),
    proto_sched:thread_num(Parallel, TraceId),
    ?ASSERT(not proto_sched:infected()),
    wait_writers_completion(Parallel),
    proto_sched:wait_for_end(TraceId),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, TraceId),
    proto_sched:cleanup(TraceId),

    {ok, Result} = gcounter_on_cseq:read(Key),
    ct:pal("gcounter state is: ~p", [gcounter_on_cseq:read_state(Key)]),
    ct:pal("Planned ~p increments, done ~p - discrepancy is NOT ok~n", [Count*Parallel, Result]),
    ?equals(Count*Parallel, Result),

    ok.

crdt_proto_sched_concurrent_read_monotonic(_Config) ->
    %% starts random number of (strong and eventual) writers
    %% start one reader
    %% values read should increase monotonic
    Key = randoms:getRandomString(),
    TraceId = default,
    UnitTestPid = self(),

    %% read for infinity
    ParallelReader = randoms:rand_uniform(1, 5),
    ReadFun = fun() -> {ok, Val} = gcounter_on_cseq:read_state(Key), Val end,
    CmpFun = fun gcounter:lteq/2,
    Readers = [spawn_monotonic_reader(UnitTestPid, ReadFun, CmpFun, TraceId)
               || _ <- lists:seq(1, ParallelReader)],

    %% Start writer
    ParallelWriter = randoms:rand_uniform(1, 10),
    Count = 100 div ParallelWriter,
    WriteFun = fun(_I) -> ok = gcounter_on_cseq:inc(Key) end,
    _ = spawn_writers(UnitTestPid, ParallelWriter, Count, WriteFun, TraceId),

    %% run ptoto sched
    Parallel = ParallelReader + ParallelWriter,
    proto_sched:thread_num(Parallel, TraceId),
    ?ASSERT(not proto_sched:infected()),
    wait_writers_completion(ParallelWriter),
    stop_readers(Readers),

    check_monotonic_reader_failure(),

    proto_sched:wait_for_end(TraceId),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, TraceId),
    proto_sched:cleanup(TraceId),

    ok.

crdt_proto_sched_concurrent_read_ordered(_Config) ->
    %% Starts random number of writer and two readers
    %% For two concurrent reads returning r1 and r2, it must alsways hold that
    %% r1 <= r2 or r2 <= r1. (of course, the same must hold for non-concurrent reads)
    Key = randoms:getRandomString(),
    TraceId = default,
    UnitTestPid = self(),

    %% start readers which will report their read results back to main process
    ReaderCount = randoms:rand_uniform(1, 5),
    ct:pal("Start ~p readers", [ReaderCount]),
    ReaderPids =
        [spawn(
           fun() ->
                   Loop = fun(F) ->
                            receive
                                {reader_done} ->
                                    UnitTestPid ! {reader_terminated}
                            after 0 ->
                                {ok, Result} = gcounter_on_cseq:read_state(Key),
                                UnitTestPid ! {testreturn, Id, Result},
                                F(F)
                            end
                          end,
                   proto_sched:thread_begin(TraceId),
                   Loop(Loop),
                   proto_sched:thread_end(TraceId)
           end)
        || Id <- lists:seq(1, ReaderCount)],

    %% Start writers
    WriterCount = randoms:rand_uniform(1, 20),
    Count = 100 div WriterCount,
    WriteFun = fun
                    (I) when I div 2 == 0 -> ok = gcounter_on_cseq:inc(Key);
                    (_)                   -> ok = gcounter_on_cseq:inc_eventual(Key)
               end,
    _ = spawn_writers(UnitTestPid, WriterCount, Count, WriteFun, TraceId),

    %% start proto_sched
    Parallel = ReaderCount + WriterCount,
    proto_sched:thread_num(Parallel, TraceId),
    ?ASSERT(not proto_sched:infected()),
    wait_writers_completion(WriterCount),
    stop_readers(ReaderPids),

    %% terminate proto_sched
    proto_sched:wait_for_end(TraceId),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, TraceId),
    proto_sched:cleanup(TraceId),

    %% verify result
    ReadResults = [
                    begin
                        Loop = fun(F, Collected) ->
                                    receive {testreturn, Id, Result} ->
                                        F(F, [Result | Collected])
                                    after 0 ->
                                        Collected
                                    end
                               end,
                        lists:reverse(Loop(Loop, []))
                    end
                  || Id <- lists:seq(1, ReaderCount)],
    %% check each pair of returns... it should be enough to only check a subset
    %% of pairs but this is good enough for now
    ct:pal("Check if all ~p reads preformend can be ordered...", [length(lists:flatten(ReadResults))]),
    _ = [begin
            {L1, L2} = {lists:nth(L1Idx, ReadResults), lists:nth(L2Idx, ReadResults)},
            [
             ?equals_w_note(gcounter:lteq(E1, E2) orelse gcounter:lteq(E2, E1), true,
                            lists:flatten(io_lib:format("(E1: ~p, E2: ~p)", [E1, E2])))
            || E1 <- L1, E2 <- L2]
         end
        || L1Idx <- lists:seq(1, ReaderCount), L2Idx <- lists:seq(1, ReaderCount), L1Idx =< L2Idx],

    ok.

crdt_proto_sched_read_your_write(_Config) ->
    %% starts concurrent worker writing/reading repeateadly
    %% each update should be visibile immediatly when using the same worker
    Key = randoms:getRandomString(),
    TraceId = default,
    UnitTestPid = self(),

    %% Start clinets
    Parallel = randoms:rand_uniform(1, 10),
    Count = 100 div Parallel,
    WriteFun = fun (_) ->
                {ok, Old} = gcounter_on_cseq:read(Key),
                ok = gcounter_on_cseq:inc(Key),
                {ok, New} = gcounter_on_cseq:read(Key),
                ?equals(true, Old < New)
               end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun, TraceId),

    proto_sched:thread_num(Parallel, TraceId),
    ?ASSERT(not proto_sched:infected()),
    wait_writers_completion(Parallel),

    proto_sched:wait_for_end(TraceId),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, TraceId),
    proto_sched:cleanup(TraceId),

    ok.

tester_type_check_crdt(_Config) ->
    Count = 10,
    config:write(no_print_ring_data, true),

    tester:register_value_creator({typedef, crdt, update_fun, []},
                                  crdt, tester_create_update_fun, 1),
    tester:register_value_creator({typedef, crdt, query_fun, []},
                                  crdt, tester_create_query_fun, 1),
    tester:register_value_creator({typedef, gcounter, crdt, []},
                                  gcounter, new, 0),
    tester:register_value_creator({typedef, pncounter, crdt, []},
                                  pncounter, new, 0),

    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {crdt, [], []},
          {crdt_beh, [], []},
          {crdt_proposer,
           [
            {start_link, 3},            % starts processes
            {start_gen_component, 5},   % unsupported types
            {on, 2},                    % sends messages
            {read, 5},                  % needs fun as input
            {read_eventual, 5},       % needs fun as input
            {write, 5},                 % needs fun as input
            {write_eventual, 5}       % needs fun as input
           ],
           [
            {add_vote_reply, 1},        % TODO? prevent generating records with undefined fields
            {add_write_reply, 1},       % TODO? prevent generating records with undefined fields
            {add_read_reply, 5},        % needs value matching db_type
            {send_to_all_replicas, 2},  % sends messages
            {send_to_local_replica, 2}, % sends messages
            {start_request, 2},         % sends messages
            {inform_client, 3},         % cannot create valid envelopes
            {inform_client, 2},         % cannot create valid envelopes
            {get_entry, 2},             % needs valid ets:tid(),
            {save_entry, 2},            % needs valid ets:tid(),
            {delete_entry, 2}           % needs valid ets:tid(),
           ]},
          {crdt_acceptor,
           [
            {init, 1},                  % needs to be in a pidgroup for db_name
            {close, 1},                 % needs valid ets:tid()
            {close_and_delete, 1},      % needs valid ets:tid()
            {on, 2},                    % sends messages
            {get_load, 1},              % needs valid ets:tid()
            {set_entry, 2},             % needs valid ets:tid()
            {get_entry, 2},             % needs valid ets:tid()
            {tab2list, 1},              % needs valid ets:tid()
            {tab2list_raw_unittest, 1}  % needs valid ets:tid()
           ],
           [
            {tab2list_raw, 1},          % needs valid ets:tid()
            {msg_update_reply, 3},      % sends messages
            {msg_merge_reply, 2},       % sends messages
            {msg_query_reply, 3},       % sends messages
            {msg_prepare_reply, 5},     % sends messages
            {msg_prepare_deny, 4},      % sends messages
            {msg_vote_deny, 4},         % sends messages
            {msg_vote_reply, 2}         % sends messages
           ]
          },
          {gcounter,
           [
            {update_nth, 3}             % requires args in bounds
           ],
           [
            {update_nth, 4}             % requires args in bounds
           ]
          },
          {gcounter_on_cseq,
           [],
           [{read_helper, 3},           % cannot create funs
            {write_helper, 3}           % cannot create funs
           ]
          },
          {pncounter, [],[] },
          {gcounter_on_cseq,
           [],
           [{read_helper, 3},           % cannot create funs
            {write_helper, 3}           % cannot create funs
           ]
          }
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],

    tester:unregister_value_creator({typedef, crdt, query_fun, []}),
    tester:unregister_value_creator({typedef, crdt, update_fun, []}),
    tester:unregister_value_creator({typedef, gcounter, crdt, []}),
    tester:unregister_value_creator({typedef, pncounter, crdt, []}),

    true.

-spec spawn_writers(pid(), non_neg_integer(), non_neg_integer(), fun((non_neg_integer()) -> ok)) -> [pid()].
spawn_writers(UnitTestPid, NumberOfWriters, IterationsPerWriter, WriteFun) ->
    spawn_writers(UnitTestPid, NumberOfWriters, IterationsPerWriter, WriteFun, none).

-spec spawn_writers(pid(), non_neg_integer(), non_neg_integer(), fun((non_neg_integer()) -> ok),
                    atom() | none) -> [pid()].
spawn_writers(UnitTestPid, NumberOfWriters, IterationsPerWriter, WriteFun, ProtoSchedTraceId) ->
    ct:pal("Starting concurrent writers: ~p~n"
           "Performing iterations: ~p~n",
           [NumberOfWriters, IterationsPerWriter]),
    [spawn(
        fun() ->
            case ProtoSchedTraceId of
                none ->
                    _ = [WriteFun(I) || I <- lists:seq(1, IterationsPerWriter)];
                TraceId ->
                    proto_sched:thread_begin(TraceId),
                    _ = [WriteFun(I) || I <- lists:seq(1, IterationsPerWriter)],
                    proto_sched:thread_end(TraceId)
            end,
            UnitTestPid ! {done}
        end)
     || _Nth <- lists:seq(1, NumberOfWriters)].


-spec wait_writers_completion(non_neg_integer()) -> ok.
wait_writers_completion(NumberOfWriter) ->
    [receive {done} ->
        ct:pal("Finished ~p/~p.~n", [Nth, NumberOfWriter]),
        ok
    end || Nth <- lists:seq(1, NumberOfWriter)],
    ok.

-spec stop_readers(pid() | [pid()]) -> ok.
stop_readers(Reader) when is_pid(Reader) ->
    stop_readers([Reader]);
stop_readers(Readers) ->
    [Reader ! {reader_done} || Reader <- Readers],
    [receive {reader_terminated} -> ok end || _ <- Readers].

-spec spawn_monotonic_reader(pid(), fun(() -> crdt:crdt()), fun((term(), term()) -> boolean())) -> pid().
spawn_monotonic_reader(UnitTestPid, ReadFun, LTEQCompareFun) ->
    spawn_monotonic_reader(UnitTestPid, ReadFun, LTEQCompareFun, none).

-spec spawn_monotonic_reader(pid(), fun(() -> crdt:crdt()), fun((term(), term()) -> boolean()),
                             atom() | none ) -> pid().
spawn_monotonic_reader(UnitTestPid, ReadFun, LTEQCompareFun, TraceId) ->
    ct:pal("Starting monotonic reader..."),
    spawn(fun() ->
        Init = ReadFun(),
        Loop =
            fun(F, Prev) ->
                receive {reader_done} ->
                    UnitTestPid ! {reader_terminated}
                after 0 ->
                    V = ReadFun(),
                    case LTEQCompareFun(Prev, V) of
                        true ->
                            F(F, V);
                        false ->
                            ct:pal("~n~w ~nis not smaller or equals than ~n~w", [Prev, V]),
                            UnitTestPid ! {compare_failed, Prev, V},
                            F(F, V)
                    end
                end
            end,
        case TraceId of
            none ->  Loop(Loop, Init);
            TraceId ->
                proto_sched:thread_begin(TraceId),
                Loop(Loop, Init),
                proto_sched:thread_end(TraceId)
        end
    end).

-spec check_monotonic_reader_failure() -> ok.
check_monotonic_reader_failure() ->
    receive {compare_failed, A, B} ->
        ?ct_fail("~n~w ~nis not smaller or equals than ~n~w", [A, B])
    after 100 ->
        ok
    end.


