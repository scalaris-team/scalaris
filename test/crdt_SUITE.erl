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

-define(NUM_REPEATS, 5).
-define(RANDOMIZE_RING, true).
-define(CMD_PER_TEST, 1000).
-define(CMD_PER_PROTO_TEST, 100).
-define(IMPLEMENTATIONS,
    [%zheng, not working properly yet
    wf_crdt_paxos,
    crdt_paxos]).
-define(DEFAULT_IMPLEMENTATION, crdt_paxos).


all()   -> 
    [
        {group, Implementation}
    || Implementation <- ?IMPLEMENTATIONS] ++ [tester_type_check_crdt].

groups() ->
    [{I, [],
        [sanity,
            {gcounter_group, [sequence, {repeat, ?NUM_REPEATS}],
             [
                crdt_gcounter_inc,
                crdt_gcounter_read_your_write,
                crdt_gcounter_read_monotonic,
                crdt_gcounter_read_monotonic2,
                crdt_gcounter_concurrent_read_monotonic,
                crdt_gcounter_ordered_concurrent_read,
                crdt_submit_update_command_list,
                crdt_submit_query_command_list
             ]},
            {pncounter_group, [sequence, {repeat, ?NUM_REPEATS}],
             [
              crdt_pncounter_banking
             ]},
            {optorset_group, [sequence, {repeat, ?NUM_REPEATS}],
             [
              crdt_optorset_lteq
             ]},
            {proto_sched_group, [sequence, {repeat, ?NUM_REPEATS}],
             [
              crdt_proto_sched_write,
              crdt_proto_sched_concurrent_read_monotonic,
              crdt_proto_sched_concurrent_read_ordered,
              crdt_proto_sched_read_your_write
            ]}
        ]
    } || I <- ?IMPLEMENTATIONS].

suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) ->
    case lists:member(Group, ?IMPLEMENTATIONS) of
        true -> [{crdt_rsm, Group} | Config];
        false -> Config
    end.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    {crdt_rsm, RSM} =
        case lists:keyfind(crdt_rsm, 1, Config) of
            false -> {crdt_rsm, ?DEFAULT_IMPLEMENTATION};
            Any -> Any
        end,
    {Size, R} =
        case ?RANDOMIZE_RING of
            true ->
                {randoms:rand_uniform(2, 8), randoms:rand_uniform(2, 8)};
            false ->
                {3, 3}
        end,

    unittest_helper:make_ring(Size, [{config, [
                                        {log_path, PrivDir},
                                        {replication_factor, R},
                                        {crdt_rsm, RSM},
                                        {ordered_links, false}]
                                    }]),

    ct:pal("Start test with ringsize ~p, replication factor ~p and CRDT implementation ~p",
        [Size, R, RSM]),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

sanity(_Config) ->

    Key = randoms:getRandomString(),

    ok = gcounter_on_cseq:inc(Key),
    {ok, Result} = gcounter_on_cseq:read(Key),

    ct:pal("gcounter state is: ~p", [gcounter_on_cseq:read_state(Key)]),
    ?equals(Result, 1),
    ok.

crdt_submit_update_command_list(_Config) ->
    %% submit a list of update commands
    Key = randoms:getRandomString(),

    Count = randoms:rand_uniform(1, ?CMD_PER_TEST),
    ct:pal("Submitting lists of ~p updates", [Count]),
    UpdateList =
        [fun(ReplicaId, Crdt) ->
            gcounter:update_add(ReplicaId, I, Crdt)
         end || I <- lists:seq(1, Count)],
    ok = crdt_on_cseq:write(Key, gcounter, UpdateList),

    {ok, Result} = gcounter_on_cseq:read(Key),
    ct:pal("gcounter state is: ~p", [gcounter_on_cseq:read_state(Key)]),
    ?equals(Count*(Count+1) div 2, Result).

crdt_submit_query_command_list(_Config) ->
    %% Check that query ordering is preserverd when submitting a list of
    %% queries
    Key = randoms:getRandomString(),

    %% filling counter
    Count = randoms:rand_uniform(1, 1000),
    Update = fun(RepId, Crdt) -> gcounter:update_add(RepId, Count, Crdt) end,
    ok = crdt_on_cseq:write(Key, gcounter, Update),

    %% sanity check
    {ok, Result} = gcounter_on_cseq:read(Key),
    ?equals(Count, Result),

    QueryListLength = randoms:rand_uniform(1, ?CMD_PER_TEST div 10),
    QueryList =
        [case randoms:rand_uniform(1, 10000) rem 2 of
            0 ->
                fun crdt:query_noop/1;
            1 ->
               fun gcounter:query_counter/1
        end || _ <- lists:seq(1, QueryListLength)],

    ct:pal("Submitting lists of ~p queries", [QueryListLength]),
    {ok, Results} = crdt_on_cseq:read(Key, gcounter, QueryList),

    %% check that result matches queries
    ?equals(QueryListLength, length(Results)),
    [?equals(Result, lists:sum(lists:flatten([R]))) || R <- Results],

    List = lists:zip(QueryList, Results),
    [?equals(Q =:= fun crdt:query_noop/1, is_list(R)) || {Q, R} <- List].


crdt_gcounter_inc(_Config) ->
    %% start random number of writers
    %% select a key to operate on
    %% start random number of increments (non transactional)
    %% check if number of increments = value in key
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    Parallel = randoms:rand_uniform(2, 10),
    Count = ?CMD_PER_TEST div Parallel,
    WriteFun = fun(_I) -> ok = gcounter_on_cseq:inc(Key) end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),

    {ok, Result} = gcounter_on_cseq:read(Key),
    ct:pal("gcounter state is: ~p", [gcounter_on_cseq:read_state(Key)]),
    ct:pal("Planned ~p increments, done ~p~n", [Count*Parallel, Result]),
    ?equals(Result, Count*Parallel),
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
    Count = ?CMD_PER_TEST div Parallel,
    WriteFun =  fun(_I) ->
                    TransactAmount = randoms:rand_uniform(1, StartMoney),
                    From = lists:nth(randoms:rand_uniform(1, AccountNum+1), Accounts),
                    To = lists:nth(randoms:rand_uniform(1, AccountNum+1), Accounts),
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
    ct:pal("The individual account balances are: ~n~p", [Balances]),
    ct:pal("Start balance: ~p~nEnd balance: ~p~n",
           [TotalMoney, EndMoney]),
    ?equals(TotalMoney, EndMoney),
    ok.

crdt_gcounter_read_your_write(_Config) ->
    %% starts concurrent worker writing/reading repeateadly
    %% each update should be visibile immediatly when using the same worker
    Key = randoms:getRandomString(),
    UnitTestPid = self(),

    %% Start writer
    Parallel = randoms:rand_uniform(20, 21),
    Count = ?CMD_PER_TEST div Parallel,
    WriteFun = fun (_) ->
                    {ok, Old} = gcounter_on_cseq:read(Key),
                    ok = gcounter_on_cseq:inc(Key),
                    {ok, New} = gcounter_on_cseq:read(Key),
                    ?equals(true, Old < New)
               end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),

    ok.

crdt_gcounter_read_monotonic(_Config) ->
    %% starts random number of writers
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
    Count = ?CMD_PER_TEST div Parallel,
    WriteFun = fun (_) ->
                    ok = gcounter_on_cseq:inc(Key)
               end,
    _ = spawn_writers(UnitTestPid, Parallel, Count, WriteFun),
    wait_writers_completion(Parallel),

    %% stop reader
    stop_readers(Reader),
    check_monotonic_reader_failure(),

    ok.

crdt_gcounter_read_monotonic2(_Config) ->
    %% starts random number of writers
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
    Count = ?CMD_PER_TEST div Parallel,
    WriteFun = fun (_) ->
                    ok = gcounter_on_cseq:inc(Key)
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
    %% starts random number of writers
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
    Count = ?CMD_PER_TEST div Parallel,
    WriteFun = fun (_) ->
                    ok = gcounter_on_cseq:inc(Key)
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
    Count = (?CMD_PER_TEST div 10) div WriterCount,
    WriteFun = fun (_) ->
                    ok = gcounter_on_cseq:inc(Key)
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

crdt_optorset_lteq(_Config) ->
    %% Checks if the optimized lteq implementation is equivalent to the paper
    %% version

    %% Generate some Sets
    InitSeeds = 1,
    SetNumber = ?CMD_PER_TEST div 10,

    ct:pal("Generating sets..."),
    ReplicaCount = config:read(replication_factor),
    MaxElement = 50, %% do not choose to large.. no sets will be removed otherwise

    InitSets = [optorset:new() || _ <- lists:seq(1, InitSeeds)],
    Generated = lists:foldl(
                  fun(_, Acc) ->
                        % take a random existing set
                        Idx = randoms:rand_uniform(1, min(length(Acc) + 1, 30)),
                        S = lists:nth(Idx, Acc),

                        % add or remove some random element on a random replica
                        E = randoms:rand_uniform(1, MaxElement + 1),
                        R = randoms:rand_uniform(1, ReplicaCount + 1),
                        NewS =
                            case optorset:query_contains(E, S) of
                                false -> optorset:update_add(R, E, S);
                                true -> optorset:update_remove(E, S)
                            end,
                        [NewS | Acc]
                  end, InitSets, lists:seq(1, SetNumber)),

    %% check all pairs of sets in generated list and look for lteq lteq/2
    %% discrepencies

    ct:pal("Generating set pairs..."),
    Pairs = [{A, B} || A <- Generated, B <- Generated],
    ct:pal("verifying equivalence..."),
    Mismatches = lists:foldl(
                   fun(E={A, B}, Acc) ->
                        R1 = optorset:lteq(A, B),
                        R2 = optorset:lteq2(A, B),
                        case R1 =:= R2 of
                            true -> Acc;
                            false ->
                                ct:pal("ERROR: ~n ~p ~n ~p ~n~n lteq: ~p lteq2: ~p",
                                       [A, B, R1, R2]),
                                [E | Acc]
                        end
                   end, [], Pairs),

    ?equals_w_note(length(Mismatches), 0, "Results form lteq and lteq2 does not match"),

    ok.


crdt_proto_sched_write(_Config) ->
    Key = randoms:getRandomString(),
    TraceId = default,
    UnitTestPid = self(),

    Parallel = randoms:rand_uniform(1, 10),
    Count = ?CMD_PER_PROTO_TEST div Parallel,
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
    ct:pal("Planned ~p increments, done ~p~n", [Count*Parallel, Result]),
    ?equals(Count*Parallel, Result),

    ok.

crdt_proto_sched_concurrent_read_monotonic(_Config) ->
    %% starts random number of writers
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
    Count = ?CMD_PER_PROTO_TEST div ParallelWriter,
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
    Count = ?CMD_PER_PROTO_TEST div WriterCount,
    WriteFun = fun (_) ->
                    gcounter_on_cseq:inc(Key)
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
    Count = ?CMD_PER_PROTO_TEST div Parallel,
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
    Count = 100,
    config:write(no_print_ring_data, true),

    tester:register_value_creator({typedef, crdt, update_fun, []},
                                  crdt, tester_create_update_fun, 1),
    tester:register_value_creator({typedef, crdt, query_fun, []},
                                  crdt, tester_create_query_fun, 1),
    tester:register_value_creator({typedef, gcounter, crdt, []},
                                  gcounter, new, 0),
    tester:register_value_creator({typedef, pncounter, crdt, []},
                                  pncounter, new, 0),
    tester:register_value_creator({typedef, gset, crdt, []},
                                  gset, new, 0),

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
            {write, 5},                 % needs fun as input
            {send_to_all_replicas, 2}  % sends messages
           ],
           [
            {add_read_reply, 5},        % needs value matching db_type
            {send_to_all_replicas, 3},  % sends messages
            {send_to_local_replica, 2}, % sends messages
            {send_to_local_replica, 3}, % sends messages
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
            {msg_prepare_reply, 5},     % sends messages
            {msg_prepare_deny, 4},      % sends messages
            {msg_vote_deny, 4},         % sends messages
            {msg_vote_reply, 2}         % sends messages
           ]
          },
          {crdt_wait_free_wrapper,
            [
             {init, 1},                 % sends messages
             {start_link, 3},           % starts processes
             {start_gen_component, 5},  % unsupported types
             {on, 2},                   % sends messages
             {read, 5},                 % needs fun as input
             {write, 5}                 % needs fun as input
            ],
            [
             {get_field, 2},            %% needs in bounds index
             {set_field, 3},            %% needs in bounds index
             {add_to_buffer, 5},        %% needs in bounds index
             {get_pstate, 2},           % needs valid ets:tid()
             {save_pstate, 3},          % needs valid ets:tid()
             {send_to_all_proposers, 2}, % sends messages
             {notify_waiting_progress, 2}, % sends messages
             {inform_all_clients, 2},   % sends messages
             {inform_client, 2},        % sends messagse
             {inform_client, 3}         % sends messagse
            ]
          },
          {gla_proposer,
           [
            {start_link, 3},            % starts processes
            {start_gen_component, 5},   % unsupported types
            {on, 2},                    % sends messages
            {read, 5},                  % needs fun as input
            {write, 5}                  % needs fun as input
           ],
           [
            {send_to_all_replicas, 2},  % sends messages
            {send_to_all_replicas, 3},  % sends messages
            {start_request, 2},         % sends messages
            {inform_client, 3},         % cannot create valid envelopes
            {inform_client, 2},         % cannot create valid envelopes
            {get_entry, 2},             % needs valid ets:tid(),
            {save_entry, 2}             % needs valid ets:tid(),
           ]},
          {gla_acceptor,
           [
            {init, 1},                  % needs to be in a pidgroup for db_name
            {close, 1},                 % needs valid ets:tid()
            {close_and_delete, 1},      % needs valid ets:tid()
            {on, 2},                    % sends messages
            {set_entry, 2},             % needs valid ets:tid()
            {get_entry, 2}              % needs valid ets:tid()
           ],
           [
            {msg_ack_reply, 3},          % sends messages
            {msg_learner_ack_reply, 6},  % sends messages
            {msg_nack_reply, 4},       % sends messages
            {entry_proposed, 2},
            {entry_update_proposed, 3}
           ]},
          {gcounter,
           [
            {update_nth, 3}             % requires args in bounds
           ],
           [
            {update_nth, 4}             % requires args in bounds
           ]
          },
          {pncounter, [],[] },
          {gset,
           [{exists,2},
            {fold,3}
           ],
           []
          }
          %% cannot test both pncounter_on_cseq and gcounter_on_cseq at the same time,
          %% as this would cause a crash if the same key is used for both
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],

    tester:unregister_value_creator({typedef, crdt, query_fun, []}),
    tester:unregister_value_creator({typedef, crdt, update_fun, []}),
    tester:unregister_value_creator({typedef, gcounter, crdt, []}),
    tester:unregister_value_creator({typedef, pncounter, crdt, []}),
    tester:unregister_value_creator({typedef, gset, crdt, []}),

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
            _ = case ProtoSchedTraceId of
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
    _ = [Reader ! {reader_done} || Reader <- Readers],
    [receive {reader_terminated} -> ok end || _ <- Readers],
    ok.

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

