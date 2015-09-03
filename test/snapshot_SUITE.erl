% @copyright 2012-2015 Zuse Institute Berlin

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

%%% @author Stefan Keidel <keidel@informatik.hu-berlin.de>
%%% @doc    Snapshot algorithm unit tests
%%% @end
%%% @version $Id$
-module(snapshot_SUITE).
-author('keidel@informatik.hu-berlin.de').
-compile(export_all).
-include("unittest.hrl").
-include("scalaris.hrl").

-vsn('$Id$').

%% all() -> [test_tx_snapshot_slide_interleave || _X <- lists:seq(1, 100)].
all() ->
    [{group, without_ring},
     {group, with_ring}
    ].

groups() ->
    [
     {without_ring, [],
      [
       test_copy_value_to_snapshot_table, test_set_get_for_snapshot_table,
       test_delete_from_snapshot_table, test_init_snapshot, test_delete_snapshot,
       tester_get_snapshot_data,
       tester_add_snapshot_data,
       %% rdht_tx_read
       test_rdht_tx_read_validate_should_abort, test_rdht_tx_read_validate_should_prepare,
       test_rdht_tx_read_validate_db_copy, test_rdht_tx_read_commit_with_snap_1,
       test_rdht_tx_read_commit_with_snap_2, test_rdht_tx_read_commit_without_snap,
       %% rdht_tx_write
       test_rdht_tx_write_validate_should_abort, test_rdht_tx_write_validate_should_prepare,
       test_rdht_tx_write_validate_db_copy, test_rdht_tx_write_commit_with_snap,
       test_rdht_tx_write_commit_without_snap, test_rdht_tx_write_abort_without_snap,
       test_rdht_tx_write_abort_with_snap,
       %% lock counting
       test_lock_counting_on_live_db]},

     {with_ring, [],
      [%% integration
       test_basic_race_multiple_snapshots, test_single_snapshot_call,
       test_spam_transactions_and_snapshots_on_fully_joined,
       test_tx_snapshot_slide_interleave,
       %% misc
       bench_increment]}
    ].

suite() -> [ {timetrap, {seconds, 45}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(without_ring, Config) ->
    unittest_helper:start_minimal_procs(Config, [], false);
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(without_ring, Config) ->
    unittest_helper:stop_minimal_procs(Config);
end_per_group(_GroupName, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    % for unit tests without a ring, we need to be in a pid_group:
    case proplists:get_value(name, proplists:get_value(tc_group_properties, Config, []), none) of
        without_ring -> pid_groups:join_as(ct_tests, ?MODULE);
        _ -> ok
    end,
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

-define(KEY(Key), ?RT:hash_key(Key)).
-define(VALUE(Val), rdht_tx:encode_value(Val)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% snapshot-related DB API tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec test_copy_value_to_snapshot_table(any()) -> ok.
test_copy_value_to_snapshot_table(_Config) ->
    InitDb = db_dht:new(db_dht),
    Db = db_dht:init_snapshot(InitDb),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 0),
    Db = db_dht:set_entry(Db, EntryFoo),
    Db = db_dht:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    Db = db_dht:set_entry(Db, db_entry:new(?KEY("123"), ?VALUE("456"), 0)),
    Db = db_dht:copy_value_to_snapshot_table(Db, ?KEY("foo")),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("key")), db_entry:new(?KEY("key"))),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("123")), db_entry:new(?KEY("123"))),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("bar")), db_entry:new(?KEY("bar"))),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("foo")), EntryFoo),
    ok.

-spec test_set_get_for_snapshot_table(any()) -> ok.
test_set_get_for_snapshot_table(_Config) ->
    InitDb = db_dht:new(db_dht),
    Db = db_dht:init_snapshot(InitDb),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 5),
    Db = db_dht:set_snapshot_entry(Db, EntryFoo),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("key")), db_entry:new(?KEY("key"))),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("foo")), EntryFoo),
    ok.

-spec test_delete_from_snapshot_table(any()) -> ok.
test_delete_from_snapshot_table(_Config) ->
    InitDb = db_dht:new(db_dht),
    Db = db_dht:init_snapshot(InitDb),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 5),
    Db = db_dht:set_snapshot_entry(Db, EntryFoo),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("foo")), EntryFoo),
    Db = db_dht:delete_snapshot_entry(Db, db_entry:new(?KEY("foo"), ?VALUE("some_val"), 4711)),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("foo")), db_entry:new(?KEY("foo"))),
    Db = db_dht:set_snapshot_entry(Db, EntryFoo),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("foo")), EntryFoo),
    Db = db_dht:delete_snapshot_entry_at_key(Db, ?KEY("foo")),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("foo")), db_entry:new(?KEY("foo"))),
    ok.

-spec test_init_snapshot(any()) -> ok.
test_init_snapshot(_Config) ->
    InitDb = db_dht:new(db_dht),
    Db = db_dht:init_snapshot(InitDb),
    Db = db_dht:set_snapshot_entry(Db, db_entry:new(?KEY("foo"), ?VALUE("bar"), 5)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 4711),
    Db = db_dht:set_entry(Db, EntryKey),
    Db = db_dht:copy_value_to_snapshot_table(Db, ?KEY("key")),
    ClearDb = db_dht:init_snapshot(Db),
    ?equals(db_dht:get_snapshot_entry(ClearDb, ?KEY("foo")), db_entry:new(?KEY("foo"))),
    ?equals(db_dht:get_snapshot_entry(ClearDb, ?KEY("key")), db_entry:new(?KEY("key"))),
    ?equals(db_dht:get_entry(ClearDb, ?KEY("key")), EntryKey),
    ClearDb = db_dht:copy_value_to_snapshot_table(ClearDb, ?KEY("key")),
    ?equals(db_dht:get_snapshot_entry(ClearDb, ?KEY("key")), EntryKey),
    ok.

-spec test_delete_snapshot(any()) -> ok.
test_delete_snapshot(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    Db = db_dht:set_snapshot_entry(Db, db_entry:new(?KEY("foo"), ?VALUE("bar"), 5)),
    DelDb = db_dht:delete_snapshot(Db),
    ?equals(db_dht:snapshot_is_running(DelDb), false),
    ok.

tester_get_snapshot_data(_Conf) ->
    tester:test(?MODULE, test_get_snapshot_data, 2, 1000, [{threads, 2}]),
    ok.

-spec test_get_snapshot_data(db_dht:db_as_list(), intervals:interval()) -> true.
test_get_snapshot_data(Data, Interval) ->
    CleanData = unittest_helper:scrub_data(Data),
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    LoadedDB = db_dht:add_snapshot_data(Db, Data),
    All = db_dht:get_snapshot_data(LoadedDB),
    ?equals(length(All), length(CleanData)),
    ExpPart = lists:foldl(
            fun(Entry, Acc) ->
                    case intervals:in(db_entry:get_key(Entry), Interval) of
                        true ->
                            [Entry | Acc];
                        _ ->
                            Acc
                    end
            end, [], CleanData),
    Part = db_dht:get_snapshot_data(LoadedDB, Interval),
    ct:pal("data: ~p~ninterval: ~p~nexppart: ~p~npart: ~p~n",
          [Data, Interval, ExpPart, Part]),
    ?equals(length(Part), length(ExpPart)),
    db_dht:close(LoadedDB),
    true.

tester_add_snapshot_data(_Conf) ->
    tester:test(?MODULE, test_add_snapshot_data, 1, 1000, [{threads, 2}]),
    ok.

-spec test_add_snapshot_data(Data::db_dht:db_as_list()) -> true.
test_add_snapshot_data(Data) ->
    CleanData = unittest_helper:scrub_data(Data),
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    NewDB = db_dht:add_snapshot_data(Db, CleanData),
    ?equals(length(db_dht:get_snapshot_data(NewDB)), length(CleanData)),
    db_dht:close(NewDB),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% snapshot-related local tx operation tests (validate, commit, abort)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% rdht_tx_read

% snapshot number in entry less than local number -> abort
-spec test_rdht_tx_read_validate_should_abort(any()) -> ok.
test_rdht_tx_read_validate_should_abort(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    Db = db_dht:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("new_val")),
    {_NewDb, Vote} = rdht_tx_read:validate(Db, 5, TLogEntry),
    ?equals(Vote, ?abort),
    ok.

% snapshot number in entry equals local number -> prepared
-spec test_rdht_tx_read_validate_should_prepare(any()) -> ok.
test_rdht_tx_read_validate_should_prepare(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    Db = db_dht:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 4711, ?value, ?VALUE("new_val")),
    {_NewDb, Vote} = rdht_tx_read:validate(Db, 4711, TLogEntry),
    ?equals(Vote, ?prepared),
    ok.

% if a snapshot is running, locking should only occur in the live db
% -> "old" entries should be copied to the snapshot-db in copy-on-write fashion
-spec test_rdht_tx_read_validate_db_copy(any()) -> ok.
test_rdht_tx_read_validate_db_copy(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    Db = db_dht:set_entry(Db, EntryKey),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 2, ?ok, 1, ?value, ?VALUE("new_val")),
    {NewDb, Vote} = rdht_tx_read:validate(Db, 1, TLogEntry),
    ct:pal("Entry: ~p~nTLog:~p~nNewDB: ~p~nVote: ~p", [EntryKey, TLogEntry, NewDb, Vote]),
    ?equals(db_dht:get_snapshot_entry(NewDb, ?KEY("key")),
            EntryKey), % no readlocks in snap db
    ?equals(db_dht:get_entry(NewDb, ?KEY("key")),
            db_entry:inc_readlock(EntryKey)), % 1 readlock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2) , changes
% (in this case: decreasing the readlock count) should be applied to both dbs.
-spec test_rdht_tx_read_commit_with_snap_1(any()) -> ok.
test_rdht_tx_read_commit_with_snap_1(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    TmpDb = db_dht:set_entry(Db, db_entry:inc_readlock(EntryKey)),
    NewDb = db_dht:set_snapshot_entry(TmpDb, db_entry:inc_readlock(EntryKey)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_read:commit(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(db_dht:get_snapshot_entry(CommitDb, ?KEY("key")),
            EntryKey), % no readlocks in snap db
    ?equals(db_dht:get_entry(CommitDb, ?KEY("key")),
            EntryKey), % no readlocks in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2) , changes
% (in this case: decreasing the readlock count) should be applied to both dbs.
% here (as opposed to the test above) is no action needed on the snapshot db
% because there is no cow-value for this key in the snapshot db. the "live" value is valid for both!
-spec test_rdht_tx_read_commit_with_snap_2(any()) -> ok.
test_rdht_tx_read_commit_with_snap_2(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    NewDb = db_dht:set_entry(Db, db_entry:inc_readlock(EntryKey)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_read:commit(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(db_dht:get_snapshot_entry(CommitDb, ?KEY("key")),
            db_entry:new(?KEY("key"))), % no entry in snap db
    ?equals(db_dht:get_entry(CommitDb, ?KEY("key")),
            EntryKey), % no readlocks in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_read_commit_without_snap(any()) -> ok.
test_rdht_tx_read_commit_without_snap(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_RL = db_entry:inc_readlock(EntryKey),
    TmpDb = db_dht:set_entry(Db, EntryKey_RL),
    NewDb = db_dht:set_snapshot_entry(TmpDb, EntryKey_RL),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 2, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_read:commit(NewDb, TLogEntry, ?prepared, 2, 2),
    ?equals(db_dht:get_snapshot_entry(CommitDb, ?KEY("key")),
            EntryKey_RL), % readlock in snap db
    ?equals(db_dht:get_entry(CommitDb, ?KEY("key")),
            EntryKey), % no readlocks in live db
    ok.

% in rdht_tx_read, abort equals commit, so abort doesn't have to be tested!

% rdht_tx_write

% snapshot number in entry less than local number -> abort
-spec test_rdht_tx_write_validate_should_abort(any()) -> ok.
test_rdht_tx_write_validate_should_abort(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    Db = db_dht:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    {_NewDb, Vote} = rdht_tx_write:validate(Db, 5, TLogEntry),
    ?equals(Vote, ?abort),
    ok.

% snapshot number in entry equals local number -> prepared
-spec test_rdht_tx_write_validate_should_prepare(any()) -> ok.
test_rdht_tx_write_validate_should_prepare(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    Db = db_dht:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 4711, ?value, ?VALUE("val")),
    {_NewDb, Vote} = rdht_tx_write:validate(Db, 4711, TLogEntry),
    ?equals(Vote, ?prepared),
    ok.

% if a snapshot is running, locking should only occur in the live db
% -> "old" entries should be copied to the snapshot-db in copy-on-write fashion
-spec test_rdht_tx_write_validate_db_copy(any()) -> ok.
test_rdht_tx_write_validate_db_copy(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    Db = db_dht:set_entry(Db, EntryKey),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    {NewDb, _Vote} = rdht_tx_write:validate(Db, 1, TLogEntry),
    ?equals(db_dht:get_snapshot_entry(NewDb, ?KEY("key")),
            EntryKey), % no lock in snap db
    ?equals(db_dht:get_entry(NewDb, ?KEY("key")),
            db_entry:set_writelock(EntryKey, 1)), % lock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2), changes should be applied to both dbs.
-spec test_rdht_tx_write_commit_with_snap(any()) -> ok.
test_rdht_tx_write_commit_with_snap(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = db_dht:set_entry(Db, EntryKey_WL),
    NewDb = db_dht:set_snapshot_entry(TmpDb, EntryKey_WL),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    NewEntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 2),
    CommitDb = rdht_tx_write:commit(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(db_dht:get_snapshot_entry(CommitDb, ?KEY("key")),
            NewEntryKey), % no lock in snap db
    ?equals(db_dht:get_entry(CommitDb, ?KEY("key")),
            NewEntryKey), % no lock in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_write_commit_without_snap(any()) -> ok.
test_rdht_tx_write_commit_without_snap(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = db_dht:set_entry(Db, EntryKey_WL),
    NewDb = db_dht:set_snapshot_entry(TmpDb, EntryKey_WL),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 2, ?value, ?VALUE("val")),
    NewEntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 2),
    CommitDb = rdht_tx_write:commit(NewDb, TLogEntry, ?prepared, 2, 2),
    ?equals(db_dht:get_snapshot_entry(CommitDb, ?KEY("key")),
            EntryKey_WL), % lock in snap db
    ?equals(db_dht:get_entry(CommitDb, ?KEY("key")),
            NewEntryKey), % no lock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2), changes should be applied to both dbs.
-spec test_rdht_tx_write_abort_with_snap(any()) -> ok.
test_rdht_tx_write_abort_with_snap(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = db_dht:set_entry(Db, EntryKey_WL),
    NewDb = db_dht:set_snapshot_entry(TmpDb, EntryKey_WL),
    ?equals(db_dht:get_entry(Db, ?KEY("key")), EntryKey_WL),
    ?equals(db_dht:get_snapshot_entry(Db, ?KEY("key")), EntryKey_WL),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_write:abort(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(db_dht:get_snapshot_entry(CommitDb, ?KEY("key")),
            EntryKey), % no lock in snap db
    ?equals(db_dht:get_entry(CommitDb, ?KEY("key")),
            EntryKey), % no lock in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_write_abort_without_snap(any()) -> ok.
test_rdht_tx_write_abort_without_snap(_) ->
    Db = db_dht:init_snapshot(db_dht:new(db_dht)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = db_dht:set_entry(Db, EntryKey_WL),
    NewDb = db_dht:set_snapshot_entry(TmpDb, EntryKey_WL),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 2, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_write:abort(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(db_dht:get_snapshot_entry(CommitDb, ?KEY("key")),
            EntryKey_WL), % lock in snap db
    ?equals(db_dht:get_entry(CommitDb, ?KEY("key")),
            EntryKey), % no lock in live db
    ok.

%%%%% lock counting tests

-spec test_lock_counting_on_live_db(any()) -> ok.
test_lock_counting_on_live_db(_) ->
    Db = db_dht:new(db_dht),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 0),
    EntryFoo_WL = db_entry:set_writelock(EntryFoo, 1),
    NewDB = {_, _, {_, LiveLC, _SnapLC}} = db_dht:set_entry(Db, EntryFoo_WL),
    ?equals(LiveLC, 1),
    {_, _, {_, NewLiveLC, _}} = db_dht:set_entry(NewDB, EntryFoo),
    ?equals(NewLiveLC, 0),
    ok.


%%%%% integration tests

-spec test_single_snapshot_call(any()) -> ok.
test_single_snapshot_call(_) ->
    unittest_helper:make_ring(10),
    {_, [{ok}, {ok}, {ok}, {ok}, {ok}]} =
        api_tx:req_list([{write, "A", 1}, {write, "B", 2}, {write, "C", 3},
                         {write, "D", 4}, {commit}]),
    ?equals_pattern(api_tx:get_system_snapshot(),
                    [{_, _, 0}, {_, _, 0}, {_, _, 0}, {_, _, 0}]),
    ok.

-spec test_basic_race_multiple_snapshots(any()) -> ok.
test_basic_race_multiple_snapshots(_) ->
    unittest_helper:make_ring(4),
    {_, [{fail,not_found}, {fail,not_found}, {ok},
         {ok, _A2}, {ok, _A2}, {ok, _A2}, {ok}, {ok}]} =
        api_tx:req_list([{read, "A"}, {read, "B"}, {write, "A", 8},
                         {read, "A"}, {read, "A"}, {read, "A"}, {write, "B", 9}, {commit}]),
    tester:test(api_tx, get_system_snapshot, 0, 100),
    ok.

-spec test_spam_transactions_and_snapshots_on_fully_joined(any()) -> ok.
test_spam_transactions_and_snapshots_on_fully_joined(_) ->
    unittest_helper:make_ring(4),
    ct:pal("wating for fully joined ring...~n~p",
           [unittest_helper:check_ring_size_fully_joined(4)]),

    % apply a couple of transactions beforehand
    tester:test(?MODULE, do_transaction_a, 1, 10),

    ct:pal("spaming transactions..."),
    SpamPid1 = erlang:spawn(fun() ->
                    [do_transaction_a(X) || X <- lists:seq(1, 50)]
          end),
    SpamPid2 = erlang:spawn(fun() ->
                    [do_transaction_b(X) || X <- lists:seq(1, 50)]
          end),

    ct:pal("spaming snapshots..."),
    % spam snapshots here
    tester:test(api_tx, get_system_snapshot, 0, 10),

    ct:pal("waiting for transaction spam..."),
    util:wait_for_process_to_die(SpamPid1),
    util:wait_for_process_to_die(SpamPid2),

    ct:pal("getting one last snapshot..."),
    % get a final snapshot and print it
    Snap = api_tx:get_system_snapshot(),
    ?equals_pattern(Snap, [{_, _, _}, {_, _, _}]),
    ct:pal("snapshot: ~p~n", [Snap]),
    ok.

-spec do_transaction_a(number()) -> any().
do_transaction_a(_I) ->
    %% ct:pal("spaming transaction_a...~p", [I]),
    api_tx:req_list([{read, "B"}, {write, "A", randoms:getRandomInt()}, {write, "B", randoms:getRandomInt()}, {commit}]).

-spec do_transaction_b(number()) -> any().
do_transaction_b(_I) ->
    %% ct:pal("spaming transaction_b...~p", [I]),
    api_tx:req_list([{read, "A"}, {write, "B", randoms:getRandomInt()}, {write, "A", randoms:getRandomInt()}, {commit}]).

-spec test_tx_snapshot_slide_interleave(any()) -> ok.
test_tx_snapshot_slide_interleave(_) ->
    unittest_helper:make_ring(1),
    ct:pal("writing a bit of data...~p", [api_tx:write("A", 1)]),
    ct:pal("wrote...~p", [api_tx:read("A")]),
    timer:sleep(1000),
    Pid = pid_groups:find_a(dht_node),
    ct:pal("setting breakpoint"),
    IsLocked = fun(Msg, _State) ->
            case element(1, Msg) of
                ?tp_do_commit_abort ->
                    comm:send_local(self(), Msg),
                    drop_single;
                _ ->
                    false
            end
    end,
    gen_component:bp_set_cond(Pid, IsLocked, snap_bp),
    ct:pal("starting transaction that triggers breakpoint"),
    TxPid = spawn_link(fun() -> ct:pal("transaction done ~p",
                    [api_tx:req_list([{write, "A", 4}, {commit}])])
            end),
    %% make sure process above gets some excution time
    timer:sleep(100),
    ct:pal("starting snapshot that blocks because of hanging transaction"),
    SnapPid = spawn_link(fun() -> ct:pal("snapshot done ~p",
                                          [api_tx:get_system_snapshot()]) end),
    %% make sure process above gets some excution time
    timer:sleep(100),
    ct:pal("adding node to provoke slide"),
    {[_], []} = api_vm:add_nodes(1),
    ct:pal("wating for fully joined ring...~n~p",
           [unittest_helper:check_ring_size_fully_joined(2)]),
    ct:pal("removing breakpoint ~p", [gen_component:bp_del(Pid, snap_bp)]),
    util:wait_for_process_to_die(TxPid),
    util:wait_for_process_to_die(SnapPid),
    ok.

-spec bench_increment(any()) -> ok.
bench_increment(_) ->
    unittest_helper:make_ring(4),
     SpamPid1 = erlang:spawn(fun() ->
               bench:increment(1, 200)
          end),
    Return = tester:test(api_tx, get_system_snapshot, 0, 20),
    ct:pal("tester return: ~p ~n", [Return]),
    util:wait_for_process_to_die(SpamPid1),
    ok.


