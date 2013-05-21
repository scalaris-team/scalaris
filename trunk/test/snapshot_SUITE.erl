% @copyright 2012 Zuse Institute Berlin

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

all() ->
    [test_copy_value_to_snapshot_table, test_set_get_for_snapshot_table,
     test_delete_from_snapshot_table, test_init_snapshot, test_delete_snapshot,
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
     test_lock_counting_on_live_db,
     %% integration
     test_basic_race_multiple_snapshots, test_single_snapshot_call,
     test_spam_transactions_and_snapshots_on_fully_joined,
     %% misc
     bench_increment
    ].

suite() -> [ {timetrap, {seconds, 30}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

-define(KEY(Key), ?RT:hash_key(Key)).
-define(VALUE(Val), rdht_tx:encode_value(Val)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% snapshot-related DB API tests 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec test_copy_value_to_snapshot_table(any()) -> ok.
test_copy_value_to_snapshot_table(_Config) ->
    InitDb = ?DB:new(),
    Db = ?DB:init_snapshot(InitDb),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 0),
    Db = ?DB:set_entry(Db, EntryFoo),
    Db = ?DB:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    Db = ?DB:set_entry(Db, db_entry:new(?KEY("123"), ?VALUE("456"), 0)),
    Db = ?DB:copy_value_to_snapshot_table(Db, ?KEY("foo")),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("key")), {false, db_entry:new(?KEY("key"))}),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("123")), {false, db_entry:new(?KEY("123"))}),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("bar")), {false, db_entry:new(?KEY("bar"))}),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("foo")), {true, EntryFoo}),
    ok.

-spec test_set_get_for_snapshot_table(any()) -> ok.
test_set_get_for_snapshot_table(_Config) ->
    InitDb = ?DB:new(),
    Db = ?DB:init_snapshot(InitDb),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 5),
    Db = ?DB:set_snapshot_entry(Db, EntryFoo),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("key")), {false, db_entry:new(?KEY("key"))}),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("foo")), {true, EntryFoo}),
    ok.

-spec test_delete_from_snapshot_table(any()) -> ok.
test_delete_from_snapshot_table(_Config) ->
    InitDb = ?DB:new(),
    Db = ?DB:init_snapshot(InitDb),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 5),
    Db = ?DB:set_snapshot_entry(Db, EntryFoo),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("foo")), {true, EntryFoo}),
    Db = ?DB:delete_snapshot_entry(Db, db_entry:new(?KEY("foo"), ?VALUE("some_val"), 4711)),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("foo")), {false, db_entry:new(?KEY("foo"))}),
    Db = ?DB:set_snapshot_entry(Db, EntryFoo),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("foo")), {true, EntryFoo}),
    Db = ?DB:delete_snapshot_entry_at_key(Db, ?KEY("foo")),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("foo")), {false, db_entry:new(?KEY("foo"))}),
    ok.

-spec test_init_snapshot(any()) -> ok.
test_init_snapshot(_Config) ->
    InitDb = ?DB:new(),
    Db = ?DB:init_snapshot(InitDb),
    Db = ?DB:set_snapshot_entry(Db, db_entry:new(?KEY("foo"), ?VALUE("bar"), 5)),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 4711),
    Db = ?DB:set_entry(Db, EntryKey),
    Db = ?DB:copy_value_to_snapshot_table(Db, ?KEY("key")),
    ClearDb = ?DB:init_snapshot(Db),
    ?equals(?DB:get_snapshot_entry(ClearDb, ?KEY("foo")), {false, db_entry:new(?KEY("foo"))}),
    ?equals(?DB:get_snapshot_entry(ClearDb, ?KEY("key")), {false, db_entry:new(?KEY("key"))}),
    ?equals(?DB:get_entry2(ClearDb, ?KEY("key")), {true, EntryKey}),
    ClearDb = ?DB:copy_value_to_snapshot_table(ClearDb, ?KEY("key")),
    ?equals(?DB:get_snapshot_entry(ClearDb, ?KEY("key")), {true, EntryKey}),
    ok.

-spec test_delete_snapshot(any()) -> ok.
test_delete_snapshot(_) ->
    InitDb = ?DB:new(),
    Db = ?DB:init_snapshot(InitDb),
    Db = ?DB:set_snapshot_entry(Db, db_entry:new(?KEY("foo"), ?VALUE("bar"), 5)),
    DelDb = ?DB:delete_snapshot(Db),
    ?equals(?DB:snapshot_is_running(DelDb), false),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% snapshot-related local tx operation tests (validate, commit, abort)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% rdht_tx_read

% snapshot number in entry less than local number -> abort
-spec test_rdht_tx_read_validate_should_abort(any()) -> ok.
test_rdht_tx_read_validate_should_abort(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    Db = ?DB:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("new_val")),
    {_NewDb, Vote} = rdht_tx_read:validate(Db, 5, TLogEntry),
    ?equals(Vote, ?abort),
    ok.

% snapshot number in entry equals local number -> prepared
-spec test_rdht_tx_read_validate_should_prepare(any()) -> ok.
test_rdht_tx_read_validate_should_prepare(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    Db = ?DB:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 4711, ?value, ?VALUE("new_val")),
    {_NewDb, Vote} = rdht_tx_read:validate(Db, 4711, TLogEntry),
    ?equals(Vote, ?prepared),
    ok.

% if a snapshot is running, locking should only occur in the live db
% -> "old" entries should be copied to the snapshot-db in copy-on-write fashion
-spec test_rdht_tx_read_validate_db_copy(any()) -> ok.
test_rdht_tx_read_validate_db_copy(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    Db = ?DB:set_entry(Db, EntryKey),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 2, ?ok, 1, ?value, ?VALUE("new_val")),
    {NewDb, _Vote} = rdht_tx_read:validate(Db, 1, TLogEntry),
    ?equals(?DB:get_snapshot_entry(NewDb, ?KEY("key")),
            {true, EntryKey}), % no readlocks in snap db
    ?equals(?DB:get_entry2(NewDb, ?KEY("key")),
            {true, db_entry:inc_readlock(EntryKey)}), % 1 readlock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2) , changes
% (in this case: decreasing the readlock count) should be applied to both dbs.
-spec test_rdht_tx_read_commit_with_snap_1(any()) -> ok.
test_rdht_tx_read_commit_with_snap_1(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    TmpDb = ?DB:set_entry(Db, db_entry:inc_readlock(EntryKey)),
    NewDb = ?DB:set_snapshot_entry(TmpDb, db_entry:inc_readlock(EntryKey)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_read:commit(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(?DB:get_snapshot_entry(CommitDb, ?KEY("key")),
            {true, EntryKey}), % no readlocks in snap db
    ?equals(?DB:get_entry2(CommitDb, ?KEY("key")),
            {true, EntryKey}), % no readlocks in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2) , changes
% (in this case: decreasing the readlock count) should be applied to both dbs.
% here (as opposed to the test above) is no action needed on the snapshot db
% because there is no cow-value for this key in the snapshot db. the "live" value is valid for both!
-spec test_rdht_tx_read_commit_with_snap_2(any()) -> ok.
test_rdht_tx_read_commit_with_snap_2(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    NewDb = ?DB:set_entry(Db, db_entry:inc_readlock(EntryKey)),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_read:commit(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(?DB:get_snapshot_entry(CommitDb, ?KEY("key")),
            {false, db_entry:new(?KEY("key"))}), % no entry in snap db
    ?equals(?DB:get_entry2(CommitDb, ?KEY("key")),
            {true, EntryKey}), % no readlocks in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_read_commit_without_snap(any()) -> ok.
test_rdht_tx_read_commit_without_snap(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_RL = db_entry:inc_readlock(EntryKey),
    TmpDb = ?DB:set_entry(Db, EntryKey_RL),
    NewDb = ?DB:set_snapshot_entry(TmpDb, EntryKey_RL),
    TLogEntry = tx_tlog:new_entry(?read, ?KEY("key"), 1, ?ok, 2, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_read:commit(NewDb, TLogEntry, ?prepared, 2, 2),
    ?equals(?DB:get_snapshot_entry(CommitDb, ?KEY("key")),
            {true, EntryKey_RL}), % readlock in snap db
    ?equals(?DB:get_entry2(CommitDb, ?KEY("key")),
            {true, EntryKey}), % no readlocks in live db
    ok.

% in rdht_tx_read, abort equals commit, so abort doesn't have to be tested!

% rdht_tx_write

% snapshot number in entry less than local number -> abort
-spec test_rdht_tx_write_validate_should_abort(any()) -> ok.
test_rdht_tx_write_validate_should_abort(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    Db = ?DB:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    {_NewDb, Vote} = rdht_tx_write:validate(Db, 5, TLogEntry),
    ?equals(Vote, ?abort),
    ok.

% snapshot number in entry equals local number -> prepared
-spec test_rdht_tx_write_validate_should_prepare(any()) -> ok.
test_rdht_tx_write_validate_should_prepare(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    Db = ?DB:set_entry(Db, db_entry:new(?KEY("key"), ?VALUE("val"), 1)),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 4711, ?value, ?VALUE("val")),
    {_NewDb, Vote} = rdht_tx_write:validate(Db, 4711, TLogEntry),
    ?equals(Vote, ?prepared),
    ok.

% if a snapshot is running, locking should only occur in the live db
% -> "old" entries should be copied to the snapshot-db in copy-on-write fashion
-spec test_rdht_tx_write_validate_db_copy(any()) -> ok.
test_rdht_tx_write_validate_db_copy(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    Db = ?DB:set_entry(Db, EntryKey),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    {NewDb, _Vote} = rdht_tx_write:validate(Db, 1, TLogEntry),
    ?equals(?DB:get_snapshot_entry(NewDb, ?KEY("key")),
            {true, EntryKey}), % no lock in snap db
    ?equals(?DB:get_entry2(NewDb, ?KEY("key")),
            {true, db_entry:set_writelock(EntryKey, 1)}), % lock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2), changes should be applied to both dbs.
-spec test_rdht_tx_write_commit_with_snap(any()) -> ok.
test_rdht_tx_write_commit_with_snap(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = ?DB:set_entry(Db, EntryKey_WL),
    NewDb = ?DB:set_snapshot_entry(TmpDb, EntryKey_WL),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    NewEntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 2),
    CommitDb = rdht_tx_write:commit(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(?DB:get_snapshot_entry(CommitDb, ?KEY("key")),
            {true, NewEntryKey}), % no lock in snap db
    ?equals(?DB:get_entry2(CommitDb, ?KEY("key")),
            {true, NewEntryKey}), % no lock in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_write_commit_without_snap(any()) -> ok.
test_rdht_tx_write_commit_without_snap(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = ?DB:set_entry(Db, EntryKey_WL),
    NewDb = ?DB:set_snapshot_entry(TmpDb, EntryKey_WL),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 2, ?value, ?VALUE("val")),
    NewEntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 2),
    CommitDb = rdht_tx_write:commit(NewDb, TLogEntry, ?prepared, 2, 2),
    ?equals(?DB:get_snapshot_entry(CommitDb, ?KEY("key")),
            {true, EntryKey_WL}), % lock in snap db
    ?equals(?DB:get_entry2(CommitDb, ?KEY("key")),
            {true, NewEntryKey}), % no lock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2), changes should be applied to both dbs.
-spec test_rdht_tx_write_abort_with_snap(any()) -> ok.
test_rdht_tx_write_abort_with_snap(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = ?DB:set_entry(Db, EntryKey_WL),
    NewDb = ?DB:set_snapshot_entry(TmpDb, EntryKey_WL),
    ?equals(?DB:get_entry2(Db, ?KEY("key")), {true, EntryKey_WL}),
    ?equals(?DB:get_snapshot_entry(Db, ?KEY("key")), {true, EntryKey_WL}),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 1, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_write:abort(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(?DB:get_snapshot_entry(CommitDb, ?KEY("key")),
            {true, EntryKey}), % no lock in snap db
    ?equals(?DB:get_entry2(CommitDb, ?KEY("key")),
            {true, EntryKey}), % no lock in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_write_abort_without_snap(any()) -> ok.
test_rdht_tx_write_abort_without_snap(_) ->
    Db = ?DB:init_snapshot(?DB:new()),
    EntryKey = db_entry:new(?KEY("key"), ?VALUE("val"), 1),
    EntryKey_WL = db_entry:set_writelock(EntryKey, 1),
    TmpDb = ?DB:set_entry(Db, EntryKey_WL),
    NewDb = ?DB:set_snapshot_entry(TmpDb, EntryKey_WL),
    TLogEntry = tx_tlog:new_entry(?write, ?KEY("key"), 1, ?ok, 2, ?value, ?VALUE("val")),
    CommitDb = rdht_tx_write:abort(NewDb, TLogEntry, ?prepared, 1, 2),
    ?equals(?DB:get_snapshot_entry(CommitDb, ?KEY("key")),
            {true, EntryKey_WL}), % lock in snap db
    ?equals(?DB:get_entry2(CommitDb, ?KEY("key")),
            {true, EntryKey}), % no lock in live db
    ok.

%%%%% lock counting tests

-spec test_lock_counting_on_live_db(any()) -> ok.
test_lock_counting_on_live_db(_) ->
    Db = ?DB:new(),
    EntryFoo = db_entry:new(?KEY("foo"), ?VALUE("bar"), 0),
    EntryFoo_WL = db_entry:set_writelock(EntryFoo, 1),
    NewDB = {_, _, {_, LiveLC, _SnapLC}} = ?DB:set_entry(Db, EntryFoo_WL),
    ?equals(LiveLC, 1),
    {_, _, {_, NewLiveLC, _}} = ?DB:set_entry(NewDB, EntryFoo),
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
    ct:pal("wating for fully joind ring...~n~p", 
           [unittest_helper:check_ring_size_fully_joined(4)]),
    
    % apply a couple of transactions beforehand
    tester:test(?MODULE, do_transaction_a, 1, 10),
    
    ct:pal("spaming transactions..."),
    SpamPid1 = erlang:spawn(fun() ->
                    [do_transaction_a(X) || X <- lists:seq(1, 500)]
          end),
    SpamPid2 = erlang:spawn(fun() ->
                    [do_transaction_b(X) || X <- lists:seq(1, 500)]
          end),
    
    ct:pal("spaming snapshots..."),
    % spam snapshots here
    tester:test(api_tx, get_system_snapshot, 0, 100),
    
    ct:pal("waiting for transaction spam..."),
    util:wait_for_process_to_die(SpamPid1),
    util:wait_for_process_to_die(SpamPid2),
    
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

-spec bench_increment(any()) -> ok.
bench_increment(_) ->
    unittest_helper:make_ring(4),
     SpamPid1 = erlang:spawn(fun() ->
               bench:increment(1, 1000)
          end),
    Return = tester:test(api_tx, get_system_snapshot, 0, 200),
    ct:pal("tester return: ~p ~n", [Return]),
    util:wait_for_process_to_die(SpamPid1),
    ok.


