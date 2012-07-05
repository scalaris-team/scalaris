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
%%& @version $Id: snapshot_SUITE.erl 3244 2012-06-05 16:19:11Z stefankeidel85@gmail.com $
-module(snapshot_SUITE).
-author('keidel@informatik.hu-berlin.de').
-compile(export_all).
-include("unittest.hrl").
-include("scalaris.hrl").

-vsn('$Id: snapshot_SUITE.erl 3244 2012-06-05 16:19:11Z stefankeidel85@gmail.com $').

all() ->
    [test_copy_value_to_snapshot_table,test_set_get_for_snapshot_table,
     test_delete_from_snapshot_table,test_init_snapshot,test_delete_snapshot,
     %rdht_tx_read
     test_rdht_tx_read_validate_should_abort,test_rdht_tx_read_validate_should_prepare,
     test_rdht_tx_read_validate_db_copy,test_rdht_tx_read_commit_with_snap_1,
     test_rdht_tx_read_commit_with_snap_2, test_rdht_tx_read_commit_without_snap,
     %rdht_tx_write
     test_rdht_tx_write_validate_should_abort,test_rdht_tx_write_validate_should_prepare,
     test_rdht_tx_write_validate_db_copy,test_rdht_tx_write_commit_with_snap,
     test_rdht_tx_write_commit_without_snap, test_rdht_tx_write_abort_without_snap,
     test_rdht_tx_write_abort_with_snap,
     % lock counting
     test_lock_counting_on_live_db,
     %integration
     test_basic_race_multiple_snapshots, test_single_snapshot_call,test_spam_transactions_and_snapshots
    ].

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

-define(TEST_DB,db_ets).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% snapshot-related DB API tests 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec test_copy_value_to_snapshot_table(any()) -> ok.
test_copy_value_to_snapshot_table(_Config) ->
    InitDb = ?TEST_DB:new(),
    Db = ?TEST_DB:init_snapshot(InitDb),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("foo","bar",0)),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",1)),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("123","456",0)),
    Db = ?TEST_DB:copy_value_to_snapshot_table(Db,"foo"),
    ?equals({false,{"key",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(Db,"key")),
    ?equals({false,{"123",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(Db,"123")),
    ?equals({false,{"bar",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(Db,"bar")),
    ?equals({true,{"foo","bar",false,0,0}},?TEST_DB:get_snapshot_entry(Db,"foo")),
    ok.

-spec test_set_get_for_snapshot_table(any()) -> ok.
test_set_get_for_snapshot_table(_Config) ->
    InitDb = ?TEST_DB:new(),
    Db = ?TEST_DB:init_snapshot(InitDb),
    Db = ?TEST_DB:set_snapshot_entry(Db,db_entry:new("foo","bar",5)),
    ?equals({false,{"key",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(Db,"key")),
    ?equals({true,{"foo","bar",false,0,5}},?TEST_DB:get_snapshot_entry(Db,"foo")),
    ok.

-spec test_delete_from_snapshot_table(any()) -> ok.
test_delete_from_snapshot_table(_Config) ->
    InitDb = ?TEST_DB:new(),
    Db = ?TEST_DB:init_snapshot(InitDb),
    Db = ?TEST_DB:set_snapshot_entry(Db,db_entry:new("foo","bar",5)),
    ?equals({true,{"foo","bar",false,0,5}},?TEST_DB:get_snapshot_entry(Db,"foo")),
    Db = ?TEST_DB:delete_snapshot_entry(Db,db_entry:new("foo","some_val",4711)),
    ?equals({false,{"foo",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(Db,"foo")),
    Db = ?TEST_DB:set_snapshot_entry(Db,db_entry:new("foo","bar",5)),
    ?equals({true,{"foo","bar",false,0,5}},?TEST_DB:get_snapshot_entry(Db,"foo")),
    Db = ?TEST_DB:delete_snapshot_entry_at_key(Db,"foo"),
    ?equals({false,{"foo",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(Db,"foo")),
    ok.

-spec test_init_snapshot(any()) -> ok.
test_init_snapshot(_Config) ->
    InitDb = ?TEST_DB:new(),
    Db = ?TEST_DB:init_snapshot(InitDb),
    Db = ?TEST_DB:set_snapshot_entry(Db,db_entry:new("foo","bar",5)),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",4711)),
    Db = ?TEST_DB:copy_value_to_snapshot_table(Db,"key"),
    ClearDb = ?TEST_DB:init_snapshot(Db),
    ?equals({false,{"foo",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(ClearDb,"foo")),
    ?equals({false,{"key",empty_val,false,0,-1}},?TEST_DB:get_snapshot_entry(ClearDb,"key")),
    ?equals({true,{"key","val",false,0,4711}},?TEST_DB:get_entry2(ClearDb,"key")),
    ClearDb = ?TEST_DB:copy_value_to_snapshot_table(ClearDb,"key"),
    ?equals({true,{"key","val",false,0,4711}},?TEST_DB:get_snapshot_entry(ClearDb,"key")),
    ok.

-spec test_delete_snapshot(any()) -> ok.
test_delete_snapshot(_) ->
    InitDb = ?TEST_DB:new(),
    Db = ?TEST_DB:init_snapshot(InitDb),
    Db = ?TEST_DB:set_snapshot_entry(Db,db_entry:new("foo","bar",5)),
    DelDb = ?TEST_DB:delete_snapshot(Db),
    ?equals(false,?TEST_DB:snapshot_is_running(DelDb)),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% snapshot-related local tx operation tests (validate, commit, abort)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% rdht_tx_read

% snapshot number in entry less than local number -> abort
-spec test_rdht_tx_read_validate_should_abort(any()) -> ok.
test_rdht_tx_read_validate_should_abort(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",1)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_read,"key",1,value,1,"new_val"),
    {_NewDb,Vote} = rdht_tx_read:validate(Db,5,TLogEntry),
    ?equals(Vote,abort),
    ok.

% snapshot number in entry equals local number -> prepared
-spec test_rdht_tx_read_validate_should_prepare(any()) -> ok.
test_rdht_tx_read_validate_should_prepare(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",1)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_read,"key",1,value,4711,"new_val"),
    {_NewDb,Vote} = rdht_tx_read:validate(Db,4711,TLogEntry),
    ?equals(Vote,prepared),
    ok.

% if a snapshot is running, locking should only occur in the live db
% -> "old" entries should be copied to the snapshot-db in copy-on-write fashion
-spec test_rdht_tx_read_validate_db_copy(any()) -> ok.
test_rdht_tx_read_validate_db_copy(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",1)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_read,"key",2,value,1,"new_val"),
    {NewDb,_Vote} = rdht_tx_read:validate(Db,1,TLogEntry),
    SnapEntry = ?TEST_DB:get_snapshot_entry(NewDb,"key"),
    ?equals({true,{"key","val",false,0,1}},SnapEntry), % no readlocks in snap db
    LiveEntry = ?TEST_DB:get_entry2(NewDb,"key"),
    ?equals({true,{"key","val",false,1,1}},LiveEntry), % 1 readlock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2) , changes
% (in this case: decreasing the readlock count) should be applied to both dbs.
-spec test_rdht_tx_read_commit_with_snap_1(any()) -> ok.
test_rdht_tx_read_commit_with_snap_1(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Entry = db_entry:new("key","val",1),
    TmpDb = ?TEST_DB:set_entry(Db,db_entry:inc_readlock(Entry)),
    NewDb = ?TEST_DB:set_snapshot_entry(TmpDb,db_entry:inc_readlock(Entry)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_read,"key",1,value,1,"val"),
    CommitDb = rdht_tx_read:commit(NewDb,TLogEntry,prepared,1,2),
    SnapEntry = ?TEST_DB:get_snapshot_entry(CommitDb,"key"),
    ?equals({true,{"key","val",false,0,1}},SnapEntry), % no readlocks in snap db
    LiveEntry = ?TEST_DB:get_entry2(CommitDb,"key"),
    ?equals({true,{"key","val",false,0,1}},LiveEntry), % no readlocks in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2) , changes
% (in this case: decreasing the readlock count) should be applied to both dbs.
% here (as opposed to the test above) is no action needed on the snapshot db
% because there is no cow-value for this key in the snapshot db. the "live" value is valid for both!
-spec test_rdht_tx_read_commit_with_snap_2(any()) -> ok.
test_rdht_tx_read_commit_with_snap_2(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Entry = db_entry:new("key","val",1),
    NewDb = ?TEST_DB:set_entry(Db,db_entry:inc_readlock(Entry)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_read,"key",1,value,1,"val"),
    CommitDb = rdht_tx_read:commit(NewDb,TLogEntry,prepared,1,2),
    SnapEntry = ?TEST_DB:get_snapshot_entry(CommitDb,"key"),
    ?equals({false,{"key",empty_val,false,0,-1}},SnapEntry), % no entry in snap db
    LiveEntry = ?TEST_DB:get_entry2(CommitDb,"key"),
    ?equals({true,{"key","val",false,0,1}},LiveEntry), % no readlocks in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_read_commit_without_snap(any()) -> ok.
test_rdht_tx_read_commit_without_snap(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Entry = db_entry:new("key","val",1),
    TmpDb = ?TEST_DB:set_entry(Db,db_entry:inc_readlock(Entry)),
    NewDb = ?TEST_DB:set_snapshot_entry(TmpDb,db_entry:inc_readlock(Entry)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_read,"key",1,value,1,"val"),
    CommitDb = rdht_tx_read:commit(NewDb,TLogEntry,prepared,2,2),
    SnapEntry = ?TEST_DB:get_snapshot_entry(CommitDb,"key"),
    ?equals({true,{"key","val",false,1,1}},SnapEntry), % readlock in snap db
    LiveEntry = ?TEST_DB:get_entry2(CommitDb,"key"),
    ?equals({true,{"key","val",false,0,1}},LiveEntry), % no readlocks in live db
    ok.

% in rdht_tx_read, abort equals commit, so abort doesn't have to be tested!

% rdht_tx_write

% snapshot number in entry less than local number -> abort
-spec test_rdht_tx_write_validate_should_abort(any()) -> ok.
test_rdht_tx_write_validate_should_abort(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",1)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_write,"key",1,value,1,"new_val"),
    {_NewDb,Vote} = rdht_tx_write:validate(Db,5,TLogEntry),
    ?equals(Vote,abort),
    ok.

% snapshot number in entry equals local number -> prepared
-spec test_rdht_tx_write_validate_should_prepare(any()) -> ok.
test_rdht_tx_write_validate_should_prepare(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",1)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_write,"key",1,value,4711,"new_val"),
    {_NewDb,Vote} = rdht_tx_write:validate(Db,4711,TLogEntry),
    ?equals(Vote,prepared),
    ok.

% if a snapshot is running, locking should only occur in the live db
% -> "old" entries should be copied to the snapshot-db in copy-on-write fashion
-spec test_rdht_tx_write_validate_db_copy(any()) -> ok.
test_rdht_tx_write_validate_db_copy(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Db = ?TEST_DB:set_entry(Db,db_entry:new("key","val",1)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_write,"key",1,value,1,"new_val"),
    {NewDb,_Vote} = rdht_tx_write:validate(Db,1,TLogEntry),
    SnapEntry = ?TEST_DB:get_snapshot_entry(NewDb,"key"),
    ?equals({true,{"key","val",false,0,1}},SnapEntry), % no lock in snap db
    LiveEntry = ?TEST_DB:get_entry2(NewDb,"key"),
    ?equals({true,{"key","val",true,0,1}},LiveEntry), % lock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2), changes should be applied to both dbs.
-spec test_rdht_tx_write_commit_with_snap(any()) -> ok.
test_rdht_tx_write_commit_with_snap(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Entry = db_entry:new("key","val",1),
    TmpDb = ?TEST_DB:set_entry(Db,db_entry:set_writelock(Entry)),
    NewDb = ?TEST_DB:set_snapshot_entry(TmpDb,db_entry:set_writelock(Entry)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_write,"key",1,value,1,"new_val"),
    CommitDb = rdht_tx_write:commit(NewDb,TLogEntry,prepared,1,2),
    SnapEntry = ?TEST_DB:get_snapshot_entry(CommitDb,"key"),
    ?equals({true,{"key","new_val",false,0,2}},SnapEntry), % no lock in snap db
    LiveEntry = ?TEST_DB:get_entry2(CommitDb,"key"),
    ?equals({true,{"key","new_val",false,0,2}},LiveEntry), % no lock in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_write_commit_without_snap(any()) -> ok.
test_rdht_tx_write_commit_without_snap(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Entry = db_entry:new("key","val",1),
    TmpDb = ?TEST_DB:set_entry(Db,db_entry:set_writelock(Entry)),
    NewDb = ?TEST_DB:set_snapshot_entry(TmpDb,db_entry:set_writelock(Entry)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_write,"key",1,value,2,"new_val"),
    CommitDb = rdht_tx_write:commit(NewDb,TLogEntry,prepared,2,2),
    SnapEntry = ?TEST_DB:get_snapshot_entry(CommitDb,"key"),
    ?equals({true,{"key","val",true,0,1}},SnapEntry), % lock in snap db
    LiveEntry = ?TEST_DB:get_entry2(CommitDb,"key"),
    ?equals({true,{"key","new_val",false,0,2}},LiveEntry), % no lock in live db
    ok.

% for "old" transactions (as in TM snapnr 1 is lesser than local snapnr 2), changes should be applied to both dbs.
-spec test_rdht_tx_write_abort_with_snap(any()) -> ok.
test_rdht_tx_write_abort_with_snap(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Entry = db_entry:new("key","val",1),
    TmpDb = ?TEST_DB:set_entry(Db,db_entry:set_writelock(Entry)),
    NewDb = ?TEST_DB:set_snapshot_entry(TmpDb,db_entry:set_writelock(Entry)),
    ?equals({true,{"key","val",true,0,1}},?TEST_DB:get_entry2(Db,"key")),
    ?equals({true,{"key","val",true,0,1}},?TEST_DB:get_snapshot_entry(Db,"key")),
    TLogEntry = tx_tlog:new_entry(rdht_tx_write,"key",1,value,1,"new_val"),
    CommitDb = rdht_tx_write:abort(NewDb,TLogEntry,prepared,1,2),
    SnapEntry = ?TEST_DB:get_snapshot_entry(CommitDb,"key"),
    ?equals({true,{"key","val",false,0,1}},SnapEntry), % no lock in snap db
    LiveEntry = ?TEST_DB:get_entry2(CommitDb,"key"),
    ?equals({true,{"key","val",false,0,1}},LiveEntry), % no lock in live db
    ok.

% "new" transaction -> changes only on live db, snapshot db remains untouched
-spec test_rdht_tx_write_abort_without_snap(any()) -> ok.
test_rdht_tx_write_abort_without_snap(_) ->
    Db = ?TEST_DB:init_snapshot(?TEST_DB:new()),
    Entry = db_entry:new("key","val",1),
    TmpDb = ?TEST_DB:set_entry(Db,db_entry:set_writelock(Entry)),
    NewDb = ?TEST_DB:set_snapshot_entry(TmpDb,db_entry:set_writelock(Entry)),
    TLogEntry = tx_tlog:new_entry(rdht_tx_write,"key",1,value,2,"new_val"),
    CommitDb = rdht_tx_write:abort(NewDb,TLogEntry,prepared,1,2),
    SnapEntry = ?TEST_DB:get_snapshot_entry(CommitDb,"key"),
    ?equals({true,{"key","val",true,0,1}},SnapEntry), % lock in snap db
    LiveEntry = ?TEST_DB:get_entry2(CommitDb,"key"),
    ?equals({true,{"key","val",false,0,1}},LiveEntry), % no lock in live db
    ok.

%%%%% lock counting tests

-spec test_lock_counting_on_live_db(any()) -> ok.
test_lock_counting_on_live_db(_) ->
    Db = ?TEST_DB:new(),
    Entry = db_entry:new("foo","bar",0),
    WriteLockEntry = db_entry:set_writelock(Entry),
    NewDB = {_,_,{_,LiveLC,_SnapLC}} = ?TEST_DB:set_entry(Db,WriteLockEntry),
    ?equals(LiveLC,1),
    {_,_,{_,NewLiveLC,_}} = ?TEST_DB:set_entry(NewDB,Entry),
    ?equals(NewLiveLC,0),
    ok.
    

%%%%% integration tests

-spec test_single_snapshot_call(any()) -> ok.
test_single_snapshot_call(_) ->
    unittest_helper:make_ring(10),
    api_tx:req_list([{write,"A",1},{write,"B",2},{write,"C",3},{write,"D",4},{commit}]),
    ActualSnap = api_tx:get_system_snapshot(),                        
    ?equals_pattern(ActualSnap,[{_,_,0},{_,_,0},{_,_,0},{_,_,0}]),
    ok.
    
-spec test_basic_race_multiple_snapshots(any()) -> ok.
test_basic_race_multiple_snapshots(_) ->
    unittest_helper:make_ring(4),
    api_tx:req_list([{read,"A"},{read,"B"},{write,"A",8},
                     {read,"A"},{read,"A"},{read,"A"},{write,"B", 9},{commit}]),
    tester:test(api_tx, get_system_snapshot, 0, 100),
    ok.

-spec test_spam_transactions_and_snapshots(any()) -> ok.
test_spam_transactions_and_snapshots(_) ->
    unittest_helper:make_ring(4),
    
    % apply a couple of transactions beforehand
    tester:test(?MODULE, do_transaction, 0, 100),
    
    % spam transactions in sepreate process
    SpamPid = erlang:spawn(fun() ->
               tester:test(?MODULE, do_transaction, 0, 100)
          end),
    
    % spam snapshots here
    tester:test(api_tx, get_system_snapshot, 0, 100),
    
    % wait for transaction spam
    util:wait_for_process_to_die(SpamPid),
    
    % get a final snapshot and print it
    Snap = api_tx:get_system_snapshot(),
    ?equals_pattern(Snap,[{_,_,_},{_,_,_}]),
    ct:pal("snapshot: ~p~n",[Snap]),
    ok.

-spec do_transaction() -> any().
do_transaction() ->
    api_tx:req_list([{read,"A"},{read,"B"},{write,"A",randoms:getRandomInt()},
                     {read,"A"},{read,"A"},{read,"A"},{write,"B", randoms:getRandomInt()},
                     {commit}]).
 


