% @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc Unit tests for transactions under churn
%% @end
-module(tx_tm_rtm_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).
-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [abort_prepared_r,
     abort_prepared_w,
     abort_prepared_rc,
     abort_prepared_rmc,
     abort_prepared_wmc,
     tm_crash,
     tp_crash,
     all_tp_crash
    ].

suite() -> [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    unittest_helper:make_ring_with_ids(fun() -> ?RT:get_replica_keys(?RT:hash_key("0")) end, [{config, [{log_path, PrivDir}]}]),
    Config2.

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    _ = case TestCase of
            tm_crash ->
                %% stop ring from previous test case (may ran into a timeout)
                unittest_helper:stop_ring(),
                {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
                unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
                timer:sleep(500);
            tp_crash ->
                %% stop ring from previous test case (may ran into a timeout)
                unittest_helper:stop_ring(),
                {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
                unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
                timer:sleep(500);
            all_tp_crash ->
                %% stop ring from previous test case (may ran into a timeout)
                unittest_helper:stop_ring(),
                {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
                unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
                timer:sleep(500);
            _ -> ok
        end,
    Config.

end_per_testcase(TestCase, Config) ->
    _ = case TestCase of
            tm_crash ->
                unittest_helper:stop_ring();
            tp_crash ->
                unittest_helper:stop_ring();
            all_tp_crash ->
                unittest_helper:stop_ring();
            _ -> ok
        end,
    Config.

causes() -> [readlock, writelock, versiondec, versioninc, none].

abort_prepared_r(_) ->
    %% check all combinations of abort / prepared and the corresponding
    %% tx decisions
    %% modify DB and then do operations on the changed DB
    %% quorum reads should alway succeed
    _ = [ begin
              Key = init_new_db_key("abort_prepared_r"),
              abort_prepared(Key, read, [MC1, MC2, MC3, MC4], {ok, "abort_prepared_r"})
          end
          || MC1 <- causes(), MC2 <- causes(),
             MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_w(_) ->
    %% check all combinations of abort / prepared and the corresponding
    %% tx decisions
    %% modify DB and then do operations on the changed DB
    _ = [ begin
              Key = init_new_db_key("abort_prepared_w"),
              abort_prepared(Key, write, [MC1, MC2, MC3, MC4],
                             calc_w_outcome([MC1, MC2, MC3, MC4]))
          end
          || MC1 <- causes(), MC2 <- causes(),
             MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_rc(_) ->
    %% check all combinations of abort / prepared and the corresponding
    %% tx decisions
    %% modify DB and then do operations on the changed DB
    _ = [ begin
              Key = init_new_db_key("abort_prepared_rc"),
              abort_prepared(Key, read_commit, [MC1, MC2, MC3, MC4],
                             calc_rc_outcome([MC1, MC2, MC3, MC4]))
          end
          || MC1 <- causes(), MC2 <- causes(),
             MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_rmc(_) ->
     %% modify DB after work_phase, before validation
     %% read - modify - commit (rmc)
    _ = [ begin
              Key = init_new_db_key("abort_prepared_rmc"),
              {TLog, _} =
                  api_tx:req_list(api_tx:new_tlog(),
                                               [{read, Key}]),
              abort_prepared(Key, {commit_tlog, TLog}, [MC1, MC2, MC3, MC4],
                             calc_rmc_outcome([MC1, MC2, MC3, MC4]))
          end
          || MC1 <- causes(), MC2 <- causes(),
             MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_wmc(_) ->
     %% modify DB after work_phase, before validation
     %% write - modify - commit (rmc)
    _ = [ begin
              Key = init_new_db_key("abort_prepared_wmc"),
              {TLog, _} =
                  api_tx:req_list(api_tx:new_tlog(),
                                               [{write, Key, "wmc"}]),
              Pattern = [MC1, MC2, MC3, MC4],
              abort_prepared(Key, {commit_tlog, TLog}, Pattern,
                             calc_wmc_outcome(Pattern))
          end
          || MC1 <- causes(), MC2 <- causes(),
             MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared(Key, Op, PreOps, ExpectedOutcome) ->
    Keys = ?RT:get_replica_keys(?RT:hash_key(Key)),
    DBEntries = get_db_entries(Keys),
    NewDBEntries =
        [ begin
              NewDBEntry =
                  case PreOp of
                      readlock -> db_entry:inc_readlock(DBEntry);
                      writelock -> db_entry:set_writelock(DBEntry);
                      versiondec -> db_entry:set_version(DBEntry, db_entry:get_version(DBEntry) -1);
                      versioninc -> db_entry:inc_version(DBEntry);
                      none -> DBEntry
                  end,
              api_dht_raw:unreliable_lookup(db_entry:get_key(DBEntry),
                                       {set_key_entry, comm:this(), NewDBEntry}),
              receive {set_key_entry_reply, NewDBEntry} -> ok end,
              NewDBEntry
          end
          || {DBEntry, PreOp} <- lists:zip(DBEntries, PreOps) ],

    Outcome =
        case Op of
            write ->
                api_tx:write(Key, io_lib:format("~p with ~p", [Op, PreOps]));
            read ->
                api_tx:read(Key);
            read_commit ->
                {_TLog, [_, Res]} = api_tx:req_list([{read, Key}, {commit}]),
                Res;
            {commit_tlog, TLog} ->
                api_tx:commit(TLog)
        end,
    case ExpectedOutcome of
        ok_or_abort ->
            %% ct:pal("~w with ~w results in ~w~n", [Op, PreOps, Outcome]),
            ok;
        _ ->
            ?equals_w_note(Outcome, ExpectedOutcome,
                           io_lib:format("~p with ~p and~nDB entries initial: ~p~nafter preops ~p~n and after operation ~p~n",
                                         [Op, PreOps, DBEntries, NewDBEntries, get_db_entries(Keys)]))
    end,
   ok.

calc_w_outcome(PreOps) ->
    NumReadlock =   length([ X || X <- PreOps, X =:= readlock ]),
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionDec = length([ X || X <- PreOps, X =:= versiondec ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),
    NumNone =       length([ X || X <- PreOps, X =:= none ]),

    if (4 =:= NumVersionInc) -> {ok};
       (4 =:= NumVersionDec) -> {ok};
       (4 =:= NumNone) -> {ok};
       (1 =:= NumReadlock andalso NumNone =:= 3) -> {ok};
       (1 =:= NumWritelock andalso NumNone =:= 3) -> {ok};
       (1 =:= NumVersionDec andalso NumNone =:= 3) -> {ok};
       (1 =:= NumVersionInc andalso 3 =:= NumNone) -> ok_or_abort;

       (3 =:= NumVersionDec andalso 1 =:= NumReadlock) -> ok_or_abort;
       (3 =:= NumVersionDec andalso 1 =:= NumWritelock) -> ok_or_abort;
       (3 =:= NumVersionDec andalso 1 =:= NumVersionInc) -> ok_or_abort;
       (3 =:= NumVersionDec andalso 1 =:= NumNone) -> ok_or_abort;
       (3 =:= NumVersionInc andalso 1 =:= NumReadlock) -> ok_or_abort;
       (3 =:= NumVersionInc andalso 1 =:= NumVersionDec) -> {ok};
       (3 =:= NumVersionInc andalso 1 =:= NumNone) -> {ok};
       (3 =:= NumVersionInc andalso 1 =:= NumWritelock) -> {ok};

       true -> {fail, abort}
    end.

calc_rc_outcome(PreOps) ->
    %% DB is static over whole tx.
    %% Read phase and validation phase may operate on different
    %% replica subsets.
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),

    if (0 =:= NumWritelock) -> {ok};
       (1 =:= NumWritelock andalso 1 =/= NumVersionInc) -> {ok};
       (1 =:= NumWritelock andalso 1 =:= NumVersionInc) -> ok_or_abort;

       true -> {fail, abort}
    end.

calc_rmc_outcome(PreOps) ->
    NumReadlock =   length([ X || X <- PreOps, X =:= readlock ]),
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionDec = length([ X || X <- PreOps, X =:= versiondec ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),
    NumNone =       length([ X || X <- PreOps, X =:= none ]),

    if (4 =:= (NumReadlock + NumNone)) -> {ok};
       (3 =:= NumReadlock) -> {ok};
       (3 =:= NumReadlock + NumVersionDec + NumNone) -> {ok};
       (2 =< NumReadlock + NumNone)
       andalso (0 =:= NumWritelock + NumVersionInc) -> {ok};
       (1 =:= NumWritelock) -> {fail, abort};
       (3 =:= NumVersionDec) -> {ok};
       (4 =:= NumVersionDec) -> {ok};

       true -> {fail, abort}
    end.

calc_wmc_outcome(PreOps) ->
    NumReadlock =   length([ X || X <- PreOps, X =:= readlock ]),
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionDec = length([ X || X <- PreOps, X =:= versiondec ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),
    NumNone =       length([ X || X <- PreOps, X =:= none ]),

    if (NumVersionInc >= 2) -> {fail, abort};
       (NumVersionDec >= 2) -> {fail, abort};
       (NumReadlock >= 2) -> {fail, abort};
       (NumReadlock =:= 1 andalso NumNone =:= 3) -> {ok};
       (NumWritelock =:= 1 andalso NumNone =:= 3) -> {ok};
       (NumVersionDec =:= 1 andalso NumNone =:= 3) -> {ok};
       (NumNone =:= 4) -> {ok};
       (NumVersionInc =:= 1 andalso NumNone =:= 3) -> {ok};

       true -> {fail, abort}
    end.

init_new_db_key(Value) ->
    UID = util:get_global_uid(),
    NewKey = lists:flatten(io_lib:format("~p", [UID])),
    Keys = ?RT:get_replica_keys(?RT:hash_key(NewKey)),
    _ = [ begin
              E1 = db_entry:new(Key),
              %% inc twice, so decversion is ok
              E2 = db_entry:inc_version(E1),
              E3 = db_entry:inc_version(E2),
              %% set a value
              E4 = db_entry:set_value(E3, Value),
              api_dht_raw:unreliable_lookup(db_entry:get_key(E4),
                                       {set_key_entry, comm:this(), E4}),
              receive {set_key_entry_reply, E4} -> ok end
          end || Key <- Keys ],
    NewKey.

get_db_entries(Keys) ->
    [ begin
          api_dht_raw:unreliable_lookup(X, {get_key_entry, comm:this(), X}),
          receive
              {get_key_entry_reply, Entry} -> Entry
          end
      end
      || X <- Keys ].

bp_cond_tm_crash(Message, _State) ->
    case Message of
        {tx_tm_rtm_commit, _, _, [{rdht_tx_read,"a",value, _, 0}]} -> true;
        _ -> false
    end.

tm_crash(_) ->
    ct:pal("Starting tm_crash~n"),
    _ = api_tx:write("a", "Hello world!"),
    %% ct:pal("written initial value and setting breakpoints now~n"),
    TMs = pid_groups:find_all(tx_tm),
    %% all TMs break at next commit request:
    _ = [ gen_component:bp_set_cond(X, fun tx_tm_rtm_SUITE:bp_cond_tm_crash/2,
                                    tm_crash) || X <- TMs ],
    %% ct:pal("Breakpoints set~n"),
    _ = [ gen_component:bp_barrier(X) || X <- TMs ],
    %% ct:pal("Barriers set~n"),

    %% TM only will perform the tx_tm_rtm_commit, that lead to the BP
    %% bp_step blocks. Do it asynchronously. (We don't know which TM
    %% got the request.
    Pids = [ spawn(fun () -> gen_component:bp_step(X) end) || X <- TMs ],

    %% report all tx_tms as failed after the commit has started...
    _ = [ spawn(fun() ->
                        timer:sleep(1500),
                        comm:send_local(fd, {unittest_report_down, comm:make_global(X)})
                end)
          || X <- TMs ],

    Res = api_tx:req_list([{read, "a"}, {commit}]),

    ct:pal("Res: ~p~n", [Res]),
    %% may fail or succeed, so no equals check possible.
    %%?equals_pattern(Res, {_,[_,{ok}]}),

    _ = [ erlang:exit(Pid, kill) || Pid <- Pids ],

    _ = [ gen_component:bp_del(X, tm_crash) || X <- TMs ],

    [ gen_component:bp_cont(X) || X <- TMs ],
    ok.

tp_crash(_) ->
    ct:pal("Starting tp_crash, simulated by holding the dht_node_proposer~n"),
    api_tx:write("a", "Hello world!"),
    %% ct:pal("written initial value and setting breakpoints now~n"),
    AllProposers = pid_groups:find_all(paxos_proposer),
    Proposers = [X || X <- AllProposers,
                      "basic_services" =/= pid_groups:group_of(X)],
    Proposer = hd(Proposers),
    %% ct:pal("Selected ~p~n", [pid_groups:group_and_name_of(Proposer)]),
    %% break one TP (minority) after proposer initialize:
    gen_component:bp_set(Proposer, proposer_initialize, tp_crash),
    %% ct:pal("Breakpoints set~n"),
    gen_component:bp_barrier(Proposer),
    %% ct:pal("Barriers set~n"),

    %% TM only performs the tx_tm_rtm_commit that lead to the BP
    %% bp_step blocks. Do it asynchronously. (We don't know which TM
    %% got the request.
    %% Pids = [ spawn(fun () -> gen_component:bp_step(X) end) || X <- Proposers ],
    %% report the one tp as failed
    comm:send_local(fd, {unittest_report_down, comm:make_global(Proposer)}),

    %% ct:pal("Starting read commit~n"),
    Res = api_tx:req_list([{read, "a"}, {commit}]),

    ct:pal("Res: ~p~n", [Res]),

    %%[ erlang:exit(Pid, kill) || Pid <- Pids ],

    gen_component:bp_del(Proposer, tp_crash),
    gen_component:bp_cont(Proposer).

all_tp_crash(_) ->
    ct:pal("Starting all_tp_crash, simulated by holding the dht_node_proposers~n"),
    api_tx:write("a", "Hello world!"),
    %% ct:pal("written initial value and setting breakpoints now~n"),
    AllProposers = pid_groups:find_all(paxos_proposer),
    Proposers = [X || X <- AllProposers,
                      "basic_services" =/= pid_groups:group_of(X)],
    %% break all TPs (majority) after proposer initialize:
    [ gen_component:bp_set(Proposer, proposer_initialize, all_tp_crash)
      || Proposer <- Proposers],
    %% ct:pal("Breakpoints set~n"),
    [ gen_component:bp_barrier(Proposer) || Proposer <- Proposers ],
    %% ct:pal("Barriers set~n"),

    %% TM only performs the tx_tm_rtm_commit that lead to the BP
    %% bp_step blocks. Do it asynchronously. (We don't know which TM
    %% got the request.
    %% Pids = [ spawn(fun () -> gen_component:bp_step(X) end) || X <- Proposers ],
    %% report the one tp as failed
    [ comm:send_local(fd, {unittest_report_down, comm:make_global(Proposer)})
      || Proposer <- Proposers],

    %% ct:pal("Starting read commit~n"),
    Res = api_tx:req_list([{read, "a"}, {commit}]),

    ct:pal("Res: ~p~n", [Res]),

    %%[ erlang:exit(Pid, kill) || Pid <- Pids ],

    [ gen_component:bp_del(Proposer, all_tp_crash) ||  Proposer <- Proposers],
    [ gen_component:bp_cont(Proposer) || Proposer <- Proposers].
