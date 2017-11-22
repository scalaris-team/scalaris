%% @copyright 2013-2017 Zuse Institute Berlin

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
%% @doc    Unit tests for prbr
%% @end
%% @version $Id$
-module(prbr_SUITE).
-author('schintke@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [
            tester_type_check_rbr,
            rbr_increment,
            rbr_concurrency_kv,
            rbr_concurrency_leases,
            rbr_consistency,
            rbr_consistency_delete
           ].
suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        rbr_concurrency_kv ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Size = randoms:rand_uniform(3, 14),
            unittest_helper:make_ring(Size, [{config, [{log_path, PrivDir}]}]),
            %% necessary for the consistency check:
            unittest_helper:check_ring_size_fully_joined(Size),
            ok;
        rbr_concurrency_leases ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Size = 1, %% larger rings not supported by leases yet,
            %% Size = randoms:rand_uniform(2, 14),
            unittest_helper:make_ring(Size, [{config, [{log_path, PrivDir},
                                                       {leases, true}]}]),
            %% necessary for the consistency check:
            unittest_helper:check_ring_size_fully_joined(Size),
            ok;
        rbr_consistency ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir}]}]),
            %% necessary for the consistency check:
            unittest_helper:check_ring_size_fully_joined(config:read(replication_factor)),

            ok;
        _ ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Size = randoms:rand_uniform(1, 9),
            unittest_helper:make_ring(Size, [{config, [{log_path, PrivDir}]}]),
            ok
    end,
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%% TODO: unittest for: retrigger on read works
%% TODO: unittest for: retrigger on write works

rbr_increment(_Config) ->
    %% start random number of nodes
    %% select a key to operate on
    %% start random number of increments (non transactional)
    %% check if number of increments = value in key
    Key = randoms:getRandomString(),

    Parallel = randoms:rand_uniform(5, 11),
    Count = 1000 div Parallel,
    ct:pal("Starting concurrent writers: ~p~n"
           "Performing iterations: ~p~n",
           [Parallel, Count]),
    UnitTestPid = self(),
    _Pids = [ spawn(fun() ->
                            _ = [ begin
                                      {ok} = inc_on_cseq:inc(Key)
                                  end
                                  || _I <- lists:seq(1, Count)],
                           UnitTestPid ! {done}
                   end)
             || _Nth <- lists:seq(1, Parallel)],

    _ = [ receive {done} ->
                      ct:pal("Finished ~p/~p.~n", [Nth, Parallel]),
                      ok
          end || Nth <- lists:seq(1, Parallel)],

    {ok, Result} = inc_on_cseq:read(Key),
    ct:pal("Planned ~p increments, done ~p - discrepancy is NOT ok~n",
           [Count*Parallel, Result]),
    ?equals(Count*Parallel, Result),
    ok.

rbr_concurrency_kv(_Config) ->
    %% start random number of nodes
    %% select a key to operate on
    %% start random number of writers (increment operations
    %%   / use increment as write filter)
    %% start random number of readers
    %%   only observe increasing values in reads
    Key = randoms:getRandomString(),

    {ok} = kv_on_cseq:write(Key, 1),
    Parallel = randoms:rand_uniform(1, 11),
    Count = 1000 div Parallel,
    ct:pal("Starting concurrent writers: ~p~n"
           "Performing iterations: ~p~n",
           [Parallel, Count]),
    UnitTestPid = self(),
    _Pids = [ spawn(fun() ->
                            _ = [ begin
                                      {ok, V} = kv_on_cseq:read(Key),
                                      {ok} = kv_on_cseq:write(Key, V+1)
                                      %% if 0 == I rem 100 ->
                                      %%   ct:pal("~p performed write ~p.~n",
                                      %%   [_Nth, I]);
                                      %%   true -> ok
                                      %% end
                                  end
                                  || _I <- lists:seq(1, Count)],
                           UnitTestPid ! {done}
                   end)
             || _Nth <- lists:seq(1, Parallel)],

    _ = [ receive {done} ->
                      ct:pal("Finished ~p/~p.~n", [Nth, Parallel]),
                      ok
          end || Nth <- lists:seq(1, Parallel)],

    ct:pal("Planned ~p increments, done ~p - discrepancy is ok~n",
           [Count*Parallel, kv_on_cseq:read(Key)]),
    ok.

rbr_concurrency_leases(_Config) ->
    %% start random number of nodes
    %% select a key to operate on
    %% start random number of writers (increment operations
    %%   / use increment as write filter)
    %% start random number of readers
    %%   only observe increasing values in reads
    Key = ?RT:get_random_node_id(),

    ContentCheck =
        fun (Current, _WriteFilter, _Next) ->
                case Current == prbr_bottom of
                    true ->
                        {true, null};
                    false ->
                        {false, lease_already_exists}
                end
        end,
    New = l_on_cseq:unittest_create_lease(Key),
    DB = rbrcseq:get_db_for_id(lease_db, Key),
    rbrcseq:qwrite(DB, self(), Key, l_on_cseq,
                   ContentCheck,
                   New),
    receive
        {qwrite_done, _ReqId, _Round, _, _} -> ok
    end,

    Parallel = randoms:rand_uniform(4, 11),
    Count = 1000 div Parallel,
    ct:pal("Starting concurrent writers: ~p~n"
           "Performing iterations: ~p~n",
           [Parallel, Count]),
    UnitTestPid = self(),
    DHTNodeGroups = pid_groups:groups_with(dht_node),
    DHTNodeGroupsLen = length(DHTNodeGroups),
    _Pids =
        [ spawn(
            fun() ->
                    Group = lists:nth(1 + Nth rem DHTNodeGroupsLen,
                                      DHTNodeGroups),
                    pid_groups:join(Group),
                    _ = [ begin
                              F = fun(X) ->
                                          {ok, V} = l_on_cseq:read(Key),
                                          Update =
                                              l_on_cseq:unittest_lease_update_unsafe(
                                                V,
                                                l_on_cseq:set_version(
                                                  V, l_on_cseq:get_version(V)+1), passive),
                                          case Update of
                                              ok -> ok;
                                              failed ->
                                                  %% ct:pal("~p retry ~p.~n",
                                                  %%        [_Nth, l_on_cseq:get_version(V)+1]),
                                                  X(X)
                                          end
                                  end,
                              F(F)
                          %% ct:pal("~p performed write.~n", [_Nth])
                          end
                          || _I <- lists:seq(1, Count)],
                    UnitTestPid ! {done}
            end)
          || Nth <- lists:seq(1, Parallel)],

    _ = [ receive {done} ->
                      ct:pal("Finished ~p/~p.~n", [Nth, Parallel]),
                      ok
          end || Nth <- lists:seq(1, Parallel)],

    ct:pal("Planned ~p increments, done ~p, discrepancy is ok~n",
           [Count*Parallel, l_on_cseq:read(Key)]),

    ok.

rbr_consistency(_Config) ->
    %% create an rbr entry
    %% update 1 to 3 of its replicas
    %% perform read in all quorum permutations
    %% (intercept read on a single dht node)
    %% output must be the old value or the new value
    %% if the new value was seen once, the old must not be readable again.

    Nodes = pid_groups:find_all(dht_node),
    Key = "a",

    %% initialize key
    {ok} = kv_on_cseq:write(Key, 1),

    %% select a replica
    Replicas = ?RT:get_replica_keys(?RT:hash_key(Key)),

%%    print modified rbr entries
%%    api_tx_proto_sched_SUITE:rbr_invariant(a,b,c),

    _ = [ begin
              New = N+100,
              {ok, Old} = kv_on_cseq:read(Key),

              modify_rbr_at_key(R, N+100),
              %% ct:pal("After modification:"),
              %% print modified rbr entries
              %% api_tx_proto_sched_SUITE:rbr_invariant(a,b,c),

              %% intercept and drop a message at r1
              _ = lists:foldl(read_quorum_without(Key), {Old, New}, Nodes),
              ok
          end || {R,N} <- lists:zip(Replicas, lists:seq(1, config:read(replication_factor)))],

    ok.

rbr_consistency_delete(_Config) ->
    %% create an rbr entry
    %% update 1 to 3 of its replicas
    %% perform read in all quorum permutations
    %% (intercept read on a single dht node)
    %% output must be the old value or the new value
    %% if the new value was seen once, the old must not be readable again.

    %% Nodes = pid_groups:find_all(dht_node),
    Key = "a",

    %% initialize key
    {ok} = kv_on_cseq:write(Key, 1),

    %% select a replica
    Replicas = ?RT:get_replica_keys(?RT:hash_key(Key)),

%%    print modified rbr entries
%%    api_tx_proto_sched_SUITE:rbr_invariant(a,b,c),

    ct:pal("Starting delete test~n"),


    Res = [ begin
                ct:pal("Read iteration: ~p~n", [R]),

                {ok, Old} = kv_on_cseq:read(Key),

                delete_rbr_entry_at_key(R),
                Next = Old + 1,

                ct:pal("Write in iteration: ~p~n", [R]),
                _ = kv_on_cseq:write(Key, Next),

                %% ct:pal("After modification:"),
                %% print modified rbr entries
                %% api_tx_proto_sched_SUITE:rbr_invariant(a,b,c),

                ct:pal("Reread in iteration: ~p~n", [R]),
                {ok, Next} = kv_on_cseq:read(Key)
          end || R <- Replicas],

    ct:pal("Result: ~p~n", [Res]),
    ok.



tester_type_check_rbr(_Config) ->
    Count = 250,
    config:write(no_print_ring_data, true),

    tester:register_value_creator({typedef, prbr, write_filter, []},
                                  prbr, tester_create_write_filter, 1),

    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {txid_on_cseq,
           [ {is_valid_new, 3}, %% cannot create funs
             {is_valid_decide, 3}, %% cannot create funs
             {is_valid_delete, 3}, %% cannot create funs
             {decide, 5}, %% cannot create pids
             {delete, 2}, %% cannot create pids
             {read, 2} %% cannot create pids
           ],
           [ ]
          },
          {tx_tm,
           [{start_link, 2},       %% starts processes
            {start_gen_component,5}, %% unsupported types
            {init, 1},             %% needs to be pid_group member
            {on, 2},               %% needs valid messages
            {on_init, 2},          %% needs valid messages
            {commit, 4},           %% needs valid clients pid
            {msg_commit_reply, 3} %% needs valid clients pid
           ],
           [ {get_entry, 2},         %% could read arb, entries
             %% guessing keys of tx entries...
             {tx_state_add_nextround_writtenval_for_commit, 4}
           ]
          },
          {kv_on_cseq,
           [ {commit_read, 5},  %% tested via feeder
             {commit_write, 5}, %% tested via feeder
             {abort_read, 5},   %% tested via feeder
             {abort_write, 5},  %% tested via feeder
             {get_commuting_wf_for_rf, 1}], %% cannot create funs
           []},
          {inc_on_cseq, [],[]},
          {pr,
           [
           ],
           []},
          {prbr,
           [ {init, 1},             %% needs to be in a pidgroup for db_name
             {close, 1},            %% needs valid ets:tid()
             {close_and_delete, 1}, %% needs valid ets:tid()
             {on, 2},               %% sends messages
             {get_load, 1},         %% needs valid ets:tid()
             {set_entry, 2},        %% needs valid ets:tid()
             {get_entry, 2},        %% needs valid ets:tid()
             {delete_entry, 2},     %% needs valid ets:tid()
             {tab2list, 1},         %% needs valid ets:tid()
             {tab2list_raw_unittest, 1} %% needs valid ets:tid()
          ],
           [ {msg_read_reply, 6},  %% sends messages
             {msg_write_reply, 6}, %% sends messages
             {msg_write_deny, 4},  %% sends messages
             {tab2list_raw, 1}     %% needs valid ets:tid()
            ]},
          {rbrcseq,
           [ {on, 2},          %% sends messages
             {qread, 4},       %% tries to create envelopes
             {qread, 5},       %% needs fun as input
             {start_link, 3},  %% needs fun as input
             {start_gen_component,5}, %% unsupported types
             {qwrite, 6},      %% needs funs as input
             {qwrite, 8},      %% needs funs as input
             {qwrite_fast, 8}, %% needs funs as input
             {qwrite_fast, 10}  %% needs funs as input
           ],
           [ {inform_client, 3}, %% cannot create valid envelopes
             {start_request, 2}, %% cannot create valid envelopes
             {get_entry, 2},     %% needs valid ets:tid()
             {save_entry, 2},     %% needs valid ets:tid()
             {update_entry, 2},     %% needs valid ets:tid()
             {delete_entry, 2},     %% needs valid ets:tid()
             {delete_newer_entries, 2}, %% needs valid ets:tid()
             {delete_all_entries, 3}, %% needs valid ets:tid()
             {retrigger, 3},     %% needs valid ets:tid()
             {add_rr_reply, 10},  %% needs client_value matching db_type
             {add_read_reply, 11},%% needs client_value matching db_type
             {update_write_state, 5}, %% needs client_value matching db_type
             {add_read_deny, 5}, %% needs valid entry()
             {add_write_reply, 3},%% needs valid entry()
             {add_write_deny, 3}, %% needs valid entry()
             {is_read_commuting, 3} %% cannot create funs
           ]},
          {replication,
           [ {get_read_value, 2},     %% cannot create funs
             {collect_read_value, 3}  %% needs client_value matching datatype
           ],
           []}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_value_creator({typedef, prbr, write_filter, []}),

    true.

modify_rbr_at_key(R, N) ->
    %% get a valid round number

    %% we ask all replicas to not get an outdated round number (select
    %% the highest one.
    ProposerUID = unittest_rbr_consistency1_id,
    Rounds = [ begin
                   %% let fill in whether lookup was consistent
                   LookupReadEnvelope = dht_node_lookup:envelope(
                                          4,
                                          {prbr, round_request, kv_db, '_', comm:this(),
                                           Repl, kv_on_cseq, ProposerUID,
                                           fun prbr:noop_read_filter/1, write}),
                   comm:send_local(pid_groups:find_a(dht_node),
                                   {?lookup_aux, Repl, 0, LookupReadEnvelope}),
                   receive
                       {round_request_reply, _, AssignedRound, OldWriteRound,  _Val, _LastWF} ->
                           {AssignedRound, OldWriteRound}
                   end
               end || Repl <- ?RT:get_replica_keys(R) ],
    {ReadRounds, WriteRounds} = lists:foldl(fun({RR, WR}, {AccR, AccW}) ->
                                                    {[RR|AccR], [WR|AccW]}
                                            end, {[], []}, Rounds),
    ct:pal("~p ~n~p", [ReadRounds, WriteRounds]),
    {HighestReadRound, HighestWriteRound} = {lists:max(ReadRounds),
                                             lists:max(WriteRounds)},

    %% perform a write
    %% let fill in whether lookup was consistent
    LookupWriteEnvelope = dht_node_lookup:envelope(
                            4,
                            {prbr, write, kv_db, '_', comm:this(), R,
                             kv_on_cseq, ProposerUID, HighestReadRound, HighestWriteRound,
                             {[], false, _Version = N-100, _Value = N},
                             null,
                             fun prbr:noop_write_filter/3, false}),
    %% modify the replica at key R, therefore we use a lookup...
    comm:send_local(pid_groups:find_a(dht_node),
                    {?lookup_aux, R, 0, LookupWriteEnvelope}),
    receive
        {write_reply, _, R, _, _NextRound, _} ->
            ok
    end.

delete_rbr_entry_at_key(R) ->
    comm:send_local(pid_groups:find_a(dht_node),
                    {?lookup_aux, R, 0,
                    {prbr, delete_key, kv_db, self(), R}}),
    receive {delete_key_reply, R} -> ok end.

drop_prbr_read_request(Client, Tag) ->
    fun (Message, _State) ->
            case Message of
%%                {prbr, _, kv_db, ReqClient, Key, _Round, _RF} ->
                _ when element(1, Message) =:= prbr
                       andalso element(3, Message) =:= kv_db ->
                    ct:pal("Detected read, dropping it ~p, key ~p~n",
                           [self(), element(5, Message)]),
                    comm:send_local(Client, {Tag, done}),
                    drop_single;
                _ when element(1, Message) =:= prbr ->
                    false;
                _ -> false
            end
    end.

read_quorum_without(Key) ->
    fun (ExcludedDHTNode, {Old, New}) ->
            gen_component:bp_set_cond(
              ExcludedDHTNode,
              drop_prbr_read_request(self(), drop_prbr_read),
              drop_prbr_read),

            {ok, Val} = kv_on_cseq:read(Key),
            io:format("Old: ~p, Val: ~p New: ~p", [Old, Val, New]),
            receive
                {drop_prbr_read, done} ->
                    gen_component:bp_del(ExcludedDHTNode, drop_prbr_read),
                    ok
            end,
            cleanup({drop_prbr_read, done}),
            %% print modified rbr entries:
            %% api_tx_proto_sched_SUITE:rbr_invariant(a,b,c),
            case Val of
                Old ->
                    {Old, New}; %% valid for next read
                New ->
                    {New, New}; %% old is no longer acceptable
                X ->
                    %% maybe an update was not propagated at all in the previous round
                    case X > Old andalso X < New of
                        true -> {X, New};
                        _ -> ?equals(Val, New)
                    end
            end
    end.

cleanup(Msg) ->
    receive Msg -> cleanup(Msg)
    after 0 -> ok
    end.
