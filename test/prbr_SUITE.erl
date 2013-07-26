%% @copyright 2012, 2013 Zuse Institute Berlin

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
            rbr_concurrency_kv,
            rbr_concurrency_leases,
            rbr_consistency
           ].
suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        rbr_concurrency_kv ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Size = randoms:rand_uniform(3, 14),
            unittest_helper:make_ring(Size, [{config, [{log_path, PrivDir}]}]),
            %% necessary for the consistency check:
            unittest_helper:check_ring_size_fully_joined(Size),
            Config;
        rbr_concurrency_leases ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Size = 1, %% larger rings not supported by leases yet,
            %% Size = randoms:rand_uniform(2, 14),
            unittest_helper:make_ring(Size, [{config, [{log_path, PrivDir},
                                                       {leases, true}]}]),
            %% necessary for the consistency check:
            unittest_helper:check_ring_size_fully_joined(Size),
            Config;
        rbr_consistency ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring_with_ids(?RT:get_replica_keys(0),
                                               [{config, [{log_path, PrivDir}]}]),
            %% necessary for the consistency check:
            unittest_helper:check_ring_size_fully_joined(4),

            Config;
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Size = randoms:rand_uniform(1, 9),
            unittest_helper:make_ring(Size, [{config, [{log_path, PrivDir}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

%% TODO: unittest for: retrigger on read works
%% TODO: unittest for: retrigger on write works

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
    DB = l_on_cseq:get_db_for_id(Key),
    rbrcseq:qwrite(DB, self(), Key,
                   ContentCheck,
                   New),
    receive
        {qwrite_done, _ReqId, _Round, _} -> ok
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
                                              l_on_cseq:unittest_lease_update(
                                                V,
                                                l_on_cseq:set_version(
                                                  V, l_on_cseq:get_version(V)+1)),
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
    %% if the new value was seen once, the old must not be readable again

    Nodes = pid_groups:find_all(dht_node),
    Key = "a",

    %% initialize key
    {ok} = kv_on_cseq:write(Key, 1),

    %% select a replica
    Replicas = ?RT:get_replica_keys(?RT:hash_key(Key)),

    _ = [ begin
              New = N+100,
              Old = case N of
                        1 -> 1;
                        _ -> N+99
                    end,

              modify_rbr_at_key(R, N+100),

              %% intercept and drop a message at r1
              _ = lists:foldl(read_quorum_without(Key), {Old, New}, Nodes),
              ok
          end || {R,N} <- lists:zip(Replicas, lists:seq(1,4))],

    ok.

tester_type_check_rbr(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {txid_on_cseq,
           [ {is_valid_new, 3}, %% cannot create funs
             {is_valid_decide, 3}, %% cannot create funs
             {is_valid_delete, 3}, %% cannot create funs
             {new, 3}, %% tested via feeder
             {decide, 5}, %% cannot create pids
             {delete, 2}, %% cannot create pids
             {read, 2} %% cannot create pids
           ],
           [ {cc_single_write, 3}, %% cannot create funs
             {cc_set_rl, 3}, %% cannot create funs
             {cc_set_wl, 3},  %% cannot create funs
             {cc_commit_read, 3},  %% cannot create funs
             {cc_commit_write, 3}  %% cannot create funs
           ]},
          {tx_tm,
           [{start_link, 2},       %% starts processes
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
           [ {commit_read, 5}, %% tested via feeder
             {commit_write, 5}, %% tested via feeder
             {abort_read, 5}, %% tested via feeder
             {abort_write, 5}, %% tested via feeder
             {set_lock, 3} %% tested via feeder
           ],
           [ {cc_single_write, 3}, %% cannot create funs
             {cc_set_rl, 3}, %% cannot create funs
             {cc_set_wl, 3},  %% cannot create funs
             {cc_commit_read, 3},  %% cannot create funs
             {cc_commit_write, 3},  %% cannot create funs
             {cc_abort_read, 3},  %% cannot create funs
             {cc_abort_write, 3}  %% cannot create funs
           ]},
          {prbr,
           [ {on, 2},       %% sends messages
             {set_entry, 2} %% needs valid tid()
          ],
           [ {msg_read_reply, 4},  %% sends messages
             {msg_read_deny, 3},   %% sends messages
             {msg_write_reply, 4}, %% sends messages
             {msg_write_deny, 3},  %% sends messages
             {get_entry, 2}        %% needs valid tid()
            ]},
          {rbrcseq,
           [ {on, 2},          %% sends messages
             {qread, 3},       %% tries to create envelopes
             {qread, 4},       %% needs fun as input
             {start_link, 3},  %% needs fun as input
             {qwrite, 5},      %% needs funs as input
             {qwrite, 7},      %% needs funs as input
             {qwrite_fast, 7}, %% needs funs as input
             {qwrite_fast, 9}  %% needs funs as input
           ],
           [ {inform_client, 2}, %% cannot create valid envelopes
             {get_entry, 2},     %% needs valid tid()
             {set_entry, 2}      %% needs valid tid()
           ]
          }
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    true.

modify_rbr_at_key(R, N) ->
    %% get a valid round number
    comm:send_local(pid_groups:find_a(dht_node),
                    {?lookup_aux, R, 0,
                     {prbr, read, kv, comm:this(),
                      R, unittest_rbr_consistency1,
                      fun prbr:noop_read_filter/1}}),
    receive
        {read_reply, AssignedRound, _, _} ->
            ok
    end,
    %% perform a write
    comm:send_local(pid_groups:find_a(dht_node),
                    {?lookup_aux, R, 0,
                     {prbr, write, kv, comm:this(),
                      R, AssignedRound, {[], false, N+1, N}, null,
                      fun prbr:noop_write_filter/3}}),
    receive
        {write_reply, R, _, _NextRound} ->
            ok
    end.

drop_prbr_read_request(Client, Tag) ->
    fun (Message, _State) ->
            case Message of
%%                {prbr, _, kv, ReqClient, Key, _Round, _RF} ->
                _ when element(1, Message) =:= prbr
                       andalso element(3, Message) =:= kv ->
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
    fun (X, {Old, New}) ->
            gen_component:bp_set_cond(
              X,
              drop_prbr_read_request(self(), drop_prbr_read),
              drop_prbr_read),

            {ok, Val} = kv_on_cseq:read(Key),
            receive
                {drop_prbr_read, done} ->
                    gen_component:bp_del(X, drop_prbr_read),
                    ok
            end,
            cleanup({drop_prbr_read, done}),
            case Val of
                Old ->
                    {Old, New}; %% valid for next read
                New ->
                    {New, New}; %% old is no longer acceptable
                _ -> ?equals(Val, New)
            end
    end.

cleanup(Msg) ->
    receive Msg -> cleanup(Msg)
    after 0 -> ok
    end.
