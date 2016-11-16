%  @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @author Nico Kruber <kruber@zib.de>
%% @doc    Unit tests for the replica repair modules.
%%   rrepair_SUITE.erl:
%%       The regular execution of the suite.
%%   rrepair_proto_sched_SUITE.erl:
%%       Executes using the proto scheduler which serializes all messages to
%%       generate a random interleaving of messages on different channels.
%% @end
%% @version $Id$

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").
-include("client_types.hrl").
-include("record_helpers.hrl").

-define(DBSizeKey, rrepair_SUITE_dbsize).    %Process Dictionary Key for generated db size

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

repair_default() ->
    [no_diff,        % ring is not out of sync e.g. no outdated or missing replicas
     one_node,       % sync in ring with only one node
     %mpath
     dest,           % run one sync with a specified dest node
     simple,         % run one sync round
     multi_round,    % run multiple sync rounds with sync probability 1
     multi_round2,   % run multiple sync rounds with sync probability 0.4
     parts,          % get_chunk with limited items (leads to multiple get_chunk calls, in case of bloom also multiple bloom filters)
     asymmetric_ring % rrepair in an asymmetric ring with a node covering more than a quadrant (no other checks - it just needs to run successfully)
    ].

regen_special() ->
    [
     dest_empty_node % run one sync with empty dest node
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_suite(Config) ->
    tester:register_type_checker({typedef, intervals, interval, []}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, continuous_interval, []}, intervals, is_continuous),
    tester:register_type_checker({typedef, intervals, non_empty_interval, []}, intervals, is_non_empty),
    tester:register_value_creator({typedef, intervals, interval, []}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval, []}, intervals, tester_create_continuous_interval, 4),
    tester:register_value_creator({typedef, intervals, non_empty_interval, []}, intervals, tester_create_non_empty_interval, 2),
    rt_SUITE:register_value_creator(),
    Config.

end_per_suite(_Config) ->
    tester:unregister_type_checker({typedef, intervals, interval, []}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval, []}),
    tester:unregister_type_checker({typedef, intervals, non_empty_interval, []}),
    tester:unregister_value_creator({typedef, intervals, interval, []}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval, []}),
    tester:unregister_value_creator({typedef, intervals, non_empty_interval, []}),
    rt_SUITE:unregister_value_creator(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_group(Group, Config0) ->
    ct:comment(io_lib:format("BEGIN ~p", [Group])),
    Config = init_per_group_special(Group, Config0),
    case Group of
        upd_trivial -> [{ru_method, trivial}, {ftype, update}];
        upd_shash -> [{ru_method, shash}, {ftype, update}];
        upd_bloom -> [{ru_method, bloom}, {ftype, update}];
        upd_merkle -> [{ru_method, merkle_tree}, {ftype, update}];
        upd_art -> [{ru_method, art}, {ftype, update}];
        regen_trivial -> [{ru_method, trivial}, {ftype, regen}];
        regen_shash -> [{ru_method, shash}, {ftype, regen}];
        regen_bloom -> [{ru_method, bloom}, {ftype, regen}];
        regen_merkle -> [{ru_method, merkle_tree}, {ftype, regen}];
        regen_art -> [{ru_method, art}, {ftype, regen}];
        mixed_trivial -> [{ru_method, trivial}, {ftype, mixed}];
        mixed_shash -> [{ru_method, shash}, {ftype, mixed}];
        mixed_bloom -> [{ru_method, bloom}, {ftype, mixed}];
        mixed_merkle -> [{ru_method, merkle_tree}, {ftype, mixed}];
        mixed_art -> [{ru_method, art}, {ftype, mixed}];
        _ -> []
    end ++ Config.

end_per_group(Group, Config) ->
    Method = proplists:get_value(ru_method, Config, undefined),
    FType = proplists:get_value(ftype, Config, undefined),
    case Method of
        undefined -> ct:comment(io_lib:format("END ~p", [Group]));
        M -> ct:comment(io_lib:format("END ~p/~p", [FType, M]))
    end,
    Config2 = proplists:delete(ru_method, Config),
    end_per_group_special(Group, Config2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_testcase(_TestCase, Config) ->
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_rep_upd_config(Method) ->
    [{rrepair_enabled, true},
     {rr_trigger_interval, 0}, %stop trigger
     {rr_recon_method, Method},
     {rr_session_ttl, 100000},
     {rr_gc_interval, 60000},
     {rr_recon_failure_rate, 0.1},
     {rr_trigger_probability, 100},
     {rr_art_inner_fpr, 0.01},
     {rr_art_leaf_fpr, 0.1},
     {rr_art_correction_factor, 2},
     {rr_merkle_branch_factor, 4},
     {rr_merkle_bucket_size, 3}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Replica Update tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

no_diff(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    start_sync(Config, 4, 1000, [{fprob, 0}, {ftype, FType}],
               1, 0.1, get_rep_upd_config(Method), fun erlang:'=:='/2).

one_node(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    % atm, there is no rrepair with self (we wouldn't need a complex protocol for that)
    start_sync(Config, 1, 1, [{fprob, 50}, {ftype, FType}],
               1, 0.2, get_rep_upd_config(Method), fun erlang:'=:='/2).

mpath_map({X, {?key_upd, KVV, ReqKeys}, _}, _Source, _Dest)
  when X =:= request_resolve orelse X =:= continue_resolve ->
    {?key_upd, length(KVV), length(ReqKeys)};
mpath_map({X, _, {?key_upd, KVV, ReqKeys}, _}, _Source, _Dest)
  when X =:= request_resolve orelse X =:= continue_resolve ->
    {?key_upd, length(KVV), length(ReqKeys)};
mpath_map(Msg, _Source, _Dest) ->
    {element(1, Msg)}.

mpath(Config) ->
    %parameter
    NodeCount = 4,
    DataCount = 1000,
    FR = 0.1,
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    TraceName = erlang:list_to_atom(atom_to_list(Method)++atom_to_list(FType)),
    %build and fill ring
    _ = build_ring(NodeCount, Config, [get_rep_upd_config(Method),
                                       {rr_recon_failure_rate, FR}]),
    _ = db_generator:fill_ring(random, DataCount, [{ftype, FType},
                                                   {fprob, 50},
                                                   {distribution, uniform}]),
    %chose node pair
    SKey = ?RT:get_random_node_id(),
    CKey = util:randomelem(lists:delete(SKey, ?RT:get_replica_keys(SKey))),
    %server starts sync
    %trace_mpath:start(TraceName, fun mpath_map/3),
    trace_mpath:start(TraceName),
    api_dht_raw:unreliable_lookup(SKey, {?send_to_group_member, rrepair,
                                              {request_sync, Method, CKey, comm:this()}}),
    waitForSyncRoundEnd([SKey, CKey], true),
    trace_mpath:stop(),
    %TRACE
    A = trace_mpath:get_trace(TraceName, cleanup),
    B = [X || X = {log_send, _Time, _TraceID,
                   {{_FIP,_FPort,_FPid}, _FName},
                   {{_TIP,_TPort,_TPid}, _TName},
                   _Msg, _LocalOrGlobal} <- A],
    ok = file:write_file("TRACE_" ++ atom_to_list(TraceName) ++ ".txt",
                         io_lib:fwrite("~.0p\n", [B])),
    ok = file:write_file("TRACE_HISTO_" ++ atom_to_list(TraceName) ++ ".txt",
                         io_lib:fwrite("~.0p\n", [trace_mpath:send_histogram(B)])),
    %ok = file:write_file("TRACE_EVAL_" ++ atom_to_list(TraceName) ++ ".txt",
    %                     io_lib:fwrite("~.0p\n", [rr_eval_admin:get_bandwidth(A)])),
    ok.

simple(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    start_sync(Config, 4, 1000, [{fprob, 10}, {ftype, FType}],
               1, 0.1, get_rep_upd_config(Method), fun erlang:'<'/2).

multi_round(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    start_sync(Config, 6, 1000, [{fprob, 10}, {ftype, FType}],
               3, 0.1, get_rep_upd_config(Method), fun erlang:'<'/2).

multi_round2(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    _RUConf = get_rep_upd_config(Method),
    RUConf = [{rr_trigger_probability, 40} | proplists:delete(rr_trigger_probability, _RUConf)],
    start_sync(Config, 6, 1000, [{fprob, 10}, {ftype, FType}],
               3, 0.1, RUConf, fun erlang:'<'/2).

dest(Config) ->
    %parameter
    NodeCount = 7,
    DataCount = 1000,
    FR = 0.1,
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    %build and fill ring
    _ = build_ring(NodeCount, Config, [get_rep_upd_config(Method),
                                       {rr_recon_failure_rate, FR}]),
    _ = db_generator:fill_ring(random, DataCount, [{ftype, FType},
                                                   {fprob, 50},
                                                   {distribution, uniform}]),
    RingData = unittest_helper:get_ring_data(kv),
    {DBKeys, _DBKeysNum, _Outdated} = create_full_db(RingData, [], 0, 0),
    %chose node pair
    SKey = ?RT:get_random_node_id(),
    CKey = util:randomelem(lists:delete(SKey, ?RT:get_replica_keys(SKey))),
    %measure initial sync degree
    SO = count_outdated(RingData, SKey),
    SM = count_dbsize(RingData, SKey),
    CO = count_outdated(RingData, CKey),
    CM = count_dbsize(RingData, CKey),
    %server starts sync
    ?proto_sched(start),
    api_dht_raw:unreliable_lookup(SKey, {?send_to_group_member, rrepair,
                                              {request_sync, Method, CKey, comm:this()}}),
    waitForSyncRoundEnd([SKey, CKey], true),
    ?proto_sched(stop),
    %measure sync degree
    RingDataNew = unittest_helper:get_ring_data(kv),
    SONew = count_outdated(RingDataNew, SKey),
    SMNew = count_dbsize(RingDataNew, SKey),
    CONew = count_outdated(RingDataNew, CKey),
    CMNew = count_dbsize(RingDataNew, CKey),
    remove_full_db(DBKeys),
    ct:pal("SYNC RUN << ~p / ~p >>~nServerKey=~p~nClientKey=~p~n"
           "Server Outdated=[~p -> ~p] DBSize=[~p -> ~p] - Upd=~p ; Regen=~p~n"
           "Client Outdated=[~p -> ~p] DBSize=[~p -> ~p] - Upd=~p ; Regen=~p",
           [Method, FType, SKey, CKey,
            SO, SONew, SM, SMNew, SO - SONew, SMNew - SM,
            CO, CONew, CM, CMNew, CO - CONew, CMNew - CM]),
    %clean up
    ?implies(SO > 0 orelse CO > 0, SONew < SO orelse CONew < CO) andalso
        ?implies(SM =/= SMNew, SMNew > SM) andalso
        ?implies(CM =/= CMNew, CMNew > CM).

dest_empty_node(Config) ->
    %parameter
    DataCount = 1000,
    FR = 0.1,
    Method = proplists:get_value(ru_method, Config),
    %build and fill ring
    _ = build_ring(symmetric, Config,
                   [get_rep_upd_config(Method), {rr_recon_failure_rate, FR}]),
    _ = db_generator:fill_ring(random, DataCount, [{ftype, regen},
                                                   {fprob, 100},
                                                   {distribution, uniform},
                                                   {fdest, [1]}]),
    %chose any node not in quadrant 1
    KeyGrp = ?RT:get_replica_keys(?RT:get_random_node_id()),
    [Q1 | _] = rr_recon:quadrant_intervals(),
    IKey = util:randomelem([X || X <- KeyGrp, not intervals:in(X, Q1)]),
    CKey = hd([Y || Y <- KeyGrp, intervals:in(Y, Q1)]),
    %measure initial sync degree
    RingData = unittest_helper:get_ring_data(kv),
    IM = count_dbsize(RingData, IKey),
    CM = count_dbsize(RingData, CKey),
    ?equals(CM, 0),
    %server starts sync
    ?proto_sched(start),
    api_dht_raw:unreliable_lookup(IKey, {?send_to_group_member, rrepair,
                                              {request_sync, Method, CKey, comm:this()}}),
    waitForSyncRoundEnd([IKey, CKey], true),
    ?proto_sched(stop),
    %measure sync degree
    RingDataNew = unittest_helper:get_ring_data(kv),
    IMNew = count_dbsize(RingDataNew, IKey),
    CMNew = count_dbsize(RingDataNew, CKey),
    ct:pal("SYNC RUN << ~p >>~nServerKey=~p~nClientKey=~p~n"
           "Server DBSize=[~p -> ~p] - Regen=~p~n"
           "Client DBSize=[~p -> ~p] - Regen=~p",
           [Method, IKey, CKey,
            IM, IMNew, IMNew - IM,
            CM, CMNew, CMNew - CM]),
    %clean up
    case Method of
        art ->
            % ART is not able to detect this case and resolve everything
            ?compare(fun erlang:'>'/2, CMNew, CM);
        _   -> ?equals(CMNew, IM)
    end.

parts(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    OldConf = get_rep_upd_config(Method),
    Conf = lists:keyreplace(rr_max_items, 1, OldConf, {rr_max_items, 500}),
    start_sync(Config, 4, 1000, [{fprob, 100}, {ftype, FType}],
               2, 0.2, Conf, fun erlang:'<'/2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec wait_until_true(DestKey::?RT:key(), Request::comm:message(),
                      ConFun::fun((StateResponse::term()) -> boolean()),
                      MaxWait::integer()) -> boolean().
wait_until_true(DestKey, Request, ConFun, MaxWait) ->
    api_dht_raw:unreliable_lookup(DestKey, Request),
    trace_mpath:thread_yield(),
    Result = receive ?SCALARIS_RECV({get_state_response, R}, ConFun(R)) end,
    if Result -> true;
       not Result andalso MaxWait > 0 ->
           erlang:yield(),
           timer:sleep(10),
           wait_until_true(DestKey, Request, ConFun, MaxWait - 10);
       not Result andalso MaxWait =< 0 ->
           false
    end.

session_ttl(Config) ->
    %parameter
    NodeCount = 7,
    DataCount = 1000,
    Method = merkle_tree,
    FType = mixed,
    TTL = 2000,

    RRConf1 = lists:keyreplace(rr_session_ttl, 1, get_rep_upd_config(Method), {rr_session_ttl, TTL div 2}),
    RRConf = lists:keyreplace(rr_gc_interval, 1, RRConf1, {rr_gc_interval, erlang:round(TTL div 2)}),

    %build and fill ring
    _ = build_ring(NodeCount, Config, RRConf),
    _ = db_generator:fill_ring(random, DataCount, [{ftype, FType},
                                                   {fprob, 90},
                                                   {distribution, uniform}]),
    %chose node pair
    SKey = ?RT:get_random_node_id(),
    CKey = util:randomelem(lists:delete(SKey, ?RT:get_replica_keys(SKey))),

    api_dht_raw:unreliable_lookup(CKey, {get_pid_group, comm:this()}),
    CName = receive {get_pid_group_response, Key} -> Key end,

    %server starts sync
    ?proto_sched(start),
    api_dht_raw:unreliable_lookup(SKey, {?send_to_group_member, rrepair,
                                              {request_sync, Method, CKey}}),
    Req = {?send_to_group_member, rrepair, {get_state, comm:this(), open_sessions}},
    SessionOpened = wait_until_true(SKey, Req, fun(X) -> length(X) =/= 0 end, TTL),
    ?proto_sched(stop),

    %check timeout
    api_vm:kill_node(CName),
    timer:sleep(TTL),
    api_dht_raw:unreliable_lookup(SKey, Req),
    SessionGarbageCollected = receive {get_state_response, R2} -> length(R2) =:= 0 end,
    case SessionOpened of
        true ->
            ?equals(SessionGarbageCollected, SessionOpened);
        false ->
            ct:pal("Session finished before client node could be killed.")
    end,
    ok.

asymmetric_ring(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    start_sync(Config, 4, 1000, [{fprob, 10}, {ftype, FType}],
               1, 0.01, get_rep_upd_config(Method), fun erlang:'=<'/2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc
%    runs the bloom filter synchronization [Rounds]-times
%    and records the sync degree after each round
%    returns list of sync degrees per round, first value is initial sync degree
% @end
-spec start_sync(Config, Nodes::Int | symmetric, DBSize::Int, DBParams,
                 Rounds::Int, FR, RRConf::Config, CompFun) -> true when
    is_subtype(Config,      [tuple()]),
    is_subtype(Int,         pos_integer()),
    is_subtype(DBParams,    [db_generator:db_parameter()]),
    is_subtype(FR,         float()),
    is_subtype(CompFun,     fun((T, T) -> boolean())).
start_sync(Config, NodeCount, DBSize, DBParams, Rounds, FR, RRConfig, CompFun) ->
    % NOTE: a sync round may not decrease the sync degree if there is no error on the participating nodes!
    NodeKeys = build_ring(NodeCount, Config, [RRConfig, {rr_recon_failure_rate, FR}]),
    Nodes = [begin
                 comm:send_local(NodePid, {get_node_details, comm:this(), [node]}),
                 trace_mpath:thread_yield(),
                 receive
                     ?SCALARIS_RECV({get_node_details_response, NodeDetails},
                                    begin
                                        Node = node_details:get(NodeDetails, node),
                                        Pid = comm:make_local(node:pidX(Node)),
                                        {node:id(Node), Pid, pid_groups:group_of(Pid)}
                                    end);
                     ?SCALARIS_RECV(Y, ?ct_fail("unexpected message while "
                                                "waiting for get_node_details_response: ~.0p",
                                                [Y]))
                 end
             end || NodePid <- pid_groups:find_all(dht_node)],
    ct:pal(">>Nodes: ~.2p", [Nodes]),
    _ = db_generator:fill_ring(random, DBSize, DBParams),
    InitDBStat = get_db_status(),
    print_status(0, InitDBStat),
    ?proto_sched(start),
    _ = util:for_to_ex(1, Rounds,
                       fun(I) ->
                               ct:pal("Starting round ~p", [I]),
                               startSyncRound(NodeKeys),
                               waitForSyncRoundEnd(NodeKeys, false),
                               if I =/= Rounds ->
                                      print_status(I, get_db_status());
                                  true -> ok
                               end
                       end),
    ?proto_sched(stop),
    EndStat = get_db_status(),
    print_status(Rounds, EndStat),
    ?compare_w_note(CompFun, sync_degree(InitDBStat), sync_degree(EndStat),
                    io_lib:format("CompFun: ~p", [CompFun])),
    unittest_helper:stop_ring(),
    true.

-spec print_status(Round::integer(), db_generator:db_status()) -> ok.
print_status(R, {_, _, M, O}) ->
    ct:pal(">>SYNC RUN [Round ~p] Missing=[~p] Outdated=[~p]", [R, M, O]).

-spec create_full_db(RingData::unittest_helper:ring_data([{?RT:key(), client_version()}]),
                     Keys, KeysNum, Outdated)
        -> {Keys, KeysNum, Outdated}
          when is_subtype(Keys, [?RT:key()]),
               is_subtype(KeysNum, non_neg_integer()),
               is_subtype(Outdated, non_neg_integer()).
create_full_db([{_Pid, _I, DB, _Pred, _Succ, ok} | Rest], Keys, KeysNum, Outdated) ->
    {Keys2, KeysNum2, Outdated2} = create_full_db2(DB, Keys, KeysNum, Outdated),
    create_full_db(Rest, Keys2, KeysNum2, Outdated2);
create_full_db([{exception, _Level, _Reason, _Stacktrace} | Rest], Keys, KeysNum, Outdated) ->
    create_full_db(Rest, Keys, KeysNum, Outdated);
create_full_db([], Keys, KeysNum, Outdated) ->
    {Keys, KeysNum, Outdated}.

-spec create_full_db2(DB::db_dht:db_as_list(), Keys, KeysNum, Outdated)
        -> {Keys, KeysNum, Outdated}
          when is_subtype(Keys, [?RT:key()]),
               is_subtype(KeysNum, non_neg_integer()),
               is_subtype(Outdated, non_neg_integer()).
create_full_db2([H | TL], Keys, KeysNum, Outdated) ->
    Key = rr_recon:map_key_to_quadrant(element(1, H), 1),
    Version = element(2, H),
    DictKey = {'$test_full_db', Key},
    case erlang:get(DictKey) of
        Version ->
            % note: Key already in Keys
            create_full_db2(TL, Keys, KeysNum, Outdated);
        undefined ->
            _ = erlang:put(DictKey, Version),
            create_full_db2(TL, [Key | Keys], KeysNum + 1, Outdated);
        VOld when VOld < Version ->
            _ = erlang:put(DictKey, Version),
            % note: Key already in Keys
            create_full_db2(TL, Keys, KeysNum, Outdated + 1);
        _VOld -> %when VOld > Version ->
            % note: Key already in Keys
            create_full_db2(TL, Keys, KeysNum, Outdated + 1)
    end;
create_full_db2([], Keys, KeysNum, Outdated) ->
    {Keys, KeysNum, Outdated}.

-spec remove_full_db([?RT:key()]) -> ok.
remove_full_db(Keys) ->
    lists:foreach(fun(Key) -> erlang:erase({'$test_full_db', Key}) end, Keys),
    ok.

%% @doc Counts outdated items on the node responsible for the given key.
%%      PreCond: full DB in process dictionary - see create_full_db/2!
-spec count_outdated(RingData::unittest_helper:ring_data([{?RT:key(), client_version()}]),
                     Node::?RT:key()) -> non_neg_integer().
count_outdated(RingData, NodeKey) ->
    N = lists:filter(fun({_Pid, {LBr, LK, RK, RBr}, _DB, _Pred, _Succ, ok}) ->
                             intervals:in(NodeKey, intervals:new(LBr, LK, RK, RBr))
                     end, RingData),
    case N of
        [{_Pid, _I, DB, _Pred, _Succ, ok}] ->
            lists:foldl(
              fun(E, Acc) ->
                      Key = rr_recon:map_key_to_quadrant(element(1, E), 1),
                      MyVersion = element(2, E),
                      UpdVersion = erlang:get({'$test_full_db', Key}),
                      ?DBG_ASSERT(UpdVersion =/= undefined),
                      if UpdVersion > MyVersion -> Acc + 1;
                         true -> Acc
                      end
              end, 0, DB);
        _ -> 0
    end.

-spec get_node_list() -> [comm:mypid()].
get_node_list() ->
    mgmt_server:node_list(),
    receive
        {get_list_response, N} -> N
    end.

%% @doc Counts db size on the node responsible for the given key.
-spec count_dbsize(RingData::unittest_helper:ring_data([{?RT:key(), client_version()}]),
                   Node::?RT:key()) -> non_neg_integer().
count_dbsize(RingData, NodeKey) ->
    N = lists:filter(
          fun({_Pid, {LBr, LK, RK, RBr}, _DB, _Pred, _Succ, ok}) ->
                  intervals:in(NodeKey, intervals:new(LBr, LK, RK, RBr))
          end, RingData),
    case N of
        [{_Pid, _I, DB, _Pred, _Succ, ok}] -> length(DB);
        _ -> 0
    end.

-spec get_db_status() -> db_generator:db_status().
get_db_status() ->
    RingData = unittest_helper:get_ring_data(kv),
    Stored =
        lists:foldl(
          fun({_Pid, _I, DB, _Pred, _Succ, ok}, Acc) ->
                  length(DB) + Acc;
             ({exception, _Level, _Reason, _Stacktrace}, Acc) ->
                  Acc
          end, 0, RingData),
    {DBKeys, DBKeysNum, Outdated} = create_full_db(RingData, [], 0, 0),
    DBSize = config:read(replication_factor) * DBKeysNum,
    remove_full_db(DBKeys),
    {DBSize, Stored, DBSize - Stored, Outdated}.

build_ring(NodeCount0, Config, RRConfig) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    case NodeCount0 of
        symmetric ->
            % Build ring with NodeCount symmetric nodes
            unittest_helper:make_symmetric_ring(
              [{config, lists:flatten([{log_path, PrivDir}, RRConfig])}]),
            NodeCount = config:read(replication_factor),
            ok;
        NodeCount when is_integer(NodeCount) andalso NodeCount > 0 ->
            % Build ring with NodeCount arbitrary nodes
            Config2 = unittest_helper:start_minimal_procs(Config, [], false),
            NodeKeys0 = util:for_to_ex(1, NodeCount, fun(_) -> ?RT:get_random_node_id() end),
            _ = unittest_helper:stop_minimal_procs(Config2),
            unittest_helper:make_ring_with_ids(
              NodeKeys0,
              [{config, lists:flatten([{log_path, PrivDir}, RRConfig])}])
    end,
    % wait for all nodes to finish their join
    unittest_helper:check_ring_size_fully_joined(NodeCount),
%%     % wait a bit for the rm-processes to settle
%%     timer:sleep(500),
    _NodeKeys = [begin
                     comm:send_local(Pid, {get_state, comm:this(), node_id}),
                     receive
                         ?SCALARIS_RECV({get_state_response, Key}, Key)
                     end
                 end || Pid <- pid_groups:find_all(dht_node)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Analysis
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec startSyncRound(NodeKeys::[?RT:key()]) -> ok.
startSyncRound(NodeKeys) ->
    lists:foreach(
      fun(X) ->
              Req = {?send_to_group_member, rrepair,
                     {request_sync, config:read(rr_recon_method), random}},
              api_dht_raw:unreliable_lookup(X, Req)
      end,
      NodeKeys),
    ok.

-spec waitForSyncRoundEnd(NodeKeys::[?RT:key()], RcvReqCompleteMsg::boolean()) -> ok.
waitForSyncRoundEnd(NodeKeys, RcvReqCompleteMsg) ->
    case RcvReqCompleteMsg of
        true ->
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV(
                   {request_sync_complete = _MsgTag, _}, % ->
                   ok)
                end;
        false -> ok
    end,
    Req = {?send_to_group_member, rrepair,
           {get_state, comm:this(), [open_sessions, open_recon, open_resolve]}},
    util:wait_for(fun() -> wait_for_sync_round_end2(Req, NodeKeys) end, 100).

-spec wait_for_sync_round_end2(Req::comm:message(), [?RT:key()]) -> boolean().
wait_for_sync_round_end2(_Req, []) -> true;
wait_for_sync_round_end2(Req, [Key | Keys]) ->
    api_dht_raw:unreliable_lookup(Key, Req),
    KeyResult =
        begin
        trace_mpath:thread_yield(),
        receive
            ?SCALARIS_RECV(
            {get_state_response = _MsgTag, [Sessions, ORC, ORS]}, % ->
            begin
                if (ORC =:= 0 andalso ORS =:= 0 andalso
                        Sessions =:= []) ->
                       true;
                   true ->
%%                        log:pal("Key: ~.2p~nOS : ~.2p~nORC: ~p, ORS: ~p~n",
%%                                [Key, Sessions, ORC, ORS]),
                       false
                end
            end)
        end end,
    if KeyResult -> wait_for_sync_round_end2(Req, Keys);
       true -> false
    end.

-spec sync_degree(db_generator:db_status()) -> float().
sync_degree({Count, _Ex, M, O}) ->
    (Count - M - O) / Count.

