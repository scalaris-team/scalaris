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
-include("record_helpers.hrl").

-define(REP_FACTOR, 4).
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
     parts           % get_chunk with limited items (leads to multiple get_chunk calls, in case of bloom also multiple bloom filters)
    ].

regen_special() ->
    [
     dest_empty_node % run one sync with empty dest node
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    tester:register_type_checker({typedef, intervals, interval}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, continuous_interval}, intervals, is_continuous),
    tester:register_type_checker({typedef, intervals, non_empty_interval}, intervals, is_non_empty),
    tester:register_value_creator({typedef, intervals, interval}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval}, intervals, tester_create_continuous_interval, 4),
    tester:register_value_creator({typedef, intervals, non_empty_interval}, intervals, tester_create_non_empty_interval, 2),
    Config2.

end_per_suite(Config) ->
    erlang:erase(?DBSizeKey),
    tester:unregister_type_checker({typedef, intervals, interval}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval}),
    tester:unregister_type_checker({typedef, intervals, non_empty_interval}),
    tester:unregister_value_creator({typedef, intervals, interval}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval}),
    tester:unregister_value_creator({typedef, intervals, non_empty_interval}),
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_group(Group, Config) ->
    ct:comment(io_lib:format("BEGIN ~p", [Group])),
    case Group of
        upd_trivial -> [{ru_method, trivial}, {ftype, update}];
        upd_bloom -> [{ru_method, bloom}, {ftype, update}];
        upd_merkle -> [{ru_method, merkle_tree}, {ftype, update}];
        upd_art -> [{ru_method, art}, {ftype, update}];
        regen_trivial -> [{ru_method, trivial}, {ftype, regen}];
        regen_bloom -> [{ru_method, bloom}, {ftype, regen}];
        regen_merkle -> [{ru_method, merkle_tree}, {ftype, regen}];
        regen_art -> [{ru_method, art}, {ftype, regen}];
        mixed_trivial -> [{ru_method, trivial}, {ftype, mixed}];
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
    proplists:delete(ru_method, Config).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_minimal_procs(Config),
    unittest_helper:stop_ring(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_rep_upd_config(Method) ->
    [{rrepair_enabled, true},
     {rr_trigger_interval, 0}, %stop trigger
     {rr_recon_method, Method},
     {rr_session_ttl, 100000},
     {rr_gc_interval, 60000},
     {rr_recon_p1e, 0.1},
     {rr_trigger_probability, 100},
     {rr_art_inner_fpr, 0.01},
     {rr_art_leaf_fpr, 0.1},
     {rr_art_correction_factor, 2},
     {rr_merkle_branch_factor, 2},
     {rr_merkle_bucket_size, 25}].

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
    P1E = 0.1,
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    TraceName = erlang:list_to_atom(atom_to_list(Method)++atom_to_list(FType)),
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, [get_rep_upd_config(Method), {rr_recon_p1e, P1E}]),
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
    A = trace_mpath:get_trace(TraceName),
    trace_mpath:cleanup(TraceName),
    B = [X || X = {log_send, _Time, _TraceID,
                   {{_FIP,_FPort,_FPid}, _FName},
                   {{_TIP,_TPort,_TPid}, _TName},
                   _Msg, _LocalOrGlobal} <- A],
    ok = file:write_file("TRACE_" ++ atom_to_list(TraceName) ++ ".txt", io_lib:fwrite("~.0p\n", [B])),
    ok = file:write_file("TRACE_HISTO_" ++ atom_to_list(TraceName) ++ ".txt", io_lib:fwrite("~.0p\n", [trace_mpath:send_histogram(B)])),
    %ok = file:write_file("TRACE_EVAL_" ++ atom_to_list(TraceName) ++ ".txt", io_lib:fwrite("~.0p\n", [rr_eval_admin:get_bandwidth(A)])),
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
    P1E = 0.1,
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, [get_rep_upd_config(Method), {rr_recon_p1e, P1E}]),
    _ = db_generator:fill_ring(random, DataCount, [{ftype, FType},
                                                   {fprob, 50},
                                                   {distribution, uniform}]),
    %chose node pair
    SKey = ?RT:get_random_node_id(),
    CKey = util:randomelem(lists:delete(SKey, ?RT:get_replica_keys(SKey))),
    %measure initial sync degree
    SO = count_outdated(SKey),
    SM = count_dbsize(SKey),
    CO = count_outdated(CKey),
    CM = count_dbsize(CKey),
    %server starts sync
    ?proto_sched(start),
    api_dht_raw:unreliable_lookup(SKey, {?send_to_group_member, rrepair,
                                              {request_sync, Method, CKey, comm:this()}}),
    waitForSyncRoundEnd([SKey, CKey], true),
    ?proto_sched(stop),
    %measure sync degree
    SONew = count_outdated(SKey),
    SMNew = count_dbsize(SKey),
    CONew = count_outdated(CKey),
    CMNew = count_dbsize(CKey),
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
    NodeCount = 4,
    DataCount = 1000,
    P1E = 0.1,
    Method = proplists:get_value(ru_method, Config),
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, [get_rep_upd_config(Method), {rr_recon_p1e, P1E}]),
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
    IM = count_dbsize(IKey),
    CM = count_dbsize(CKey),
    %server starts sync
    ?proto_sched(start),
    api_dht_raw:unreliable_lookup(IKey, {?send_to_group_member, rrepair,
                                              {request_sync, Method, CKey, comm:this()}}),
    waitForSyncRoundEnd([IKey, CKey], true),
    ?proto_sched(stop),
    %measure sync degree
    IMNew = count_dbsize(IKey),
    CMNew = count_dbsize(CKey),
    ct:pal("SYNC RUN << ~p >>~nServerKey=~p~nClientKey=~p~n"
           "Server DBSize=[~p -> ~p] - Regen=~p~n"
           "Client DBSize=[~p -> ~p] - Regen=~p",
           [Method, IKey, CKey,
            IM, IMNew, IMNew - IM,
            CM, CMNew, CMNew - CM]),
    %clean up
    ?equals(CM, 0),
    ?compare(fun erlang:'>'/2, CMNew, CM).

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
    build_symmetric_ring(NodeCount, Config, RRConf),
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc
%    runs the bloom filter synchronization [Rounds]-times
%    and records the sync degree after each round
%    returns list of sync degrees per round, first value is initial sync degree
% @end
-spec start_sync(Config, Nodes::Int, DBSize::Int, DBParams,
                 Rounds::Int, P1E, RRConf::Config, CompFun) -> true when
    is_subtype(Config,      [tuple()]),
    is_subtype(Int,         pos_integer()),
    is_subtype(DBParams,    [db_generator:db_parameter()]),
    is_subtype(P1E,         float()),
    is_subtype(CompFun,     fun((T, T) -> boolean())).
start_sync(Config, NodeCount, DBSize, DBParams, Rounds, P1E, RRConfig, CompFun) ->
    NodeKeys = lists:sort(get_symmetric_keys(NodeCount)),
    build_symmetric_ring(NodeCount, Config, [RRConfig, {rr_recon_p1e, P1E}]),
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
    erlang:put(?DBSizeKey, ?REP_FACTOR * DBSize),
    _ = db_generator:fill_ring(random, DBSize, DBParams),
    InitDBStat = get_db_status(),
    print_status(0, InitDBStat),
    _ = util:for_to_ex(1, Rounds,
                       fun(I) ->
                               ct:pal("Starting round ~p", [I]),
                               ?proto_sched(start),
                               startSyncRound(NodeKeys),
                               waitForSyncRoundEnd(NodeKeys, false),
                               ?proto_sched(stop),
                               print_status(I, get_db_status())
                       end),
    EndStat = get_db_status(),
    ?compare_w_note(CompFun, sync_degree(InitDBStat), sync_degree(EndStat),
                    io_lib:format("CompFun: ~p", [CompFun])),
    unittest_helper:stop_ring(),
    true.

-spec print_status(Round::integer(), db_generator:db_status()) -> ok.
print_status(R, {_, _, M, O}) ->
    ct:pal(">>SYNC RUN [Round ~p] Missing=[~p] Outdated=[~p]", [R, M, O]).

-spec count_outdated(?RT:key()) -> non_neg_integer().
count_outdated(Key) ->
    Req = {rr_stats, {count_old_replicas, comm:this(), intervals:all()}},
    api_dht_raw:unreliable_lookup(Key, {?send_to_group_member, rrepair, Req}),
    receive
        {count_old_replicas_reply, Old} -> Old
    end.

-spec count_outdated() -> non_neg_integer().
count_outdated() ->
    Req = {rr_stats, {count_old_replicas, comm:this(), intervals:all()}},
    lists:foldl(
      fun(Node, Acc) ->
              comm:send(Node, {?send_to_group_member, rrepair, Req}),
              receive
                  {count_old_replicas_reply, Old} -> Acc + Old
              end
      end,
      0, get_node_list()).

-spec get_node_list() -> [comm:mypid()].
get_node_list() ->
    mgmt_server:node_list(),
    receive
        {get_list_response, N} -> N
    end.

% @doc counts db size on node responsible for key
-spec count_dbsize(?RT:key()) -> non_neg_integer().
count_dbsize(Key) ->
    RingData = unittest_helper:get_ring_data(),
    N = lists:filter(fun({_Pid, {LBr, LK, RK, RBr}, _DB, _Pred, _Succ, ok}) ->
                             intervals:in(Key, intervals:new(LBr, LK, RK, RBr))
                     end, RingData),
    case N of
        [{_Pid, _I, DB, _Pred, _Succ, ok}] -> length(DB);
        _ -> 0
    end.

-spec get_db_status() -> db_generator:db_status().
get_db_status() ->
    DBSize = erlang:get(?DBSizeKey),
    Ring = statistics:get_ring_details(),
    Stored = statistics:get_total_load(load, Ring),
    {DBSize, Stored, DBSize - Stored, count_outdated()}.

-spec get_symmetric_keys(pos_integer()) -> [?RT:key()].
get_symmetric_keys(NodeCount) ->
    [element(2, intervals:get_bounds(I)) || I <- intervals:split(intervals:all(), NodeCount)].

build_symmetric_ring(NodeCount, Config, RRConfig) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    %Build ring with NodeCount symmetric nodes
    unittest_helper:make_ring_with_ids(
      fun() -> get_symmetric_keys(NodeCount) end,
      [{config, lists:flatten([{log_path, PrivDir}, RRConfig])}]),
    % wait for all nodes to finish their join
    unittest_helper:check_ring_size_fully_joined(NodeCount),
%%     % wait a bit for the rm-processes to settle
%%     timer:sleep(500),
    ok.

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

