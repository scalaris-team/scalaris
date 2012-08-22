%  @copyright 2010-2012 Zuse Institute Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    rrepair_SUITE.erl
%%% @author Maik Lange <malange@informatik.hu-berlin.de
%%% @doc    Tests for rep update module.
%%% @end
%%% Created : 2011-05-27
%%%-------------------------------------------------------------------
%% @version $Id $

-module(rrepair_SUITE).

-author('malange@informatik.hu-berlin.de').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").
-include("record_helpers.hrl").

-define(REP_FACTOR, 4).
-define(DBSizeKey, rrepair_SUITE_dbsize).    %Process Dictionary Key for generated db size

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

basic_tests() ->
    [get_symmetric_keys_test,
     tester_blob_coding,     
     tester_get_key_quadrant,
     tester_map_interval,
     tester_map_key_to_interval,
     tester_find_intersection
    ].

repair_tests() ->
    [no_diff,        % ring is not out of sync e.g. no outdated or missing replicas
     one_node,       % sync in ring with only one node
     %mpath
     dest,           % run one sync with a specified dest node 
     simple,         % run one sync round
     multi_round,    % run multiple sync rounds with sync probability 1
     multi_round2    % run multiple sync rounds with sync probability 0.4
	].

bloom_tests() ->    
    repair_tests() ++ 
        [parts]. % get_chunk with limited items / leads to multiple bloom filters

all() ->
    [{group, basic},
     session_ttl,
     {group, repair}     
     ].

groups() ->
    [{basic,  [parallel], basic_tests()},     
     {repair, [sequence], [{upd_bloom,    [sequence], bloom_tests()}, %{repeat_until_any_fail, 1000}
                           {upd_merkle,   [sequence], repair_tests()},
                           {upd_art,      [sequence], repair_tests()},
                           {regen_bloom,  [sequence], bloom_tests()},
                           {regen_merkle, [sequence], repair_tests()},
                           {regen_art,    [sequence], repair_tests()},
                           {mixed_bloom,  [sequence], bloom_tests()}, 
                           {mixed_merkle, [sequence], repair_tests()},
                           {mixed_art,    [sequence], repair_tests()}]}
    ].

suite() ->
    [
     {timetrap, {seconds, 240}}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_suite(Config) ->
    _ = crypto:start(),    
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    erlang:erase(?DBSizeKey),
    crypto:stop(),
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_group(Group, Config) ->
    ct:comment(io_lib:format("BEGIN ~p", [Group])),
    case Group of
        upd_bloom -> [{ru_method, bloom}, {ftype, update}];
        upd_merkle -> [{ru_method, merkle_tree}, {ftype, update}];
        upd_art -> [{ru_method, art}, {ftype, update}];
        regen_bloom -> [{ru_method, bloom}, {ftype, regen}];
        regen_merkle -> [{ru_method, merkle_tree}, {ftype, regen}];
        regen_art -> [{ru_method, art}, {ftype, regen}];
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

end_per_testcase(_TestCase, _Config) ->
    unittest_helper:stop_ring(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_rep_upd_config(Method) ->
    [{rrepair_enabled, true},
     {rr_trigger, trigger_periodic},
     {rr_trigger_interval, 100000000}, %stop trigger
     {rr_recon_method, Method},
     {rr_session_ttl, 100000},
     {rr_gc_interval, 60000},
     {rr_bloom_fpr, 0.1},
	 {rr_trigger_probability, 100},
     {rr_max_items, 10000},
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
    {Start, End} = start_sync(Config, 4, 1000, [{fprob, 0}, {ftype, FType}], 
                              1, 0.1, get_rep_upd_config(Method)),
    ?assert(sync_degree(Start) =:= sync_degree(End)).

one_node(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    {Start, End} = start_sync(Config, 1, 1, [{fprob, 50}, {ftype, FType}], 
                              1, 0.2, get_rep_upd_config(Method)),
    ?assert(sync_degree(Start) =:= sync_degree(End)).    

mpath_map({request_resolve, _, {key_upd, L}, _}) ->
    {key_upd, length(L)};
mpath_map(Msg) ->
    {element(1, Msg)}.

mpath(Config) ->
	%parameter
    NodeCount = 4,
    DataCount = 1000,
    Fpr = 0.1,
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
	TraceName = erlang:list_to_atom(atom_to_list(Method)++atom_to_list(FType)),
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, get_rep_upd_config(Method)),
    config:write(rr_bloom_fpr, Fpr),
    db_generator:fill_ring(random, DataCount, [{ftype, FType}, 
                                               {fprob, 50}, 
                                               {distribution, uniform}]),
    %chose node pair    
    SKey = ?RT:get_random_node_id(),
    CKey = util:randomelem(lists:delete(SKey, ?RT:get_replica_keys(SKey))),
    %server starts sync
	%trace_mpath:start(TraceName, fun mpath_map/1),
    trace_mpath:start(TraceName),
    api_dht_raw:unreliable_lookup(SKey, {send_to_group_member, rrepair, 
                                              {request_sync, Method, CKey}}),
    %waitForSyncRoundEnd(NodeKeys),
	timer:sleep(3000),
	trace_mpath:stop(),
	%TRACE
	A = trace_mpath:get_trace(TraceName),
    trace_mpath:cleanup(TraceName),
	B = [X || X = {log_send, _Time, _, 
				   {{_FIP,_FPort,_FPid}, _FName}, 
				   {{_TIP,_TPort,_TPid}, _TName}, 
				   _Msg} <- A],
	file:write_file("TRACE_" ++ atom_to_list(TraceName) ++ ".txt", io_lib:fwrite("~.0p\n", [B])), 
	file:write_file("TRACE_HISTO_" ++ atom_to_list(TraceName) ++ ".txt", io_lib:fwrite("~.0p\n", [trace_mpath:send_histogram(B)])),
    %file:write_file("TRACE_EVAL_" ++ atom_to_list(TraceName) ++ ".txt", io_lib:fwrite("~.0p\n", [eval_admin:get_bandwidth(A)])),  
	ok.

simple(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    {Start, End} = start_sync(Config, 4, 1000, [{fprob, 10}, {ftype, FType}], 
                              1, 0.1, get_rep_upd_config(Method)),
    ?assert(sync_degree(Start) < sync_degree(End)).

multi_round(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    {Start, End} = start_sync(Config, 6, 1000, [{fprob, 10}, {ftype, FType}], 
                              3, 0.1, get_rep_upd_config(Method)),
    ?assert(sync_degree(Start) < sync_degree(End)).

multi_round2(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    _RUConf = get_rep_upd_config(Method),
    RUConf = [{rr_trigger_probability, 40} | proplists:delete(rr_trigger_probability, _RUConf)],
    {Start, End} = start_sync(Config, 6, 1000, [{fprob, 10}, {ftype, FType}], 
                              3, 0.1, RUConf),
    ?assert(sync_degree(Start) < sync_degree(End)). 

dest(Config) ->
    %parameter
    NodeCount = 7,
    DataCount = 1000,
    Fpr = 0.1,
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, get_rep_upd_config(Method)),
    config:write(rr_bloom_fpr, Fpr),
    db_generator:fill_ring(random, DataCount, [{ftype, FType}, 
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
    api_dht_raw:unreliable_lookup(SKey, {send_to_group_member, rrepair, 
                                              {request_sync, Method, CKey}}),
    %waitForSyncRoundEnd(NodeKeys),
    waitForSyncRoundEnd([SKey, CKey]),
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

parts(Config) ->
    Method = proplists:get_value(ru_method, Config),
    FType = proplists:get_value(ftype, Config),
    OldConf = get_rep_upd_config(Method),
    Conf = lists:keyreplace(rr_max_items, 1, OldConf, {rr_max_items, 500}),    
    {Start, End} = start_sync(Config, 4, 1000, [{fprob, 100}, {ftype, FType}], 
                              2, 0.2, Conf),
    ?assert(sync_degree(Start) < sync_degree(End)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Basic Functions Group
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

get_symmetric_keys_test(Config) ->
    Conf2 = unittest_helper:start_minimal_procs(Config, [], true),
    ToTest = lists:sort(get_symmetric_keys(4)),
    ToBe = lists:sort(?RT:get_replica_keys(?MINUS_INFINITY)),
    unittest_helper:stop_minimal_procs(Conf2),
    ?equals_w_note(ToTest, ToBe, 
                   io_lib:format("GenKeys=~w~nRTKeys=~w", [ToTest, ToBe])),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_blob_coding(?RT:key(), ?DB:value() | ?DB:version()) -> boolean().
prop_blob_coding(A, B) ->
    Coded = rr_recon:encodeBlob(A, B),
    {DA, DB} = rr_recon:decodeBlob(Coded),
    ?equals_w_note(A, DA, 
                   io_lib:format("A=~p ; Coded=~p ; DecodedA=~p", [A, Coded, DA])) 
        andalso ?equals_w_note(B, DB, 
                               io_lib:format("B=~p ; Coded=~p ; DecodedB=~p", [B, Coded, DB])).

tester_blob_coding(_) ->
    tester:test(?MODULE, prop_blob_coding, 2, 50, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

session_ttl(Config) ->
    %parameter
    NodeCount = 7,
    DataCount = 1000,
    Method = bloom,
    FType = mixed,
    TTL = 2500,
    
    _R1 = get_rep_upd_config(Method),
    _R2 = lists:keyreplace(rr_session_ttl, 1, _R1, {rr_session_ttl, TTL / 2}),
    RRConf = lists:keyreplace(rr_gc_interval, 1, _R2, {rr_gc_interval, erlang:round(TTL / 10)}),
    
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, RRConf),    
    db_generator:fill_ring(random, DataCount, [{ftype, FType}, 
                                               {fprob, 50}, 
                                               {distribution, uniform}]),
    %chose node pair
    SKey = ?RT:get_random_node_id(),
    CKey = util:randomelem(lists:delete(SKey, ?RT:get_replica_keys(SKey))),
    
    api_dht_raw:unreliable_lookup(CKey, {get_pid_group, comm:this()}),    
    CName = receive {get_pid_group_response, Key} -> Key end,
    
    %server starts sync
    api_dht_raw:unreliable_lookup(SKey, {send_to_group_member, rrepair, 
                                              {request_sync, Method, CKey}}),
    api_vm:kill_node(CName),

    %check timeout
    Req = {send_to_group_member, rrepair, {get_state, comm:this(), open_sessions}},
    api_dht_raw:unreliable_lookup(SKey, Req),
    Open = receive {get_state_response, R1} -> R1 =/= 0 end,
    timer:sleep(TTL),
    api_dht_raw:unreliable_lookup(SKey, Req),
    Open2 = receive {get_state_response, R2} -> R2 =:= 0 end,
    ?equals_pattern_w_note(Open, Open2, io_lib:format("start session open = ~p - end session closed = ~p", [Open, Open2])),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_get_key_quadrant(?RT:key()) -> boolean().
prop_get_key_quadrant(Key) ->
    Q = rr_recon:get_key_quadrant(Key),
    QI = intervals:split(intervals:all(), 4),
    {TestStatus, TestQ} = 
        lists:foldl(fun(I, {Status, Nr} = Acc) ->
                            case intervals:in(Key, I) of
                                true when Status =:= no -> {yes, Nr};
                                false when Status =:= no -> {no, Nr + 1};
                                _ -> Acc
                            end
                    end, {no, 1}, QI),
    ?assert(Q > 0 andalso Q =< ?REP_FACTOR) andalso
        ?equals(TestStatus, yes) andalso
        ?equals_w_note(TestQ, Q, 
                       io_lib:format("Quadrants=~p~nKey=~w~nQuadrant=~w~nCheckQuadrant=~w", 
                                     [QI, Key, Q, TestQ])).

tester_get_key_quadrant(_) ->
    _ = [prop_get_key_quadrant(Key) || Key <- ?RT:get_replica_keys(?MINUS_INFINITY)],
    tester:test(?MODULE, prop_get_key_quadrant, 1, 16, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_map_interval(intervals:key(), intervals:key(), 1..4) -> boolean().
prop_map_interval(L, R, Q) ->
    I = unittest_helper:build_interval(L, R),
    Mapped = rr_recon:mapInterval(I, Q),
    {LBr, L1, R1, RBr} = intervals:get_bounds(Mapped),
    LQ = rr_recon:get_key_quadrant(L1),
    RQ = rr_recon:get_key_quadrant(R1),
    L2 = ?RT:get_split_key(L1, ?RT:get_split_key(L1, R1, {1, 100}), {1,100}),
    L2Q = rr_recon:get_key_quadrant(L2),
    ?equals(L2Q, Q) andalso
        ?implies(LBr =:= '[', ?equals(LQ, Q)) andalso        
        ?implies(RBr =:= ']', ?equals(RQ, Q)) andalso
        ?implies(LBr =:= '[' andalso RBr =:= LBr, ?equals(LQ, RQ) andalso ?equals(LQ, Q)) andalso
        ?equals(rr_recon:get_interval_quadrant(Mapped), Q).
    
tester_map_interval(_) ->
    _ = [prop_map_interval(?MINUS_INFINITY, ?PLUS_INFINITY, I) || I <- lists:seq(1, 4)],
    tester:test(?MODULE, prop_map_interval, 3, 16, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_map_key_to_interval(intervals:key(), intervals:key(), ?RT:key()) -> boolean().
prop_map_key_to_interval(L, R, Key) ->
    I = unittest_helper:build_interval(L, R),
    Mapped = rr_recon:map_key_to_interval(Key, I),
    case intervals:in(Key, I) of
        true -> ?equals_w_note(Mapped, Key,
                               io_lib:format("Violation: if key is in i than mapped key equals key!~nKey=~p~nMapped=~p", [Key, Mapped]));
        false when Mapped =/= none ->
            RGrp = ?RT:get_replica_keys(Key),
            InGrp = [X || X <- RGrp, intervals:in(X, I)],
            case InGrp of
                [] -> true;
                [W] -> ?equals(Mapped, W);
                [_|_] ->
                    NotIn = [Y || Y <- RGrp, Y =/= Key, not intervals:in(Y, I)],
                    [?assert(rr_recon:map_key_to_interval(Z, I) =/= Mapped) || Z <- NotIn], 
                    ?assert(intervals:in(Mapped, I))
            end;
        _ -> true
    end.

tester_map_key_to_interval(_) ->
    tester:test(?MODULE, prop_map_key_to_interval, 3, 40, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_find_intersection(ALeft::intervals:key(), ARight::intervals:key(), 
                             BLeft::intervals:key(), BRight::intervals:key()) -> boolean().
prop_find_intersection(KeyA, KeyB, KeyC, KeyD) ->
    A = unittest_helper:build_interval(KeyA, KeyB),
    B = unittest_helper:build_interval(KeyC, KeyD),
    SA = rr_recon:find_intersection(A, B),
    SB = rr_recon:find_intersection(B, A),
    IS = intervals:intersection(A, B),
    SizeSA = rr_recon:get_interval_size(SA),
    SizeSB = rr_recon:get_interval_size(SB),
    ?implies(A =/= B, SA =/= SB) andalso
        ?assert(rr_recon:find_intersection(A, A) =:= A) andalso
        ?assert(rr_recon:find_intersection(B, B) =:= B) andalso
        ?implies(intervals:is_subset(B, A), SB =:= SA) andalso
        ?implies(intervals:is_subset(A, B), SB =:= SA) andalso
        ?assert(intervals:is_subset(SA, A)) andalso
        ?assert(intervals:is_subset(SB, B)) andalso
        ?assert_w_note(SizeSA =:= SizeSB, 
                       [{a, A}, {b, B}, {sa, SA}, {sb, SB}, {sizeSa, SizeSA}, {sizeSb, SizeSB}]) andalso
        ?implies(IS =/= [], IS =:= SA andalso IS =:= SB) andalso
        ?implies(not intervals:is_empty(SA), not intervals:is_empty(SB)).

tester_find_intersection(_) ->
    tester:test(?MODULE, prop_find_intersection, 4, 120, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc
%    runs the bloom filter synchronization [Rounds]-times 
%    and records the sync degree after each round
%    returns list of sync degrees per round, first value is initial sync degree
% @end
-spec start_sync(Config, Nodes::Int, DBSize::Int, DBParams,
                 Rounds::Int, Fpr, RRConf::Config) -> {Start::Status, End::Status}
when
    is_subtype(Config,      [tuple()]),
    is_subtype(Int,         pos_integer()),
    is_subtype(DBParams,    [db_generator:db_parameter()]),
    is_subtype(Fpr,         float()),
    is_subtype(Status,      db_generator:db_status()).
start_sync(Config, NodeCount, DBSize, DBParams, Rounds, Fpr, RRConfig) ->
    NodeKeys = lists:sort(get_symmetric_keys(NodeCount)),
    build_symmetric_ring(NodeCount, Config, RRConfig),
    config:write(rr_bloom_fpr, Fpr),
    erlang:put(?DBSizeKey, ?REP_FACTOR * DBSize),
    db_generator:fill_ring(random, DBSize, DBParams),    
    InitDBStat = get_db_status(),
    print_status(0, InitDBStat),
    util:for_to_ex(1, Rounds, 
                   fun(I) ->
                           startSyncRound(NodeKeys),
                           waitForSyncRoundEnd(NodeKeys),
                           print_status(I, get_db_status())
                   end),
    EndStat = get_db_status(),
    unittest_helper:stop_ring(),
    {InitDBStat, EndStat}.

-spec print_status(Round::integer(), db_generator:db_status()) -> ok.
print_status(R, {_, _, M, O}) ->
    ct:pal(">>SYNC RUN [Round ~p] Missing=[~p] Outdated=[~p]", [R, M, O]).

-spec count_outdated(?RT:key()) -> non_neg_integer().
count_outdated(Key) ->
    Req = {rr_stats, {count_old_replicas, comm:this(), intervals:all()}},
    api_dht_raw:unreliable_lookup(Key, {send_to_group_member, rrepair, Req}),
    receive
        {count_old_replicas_reply, Old} -> Old
    end.

-spec count_outdated() -> non_neg_integer().
count_outdated() ->
    Req = {rr_stats, {count_old_replicas, comm:this(), intervals:all()}},
    lists:foldl(
      fun(Node, Acc) -> 
              comm:send(Node, {send_to_group_member, rrepair, Req}),
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
    Stored = statistics:get_total_load(Ring),
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
      fun() ->  get_symmetric_keys(NodeCount) end,
      [{config, lists:flatten([{log_path, PrivDir}, 
                               RRConfig])}]),
    % wait for all nodes to finish their join 
    unittest_helper:check_ring_size_fully_joined(NodeCount),
    % wait a bit for the rm-processes to settle
    timer:sleep(500),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Analysis
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
startSyncRound(NodeKeys) ->
    lists:foreach(fun(X) ->
                          api_dht_raw:unreliable_lookup(X, {send_to_group_member, rrepair, {rr_trigger}})
                  end, 
                  NodeKeys),
    ok.

waitForSyncRoundEnd(NodeKeys) ->
    Req = {send_to_group_member, rrepair, {get_state, comm:this(), open_sessions}},
    lists:foreach(
      fun(Key) -> 
              util:wait_for(
                fun() -> 
                        api_dht_raw:unreliable_lookup(Key, Req),
                        receive 
							{get_state_response, Val} -> Val =:= 0
						end
                end)
      end, 
      NodeKeys),
    ok.

-spec sync_degree(db_generator:db_status()) -> float().
sync_degree({Count, _Ex, M, O}) ->
    (Count - M - O) / Count.
    