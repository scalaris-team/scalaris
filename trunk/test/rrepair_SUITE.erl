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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(rrNodeStatus, {
                        nodeKey     = ?required(rrNodeStatus, nodeKey)      :: ?RT:key(), 
                        nodeRange   = ?required(rrNodeStatus, nodeRange)    :: intervals:interval(),
                        dbItemCount = 0                                     :: pos_integer(),
                        versionSum  = 0                                     :: pos_integer()
                      }).
-type rrDBStatus() :: [#rrNodeStatus{}].

basic_tests() ->
    [get_symmetric_keys_test,
     blobCoding,
     tester_get_key_quadrant,
     tester_mapInterval,
     tester_minKeyInInterval].

upd_tests() ->
    [upd_no_outdated,
     upd_min_nodes,     % sync in an single node ring
     upd_simple,        % run one sync round
     upd_dest,          % run one sync with a specified dest node 
     upd_parts].        % get_chunk with limited items / leads to multiple bloom filters and/or successive merkle tree building

all() ->
    [{group, basic_tests},
     {group, upd_tests}
     %bloomSync_times
     ].

groups() ->
    [{basic_tests,  [parallel], basic_tests()},
     {upd_tests,    [sequence], [{upd_bloom,    [sequence], upd_tests()}, %{repeat_until_any_fail, 1000}
                                 {upd_merkle,   [sequence], upd_tests()},
                                 {upd_art,      [sequence], upd_tests()}]}
    ].

suite() ->
    [
     {timetrap, {seconds, 15}}
    ].

init_per_suite(Config) ->
    _ = crypto:start(),    
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    crypto:stop(),
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

end_per_testcase(_TestCase, _Config) ->
    unittest_helper:stop_ring(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_rep_upd_config(Method) ->
    [{rrepair_enabled, true},
     {rep_update_interval, 100000000}, %stop trigger
     {rep_update_trigger, trigger_periodic},
     {rep_update_recon_method, Method},
     {rep_update_resolve_method, simple},
     {rep_update_recon_fpr, 0.1},
     {rep_update_max_items, case Method of
                                bloom -> 10000;
                                _ -> 100000
                            end},
     {rep_update_negotiate_sync_interval, case Method of
                                              bloom -> false;
                                              _ -> true
                                          end}].    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Replica Update tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_group(Group, Config) ->
    ct:comment(io_lib:format("BEGIN ~p", [Group])),
    case Group of
        upd_bloom -> [{ru_method, bloom} | Config];
        upd_merkle -> [{ru_method, merkle_tree} | Config];
        upd_art -> [{ru_method, art} | Config];
        _ -> Config
    end.

end_per_group(Group, Config) ->  
    Method = proplists:get_value(ru_method, Config, undefined),
    case Method of
        undefined -> ct:comment(io_lib:format("END ~p", [Group]));
        M -> ct:comment(io_lib:format("END ~p/~p", [Group, M]))
    end,
    proplists:delete(ru_method, Config).


upd_no_outdated(Config) ->
    Method = proplists:get_value(ru_method, Config),
    [Start, End] = start_sync(Config, 4, 1000, 0, 1, 0.1, get_rep_upd_config(Method)),
    ?assert(Start =:= End).

upd_min_nodes(Config) ->
    Method = proplists:get_value(ru_method, Config),
    [Start, End] = start_sync(Config, 1, 1, 1000, 1, 0.2, get_rep_upd_config(Method)),
    ?assert(Start =:= End).    

upd_simple(Config) ->
    Method = proplists:get_value(ru_method, Config),
    [Start, End] = start_sync(Config, 4, 1000, 10, 1, 0.1, get_rep_upd_config(Method)),
    ?assert(Start < End).    

upd_dest(Config) ->
    %parameter
    NodeCount = 7,
    DataCount = 1000,
    Fpr = 0.1,
    Method = proplists:get_value(ru_method, Config),
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, get_rep_upd_config(Method)),
    config:write(rep_update_recon_fpr, Fpr),
    db_generator:fill_ring(random, DataCount, [{ftype, update}, 
                                               {fprob, 50}, 
                                               {distribution, uniform}]),
    %chose node pair    
    SKey = ?RT:get_random_node_id(),
    CKey = util:randomelem(lists:delete(SKey, ?RT:get_replica_keys(SKey))),
    %measure initial sync degree
    SO = count_outdated(SKey),
    CO = count_outdated(CKey),    
    %server starts sync
    api_dht_raw:unreliable_lookup(SKey, {send_to_group_member, rrepair, 
                                              {request_sync, Method, CKey}}),
    %waitForSyncRoundEnd(NodeKeys),
    waitForSyncRoundEnd([SKey, CKey]),
    %measure sync degree
    SONew = count_outdated(SKey),
    CONew = count_outdated(CKey),
    ct:pal("SYNC RUN << ~p >>~nServerKey=~p~nClientKey=~p
            Server outdated: [~p] -> [~p]~nClient outdated: [~p] -> [~p]", 
           [Method, SKey, CKey, SO, SONew, CO, CONew]),
    %clean up
    unittest_helper:stop_ring(),
    ?implies(SO > 0, ?assert(SONew < SO)) andalso ?implies(CO > 0, ?assert(CONew < CO)).

upd_parts(Config) ->
    Method = proplists:get_value(ru_method, Config),
    OldConf = get_rep_upd_config(Method),
    Conf = lists:keyreplace(rep_update_max_items, 1, OldConf, {rep_update_max_items, 500}),
    [Start, End] = start_sync(Config, 4, 1000, 100, 1, 0.1, Conf),
    ?assert(Start < End).

upd_fpr_compare(Config) ->
    Method = proplists:get_value(ru_method, Config),
    Conf = get_rep_upd_config(Method),    
    Fpr1 = 0.2,
    Fpr2 = 0.01,
    R1 = start_sync(Config, 4, 1000, 100, 2, Fpr1, Conf),
    R2 = start_sync(Config, 4, 1000, 100, 2, Fpr2, Conf),
    ct:pal("Result FPR=~p - ~p~nFPR=~p - ~p", [Fpr1, R1, Fpr2, R2]),
    ?assert(lists:nth(2, R1) < lists:nth(2, R2) orelse lists:last(R1) < lists:last(R2)).    

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

blobCoding(_) ->
    A = 180000001,
    B = 4,
    Coded = rr_recon:encodeBlob(A, B),
    {DA, DB} = rr_recon:decodeBlob(Coded),
    ?equals_w_note(A, DA, io_lib:format("A=~p ; Coded=~p ; DecodedA=~p", [A, Coded, DA])),
    ?equals_w_note(B, DB, io_lib:format("B=~p ; Coded=~p ; DecodedB=~p", [B, Coded, DB])),
    ok.

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
    tester:test(?MODULE, prop_get_key_quadrant, 1, 4, [{threads, 4}]).

-spec prop_mapInterval(?RT:key(), ?RT:key(), 1..4) -> true.
prop_mapInterval(A, B, Q) ->
    I = case A < B of
            true -> intervals:new('[', A, B, ']');
            false -> intervals:new('[', B, A, ']')
        end,
    Mapped = rr_recon:mapInterval(I, Q),
    {LBr, L1, R1, RBr} = intervals:get_bounds(Mapped),
    LQ = rr_recon:get_key_quadrant(L1),
    RQ = rr_recon:get_key_quadrant(R1),    
    ?implies(LBr =:= '(', LQ =/= Q) andalso
        ?implies(LBr =:= '[', ?equals(LQ, Q)) andalso
        ?implies(RBr =:= ')', RQ =/= Q) andalso
        ?implies(RBr =:= ']', ?equals(RQ, Q)) andalso
        ?implies(LBr =:= '[' andalso RBr =:= LBr, ?equals(LQ, RQ) andalso ?equals(LQ, Q)) andalso
        ?equals(rr_recon:get_interval_quadrant(Mapped), Q).
    
tester_mapInterval(_) ->
    tester:test(?MODULE, prop_mapInterval, 3, 10, [{threads, 1}]).

-spec prop_minKeyInInterval(?RT:key(), ?RT:key()) -> true.
prop_minKeyInInterval(L, L) -> true;
prop_minKeyInInterval(LeftI, RightI) ->
    I = intervals:new('[', LeftI, RightI, ']'),    
    Keys = [X || X <- ?RT:get_replica_keys(LeftI), X =/= LeftI],
    AnyK = util:randomelem(Keys),
    MinLeft = rr_recon:minKeyInInterval(AnyK, I),
    ct:pal("I=~p~nKeys=~p~nAnyKey=~p~nMin=~p", [I, Keys, AnyK, MinLeft]),
    ?implies(MinLeft =:= LeftI, MinLeft =/= AnyK).

tester_minKeyInInterval(_) ->
    tester:test(?MODULE, prop_minKeyInInterval, 2, 10, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Bloom Filter Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

bloomSync_times(Config) ->
    %Parameter
    NodeCount = 4,
    DataCount = 1000,
    Rounds = 1,
    Fpr = 0.1,
    %start_bloom_sync measurement
    NodeKeys = lists:sort(get_symmetric_keys(NodeCount)),
    DestVersCount = NodeCount * 2 * DataCount,
    ItemCount = NodeCount * DataCount,
    %Build Ring
    {BuildRingTime, _} = util:tc(?MODULE, build_symmetric_ring, [NodeCount, Config, get_rep_upd_config(bloom)]),
    config:write(rep_update_fpr, Fpr),
    {FillTime, _} = util:tc(?MODULE, fill_symmetric_ring, [DataCount, NodeCount, 100]),
    %measure initial sync degree
    {DBStatusTime, DBStatus} = util:tc(?MODULE, getDBStatus, []),
    {GetVersionCountTime, VersCount} = util:tc(?MODULE, getVersionCount, [DBStatus]),
    InitialOutdated = DestVersCount - VersCount,
    %run sync rounds    
    Result = [calc_sync_degree(InitialOutdated, ItemCount) |
                  lists:reverse(util:for_to_ex(1,
                                               Rounds, 
                                               fun(_I) ->
                                                       startSyncRound(NodeKeys),
                                                       timer:sleep(5000),
                                                       calc_sync_degree(DestVersCount - getVersionCount(getDBStatus()), 
                                                                        ItemCount)
                                               end))],
    ct:pal(">>BLOOM SYNC RUN>> ~w Rounds  Fpr=~w  SyncLog ~w", [Rounds, Fpr, Result]),
    %clean up
    {StopRingTime, _} = util:tc(unittest_helper, stop_ring, []),    
    ct:pal("EXECUTION TIMES in microseconds (10^-6)~nBuildRing = ~w~nFillRing = ~w~nDBStatus = ~w~nGetVersionCount = ~w~nStopRing = ~w",
           [BuildRingTime, FillTime, DBStatusTime, GetVersionCountTime, StopRingTime]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc
%    runs the bloom filter synchronization [Rounds]-times 
%    and records the sync degree after each round
%    returns list of sync degrees per round, first value is initial sync degree
% @end
-spec start_sync(Config, NodeCount::Int, DataCount::Int, 
                 OutdatedP, Rounds::Int, Fpr, RepConfig::Config) -> [Fpr] 
when
    is_subtype(Config,      [tuple()]),
    is_subtype(Int,         pos_integer()),
    is_subtype(OutdatedP,   0..100),        %outdated probability in percent
    is_subtype(Fpr,         float()).          
start_sync(Config, NodeCount, DataCount, OutdatedProb, Rounds, Fpr, RepUpdConfig) ->
    NodeKeys = lists:sort(get_symmetric_keys(NodeCount)),
    DestVersCount = NodeCount * 2 * DataCount,
    ItemCount = NodeCount * DataCount,
    %build and fill ring
    build_symmetric_ring(NodeCount, Config, RepUpdConfig),
    config:write(rep_update_recon_fpr, Fpr),
    fill_symmetric_ring(DataCount, NodeCount, OutdatedProb),
    %measure initial sync degree
    InitialOutdated = DestVersCount - getVersionCount(getDBStatus()),
    %run sync rounds
    Result = [calc_sync_degree(InitialOutdated, ItemCount) |
                  lists:reverse(util:for_to_ex(1,
                                               Rounds, 
                                               fun(_I) ->
                                                       startSyncRound(NodeKeys),
                                                       waitForSyncRoundEnd(NodeKeys),
                                                       calc_sync_degree(DestVersCount - getVersionCount(getDBStatus()), 
                                                                        ItemCount)
                                               end))],
    SyncMethod = proplists:get_value(rep_update_recon_method, RepUpdConfig),
    ct:pal(">>[~p] SYNC RUN>> ~w Rounds  Fpr=~w  SyncLog ~w", [SyncMethod, Rounds, Fpr, Result]),
    %clean up
    unittest_helper:stop_ring(),
    Result.

-spec count_outdated(?RT:key()) -> non_neg_integer().
count_outdated(Key) ->
    Req = {rr_stats, {count_old_replicas, comm:this(), intervals:all()}},
    api_dht_raw:unreliable_lookup(Key, {send_to_group_member, rrepair, Req}),
    receive
        {count_old_replicas_reply, Old} -> Old
    end.

-spec get_symmetric_keys(pos_integer()) -> [?RT:key()].
get_symmetric_keys(NodeCount) ->
    [element(2, intervals:get_bounds(I)) || I <- intervals:split(intervals:all(), NodeCount)].

build_symmetric_ring(NodeCount, Config, RepUpdConfig) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    %Build ring with NodeCount symmetric nodes
    unittest_helper:make_ring_with_ids(
      fun() ->  get_symmetric_keys(NodeCount) end,
      [{config, lists:flatten([{log_path, PrivDir}, 
                               {dht_node, mockup_dht_node},
                               RepUpdConfig])}]),
    % wait for all nodes to finish their join 
    unittest_helper:check_ring_size_fully_joined(NodeCount),
    % wait a bit for the rm-processes to settle
    timer:sleep(500),
    ok.

-spec fill_symmetric_ring(non_neg_integer(), pos_integer(), 0..100) -> ok.
fill_symmetric_ring(DataCount, NodeCount, OutdatedProbability) ->
    NodeIds = lists:sort(get_symmetric_keys(NodeCount)),
    util:for_to(1, 
                NodeCount div 4,
                fun(N) ->
                        From = lists:nth(N, NodeIds),
                        To = lists:nth(N + 1, NodeIds),
                        % write DataCount items to nth-Node and its symmetric replicas
                        [begin
                             Key = element(2, intervals:get_bounds(I)),
                             RepKeys = ?RT:get_replica_keys(Key),
                             %write replica group
                             lists:foreach(fun(X) -> 
                                                   DBEntry = db_entry:new(X, "2", 2),
                                                   api_dht_raw:unreliable_lookup(X, 
                                                                                 {set_key_entry, comm:this(), DBEntry}),
                                                   receive {set_key_entry_reply, _} -> ok end
                                           end, 
                                           RepKeys),
                             %random replica is outdated                             
                             case OutdatedProbability >= randoms:rand_uniform(1, 100) of
                                 true ->
                                     OldKey = util:randomelem(RepKeys),
                                     api_dht_raw:unreliable_lookup(OldKey, {set_key_entry, comm:this(), db_entry:new(OldKey, "1", 1)}),
                                     receive {set_key_entry_reply, _} -> ok end,
                                     ok;
                                 _ -> ok
                             end
                         end || I <- intervals:split(intervals:new('[', From, To, ']'), DataCount)]
                end),
    ct:pal("[~w]-Nodes-Ring filled with [~w] items per node", [NodeCount, DataCount]),
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
    Req = {send_to_group_member, rrepair, {get_state, comm:this(), open_sync}},
    lists:foreach(
      fun(Key) -> 
              util:wait_for(
                fun() -> 
                        api_dht_raw:unreliable_lookup(Key, Req),
                        receive {get_state_response, Val} -> Val =:= 0 end
                end)
      end, 
      NodeKeys),
    ok.

% @doc returns replica update specific node db information
-spec getDBStatus() -> rrDBStatus().
getDBStatus() ->
    RingData = unittest_helper:get_ring_data(),
    [ #rrNodeStatus{ nodeKey = LV,
                     nodeRange = intervals:new(LBr, LV, RV, RBr), 
                     dbItemCount = length(DB),
                     versionSum = lists:sum(lists:map(fun(X) -> db_entry:get_version(X) end, DB))
                     } 
    || {_Pid, {LBr, LV, RV, RBr}, DB, _Pred, _Succ, ok} = _Node <- RingData].

getVersionCount(RingStatus) ->
    lists:sum(lists:map(fun(#rrNodeStatus{ versionSum = V}) -> V end, RingStatus)).

print_sync_status(ObsoleteCount, ItemCount) ->
    ct:pal("SyncDegree: ~7.4f%   -- ItemsOutdated=~w",
           [100 * calc_sync_degree(ObsoleteCount, ItemCount),
            ObsoleteCount]).

calc_sync_degree(ObsoleteCount, ItemCount) ->
    (ItemCount - ObsoleteCount) / ItemCount.
