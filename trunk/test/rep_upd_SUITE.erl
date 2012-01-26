%  @copyright 2010-2011 Zuse Institute Berlin
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
%%% Created : 2011-05-27 by Maik Lange
%%%-------------------------------------------------------------------
%% @version $Id $

-module(rep_upd_SUITE).

-author('malange@informatik.hu-berlin.de').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").
-include("record_helpers.hrl").

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
     mapInterval,
     tester_minKeyInInterval].

upd_tests() ->
    [upd_no_outdated,
     upd_min_nodes,     % sync in an single node ring
     upd_simple,        % run one sync round
     upd_parts].        % get_chunk with limited items / leads to multiple bloom filters and/or successive merkle tree building

all() ->
    [{group, basic_tests},
     {group, upd_tests},
     bloomSync_times
     ].

groups() ->
    [{basic_tests,  [parallel], basic_tests()},
     {upd_tests,    [sequence], [{upd_bloom,    [sequence], upd_tests()},
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
    [{rep_update_activate, true},
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
    Config2 = unittest_helper:start_minimal_procs(Config, [], true),
    ToTest = lists:sort(get_symmetric_keys(4)),
    ToBe = lists:sort(?RT:get_replica_keys(?MINUS_INFINITY)),
    unittest_helper:stop_minimal_procs(Config2),
    ct:pal("GeneratedKeys = ~w~nRT-GetReplicaKeys = ~w", [ToTest, ToBe]),
    ?equals(ToTest, ToBe),
    ok.

blobCoding(_) ->
    A = 180000001,
    B = 4,
    Coded = rep_upd_recon:encodeBlob(A, B),
    {DA, DB} = rep_upd_recon:decodeBlob(Coded),
    ct:pal("A=~p ; B=~p 
            Coded=[~p] 
            decoded A=[~p] B=[~p]", [A, B, Coded, DA, DB]),
    ?equals(A, DA),
    ?equals(B, DB),
    ok.

mapInterval(_) ->
    K = ?RT:get_split_key(?MINUS_INFINITY, ?PLUS_INFINITY, {7, 8}),
    I = intervals:new('[', K, ?MINUS_INFINITY ,']'),
    ct:pal("I1=~p", [I]),
    lists:foreach(fun(X) -> 
                          MappedI = rep_upd_recon:mapInterval(I, X),
                          ?equals(intervals:is_empty(intervals:intersection(I, MappedI)),true),
                          ?equals(rep_upd_recon:get_interval_quadrant(MappedI), X)
                  end, 
                  [1,2,3]),
    ok.

-spec prop_minKeyInInterval(?RT:key(), ?RT:key()) -> true.
prop_minKeyInInterval(L, L) -> true;
prop_minKeyInInterval(LeftI, RightI) ->
    I = intervals:new('[', LeftI, RightI, ']'),    
    Keys = [X || X <- ?RT:get_replica_keys(LeftI), X =/= LeftI],
    AnyK = util:randomelem(Keys),
    MinLeft = rep_upd_recon:minKeyInInterval(AnyK, I),
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
-spec start_sync(Config, NodeCount, DataCount, 
                 OutdatedP, Rounds, Fpr, RepConfig) -> [Fpr] when
    is_subtype(Config,      [tuple()]),
    is_subtype(NodeCount,   pos_integer()),
    is_subtype(DataCount,   pos_integer()),
    is_subtype(OutdatedP,   0..100),        %outdated probability in percent
    is_subtype(Rounds,      pos_integer()),
    is_subtype(Fpr,         float()),
    is_subtype(RepConfig,   [tuple()]).
          
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
                          api_dht_raw:unreliable_lookup(X, {send_to_group_member, rep_upd, {rep_update_trigger}})
                  end, 
                  NodeKeys),
    ok.

waitForSyncRoundEnd(NodeKeys) ->
    lists:foreach(
      fun(Node) -> 
              util:wait_for(
                fun() -> 
                        api_dht_raw:unreliable_lookup(Node, 
                                                      {send_to_group_member, rep_upd, 
                                                       {get_state, comm:this(), open_sync}}),
                        receive
                            {get_state_response, Val} -> Val =:= 0
                        end
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
