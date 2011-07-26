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

-module(rrepair_SUITE).

-author('malange@informatik.hu-berlin.de').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

-include("record_helpers.hrl").

-record(rrNodeStatus, {
                        nodeKey     = ?required(rrNodeStatus, nodeKey)      :: ?RT:key(), 
                        nodeRange   = ?required(rrNodeStatus, nodeRange)    :: intervals:interval(),
                        dbItemCount = 0                                     :: pos_integer(),
                        versionSum  = 0                                     :: pos_integer()
                      }).
-type rrDBStatus() :: [#rrNodeStatus{}].

all() ->
    [get_symmetric_keys_test,
     simpleBloomSync_test,
     bloomSync_FprCompare_check,
     bloomSync_times].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

get_rrepair_config_parameter() ->
    [{rep_update_activate, true},
     {rep_update_interval, 100000000}, %stop trigger
     {rep_update_trigger, trigger_periodic},
     {rep_update_sync_method, bloom}, %bloom, merkleTree, art
     {rep_update_fpr, 0.1},
     {rep_update_max_items, 1000}].

end_per_testcase(_TestCase, _Config) ->
    unittest_helper:stop_ring(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Test Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_symmetric_keys_test(_) ->
    ToTest = get_symmetric_keys(4),
    ToBe = ?RT:get_replica_keys(0),
    ct:pal("GeneratedKeys = ~w~nRT-GetReplicaKeys = ~w", [ToTest, ToBe]),
    Equal = lists:foldl(fun(I, Acc) -> lists:member(I, ToBe) andalso Acc end, true, ToTest),
    ?equals(Equal, true),
    ok.

% @doc Bloom Synchronization should update at least one Item.
simpleBloomSync_test(Config) ->
    [Start, End] = start_bloom_sync(Config, 4, 1000, 1, 0.1),
    ?assert(Start < End),
    ok.

% @doc Better Fpr should result in a higher synchronization degree.
bloomSync_FprCompare_check(Config) ->
    R1 = start_bloom_sync(Config, 4, 1000, 2, 0.2),
    R2 = start_bloom_sync(Config, 4, 1000, 2, 0.1),
    ?assert(lists:last(R1) < lists:last(R2)),
    ok.

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
    {BuildRingTime, _} = util:tc(?MODULE, build_symmetric_ring, [NodeCount, Config]),
    config:write(rep_update_fpr, Fpr),
    {FillTime, _} = util:tc(?MODULE, fill_symmetric_ring, [DataCount, NodeCount]),
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
-spec start_bloom_sync([tuple()], pos_integer(), pos_integer(), pos_integer(), float()) -> [float()].
start_bloom_sync(Config, NodeCount, DataCount, Rounds, Fpr) ->
    NodeKeys = lists:sort(get_symmetric_keys(NodeCount)),
    DestVersCount = NodeCount * 2 * DataCount,
    ItemCount = NodeCount * DataCount,
    %build and fill ring
    build_symmetric_ring(NodeCount, Config),
    config:write(rep_update_fpr, Fpr),
    fill_symmetric_ring(DataCount, NodeCount),
    %measure initial sync degree
    InitialOutdated = DestVersCount - getVersionCount(getDBStatus()),
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
    unittest_helper:stop_ring(),
    Result.

-spec get_symmetric_keys(pos_integer()) -> [?RT:key()].
get_symmetric_keys(NodeCount) ->
    B = (16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF div NodeCount) + 1,
    util:for_to_ex(0, NodeCount - 1, fun(I) -> I*B end).

build_symmetric_ring(NodeCount, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    %Build ring with NodeCount symmetric nodes
    unittest_helper:make_ring_with_ids(
      fun() ->  get_symmetric_keys(NodeCount) end,
      [{config, lists:flatten([{log_path, PrivDir}, 
                               {dht_node, mockup_dht_node},
                               get_rrepair_config_parameter()])}]),
    % wait for all nodes to finish their join 
    unittest_helper:check_ring_size_fully_joined(NodeCount),
    % wait a bit for the rm-processes to settle
    timer:sleep(500),
    ok.

fill_symmetric_ring(DataCount, NodeCount) ->
    NodeIds = lists:sort(get_symmetric_keys(NodeCount)),
    util:for_to(1, 
                NodeCount div 4, 
                fun(I) ->
                        FirstKey = lists:nth(I, NodeIds) + 1,
                        %write DataCount-items to nth-Node and its symmetric replicas
                        util:for_to(FirstKey, 
                                    FirstKey + DataCount - 1, 
                                    fun(Key) ->
                                            RepKeys = ?RT:get_replica_keys(Key),
                                            %write replica group
                                            lists:foreach(fun(X) -> 
                                                                  DBEntry = db_entry:new(X, "2", 2),
                                                                  %DBEntry = db_entry:new(X),
                                                                  api_dht_raw:unreliable_lookup(X, 
                                                                                                {set_key_entry, comm:this(), DBEntry}),
                                                                  receive {set_key_entry_reply, _} -> ok end
                                                          end, 
                                                          RepKeys),
                                            %random replica is outdated
                                            OldKey = lists:nth(randoms:rand_uniform(1, length(RepKeys) + 1), RepKeys),
                                            api_dht_raw:unreliable_lookup(OldKey, {set_key_entry, comm:this(), db_entry:new(OldKey, "1", 1)}),
                                            receive {set_key_entry_reply, _} -> ok end,
                                            ok
                                    end)
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