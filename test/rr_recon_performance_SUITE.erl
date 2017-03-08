%  @copyright 2010-2016 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de
%% @doc    Performance of rrepair modules
%% @end
%% @version $Id$
-module(rr_recon_performance_SUITE).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

-include("record_helpers.hrl").

-export([]).

-type result_pair() :: {build_time,     measure_util:result()} |
                       {tree_time,      measure_util:result()} |
                       {hash_fun_count, pos_integer()}.
-type result() :: [result_pair()].

all() ->
    [art,
     merkle_tree,
     bloom,
     bloom2,
     comparison
    ].

suite() ->
    [
     {timetrap, {seconds, 200}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

comparison(_) ->
    Iter = 100,
    DBSize = 10000,
    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    DB = db_generator:get_db(I, DBSize, uniform, [{output, list_keytpl}]),
    
    BBuildT = bloom_build_time(I, DB, DBSize, Iter, 0.1),
    MBuildT = merkle_build_time(I, DB, DBSize, Iter, []),
    ArtT = art_build_time(I, DB, DBSize, Iter, [], []),
        
    ABuildT = proplists:get_value(build_time, ArtT),
    TreeT = proplists:get_value(tree_time, ArtT),
    
    ct:pal("Performance Comparison
            DBSize=~p ; Iterations=~p
            -----------------------------------------------
                                bloom |merkleTree|  art
            BuildTime Avg ms: ~f | ~f | ~f (~f without Merkle)",
           [DBSize, Iter,
            measure_util:get(BBuildT, avg, ms),
            measure_util:get(MBuildT, avg, ms),
            measure_util:get(ABuildT, avg, ms),
            measure_util:get(TreeT, avg, ms)]),
    ok.

-spec bloom_build_time(intervals:interval(), [any()], pos_integer(), pos_integer(), float()) -> measure_util:result().
bloom_build_time(_, DB, DBSize, Iterations, Fpr) ->
    BaseBF = bloom:new_fpr(DBSize, Fpr),
    measure_util:time_avg(
      fun() ->
              lists:foldl(fun(I, Bloom) -> bloom:add(Bloom, I) end, BaseBF, DB)
      end, Iterations, []).

-spec merkle_build_time(intervals:interval(), [any()], pos_integer(), pos_integer(),
            merkle_tree:mt_config_params()) -> measure_util:result().
merkle_build_time(I, DB, _DBSize, Iterations, Config) ->
    measure_util:time_avg(fun() -> merkle_tree:new(I, DB, Config) end,
                          Iterations, []).

-spec art_build_time(intervals:interval(), [any()], pos_integer(), pos_integer(),
            merkle_tree:mt_config_params(), art:config()) -> result().
art_build_time(I, DB, _DBSize, Iterations, MerkleConfig, ArtConfig) ->
    {Tree, TreeTime} =
        measure_util:time_with_result(fun() -> merkle_tree:new(I, DB, MerkleConfig) end, Iterations, []),
    BuildTime =
        measure_util:time_avg(fun() -> art:new(Tree, ArtConfig) end, Iterations, []),
    
    [{build_time, measure_util:add(BuildTime, TreeTime)},
     {tree_time, TreeTime}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

art(_) ->
    ToAdd = 10000,
    ExecTimes = 100,
    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    DB = db_generator:get_db(I, ToAdd, uniform, [{output, list_keytpl}]),

    {TreeTime, Tree} =
        util:tc(fun() -> merkle_tree:new(I, DB, [{leaf_hf, fun art:merkle_leaf_hf/2}]) end, []),
    BuildTime =
        measure_util:time_avg(fun() -> art:new(Tree) end, ExecTimes, []),

    ct:pal("ART Performance
            ------------------------
            PARAMETER: AddedItems=~p ; ExecTimes=~p
            TreeTime (ms)= ~p
            ARTBuildTime (ms)= ~p",
           [ToAdd, ExecTimes, TreeTime / 1000,
            measure_util:print(BuildTime, ms)]),
    true.

% @doc measures performance of merkle_tree operations
merkle_tree(_) ->
    % PARAMETER
    ExecTimes = 100,
    ToAdd = 10000,
    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    DB = db_generator:get_db(I, ToAdd, uniform, [{output, list_keytpl}]),
    
    TestTree = merkle_tree:new(I, DB, []),
    {Inner, Leafs, _EmptyLeafs, _Items} = merkle_tree:size_detail(TestTree),
    
    BuildT = measure_util:time_avg(
           fun() -> merkle_tree:new(I, DB, []) end,
           ExecTimes, []),
        
    IterateT = measure_util:time_avg(
           fun() -> merkle_tree_SUITE:iterate(merkle_tree:iterator(TestTree), fun(_, Acc) -> Acc + 1 end, 0) end,
           ExecTimes, []),
    
    GenHashT = measure_util:time_avg(
            fun() -> merkle_tree:gen_hash(TestTree) end,
            ExecTimes, []),
    
    SimpleSizeT = measure_util:time_avg(
            fun() -> merkle_tree:size(TestTree) end,
            ExecTimes, []),
    
    DetailSizeT = measure_util:time_avg(
            fun() -> merkle_tree:size_detail(TestTree) end,
            ExecTimes, []),

    ct:pal("
            Merkle_Tree Performance
            ------------------------
            PARAMETER: AddedItems=~p ; ExecTimes=~p
            TreeSize: InnerNodes=~p ; Leafs=~p,
            BuildTime:      ~p
            IterationTime : ~p
            GenHashTime:    ~p
            SimpleSizeTime: ~p
            DetailSizeTime: ~p",
           [ToAdd, ExecTimes, Inner, Leafs,
            measure_util:print(BuildT, ms),
            measure_util:print(IterateT),
            measure_util:print(GenHashT),
            measure_util:print(SimpleSizeT),
            measure_util:print(DetailSizeT)]),
    ok.

bloom(_) ->
    %parameter
    ExecTimes = 100,
    ToAdd = 10000, % req: mod 2 = 0
    Fpr = 0.1,

    BaseBF = bloom:new_fpr(ToAdd, Fpr),
    List = random_list(ToAdd),
    TestBF = bloom:add_list(BaseBF, List),

    AddT = measure_util:time_avg(fun() -> for_to_ex(1, ToAdd,
                                                    fun(I) -> I end,
                                                    fun(I, B) -> bloom:add(B, I) end,
                                                    BaseBF)
                                 end,
                                 ExecTimes, []),
    
    AddListT = measure_util:time_avg(fun() -> bloom:add_list(BaseBF, List) end,
                                     ExecTimes, []),

    IsElemT = measure_util:time_avg(
                fun() ->
                        lists:foreach(fun(I) -> bloom:is_element(TestBF, I) end, List)
                end,
                ExecTimes, []),
    
    %measure join time
    BF1 = for_to_ex(1,
                    round(ToAdd / 2),
                    fun(I) -> I end, fun(I, B) -> bloom:add(B, I) end, BaseBF),
    BF2 = for_to_ex(round(ToAdd / 2) + 1,
                    ToAdd,
                    fun(I) -> I end, fun(I, B) -> bloom:add(B, I) end, BaseBF),
    JoinTime =
        measure_util:time_avg(fun() -> bloom:join(BF1, BF2) end, ExecTimes, []),

    %print results
    ct:pal("AVG EXECUTION TIMES OF BLOOM FILTER OPERATIONS
            PARAMETER: AddedItems=~p ; ExecTimes=~p
            --------------------------------------------------------------
            Add       : ~p
            AddList   : ~p
            IsElement : ~p
            Join      : ~p",
           [ToAdd, ExecTimes,
            measure_util:print(AddT),
            measure_util:print(AddListT),
            measure_util:print(IsElemT),
            measure_util:print(JoinTime)]),
    ok.

bloom2(_) ->
    %parameter
    ExecTimes = 50,
    ToAdd = 1024*8, % req: mod 2 = 0
    Fpr = 0.1,

    BaseBF = bloom:new_fpr(ToAdd, Fpr),
    BFSize = bloom:get_property(BaseBF, size),
    
    BloomAddListFunAdd =
        fun(ChunkSize) ->
                for_to_ex(
                  1, ToAdd div ChunkSize,
                  fun(I) -> I end,
                  fun(_I, B) ->
                          bloom:add_list(B, random_list(ChunkSize))
                  end,
                  BaseBF)
        end,
    BloomAddListFunNew =
        fun(ChunkSize) ->
                for_to_ex(
                  1, ToAdd div ChunkSize,
                  fun(I) -> I end,
                  fun(_I, _B) ->
                          bloom:add_list(BaseBF, random_list(ChunkSize))
                  end,
                  ok)
        end,
    
    AddTimesAdd =
        [begin
             AvgV = measure_util:get(
                      measure_util:time_avg(
                        fun() -> BloomAddListFunAdd(I) end, ExecTimes, []), avg),
             {I, AvgV}
         end || I <- [8, 16, 4096]],
    AddTimesNew =
        [begin
             AvgV = measure_util:get(
                      measure_util:time_avg(
                        fun() -> BloomAddListFunNew(I) end, ExecTimes, []), avg),
             {I, AvgV}
         end || I <- [8, 16, 4096]],

    %print results
    ct:pal("AVG EXECUTION TIMES OF BLOOM FILTER OPERATIONS
            PARAMETER: AddedItems=~p ; ExecTimes=~p
                       BFSize=~p ; #Hfs=~p
            --------------------------------------------------------------" ++ "
            using bloom:add_list/2 consecutively:" ++
               lists:append(lists:duplicate(length(AddTimesAdd), "
            Res: ~.2p")) ++ "
            --------------------------------------------------------------" ++ "
            using bloom:add_list/2 on an empty bloom filter:" ++
               lists:append(lists:duplicate(length(AddTimesNew), "
            Res: ~.2p")),
           [ToAdd, ExecTimes, BFSize, bloom:get_property(BaseBF, hfs_size)]
               ++ AddTimesAdd ++ AddTimesNew),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% UTILS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec random_list(Count::pos_integer()) -> [pos_integer(),...].
random_list(Count) ->
    util:for_to_ex(1, Count, fun(_) -> randoms:getRandomInt() end).

for_to_ex(I, N, Fun, AccuFun, Accu) ->
    NewAccu = AccuFun(Fun(I), Accu),
    if
        I < N ->
            for_to_ex(I + 1, N, Fun, AccuFun, NewAccu);
        I =:= N ->
            NewAccu;
        I > N ->
            failed
    end.
