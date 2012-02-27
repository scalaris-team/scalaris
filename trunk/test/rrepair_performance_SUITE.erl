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
%%% File    rrepair_performance.erl
%%% @author Maik Lange <malange@informatik.hu-berlin.de
%%% @doc    Performance of rrepair modules
%%% @end
%%% Created : 2011-12-31 by Maik Lange
%%%-------------------------------------------------------------------
%% @version $Id $

-module(rrepair_performance_SUITE).

-author('malange@informatik.hu-berlin.de').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

-include("record_helpers.hrl").

-export([]).

all() ->
    [art,
     merkle_tree,
     bloom].
     %comparison].

suite() ->
    [
     {timetrap, {seconds, 90}}
    ].

init_per_suite(Config) ->
    _ = crypto:start(),
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    crypto:stop(),
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

comparison(_) ->
    Iter = 100,
    DBSize = 1000,
    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    DB = db_generator:get_db(I, DBSize, uniform),    
    
    BloomT = m_bloom(I, DB, DBSize, Iter),
    MerkleT = m_merkle(I, DB, DBSize, Iter),
    ArtT = m_art(I, DB, DBSize, Iter),        
        
    BBuildT = proplists:get_value(build_time, BloomT),
    MBuildT = proplists:get_value(build_time, MerkleT),
    ABuildT = proplists:get_value(build_time, ArtT),
    TreeT = proplists:get_value(tree_time, ArtT),
    
    ct:pal("Performance Comparison
            DBSize=~p ; Iterations=~p
            -----------------------------------------------
                                bloom|merkleTree|  art
            BuildTime Avg ms: ~f | ~f | ~f (~f ohne Merkle)", 
           [DBSize, Iter,
            measure_util:get(BBuildT, avg, ms), 
            measure_util:get(MBuildT, avg, ms), 
            measure_util:get(ABuildT, avg, ms) + TreeT / 1000, 
            measure_util:get(ABuildT, avg, ms)]),
    ok.

m_bloom(_I, DB, DBSize, Iterations) ->
    Fpr= 0.1,
    Hfs = hfs_lhsp:new(bloom:calc_HF_numEx(DBSize, Fpr)),
    BaseBF = bloom:new(DBSize, Fpr, Hfs),
    
    %measure Build times    
    BuildTime = measure_util:time_avg(
                  fun() -> 
                          lists:foldl(fun(I, Bloom) -> bloom:add(Bloom, I) end, BaseBF, DB)
                  end, Iterations, []),
    [{build_time, BuildTime}].

m_merkle(I, DB, _DBSize, Iterations) ->
    BuildT = 
        measure_util:time_avg(fun() -> merkle_tree:bulk_build(I, DB) end, 
                              Iterations, []),
    
    [{build_time, BuildT}].

m_art(I, DB, _DBSize, Iterations) ->    
    {TreeTime, Tree} = 
        util:tc(fun() -> merkle_tree:bulk_build(I, DB) end, []),
    BuildTime = 
        measure_util:time_avg(fun() -> art:new(Tree) end, Iterations, []),
    
    [{build_time, BuildTime}, {tree_time, TreeTime}].

art(_) ->    
    ToAdd = 2000,
    ExecTimes = 100,
    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    DB = db_generator:get_db(I, ToAdd, uniform),

    {TreeTime, Tree} = 
        util:tc(fun() -> merkle_tree:bulk_build(I, DB) end, []),
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
    ToAdd = 2000,
    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    DB = db_generator:get_db(I, ToAdd, uniform),
    
    TestTree = merkle_tree:bulk_build(I, DB),
    {Inner, Leafs} = merkle_tree:size_detail(TestTree),    
    
    BuildT = measure_util:time_avg(
           fun() -> merkle_tree:bulk_build(I, DB) end, 
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
    ToAdd = 2000, % req: mod 2 = 0
    Fpr = 0.1,

    Hfs = hfs_lhsp:new(bloom:calc_HF_numEx(ToAdd, Fpr)),
    BaseBF = bloom:new(ToAdd, Fpr, Hfs),
    List = random_list(ToAdd),
    TestBF = bloom:add(BaseBF, List),

    AddT = measure_util:time_avg(fun() -> for_to_ex(1, ToAdd,
                                                    fun(I) -> I end,
                                                    fun(I, B) -> bloom:add(B, I) end,
                                                    BaseBF)
                                 end,
                                 ExecTimes, []),
    
    AddListT = measure_util:time_avg(fun() -> bloom:add(BaseBF, List) end,
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% UTILS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
