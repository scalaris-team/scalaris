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
     bloom,
     comparison].

suite() ->
    [
     {timetrap, {seconds, 30}}
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
    
    ct:pal("Performance Comparison"
           "----------------------"
           "BuildTime:~nbloom:~p~nmerkle_tree:~p~nart:~p", 
           [measure_util:print_result(BloomT, ms), 
            measure_util:print_result(MerkleT, ms), 
            measure_util:print_result(ArtT, ms)]),
    ok.

m_bloom(_I, DB, DBSize, Iterations) ->
    Fpr= 0.1,
    Hfs = hfs_lhsp:new(bloom:calc_HF_numEx(DBSize, Fpr)),
    BaseBF = bloom:new(DBSize, Fpr, Hfs),
    
    %measure Build times    
    BuildTime =
        measure_util:time_avg(fun() -> 
                                      lists:foldl(fun(I, Bloom) -> bloom:add(Bloom, I) end, BaseBF, DB)
                              end,
                              Iterations, false),    
    BuildTime.

m_merkle(I, DB, _DBSize, Iterations) ->
    BuildT = 
        measure_util:time_avg(fun() -> merkle_tree:bulk_build(I, DB) end, 
                              Iterations, false),
    
    BuildT.

m_art(I, DB, _DBSize, Iterations) ->    
    {_TreeTime, Tree} = 
        util:tc(fun() -> merkle_tree:bulk_build(I, DB) end, []),
    BuildTime = 
        measure_util:time_avg(fun() -> art:new(Tree) end, Iterations, false),
    
    BuildTime.

art(_) ->    
    ToAdd = 2000,
    ExecTimes = 100,
    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    DB = db_generator:get_db(I, ToAdd, uniform),

    {TreeTime, Tree} = 
        util:tc(fun() -> merkle_tree:bulk_build(I, DB) end, []),
    BuildTime = 
        measure_util:time_avg(fun() -> art:new(Tree) end, ExecTimes, false),    

    ct:pal("ART Performance
            ------------------------
            PARAMETER: AddedItems=~p ; ExecTimes=~p
            TreeTime (ms)= ~p
            ARTBuildTime (ms)= ~p", 
           [ToAdd, ExecTimes, TreeTime / 1000,
            measure_util:print_result(BuildTime, ms)]),
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
           ExecTimes, false),
        
    IterateT = measure_util:time_avg(
           fun() -> merkle_tree_SUITE:count_iter(merkle_tree:iterator(TestTree), 0) end, 
           ExecTimes, false),
    
    GenHashT = measure_util:time_avg(
            fun() -> merkle_tree:gen_hash(TestTree) end,
            ExecTimes, false),
    
    SimpleSizeT = measure_util:time_avg(
            fun() -> merkle_tree:size(TestTree) end,
            ExecTimes, false),
    
    DetailSizeT = measure_util:time_avg(
            fun() -> merkle_tree:size_detail(TestTree) end,
            ExecTimes, false),

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
            measure_util:print_result(BuildT, ms), 
            measure_util:print_result(IterateT),
            measure_util:print_result(GenHashT),
            measure_util:print_result(SimpleSizeT),
            measure_util:print_result(DetailSizeT)]),    
    ok.

bloom(_) ->
    %parameter
    ExecTimes = 100,
    ToAdd = 2000, % req: mod 2 = 0
    Fpr = 0.1,

    Hfs = hfs_lhsp:new(bloom:calc_HF_numEx(ToAdd, Fpr)),
    BaseBF = bloom:new(ToAdd, Fpr, Hfs),
    
    %measure Build times
    BuildTime =
        measure_util:time_avg(fun() -> for_to_ex(1, ToAdd,
                                                 fun(I) -> I end,
                                                 fun(I, B) -> bloom:add(B, I) end,
                                                 BaseBF)
                              end,
                              ExecTimes, false),
    %measure join time
    BF1 = for_to_ex(1,
                    round(ToAdd / 2),
                    fun(I) -> I end, fun(I, B) -> bloom:add(B, I) end, BaseBF),
    BF2 = for_to_ex(round(ToAdd / 2) + 1,
                    ToAdd,
                    fun(I) -> I end, fun(I, B) -> bloom:add(B, I) end, BaseBF),
    JoinTime =
        measure_util:time_avg(fun() -> bloom:join(BF1, BF2) end, ExecTimes, false),

    %print results
    ct:pal("EXECUTION TIMES in microseconds
           PARAMETER: AddedItems=~p ; ExecTimes=~p
           BuildTimes: ~p
           JoinTimes : ~p",
           [ToAdd, ExecTimes, 
            measure_util:print_result(BuildTime, ms), 
            measure_util:print_result(JoinTime, us)]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% UTILS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
