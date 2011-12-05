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
%%% File    merkle_tree_SUITE.erl
%%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%%% @doc    Tests for merkle tree module.
%%% @end
%%% Created : 06/04/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id: $

-module(merkle_tree_SUITE).

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

all() -> [
          %performance,
          tester_branch_bucket,
          tester_size,
          tester_store_to_dot,          
          tester_tree_hash,
          tester_insert,
          tester_iter          
         ].

suite() ->
    [
     {timetrap, {seconds, 45}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc measures performance of merkle_tree operations
performance(_) ->
    % PARAMETER
    ExecTimes = 100,
    ToAdd = 2000,
    
    %I = intervals:new('[', ?MINUS_INFINITY, ?PLUS_INFINITY, ']'),    
    I = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100000000), ']'),
    InitTree = merkle_tree:new(I),
    TestTree = merkle_tree_builder:build(InitTree, ToAdd, uniform),
    {Inner, Leafs} = merkle_tree:size_detail(TestTree),
    
    %measure build times
    BT = measure_util:time_avg(
           fun() -> 
                   merkle_tree_builder:build(InitTree, ToAdd, uniform) 
           end,
           [], ExecTimes, false),
        
    %measure iteration time
    IT = measure_util:time_avg(fun() -> 
                                       count_iter(merkle_tree:iterator(TestTree), 0) 
                               end, 
                               [], ExecTimes, false),    
    
    ct:pal("
            Merkle_Tree Performance
            ------------------------
            PARAMETER: AddedItems=~p ; ExecTimes=~p
            TreeSize: InnerNodes=~p ; Leafs=~p,
            BuildTime (ms)= ~p
            IterationTime (us)= ~p", 
           [ToAdd, ExecTimes, Inner, Leafs,
            measure_util:print_result(BT, ms), 
            measure_util:print_result(IT, us)]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Tests branching and bucketing
-spec prop_branch_bucket(intervals:key(), intervals:key(), 
                         BranchFactor::2..16, BucketSize::24..512) -> true.
prop_branch_bucket(L, L, _, _) -> true;
prop_branch_bucket(L, R, Branch, Bucket) ->
    Tree = merkle_tree:new(intervals:new('[', L, R, ']'),
                           [{branch_factor, Branch},
                            {bucket_size, Bucket}]),    
    Tree1 = merkle_tree_builder:build(Tree, Bucket, uniform),
    Tree2 = merkle_tree_builder:build(Tree, 2 * Bucket, uniform),
    %ct:pal("Branch=~p ; Bucket=~p ; Tree1Size=~p ; Tree2Size=~p", 
    %       [Branch, Bucket, merkle_tree:size(Tree1), merkle_tree:size(Tree2)]),
    ?equals(merkle_tree:size(Tree1), 1),    
    ?equals(merkle_tree:size(Tree2), 1 + Branch),    
    true.

tester_branch_bucket(_) ->
    tester:test(?MODULE, prop_branch_bucket, 4, 10, [multi_threaded]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Tests hash generation
-spec prop_tree_hash(intervals:key(), intervals:key(), 1..100) -> true.
prop_tree_hash(L, L, _) -> true;
prop_tree_hash(L, R, ToAdd) ->
    %ct:pal("prop_tree_hash params: L=~p ; R=~p ; ToAdd=~p", [L, R, ToAdd]),
    I = intervals:new('[', L, R, ']'),
    Tree1 = merkle_tree_builder:build(merkle_tree:new(I), ToAdd, uniform),
    Tree2 = merkle_tree_builder:build(merkle_tree:new(I), ToAdd, uniform),
    Tree3 = merkle_tree_builder:build(merkle_tree:new(I), ToAdd + 1, uniform),    
    RootHash1 = merkle_tree:get_hash(Tree1),
    RootHash2 = merkle_tree:get_hash(Tree2),
    RootHash3 = merkle_tree:get_hash(Tree3),    
    %ct:pal("Hash1=[~p]~nHash2=[~p]~nHash3=[~p]", [RootHash1, RootHash2, RootHash3]),
    ?equals(RootHash1, RootHash2),
    ?assert(RootHash1 > 0),
    ?assert(RootHash3 > 0),
    ?assert(RootHash3 =/= RootHash1),
    true.

tester_tree_hash(_) ->
    tester:test(?MODULE, prop_tree_hash, 3, 10, [multi_threaded]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_insert(intervals:key(), intervals:key(), 1..1000) -> true.
prop_insert(L, L, _) -> true;
prop_insert(L, R, ToAdd) ->
    I = intervals:new('[', L, R, ']'),
    Tree1 = merkle_tree_builder:build(merkle_tree:new(I), ToAdd, uniform),
    Tree2 = merkle_tree_builder:build(merkle_tree:new(I), ToAdd, uniform),
    Tree3 = merkle_tree_builder:build(merkle_tree:new(I), 2 * ToAdd, uniform),
    Size1 = merkle_tree:size(Tree1),
    Size2 = merkle_tree:size(Tree2),
    Size3 = merkle_tree:size(Tree3),
    %ct:pal("ToAdd=~p ; Tree1Size=~p ; Tree2Size=~p ; Tree3Size=~p", [ToAdd, Size1, Size2,Size3]),
    ?equals(Size1, Size2),
    ?assert(Size1 < Size3),
    true.

tester_insert(_) ->
    tester:test(?MODULE, prop_insert, 3, 2, [multi_threaded]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_size(intervals:key(), intervals:key(), 1..100) -> true.
prop_size(L, L, _) -> true;
prop_size(L, R, ToAdd) ->
    I = intervals:new('[', L, R, ']'),
    Tree = merkle_tree_builder:build(merkle_tree:new(I), ToAdd, uniform),
    Size = merkle_tree:size(Tree),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),
    ?equals(Size, Inner + Leafs),
    true.
    
tester_size(_) ->
  tester:test(?MODULE, prop_size, 3, 10, [multi_threaded]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_iter(intervals:key(), intervals:key(), 1000..4000) -> true.
prop_iter(L, L, _) -> true;
prop_iter(L, R, ToAdd) ->
    I = intervals:new('[', L, R, ']'),
    Tree = merkle_tree_builder:build(merkle_tree:new(I), ToAdd, uniform),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),
    {IterateT, Count} = util:tc(fun() -> count_iter(merkle_tree:iterator(Tree), 0) end),
    ct:pal("Args: Interval=[~p, ~p] - ToAdd =~p~n"
           "Tree: IterationTime=~p s", 
           [L, R, ToAdd, IterateT / (1000*1000)]),
    ?equals(Count, Inner + Leafs),
    true.

tester_iter(_Config) ->
    %prop_iter(0, 10000001, 10000).
    tester:test(?MODULE, prop_iter, 3, 5, [multi_threaded]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_store_to_dot(intervals:key(), intervals:key(), 1..1000) -> true.
prop_store_to_dot(L, L, _) -> true;
prop_store_to_dot(L, R, ToAdd) ->
    I = intervals:new('[', L, R, ']'),
    Tree = merkle_tree_builder:build(merkle_tree:new(I), ToAdd, uniform),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),
    ct:pal("Tree Size Added =~p - Inner=~p ; Leafs=~p", [ToAdd, Inner, Leafs]),
    merkle_tree:store_to_DOT(Tree, "StoreToDotTest"),
    true.

tester_store_to_dot(_) ->
  tester:test(?MODULE, prop_store_to_dot, 3, 2, [multi_threaded]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
count_iter(none, Count) ->
    Count;
count_iter(Iter, Count) ->
    Next = merkle_tree:next(Iter),
    case Next of
        none -> Count;
        {_, Iter2} -> count_iter(Iter2, Count + 1)
    end.

