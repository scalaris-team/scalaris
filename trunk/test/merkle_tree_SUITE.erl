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
          insert_bucketing,
          treeHash,
          branchTest,          
          storeToDot,
          sizeTest,
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

insert_bucketing(_) ->    
    DefBucketSize = merkle_tree:get_bucket_size(merkle_tree:empty()),
    Tree1 = build_tree(intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(1000), ']'),
                       [{1, DefBucketSize - 1}]),
    ?equals(merkle_tree:size(Tree1), 1),
    Tree2 = add_to_tree(1000 - DefBucketSize + 1, 1000, Tree1),
    ?equals(merkle_tree:size(Tree2), 3),
    ok.

treeHash(_) ->
    Interval = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(1000), ']'),
    Tree1 = build_tree(Interval, [{450, 500}, {1, 63}]),
    Tree2 = build_tree(Interval, [{450, 500}, {1, 63}]),
    Tree3 = build_tree(Interval, [{451, 500}, {1, 63}]),
    RootHash1 = merkle_tree:get_hash(Tree1),
    RootHash2 = merkle_tree:get_hash(Tree2),
    RootHash3 = merkle_tree:get_hash(Tree3),
    ct:pal("Hash1=[~p]~nHash2=[~p]~nHash3=[~p]", [RootHash1, RootHash2, RootHash3]),
    ?equals(RootHash1, RootHash2),
    ?assert(RootHash1 > 0),
    ?assert(RootHash3 > 0),
    ?assert(RootHash3 =/= RootHash1),
    ok.

branchTest(_) ->
    Interval = intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(100), ']'),
    Tree1 = build_tree(Interval, {3, 5}, [{1, 5}, {50, 55}]),
    Tree2 = build_tree(Interval, {3, 10}, [{1, 10}, {50, 60}]),
    Tree3 = build_tree(Interval, {10, 10}, [{1, 10}, {11, 15}, {40, 49}, {90, 99}]),
    ?equals(merkle_tree:size(Tree1), 4),
    ?equals(merkle_tree:size(Tree2), 4),
    ?equals(merkle_tree:size(Tree3), 11),
    ok.

sizeTest(_) ->
    DefBucketSize = merkle_tree:get_bucket_size(merkle_tree:empty()),
    Tree1 = build_tree(intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(1000), ']'),
                       [{1, DefBucketSize - 1}]),
    Tree2 = add_to_tree(1000 - 2 * DefBucketSize, 1000, Tree1),
    {Inner, Leafs} = merkle_tree:size_detail(Tree2),
    Size = merkle_tree:size(Tree2),
    ct:pal("Size=~p ; InnerCount=~p ; Leafs=~p", [Size, Inner, Leafs]),
    ?equals(Size, Inner + Leafs),
    ok.

-spec prop_iter(intervals:key(), intervals:key(), 1..10000) -> true.
prop_iter(X, Y, Items) ->
    I = intervals:new('[', X, Y, ']'),
    ToInsert = if 
                   erlang:abs(Y - X) < Items -> erlang:abs(Y - X) - 10;
                   true -> Items
               end,
    {_, LKey, _RKey, _} = intervals:get_bounds(I),
    {BuildT, Tree} = util:tc(fun() -> add_to_tree(LKey + 1, LKey + ToInsert, merkle_tree:new(I)) end),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),
    {IterateT, Count} = util:tc(fun() -> count_iter(merkle_tree:iterator(Tree), 0) end),
    ct:pal("Args: Interval=[~p, ~p] - ToAdd =~p~n"
           "Tree: BuildingTime=~p s ; IterationTime=~p s", 
           [X, Y, Items, BuildT / (1000*1000), IterateT / (1000*1000)]),
    ?equals(Count, Inner + Leafs),
    true.

tester_iter(_Config) ->
    %prop_iter(0, 10000001, 10000).
    tester:test(?MODULE, prop_iter, 3, 1).

storeToDot(_) ->
    DefBucketSize = merkle_tree:get_bucket_size(merkle_tree:empty()),
    Tree = build_tree(intervals:new('[', rt_SUITE:number_to_key(1), rt_SUITE:number_to_key(1000), ']'),
                       [{1, 3*DefBucketSize - 1}, {1000 - 4*DefBucketSize + 1, 1000}]),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),
    ct:pal("Tree Size - Inner=~p ; Leafs=~p", [Inner, Leafs]),
    merkle_tree:store_to_DOT(Tree),
    ok.

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

-spec build_tree(intervals:interval(), [{Min::pos_integer(), Max::pos_integer()}]) 
        -> merkle_tree:merkle_tree().
build_tree(Interval, KeyIntervalList) ->
    build_tree(Interval, {}, KeyIntervalList).

build_tree(Interval, Config, KeyIntervalList) ->
    Tree1 = lists:foldl(fun({From, To}, Tree) -> 
                                add_to_tree(From, To, Tree)
                        end,
                        case size(Config) of                            
                            2 ->
                                {Branch, Bucket} = Config,
                                merkle_tree:new(Interval, 
                                                [{branch_factor, Branch}, {bucket_size, Bucket}]);
                            _ -> merkle_tree:new(Interval)
                        end,
                        KeyIntervalList),
    merkle_tree:gen_hash(Tree1).

add_to_tree(To, To, Tree) ->
    Tree;
add_to_tree(From, To, Tree) ->
    add_to_tree(From + 1, To, merkle_tree:insert(rt_SUITE:number_to_key(From), someVal, Tree)).
