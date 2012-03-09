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
%% @version $Id$

-module(merkle_tree_SUITE).

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

all() -> [
          %tester_branch_bucket,
          tester_size,
          tester_store_to_dot,          
          tester_tree_hash,
          tester_insert_list,
          tester_bulk_insert,
          tester_iter,
          tester_lookup,
          test_empty
          %eprof,
          %fprof          
         ].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

eprof(_) ->
    %L=306299575959936430269855431475160361337,
    L=0,
    R=193307343591240590005637476551917548364,
    ToAdd=1273,
    
    I = intervals:new('[', L, R, ']'),
    ct:pal("L=~p ; R=~p ; ToAdd=~p", [L, R, ToAdd]),
    Keys = db_generator:get_db(I, ToAdd, uniform),
    ct:pal("DB GEN OK"),

    eprof:start(),
    Fun = fun() -> merkle_tree:bulk_build(I, [], Keys) end,
    eprof:profile([], Fun),
    eprof:stop_profiling(),
    eprof:analyze(procs, [{sort, time}]),
    
    ok.

fprof(_) ->
    %L=306299575959936430269855431475160361337,
    L=0,
    R=193307343591240590005637476551917548364,
    ToAdd=1273,
    
    I = intervals:new('[', L, R, ']'),
    ct:pal("L=~p ; R=~p ; ToAdd=~p", [L, R, ToAdd]),
    Keys = db_generator:get_db(I, ToAdd, uniform),
    ct:pal("DB GEN OK"),

    fprof:apply(merkle_tree, bulk_build, [I, [], Keys]),
    fprof:profile(),
    fprof:analyse(),
    
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_lookup(intervals:key(), intervals:key()) -> true.
prop_lookup(L, L) -> true;
prop_lookup(L, R) ->
    ToAdd = 200,
    I = intervals:new('[', L, R, ']'),
    Tree = build_tree(I, [], ToAdd, uniform),
    Branch = merkle_tree:get_branch_factor(Tree),
    SplitI = intervals:split(I, Branch),
    SplitI2 = intervals:split(I, Branch + 1),
    lists:foreach(
      fun(SubI) -> 
              ?assert(merkle_tree:lookup(SubI, Tree) =/= not_found)
      end, SplitI),
    lists:foreach(
      fun(SubI) -> 
              ?assert(merkle_tree:lookup(SubI, Tree) =:= not_found)
      end, SplitI2),    
    true.

tester_lookup(_) ->
    tester:test(?MODULE, prop_lookup, 2, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_empty(_) ->
    Tree = merkle_tree:new(intervals:empty()),
    Empty = merkle_tree:empty(),    
    ?equals(Tree, Empty),
    ?assert(merkle_tree:is_empty(Tree)),
    ?assert(merkle_tree:is_empty(Empty)).    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Tests branching and bucketing
-spec prop_branch_bucket(intervals:key(), intervals:key(), 
                         BranchFactor::2..16, BucketSize::24..512) -> true.
prop_branch_bucket(L, L, _, _) -> true;
prop_branch_bucket(L, R, Branch, Bucket) ->
    I = intervals:new('[', L, R, ']'),
    Config = [{branch_factor, Branch}, {bucket_size, Bucket}],
    Tree1 = build_tree(I, Config, Bucket, uniform),
    Tree2 = build_tree(I, Config, Bucket + 1, uniform),
    ct:pal("Branch=~p ; Bucket=~p ; Tree1Size=~p ; Tree2Size=~p", 
           [Branch, Bucket, merkle_tree:size(Tree1), merkle_tree:size(Tree2)]),
    ?equals(merkle_tree:size(Tree1), 1),    
    ?equals(merkle_tree:size(Tree2), Branch + 1).

tester_branch_bucket(_) ->
    tester:test(?MODULE, prop_branch_bucket, 4, 10, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Tests hash generation
-spec prop_tree_hash(intervals:key(), intervals:key(), 1..100) -> true.
prop_tree_hash(L, L, _) -> true;
prop_tree_hash(L, R, ToAdd) ->
    I = intervals:new('[', L, R, ']'),
    DB = db_generator:get_db(I, ToAdd, uniform),
    
    Tree1 = merkle_tree:gen_hash(merkle_tree:bulk_build(I, DB)),
    Tree2 = merkle_tree:gen_hash(merkle_tree:bulk_build(I, DB)),
    Tree3 = build_tree(I, ToAdd + 1, uniform),
    
    RootHash1 = merkle_tree:get_hash(Tree1),
    RootHash2 = merkle_tree:get_hash(Tree2),
    RootHash3 = merkle_tree:get_hash(Tree3),    
    ?equals(RootHash1, RootHash2),
    ?assert(RootHash1 > 0),
    ?assert(RootHash3 > 0),
    ?assert(RootHash3 =/= RootHash1).

tester_tree_hash(_) ->
    tester:test(?MODULE, prop_tree_hash, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_insert_list(intervals:key(), intervals:key(), 1..2500) -> true.
prop_insert_list(L, L, _) -> true;
prop_insert_list(L, R, Count) ->
    I = intervals:new('[', L, R, ']'),
    DB = db_generator:get_db(I, Count, uniform),
    Tree = merkle_tree:insert_list(DB, merkle_tree:new(I)),
    ItemCount = iterate(merkle_tree:iterator(Tree), 
                        fun(N, Acc) -> Acc + case merkle_tree:is_leaf(N) of
                                                 true -> merkle_tree:get_item_count(N);
                                                 false -> 0
                                             end
                        end, 
                        0),
    ?equals_w_note(Count, ItemCount, 
                   io_lib:format("DB=~p~nTree=~p~nCount=~p; DBSize=~p; TreeCount=~p", 
                                 [DB, Tree, Count, length(DB), ItemCount])).

tester_insert_list(_) ->
    tester:test(?MODULE, prop_insert_list, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_bulk_insert(intervals:key(), intervals:key(), 1..50) -> true.
prop_bulk_insert(L, L, _) -> true;
prop_bulk_insert(L, R, BucketSize) ->
    I = intervals:new('[', L, R, ']'),
    DB = db_generator:get_db(I, BucketSize, uniform),
    Tree1 = merkle_tree:bulk_build(I, [{bucket_size, BucketSize}], DB),
    Tree2 = merkle_tree:bulk_build(I, [{bucket_size, BucketSize}], DB),    
    Tree3 = build_tree(I, [{bucket_size, BucketSize}], BucketSize * 2 + 1, uniform),    
    Size1 = merkle_tree:size(Tree1),
    Size2 = merkle_tree:size(Tree2),
    Size3 = merkle_tree:size(Tree3),
    ?equals(Size1, Size2),
    ?assert(Size1 < Size3).

tester_bulk_insert(_) ->
    tester:test(?MODULE, prop_bulk_insert, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_size(intervals:key(), intervals:key(), 1..100) -> true.
prop_size(L, L, _) -> true;
prop_size(L, R, ToAdd) ->
    I = intervals:new('[', L, R, ']'),
    Tree = build_tree(I, ToAdd, uniform),
    Size = merkle_tree:size(Tree),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),
    ?equals_w_note(Size, Inner + Leafs, 
                   io_lib:format("TreeSize~nItemsAdded: ~p
                                  Simple: ~p Nodes
                                  InnerNodes: ~p   ;   Leafs: ~p",
                                 [ToAdd, Size, Inner, Leafs])).
    
tester_size(_) ->
  tester:test(?MODULE, prop_size, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_iter(intervals:key(), intervals:key(), 1000..2000) -> true.
prop_iter(L, L, _) -> true;
prop_iter(L, R, ToAdd) ->
    I = intervals:new('[', L, R, ']'),
    Tree = build_tree(I, ToAdd, uniform),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),    
    Count = iterate(merkle_tree:iterator(Tree), fun(_, Acc) -> Acc + 1 end, 0),
    ?equals_w_note(Count, Inner + Leafs,
                   io_lib:format("Args: Interval=[~p, ~p] - ToAdd =~p~n", 
                                 [L, R, ToAdd])).

tester_iter(_Config) ->
    tester:test(?MODULE, prop_iter, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_store_to_dot(intervals:key(), intervals:key(), 1..1000) -> true.
prop_store_to_dot(L, L, _) -> true;
prop_store_to_dot(L, R, ToAdd) ->
    ct:pal("PARAMS: L=~p ; R=~p ; ToAdd=~p", [L, R, ToAdd]),
    I = intervals:new('[', L, R, ']'),
    Tree = build_tree(I, ToAdd, uniform),
    {Inner, Leafs} = merkle_tree:size_detail(Tree),
    ct:pal("Tree Size Added =~p - Inner=~p ; Leafs=~p
            Saved to ..\MerkleTree.png", [ToAdd, Inner, Leafs]),
    merkle_tree:store_graph(Tree, "MerkleTree"),
    true.

tester_store_to_dot(_) ->
  tester:test(?MODULE, prop_store_to_dot, 3, 1, [{threads, 1}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_tree(I, ToAdd, Distribution) -> Tree when
    is_subtype(I,            intervals:interval()),
    is_subtype(ToAdd,        non_neg_integer()),
    is_subtype(Distribution, db_generator:distribution()),
    is_subtype(Tree,         merkle_tree:merkle_tree()).
build_tree(Interval, ToAdd, Distribution) ->
    build_tree(Interval, [], ToAdd, Distribution).

-spec build_tree(I, Config, ToAdd, Distribution) -> Tree when
    is_subtype(I,            intervals:interval()),
    is_subtype(Config,       merkle_tree:mt_config()),
    is_subtype(ToAdd,        non_neg_integer()),
    is_subtype(Distribution, db_generator:distribution()),
    is_subtype(Tree,         merkle_tree:merkle_tree()).
build_tree(Interval, Config, ToAdd, Distribution) ->
    Keys = db_generator:get_db(Interval, ToAdd, Distribution),
    T = merkle_tree:bulk_build(Interval, Config, Keys),
    merkle_tree:gen_hash(T).

-spec iterate(merkle_tree:mt_iter(), fun((merkle_tree:mt_node(), T) -> T), T) -> T. 
iterate(none, _, Acc) -> Acc;
iterate(Iter, Fun, Acc) ->
    Next = merkle_tree:next(Iter),
    case Next of
        none -> Acc;
        {Node, Iter2} -> iterate(Iter2, Fun, Fun(Node, Acc))
    end.
