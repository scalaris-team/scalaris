%  @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%% @doc    Tests for merkle tree module.
%% @end
%% @version $Id$
-module(merkle_tree_SUITE).
-author('mlange@informatik.hu-berlin.de').
-vsn('$Id$').

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
    rt_SUITE:register_value_creator(),
    unittest_helper:start_minimal_procs(Config, [], true).

end_per_suite(Config) ->
    rt_SUITE:unregister_value_creator(),
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

eprof(_) ->
    L=0,
    R=193307343591240590005637476551917548364,
    ToAdd=1273,
    
    I = intervals:new('[', L, R, ']'),
    ct:pal("L=~p ; R=~p ; ToAdd=~p", [L, R, ToAdd]),
    Keys = db_generator:get_db(I, ToAdd, uniform, [{output, list_keytpl}]),
    ct:pal("DB GEN OK"),

    _ = eprof:start(),
    Fun = fun() -> merkle_tree:new(I, Keys, []) end,
    eprof:profile([], Fun),
    eprof:stop_profiling(),
    eprof:analyze(procs, [{sort, time}]),
    
    ok.

fprof(_) ->
    L=0,
    R=193307343591240590005637476551917548364,
    ToAdd=1273,
    
    I = intervals:new('[', L, R, ']'),
    ct:pal("L=~p ; R=~p ; ToAdd=~p", [L, R, ToAdd]),
    Keys = db_generator:get_db(I, ToAdd, uniform, [{output, list_keytpl}]),
    ct:pal("DB GEN OK"),

    fprof:apply(merkle_tree, new, [I, Keys, []]),
    fprof:profile(),
    fprof:analyse(),
    
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_lookup(intervals:key(), intervals:key()) -> true.
prop_lookup(L, R) ->
    ToAdd = 200,
    I = unittest_helper:build_interval(L, R),
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
    case ?RT:get_replica_keys(?MINUS_INFINITY) of
        [_K1, K2, _K3, K4] -> prop_lookup(K4, K2);
        _ -> ok
    end,
    tester:test(?MODULE, prop_lookup, 2, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_empty(_) ->
    Tree1 = merkle_tree:new(intervals:empty()),
    Tree1a = merkle_tree:gen_hash(Tree1),
    Tree2 = merkle_tree:new(intervals:all()),
    Tree2a = merkle_tree:gen_hash(Tree2),
    Tree3 = merkle_tree:new(intervals:empty(), [], []),
    Tree3a = merkle_tree:gen_hash(Tree3),
    Tree4 = merkle_tree:new(intervals:all(), [], []),
    Tree4a = merkle_tree:gen_hash(Tree4),
    Tree5 = merkle_tree:new(intervals:empty(), [], [{keep_bucket, true}]),
    Tree5a = merkle_tree:gen_hash(Tree5),
    Tree6 = merkle_tree:new(intervals:all(), [], [{keep_bucket, true}]),
    Tree6a = merkle_tree:gen_hash(Tree6),

    ?assert(merkle_tree:is_empty(Tree1)),
    ?assert(merkle_tree:is_empty(Tree1a)),
    ?assert(merkle_tree:is_empty(Tree2)),
    ?assert(merkle_tree:is_empty(Tree2a)),
    ?assert(merkle_tree:is_empty(Tree3)),
    ?assert(merkle_tree:is_empty(Tree3a)),
    ?assert(merkle_tree:is_empty(Tree4)),
    ?assert(merkle_tree:is_empty(Tree4a)),
    ?assert(merkle_tree:is_empty(Tree5)),
    ?assert(merkle_tree:is_empty(Tree5a)),
    ?assert(merkle_tree:is_empty(Tree6)),
    ?assert(merkle_tree:is_empty(Tree6a)),

    ?equals(Tree1, Tree1a),
    ?equals(Tree2, Tree2a),
    ?equals(Tree3, Tree3a),
    ?equals(Tree4, Tree4a),
    ?equals(Tree5, Tree5a),
    ?equals(Tree6, Tree6a),

    ?equals(Tree1, Tree3),
    ?equals(Tree2, Tree4),
    ?equals(merkle_tree:get_root(Tree3), merkle_tree:get_root(Tree5)),
    ?equals(merkle_tree:get_root(Tree4), merkle_tree:get_root(Tree6)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Tests branching and bucketing
-spec prop_branch_bucket(intervals:key(), intervals:key(),
                         BranchFactor::2..16, BucketSize::24..512) -> true.
prop_branch_bucket(L, R, Branch, Bucket) ->
    I = unittest_helper:build_interval(L, R),
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
prop_tree_hash(L, R, ToAdd) ->
    I = unittest_helper:build_interval(L, R),
    DB1 = db_generator:get_db(I, ToAdd, uniform, [{output, list_keytpl}]),
    DB1a = util:shuffle(DB1),
    DB1Size = length(DB1),
    
    DB1Tree1 = merkle_tree:new(I, DB1, []),
    DB1Tree2 = merkle_tree:new(I, DB1, []),
    DB1Tree2a = merkle_tree:gen_hash(merkle_tree:new(I, DB1, [])),
    DB1Tree2b = merkle_tree:gen_hash(merkle_tree:insert_list(DB1, merkle_tree:new(I, [{keep_bucket, true}]))),
    DB1Tree2c = merkle_tree:gen_hash(merkle_tree:insert_list(DB1, merkle_tree:new(I, [{keep_bucket, true}])), true),
    
    DB1aTree1 = merkle_tree:new(I, DB1a, []),
    DB1aTree2 = merkle_tree:new(I, DB1a, []),
    DB1aTree2a = merkle_tree:gen_hash(merkle_tree:new(I, DB1a, [])),
    DB1aTree2b = merkle_tree:gen_hash(merkle_tree:insert_list(DB1a, merkle_tree:new(I, [{keep_bucket, true}]))),
    DB1aTree2c = merkle_tree:gen_hash(merkle_tree:insert_list(DB1a, merkle_tree:new(I, [{keep_bucket, true}])), true),
    
    DB3Size = DB1Size + 1,
    DB3Tree1 = build_tree(I, DB3Size, uniform),
    
    DB1RootHash1 = merkle_tree:get_hash(DB1Tree1),
    DB1RootHash2 = merkle_tree:get_hash(DB1Tree2),
    DB1RootHash2a = merkle_tree:get_hash(DB1Tree2a),
    DB1RootHash2b = merkle_tree:get_hash(DB1Tree2b),
    DB1RootHash2c = merkle_tree:get_hash(DB1Tree2c),
    
    DB1aRootHash1 = merkle_tree:get_hash(DB1aTree1),
    DB1aRootHash2 = merkle_tree:get_hash(DB1aTree2),
    DB1aRootHash2a = merkle_tree:get_hash(DB1aTree2a),
    DB1aRootHash2b = merkle_tree:get_hash(DB1aTree2b),
    DB1aRootHash2c = merkle_tree:get_hash(DB1aTree2c),
    
    DB3RootHash1 = merkle_tree:get_hash(DB3Tree1),
    
    ?equals(DB1RootHash1, DB1RootHash2),
    ?equals(DB1RootHash2, DB1RootHash2a),
    ?equals(DB1RootHash2, DB1RootHash2b),
    ?equals(DB1RootHash2, DB1RootHash2c),
    
    ?equals(DB1aRootHash1, DB1aRootHash2),
    ?equals(DB1aRootHash2, DB1aRootHash2a),
    ?equals(DB1aRootHash2, DB1aRootHash2b),
    ?equals(DB1aRootHash2, DB1aRootHash2c),
    
    ?equals(DB1RootHash1, DB1aRootHash1),
    
    ?compare(fun erlang:'>'/2, DB1RootHash1, 0),
    ?compare(fun erlang:'>'/2, DB3RootHash1, 0),

    MTSize = merkle_tree:size_detail(DB3Tree1),
    case element(4, MTSize) of
        DB1Size ->
            % could not split the interval any further to add one more item
            % -> items should be equal and thus also the hashes
            ?equals(DB3RootHash1, DB1RootHash1);
        DB3Size ->
            % there is one more item and thus the hashes should differ
            ?compare(fun erlang:'=/='/2, DB3RootHash1, DB1RootHash1);
        _ ->
            ?ct_fail("DB1: ~B, DB3: ~B, Other: ~p", [DB1Size, DB3Size, MTSize])
    end.

tester_tree_hash(_) ->
    case rt_SUITE:default_rt_has_chord_keys() of
        true ->
            prop_tree_hash(310893024101176788593096495898246585537, 310893024101176788593096495898246585538, 83),
            prop_tree_hash(310893024101176788593096495898246585537, 56222597632513189683777859800513757781, 83);
        _ -> ok
    end,
    tester:test(?MODULE, prop_tree_hash, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_insert_list(intervals:key(), intervals:key(), 1..2500) -> true.
prop_insert_list(L, R, Count) ->
    I = unittest_helper:build_interval(L, R),
    DB = db_generator:get_db(I, Count, uniform, [{output, list_keytpl}]),
    Tree = merkle_tree:insert_list(DB, merkle_tree:new(I, [{keep_bucket, true}])),
    ItemCount = merkle_tree:get_item_count(Tree),
    ?equals_w_note(Count, ItemCount,
                   io_lib:format("DB=~p~nTree=~p~nCount=~p; DBSize=~p; TreeCount=~p",
                                 [DB, Tree, Count, length(DB), ItemCount])).

tester_insert_list(_) ->
    tester:test(?MODULE, prop_insert_list, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_bulk_insert(intervals:key(), intervals:key(), 1..50) -> true.
prop_bulk_insert(L, R, BucketSize) ->
    I = unittest_helper:build_interval(L, R),
    DB = db_generator:get_db(I, BucketSize, uniform, [{output, list_keytpl}]),
    Tree1 = merkle_tree:new(I, DB, [{bucket_size, BucketSize}]),
    Tree2 = merkle_tree:new(I, DB, [{bucket_size, BucketSize}]),
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
prop_size(L, R, ToAdd) ->
    I = unittest_helper:build_interval(L, R),
    Tree = build_tree(I, ToAdd, uniform),
    Size = merkle_tree:size(Tree),
    {Inner, Leafs, EmptyLeafs, Items} = merkle_tree:size_detail(Tree),
    ?equals_w_note(Size, Inner + Leafs,
                   io_lib:format("TreeSize~nItemsAdded: ~p
                                  Simple: ~p Nodes
                                  InnerNodes: ~B   ;   Leafs: ~B   ;   Items: ~B",
                                 [ToAdd, Size, Inner, Leafs, Items])),
    ?equals_w_note(Leafs, merkle_tree:get_leaf_count(Tree),
                   io_lib:format("TreeSize~nItemsAdded: ~p
                                  Simple: ~p Nodes
                                  InnerNodes: ~B   ;   Leafs: ~B   ;   Items: ~B",
                                 [ToAdd, Size, Inner, Leafs, Items])),
    ?equals_w_note(Items, merkle_tree:get_item_count(Tree),
                   io_lib:format("TreeSize~nItemsAdded: ~p
                                  Simple: ~p Nodes
                                  InnerNodes: ~B   ;   Leafs: ~B   ;   Items: ~B",
                                 [ToAdd, Size, Inner, Leafs, Items])),
    ?equals_w_note(EmptyLeafs,
                   length([ok || Leaf <- merkle_tree:get_leaves(Tree),
                                 merkle_tree:is_empty(Leaf)]),
                   io_lib:format("TreeSize~nItemsAdded: ~p
                                  Simple: ~p Nodes
                                  InnerNodes: ~B   ;   Leafs: ~B   ;   Items: ~B",
                                 [ToAdd, Size, Inner, Leafs, Items])).
    
tester_size(_) ->
  tester:test(?MODULE, prop_size, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_iter(intervals:key(), intervals:key(), 1000..2000) -> true.
prop_iter(L, R, ToAdd) ->
    I = unittest_helper:build_interval(L, R),
    Tree = build_tree(I, ToAdd, uniform),
    {Inner, Leafs, _EmptyLeafs, _Items} = merkle_tree:size_detail(Tree),
    Count = iterate(merkle_tree:iterator(Tree), fun(_, Acc) -> Acc + 1 end, 0),
    ?equals_w_note(Count, Inner + Leafs,
                   io_lib:format("Args: Interval=[~p, ~p] - ToAdd =~p~n",
                                 [L, R, ToAdd])).

tester_iter(_Config) ->
    tester:test(?MODULE, prop_iter, 3, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_store_to_dot(intervals:key(), intervals:key(), 1..1000) -> true.
prop_store_to_dot(L, R, ToAdd) ->
    ct:pal("PARAMS: L=~p ; R=~p ; ToAdd=~p", [L, R, ToAdd]),
    I = unittest_helper:build_interval(L, R),
    Tree = build_tree(I, ToAdd, uniform),
    {Inner, Leafs, EmptyLeafs, Items} = merkle_tree:size_detail(Tree),
    ct:pal("Tree Size Added =~B - Inner=~B ; Leafs=~B (empty: ~B) ; Items=~B
            Saved to ../MerkleTree.png", [ToAdd, Inner, Leafs, EmptyLeafs, Items]),
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
    is_subtype(Config,       merkle_tree:mt_config_params()),
    is_subtype(ToAdd,        non_neg_integer()),
    is_subtype(Distribution, db_generator:distribution()),
    is_subtype(Tree,         merkle_tree:merkle_tree()).
build_tree(Interval, Config, ToAdd, Distribution) ->
    Keys = db_generator:get_db(Interval, ToAdd, Distribution, [{output, list_keytpl}]),
    merkle_tree:new(Interval, Keys, Config).

-spec iterate(merkle_tree:mt_iter(), fun((merkle_tree:mt_node(), T) -> T), T) -> T.
iterate(none, _, Acc) -> Acc;
iterate(Iter, Fun, Acc) ->
    Next = merkle_tree:next(Iter),
    case Next of
        none -> Acc;
        {Node, Iter2} -> iterate(Iter2, Fun, Fun(Node, Acc))
    end.
