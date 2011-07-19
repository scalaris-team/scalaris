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
          split,
          insert1,
          treeHash,
          branchTest
         ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

split(_) ->
    I1 = intervals:new('[', 1, 1000, ']'),
    Split1 = intervals:split(I1, 4),
    ct:pal("Split1=~w", [Split1]),
    ?equals(length(Split1), 4),
    ct:pal("Split 1-1000 = ~p", [Split1]),
    I2 = intervals:new('[', ?PLUS_INFINITY - 800, ?MINUS_INFINITY + 1200, ']'),
    Split2 = intervals:split(I2, 4),
    ct:pal("Split wrapping = ~p", [Split2]),
    ok.

insert1(_) ->    
    Tree1 = build_tree(intervals:new('[', 1, 1000, ']'), [{1, 63}]),
    ?equals(merkle_tree:size(Tree1), 1),
    Tree2 = add_to_tree(950, 1000, Tree1),
    ?equals(merkle_tree:size(Tree2), 3),
    ok.

treeHash(_) ->
    Interval = intervals:new('[', 1, 1000, ']'),
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
    Interval = intervals:new('[', 1, 100, ']'),
    Tree1 = build_tree(Interval, {3, 5}, [{1, 5}, {50, 55}]),
    Tree2 = build_tree(Interval, {3, 10}, [{1, 10}, {50, 60}]),
    Tree3 = build_tree(Interval, {10, 10}, [{1, 10}, {11, 15}, {40, 49}, {90, 99}]),
    ?equals(merkle_tree:size(Tree1), 4),
    ?equals(merkle_tree:size(Tree2), 4),
    ?equals(merkle_tree:size(Tree3), 11),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
                                merkle_tree:new(Interval, Branch, Bucket);
                            _ -> merkle_tree:new(Interval)                        
                        end,
                        KeyIntervalList),
    merkle_tree:gen_hashes(Tree1).

add_to_tree(To, To, Tree) ->
    Tree;
add_to_tree(From, To, Tree) ->
    add_to_tree(From + 1, To, merkle_tree:insert(From, someVal, Tree)).
