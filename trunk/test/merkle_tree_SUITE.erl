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
          insert1
		 ].


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
    Root = merkle_tree:new(intervals:new('[', 1, 1000, ']')),
    Tree1 = add_to_tree(1, 63, Root),
    ?equals(merkle_tree:size(Tree1), 1),
    ct:pal("<<<<1 Add OK Size=~p", [merkle_tree:size(Tree1)]),
    Tree2 = add_to_tree(950, 1000, Tree1),
    ct:pal("<<<<2 Add OK Size=~p", [merkle_tree:size(Tree2)]),
    ?equals(merkle_tree:size(Tree2), 3),
    ok.

add_to_tree(To, To, Tree) ->
    Tree;
add_to_tree(From, To, Tree) ->
    add_to_tree(From + 1, To, merkle_tree:insert(From, someVal, Tree)).