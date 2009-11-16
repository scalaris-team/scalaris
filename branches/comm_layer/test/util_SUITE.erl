%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : util_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/util.erl
%%%
%%% Created :  22 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(util_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [is_between, is_between_closed, trunc, min_max, largest_smaller_than,
     gb_trees_foldl].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

is_between(_Config) ->
    ?assert(util:is_between("1", "2", "3")),
    ?assert(not util:is_between("1", "4", "3")),
    ?assert(util:is_between("3", "4", "1")),
    ?assert(not util:is_between("3", "2", "1")),
    ?assert(util:is_between("1", "2", "2")),
    ?assert(not util:is_between("1", "1", "2")),
    ?assert(util:is_between("2", "1", "1")),
    ?assert(not util:is_between("2", "2", "1")),
    ok.


is_between_closed(_Config) ->
    ?assert(util:is_between_closed("1", "2", "3")),
    ?assert(not util:is_between_closed("1", "4", "3")),
    ?assert(util:is_between_closed("3", "4", "1")),
    ?assert(not util:is_between_closed("3", "2", "1")),
    ?assert(not util:is_between_closed("1", "2", "2")),
    ?assert(not util:is_between_closed("1", "1", "2")),

    ?assert(not util:is_between_closed("2", "1", "1")),
    ?assert(not util:is_between_closed("2", "2", "1")),
    ok.

trunc(_Config) ->
    ?assert(util:trunc([1, 2, 3], 1) == [1]),
    ?assert(util:trunc([1, 2, 3], 2) == [1, 2]),
    ?assert(util:trunc([1, 2, 3], 3) == [1, 2, 3]),
    ?assert(util:trunc([1, 2, 3], 4) == [1, 2, 3]),
    ok.

min_max(_Config) ->
    ?assert(util:min(1, 2) == 1),
    ?assert(util:min(2, 1) == 1),
    ?assert(util:min(1, 1) == 1),
    ?assert(util:max(1, 2) == 2),
    ?assert(util:max(2, 1) == 2),
    ?assert(util:max(1, 1) == 1),
    ok.

largest_smaller_than(_Config) ->
    KVs = [{1, 1}, {2, 2}, {4, 4}, {8, 8}, {16, 16}, {32, 32}, {64, 64}],
    Tree = gb_trees:from_orddict(KVs),
    ?assert(util:gb_trees_largest_smaller_than(0, Tree) == {value, 64, 64}),
    ?assert(util:gb_trees_largest_smaller_than(1, Tree) == {value, 64, 64}),
    ?assert(util:gb_trees_largest_smaller_than(2, Tree) == {value, 1, 1}),
    ?assert(util:gb_trees_largest_smaller_than(3, Tree) == {value, 2, 2}),
    ?assert(util:gb_trees_largest_smaller_than(7, Tree) == {value, 4, 4}),
    ?assert(util:gb_trees_largest_smaller_than(9, Tree) == {value, 8, 8}),
    ?assert(util:gb_trees_largest_smaller_than(31, Tree) == {value, 16, 16}),
    ?assert(util:gb_trees_largest_smaller_than(64, Tree) == {value, 32, 32}),
    ?assert(util:gb_trees_largest_smaller_than(65, Tree) == {value, 64, 64}),
    ?assert(util:gb_trees_largest_smaller_than(1000, Tree) == {value, 64, 64}),
    ok.

gb_trees_foldl(_Config) ->
    KVs = [{1, 1}, {2, 2}, {4, 4}, {8, 8}, {16, 16}, {32, 32}, {64, 64}],
    Tree = gb_trees:from_orddict(KVs),
    ?assert(util:gb_trees_foldl(fun (K, K, Acc) ->
                                        Acc + K
                                end,
                                0,
                                Tree) == 127).
