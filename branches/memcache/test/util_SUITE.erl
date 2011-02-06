% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Unit tests for src/util.erl.
%%% @end
%% @version $Id$
-module(util_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [min_max, largest_smaller_than, gb_trees_foldl].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    unittest_helper:end_per_suite(Config),
    ok.

min_max(_Config) ->
    ?equals(util:min(1, 2), 1),
    ?equals(util:min(2, 1), 1),
    ?equals(util:min(1, 1), 1),
    ?equals(util:max(1, 2), 2),
    ?equals(util:max(2, 1), 2),
    ?equals(util:max(1, 1), 1),
    ok.

largest_smaller_than(_Config) ->
    KVs = [{1, 1}, {2, 2}, {4, 4}, {8, 8}, {16, 16}, {32, 32}, {64, 64}],
    Tree = gb_trees:from_orddict(KVs),
    ?equals(util:gb_trees_largest_smaller_than(0, Tree), nil),
    ?equals(util:gb_trees_largest_smaller_than(1, Tree), nil),
    ?equals(util:gb_trees_largest_smaller_than(2, Tree), {value, 1, 1}),
    ?equals(util:gb_trees_largest_smaller_than(3, Tree), {value, 2, 2}),
    ?equals(util:gb_trees_largest_smaller_than(7, Tree), {value, 4, 4}),
    ?equals(util:gb_trees_largest_smaller_than(9, Tree), {value, 8, 8}),
    ?equals(util:gb_trees_largest_smaller_than(31, Tree), {value, 16, 16}),
    ?equals(util:gb_trees_largest_smaller_than(64, Tree), {value, 32, 32}),
    ?equals(util:gb_trees_largest_smaller_than(65, Tree), {value, 64, 64}),
    ?equals(util:gb_trees_largest_smaller_than(1000, Tree), {value, 64, 64}),
    ok.

gb_trees_foldl(_Config) ->
    KVs = [{1, 1}, {2, 2}, {4, 4}, {8, 8}, {16, 16}, {32, 32}, {64, 64}],
    Tree = gb_trees:from_orddict(KVs),
    ?assert(util:gb_trees_foldl(fun (K, K, Acc) ->
                                        Acc + K
                                end,
                                0,
                                Tree) =:= 127).
