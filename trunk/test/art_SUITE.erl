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
%%% File    art_SUITE.erl
%%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%%% @doc    Tests for art module (approximate reconciliation tree).
%%% @end
%%% Created : 11/11/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(art_SUITE).

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() -> [
          tester_new,
          tester_lookup,
          eprof
         ].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    _ = crypto:start(),
    Config.

end_per_suite(_Config) ->
    crypto:stop(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_new(intervals:key(), intervals:key()) -> boolean().
prop_new(L, L) -> true;
prop_new(L, R) ->
    I = intervals:new('[', L, R, ']'),
    DB = db_generator:get_db(I, 400, uniform),
    Art1 = art:new(),
    Conf1 = art:get_config(Art1),
    Art2 = art:new(merkle_tree:new(I, DB, []), 
                   [{correction_factor, proplists:get_value(correction_factor, Conf1) + 1},
                    {inner_bf_fpr, proplists:get_value(inner_bf_fpr, Conf1) + 0.1},
                    {leaf_bf_fpr, proplists:get_value(leaf_bf_fpr, Conf1) + 0.1}]),
    Conf2 = art:get_config(Art2),
    ?equals(proplists:get_value(correction_factor, Conf1) + 1,
            proplists:get_value(correction_factor, Conf2)),
    ?equals(proplists:get_value(inner_bf_fpr, Conf1) + 0.1,
            proplists:get_value(inner_bf_fpr, Conf2)),
    ?equals(proplists:get_value(leaf_bf_fpr, Conf1) + 0.1,
            proplists:get_value(leaf_bf_fpr, Conf2)).

tester_new(_) ->
    tester:test(?MODULE, prop_new, 2, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_lookup(intervals:key(), intervals:key()) -> boolean().
prop_lookup(L, L) -> true;    
prop_lookup(L, R) ->    
    I = intervals:new('[', L, R, ']'),
    DB = db_generator:get_db(I, 400, uniform),
    Tree = merkle_tree:new(I, DB, []),
    Art = art:new(Tree),
    Found = nodes_in_art(merkle_tree:iterator(Tree), Art, 0),
    ct:pal("TreeNodes=~p ; Found=~p", [merkle_tree:size(Tree), Found]),
    ?assert(Found > 0).

-spec nodes_in_art(merkle_tree:mt_iter(), art:art(), non_neg_integer()) -> non_neg_integer().
nodes_in_art(Iter, Art, Acc) ->
    case merkle_tree:next(Iter) of
        none -> Acc;
        {Node, NewIter} -> 
            nodes_in_art(NewIter, Art, Acc + ?IIF(art:lookup(Node, Art), 1, 0))
    end.

tester_lookup(_) ->
  tester:test(?MODULE, prop_lookup, 2, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

eprof(_) ->
    L=0,
    R=193307343591240590005637476551917548364,
    ToAdd=1273,
    
    I = intervals:new('[', L, R, ']'),
    Keys = db_generator:get_db(I, ToAdd, uniform),
    Merkle = merkle_tree:new(I, Keys, []),
        
    eprof:start(),
    Fun = fun() -> art:new(Merkle) end,
    eprof:profile([], Fun),
    eprof:stop_profiling(),
    eprof:analyze(procs, [{sort, time}]),
    
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

