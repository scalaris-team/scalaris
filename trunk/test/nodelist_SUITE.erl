%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    nodelist_SUITE.erl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Unit tests for src/nodelist.erl
%%% @end
%%% Created : 18 May 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(nodelist_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [tester_new, tester_mk2, tester_mk4, tester_trunc, tester_to_list,
     tester_update_ids, tester_remove_outdated, merge].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

% use macro instead of function so that the output in case of errors is better:
-define(compare_neighborhood(Neighborhood, Node, NodeId, Pred, Preds, Succ, Succs, RealPred, RealSucc),
    ?equals(nodelist:node(Neighborhood), Node),
    ?equals(nodelist:nodeid(Neighborhood), NodeId),
    ?equals(nodelist:pred(Neighborhood), Pred),
    ?equals(nodelist:preds(Neighborhood), Preds),
    ?equals(nodelist:succ(Neighborhood), Succ),
    ?equals(nodelist:succs(Neighborhood), Succs),
    ?equals(nodelist:has_real_pred(Neighborhood), RealPred),
    ?equals(nodelist:has_real_succ(Neighborhood), RealSucc)).

%% @doc Tests neighborhood creation with new_neighborhood and compares the
%%      getters with what is expected.
-spec tester_new(any()) -> ok.
tester_new(_Config) ->
    N8 = node:new(pid1,   8, 0),
    N14 = node:new(pid2, 14, 0),
    N15 = node:new(pid2, 15, 1), % N14 updated its ID to 15 here
    N33 = node:new(pid3, 33, 0),
    
    Neighb11 = nodelist:new_neighborhood(N8),
    ?compare_neighborhood(Neighb11, N8, 8, N8, [N8], N8, [N8], false, false),
    
    Neighb21 = nodelist:new_neighborhood(N8, N14),
    Neighb22 = nodelist:new_neighborhood(N14, N8),
    Neighb23 = nodelist:new_neighborhood(N8, N8),
    Neighb24 = nodelist:new_neighborhood(N15, N14),
    ?compare_neighborhood(Neighb21, N8, 8, N14, [N14], N14, [N14], true, true),
    ?compare_neighborhood(Neighb22, N14, 14, N8, [N8], N8, [N8], true, true),
    ?compare_neighborhood(Neighb23, N8, 8, N8, [N8], N8, [N8], false, false),
    ?compare_neighborhood(Neighb24, N15, 15, N15, [N15], N15, [N15], false, false),
    
    Neighb31 = nodelist:new_neighborhood(N14, N8,  N33),
    Neighb32 = nodelist:new_neighborhood(N33, N8,  N14),
    Neighb33 = nodelist:new_neighborhood(N8,  N14, N33),
    Neighb34 = nodelist:new_neighborhood(N33, N14, N8),
    Neighb35 = nodelist:new_neighborhood(N14, N33, N8),
    Neighb36 = nodelist:new_neighborhood(N8,  N33, N14),
    Neighb37 = nodelist:new_neighborhood(N14, N8,  N8),
    Neighb38 = nodelist:new_neighborhood(N8, N8,  N8),
    Neighb39 = nodelist:new_neighborhood(N14, N8,  N15),
    Neighb310 = nodelist:new_neighborhood(N15, N8,  N14),
    Neighb311 = nodelist:new_neighborhood(N8, N15,  N14),
    Neighb312 = nodelist:new_neighborhood(N14, N15,  N8),
    ?compare_neighborhood(Neighb31,  N8,   8, N33, [N33], N14, [N14], true, true),
    ?compare_neighborhood(Neighb32,  N8,   8, N33, [N33], N14, [N14], true, true),
    ?compare_neighborhood(Neighb33,  N14, 14, N8,  [N8],  N33, [N33], true, true),
    ?compare_neighborhood(Neighb34,  N14, 14, N8,  [N8],  N33, [N33], true, true),
    ?compare_neighborhood(Neighb35,  N33, 33, N14, [N14], N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb36,  N33, 33, N14, [N14], N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb37,  N8,   8, N14, [N14], N14, [N14], true, true),
    ?compare_neighborhood(Neighb38,  N8,   8, N8,  [N8],  N8,  [N8],  false, false),
    ?compare_neighborhood(Neighb39,  N8,   8, N15, [N15], N15, [N15], true, true),
    ?compare_neighborhood(Neighb310, N8,   8, N15, [N15], N15, [N15], true, true),
    ?compare_neighborhood(Neighb311, N15, 15, N8,  [N8],  N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb312, N15, 15, N8,  [N8],  N8,  [N8],  true, true),
    
    %TODO: check for thrown exceptions with invalid node id updates
    ok.

%% @doc Tests neighborhood creation with mk_neighborhood/2 and compares the
%%      getters with what is expected.
-spec tester_mk2(any()) -> ok.
tester_mk2(_Config) ->
    N8 = node:new(pid1,   8, 0),
    N14 = node:new(pid2, 14, 0),
    N15 = node:new(pid2, 15, 1), % N14 updated its ID to 15 here
    N33 = node:new(pid3, 33, 0),
    
    Neighb11 = nodelist:mk_neighborhood([N8], N8),
    Neighb12 = nodelist:mk_neighborhood([], N8),
    ?compare_neighborhood(Neighb11, N8, 8, N8, [N8], N8, [N8], false, false),
    ?compare_neighborhood(Neighb12, N8, 8, N8, [N8], N8, [N8], false, false),
    
    Neighb21 = nodelist:mk_neighborhood([N8, N14], N8),
    Neighb22 = nodelist:mk_neighborhood([N14, N8], N14),
    Neighb23 = nodelist:mk_neighborhood([N8, N8], N8),
    Neighb24 = nodelist:mk_neighborhood([N14], N8),
    Neighb25 = nodelist:mk_neighborhood([N8], N14),
    Neighb26 = nodelist:mk_neighborhood([N14], N15),
    ?compare_neighborhood(Neighb21, N8,   8, N14, [N14], N14, [N14], true, true),
    ?compare_neighborhood(Neighb22, N14, 14, N8,  [N8],  N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb23, N8,   8, N8,  [N8],  N8,  [N8],  false, false),
    ?compare_neighborhood(Neighb24, N8,   8, N14, [N14], N14, [N14], true, true),
    ?compare_neighborhood(Neighb25, N14, 14, N8,  [N8],  N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb26, N15, 15, N15, [N15], N15, [N15], false, false),
    
    Neighb31 = nodelist:mk_neighborhood( [N14, N8,  N33], N8),
    Neighb32 = nodelist:mk_neighborhood( [N33, N8,  N14], N8),
    Neighb33 = nodelist:mk_neighborhood( [N8,  N14, N33], N14),
    Neighb34 = nodelist:mk_neighborhood( [N33, N14, N8],  N14),
    Neighb35 = nodelist:mk_neighborhood( [N14, N33, N8],  N33),
    Neighb36 = nodelist:mk_neighborhood( [N8,  N33, N14], N33),
    Neighb37 = nodelist:mk_neighborhood( [N14, N8,  N8],  N8),
    Neighb38 = nodelist:mk_neighborhood( [N8, N8,  N8],   N8),
    Neighb39 = nodelist:mk_neighborhood( [N14, N15],      N8),
    Neighb310 = nodelist:mk_neighborhood([N15, N14],      N8),
    Neighb311 = nodelist:mk_neighborhood([N14, N8],       N15),
    Neighb312 = nodelist:mk_neighborhood([N8, N14],       N15),
    ?compare_neighborhood(Neighb31,  N8,   8, N33, [N33, N14], N14, [N14, N33], true, true),
    ?compare_neighborhood(Neighb32,  N8,   8, N33, [N33, N14], N14, [N14, N33], true, true),
    ?compare_neighborhood(Neighb33,  N14, 14, N8,  [N8, N33],  N33, [N33, N8],  true, true),
    ?compare_neighborhood(Neighb34,  N14, 14, N8,  [N8, N33],  N33, [N33, N8],  true, true),
    ?compare_neighborhood(Neighb35,  N33, 33, N14, [N14, N8],  N8,  [N8, N14],  true, true),
    ?compare_neighborhood(Neighb36,  N33, 33, N14, [N14, N8],  N8,  [N8, N14],  true, true),
    ?compare_neighborhood(Neighb37,  N8,   8, N14, [N14],      N14, [N14],      true, true),
    ?compare_neighborhood(Neighb38,  N8,   8, N8,  [N8],       N8,  [N8],       false, false),
    ?compare_neighborhood(Neighb39,  N8,   8, N15, [N15],      N15, [N15],      true, true),
    ?compare_neighborhood(Neighb310, N8,   8, N15, [N15],      N15, [N15],      true, true),
    ?compare_neighborhood(Neighb311, N15, 15, N8,  [N8],       N8,  [N8],       true, true),
    ?compare_neighborhood(Neighb312, N15, 15, N8,  [N8],       N8,  [N8],       true, true),
    
    % some tests with duplicated nodes:
    Neighb41 = nodelist:mk_neighborhood( [N14, N8, N8,  N33], N8),
    Neighb42 = nodelist:mk_neighborhood( [N33, N8,  N14, N14], N8),
    Neighb43 = nodelist:mk_neighborhood( [N8,  N14, N33, N14], N14),
    Neighb44 = nodelist:mk_neighborhood( [N33, N14, N8, N33, N14],  N14),
    Neighb45 = nodelist:mk_neighborhood( [N14, N33, N8, N14, N8],  N33),
    ?compare_neighborhood(Neighb41,  N8,   8, N33, [N33, N14], N14, [N14, N33], true, true),
    ?compare_neighborhood(Neighb42,  N8,   8, N33, [N33, N14], N14, [N14, N33], true, true),
    ?compare_neighborhood(Neighb43,  N14, 14, N8,  [N8, N33],  N33, [N33, N8],  true, true),
    ?compare_neighborhood(Neighb44,  N14, 14, N8,  [N8, N33],  N33, [N33, N8],  true, true),
    ?compare_neighborhood(Neighb45,  N33, 33, N14, [N14, N8],  N8,  [N8, N14],  true, true),
    
    %TODO: check for thrown exceptions with invalid node id updates
    %TODO: add tests with longer lists
    ok.

%% @doc Tests neighborhood creation with mk_neighborhood/4 and compares the
%%      getters with what is expected.
-spec tester_mk4(any()) -> ok.
tester_mk4(_Config) ->
    N8 = node:new(pid1,   8, 0),
    N14 = node:new(pid2, 14, 0),
    N15 = node:new(pid2, 15, 1), % N14 updated its ID to 15 here
    N33 = node:new(pid3, 33, 0),
    
    Neighb11 = nodelist:mk_neighborhood([N8], N8, 1, 1),
    Neighb12 = nodelist:mk_neighborhood([], N8, 1, 1),
    ?compare_neighborhood(Neighb11, N8, 8, N8, [N8], N8, [N8], false, false),
    ?compare_neighborhood(Neighb12, N8, 8, N8, [N8], N8, [N8], false, false),
    
    Neighb21 = nodelist:mk_neighborhood([N8, N14], N8, 1, 1),
    Neighb22 = nodelist:mk_neighborhood([N14, N8], N14, 1, 1),
    Neighb23 = nodelist:mk_neighborhood([N8, N8], N8, 1, 1),
    Neighb24 = nodelist:mk_neighborhood([N14], N8, 1, 1),
    Neighb25 = nodelist:mk_neighborhood([N8], N14, 1, 1),
    Neighb26 = nodelist:mk_neighborhood([N14], N15, 1, 1),
    ?compare_neighborhood(Neighb21, N8,   8, N14, [N14], N14, [N14], true, true),
    ?compare_neighborhood(Neighb22, N14, 14, N8,  [N8],  N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb23, N8,   8, N8,  [N8],  N8,  [N8],  false, false),
    ?compare_neighborhood(Neighb24, N8,   8, N14, [N14], N14, [N14], true, true),
    ?compare_neighborhood(Neighb25, N14, 14, N8,  [N8],  N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb26, N15, 15, N15, [N15], N15, [N15], false, false),
    
    Neighb31 = nodelist:mk_neighborhood( [N14, N8,  N33], N8, 1, 1),
    Neighb32 = nodelist:mk_neighborhood( [N33, N8,  N14], N8, 1, 1),
    Neighb33 = nodelist:mk_neighborhood( [N8,  N14, N33], N14, 1, 1),
    Neighb34 = nodelist:mk_neighborhood( [N33, N14, N8],  N14, 1, 1),
    Neighb35 = nodelist:mk_neighborhood( [N14, N33, N8],  N33, 1, 1),
    Neighb36 = nodelist:mk_neighborhood( [N8,  N33, N14], N33, 1, 1),
    Neighb37 = nodelist:mk_neighborhood( [N14, N8,  N8],  N8, 1, 1),
    Neighb38 = nodelist:mk_neighborhood( [N8, N8,  N8],   N8, 1, 1),
    Neighb39 = nodelist:mk_neighborhood( [N14, N15],      N8, 1, 1),
    Neighb310 = nodelist:mk_neighborhood([N15, N14],      N8, 1, 1),
    Neighb311 = nodelist:mk_neighborhood([N14, N8],       N15, 1, 1),
    Neighb312 = nodelist:mk_neighborhood([N8, N14],       N15, 1, 1),
    ?compare_neighborhood(Neighb31,  N8,   8, N33, [N33], N14, [N14], true, true),
    ?compare_neighborhood(Neighb32,  N8,   8, N33, [N33], N14, [N14], true, true),
    ?compare_neighborhood(Neighb33,  N14, 14, N8,  [N8],  N33, [N33], true, true),
    ?compare_neighborhood(Neighb34,  N14, 14, N8,  [N8],  N33, [N33], true, true),
    ?compare_neighborhood(Neighb35,  N33, 33, N14, [N14], N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb36,  N33, 33, N14, [N14], N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb37,  N8,   8, N14, [N14], N14, [N14], true, true),
    ?compare_neighborhood(Neighb38,  N8,   8, N8,  [N8],  N8,  [N8],  false, false),
    ?compare_neighborhood(Neighb39,  N8,   8, N15, [N15], N15, [N15], true, true),
    ?compare_neighborhood(Neighb310, N8,   8, N15, [N15], N15, [N15], true, true),
    ?compare_neighborhood(Neighb311, N15, 15, N8,  [N8],  N8,  [N8],  true, true),
    ?compare_neighborhood(Neighb312, N15, 15, N8,  [N8],  N8,  [N8],  true, true),
    
    % some tests with duplicated nodes:
    Neighb41 = nodelist:mk_neighborhood( [N14, N8, N8,  N33], N8, 1, 1),
    Neighb42 = nodelist:mk_neighborhood( [N33, N8,  N14, N14], N8, 1, 1),
    Neighb43 = nodelist:mk_neighborhood( [N8,  N14, N33, N14], N14, 1, 1),
    Neighb44 = nodelist:mk_neighborhood( [N33, N14, N8, N33, N14],  N14, 1, 1),
    Neighb45 = nodelist:mk_neighborhood( [N14, N33, N8, N14, N8],  N33, 1, 1),
    ?compare_neighborhood(Neighb41,  N8,   8, N33, [N33], N14, [N14], true, true),
    ?compare_neighborhood(Neighb42,  N8,   8, N33, [N33], N14, [N14], true, true),
    ?compare_neighborhood(Neighb43,  N14, 14, N8,  [N8],  N33, [N33], true, true),
    ?compare_neighborhood(Neighb44,  N14, 14, N8,  [N8],  N33, [N33], true, true),
    ?compare_neighborhood(Neighb45,  N33, 33, N14, [N14], N8,  [N8],  true, true),
    
    %TODO: check for thrown exceptions with invalid node id updates
    %TODO: add tests with longer lists
    ok.

%% @doc Tests truncating neighborhood structures.
-spec tester_trunc(any()) -> ok.
tester_trunc(_Config) ->
    N8 = node:new(pid1,   8, 0),
    N14 = node:new(pid2, 14, 0),
    N20 = node:new(pid3, 20, 0),
    N33 = node:new(pid4, 33, 0),
    N50 = node:new(pid5, 50, 0),
    
    Neighb1 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20),
    
    Trunc11 = nodelist:trunc(Neighb1, 1, 1),
    Trunc12 = nodelist:trunc(Neighb1, 2, 1),
    Trunc13 = nodelist:trunc(Neighb1, 3, 1),
    Trunc14 = nodelist:trunc(Neighb1, 4, 1),
    Trunc15 = nodelist:trunc(Neighb1, 5, 1),
    Trunc16 = nodelist:trunc(Neighb1, 1, 2),
    Trunc17 = nodelist:trunc(Neighb1, 1, 3),
    Trunc18 = nodelist:trunc(Neighb1, 1, 4),
    Trunc19 = nodelist:trunc(Neighb1, 1, 5),
    Trunc110 = nodelist:trunc(Neighb1, 2, 2),
    Trunc111 = nodelist:trunc(Neighb1, 2, 3),
    Trunc112 = nodelist:trunc(Neighb1, 3, 2),
    Trunc113 = nodelist:trunc(Neighb1, 3, 3),
    Trunc114 = nodelist:trunc(Neighb1, 4, 4),
    Trunc115 = nodelist:trunc(Neighb1, 5, 5),
    ?compare_neighborhood(Trunc11, N20, 20, N14, [N14], N33, [N33], true, true),
    ?compare_neighborhood(Trunc12, N20, 20, N14, [N14, N8], N33, [N33], true, true),
    ?compare_neighborhood(Trunc13, N20, 20, N14, [N14, N8, N50], N33, [N33], true, true),
    ?compare_neighborhood(Trunc14, N20, 20, N14, [N14, N8, N50, N33], N33, [N33], true, true),
    ?compare_neighborhood(Trunc15, N20, 20, N14, [N14, N8, N50, N33], N33, [N33], true, true),
    ?compare_neighborhood(Trunc16, N20, 20, N14, [N14], N33, [N33, N50], true, true),
    ?compare_neighborhood(Trunc17, N20, 20, N14, [N14], N33, [N33, N50, N8], true, true),
    ?compare_neighborhood(Trunc18, N20, 20, N14, [N14], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc19, N20, 20, N14, [N14], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc110, N20, 20, N14, [N14, N8], N33, [N33, N50], true, true),
    ?compare_neighborhood(Trunc111, N20, 20, N14, [N14, N8], N33, [N33, N50, N8], true, true),
    ?compare_neighborhood(Trunc112, N20, 20, N14, [N14, N8, N50], N33, [N33, N50], true, true),
    ?compare_neighborhood(Trunc113, N20, 20, N14, [N14, N8, N50], N33, [N33, N50, N8], true, true),
    ?compare_neighborhood(Trunc114, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc115, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50, N8, N14], true, true),
    
    Trunc21 = nodelist:trunc_preds(Neighb1, 1),
    Trunc22 = nodelist:trunc_preds(Neighb1, 2),
    Trunc23 = nodelist:trunc_preds(Neighb1, 3),
    Trunc24 = nodelist:trunc_preds(Neighb1, 4),
    Trunc25 = nodelist:trunc_preds(Neighb1, 5),
    ?compare_neighborhood(Trunc21, N20, 20, N14, [N14], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc22, N20, 20, N14, [N14, N8], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc23, N20, 20, N14, [N14, N8, N50], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc24, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc25, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50, N8, N14], true, true),
    
    Trunc31 = nodelist:trunc_succs(Neighb1, 1),
    Trunc32 = nodelist:trunc_succs(Neighb1, 2),
    Trunc33 = nodelist:trunc_succs(Neighb1, 3),
    Trunc34 = nodelist:trunc_succs(Neighb1, 4),
    Trunc35 = nodelist:trunc_succs(Neighb1, 5),
    ?compare_neighborhood(Trunc31, N20, 20, N14, [N14, N8, N50, N33], N33, [N33], true, true),
    ?compare_neighborhood(Trunc32, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50], true, true),
    ?compare_neighborhood(Trunc33, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50, N8], true, true),
    ?compare_neighborhood(Trunc34, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50, N8, N14], true, true),
    ?compare_neighborhood(Trunc35, N20, 20, N14, [N14, N8, N50, N33], N33, [N33, N50, N8, N14], true, true),
    
    ok.

%TODO: add test for remove

%TODO: add test for merge
merge(_Config) ->
    N8 = node:new(pid,   8, 0),
    N14 = node:new(pid, 14, 0),
    N33 = node:new(pid, 33, 0),
    %TODO: implement test
%%     ?equals(rm_chord:merge([N8, N14, N8], [N8, N14, N8], 26), [N8, N14]),
%%     ?equals(rm_chord:merge([N8, N14, N33], [N8, N14, N33], 26), [N33, N8, N14]),
    
    %TODO: test whether a node appears in its own predecessor/successor list when merged with such a list
    % debug output from nodelist:merge:
%%     io:format("~nmerge "),
%%     case lists:any(fun(N) -> node:equals(N, Node1) end, Neighbors1View) of
%%         true -> io:format("merge_Neighbors1View~p ", [Neighbors1View]);
%%         false -> ok
%%     end,
%%     case lists:any(fun(N) -> node:equals(N, Node1) end, Neighbors2View) of
%%         true -> io:format("merge_Neighbors2View~p~n~p~n~p~n~p ", [Neighbors2View, to_list(Neighbors2), get_last_and_remove(Neighbors2View, Node1, []), get_last_and_remove(to_list(Neighbors2), Node1, [])]);
%%         false -> ok
%%     end,
    ok.

%TODO: add test for add_nodes
    %TODO: test whether a node appears in its own predecessor/successor list when such a list is added
    % debug output from nodelist:add_nodes:
%%     io:format("~nadd "),
%%     case lists:any(fun(N) -> node:equals(N, Node) end, NeighborsView) of
%%         true -> io:format("add_NeighborsView ");
%%         false -> ok
%%     end,
%%     case lists:any(fun(N) -> node:equals(N, Node) end, OtherView) of
%%         true -> io:format("add_OtherView ");
%%         false -> ok
%%     end,

%% @doc Tests converting neighborhood structures to node lists.
-spec tester_to_list(any()) -> ok.
tester_to_list(_Config) ->
    N8 = node:new(pid1,   8, 0),
    N14 = node:new(pid2, 14, 0),
    N20 = node:new(pid3, 20, 0),
    N33 = node:new(pid4, 33, 0),
    N50 = node:new(pid5, 50, 0),
    
    Neighb1 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20),
    Neighb2 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 3, 3),
    Neighb3 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 2, 2),
    Neighb4 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 1, 1),
    Neighb5 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 3, 1),
    Neighb6 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 3, 2),
    Neighb7 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 1, 3),
    Neighb8 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 2, 3),
    Neighb9 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 2, 1),
    Neighb10 = nodelist:mk_neighborhood([N8, N14, N33, N50], N20, 1, 2),
    
    ?equals(nodelist:to_list(Neighb1), [N20, N33, N50, N8, N14]),
    ?equals(nodelist:to_list(Neighb2), [N20, N33, N50, N8, N14]),
    ?equals(nodelist:to_list(Neighb3), [N20, N33, N50, N8, N14]),
    ?equals(nodelist:to_list(Neighb4), [N20, N33, N14]),
    ?equals(nodelist:to_list(Neighb5), [N20, N33, N50, N8, N14]),
    ?equals(nodelist:to_list(Neighb6), [N20, N33, N50, N8, N14]),
    ?equals(nodelist:to_list(Neighb7), [N20, N33, N50, N8, N14]),
    ?equals(nodelist:to_list(Neighb8), [N20, N33, N50, N8, N14]),
    ?equals(nodelist:to_list(Neighb9), [N20, N33, N8, N14]),
    ?equals(nodelist:to_list(Neighb10), [N20, N33, N50, N14]),
    
    ok.

%% @doc Tests updating node IDs in two node lists.
-spec tester_update_ids(any()) -> ok.
tester_update_ids(_Config) ->
    N8 = node:new(pid1,   8, 0),
    N9 = node:new(pid1,   9, 1), %N8 updated
    N14 = node:new(pid2, 14, 0),
    N20 = node:new(pid3, 20, 0),
    N21 = node:new(pid3, 21, 1), %N20 updated
    N33 = node:new(pid4, 33, 0),
    N50 = node:new(pid5, 50, 0),

    ?equals(nodelist:update_ids([N8, N14, N20, N33, N50], []), {[N8, N14, N20, N33, N50], []}),
    ?equals(nodelist:update_ids([N8, N9, N14, N20, N33, N50], []), {[N9, N9, N14, N20, N33, N50], []}),
    ?equals(nodelist:update_ids([N8, N9, N14, N20, N21, N33, N50], []), {[N9, N9, N14, N21, N21, N33, N50], []}),
    ?equals(nodelist:update_ids([N8, N14, N20, N33], [N50]), {[N8, N14, N20, N33], [N50]}),
    ?equals(nodelist:update_ids([N8, N14, N20], [N33, N50]), {[N8, N14, N20], [N33, N50]}),
    ?equals(nodelist:update_ids([N8, N14], [N20, N33, N50]), {[N8, N14], [N20, N33, N50]}),
    ?equals(nodelist:update_ids([N8], [N14, N20, N33, N50]), {[N8], [N14, N20, N33, N50]}),
    ?equals(nodelist:update_ids([], [N8, N14, N20, N33, N50]), {[], [N8, N14, N20, N33, N50]}),
    ?equals(nodelist:update_ids([], [N8, N9, N14, N20, N33, N50]), {[], [N9, N9, N14, N20, N33, N50]}),
    ?equals(nodelist:update_ids([], [N8, N9, N14, N20, N21, N33, N50]), {[], [N9, N9, N14, N21, N21, N33, N50]}),
    
    ?equals(nodelist:update_ids([N8, N14, N20, N33, N50], [N9]), {[N9, N14, N20, N33, N50], [N9]}),
    ?equals(nodelist:update_ids([N8, N14, N20, N33, N50], [N8]), {[N8, N14, N20, N33, N50], [N8]}),
    ?equals(nodelist:update_ids([N8, N14, N20, N33, N50], [N9, N33]), {[N9, N14, N20, N33, N50], [N9, N33]}),
    ?equals(nodelist:update_ids([N8, N14, N20, N33, N50], [N8, N33]), {[N8, N14, N20, N33, N50], [N8, N33]}),
    ?equals(nodelist:update_ids([N8, N14, N20, N33, N50], [N8, N14, N20, N33, N50]), {[N8, N14, N20, N33, N50], [N8, N14, N20, N33, N50]}),
    ?equals(nodelist:update_ids([N8, N14, N21, N33, N50], [N9, N14, N20, N33, N50]), {[N9, N14, N21, N33, N50], [N9, N14, N21, N33, N50]}),
    ?equals(nodelist:update_ids([N14, N21, N33, N50], [N9, N14, N20, N33]), {[N14, N21, N33, N50], [N9, N14, N21, N33]}),
    ?equals(nodelist:update_ids([N14, N21, N33, N50], [N9, N14, N20, N33]), {[N14, N21, N33, N50], [N9, N14, N21, N33]}),
    
    ?equals(nodelist:update_ids([N14, N20, N33, N50], [N9, N14, N20, N21, N33]), {[N14, N21, N33, N50], [N9, N14, N21, N21, N33]}),
    
    % some tests with "unsorted" lists:
    ?equals(nodelist:update_ids([N50, N14, N21, N8, N33], [N33, N50, N9, N14, N20]), {[N50, N14, N21, N9, N33], [N33, N50, N9, N14, N21]}),
    ?equals(nodelist:update_ids([N14, N33, N50, N21], [N9, N14, N20, N33]), {[N14, N33, N50, N21], [N9, N14, N21, N33]}),
    ?equals(nodelist:update_ids([N14, N21, N33, N50], [N20, N9, N14, N33]), {[N14, N21, N33, N50], [N21, N9, N14, N33]}),
    ?equals(nodelist:update_ids([N33, N14, N20, N50], [N9, N14, N20, N21, N33]), {[N33, N14, N21, N50], [N9, N14, N21, N21, N33]}),
    ok.

%% @doc Tests making removing outdated nodes from a node list.
-spec tester_remove_outdated(any()) -> ok.
tester_remove_outdated(_Config) ->
    Null = node:null(),
    N8 = node:new(pid1,   8, 0),
    N9 = node:new(pid1,   9, 1), %N8 updated
    N14 = node:new(pid2, 14, 0),
    N20 = node:new(pid3, 20, 0),
    N21 = node:new(pid3, 21, 1), %N20 updated
    N33 = node:new(pid4, 33, 0),
    N50 = node:new(pid5, 50, 0),

    ?equals(nodelist:remove_outdated([]), []),
    ?equals(nodelist:remove_outdated([Null]), []),
    ?equals(nodelist:remove_outdated([N8]), [N8]),
    ?equals(nodelist:remove_outdated([N8, Null]), [N8]),
    ?equals(nodelist:remove_outdated([Null, N8]), [N8]),
    ?equals(nodelist:remove_outdated([N8, N14, N20, N33, N50]), [N8, N14, N20, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N14, N20, N33, N50, Null]), [N8, N14, N20, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N14, N20, Null, N33, N50]), [N8, N14, N20, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N9]), [N9]),
    ?equals(nodelist:remove_outdated([Null, N8, N9]), [N9]),
    ?equals(nodelist:remove_outdated([N8, Null, N9]), [N9]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N33, N50]), [N9, N14, N20, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N21, N33, N50]), [N9, N14, N21, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N8, N21, N33, N50]), [N9, N14, N21, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N21, N33, N50, N50]), [N9, N14, N21, N33, N50, N50]),

    ?equals(nodelist:remove_outdated([], N50), []),
    ?equals(nodelist:remove_outdated([Null], N50), []),
    ?equals(nodelist:remove_outdated([N8], N50), [N8]),
    ?equals(nodelist:remove_outdated([N8, Null], N50), [N8]),
    ?equals(nodelist:remove_outdated([N8, N14, N20, N33], N50), [N8, N14, N20, N33]),
    ?equals(nodelist:remove_outdated([N8, N14, N20, N33, N50], N50), [N8, N14, N20, N33]),
    ?equals(nodelist:remove_outdated([N8, N14, N20, N33, N50, Null], N50), [N8, N14, N20, N33]),
    ?equals(nodelist:remove_outdated([N8, N14, N20, Null, N33, N50], N50), [N8, N14, N20, N33]),
    ?equals(nodelist:remove_outdated([N8, N9], N50), [N9]),
    ?equals(nodelist:remove_outdated([Null, N8, N9], N50), [N9]),
    ?equals(nodelist:remove_outdated([N8, Null, N9], N50), [N9]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N33, N50], N50), [N9, N14, N20, N33]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N21, N33, N50], N50), [N9, N14, N21, N33]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N8, N21, N33, N50], N50), [N9, N14, N21, N33]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N21, N33, N33, N50, N50], N50), [N9, N14, N21, N33, N33]),
    
    % some tests with "unsorted" lists:
    ?equals(nodelist:remove_outdated([N14, N8, N20, N33, N50]), [N14, N8, N20, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N20, N33, N14, N50, Null]), [N8, N20, N33, N14, N50]),
    ?equals(nodelist:remove_outdated([N8, N20, N33, N14, Null, N50]), [N8, N20, N33, N14, N50]),
    ?equals(nodelist:remove_outdated([N9, N8]), [N9]),
    ?equals(nodelist:remove_outdated([Null, N9, N8]), [N9]),
    ?equals(nodelist:remove_outdated([N9, Null, N8]), [N9]),
    ?equals(nodelist:remove_outdated([N8, N20, N33, N50, N9, N14]), [N20, N33, N50, N9, N14]),
    ?equals(nodelist:remove_outdated([N9, N14, N20, N8, N21, N33, N50]), [N9, N14, N21, N33, N50]),
    ?equals(nodelist:remove_outdated([N8, N9, N14, N20, N8, N21, N33, N50]), [N9, N14, N21, N33, N50]),
    ?equals(nodelist:remove_outdated([N9, N50, N14, N20, N8, N21, N33, N50]), [N9, N50, N14, N21, N33, N50]),
    
    ?equals(nodelist:remove_outdated([N14, N8, N20, N33, N50], N50), [N14, N8, N20, N33]),
    ?equals(nodelist:remove_outdated([N8, N20, N33, N14, N50, Null], N50), [N8, N20, N33, N14]),
    ?equals(nodelist:remove_outdated([N8, N20, N33, N14, Null, N50], N50), [N8, N20, N33, N14]),
    ?equals(nodelist:remove_outdated([N9, N8], N50), [N9]),
    ?equals(nodelist:remove_outdated([Null, N9, N8], N50), [N9]),
    ?equals(nodelist:remove_outdated([N9, Null, N8], N50), [N9]),
    ?equals(nodelist:remove_outdated([N8, N20, N33, N50, N9, N14], N50), [N20, N33, N9, N14]),
    ?equals(nodelist:remove_outdated([N9, N14, N20, N8, N21, N33, N50], N50), [N9, N14, N21, N33]),
    ?equals(nodelist:remove_outdated([N9, N14, N20, N8, N21, N33], N50), [N9, N14, N21, N33]),
    ?equals(nodelist:remove_outdated([N9, N14, N20, N8, N21, N8, N33], N50), [N9, N14, N21, N33]),
    ?equals(nodelist:remove_outdated([N9, N14, N20, N8, N21, N33], N50), [N9, N14, N21, N33]),
    ok.
