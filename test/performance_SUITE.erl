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
%%% File    : performance_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Performance Tests
%%%
%%% Created :  15 Dec 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(performance_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [empty,
     get_keys_for_replica_string,
     get_keys_for_replica_int,
     md5,
     next_hop].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    Config.

end_per_suite(_Config) ->
    ok.

count() ->
    1000000.

empty(_Config) ->
    iter(count(), fun () ->
                       ok
               end, "empty"),
    ok.

get_keys_for_replica_string(_Config) ->
    iter(count(), fun () ->
                          rt_chord:get_keys_for_replicas("42")
               end, "get_keys_for_replica_string"),
    ok.

md5(_Config) ->
    iter(count(), fun () ->
                          crypto:md5("42")
               end, "md5"),
    ok.

get_keys_for_replica_int(_Config) ->
    iter(count(), fun () ->
                          rt_chord:get_keys_for_replicas(42)
               end, "get_keys_for_replica_int"),
    ok.

next_hop(_Config) ->
    RT = gb_trees:enter(1, succ,
          gb_trees:enter(2, pred,
           gb_trees:enter(3, succ,
            gb_trees:enter(4, pred,
             gb_trees:enter(100, succ,
              gb_trees:enter(101, pred,
               gb_trees:enter(102, succ,
                gb_trees:enter(103, pred,
                 rt_chord:empty(succ))))))))),
    State = cs_state:new(RT, node:make(succ, 3), node:make(pred, 1), node:make(me, 2), my_range, lb, db),
    iter(count(), fun () ->
                          rt_chord:next_hop(State, 42)
               end, "next_hop"),
    ok.

iter(Count, F, Tag) ->
    Start = erlang:now(),
    iter_inner(Count, F),
    Stop = erlang:now(),
    ct:pal("~p iterations of ~p took ~ps: ~p1/s", [Count, Tag, timer:now_diff(Stop, Start) / 1000000.0, 1000000.0 / (timer:now_diff(Stop, Start) / Count)]),
    ok.

iter_inner(0, _) ->
    ok;
iter_inner(N, F) ->
    F(),
    iter_inner(N - 1, F).
