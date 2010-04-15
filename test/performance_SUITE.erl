%  Copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
     get_keys_for_replica_int,
     get_keys_for_replica_string,
     md5,
     next_hop,
     process_dictionary_lookup,
     process_dictionary_lookup_by_pid,
     ets_insert,
     ets_lookup,
     erlang_put,
     erlang_get,
     pdb_set,
     pdb_get,
     erlang_send,
     cs_send_local,
    erlang_send_after,
    erlang_spawn,
    erlang_now].

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

empty(_Config) ->
    iter(count(), fun () ->
                       ok
               end, "empty"),
    ok.

ets_lookup(_Config) ->
    ets:new(performance, [ordered_set, private, named_table]),
    ets:insert(performance, {123456, "foo"}),
    iter(count(), fun() ->
                          ets:lookup(performance, 123456)
                              end, "ets:lookup"),
    ok.

ets_insert(_Config) ->
    ets:new(ets_insert, [ordered_set, private, named_table]),
    iter(count(), fun() ->
                          ets:insert(ets_insert, {performance, "abc"})
                              end, "ets:insert"),
    ok.

erlang_get(_Config) ->
    erlang:put(performance, "foo"),
    iter(count(), fun() ->
                          erlang:get(performance)
                              end, "erlang:get"),
    ok.

erlang_put(_Config) ->
    iter(count(), fun() ->
                          erlang:put(performance, "abc")
                              end, "erlang:put"),
    ok.

pdb_get(_Config) ->
    pdb:new(pdb_get, [ordered_set, private, named_table]),
    pdb:set({performance, "foo"}, pdb_get),
    iter(count(), fun() ->
                          pdb:get(performance, pdb_get)
                  end, "pdb:get"),
    ok.

pdb_set(_Config) ->
    pdb:new(pdb_set, [ordered_set, private, named_table]),
    iter(count(), fun() ->
                          pdb:set({performance, "abc"}, pdb_set)
                  end, "pdb:set"),
    ok.

erlang_send(_Config) ->
    Pid = spawn(?MODULE, helper_rec, [count(), self()]),
    iter(count(), fun() -> Pid ! ping end, "erlang:send"),
    receive pong -> ok end,
    ok.

cs_send_local(_Config) ->
    Pid = spawn(?MODULE, helper_rec, [count(), self()]),
    iter(count(), fun() -> cs_send:send_local(Pid, ping) end, "cs_send_local"),
    receive pong -> ok end,
    ok.

helper_rec(0, Pid) -> Pid ! pong;
helper_rec(Iter, Pid) ->
    receive _Any -> ok end,
    helper_rec(Iter - 1, Pid).

erlang_send_after(_Config) ->
    Pid = spawn(?MODULE, helper_rec, [count(), self()]),
    iter(count(), fun() -> cs_send:send_local_after(5000, Pid, ping) end, "cs_send:send_after"),
    receive pong -> ok end,
    ok.

erlang_spawn(_Config) ->
    iter(count(), fun() -> spawn(fun() -> ok end) end, "erlang:spawn"),
    ok.

erlang_now(_Config) ->
    iter(count(), fun() -> erlang:now() end, "erlang:now"),
    ok.


get_keys_for_replica_string(_Config) ->
    iter(count(), fun () ->
                          rt_chord:get_keys_for_replicas("42")
               end, "get_keys_for_replica_string"),
    ok.

get_keys_for_replica_int(_Config) ->
      iter(count(), fun () ->
                            rt_chord:get_keys_for_replicas(42)
                 end, "get_keys_for_replica_int"),
      ok.

md5(_Config) ->
    iter(count(), fun () ->
                          crypto:md5("42")
               end, "crypto:md5"),
    iter(count(), fun () ->
                          erlang:md5("42")
               end, "erlang:md5"),
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
    State = dht_node_state:new(RT, node:new(succ, 3), node:new(pred, 1),
                         node:new(me, 2), my_range, lb, db),
    iter(count(), fun () ->
                          rt_chord:next_hop(State, 42)
               end, "next_hop"),
    ok.

process_dictionary_lookup(_Config) ->
    {ok, _Pid} = process_dictionary:start_link(),
    process_dictionary:register_process(?MODULE, "process_dictionary", self()),
    iter(count(), fun () ->
                          process_dictionary:lookup_process(?MODULE,
                                                            "process_dictionary")
                  end, "lookup_process by instance_id"),
    gen_component:kill(process_dictionary),
    unregister(process_dictionary),
    ok.

process_dictionary_lookup_by_pid(_Config) ->
    {ok, _Pid} = process_dictionary:start_link(),
    process_dictionary:register_process(?MODULE, "process_dictionary", self()),
    iter(count(), fun () ->
                          process_dictionary:lookup_process(process_dictionary)
                  end, "lookup_process by pid"),
    gen_component:kill(process_dictionary),
    unregister(process_dictionary),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
iter(Count, F, Tag) ->
    F(),
    Start = erlang:now(),
    iter_inner(Count, F),
    Stop = erlang:now(),
    ElapsedTime = timer:now_diff(Stop, Start) / 1000000.0,
    Frequency = 1000000.0 / (timer:now_diff(Stop, Start) / Count),
    ct:pal("~p iterations of ~p took ~ps: ~p1/s", [Count, Tag,
                                                   ElapsedTime,
                                                   Frequency]),
    ok.

iter_inner(0, _) ->
    ok;
iter_inner(N, F) ->
    F(),
    iter_inner(N - 1, F).
