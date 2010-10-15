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
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [empty,
     get_keys_for_replica_int,
     get_keys_for_replica_string,
     md5,
     {group, with_config},
     pid_groups_lookup,
     pid_groups_lookup_by_pid,
     ets_ordset_insert1,
     ets_ordset_insert2,
     ets_ordset_lookup1,
     ets_ordset_lookup2,
     erlang_put,
     erlang_get,
     pdb_set,
     pdb_get,
%%      ordsets_add_element,
%%      sets_add_element,
%%      gb_sets_add_element,
     ets_set_insert1N,
     ets_set_insert2N,
     ets_ordset_insert1N,
     ets_ordset_insert2N,
     erlang_send,
     comm_local,
     erlang_send_after,
     erlang_spawn,
     erlang_now].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

groups() ->
    [{with_config, [sequence], [next_hop_no_neighbors, next_hop_with_neighbors]}].

init_per_group(GroupName, Config) ->
    case GroupName of
        with_config ->
            Pid = spawn_config_processes(),
            [{config_pid, Pid} | Config];
        _ -> Config
    end.

end_per_group(GroupName, Config) ->
    case GroupName of
        with_config ->
            case lists:keyfind(config_pid, 1, Config) of
                false -> ok;
                {config_pid, Pid} -> stop_config_processes(Pid)
            end;
        _ -> ok
    end.

-spec spawn_config_processes() -> pid().
spawn_config_processes() ->
    unittest_helper:fix_cwd(),
    unittest_helper:start_process(
      fun() ->
%%               crypto:start(),
              pid_groups:start_link(),
              config:start_link(["scalaris.cfg", "scalaris.local.cfg"]),
              log:start_link()
      end).

-spec stop_config_processes(pid()) -> ok.
stop_config_processes(Pid) ->
    error_logger:tty(false),
    log:set_log_level(none),
    exit(Pid, kill),
    unittest_helper:stop_pid_groups(),
    ok.

init_per_suite(Config) ->
    ct:pal("Starting unittest ~p", [ct:get_status()]),
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

ets_ordset_lookup1(_Config) ->
    ets:new(ets_ordset_lookup1, [ordered_set, private, named_table]),
    ets:insert(ets_ordset_lookup1, {123456, "foo"}),
    iter(count(), fun() ->
                          ets:lookup(ets_ordset_lookup1, 123456)
                  end, "ets(ordered_set):lookup"),
    ets:delete(ets_ordset_lookup1),
    ok.

ets_ordset_lookup2(_Config) ->
    Table = ets:new(ets_ordset_lookup2, [ordered_set, private]),
    ets:insert(Table, {123456, "foo"}),
    iter(count(), fun() ->
                          ets:lookup(Table, 123456)
                  end, "ets(ordered_set_unnamed):lookup"),
    ets:delete(Table),
    ok.

ets_ordset_insert1(_Config) ->
    ets:new(ets_ordset_insert1, [ordered_set, private, named_table]),
    iter(count(), fun() ->
                          ets:insert(ets_ordset_insert1, {performance, "abc"})
                  end, "ets(ordered_set):insert"),
    ets:delete(ets_ordset_insert1),
    ok.

ets_ordset_insert2(_Config) ->
    Table = ets:new(ets_ordset_insert2, [ordered_set, private, named_table]),
    iter(count(), fun() ->
                          ets:insert(Table, {performance, "abc"})
                  end, "ets(ordered_set_unnamed):insert"),
    ets:delete(Table),
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

% weigh too slow - can not execute the default number of test runs, i.e. 1.000.000
ordsets_add_element(_Config) ->
    Set = ordsets:new(),
    Set2 = iter2_foldl(10000, fun ordsets:add_element/2, Set, "ordsets:add_element (1)"),
    _Set3 = iter2_foldl(10000, fun ordsets:add_element/2, Set2, "ordsets:add_element (2)"),
    ok.

% slow, too - do not call by default
sets_add_element(_Config) ->
    Set = sets:new(),
    Set2 = iter2_foldl(100000, fun sets:add_element/2, Set, "sets:add_element (1)"),
    _Set3 = iter2_foldl(100000, fun sets:add_element/2, Set2, "sets:add_element (2)"),
    ok.

% slow, too - do not call by default
gb_sets_add_element(_Config) ->
    Set = gb_sets:new(),
    Set2 = iter2_foldl(count(), fun gb_sets:add_element/2, Set, "gb_sets:add_element (1)"),
    _Set3 = iter2_foldl(count(), fun gb_sets:add_element/2, Set2, "gb_sets:add_element (2)"),
    ok.

ets_set_insert1N(_Config) ->
    ets:new(ets_set_insert1N, [set, private, named_table]),
    iter2(count(), fun(N) ->
                           ets:insert(ets_set_insert1N, {N})
                   end, "ets(set):insert (1N)"),
    iter2(count(), fun(N) ->
                           ets:insert(ets_set_insert1N, {N})
                   end, "ets(set):insert (2N)"),
    ets:delete(ets_set_insert1N),
    ok.

ets_set_insert2N(_Config) ->
    Table = ets:new(ets_set_insert2N, [set, private]),
    iter2(count(), fun(N) ->
                           ets:insert(Table, {N})
                   end, "ets(set_unnamed):insert (1N)"),
    iter2(count(), fun(N) ->
                           ets:insert(Table, {N})
                   end, "ets(set_unnamed):insert (2N)"),
    ets:delete(Table),
    ok.

ets_ordset_insert1N(_Config) ->
    ets:new(ets_ordset_insert1N, [ordered_set, private, named_table]),
    iter2(count(), fun(N) ->
                           ets:insert(ets_ordset_insert1N, {N})
                   end, "ets(ordered_set):insert (1N)"),
    iter2(count(), fun(N) ->
                           ets:insert(ets_ordset_insert1N, {N})
                   end, "ets(ordered_set):insert (2N)"),
    ets:delete(ets_ordset_insert1N),
    ok.

ets_ordset_insert2N(_Config) ->
    Table = ets:new(ets_set_insert2N, [ordered_set, private]),
    iter2(count(), fun(N) ->
                           ets:insert(Table, {N})
                   end, "ets(ordered_set_unnamed):insert (1N)"),
    iter2(count(), fun(N) ->
                           ets:insert(Table, {N})
                   end, "ets(ordered_set_unnamed):insert (2N)"),
    ets:delete(Table),
    ok.

erlang_send(_Config) ->
    Pid = spawn(?MODULE, helper_rec, [count(), self()]),
    iter(count(), fun() -> Pid ! {ping} end, "erlang:send"),
    receive {pong} -> ok end,
    ok.

comm_local(_Config) ->
    Pid = spawn(?MODULE, helper_rec, [count(), self()]),
    iter(count(), fun() -> comm:send_local(Pid, {ping}) end, "comm_local"),
    receive {pong} -> ok end,
    ok.

helper_rec(0, Pid) -> Pid ! {pong};
helper_rec(Iter, Pid) ->
    receive _Any -> ok end,
    helper_rec(Iter - 1, Pid).

erlang_send_after(_Config) ->
    Pid = spawn(?MODULE, helper_rec, [count(), self()]),
    iter(count(), fun() -> comm:send_local_after(5000, Pid, {ping}) end, "comm:send_after"),
    receive {pong} -> ok end,
    ok.

erlang_spawn(_Config) ->
    iter(count(), fun() -> spawn(fun() -> ok end) end, "erlang:spawn"),
    ok.

erlang_now(_Config) ->
    iter(count(), fun() -> erlang:now() end, "erlang:now"),
    ok.


get_keys_for_replica_string(_Config) ->
    iter(count(), fun () ->
                          rt_chord:get_replica_keys(rt_chord:hash_key("42"))
               end, "get_keys_for_replica_string"),
    ok.

get_keys_for_replica_int(_Config) ->
      iter(count(), fun () ->
                            rt_chord:get_replica_keys(rt_chord:hash_key(42))
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

next_hop_setup() ->
    Pred = node:new(pred, 1, 0),
    Me = node:new(me, 2, 0),
    Succ = node:new(succ, 3, 0),
    RT = gb_trees:enter(1, Pred,
          gb_trees:enter(4, node:new(succ2, 4, 0),
           gb_trees:enter(5, node:new(succ3, 5, 0),
            gb_trees:enter(6, node:new(succ4, 6, 0),
             gb_trees:enter(100, node:new(rt5, 100, 0),
              gb_trees:enter(101, node:new(rt6, 101, 0),
               gb_trees:enter(102, node:new(rt7, 102, 0),
                gb_trees:enter(103, node:new(rt8, 103, 0),
                 rt_chord:empty_ext(Succ))))))))),
    % note: the ets table will be deleted automatically since ct starts a
    % process for each test case (an ets table dies with its owner)
    TableName = list_to_atom("performance_SUITE:rm_tman"),
    NeighbTable = ets:new(TableName, [ordered_set, private]),
    Neighbors =
        nodelist:add_nodes(
          nodelist:new_neighborhood(Pred, Me, Succ),
          [node:new(list_to_atom(lists:flatten(io_lib:format("succ~w", [Id]))), Id + 2, 0) || Id <- lists:seq(2, config:read(succ_list_length))] ++
              [node:new(list_to_atom(lists:flatten(io_lib:format("pred~w", [Id]))), 1022 - Id, 0) || Id <- lists:seq(2, config:read(pred_list_length))],
          config:read(succ_list_length), config:read(pred_list_length)),
    ets:insert(NeighbTable, {neighbors, Neighbors}),
    _State = dht_node_state:new(RT, NeighbTable, db).

next_hop_no_neighbors(_Config) ->
    State = next_hop_setup(),
    config:write(rt_size_use_neighbors, 0),
    iter(count(), fun() -> rt_chord:next_hop(State, 42) end, "next_hop(42) no neighbors"),
    iter(count(), fun() -> rt_chord:next_hop(State, 5) end, "next_hop(5) no neighbors"),
    ok.

next_hop_with_neighbors(_Config) ->
    State = next_hop_setup(),
    config:write(rt_size_use_neighbors, 10),
    iter(count(), fun() -> rt_chord:next_hop(State, 42) end, "next_hop(42) with neighbors"),
    iter(count(), fun() -> rt_chord:next_hop(State, 5) end, "next_hop(5) with neighbors"),
    ok.

pid_groups_lookup(_Config) ->
    {ok, _Pid} = pid_groups:start_link(),
    pid_groups:join_as(atom_to_list(?MODULE), pid_groups),
    iter(count(), fun () ->
                          pid_groups:pid_of(atom_to_list(?MODULE),
                                            pid_groups)
                  end, "pid_of by group and process name"),
    error_logger:tty(false),
    log:set_log_level(none),
    unittest_helper:stop_pid_groups(),
    ok.

pid_groups_lookup_by_pid(_Config) ->
    {ok, _Pid} = pid_groups:start_link(),
    pid_groups:join_as(atom_to_list(?MODULE), pid_groups),
    iter(count(), fun () ->
                          pid_groups:group_and_name_of(self())
                  end, "group_and_name_of pid"),
    error_logger:tty(false),
    log:set_log_level(none),
    unittest_helper:stop_pid_groups(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec iter(Count::pos_integer(), F::fun(() -> any()), Tag::string()) -> ok.
iter(Count, F, Tag) ->
    F(),
    Start = erlang:now(),
    iter_inner(Count, F),
    Stop = erlang:now(),
    ElapsedTime = timer:now_diff(Stop, Start) / 1000000.0,
    Frequency = Count / ElapsedTime,
    ct:pal("~p iterations of ~p took ~ps: ~p1/s",
           [Count, Tag, ElapsedTime, Frequency]),
    ok.

-spec iter_inner(Count::pos_integer(), F::fun(() -> any())) -> ok.
iter_inner(0, _) ->
    ok;
iter_inner(N, F) ->
    F(),
    iter_inner(N - 1, F).

-spec iter2(Count::pos_integer(), F::fun((Count::pos_integer()) -> any()), Tag::string()) -> ok.
iter2(Count, F, Tag) ->
    F(0),
    Start = erlang:now(),
    iter2_inner(Count, F),
    Stop = erlang:now(),
    ElapsedTime = timer:now_diff(Stop, Start) / 1000000.0,
    Frequency = Count / ElapsedTime,
    ct:pal("~p iterations of ~p took ~ps: ~p1/s",
           [Count, Tag, ElapsedTime, Frequency]),
    ok.

-spec iter2_inner(Count::pos_integer(), F::fun((Count::pos_integer()) -> any())) -> ok.
iter2_inner(0, _) ->
    ok;
iter2_inner(N, F) ->
    F(N),
    iter2_inner(N - 1, F).

-spec iter2_foldl(Count::pos_integer(), F::fun((Count::pos_integer(), Acc) -> Acc), Acc, Tag::string()) -> Acc.
iter2_foldl(Count, F, Acc0, Tag) ->
    F(0, Acc0),
    Start = erlang:now(),
    FinalAcc = iter2_foldl_helper(Count, F, Acc0),
    Stop = erlang:now(),
    ElapsedTime = timer:now_diff(Stop, Start) / 1000000.0,
    Frequency = Count / ElapsedTime,
    ct:pal("~p foldl iterations of ~p took ~ps: ~p1/s",
           [Count, Tag, ElapsedTime, Frequency]),
    FinalAcc.

-spec iter2_foldl_helper(Count::pos_integer(), F::fun((Count::pos_integer(), Acc) -> Acc), Acc) -> Acc.
iter2_foldl_helper(0, _F, Acc) -> Acc;
iter2_foldl_helper(Count, F, Acc) ->
    iter2_foldl_helper(Count, F, F(Count, Acc)).
