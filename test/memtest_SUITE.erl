%% @copyright 2013-2015 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @version $Id$
-module(memtest_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").
-include("record_helpers.hrl").

all()   -> [
            {group, api_tx},
            {group, ring}
           ].

groups() ->
    [
     {api_tx, [], [write_1000, fill_1000, fill_2000, modify_100x100]},
     {ring,   [], [create_ring_100, add_remove_nodes_50, add_kill_nodes_50]}
    ].

suite() -> [ {timetrap, {seconds, 300}} ].

init_per_suite(Config) ->
    _ = code:ensure_loaded(gossip_load_default), % otherwise loaded too late
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) ->
    ct:comment(io_lib:format("BEGIN ~p", [Group])),
    Config.

end_per_group(Group, Config) ->
    ct:comment(io_lib:format("END ~p", [Group])),
    Config.

init_per_testcase(_TestCase, Config) ->
%%     Group = case proplists:get_value(tc_group_properties, Config) of
%%                 undefined                 -> undefined;
%%                 Props when is_list(Props) -> proplists:get_value(name, Props)
%%             end,
    %% stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    % start ring:
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    %% add_remove_nodes_50 and add_kill_nodes_50 throw error:system_limit for larger r
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}, {monitor_perf_interval, 0},
                                            {replication_factor, 4}]}]),
    config:write(no_print_ring_data, true),
    % load necessary code for the atom check to work (we do not create additional atoms):
    InitKey = "memtest_SUITE",
    {ok} = api_tx:write(InitKey, 1),
    api_tx_SUITE:wait_for_dht_entries(?RT:get_replica_keys(?RT:hash_key(InitKey))),
    _ = code:ensure_loaded(gossip_load_default), % otherwise loaded too late
    garbage_collect_all(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ct:pal("Memory after test:~n~p~n", [erlang:memory()]),
    unittest_helper:stop_ring(), % note: we do not use scalaris_cth's stop_ring!
    garbage_collect_all(),
    ct:pal("Memory after unittest_helper:stop_ring/0:~n~p~n", [erlang:memory()]),
    ok.

-record(mem_info, {binary         = ?required(mem_info, binary)         :: pos_integer(),
                   atom_used      = ?required(mem_info, atom_used)      :: pos_integer(),
                   processes_used = ?required(mem_info, processes_used) :: pos_integer(),
                   ets            = ?required(mem_info, ets)            :: pos_integer()
                  }).

%% @doc Creates an uncompressed binary of a list of Size integers starting at I.
-spec make_binary(I::integer(), Size::non_neg_integer()) -> binary().
make_binary(I, Size) when Size >= 0->
    Res = erlang:term_to_binary(lists:seq(I, I + Size)),
%%     ct:pal("binary size: ~B", [erlang:byte_size(Res)]),
    Res.

%% @doc Prints information about the given ets table ID and compares it with
%%      the total Erlang VM memory stats.
print_table_info(Table) ->
    print_table_info(Table, "").

%% @doc Prints information about the given ets table ID and compares it with
%%      the total Erlang VM memory stats (prefixes the message with Status).
print_table_info(Table, Status) ->
    Memory = ets:info(Table, memory),
    ContentSize = lists:sum([byte_size(element(2,X)) || X <- ets:tab2list(Table), is_binary(element(2, X))]),
    ct:pal("~s~nerlang total: ~B, binary: ~B~nets table ~p, memory: ~B, contents: ~B~n",
           [Status, erlang:memory(total), erlang:memory(binary), Table, Memory, ContentSize]),
    garbage_collect_all().

%% @doc Gets memory statistics.
-spec get_meminfo() -> #mem_info{}.
get_meminfo() ->
    [{binary, BinSize}, {atom_used, AtomUSize}, {processes_used, ProcUSize}, {ets, EtsSize}] =
        erlang:memory([binary, atom_used, processes_used, ets]),
    #mem_info{binary = BinSize, atom_used = AtomUSize,
              processes_used = ProcUSize, ets = EtsSize}.

%% @doc Checks whether the NewMemInfo field indicates increased memory use
%%      compared to PrevMemInfo and returns whether this is the case or not.
%%      AddedSize is the expected binary size difference.
%%      Note: Tolerates 250k binary, 250k processes_used and 250k ets overhead
%%            (keep in sync with check_memory_inc_/5).
-spec check_memory_inc_bool_(
        PrevMemInfo::#mem_info{}, NewMemInfo::#mem_info{},
        NewItems::non_neg_integer(), AddedSize::non_neg_integer(),
        CheckAtoms::boolean()) -> boolean().
check_memory_inc_bool_(PrevMemInfo, NewMemInfo, NewItems, AddedSize, CheckAtoms) ->
    % assume an entry for an item uses 4 * 1k memory for ets (excluding the binary size)
    EntryEtsSize = 4 * 1000,

    % we do not create additional atoms!
    ?implies(CheckAtoms, NewMemInfo#mem_info.atom_used =:= PrevMemInfo#mem_info.atom_used) andalso
    % tolerate 250k binary memory overhead for running maintenance processes and binary messages
    NewMemInfo#mem_info.binary =< (PrevMemInfo#mem_info.binary + AddedSize + 250000) andalso
    % tolerate 250k processes_used memory overhead for running maintenance processes like RT rebuild etc.
    NewMemInfo#mem_info.processes_used =< (PrevMemInfo#mem_info.processes_used + 250000) andalso
    % tolerate 250k ets memory overhead for DB data etc.
    NewMemInfo#mem_info.ets =< (PrevMemInfo#mem_info.ets + 250000 + (EntryEtsSize * NewItems)).

%% @doc If debugging is disabled, execute check_memory_inc_bool_/5 with atom_used
%%      check, otherwise do not check atom_used if the test starts nodes (with
%%      debugging, atoms in the form of registered processes are generated).
-spec check_memory_inc_bool(
        PrevMemInfo::#mem_info{}, NewMemInfo::#mem_info{},
        NewItems::non_neg_integer(), AddedSize::non_neg_integer(),
        TestStartsNodes::boolean()) -> boolean().
-ifdef(enable_debug).
check_memory_inc_bool(PrevMemInfo, NewMemInfo, NewItems, AddedSize, TestStartsNodes) ->
    check_memory_inc_bool_(PrevMemInfo, NewMemInfo, NewItems, AddedSize, not TestStartsNodes).
-else.
check_memory_inc_bool(PrevMemInfo, NewMemInfo, NewItems, AddedSize, _TestStartsNodes) ->
    check_memory_inc_bool_(PrevMemInfo, NewMemInfo, NewItems, AddedSize, true).
-endif.

%% @doc Helper for check_memory_inc/5.
%%      Note: Tolerates 250k binary, 250k processes_used and 250k ets overhead.
-spec check_memory_inc_(
        PrevMemInfo::#mem_info{}, NewMemInfo::#mem_info{},
        NewItems::non_neg_integer(), AddedSize::non_neg_integer(),
        CheckAtoms::boolean()) -> ok.
check_memory_inc_(PrevMemInfo, NewMemInfo, NewItems, AddedSize, CheckAtoms) ->
    % assume an entry for an item uses 4 * 1k memory for ets (excluding the binary size)
    EntryEtsSize = 4 * 1000,

    % we do not create additional atoms!
    ?IIF(CheckAtoms,
         ?equals(NewMemInfo#mem_info.atom_used, PrevMemInfo#mem_info.atom_used),
         ok),

    % tolerate 250k binary memory overhead for running maintenance processes and binary messages
    ?equals_pattern_w_note(NewMemInfo#mem_info.binary, X when X =< PrevMemInfo#mem_info.binary + AddedSize + 250000,
                           lists:flatten(io_lib:format("PrevBinSize: ~B, AddedSize: ~B, NewBinSize: ~B, Diff: ~B",
                                         [PrevMemInfo#mem_info.binary, NewMemInfo#mem_info.binary, AddedSize,
                                          NewMemInfo#mem_info.binary - AddedSize - PrevMemInfo#mem_info.binary]))),

    case erlang:system_info(version) of
        %% R18.1 and the current dev (8.0) have a memory leak"
        "7.1" -> %% R18.1
            ok;
        %%"8.0" -> %% current dev
        %%    %% tolerate 700k processes_used memory overhead for running maintenance processes like RT rebuild etc.
        %%    ?equals_pattern_w_note(NewMemInfo#mem_info.processes_used, X when X =< PrevMemInfo#mem_info.processes_used + 700000,
        %%                           lists:flatten(io_lib:format("PrevProcUSize: ~B, NewProcUSize: ~B, Diff: ~B",
        %%                                         [PrevMemInfo#mem_info.processes_used, NewMemInfo#mem_info.processes_used,
        %%                                          NewMemInfo#mem_info.processes_used - PrevMemInfo#mem_info.processes_used]))),
        %%
        %%    %% tolerate 270k ets memory overhead for DB data etc.
        %%    ?equals_pattern_w_note(NewMemInfo#mem_info.ets, X when X =< PrevMemInfo#mem_info.ets + 270000 + (EntryEtsSize * NewItems),
        %%                           lists:flatten(io_lib:format("PrevEtsSize: ~B, NewEtsSize: ~B, Diff: ~B",
        %%                                         [PrevMemInfo#mem_info.ets, NewMemInfo#mem_info.ets,
        %%                                          NewMemInfo#mem_info.ets - PrevMemInfo#mem_info.ets])));

        _ ->
            %% tolerate 250k processes_used memory overhead for running maintenance processes like RT rebuild etc.
            ?equals_pattern_w_note(NewMemInfo#mem_info.processes_used, X when X =< PrevMemInfo#mem_info.processes_used + 250000,
                                   lists:flatten(io_lib:format("PrevProcUSize: ~B, NewProcUSize: ~B, Diff: ~B",
                                                 [PrevMemInfo#mem_info.processes_used, NewMemInfo#mem_info.processes_used,
                                                  NewMemInfo#mem_info.processes_used - PrevMemInfo#mem_info.processes_used]))),
            %% tolerate 250k ets memory overhead for DB data etc.
            ?equals_pattern_w_note(NewMemInfo#mem_info.ets, X when X =< PrevMemInfo#mem_info.ets + 250000 + (EntryEtsSize * NewItems),
                                   lists:flatten(io_lib:format("PrevEtsSize: ~B, NewEtsSize: ~B, Diff: ~B",
                                                 [PrevMemInfo#mem_info.ets, NewMemInfo#mem_info.ets,
                                                  NewMemInfo#mem_info.ets - PrevMemInfo#mem_info.ets])))
    end,
    ok.

%% @doc If debugging is disabled, execute check_memory_inc_/5 with atom_used
%%      check, otherwise do not check atom_used if the test starts nodes (with
%%      debugging, atoms in the form of registered processes are generated).
-spec check_memory_inc(
        PrevMemInfo::#mem_info{}, NewMemInfo::#mem_info{},
        NewItems::non_neg_integer(), AddedSize::non_neg_integer(),
        TestStartsNodes::boolean()) -> ok.
-ifdef(enable_debug).
check_memory_inc(PrevMemInfo, NewMemInfo, NewItems, AddedSize, TestStartsNodes) ->
    check_memory_inc_(PrevMemInfo, NewMemInfo, NewItems, AddedSize, not TestStartsNodes).
-else.
check_memory_inc(PrevMemInfo, NewMemInfo, NewItems, AddedSize, _TestStartsNodes) ->
    check_memory_inc_(PrevMemInfo, NewMemInfo, NewItems, AddedSize, true).
-endif.

write_1000(_Config) ->
    write(1000, 200000). % ca. 1MiB RAM

%% @doc Writes newly created binaries to the same key thus overwriting previous
%%      binaries. The result should not occupy more memory than the last
%%      written binary.
write(Number, Size) when Number >= 1 ->
    Key = lists:flatten(io_lib:format("write_~B", [Number])),
    DHTNodes = pid_groups:find_all(dht_node),
%%     ct:pal("~p~n", [erlang:memory()]),
    Table = hd([Tab || Tab <- ets:all(),
                       ets:info(Tab, name) =:= dht_node_db,
                       lists:member(ets:info(Tab, owner), DHTNodes)]),
    print_table_info(Table),
    PrevMemInfo = get_meminfo(),
    % only the last binary should be kept!
    MyBinSize =
        util:for_to_fold(1, Number,
                         fun(I) ->
                                 Bin = make_binary(I, Size),
                                 {ok} = api_tx:write(Key, Bin),
                                 % wait for late write messages:
                                 api_tx_SUITE:wait_for_dht_entries(?RT:get_replica_keys(?RT:hash_key(Key))),
                                 %print_table_info(Table),
                                 erlang:byte_size(Bin)
                         end,
                         fun(BSize, _PrevSize) -> BSize end,
                         0),
    NewMemInfo = garbage_collect_all_and_check(40, PrevMemInfo, 1, MyBinSize, false),
%%     ct:pal("~p~n", [erlang:memory()]),
    print_table_info(Table),
    check_memory_inc(PrevMemInfo, NewMemInfo, 1, MyBinSize, false),
    ok.

fill_1000(_Config) ->
    fill(1000, 20000). % ca. 100MiB RAM

fill_2000(_Config) ->
    fill(2000, 20000). % ca. 200MiB RAM

fill(Number, Size) when Number >= 1 ->
    DHTNodes = pid_groups:find_all(dht_node),
%%     ct:pal("~p~n", [erlang:memory()]),
    Table = hd([Tab || Tab <- ets:all(),
                       ets:info(Tab, name) =:= dht_node_db,
                       lists:member(ets:info(Tab, owner), DHTNodes)]),
    print_table_info(Table),
    PrevMemInfo = get_meminfo(),
    MyBinSize =
        util:for_to_fold(1, Number,
                         fun(I) ->
                                 Key = lists:flatten(io_lib:format("fill_~B", [I])),
                                 Bin = make_binary(I, Size),
                                 {ok} = api_tx:write(Key, Bin),
                                 % wait for late write messages:
                                 api_tx_SUITE:wait_for_dht_entries(?RT:get_replica_keys(?RT:hash_key(Key))),
                                 %print_table_info(Table, lists:flatten(io_lib:format("~B", [I]))),
                                 erlang:byte_size(Bin)
                         end,
                         fun(BSize, TotalSize) -> TotalSize + BSize end,
                         0),
    NewMemInfo = garbage_collect_all_and_check(40, PrevMemInfo, Number, MyBinSize, false),
%%     ct:pal("~p~n", [erlang:memory()]),
    print_table_info(Table),
    check_memory_inc(PrevMemInfo, NewMemInfo, Number, MyBinSize, false),
    ok.

% @doc Modifies 100 items 100 times.
modify_100x100(_Config) ->
    modify(100, 100, 20000). % ca. 10MiB RAM

modify(Number, Repeat, Size) when Number >= 1 andalso Repeat >= 1 ->
    DHTNodes = pid_groups:find_all(dht_node),
%%     ct:pal("~p~n", [erlang:memory()]),
    Table = hd([Tab || Tab <- ets:all(),
                       ets:info(Tab, name) =:= dht_node_db,
                       lists:member(ets:info(Tab, owner), DHTNodes)]),
    print_table_info(Table),
    PrevMemInfo = get_meminfo(),
    MyBinSize =
        util:for_to_fold(
          1, Number,
          fun(I) ->
                  util:for_to_fold(
                    1, Repeat,
                    fun(J) ->
                            Key = lists:flatten(io_lib:format("modify_~B", [I])),
                            Bin = make_binary(I+J, Size),
                            {ok} = api_tx:write(Key, Bin),
                            % wait for late write messages:
                            api_tx_SUITE:wait_for_dht_entries(?RT:get_replica_keys(?RT:hash_key(Key))),
                            %print_table_info(Table, lists:flatten(io_lib:format("~B", [I]))),
                            erlang:byte_size(Bin)
                    end,
                    fun(BSize, _PrevSize) -> BSize end,
                    0)
                  end,
          fun(BSize, TotalSize) -> TotalSize + BSize end,
          0),
    NewMemInfo = garbage_collect_all_and_check(40, PrevMemInfo, Number, MyBinSize, false),
%%     ct:pal("~p~n", [erlang:memory()]),
    print_table_info(Table),
    check_memory_inc(PrevMemInfo, NewMemInfo, Number, MyBinSize, false),
    ok.

create_ring_100(Config) ->
    % stop the initial ring (the ring is needed to initialise atoms etc.)
    unittest_helper:stop_ring(),
    garbage_collect_all(),

    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    OldProcesses = unittest_helper:get_processes(),
    PrevMemInfo = get_meminfo(),
    util:for_to(
      1, 100,
      fun(_I) ->
              unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}, {monitor_perf_interval, 0}]}]),
              unittest_helper:stop_ring()
      end),
    NewMemInfo = garbage_collect_all_and_check(10, PrevMemInfo, 0, 0, true),
    NewProcesses = unittest_helper:get_processes(),
    {_OnlyOld, _Both, OnlyNew} =
        util:split_unique(OldProcesses, NewProcesses,
                          fun(P1, P2) ->
                                  element(1, P1) =< element(1, P2)
                          end, fun(_P1, P2) -> P2 end),
    ?equals(OnlyNew, []),
    %%     ct:pal("~p~n", [erlang:memory()]),
    check_memory_inc(PrevMemInfo, NewMemInfo, 0, 0, true),
    ok.

add_remove_nodes_50(_Config) ->
    config:write(gossip_vivaldi_latency_timeout, 0),
    {[TmpNode], []} = api_vm:add_nodes(1), % loads all code needed for joins
    {[TmpNode], []} = api_vm:shutdown_nodes_by_name([TmpNode]), % loads all code needed for graceful leaves
    garbage_collect_all(),
    OldProcesses = unittest_helper:get_processes(),
    PrevMemInfo = get_meminfo(),
    {NewNodes, []} = api_vm:add_nodes(50),
    unittest_helper:wait_for_stable_ring_deep(),
    _ = api_vm:shutdown_nodes_by_name(NewNodes),
    timer:sleep(1000), % wait for vivaldi_latency timeouts
    unittest_helper:wait_for_stable_ring_deep(),
    _ = garbage_collect_all_and_check(10, PrevMemInfo, 0, 0, true),
    NewProcesses = unittest_helper:get_processes(),
    {_OnlyOld, _Both, OnlyNew} =
        util:split_unique(OldProcesses, NewProcesses,
                          fun(P1, P2) ->
                                  element(1, P1) =< element(1, P2)
                          end, fun(_P1, P2) -> P2 end),
    ?equals(OnlyNew, []),
%%     ct:pal("~p~n", [erlang:memory()]),
    % TODO: add memory check? (process state may increase, e.g. dead nodes in fd)
%%     check_memory_inc(PrevMemInfo, NewMemInfo, 0, 0, true),
    ok.

add_kill_nodes_50(_Config) ->
    config:write(gossip_vivaldi_latency_timeout, 0),
    {[TmpNode], []} = api_vm:add_nodes(1), % loads all code needed for joins
    {[TmpNode], []} = api_vm:kill_nodes_by_name([TmpNode]), % loads all code needed for killing nodes
    garbage_collect_all(),
    OldProcesses = unittest_helper:get_processes(),
    PrevMemInfo = get_meminfo(),
    {NewNodes, []} = api_vm:add_nodes(50),
    unittest_helper:wait_for_stable_ring_deep(),
    _ = api_vm:kill_nodes_by_name(NewNodes),
    timer:sleep(1000), % wait for vivaldi_latency timeouts
    unittest_helper:wait_for_stable_ring_deep(),
    _ = garbage_collect_all_and_check(10, PrevMemInfo, 0, 0, true),
    NewProcesses = unittest_helper:get_processes(),
    {_OnlyOld, _Both, OnlyNew} =
        util:split_unique(OldProcesses, NewProcesses,
                          fun(P1, P2) ->
                                  element(1, P1) =< element(1, P2)
                          end, fun(_P1, P2) -> P2 end),
    ?equals(OnlyNew, []),
%%     ct:pal("~p~n", [erlang:memory()]),
    % TODO: add memory check? (process state may increase, e.g. dead nodes in fd)
%%     check_memory_inc(PrevMemInfo, NewMemInfo, 0, 0, true),
    ok.

%% @doc Starts garbage collection for all processes.
garbage_collect_all() ->
    _ = [erlang:garbage_collect(Pid) || Pid <- processes()],
    % wait a bit for the gc to finish
    timer:sleep(1000).

garbage_collect_all_and_check(0, _MemInfo, _NewItems, _AddedSize, _TestStartsNodes) ->
    get_meminfo();
garbage_collect_all_and_check(Retries, MemInfo, NewItems, AddedSize, TestStartsNodes) when Retries > 0 ->
    garbage_collect_all_and_check_(1, Retries + 1, MemInfo, NewItems, AddedSize, TestStartsNodes).

garbage_collect_all_and_check_(Max, Max, _MemInfo, _NewItems, _AddedSize, _TestStartsNodes) ->
    get_meminfo();
garbage_collect_all_and_check_(N, Max, MemInfo, NewItems, AddedSize, TestStartsNodes) when N < Max ->
    garbage_collect_all(),
    MemInfo2 = get_meminfo(),
    ct:pal("gc changes (~B):~nbin: ~12.B, atom: ~8.B, procs: ~10.B, ets: ~10.B~n    (+ ~10.B)      (+ ~6.B)       (+ ~8.B)     (+ ~8.B)",
           [N, MemInfo2#mem_info.binary, MemInfo2#mem_info.atom_used,
            MemInfo2#mem_info.processes_used, MemInfo2#mem_info.ets,
            MemInfo2#mem_info.binary - MemInfo#mem_info.binary,
            MemInfo2#mem_info.atom_used - MemInfo#mem_info.atom_used,
            MemInfo2#mem_info.processes_used - MemInfo#mem_info.processes_used,
            MemInfo2#mem_info.ets - MemInfo#mem_info.ets]),
    case check_memory_inc_bool(MemInfo, MemInfo2, NewItems, AddedSize, TestStartsNodes) of
        true  -> MemInfo2;
        false -> garbage_collect_all_and_check_(N + 1, Max, MemInfo, NewItems, AddedSize, TestStartsNodes)
    end.
