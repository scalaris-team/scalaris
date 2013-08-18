%% @copyright 2013 Zuse Institute Berlin

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
            write_1000,
            fill_1000,
            fill_2000,
            modify_100x100
           ].
suite() -> [ {timetrap, {seconds, 300}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% stop ring from previous test case (it may have run into a timeout
    unittest_helper:stop_ring(),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}, {monitor_perf_interval, 0}]}]),
    config:write(no_print_ring_data, true),
    % load necessary code for the atom check to work (we do not create additional atoms):
    InitKey = "memtest_SUITE",
    {ok} = api_tx:write(InitKey, 1),
    api_tx_SUITE:wait_for_dht_entries(?RT:get_replica_keys(?RT:hash_key(InitKey))),
    garbage_collect_all(),
    Config.

end_per_testcase(_TestCase, Config) ->
    ct:pal("Memory before unittest_helper:stop_ring/0:~n~p~n", [erlang:memory()]),
    unittest_helper:stop_ring(),
    garbage_collect_all(),
    ct:pal("Memory after unittest_helper:stop_ring/0:~n~p~n", [erlang:memory()]),
    Config.

-record(mem_info, {binary         = ?required(mem_info, binary)         :: pos_integer(),
                   atom_used      = ?required(mem_info, atom_used)      :: pos_integer(),
                   processes_used = ?required(mem_info, processes_used) :: pos_integer(),
                   ets            = ?required(mem_info, ets)            :: pos_integer()
                  }).

-spec make_binary(I::integer(), Size::non_neg_integer()) -> binary().
make_binary(I, Size) when Size >= 0->
    Res = erlang:term_to_binary(lists:seq(I, I + Size)),
%%     ct:pal("binary size: ~B", [erlang:byte_size(Res)]),
    Res.

print_table_info(Table) ->
    print_table_info(Table, "").

print_table_info(Table, Status) ->
    Memory = ets:info(Table, memory),
    ContentSize = lists:sum([byte_size(element(2,X)) || X <- ets:tab2list(Table), is_binary(element(2, X))]),
    ct:pal("~s~nerlang total: ~B, binary: ~B~nets table ~p, memory: ~B, contents: ~B~n",
           [Status, erlang:memory(total), erlang:memory(binary), Table, Memory, ContentSize]),
    garbage_collect_all().

-spec get_meminfo() -> #mem_info{}.
get_meminfo() ->
    [{binary, BinSize}, {atom_used, AtomUSize}, {processes_used, ProcUSize}, {ets, EtsSize}] =
        erlang:memory([binary, atom_used, processes_used, ets]),
    #mem_info{binary = BinSize, atom_used = AtomUSize,
              processes_used = ProcUSize, ets = EtsSize}.

-spec check_memory_inc_bool(PrevMemInfo::#mem_info{}, NewMemInfo::#mem_info{}, NewItems::non_neg_integer(), AddedSize::non_neg_integer()) -> boolean().
check_memory_inc_bool(PrevMemInfo, NewMemInfo, NewItems, AddedSize) ->
    % assume an entry for an item uses 4 * 1k memory for ets (excluding the binary size)
    EntryEtsSize = 4 * 1000,
    
    % NOTE: use stronger bounds than check_memory_inc/4 as other processes may increase the memory use again
    % we do not create additional atoms!
    NewMemInfo#mem_info.atom_used =:= PrevMemInfo#mem_info.atom_used andalso
    % tolerate 50k binary memory overhead for running maintenance processes and binary messages
    NewMemInfo#mem_info.binary =< (PrevMemInfo#mem_info.binary + AddedSize + 50000) andalso
    % tolerate 50k processes_used memory overhead for running maintenance processes like RT rebuild etc.
    NewMemInfo#mem_info.processes_used =< (PrevMemInfo#mem_info.processes_used + 50000) andalso
    NewMemInfo#mem_info.ets =< (PrevMemInfo#mem_info.ets + 50000 + (EntryEtsSize * NewItems)).

-spec check_memory_inc(PrevMemInfo::#mem_info{}, NewMemInfo::#mem_info{}, NewItems::non_neg_integer(), AddedSize::non_neg_integer()) -> ok.
check_memory_inc(PrevMemInfo, NewMemInfo, NewItems, AddedSize) ->
    % assume an entry for an item uses 4 * 1k memory for ets (excluding the binary size)
    EntryEtsSize = 4 * 1000,
    
    % we do not create additional atoms!
    ?equals(NewMemInfo#mem_info.atom_used, PrevMemInfo#mem_info.atom_used),
    
    % tolerate 250k binary memory overhead for running maintenance processes and binary messages
    ?equals_pattern_w_note(NewMemInfo#mem_info.binary, X when X =< PrevMemInfo#mem_info.binary + AddedSize + 250000,
                           io_lib:format("PrevBinSize: ~B, AddedSize: ~B, NewBinSize: ~B, Diff: ~B",
                                         [PrevMemInfo#mem_info.binary, NewMemInfo#mem_info.binary, AddedSize,
                                          NewMemInfo#mem_info.binary - AddedSize - PrevMemInfo#mem_info.binary])),
    
    % tolerate 250k processes_used memory overhead for running maintenance processes like RT rebuild etc.
    ?equals_pattern_w_note(NewMemInfo#mem_info.processes_used, X when X =< PrevMemInfo#mem_info.processes_used + 250000,
                           io_lib:format("PrevProcUSize: ~B, NewProcUSize: ~B, Diff: ~B",
                                         [PrevMemInfo#mem_info.processes_used, NewMemInfo#mem_info.processes_used,
                                          NewMemInfo#mem_info.processes_used - PrevMemInfo#mem_info.processes_used])),
    ?equals_pattern_w_note(NewMemInfo#mem_info.ets, X when X =< PrevMemInfo#mem_info.ets + 250000 + (EntryEtsSize * NewItems),
                           io_lib:format("PrevEtsSize: ~B, NewEtsSize: ~B, Diff: ~B",
                                         [PrevMemInfo#mem_info.ets, NewMemInfo#mem_info.ets,
                                          NewMemInfo#mem_info.ets - PrevMemInfo#mem_info.ets])),
    ok.

write_1000(_Config) ->
    write(1000, 200000). % ca. 1MiB RAM

%% @doc Writes newly created binaries to the same key thus overwriting previous
%%      binaries. The result should not occupy more memory than the last
%%      written binary.
write(Number, Size) when Number >= 1 ->
    Key = lists:flatten(io_lib:format("write_~B", [Number])),
%%     ct:pal("~p~n", [erlang:memory()]),
    Table = hd([Tab || Tab <- ets:all(), is_atom(Tab), lists:prefix("db_", erlang:atom_to_list(Tab))]),
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
    garbage_collect_all_and_check(40, PrevMemInfo, 1, MyBinSize),
    NewMemInfo = get_meminfo(),
%%     ct:pal("~p~n", [erlang:memory()]),
    print_table_info(Table),
    check_memory_inc(PrevMemInfo, NewMemInfo, 1, MyBinSize),
    ok.

fill_1000(_Config) ->
    fill(1000, 20000). % ca. 100MiB RAM

fill_2000(_Config) ->
    fill(2000, 20000). % ca. 200MiB RAM

fill(Number, Size) ->
    fill(1, Number, Size).

fill(Start, End, Size) when End >= Start ->
%%     ct:pal("~p~n", [erlang:memory()]),
    Table = hd([Tab || Tab <- ets:all(), is_atom(Tab), lists:prefix("db_", erlang:atom_to_list(Tab))]),
    print_table_info(Table),
    PrevMemInfo = get_meminfo(),
    MyBinSize =
        util:for_to_fold(Start, End,
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
    garbage_collect_all_and_check(40, PrevMemInfo, (End - Start + 1), MyBinSize),
    NewMemInfo = get_meminfo(),
%%     ct:pal("~p~n", [erlang:memory()]),
    print_table_info(Table),
    check_memory_inc(PrevMemInfo, NewMemInfo, (End - Start + 1), MyBinSize),
    ok.

% @doc Modifies 100 items 100 times.
modify_100x100(_Config) ->
    modify(100, 100, 20000). % ca. 10MiB RAM

modify(Number, Repeat, Size) ->
    modify(1, Number, Repeat, Size).

modify(Start, End, Repeat, Size) when End >= Start andalso Repeat >= 1 ->
%%     ct:pal("~p~n", [erlang:memory()]),
    Table = hd([Tab || Tab <- ets:all(), is_atom(Tab), lists:prefix("db_", erlang:atom_to_list(Tab))]),
    print_table_info(Table),
    PrevMemInfo = get_meminfo(),
    MyBinSize =
        util:for_to_fold(
          Start, End,
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
    garbage_collect_all_and_check(40, PrevMemInfo, (End - Start + 1), MyBinSize),
    NewMemInfo = get_meminfo(),
%%     ct:pal("~p~n", [erlang:memory()]),
    print_table_info(Table),
    check_memory_inc(PrevMemInfo, NewMemInfo, (End - Start + 1), MyBinSize),
    ok.

garbage_collect_all() ->
    _ = [erlang:garbage_collect(Pid) || Pid <- processes()],
    % wait a bit for the gc to finish
    timer:sleep(1000).

garbage_collect_all_and_check(0, _MemInfo, _NewItems, _AddedSize) -> ok;
garbage_collect_all_and_check(Retries, MemInfo, NewItems, AddedSize) when Retries > 0 ->
    garbage_collect_all_and_check_(1, Retries + 1, MemInfo, NewItems, AddedSize).
    
garbage_collect_all_and_check_(Max, Max, _MemInfo, _NewItems, _AddedSize) -> ok;
garbage_collect_all_and_check_(N, Max, MemInfo, NewItems, AddedSize) when N < Max ->
    garbage_collect_all(),
    MemInfo2 = get_meminfo(),
    ct:pal("gc changes (~B):~nbin: ~12.B, atom: ~8.B, procs: ~10.B, ets: ~10.B~n    (+ ~10.B)      (+ ~6.B)       (+ ~8.B)     (+ ~8.B)",
           [N, MemInfo2#mem_info.binary, MemInfo2#mem_info.atom_used,
            MemInfo2#mem_info.processes_used, MemInfo2#mem_info.ets,
            MemInfo2#mem_info.binary - MemInfo#mem_info.binary,
            MemInfo2#mem_info.atom_used - MemInfo#mem_info.atom_used,
            MemInfo2#mem_info.processes_used - MemInfo#mem_info.processes_used,
            MemInfo2#mem_info.ets - MemInfo#mem_info.ets]),
    case check_memory_inc_bool(MemInfo, MemInfo2, NewItems, AddedSize) of
        true  -> ok;
        false -> garbage_collect_all_and_check_(N + 1, Max, MemInfo, NewItems, AddedSize)
    end.
