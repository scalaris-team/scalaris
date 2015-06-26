%  @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Jens V. Fischer <jensvfischer@gmail.com>
%% @doc    Integrationstests for the gossip and gossip_load modules.
%% @end
%% @version $Id$
-module(gossip_SUITE).

-author('jensvfischer@gmail.com').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

-dialyzer({no_fail_call, test_request_histogram1/1}).

-define(NO_OF_NODES, 5).

all() ->
    [
        test_no_load,
        test_load,
        test_request_histogram1,
        test_request_histogram2
    ].

suite() ->
    [
     {timetrap, {seconds, 80}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) ->
    unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    unittest_helper:make_ring_with_ids(
      [?MINUS_INFINITY],
      [{config, [
                 {monitor_perf_interval, 0},  % deactivate monitor_perf
                 {gossip_load_interval, 100}, % truncated to 0, i.e. immediate delivery
                 {gossip_load_convergence_count_new_round, 5},
                 {gossip_load_convergence_count_best_values, 1},
                 {gossip_log_level_warn, warn},
                 {gossip_log_level_error, error}
                ]
       }]),
    {_, []} = api_vm:add_nodes(?NO_OF_NODES - 1),
    unittest_helper:wait_for_stable_ring_deep(),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Testcases
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_no_load(_Config) ->

    % get values from gossiping (after round finishes)
    wait_n_rounds(1),
    send2gossip({cb_msg, {gossip_load, default}, {gossip_get_values_all, self()}}, 1),

    %                 {load_info, avg, stddev, size_ldr, size_kr, minLoad, maxLoad, merged}
    LoadInfoExpected ={load_info, 0.0, 0.0, 5.0, 5.0, 0, 0, whatever, []},
    receive {gossip_get_values_all_response, {PrevLoadInfo, _, _}} ->
            ?compare(fun compare/2, PrevLoadInfo, LoadInfoExpected)
    end.


test_load(_Config) ->
    write(100),

    % request load from all nodes and calc expected values manually
    Loads = lists:map(fun (State) -> dht_node_state:get(State, load) end, get_node_states()),
    Avg = lists:sum(Loads)/?NO_OF_NODES,
    Stddev = calc_stddev(Loads),
    Size = ?NO_OF_NODES,
    Min = lists:min(Loads),
    Max = lists:max(Loads),

    %                  {load_info, avg, stddev, size_ldr, size_kr, minLoad, maxLoad, merged, other}
    LoadInfoExpected = {load_info, Avg, Stddev, Size,     Size,    Min,     Max,      dc,    []},

    % get values from gossiping (after round finishes)
    % first round might be interrupted by node joins, thus wait two rounds
    wait_n_rounds(1),
    send2gossip({cb_msg, {gossip_load, default}, {gossip_get_values_all, self()}}, 1),

    receive {gossip_get_values_all_response, {PrevLoadInfo, _, _}} ->
                ?compare(fun compare/2, PrevLoadInfo, LoadInfoExpected)
    end.


test_request_histogram1(_Config) ->
    ?expect_exception(gossip_load:request_histogram(0, comm:this()), error, function_clause).


test_request_histogram2(_Config) ->
    write(100),
    NoOfBuckets = 10,

    % get the states from all nodes and calc a histogram manually
    DHTStates = get_node_states(),
    Histos = lists:map(fun(State) -> init_histo(State, NoOfBuckets) end, DHTStates),
    MergeFun =
        fun(Histo, Acc) ->
            Combine = fun({Load1, N1}, {Load2, N2}) -> {Load1+Load2, N1+N2};
                         (unknown, {Load, N}) -> {Load, N};
                         ({Load, N}, unknown) -> {Load, N} end,
            lists:zipwith(Combine, Histo, Acc)
        end,
    InitialAcc = [ {0,0} || _X <- lists:seq(1, NoOfBuckets)],
    MergedHisto = lists:foldl(MergeFun, InitialAcc, Histos),

    % calculate the histogram from Sums and Numbers of Nodes
    Histo = lists:map(fun({Sum, N}) -> Sum/N end, MergedHisto),
    %% log:log("Histo: ~w", [Histo]),

    gossip_load:request_histogram(NoOfBuckets, comm:this()),
    %% GossipedHisto = receive {histogram, Histo} -> Histo end, % doesn't work, don't know why!
    GossipedHisto = receive Msg -> element(2, Msg) end,

    % remove the intervals from gossiped histo:
    GossipedHisto1 = lists:map(fun({_Interval, Value}) -> Value end, GossipedHisto),

    % calc the estimates
    GossipedHisto2 = lists:map(fun({Value, Weight}) -> Value/Weight end, GossipedHisto1),
    %% log:log("GossipedHisto: ~w", [GossipedHisto2]),
    ?compare(fun compare/2, GossipedHisto2, Histo).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a Msg to the gossip process
-spec send2gossip(Msg::comm:message(), Delay::non_neg_integer()) -> ok.
send2gossip(Msg, Delay) ->
    Group = pid_groups:group_with(gossip),
    Pid = pid_groups:pid_of(Group, gossip),
    msg_delay:send_local(Delay, Pid, Msg).

%% @doc Waits n rounds, pulling the web_debug_info every second.
-spec wait_n_rounds(NoOfRounds::pos_integer()) -> ok.
wait_n_rounds(NoOfRounds) ->
    Round = get_current_round(),
    wait_for_round(Round+NoOfRounds).


%% @doc Helper for wait_n_round/0
-spec wait_for_round(TargetRound::pos_integer()) -> ok.
wait_for_round(TargetRound) ->
    Round = get_current_round(),
    %% log:log("CurrentRound: ~w, TargetRound: ~w", [Round, TargetRound]),
    if Round >= TargetRound -> ok;
       Round =/= TargetRound -> wait_for_round(TargetRound)
    end.


%% @doc Get the current round (with one second delay).
-spec get_current_round() -> non_neg_integer().
get_current_round() ->
    send2gossip({web_debug_info, self()}, 1),
    receive {web_debug_info_reply, KeyValueList} ->
        case lists:keyfind("cur_round", 1, KeyValueList) of
            {"cur_round", Round} -> Round;
            false -> get_current_round() % happens if cb module not yet initiated
        end
    end.


%% @doc Writes the given number of dummy entries to scalaris.
-spec write(NoOfEntries::pos_integer()) -> ok.
write(NoOfEntries) ->
    _CommitLog = [ begin
        K = io_lib:format("k~w", [N]),
        V = io_lib:format("v~w", [N]),
        api_tx:write(K,V)
      end || N <- lists:seq(1,NoOfEntries) ],
    %% log:log("CommitLog: ~w", [_CommitLog]),
    ok.


%% @doc Get the states from all dht nodes.
-spec get_node_states() -> [dht_node_state:state(), ...].
get_node_states() ->
    % request load from all nodes and calc expected values manually
    [gen_component:get_state(Pid) || Pid <- pid_groups:find_all(dht_node)].


%% @doc Builds a initial histogram, i.e. a histogram for a single node.
-spec init_histo(DHTNodeState::dht_node_state:state(), NoOfBuckets::pos_integer()) -> [{non_neg_integer(), 1}, ...].
init_histo(DHTNodeState, NoOfBuckets) ->
    DB = dht_node_state:get(DHTNodeState, db),
    MyRange = dht_node_state:get(DHTNodeState, my_range),
    Buckets = intervals:split(intervals:all(), NoOfBuckets),
    _Histo = [ get_load_for_interval(BucketInterval, MyRange, DB) || BucketInterval <- Buckets ],
    %% log:log("Histo ~w", [_Histo]),
    _Histo.


%% @doc Gets the load for a given interval.
-spec get_load_for_interval(BucketInterval::intervals:interval(),
    MyRange::intervals:interval(), DB::db_dht:db()) -> {non_neg_integer(), 1}.
get_load_for_interval(BucketInterval, MyRange, DB) ->
    case intervals:is_empty(intervals:intersection(BucketInterval, MyRange)) of
        true -> unknown;
        false ->
            Load = db_dht:get_load(DB, BucketInterval),
            {Load, 1}
    end.


%% @doc Compares LoadInfo records and Histo. Returns true if the difference is less than 5 %.
-spec compare(LoadInfo1::gossip_load:load_info(), LoadInfo1::gossip_load:load_info()) -> boolean();
             (Histo1::[float(),...], Histo2::[float(),...]) -> boolean().
compare(LoadInfo1, LoadInfo2) when is_tuple(LoadInfo1) andalso is_tuple(LoadInfo2) ->
    %% log:log("LoadInfo1: ~w~n", [LoadInfo1]),
    %% log:log("LoadInfo2: ~w~n", [LoadInfo2]),
    Fun = fun(Key, {Acc, LI1, LI2}) ->
            Value1 = gossip_load:load_info_get(Key, LI1),
            Value2 = gossip_load:load_info_get(Key, LI2),
            {Acc andalso calc_diff(Value1, Value2) < 5.0, LI1, LI2}
    end,
    % merged counter is excluded from comparison
    element(1, lists:foldl(Fun, {true, LoadInfo1, LoadInfo2},
                           [avgLoad, stddev, size_ldr, size_kr, minLoad, maxLoad]));

compare(Histo1, Histo2) when is_list(Histo1) andalso is_list(Histo2) ->
    Fun = fun(Val1, Val2) -> calc_diff(Val1, Val2) < 5.0 end,
    ComparisonResult = lists:zipwith(Fun, Histo1, Histo2),
    lists:all(fun(X) -> X end, ComparisonResult).


%% @doc Calculates the difference in percent from one value to another value.
-spec calc_diff(Value1::float()|unknown, Value2::float()|unknown) -> float().
calc_diff(Value1, Value2) ->
    if
        (Value1 =/= unknown) andalso (Value1 =:= Value2) -> 0.0;
        (Value1 =:= unknown) orelse (Value2 =:= unknown) orelse (Value1 == 0) -> 100.0;
        true -> ((Value1 + abs(Value2 - Value1)) * 100.0 / Value1) - 100
    end.

%% @doc Calculate the standard deviation for a given list of numbers.
-spec calc_stddev(Loads::[number()]) -> float().
calc_stddev(Loads) ->
    Avg = lists:sum(Loads)/?NO_OF_NODES,
    LoadsSquared = lists:map(fun (Load) -> math:pow(Avg-Load, 2) end, Loads),
    _Stddev = math:sqrt(lists:sum(LoadsSquared)/?NO_OF_NODES).

