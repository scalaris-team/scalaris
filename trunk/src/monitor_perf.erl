%  @copyright 2011 Zuse Institute Berlin

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
%% @doc    Periodically executes a small benchmark to monitor the overall
%%         performance of Scalaris.
%% @end
%% @version $Id$
-module(monitor_perf).
-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-include("scalaris.hrl").

% monitor process functions
-export([start_link/1, init/1, on/2, check_config/0]).

-type state() :: {Round::non_neg_integer(), Stats::rrd:rrd()}.
-type message() ::
    {bench} |
    {propagate} |
    {get_node_details_response, node_details:node_details()} |
    {gather_stats, SourcePid::comm:mypid(), Round::pos_integer()} |
    {{get_rrd_response, Process::atom(), Key::string(), DB::rrd:rrd() | undefined}, {SourcePid::comm:mypid(), Round::pos_integer()}} |
    {gather_stats_response, Round::pos_integer(), Data::rrd:timing_type(number())} |
    {report_value, Round::pos_integer(), Stats::rrd:rrd()}.

-spec run_bench() -> ok.
run_bench() ->
    Key1 = randoms:getRandomId(),
    Key2 = randoms:getRandomId(),
    ReqList = [{read, Key1}, {read, Key2}, {commit}],
    {TimeInUs, _Result} = util:tc(fun api_tx:req_list/1, [ReqList]),
    monitor:proc_set_value(
      ?MODULE, "read_read",
      fun(Old) ->
              Old2 = case Old of
                         % 1m monitoring interval, only keep newest
                         undefined -> rrd:create(60 * 1000000, 1, {timing, us});
                         _ -> Old
                     end,
              rrd:add_now(TimeInUs, Old2)
      end).

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message(), state()) -> state().
on({bench}, State) ->
    msg_delay:send_local(get_bench_interval(), self(), {bench}),
    run_bench(),
    State;

on({propagate}, State) ->
    msg_delay:send_local(get_gather_interval(), self(), {propagate}),
    DHT_Node = pid_groups:get_my(dht_node),
    comm:send_local(DHT_Node, {get_node_details, comm:this(), [my_range]}),
    State;

on({get_node_details_response, NodeDetails}, State) ->
    MyRange = node_details:get(NodeDetails, my_range),
    case is_leader(MyRange) of
        false -> State;
        _ ->
            % start a new timeslot and gather stats...
            {Round, Stats} = State,
            NewRound = Round + 1,
            Msg = {send_to_group_member, monitor_perf, {gather_stats, comm:this(), NewRound}},
            bulkowner:issue_bulk_owner(intervals:all(), Msg),
            NewStats = rrd:check_timeslot_now(Stats),
            broadcast_value(Stats, NewStats, Round),
            {NewRound, NewStats}
    end;

on({gather_stats, SourcePid, Round}, State) ->
    This = comm:this_with_cookie({SourcePid, Round}),
    comm:send_local(pid_groups:get_my(monitor),
                    {get_rrd, ?MODULE, "read_read", This}),
    State;

on({{get_rrd_response, _Process, _Key, undefined}, _Cookie}, State) ->
    State;

on({{get_rrd_response, _Process, _Key, DB}, {SourcePid, Round}}, State) ->
    DB2 = rrd:reduce_timeslots(1, DB),
    DataL = rrd:dump_with(DB2, fun(_DB, _From, _To, X) -> X end),
    case DataL of
        [Data] -> comm:send(SourcePid, {gather_stats_response, Round, Data});
        []     -> ok
    end,
    State;

on({gather_stats_response, Round, Data}, {Round, Stats}) ->
    Time = rrd:get_current_time(Stats),
    {Round, rrd:add_with(Time, Data, Stats, fun timing_update_fun/3)};
on({gather_stats_response, _Round, _Data}, State) ->
    State;

on({report_value, Round, Stats}, {OldRound, OldStats}) when OldRound < Round ->
    Time = rrd:get_current_time(Stats),
    DataL = rrd:dump_with(Stats, fun(_DB, _From, _To, X) -> X end),
    NewStats = case DataL of
        [Data] -> rrd:add_with(Time, Data, OldStats, fun timing_update_fun/3);
        []     -> OldStats
    end,
    {Round, NewStats};

on({report_value, _Round, _Stats}, State) ->
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the monitor process, registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, null,
                             [{pid_groups_join_as, DHTNodeGroup, monitor_perf}]).

%% @doc Initialises the module with an empty state.
-spec init(null) -> state().
init(null) ->
    FirstDelay = randoms:rand_uniform(1, get_bench_interval() + 1),
    msg_delay:send_local(FirstDelay, self(), {bench}),
    msg_delay:send_local(get_gather_interval(), self(), {propagate}),
    {0, rrd:create(get_gather_interval() * 1000000, 60, {timing, us})}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec timing_update_fun(Time::rrd:internal_time(), Old::rrd:timing_type(T) | undefined, New::rrd:timing_type(T))
        -> rrd:timing_type(T) when is_subtype(T, number()).
timing_update_fun(_Time, undefined, New) ->
    New;
timing_update_fun(_Time, {Sum, Sum2, Count, Min, Max, Hist},
                  {NewSum, NewSum2, NewCount, NewMin, NewMax, NewHist}) ->
    NewHist2 = lists:foldl(fun({V, C}, Acc) ->
                                  histogram:add(V, C, Acc)
                          end, Hist, histogram:get_data(NewHist)),
    {Sum + NewSum, Sum2 + NewSum2, Count + NewCount,
     erlang:min(Min, NewMin), erlang:max(Max, NewMax), NewHist2}.

broadcast_value(OldStats, Stats, OldRound) ->
    % broadcast the latest value only if a new time slot was started
    SlotOld = rrd:get_slot_start(0, OldStats),
    SlotNew = rrd:get_slot_start(0, Stats),
    case SlotNew of
        SlotOld -> ok; %nothing to do
        _  -> % new slot -> broadcast latest value only:
            DB2 = rrd:reduce_timeslots(1, OldStats),
            Msg = {send_to_group_member, monitor_perf, {report_value, OldRound, DB2}},
            bulkowner:issue_bulk_owner(intervals:all(), Msg)
    end.

%% @doc Checks whether the node is the current leader.
-spec is_leader(MyRange::intervals:interval()) -> boolean().
is_leader(MyRange) ->
    intervals:in(?RT:hash_key("0"), MyRange).

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(monitor_perf_interval) and
    config:is_greater_than(monitor_perf_interval, 0).

-spec get_bench_interval() -> pos_integer().
get_bench_interval() ->
    config:read(monitor_perf_interval).

%% @doc Gets the interval of executing a broadcast gathering all nodes' stats
%%      (in seconds).
-spec get_gather_interval() -> pos_integer().
get_gather_interval() -> 60.
