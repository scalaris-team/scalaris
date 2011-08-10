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

-include("record_helpers.hrl").
-include("scalaris.hrl").

% monitor process functions
-export([start_link/1, init/1, on/2, check_config/0]).

-record(state,
        {id      = ?required(state, id)      :: util:global_uid(),
         perf_rr = ?required(state, perf_rr) :: rrd:rrd(),
         perf_lh = ?required(state, perf_lh) :: rrd:rrd()
        }).

-type state() :: {AllNodes::#state{}, CollectingAtLeader::#state{}}.
-type message() ::
    {bench} |
    {propagate} |
    {get_node_details_response, node_details:node_details()} |
    {gather_stats, SourcePid::comm:mypid(), Id::util:global_uid()} |
    {{get_rrds_response, [{Process::atom(), Key::string(), DB::rrd:rrd() | undefined}]}, {SourcePid::comm:mypid(), Id::util:global_uid()}} |
    {gather_stats_response, Id::util:global_uid(), [{Process::atom(), Key::string(), Data::rrd:timing_type(number())}]} |
    {report_value, StatsOneRound::#state{}} |
    {get_rrds, [{Process::atom(), Key::string()},...], SourcePid::comm:mypid()}.

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).

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
                         undefined -> rrd:create(60 * 1000000, 1, {timing, ms});
                         _ -> Old
                     end,
              rrd:add_now(TimeInUs / 1000, Old2)
      end).

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message(), state()) -> state().
on({bench} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    msg_delay:send_local(get_bench_interval(), self(), {bench}),
    run_bench(),
    State;

on({propagate} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    msg_delay:send_local(get_gather_interval(), self(), {propagate}),
    DHT_Node = pid_groups:get_my(dht_node),
    comm:send_local(DHT_Node, {get_node_details, comm:this(), [my_range]}),
    State;

on({get_node_details_response, NodeDetails} = _Msg, {AllNodes, Leader} = State) ->
    ?TRACE1(_Msg, State),
    case is_leader(node_details:get(NodeDetails, my_range)) of
        false -> State;
        _ ->
            % start a new timeslot and gather stats...
            NewId = util:get_global_uid(),
            Msg = {send_to_group_member, monitor_perf, {gather_stats, comm:this(), NewId}},
            bulkowner:issue_bulk_owner(intervals:all(), Msg),
            Leader1 = check_timeslots(Leader),
            broadcast_values(Leader, Leader1),
            {AllNodes, Leader1#state{id = NewId}}
    end;

on({gather_stats, SourcePid, Id} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    This = comm:this_with_cookie({SourcePid, Id}),
    comm:send_local(pid_groups:get_my(monitor),
                    {get_rrds, [{?MODULE, "read_read"}, {dht_node, "lookup_hops"}], This}),
    State;

on({{get_rrds_response, DBs}, {SourcePid, Id}} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    DataL = process_rrds(DBs),
    case DataL of
        [] -> ok;
        _  -> comm:send(SourcePid, {gather_stats_response, Id, DataL})
    end,
    State;

on({gather_stats_response, Id, DataL} = _Msg, {AllNodes, Leader} = _State)
  when Id =:= Leader#state.id ->
    ?TRACE1(_Msg, _State),
    Leader1 =
        lists:foldl(
          fun(Data, A) ->
                  case Data of
                      {?MODULE, "read_read", PerfRR} ->
                          DB = A#state.perf_rr,
                          T = rrd:get_current_time(DB),
                          A#state{perf_rr = rrd:add_with(T, PerfRR, DB, fun timing_update_fun/3)};
                      {dht_node, "lookup_hops", PerfLH} ->
                          DB = A#state.perf_lh,
                          T = rrd:get_current_time(DB),
                          A#state{perf_lh = rrd:add_with(T, PerfLH, DB, fun timing_update_fun/3)}
                  end
          end, Leader, DataL),
    {AllNodes, Leader1};
on({gather_stats_response, _Id, _Data} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    State;

on({report_value, OtherState} = _Msg, {AllNodes, Leader} = _State) ->
    ?TRACE1(_Msg, _State),
    AllNodes1 = integrate_values(OtherState, AllNodes),
    {AllNodes1, Leader};

on({get_rrds, KeyList, SourcePid}, {AllNodes, _Leader} = State) ->
    MyData = lists:flatten(
               [begin
                    Value = case FullKey of
                                {?MODULE, "read_read"} ->
                                    AllNodes#state.perf_rr;
                                {dht_node, "lookup_hops"} ->
                                    AllNodes#state.perf_lh;
                                _ -> undefined
                            end,
                    {Process, Key, Value}
                end || {Process, Key} = FullKey <- KeyList]),
    comm:send(SourcePid, {get_rrds_response, MyData}),
    State;

on({web_debug_info, Requestor} = _Msg, {AllNodes, Leader} = State) ->
    ?TRACE1(_Msg, _State),
    KeyValueList =
        [{"all nodes", lists:flatten(io_lib:format("~p", [AllNodes]))},
         {"leader",    lists:flatten(io_lib:format("~p", [Leader]))}],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
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
    Now = os:timestamp(),
    State = #state{id = util:get_global_uid(),
                   perf_rr = rrd:create(get_gather_interval() * 1000000, 60, {timing, ms}, Now),
                   perf_lh = rrd:create(get_gather_interval() * 1000000, 60, {timing, count}, Now)},
    {State, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec timing_update_fun(Time::rrd:internal_time(), Old::rrd:timing_type(T) | undefined, New::rrd:timing_type(T) | undefined)
        -> rrd:timing_type(T) | undefined when is_subtype(T, number()).
timing_update_fun(_Time, undefined, New) ->
    New;
timing_update_fun(_Time, Old, undefined) ->
    Old;
timing_update_fun(_Time, {Sum, Sum2, Count, Min, Max, Hist},
                  {NewSum, NewSum2, NewCount, NewMin, NewMax, NewHist}) ->
    NewHist2 = lists:foldl(fun({V, C}, Acc) ->
                                  histogram:add(V, C, Acc)
                          end, Hist, histogram:get_data(NewHist)),
    {Sum + NewSum, Sum2 + NewSum2, Count + NewCount,
     erlang:min(Min, NewMin), erlang:max(Max, NewMax), NewHist2}.

-spec check_timeslots(#state{}) -> #state{}.
check_timeslots(State = #state{perf_rr = PerfRR, perf_lh = PerfLH}) ->
    State#state{perf_rr = rrd:check_timeslot_now(PerfRR),
                perf_lh = rrd:check_timeslot_now(PerfLH)}.

-spec broadcast_values(OldState::#state{}, NewState::#state{}) -> ok.
broadcast_values(OldState, NewState) ->
    % broadcast the latest value only if a new time slot was started
    OldPerfRR = OldState#state.perf_rr,
    NewPerfRR = NewState#state.perf_rr,
    OldPerfLH = OldState#state.perf_lh,
    NewPerfLH = NewState#state.perf_lh,
    case rrd:get_slot_start(0, OldPerfRR) =/= rrd:get_slot_start(0, NewPerfRR) orelse
             rrd:get_slot_start(0, OldPerfLH) =/= rrd:get_slot_start(0, NewPerfLH) of
        true -> % new slot -> broadcast latest values only:
            SendState = reduce_timeslots(1, OldState),
            Msg = {send_to_group_member, monitor_perf, {report_value, SendState}},
            bulkowner:issue_bulk_owner(intervals:all(), Msg);
        _ -> ok %nothing to do
    end.

-spec reduce_timeslots(N::pos_integer(), State::#state{}) -> #state{}.
reduce_timeslots(N, State) ->
    State#state{perf_rr = rrd:reduce_timeslots(N, State#state.perf_rr),
                perf_lh = rrd:reduce_timeslots(N, State#state.perf_lh)}.

-spec integrate_values(OtherState::#state{}, MyState::#state{}) -> #state{}.
integrate_values(OtherState, MyState) ->
    MyPerfRR1 = integrate_value(OtherState#state.perf_rr, MyState#state.perf_rr),
    MyPerfLH1 = integrate_value(OtherState#state.perf_lh, MyState#state.perf_lh),
    MyState#state{id = OtherState#state.id,
                  perf_rr = MyPerfRR1, perf_lh = MyPerfLH1}.

-spec integrate_value(OtherDB::rrd:rrd(), MyDB::rrd:rrd()) -> rrd:rrd().
integrate_value(OtherDB, MyDB) ->
    DataL = rrd:dump_with(OtherDB, fun(_DB, _From, _To, X) -> X end),
    case DataL of
        [Data] -> 
            Time = rrd:get_current_time(OtherDB),
            rrd:add_with(Time, Data, MyDB, fun timing_update_fun/3);
        []     -> MyDB
    end.

%% @doc Checks whether the node is the current leader.
-spec is_leader(MyRange::intervals:interval()) -> boolean().
is_leader(MyRange) ->
    intervals:in(?RT:hash_key("0"), MyRange).

-spec process_rrds(DBs::[{Process::atom(), Key::string(), DB::rrd:rrd() | undefined}]) ->
          [{Process::atom(), Key::string(), Data::rrd:timing_type(number())}].
process_rrds(DBs) ->
    lists:flatten(
      [begin
           % DB slot length may be different -> try to gather data from our full time span:
           Slots = erlang:max(1, (get_gather_interval() * 1000000) div rrd:get_slot_length(DB)),
           DB2 = rrd:reduce_timeslots(Slots, DB),
           DBDump = rrd:dump_with(DB2, fun(_DB, _From, _To, X) -> X end),
           case DBDump of
               []      -> [];
               [H | T] ->
                   Data = lists:foldl(fun(E, A) -> timing_update_fun(0, A, E) end, H, T),
                   {Process, Key, Data}
           end
       end || {Process, Key, DB} <- DBs, DB =/= undefined]).

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
