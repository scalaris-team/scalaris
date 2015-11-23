%  @copyright 2011-2015 Zuse Institute Berlin

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
-vsn('$Id$').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

% monitor process functions
-export([start_link/1, init/1, on/2, check_config/0]).

-include("gen_component.hrl").

-record(state,
        {id        = ?required(state, id)        :: uid:global_uid(),
         perf_rr   = ?required(state, perf_rr)   :: rrd:rrd(),
         perf_lh   = ?required(state, perf_lh)   :: rrd:rrd(),
         perf_tx   = ?required(state, perf_tx)   :: rrd:rrd()
        }).

-type state() :: {AllNodes::#state{}, CollectingAtLeader::#state{}, BenchPid::pid() | ok, IgnoreBenchTimeout::boolean()}.
-type message() ::
    {bench} |
    {bench_result, Time::erlang_timestamp(), TimeInMs::non_neg_integer()} |
    {bench_timeout, Time::erlang_timestamp(), BenchPid::pid()} |
    {collect_system_stats} |
    {propagate} |
    {get_node_details_response, node_details:node_details()} |
    {bulkowner, deliver, Id::uid:global_uid(), Range::intervals:interval(), {gather_stats, SourcePid::comm:mypid(), Id::uid:global_uid()}, Parents::[comm:mypid(),...]} |
    {bulkowner, gather, Id::uid:global_uid(), Target::comm:mypid(), Msgs::[comm:message(),...], Parents::[comm:mypid()]} |
    {bulkowner, reply, Id::uid:global_uid(), {gather_stats_response, Id::uid:global_uid(), [{Process::atom(), Key::monitor:key(), Data::rrd:timing_type()}]}} |
    {bulkowner, deliver, Id::uid:global_uid(), Range::intervals:interval(), {report_value, StatsOneRound::#state{}}, Parents::[comm:mypid(),...]}.

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).

%% @doc Creates a monitoring value for benchmarks with a 1m monitoring interval
%%      and a timing histogram, only keeping the newest value.
-spec init_bench() -> ok.
init_bench() ->
    monitor:proc_set_value(
      ?MODULE, 'read_read', rrd:create(60 * 1000000, 1, {timing_with_hist, ms})).

-spec bench_service(Owner::pid()) -> ok.
bench_service(Owner) ->
    % do not use gen_component:monitor/1 since this is not a gen_component
    % -> tolerates that this side channel will not be traced by
    %    trace_mpath/proto_sched if the monitor dies (it should not die unless
    %    shutting down the VM anyway!)
    erlang:monitor(process, Owner),
    bench_service_loop(Owner).

-spec bench_service_loop(Owner::comm:erl_local_pid()) -> ok.
bench_service_loop(Owner) ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({bench}, %% ->
            begin
                Key1 = randoms:getRandomString(),
                Key2 = randoms:getRandomString(),
                ReqList = [{read, Key1}, {read, Key2}, {commit}],
                Time = os:timestamp(),
                {TimeInUs, _Result} = util:tc(fun api_tx:req_list/1, [ReqList]),
                comm:send_local(Owner, {bench_result, Time, TimeInUs / 1000}),
                bench_service_loop(Owner)
            end);
        ?SCALARIS_RECV({tx_tm_rtm_commit_reply, _, _}, %% ->
            % left-over commit information from bench, more specifically api_tx:req_list/1
            bench_service_loop(Owner));
        {'DOWN', _MonitorRef, process, Owner, _Info1} -> ok
    end.

-spec init_system_stats() -> ok.
init_system_stats() ->
    % system stats in 10s intervals:
    monitor:monitor_set_value(
      ?MODULE, 'mem_total', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'mem_processes', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'mem_system', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'mem_atom', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'mem_binary', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'mem_ets', rrd:create(15 * 1000000, 1, gauge)),

    monitor:monitor_set_value(
      ?MODULE, 'rcv_count', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'rcv_bytes', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'send_count', rrd:create(15 * 1000000, 1, gauge)),
    monitor:monitor_set_value(
      ?MODULE, 'send_bytes', rrd:create(15 * 1000000, 1, gauge)),

    collect_system_stats().

-spec collect_system_stats() -> ok.
collect_system_stats() ->
    [{total, MemTotal}, {processes, MemProcs}, {system, MemSys},
     {atom, MemAtom}, {binary, MemBin}, {ets, MemEts}] =
        erlang:memory([total, processes, system, atom, binary, ets]),

    monitor:monitor_set_value(?MODULE, 'mem_total',
                           fun(Old) -> rrd:add_now(MemTotal, Old) end),
    monitor:monitor_set_value(?MODULE, 'mem_processes',
                           fun(Old) -> rrd:add_now(MemProcs, Old) end),
    monitor:monitor_set_value(?MODULE, 'mem_system',
                           fun(Old) -> rrd:add_now(MemSys, Old) end),
    monitor:monitor_set_value(?MODULE, 'mem_atom',
                           fun(Old) -> rrd:add_now(MemAtom, Old) end),
    monitor:monitor_set_value(?MODULE, 'mem_binary',
                           fun(Old) -> rrd:add_now(MemBin, Old) end),
    monitor:monitor_set_value(?MODULE, 'mem_ets',
                           fun(Old) -> rrd:add_now(MemEts, Old) end),

    {RcvCnt, RcvBytes, SendCnt, SendBytes} = comm_stats:get_stats(),
    monitor:monitor_set_value(?MODULE, 'rcv_count',
                           fun(Old) -> rrd:add_now(RcvCnt, Old) end),
    monitor:monitor_set_value(?MODULE, 'rcv_bytes',
                           fun(Old) -> rrd:add_now(RcvBytes, Old) end),
    monitor:monitor_set_value(?MODULE, 'send_count',
                           fun(Old) -> rrd:add_now(SendCnt, Old) end),
    monitor:monitor_set_value(?MODULE, 'send_bytes',
                           fun(Old) -> rrd:add_now(SendBytes, Old) end).

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message(), state()) -> state().
on({bench} = Msg, {AllNodes, Leader, BenchPid, _IgnBenchT} = _State) ->
    ?TRACE1(Msg, _State),
    % periodic task to execute a mini-benchmark (in a separate process)
    % (result will be send in a 'bench_result' message)
    case get_bench_interval() of
        0 -> ok;
        I -> msg_delay:send_trigger(I, {bench}),
             comm:send_local(BenchPid, Msg),
             %% send a timeout so that a hanging bench service gets re-started
             msg_delay:send_trigger(get_bench_timeout_interval(), {bench_timeout, os:timestamp(), BenchPid})
    end,
    {AllNodes, Leader, BenchPid, false};

on({bench_result, Time, TimeInMs} = _Msg,
   {AllNodes, Leader, BenchPid, _IgnBenchT} = _State) ->
    ?TRACE1(_Msg, _State),
    % result from the mini-benchmark triggered by the {bench} message
    monitor:proc_set_value(?MODULE, 'read_read',
                           fun(Old) -> rrd:add(Time, TimeInMs, Old) end),
    % ignore the bench_timeout message that will follow
    {AllNodes, Leader, BenchPid, true};

on({bench_timeout, Time, BenchPid} = _Msg,
   {AllNodes, Leader, BenchPid, false} = _State) ->
    ?TRACE1(_Msg, _State),
    % the bench service could not reply within get_bench_timeout_interval() seconds
    % -> re-start service (it may hang) and assume this interval as the reported time
    erlang:exit(BenchPid, kill),
    monitor:proc_set_value(
      ?MODULE, 'read_read',
      fun(Old) -> rrd:add(Time, get_bench_timeout_interval() * 1000, Old) end),
    Self = self(),
    NewBenchPid = erlang:spawn(fun() -> bench_service(Self) end),
    {AllNodes, Leader, NewBenchPid, false};

on({bench_timeout, _Time, _BenchPid} = _Msg, State) ->
    %?TRACE1(_Msg, State),
    % old or ignored timeout message
    State;

on({collect_system_stats} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % collect system stats, e.g. memory info, and send to monitor
    msg_delay:send_trigger(10, {collect_system_stats}),
    collect_system_stats(),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gather global performance data with bulkowner
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({propagate} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % identify whether a DHT leader node is present in this VM
    msg_delay:send_trigger(get_gather_interval(), {propagate}),
    Msg = {get_node_details, comm:this(), [my_range]},
    _ = [comm:send_local(Pid, Msg) || Pid <- pid_groups:find_all(dht_node)],
    State;

on({get_node_details_response, NodeDetails} = _Msg,
   {AllNodes, Leader, BenchPid, IgnBenchT} = State) ->
    ?TRACE1(_Msg, State),
    % response to {propagate} from all local DHT nodes
    % if any of them is a leader node, start propagation with bulkowner
    case is_leader(node_details:get(NodeDetails, my_range)) of
        false -> State;
        _ ->
            % start a new timeslot and gather stats...
            NewId = uid:get_global_uid(),
            BMsg = {?send_to_registered_proc, monitor_perf, {gather_stats, comm:this()}},
            bulkowner:issue_bulk_owner(NewId, intervals:all(), BMsg),
            % create a new timeslot if required
            Leader1 = check_timeslots(Leader),
            % broadcast values from the previous timeslot if a new one was started
            broadcast_previous_values(Leader, Leader1),
            {AllNodes, Leader1#state{id = NewId}, BenchPid, IgnBenchT}
    end;

on({bulkowner, deliver, Id, _Range, {gather_stats, SourcePid}, Parents} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % retrieve stats for gather_stats
    MyMonitor = pid_groups:get_my(monitor), % "basic_services" group
    [Val_RR] = monitor:get_rrds(MyMonitor, [{?MODULE, 'read_read'}]),
    ClientMonitor = pid_groups:pid_of(clients_group, monitor),
    [Val_TX] = monitor:get_rrds(ClientMonitor, [{api_tx, 'req_list'}]),
    % no need to reduce the multiple LH values - the next gather handler will do that
    DB_LH = [begin
                 [Val_LH] = monitor:get_rrds(Monitor, [{dht_node, 'lookup_hops'}]),
                 Val_LH
             end || Monitor <- pid_groups:find_all(monitor),
                    Monitor =/= MyMonitor,
                    Monitor =/= ClientMonitor],
    case process_rrds([Val_RR, Val_TX | DB_LH ]) of
        [] -> ok;
        AllData ->
            ReplyMsg = {?send_to_registered_proc, monitor_perf, {gather_stats_response, AllData}},
            bulkowner:issue_send_reply(Id, SourcePid, ReplyMsg, Parents)
    end,
    State;

on({bulkowner, gather, Id, Target, Msgs, Parents} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % gather replies from bulkowner_deliver with gather_stats
    {PerfRR, PerfLH, PerfTX} =
        lists:foldl(
             fun({gather_stats_response, Data1}, {PerfRR1, PerfLH1, PerfTX1}) ->
                     lists:foldl(
                       fun({?MODULE, 'read_read', PerfRR3}, {PerfRR2, PerfLH2, PerfTX2}) ->
                               {rrd:timing_with_hist_merge_fun(0, PerfRR2, PerfRR3), PerfLH2, PerfTX2};
                          ({dht_node, 'lookup_hops', PerfLH3}, {PerfRR2, PerfLH2, PerfTX2}) ->
                               {PerfRR2, rrd:timing_with_hist_merge_fun(0, PerfLH2, PerfLH3), PerfTX2};
                          ({api_tx, 'req_list', PerfTX3}, {PerfRR2, PerfLH2, PerfTX2}) ->
                               {PerfRR2, PerfLH2, rrd:timing_with_hist_merge_fun(0, PerfTX2, PerfTX3)}
                       end, {PerfRR1, PerfLH1, PerfTX1}, Data1)
             end, {undefined, undefined, undefined}, Msgs),

    Msg = {?send_to_registered_proc, monitor_perf,
           {gather_stats_response, [{?MODULE, 'read_read', PerfRR},
                                    {dht_node, 'lookup_hops', PerfLH},
                                    {api_tx, 'req_list', PerfTX}]}},
    bulkowner:send_reply(Id, Target, Msg, Parents, self()),
    State;

on({send_error, FailedTarget, {bulkowner, reply, Id, Target, BMsg, Parents}, _Reason} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % if sending the reply from bulkowner gather fails
    bulkowner:send_reply_failed(Id, Target, BMsg, Parents, self(), FailedTarget),
    State;

on({send_error, FailedTarget,
    {?send_to_registered_proc, monitor_perf,
     {bulkowner, reply, Id, Target, BMsg, Parents}}, Reason} = _Msg, State) ->
    %% redirect to other send_error
    gen_component:post_op({send_error, FailedTarget, {bulkowner, reply, Id, Target, BMsg, Parents}, Reason}, State);

on({bulkowner, reply, Id, {gather_stats_response, DataL}} = _Msg,
   {AllNodes, Leader, BenchPid, IgnBenchT} = _State)
  when Id =:= Leader#state.id ->
    ?TRACE1(_Msg, _State),
    % final aggregation of gather_stats at the leader
    Leader1 =
        lists:foldl(
          fun({?MODULE, 'read_read', PerfRR}, A = #state{perf_rr = DB}) ->
                  T = rrd:get_current_time(DB),
                  A#state{perf_rr = rrd:add_with(T, PerfRR, DB, fun rrd:timing_with_hist_merge_fun/3)};
             ({dht_node, 'lookup_hops', PerfLH}, A = #state{perf_lh = DB}) ->
                  T = rrd:get_current_time(DB),
                  A#state{perf_lh = rrd:add_with(T, PerfLH, DB, fun rrd:timing_with_hist_merge_fun/3)};
             ({api_tx, 'req_list', PerfTX}, A = #state{perf_tx = DB}) ->
                  T = rrd:get_current_time(DB),
                  A#state{perf_tx = rrd:add_with(T, PerfTX, DB, fun rrd:timing_with_hist_merge_fun/3)}
          end, Leader, DataL),
    {AllNodes, Leader1, BenchPid, IgnBenchT};
on({bulkowner, reply, _Id, {gather_stats_response, _Data}} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % final aggregation of gather_stats when not the leader or old ID -> ignore
    State;

on({bulkowner, deliver, Id, _Range, {report_value, _OtherState}, _Parents} = _Msg,
   {AllNodes, _Leader, _BenchPid, _IgnBenchT} = State)
  when Id =:= AllNodes#state.id ->
    % duplicate message
    State;

on({bulkowner, deliver, Id, _Range, {report_value, OtherState}, _Parents} = _Msg,
   {AllNodes, Leader, BenchPid, IgnBenchT} = State) ->
    % if the leader has not changed, only accept newer GUIDs
    % (there could be multiple deliver messages if more than one dht_node exist in this VM)
    case (not uid:from_same_pid(Id, AllNodes#state.id)) orelse
             uid:is_old_uid(AllNodes#state.id, Id) of
        true ->
            ?TRACE1(_Msg, State),
            % integrate value send via broadcast from leader
            #state{perf_rr = Perf_RR, perf_lh = Perf_LH, perf_tx = Perf_TX} =
                AllNodes1 = integrate_values(AllNodes, OtherState),
            % send to the own monitor
            monitor:monitor_set_value(?MODULE,  'agg_read_read', Perf_RR),
            monitor:monitor_set_value(dht_node, 'agg_lookup_hops', Perf_LH),
            monitor:monitor_set_value(api_tx,   'agg_req_list', Perf_TX),
            {AllNodes1#state{id = Id}, Leader, BenchPid, IgnBenchT};
        _ ->
            % drop old or duplicate message
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% misc.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({web_debug_info, Requestor} = _Msg,
   {AllNodes, Leader, _BenchPid, _IgnBenchT} = State) ->
    ?TRACE1(_Msg, State),
    [KVAllNodes, KVLeader] =
        [begin
             PerfRR5 = rrd:reduce_timeslots(5, Data#state.perf_rr),
             PerfLH5 = rrd:reduce_timeslots(5, Data#state.perf_lh),
             PerfTX5 = rrd:reduce_timeslots(5, Data#state.perf_tx),
             [monitor:web_debug_info_merge_values({?MODULE, perf_rr}, PerfRR5),
              monitor:web_debug_info_merge_values({dht_node, perf_lh}, PerfLH5),
              monitor:web_debug_info_merge_values({api_tx, perf_tx}, PerfTX5)]
         end || Data <- [AllNodes, Leader]],
    KeyValueList = lists:flatten([{"all nodes:", ""}, KVAllNodes,
                                  {"leader:",    ""}, KVLeader]),
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the monitor process, registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, null,
                             [{erlang_register, ?MODULE},
                              {pid_groups_join_as, DHTNodeGroup, monitor_perf}]).

%% @doc Initialises the module with an empty state.
-spec init(null) -> state().
init(null) ->
    BenchPid =
        case get_bench_interval() of
            0 -> ok;
            I -> FirstDelay = randoms:rand_uniform(1, I + 1),
                 msg_delay:send_trigger(FirstDelay, {bench}),
                 msg_delay:send_trigger(get_gather_interval(), {propagate}),
                 init_bench(),
                 Self = self(),
                 erlang:spawn(fun() -> bench_service(Self) end)
        end,
    init_system_stats(),
    msg_delay:send_trigger(10, {collect_system_stats}),
    Now = os:timestamp(),
    Perf_RR = rrd:create(get_gather_interval() * 1000000, 60, {timing_with_hist, ms}, Now),
    Perf_LH = rrd:create(get_gather_interval() * 1000000, 60, {timing, count}, Now),
    Perf_TX = rrd:create(get_gather_interval() * 1000000, 60, {timing_with_hist, ms}, Now),
    State = #state{id = uid:get_global_uid(),
                   perf_rr = Perf_RR, perf_lh = Perf_LH, perf_tx = Perf_TX},
    monitor:monitor_set_value(?MODULE,  'agg_read_read', Perf_RR),
    monitor:monitor_set_value(dht_node, 'agg_lookup_hops', Perf_LH),
    monitor:monitor_set_value(api_tx,   'agg_req_list', Perf_TX),
    {State, State, BenchPid, false}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec check_timeslots(#state{}) -> #state{}.
check_timeslots(State = #state{perf_rr = PerfRR, perf_lh = PerfLH, perf_tx = PerfTX}) ->
    State#state{perf_rr = rrd:check_timeslot_now(PerfRR),
                perf_lh = rrd:check_timeslot_now(PerfLH),
                perf_tx = rrd:check_timeslot_now(PerfTX)}.

-spec broadcast_previous_values(OldState::#state{}, NewState::#state{}) -> ok.
broadcast_previous_values(OldState, NewState) ->
    % broadcast the latest value only if a new time slot was started
    PerfRRNewSlot = rrd:get_slot_start(0, OldState#state.perf_rr) =/=
                        rrd:get_slot_start(0, NewState#state.perf_rr),
    PerfLHNewSlot = rrd:get_slot_start(0, OldState#state.perf_lh) =/=
                        rrd:get_slot_start(0, NewState#state.perf_lh),
    PerfTXNewSlot = rrd:get_slot_start(0, OldState#state.perf_tx) =/=
                        rrd:get_slot_start(0, NewState#state.perf_tx),
    if PerfRRNewSlot orelse PerfLHNewSlot orelse PerfTXNewSlot ->
           % new slot -> broadcast latest values only:
           SendState = reduce_timeslots(1, OldState),
           Msg = {?send_to_registered_proc, monitor_perf, {report_value, SendState}},
           bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), Msg);
       true -> ok % nothing to do
    end.

-spec reduce_timeslots(N::pos_integer(), State::#state{}) -> #state{}.
reduce_timeslots(N, State) ->
    State#state{perf_rr = rrd:reduce_timeslots(N, State#state.perf_rr),
                perf_lh = rrd:reduce_timeslots(N, State#state.perf_lh),
                perf_tx = rrd:reduce_timeslots(N, State#state.perf_tx)}.

%% @doc Integrates values from OtherState by merging all rrd records into MyState.
-spec integrate_values(MyState::#state{}, OtherState::#state{}) -> #state{}.
integrate_values(MyState, OtherState) ->
    MyPerfRR1 = rrd:merge(MyState#state.perf_rr, OtherState#state.perf_rr),
    MyPerfLH1 = rrd:merge(MyState#state.perf_lh, OtherState#state.perf_lh),
    MyPerfTX1 = rrd:merge(MyState#state.perf_tx, OtherState#state.perf_tx),
    MyState#state{id = OtherState#state.id,
                  perf_rr = MyPerfRR1, perf_lh = MyPerfLH1, perf_tx = MyPerfTX1}.

%% @doc Checks whether the node is the current leader.
-spec is_leader(MyRange::intervals:interval()) -> boolean().
is_leader(MyRange) ->
    intervals:in(?RT:hash_key("0"), MyRange).

%% @doc For each rrd in the given list, accumulate all values in our time span
%%      into a single timing type value.
-spec process_rrds(DBs::[{Process::atom(), Key::monitor:key(), DB::rrd:rrd() | undefined}]) ->
          [{Process::atom(), Key::monitor:key(), Data::rrd:timing_type()}].
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
                   Data = lists:foldl(fun(E, A) -> rrd:timing_with_hist_merge_fun(0, A, E) end, H, T),
                   {Process, Key, Data}
           end
       end || {Process, Key, DB} <- DBs, DB =/= undefined]).

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(monitor_perf_interval) and
    config:cfg_is_greater_than_equal(monitor_perf_interval, 0).

-spec get_bench_interval() -> non_neg_integer().
get_bench_interval() ->
    config:read(monitor_perf_interval).

%% @doc Timeout interval (in seconds) for a bench request.
%%      NOTE: this must be smaller than get_bench_interval()!
-spec get_bench_timeout_interval() -> non_neg_integer().
get_bench_timeout_interval() ->
    get_bench_interval() div 2.

%% @doc Gets the interval of executing a broadcast gathering all nodes' stats
%%      (in seconds).
-spec get_gather_interval() -> pos_integer().
get_gather_interval() -> 60.
