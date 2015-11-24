% @copyright 2007-2015 Zuse Institute Berlin

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

%% @author Marie Hoffmann <hoffmann@zib.de>
%% @doc Ganglia monitoring interface.
%% @end
%% @version $Id$
-module(ganglia).
-author('hoffmann@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1, init/1, on/2,
         check_config/0]).

-include("gen_component.hrl").

-define(TRACE(X,Y), ok).
%define(TRACE(X,Y), io:format(X, Y)).

-define(FD_SUBSCR_PID(AggId),
        comm:reply_as(self(), 3, {fd, AggId, '_'})).

-type load_aggregation() :: {AggId :: non_neg_integer(),
                             Pending :: non_neg_integer(),
                             Load :: non_neg_integer()}.

-type state() :: {LastUpdated :: non_neg_integer(),
                  LoadAggregation :: load_aggregation()}.

-type message() :: {ganglia_trigger} |
                   {ganglia_periodic} |
                   {ganglia_dht_load_aggregation, DHTNode :: pid(), AggId :: non_neg_integer(), message()} |
                   {ganglia_vivaldi_confidence, pid(), message()} |
                   {fd_notify, fd:event(), PID::comm:mypid(), _Cookie::{ganglia, AggId::non_neg_integer()}, Reason::fd:reason()}.

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, ServiceGroup, ganglia}]).

-spec init([]) -> state().
init([]) ->
    UpdateInterval = config:read(ganglia_interval) div 1000,
    msg_delay:send_trigger(UpdateInterval, {ganglia_trigger}),
    {_LastActive = 0, {0, 0, 0}}.

-spec on(message(), state()) -> state().
on({ganglia_trigger}, State) ->
    UpdateInterval = config:read(ganglia_interval) div 1000,
    msg_delay:send_trigger(UpdateInterval, {ganglia_trigger}),
    StateNew = inc_agg_id(State),
    gen_component:post_op(_Msg = {ganglia_periodic}, StateNew);

on({ganglia_periodic}, State) ->
    NewState = send_dht_node_metrics(State),
    send_general_metrics(),
    send_message_metrics(),
    send_rrd_metrics(),
    send_vivaldi_errors(),
    set_last_active(NewState);

%% @doc aggregates load information from the dht nodes and
%%     and sends it out to ganglia if all nodes have replied
on({ganglia_dht_load_aggregation, PID, AggId, Msg}, State) ->
    ?TRACE("~p: ~p~n", [AggId, Msg]),
    % all went well, we no longer need the failure detector
    fd:unsubscribe(?FD_SUBSCR_PID(AggId), [PID]),
    {get_node_details_response, [{load, Load}]} = Msg,
    CurAggId = get_agg_id(State),
    if
        %% check for correct aggregation id
        AggId == CurAggId  ->
            NewState = set_agg_load(Load, State),
            agg_check_pending(NewState);
        %% otherwise drop this message
        true -> State
    end;

%% @doc handler for messages from failure detector
%%     if a node crashes before sending out the load data
%%     we ignore its load information
on({fd, AggId, {fd_notify, crash, _PID, _Reason}}, State) ->
    ?TRACE("Node failed~n",[]),
    CurAggId = get_agg_id(State),
    if
        AggId == CurAggId ->
            Pending = get_agg_pending(State),
            set_agg_pending(Pending - 1, State);
        true ->
            State
    end;
on({fd, _AggId, {fd_notify, _Event, _PID, jump}}, State) ->
    State;

%% @doc handler for messages from the vivaldi process
%%     reporting its confidence
on({ganglia_vivaldi_confidence, DHTName, Msg}, State) ->
    {vivaldi_get_coordinate_response, _, Confidence} = Msg,
    _ = gmetric(both, lists:flatten(io_lib:format("vivaldi_confidence_~s", [DHTName])), "float", Confidence, "Confidence"),
    State.

-spec send_general_metrics() -> ok.
send_general_metrics() ->
    % general erlang status information
    _ = gmetric(both, "Erlang Processes", "int32", erlang:system_info(process_count), "Total Number"),
    _ = gmetric(both, "Memory used by Erlang processes", "int32", erlang:memory(processes_used), "Bytes"),
    _ = gmetric(both, "Memory used by ETS tables", "int32", erlang:memory(ets), "Bytes"),
    _ = gmetric(both, "Memory used by atoms", "int32", erlang:memory(atom), "Bytes"),
    _ = gmetric(both, "Memory used by binaries", "int32", erlang:memory(binary), "Bytes"),
    _ = gmetric(both, "Memory used by system", "int32", erlang:memory(system), "Bytes"),
    ok.

%%@doc aggregate the number of key-value pairs
%%     and the amount of memory for this VM
-spec send_dht_node_metrics(state()) -> state().
send_dht_node_metrics(State) ->
    DHTNodes = pid_groups:find_all(dht_node),
    AggId = get_agg_id(State),
    %% Memory Usage of DHT Nodes
    MemoryUsage = lists:sum([element(2, erlang:process_info(P, memory))
                             || P <- DHTNodes]),
    _ = gmetric(both, "Memory used by dht_nodes", "int32", MemoryUsage, "Bytes"),
    %% Query DHT Nodes for load, let them reply to the on handler ganglia_dht_node_reply
    %% The FD tells us if a node is down
    lists:foreach(fun (PID) ->
                          GlobalPID = comm:make_global(PID),
                          %% Let the failure detector inform ganglia about crashes of this DHT node
                          fd:subscribe(?FD_SUBSCR_PID(AggId), [GlobalPID]),
                          Envelope = comm:reply_as(comm:this(), 4,
                                                   {ganglia_dht_load_aggregation, GlobalPID, AggId , '_'}),
                          comm:send_local(PID, {get_node_details, Envelope, [load]})
                  end, DHTNodes),
    set_agg_pending(length(DHTNodes), State).

-spec send_message_metrics() -> ok.
send_message_metrics() ->
    {Received, Sent, _Time} = comm_logger:dump(),
    traverse(received, gb_trees:iterator(Received)),
    traverse(sent, gb_trees:iterator(Sent)),
    ok.

%% @doc Sends latency and transactions/s rrd data from the monitor to Ganglia
-spec send_rrd_metrics() -> ok.
send_rrd_metrics() ->
    case pid_groups:pid_of(clients_group, monitor) of
        failed -> ok;
        ClientMonitor ->
            RRDMetrics = case monitor:get_rrds(ClientMonitor, [{api_tx, 'req_list'}]) of
                             [{_,_, undefined}] -> [];
                             [{_, _, RRD}] ->
                                 case rrd:dump(RRD) of
                                     [H | _] ->
                                         {From_, To_, Value} = H,
                                         Diff_in_s = timer:now_diff(To_, From_) div 1000000,
                                         {Sum, _Sum2, Count, _Min, _Max, _Hist} = Value,
                                         AvgPerS = Count / Diff_in_s,
                                         Avg = Sum / Count,
                                         [{both, "tx latency", "float", Avg, "ms"},
                                          {both, "transactions/s", "float", AvgPerS, "1/s"}];
                                     _ -> []
                                 end
                         end,
            gmetric(RRDMetrics)
    end,
    ok.

-spec send_vivaldi_errors() -> ok.
send_vivaldi_errors() ->
    DHTNodeGroups = case pid_groups:groups_with(dht_node) of
                        failed ->
                            [];
                        X -> X
                    end,
    lists:foreach(fun (Group) ->
                          PID = pid_groups:pid_of(Group, gossip),
                          Envelope = comm:reply_as(comm:this(), 3,
                                                   {ganglia_vivaldi_confidence, Group, '_'}),
                          comm:send_local(PID, {cb_msg, {gossip_vivaldi, default}, {get_coordinate, Envelope}})
                  end, DHTNodeGroups),
    ok.


%%%%%%%%%%%%
%% Helpers %
%%%%%%%%%%%%

-spec gmetric(list()) -> ok.
gmetric(MetricsList) ->
    lists:foreach(fun({Slope, Metric, Type, Value, Unit}) ->
                          gmetric(Slope, Metric, Type, Value, Unit)
                  end, MetricsList).

-spec gmetric(Slope::both | positive, Metric::string(), Type::string(), Value::number(), Unit::string()) -> string().
gmetric(Slope, Metric, Type, Value, Unit) ->
    Cmd = lists:flatten(io_lib:format("gmetric --slope ~p --name ~p --type ~p --value ~p --units ~p~n",
                                      [Slope, Metric, Type, Value, Unit])),
    Res = os:cmd(Cmd),
    ?TRACE("~s: ~s~n", [Cmd, Res]),
    Res.

-spec traverse(received | sent, Iter1::term()) -> ok.
traverse(RcvSnd, Iter1) ->
  case gb_trees:next(Iter1) of
    none -> ok;
    {Key, {Bytes, _Count}, Iter2} ->
          _ = gmetric(positive, lists:flatten(io_lib:format("~s ~p", [RcvSnd, Key])), "int32", Bytes, "Bytes"),
          traverse(RcvSnd, Iter2)
  end.

-spec agg_check_pending(state()) -> state().
agg_check_pending(State) ->
    Pending = get_agg_pending(State),
    if
        Pending == 0 ->
            Load = get_agg_load(State),
            _ = gmetric(both, "kv pairs", "int32", Load, "pairs"),
            State;
        true ->
            State
    end.


%% -spec get_last_active(state()) -> non_neg_integer().
%% get_last_active(State) ->
%%     element(1, State).

-spec set_last_active(state()) -> state().
set_last_active(State) ->
    {_Megasecs, Secs, _MicroSecs} = os:timestamp(),
    setelement(1, State, Secs).

-spec get_load_agg(state()) -> load_aggregation().
get_load_agg(State) ->
    element(2, State).

-spec set_load_agg(state(), load_aggregation()) -> state().
set_load_agg(State, LoadAgg) ->
    setelement(2, State, LoadAgg).

-spec get_agg_id(state()) -> non_neg_integer().
get_agg_id(State) ->
    element(1, get_load_agg(State)).

-spec get_agg_load(state()) -> non_neg_integer().
get_agg_load(State) ->
    {_AggId, _Pending, Load} = get_load_agg(State),
    Load.

-spec set_agg_load(non_neg_integer(), state()) -> state().
set_agg_load(Load, State) ->
    {AggId, Pending, OldLoad} = get_load_agg(State),
    set_load_agg(State, {AggId, Pending - 1, OldLoad + Load}).

-spec get_agg_pending(state()) -> non_neg_integer().
get_agg_pending(State) ->
    {_AggId, Pending, _Load} = get_load_agg(State),
    Pending.

-spec set_agg_pending(non_neg_integer(), state()) -> state().
set_agg_pending(NumPending, State) ->
    {AggId, _Pending, Load} = get_load_agg(State),
    set_load_agg(State, {AggId, NumPending, Load}).

-spec inc_agg_id(state()) -> state().
inc_agg_id(State) ->
    {AggId, _Pending, _Load} = get_load_agg(State),
    set_load_agg(State, {AggId + 1, 0, 0}).

%% @doc Checks whether config parameters of the ganglia process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(ganglia_interval) and
    config:cfg_is_greater_than(ganglia_interval, 0).
