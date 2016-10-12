%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%  @copyright 2008-2015 Zuse Institute Berlin

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
%% @doc Gossip based aggregation of load information.
%%      This module implements the symmetric push-sum protocol. The algorithm is
%%      used to compute aggregates of the load information, which is measured as
%%      the count of items currently in a node's key range. <br/>
%%      The aggregation of load information is used in Scalaris for two purposes:
%%      First, for passive load balancing. When a node  joins, the gossiped load
%%      information is used to decide where to place the new node. The node will
%%      be placed so that the standard deviation of the load is reduced the most.
%%      Second, the gossiping is used for system monitoring. The local estimates
%%      of the gossiping can be viewed for example in the Web Interface of every
%%      Scalaris node. <br/>
%%      Different metrics are computed on the load information:
%%      <ul>
%%          <li> average load, the arithmetic mean of all nodes load information </li>
%%          <li> the maximum load </li>
%%          <li> the minimum load </li>
%%          <li> standard deviation of the average load </li>
%%          <li> leader based size, based on counting </li>
%%          <li> key range bases size, calculated as address space of keys / average key range per node </li>
%%          <li> histogram of load per key range (load measured as number of items per node) </li>
%%      </ul>
%%      The module is initialised during the startup of the gossiping framework,
%%      continuously aggregating load information in the background. Additionally,
%%      is it possible to start instances of the module for the purpose of computing
%%      different sizes histograms, request_histogram/1.
%% @end
%% @version $Id$
-module(gossip_load).
-behaviour(gossip_beh).
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

% API
-export([get_values_best/1, request_histogram/2, load_info_get/2, load_info_other_get/3]).

% gossip_beh
-export([init/1, check_config/0, trigger_interval/0, fanout/0,
        select_node/1, select_data/1, select_reply_data/4, integrate_data/3,
        handle_msg/2, notify_change/3, min_cycles_per_round/0, max_cycles_per_round/0,
        round_has_converged/1, web_debug_info/1, shutdown/1]).

%% for testing
-export([tester_create_round/1, tester_create_state/11, tester_create_histogram/1,
         tester_create_load_data_list/1, is_histogram/1,
         tester_create_histogram_size/1, init_feeder/1, get_values_best_feeder/1]).

-export_type([state/0, histogram/0, histogram_size/0, bucket/0]). %% for config gossip_load_*.erl
-export_type([avg/0, avg_kr/0, min/0, max/0]). %% for config gossip_load_*.erl
-export_type([load_info/0, load_info_other/0, merged/0]).

%% -define(SHOW, config:read(log_level)).
-define(SHOW, debug).

-define(DEFAULT_MODULE, gossip_load_default).

%% List of module names for additional aggregation
%% (Modules need to implement gossip_load_beh)
-define(ADDITIONAL_MODULES, [lb_active_gossip_load_metric, lb_active_gossip_request_metric]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state_key() :: convergence_count | instance | leader | load_data | ring_data | merged |
    no_of_buckets | prev_state | range | request | requestor | round | status.
-type avg() :: {Value::float(), Weight::float()}.
-type avg_kr() :: {Value::number(), Weight::float()}.
-type min() :: non_neg_integer().
-type max() :: non_neg_integer().
-type merged() :: non_neg_integer().
-type bucket() :: {Interval::intervals:interval(), Avg::avg()|unknown}.
-type histogram() :: [bucket()].
-type histogram_size() :: pos_integer().
-type instance() :: {Module :: gossip_load, Id :: atom() | uid:global_uid()}.
-type status() :: init | uninit.
-type round() :: non_neg_integer().

%% Record for ring related gossipping data
-record(ring_data, {
            size_inv  = unknown :: unknown | avg(), % 1 / size
            avg_kr    = unknown :: unknown | avg_kr() % average key range (distance between nodes in the address space)})
    }).

-type ring_data_uninit() :: #ring_data{}.
-type ring_data() :: {ring_data, SizeInv::avg(), AvgKr::avg_kr()}.
-type ring_data2() :: ring_data_uninit() | ring_data().

%% record of load data values for gossiping
-record(load_data, {
            name      = ?required(name, load_data) :: atom(), %% unique name
            avg       = unknown :: unknown | avg(), % average load
            avg2      = unknown :: unknown | avg(), % average of load^2
            min       = unknown :: unknown | min(),
            max       = unknown :: unknown | max(),
            histo     = unknown :: unknown | histogram()
    }).

-type load_data_uninit() :: #load_data{}.
-type load_data_skipped() :: {load_data, atom(), skip}.
-type load_data() :: {load_data, Name::atom(), Avg::avg(), Avg2::avg(),
                      Min::non_neg_integer(), Max::non_neg_integer(),
                      Histogram::histogram()}.

%% -type load_data_list() :: [ load_data_uninit() | load_data(), ...].
-type load_data_list() :: [load_data() | load_data_skipped(), ...].
-type load_data_list2() :: [load_data_uninit() | load_data_skipped() | load_data(), ...].

-type data() :: {load_data_list(), ring_data()}.

%% state record
-record(state, {
        status              = uninit :: status(),
        instance            = ?required(state, instance) :: instance(),
        load_data_list      = ?required(state, load_data_list2) :: load_data_list2(),
        ring_data           = ?required(state, ring_data2) :: ring_data2(),
        leader              = unknown :: unknown | boolean(),
        range               = unknown :: unknown | intervals:interval(),
        request             = false :: boolean(),
        requestor           = none :: none | comm:mypid(),
        no_of_buckets       = ?required(state, no_of_buckets) :: non_neg_integer(),
        round               = 0 :: round(),
        merged              = 0 :: non_neg_integer(),
        convergence_count   = 0 :: non_neg_integer()
    }).

-type state() :: #state{}.
-type full_state() :: {PrevState :: unknown | state(), CurrentState :: state()}.

-record(load_info_other, { %% additional gossip values
            name    = ?required(name, load_info_other) :: atom(),
            avg     = unknown :: unknown | float(),  % average load
            stddev  = unknown :: unknown | float(), % standard deviation of the load
            min     = unknown :: unknown | min(), % minimum load
            max     = unknown :: unknown | max() % maximum load
    }).

-type(load_info_other() :: #load_info_other{}).

% record of load data values for use by other modules
-record(load_info, { %% general info and default gossip values
            avg       = unknown :: unknown | float(),  % average load
            stddev    = unknown :: unknown | float(), % standard deviation of the load
            size_ldr  = unknown :: unknown | float(), % estimated ring size: 1/(average of leader=1, others=0)
            size_kr   = unknown :: unknown | float(), % estimated ring size based on average key ranges
            min       = unknown :: unknown | min(), % minimum load
            max       = unknown :: unknown | max(), % maximum load
            merged    = unknown :: unknown | merged(), % how often the data was merged since the node entered/created the round
            other     = []      :: [load_info_other()]
    }).

-opaque(load_info() :: #load_info{}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Config Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%--------- External config function (called by bh module) ---------%%

%% @doc The time interval in ms after which a new cycle is trigger by the behaviour
%%      module.
-spec trigger_interval() -> pos_integer().
trigger_interval() -> % in ms
    config:read(gossip_load_interval).


%% @doc The fanout (number of peers contacted per cycle).
-spec fanout() -> pos_integer().
fanout() ->
    config:read(gossip_load_fanout).


%% @doc The minimum number of cycles per round.
%%      Only full cycles (i.e. received replies) are counted (ignored triggers
%%      do not count as cycle).
%%      Only relevant for leader, all other nodes enter rounds when told to do so.
-spec min_cycles_per_round() -> non_neg_integer().
min_cycles_per_round() ->
    config:read(gossip_load_min_cycles_per_round).


%% @doc The maximum number of cycles per round.
%%      Only full cycles (i.e. received replies) are counted (ignored triggers
%%      do not count as cycle).
%%      Only relevant for leader, all other nodes enter rounds when told to do so.
-spec max_cycles_per_round() -> pos_integer().
max_cycles_per_round() ->
    config:read(gossip_load_max_cycles_per_round).


%%------------------- Private config functions ---------------------%%

-spec convergence_count_best_values() -> pos_integer().
convergence_count_best_values() ->
% convergence is counted (on average) twice every cycle:
% 1) When receiving the answer to an request (integrate_data()) (this happens
%       once every cycle).
% 2) When receiving a request (select_reply_data()) (this happens randomly,
%       but *on average* once per round).
    config:read(gossip_load_convergence_count_best_values).


-spec convergence_count_new_round() -> pos_integer().
convergence_count_new_round() ->
    config:read(gossip_load_convergence_count_new_round).

-spec convergence_epsilon() -> float().
convergence_epsilon() ->
    config:read(gossip_load_convergence_epsilon).

-spec no_of_buckets() -> histogram_size().
no_of_buckets() ->
    config:read(gossip_load_number_of_buckets).

-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(gossip_load_interval) and
    config:cfg_is_greater_than(gossip_load_interval, 0) and

    config:cfg_is_integer(gossip_load_min_cycles_per_round) and
    config:cfg_is_greater_than_equal(gossip_load_min_cycles_per_round, 0) and

    config:cfg_is_integer(gossip_load_max_cycles_per_round) and
    config:cfg_is_greater_than_equal(gossip_load_max_cycles_per_round, 1) and

    config:cfg_is_float(gossip_load_convergence_epsilon) and
    config:cfg_is_in_range(gossip_load_convergence_epsilon, 0.0, 100.0) and

    config:cfg_is_integer(gossip_load_convergence_count_best_values) and
    config:cfg_is_greater_than(gossip_load_convergence_count_best_values, 0) and

    config:cfg_is_integer(gossip_load_convergence_count_new_round) and
    config:cfg_is_greater_than(gossip_load_convergence_count_new_round, 0) and

    config:cfg_is_integer(gossip_load_number_of_buckets) and
    config:cfg_is_greater_than(gossip_load_number_of_buckets, 0) and

    config:cfg_is_integer(gossip_load_fanout) and
    config:cfg_is_greater_than(gossip_load_fanout, 0) and

    config:cfg_is_list(gossip_load_additional_modules,
                       fun(El) -> lists:member(El, ?ADDITIONAL_MODULES) end,
                       "A valid additional gossip_load module").


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc Sends a (local) message to the gossip module of the requesting process'
%%      group asking for the best aggregation results.
%%      The response in the form {gossip_get_values_best_response, BestValues}
%%      will be send (local) to the requesting process.
%%      BestValues are either the aggregation restult from the current or previous round,
%%      depending on convergence_count_best_values.
%%      If the option {instance, Instance} is present, the Instance is used.
%%      Otherwise {gossip_load, default} is used as instance.
%%      If the option {source_pid, SourcePid} is present, response is sent to
%%      the SourcePid instead of the pid of the requesting process.
%%      If the option {delay, Seconds} is present, the request is delayed for (at
%%      least) Seconds seconds.
%%      If the option {send_after, Milliseconds} is present, the request is delayed
%%      for excatly Milliseconds milliseconds.
-spec get_values_best(Options::[proplists:property()]) -> ok.
get_values_best(Options) when is_list(Options) ->
    Instance = proplists:get_value(instance, Options, {gossip_load, default}),
    SourcePid = proplists:get_value(source_pid, Options, self()),
    Delay = proplists:get_value(msg_delay, Options, 0),
    SendAfter = proplists:get_value(send_after, Options, 0),
    Pid = pid_groups:get_my(gossip),
    Msg = {cb_msg, Instance, {gossip_get_values_best, SourcePid}},
    if Delay =:= 0 andalso SendAfter =:= 0 ->
           comm:send_local(Pid, Msg);
       Delay =:= 0 andalso SendAfter =/= 0 ->
           comm:send_local_after(SendAfter, Pid, Msg), ok;
       Delay =/= 0 ->
           msg_delay:send_local(Delay, Pid, Msg)
    end.


%% @doc Request a histogram with Size number of Buckets. <br/>
%%      The resulting histogram will be sent to SourceId, when all values have
%%      properly converged.
-spec request_histogram(Size::histogram_size(), SourcePid::comm:mypid()) -> ok.
request_histogram(Size, SourcePid) when Size >= 1 ->
    gossip:start_gossip_task(?MODULE, [{no_of_buckets, Size}, {requestor, SourcePid}]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initiate the gossip_load module. <br/>
%%      Instance (mandatory) makes the module aware of its own instance id, which
%%      is saved in the state of the module.
%%      NoOfBuckets (optional) defines the size of the histogram calculated.
%%      Requestor (optinal) defines to whom the calculated histogram will be sent
%%      (used for request_histogram/1, called through the gossip module).
-spec init(Args::[proplists:property()]) -> {ok, full_state()}.
init(Args) ->
    Instance = proplists:get_value(instance, Args),
    NoOfBuckets = proplists:get_value(no_of_buckets, Args, no_of_buckets()),
    Requestor = proplists:get_value(requestor, Args, none),
    log:log(debug, "[ ~w ] CBModule initiated. NoOfBuckets: ~w, Requestor: ~w",
        [Instance, NoOfBuckets, Requestor]),
    CurState = #state{
        load_data_list = load_data_list_new(),
        ring_data = ring_data_new(),
        no_of_buckets = NoOfBuckets,
        request = Requestor =/= none,
        requestor = Requestor,
        instance = Instance
    },
    {ok, {unknown, CurState}}.


%% @doc Returns false, i.e. peer selection is done by gossip module.
%%      State: the state of the gossip_load module
-spec select_node(FullState::full_state()) -> {boolean(), full_state()}.
select_node(FullState) ->
    {false, FullState}.

%% @doc Select and prepare the load information to be sent to the peer. <br/>
%%      Called by the gossip module at the beginning of every cycle. <br/>
%%      The selected exchange data is sent back to the gossip module as a message
%%      of the form {selected_data, Instance, ExchangeData}.
%%      State: the state of the gossip_load module
-spec select_data(FullState::full_state()) -> {ok, full_state()}.
select_data({PrevState, CurState}) ->
    log:log(debug, "[ ~w ] select_data: State: ~w", [state_get(instance, CurState), CurState]),
    CurState1 = case state_get(status, CurState) of
        uninit ->
            request_node_details(CurState), CurState;
        init ->
            {Data, NewCurState1} = prepare_data([], CurState),
            Pid = pid_groups:get_my(gossip),
            comm:send_local(Pid, {selected_data, state_get(instance, NewCurState1), Data}),
            NewCurState1
    end,
    {ok, {PrevState, CurState1}}.


%% @doc Process the data from the requestor and select reply data. <br/>
%%      Called by the behaviour module upon a p2p_exch message. <br/>
%%      PData: exchange data from the p2p_exch request <br/>
%%      Ref: used by the gossip module to identify the request <br/>
%%      RoundStatus / Round: round information used for special handling of
%%          messages from previous rounds <br/>
%%      State: the state of the gossip_load module
-spec select_reply_data(PData::data(), Ref::pos_integer(), Round::round(),
    FullState::full_state()) -> {discard_msg | ok | retry | send_back, full_state()}.
select_reply_data(PData, Ref, Round, {PrevState, CurState}) ->

    CurRound = state_get(round, CurState),
    PrevRound = state_get(round, PrevState),

    case state_get(status, CurState) of
        uninit ->
            log:log(?SHOW, "[ ~w ] select_reply_data in uninit", [?MODULE]),
            {retry,{PrevState, CurState}};
        init when Round =:= CurRound ->
            CurState1 = select_reply_data_helper(PData, Ref, Round, CurState),
            {ok, {PrevState, CurState1}};
        init when Round =:= PrevRound ->
            PrevState1 = select_reply_data_helper(PData, Ref, Round, PrevState),
            {ok, {PrevState1, CurState}};
        _ ->
            log:log(warn(), "[ ~w ] Discarded data in select_reply_data. Reason: invalid round.",
                [state_get(instance, CurState)]),
            {send_back, {PrevState, CurState}}
    end.


%% @doc Helper for select_reply_data()
-spec select_reply_data_helper(PData::data(), Ref::pos_integer(), round(),
                               state()) -> state().
select_reply_data_helper(PData, Ref, Round, State) ->
    PLoadList = element(1, PData),
    PSkipped = [Module || {load_data, Module, skip} <- PLoadList],
    {Data1, State1} = prepare_data(PSkipped, State),
    Data2 = {replace_skipped(PLoadList, element(1, Data1)),
             element(2, Data1)},
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {selected_reply_data, state_get(instance, State1),
                          Data2, Ref, Round}),
    {_Data2, State2} = merge_load_data(PData, State1),
    State2.


%% @doc Integrate the reply data. <br/>
%%      Called by the behaviour module upon a p2p_exch_reply message. <br/>
%%      QData: the reply data from the peer <br/>
%%      RoundStatus / Round: round information used for special handling of
%%          messages from previous rounds <br/>
%%      State: the state of the gossip_load module <br/>
%%      Upon finishing the processing of the data, a message of the form
%%      {integrated_data, Instance, RoundStatus} is to be sent to the gossip module.
-spec integrate_data(QData::data(), Round::round(), FullState::full_state()) ->
    {discard_msg | ok | retry | send_back, full_state()}.
integrate_data(QData, Round, {PrevState, CurState}=FullState) ->
    log:log(debug, "[ ~w ] Reply-Data: ~w~n", [state_get(instance, CurState), QData]),

    CurRound = state_get(round, CurState),
    PrevRound = state_get(round, PrevState),

    case state_get(status, CurState) of
        uninit ->
            log:log(?SHOW, "[ ~w ] integrate_data in uninit", [?MODULE]),
            {retry, FullState};
        init when Round =:= CurRound ->
            CurState1 = element(2, merge_load_data(QData, CurState)),
            log:log(info, "Load-Info: ~s", [to_string(get_load_info(CurState1))]),
            comm:send_local(pid_groups:get_my(gossip),
                            {integrated_data, state_get(instance, CurState1), cur_round}),
            {ok, {PrevState, CurState1}};
        init when Round =:= PrevRound ->
            PrevState1 = element(2, merge_load_data(QData, PrevState)),
            log:log(info, "Load-Info: ~s", [to_string(get_load_info(PrevState1))]),
            comm:send_local(pid_groups:get_my(gossip),
                            {integrated_data, state_get(instance, PrevState1), prev_round}),
            {ok, {PrevState1, CurState}};
        _ ->
            log:log(warn(), "[ ~w ] Discarded data in integrate_data. Reason: invalid round.", [?MODULE]),
            {send_back, FullState}
    end.


%% @doc Handle get_state_response messages from the dht_node. <br/>
%%      The received load information is stored and the status is set to init,
%%      allowing the start of a new gossip round.
%%      State: the state of the gossip_load module <br/>
-spec handle_msg(Message::{get_node_details_response, node_details:node_details()},
    FullState::full_state()) -> {ok, full_state()}.
handle_msg({get_node_details_response, NodeDetails}, {PrevState, CurState}) ->

    LoadDataNew =
        [ begin
              Module = data_get(name, LoadData),
              Load = Module:get_load(NodeDetails),
              case Load of
                  unknown -> {load_data, Module, skip};
                  Load ->
                      log:log(?SHOW, "[ ~w ] Load: ~w", [state_get(instance, CurState), Load]),
                      LoadData1 = data_set(avg, {float(Load), 1.0}, LoadData),
                      LoadData2 = data_set(min, Load, LoadData1),
                      LoadData3 = data_set(max, Load, LoadData2),
                      LoadData4 = data_set(avg2, {float(Load*Load), 1.0}, LoadData3),
                      % histogram
                      Histo = init_histo(Module, NodeDetails, CurState),
                      log:log(?SHOW,"[ ~w ] Histo: ~s", [state_get(instance, CurState), to_string(Histo)]),
                      _LoadData5 = data_set(histo, Histo, LoadData4)
              end
          end
        || LoadData <- state_get(load_data_list, CurState)],

    CurState1 = state_set(load_data_list, LoadDataNew, CurState),

    RingData = state_get(ring_data, CurState1),
    RingData1 = case (state_get(leader, CurState1)) of
                    true ->
                        data_set(size_inv, {1.0, 1.0}, RingData);
                    false ->
                        data_set(size_inv, {1.0, 0.0}, RingData)
                end,
    AvgKr = calc_initial_avg_kr(state_get(range, CurState1)),
    RingData2 = data_set(avg_kr, AvgKr, RingData1),
    CurState2 = state_set(ring_data, RingData2, CurState1),

    {NewData, CurState3} = prepare_data([], CurState2),
    CurState4 = state_set(status, init, CurState3),

    % send PData to BHModule
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {selected_data, state_get(instance, CurState4), NewData}),
    {ok, {PrevState, CurState4}};

handle_msg({gossip_get_values_best, SourcePid}, {PrevState, CurState}) ->
    BestState = previous_or_current(PrevState, CurState),
    BestValues = get_load_info(BestState),
    comm:send_local(SourcePid, {gossip_get_values_best_response, BestValues}),
    {ok, {PrevState, CurState}};

handle_msg({gossip_get_values_all, SourcePid}, {unknown, CurState}) ->
    CurInfo = get_load_info(CurState),
    AllValues = {#load_info{}, CurInfo, CurInfo},
    comm:send_local(SourcePid, {gossip_get_values_all_response, AllValues}),
    {ok, {unknown, CurState}};

handle_msg({gossip_get_values_all, SourcePid}, {PrevState, CurState}) ->
    BestState = previous_or_current(PrevState, CurState),
    AllValues = {get_load_info(PrevState), get_load_info(CurState),
                  get_load_info(BestState)},
    comm:send_local(SourcePid, {gossip_get_values_all_response, AllValues}),
    {ok, {PrevState, CurState}}.


%% @doc Checks if the current round has converged yet <br/>
%%      Returns true if the round has converged, false otherwise.
-spec round_has_converged(FullState::full_state()) -> {boolean(), full_state()}.
round_has_converged({_PrevState, CurState}=FullState) ->
    ConvergenceCount = convergence_count_new_round(),
    {has_converged(ConvergenceCount, CurState), FullState}.


%% @doc Notifies the gossip_load module about changes. <br/>
%%      Changes can be one of the following:
%%      <ol>
%%          <li> new_round <br/>
%%               Notifies the the callback module about the beginning of round </li>
%%          <li> leader <br/>
%%               Notifies the the callback module about a change in the key range
%%               of the node. The MsgTag indicates whether the node is a leader
%%               or not, the NewRange is the new key range of the node. </li>
%%          <li> exch_failure <br/>
%%               Notifies the the callback module about a failed message delivery,
%%               including the exchange data and round from the original message. </li>
%%      </ol>
-spec notify_change(Keyword::new_round, NewRound::round(), FullState::full_state()) -> {ok, full_state()};
    (Keyword::leader, {MsgTag::is_leader | no_leader, NewRange::intervals:interval()},
            FullState::full_state()) -> {ok, full_state()};
    (Keyword::exch_failure, {_MsgTag::atom(), Data::data(), _Round::round()},
             FullState::full_state()) -> {ok, full_state()}.
notify_change(new_round, NewRound, {PrevState, CurState}) ->
    log:log(debug, "[ ~w ~w ] new_round notification. NewRound: ~w. Leader: ~w",
            [state_get(instance, CurState), comm:this(), NewRound, state_get(leader, CurState)]),
    case state_get(request, CurState) of
        true ->
          {ok, {PrevState, finish_request(CurState)}};
        false ->
          new_round(NewRound, PrevState, CurState)
    end;

notify_change(leader, {MsgTag, NewRange}, {PrevState, CurState}) when MsgTag =:= is_leader ->
    log:log(?SHOW, "[ ~w ] ~w is the leader", [state_get(instance, CurState), comm:this()]),
    CurState1 = state_set([{leader, true}, {range, NewRange}], CurState),
    {ok, {PrevState, CurState1}};

notify_change(leader, {MsgTag, NewRange}, {PrevState, CurState}) when MsgTag =:= no_leader ->
    log:log(?SHOW, "[ ~w ] ~w is no leader", [state_get(instance, CurState), comm:this()]),
    CurState1 = state_set([{leader, false}, {range, NewRange}], CurState),
    {ok, {PrevState, CurState1}};


notify_change(exch_failure, {_MsgTag, Data, Round}, {PrevState, CurState} = FullState) ->
    CurRound = state_get(round, CurState),
    PrevRound = state_get(round, PrevState),
    FullState1 =
        case {Round, Data} of
            {CurRound, undefined} ->
                FullState;
            {CurRound, _} ->
                log:log(debug, " [ ~w ] exch_failure in valid current round", [?MODULE]),
                {_NewData, CurState1} = merge_failed_exch_data(Data, CurState),
                {PrevState, CurState1};
            {PrevRound, undefined} ->
                FullState;
            {PrevRound, _} ->
                log:log(debug, " [ ~w ] exch_failure in valid old round", [?MODULE]),
                {_NewData, PrevState1} = merge_failed_exch_data(Data, PrevState),
                {PrevState1, CurState};
            _ ->
                log:log(warn(), " [ ~w ] exch_failure in invalid round", [?MODULE]),
                FullState
        end,
    {ok, FullState1}.



%% @doc Returns a key-value list of debug infos for the Web Interface. <br/>
%%      Called by the gossip module upon {web_debug_info} messages.
%%      State: the state of the gossip_load module.
-spec web_debug_info(full_state()) ->
    {KeyValueList::[{Key::string(), Value::any()},...], full_state()}.
web_debug_info({PrevState, CurState}=FullState) ->
    PreviousLoadData = state_get(load_data_list, PrevState),
    CurrentLoadData = state_get(load_data_list, CurState),
    PreviousRingData = state_get(ring_data, PrevState),
    CurrentRingData = state_get(ring_data, CurState),
    BestState = previous_or_current(PrevState, CurState),
    Best = case BestState of
               CurState -> current_data;
               PrevState -> previous_data
           end,
    KeyValueList =
        [{to_string(state_get(instance, CurState)), ""},
         {"best",                Best},
         {"leader",              state_get(leader, CurState)},
         %% previous round
         {"prev_round",          state_get(round, PrevState)},
         {"prev_merged",         state_get(merged, PrevState)},
         {"prev_conv_avg_count", state_get(convergence_count, PrevState)},
         {"prev_size_ldr",       get_current_estimate(size_inv, PreviousRingData)},
         {"prev_size_kr",        calc_size_kr(PreviousRingData)}
        ]
        ++
            lists:flatten(
          [ begin
                [
                 {"prev_name",           data_get(name, LoadData)},
                 {"prev_avg",            get_current_estimate(avg, LoadData)},
                 {"prev_min",            data_get(min, LoadData)},
                 {"prev_max",            data_get(max, LoadData)},
                 {"prev_stddev",         calc_stddev(LoadData)},
                 {"prev_histo",          to_string(data_get(histo, LoadData))}
                ]
            end
            || PreviousLoadData =/= unknown, LoadData <- PreviousLoadData])
        ++
        [
         %% current round
         {"cur_round",           state_get(round, CurState)},
         {"cur_merged",          state_get(merged, CurState)},
         {"cur_conv_avg_count",  state_get(convergence_count, CurState)},
         {"cur_size_ldr",        get_current_estimate(size_inv, CurrentRingData)},
         {"cur_size_kr",         calc_size_kr(CurrentRingData)}
        ]
        ++
            lists:flatten(
          [ begin
                [
                 {"cur_name",           data_get(name, LoadData)},
                 {"cur_avg",            get_current_estimate(avg, LoadData)},
                 {"cur_min",            data_get(min, LoadData)},
                 {"cur_max",            data_get(max, LoadData)},
                 {"cur_stddev",         calc_stddev(LoadData)},
                 {"cur_histo",          to_string(data_get(histo, LoadData))}
                ]
            end
            || LoadData <- CurrentLoadData]),
    {KeyValueList, FullState}.


%% @doc Shutd down the gossip_load module. <br/>
%%      Called by the gossip module upon stop_gossip_task(CBModule).
-spec shutdown(State::full_state()) -> {ok, shutdown}.
shutdown(_FullState) ->
    % nothing to do
    {ok, shutdown}.


%%------------------- Callback Functions: Helpers ------------------%%

-spec request_node_details(State::state()) -> ok.
request_node_details(State) ->
    % get state of dht node
    case pid_groups:get_my(dht_node) of
        failed ->
            % our dht_node died and was removed from pid_groups
            log:log(warn, "[ ~w ~w ] request_node_details from ~p failed",
                   [state_get(instance, State), comm:this(), pid_groups:my_groupname()]),
            ok;
        DHT_Node ->
            EnvPid = comm:reply_as(comm:this(), 3, {cb_msg, state_get(instance, State), '_'}),
            comm:send_local(DHT_Node,
                            {get_node_details, EnvPid, [load, load2, load3, db, my_range]})
    end.


-spec new_round(NewRound::round(), PrevState::state(), CurState::state())
        -> {ok, FullState::full_state()}.
new_round(NewRound, PrevState, CurState) ->
    % Only replace prev round with current round if current has converged.
    % Cases in which current round has not converged: e.g. late joining, sleeped/paused.
    % ConvergenceTarget should be less than covergence_count_new_round,
    % otherwise non leader groups seldom replace prev_load_data
    {PrevState1, CurState1} = case has_converged(convergence_count_best_values(), CurState) of
        true ->
            % make the current state the previous state
            NewState1 = state_new(CurState),
            {CurState, NewState1};
        false ->
            % make the old previous state the new previous state and discard the current state
            ConvCount = state_get(convergence_count, CurState),
            CurRound = state_get(round, CurState),
            log:log(warn(), "[ ~w ] Entering new round (~w), but current round (~w) "++
                "has not converged. Convergence Count: ~w", [?MODULE, NewRound, CurRound, ConvCount]),
            NewState1 = state_new(CurState),
            {PrevState, NewState1}
    end,
    CurState2 =  state_set(round, NewRound, CurState1),
    {ok, {PrevState1, CurState2}}.


-spec has_converged(TargetConvergenceCount::pos_integer(), CurState::state()) -> boolean().
has_converged(TargetConvergenceCount, CurState) ->
    CurrentConvergenceCount = state_get(convergence_count, CurState),
    CurrentConvergenceCount >= TargetConvergenceCount.


-spec finish_request(CurState::state()) -> state().
finish_request(CurState) ->
    case state_get(leader, CurState) of
        true ->
            LoadDataList = state_get(load_data_list, CurState),
            Histo = data_get(histo, get_default_load_data(LoadDataList)),
            Requestor = state_get(requestor, CurState),
            comm:send(Requestor, {histogram, Histo}),
            gossip:stop_gossip_task(state_get(instance, CurState));
        false ->
            do_nothing
    end,
    CurState.


-spec update_convergence_count(OldData::data(), NewData::data(), CurState::state()) -> state().
update_convergence_count({OldLoadList, OldRing}, {NewLoadList, NewRing}, CurState) ->

    % check whether all average based values changed less than epsilon percent
    AvgChangeEpsilon = convergence_epsilon(),
    HasConvergedFun =
        fun(AvgType, Old, New) ->
                OldValue = calc_current_estimate(data_get(AvgType, Old)),
                NewValue = calc_current_estimate(data_get(AvgType, New)),
                log:log(debug, "[ ~w ] ~w: OldValue: ~w, New Value: ~w",
                        [state_get(instance, CurState), AvgType, OldValue, NewValue]),
                calc_change(OldValue, NewValue) < AvgChangeEpsilon
        end,

    %% Ring convergence
    HaveConverged1 = HasConvergedFun(size_inv, OldRing, NewRing) andalso
                        HasConvergedFun(avg_kr, OldRing, NewRing),

    %% Load convergence
    HaveConverged2 =
        HaveConverged1 andalso
            lists:all(
              fun({OldLoad, NewLoad, AvgType}) ->
                      ?ASSERT(data_get(name, OldLoad) =:= data_get(name, NewLoad)),
                      HasConvergedFun(AvgType, OldLoad, NewLoad)
              end,
              [{OldLoad, NewLoad, AvgType}
               || {OldLoad, NewLoad} <- lists:zip(OldLoadList, NewLoadList),
                  % =/= {load_data, _Name, skip}
                  not (tuple_size(NewLoad) =:= 3 andalso element(3, NewLoad) =:= skip),
                  AvgType            <- [avg, avg2]
              ]),

    log:log(debug, "Averages have converged: ~w", [HaveConverged2]),

    %% TODO: Fix convergence counting for histograms with skipped values
    %% (atm, all histogrames are ignored in regard to convergence counting)

    %% HistoModulesConverged =
    %%     [ begin
    %%         case lists:member(data_get(name, NewLoad), Modules) of
    %%              true -> true;
    %%              false ->
    %%                   ?ASSERT(data_get(name, OldLoad) =:= data_get(name, NewLoad)),
    %%                   % Combine the avg values of the buckets of two histograms into one tuple list
    %%                   Combine = fun ({Interval, AvgOld}, {Interval, AvgNew}) -> {AvgOld, AvgNew} end,
    %%                   CompList = lists:zipwith(Combine,
    %%                                            data_get(histo, OldLoad), data_get(histo, NewLoad)),
    %%
    %%                   % check that all the buckets of histogram have changed less than epsilon percent
    %%                   Fun1 = fun({OldAvg, NewAvg}) ->
    %%                                  OldEstimate = calc_current_estimate(OldAvg),
    %%                                  NewEstimate = calc_current_estimate(NewAvg),
    %%                                  _HasConverged = calc_change(OldEstimate, NewEstimate) < AvgChangeEpsilon
    %%                          end,
    %%                   _HaveConverged2 = lists:all(Fun1, CompList)
    %%         end
    %%       end
    %%       || {OldLoad, NewLoad} <- lists:zip(OldLoadList, NewLoadList)
    %%     ],
    %%
    %% HaveConverged3 = HaveConverged2 andalso lists:all(fun(X) -> X end, HistoModulesConverged),
    HaveConverged3 = HaveConverged2,
    %% log:log(debug, "Histogram has converged: ~w", [self(), HaveConverged2]),

    if HaveConverged3 ->
           state_update(convergence_count, fun inc/1, CurState);
       true ->
           state_set(convergence_count, 0, CurState)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% State of gossip_load: Getters, Setters and Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [state_get/2, state_set/3, state_update/3]}).

%% @doc Create a new state with information from another state record.
%%      This is used to build a new state from the current state when a new round
%%      is entered. Some values (mainly counters) are resetted, others are taken
%%      from the Parent state.
-spec state_new(Parent::state()) -> state().
state_new(Parent) ->
    #state{
        load_data_list = load_data_list_new(),
        ring_data = ring_data_new(),
        leader = state_get(leader, Parent),
        range = state_get(range, Parent),
        no_of_buckets = state_get(no_of_buckets, Parent),
        request = state_get(request, Parent),
        requestor = state_get(requestor, Parent),
        instance = state_get(instance, Parent)
    }.


-spec state_set (status, status(), state()) -> state();
                (instance, instance(), state()) -> state();
                (load_data_list, load_data_list(), state()) -> state();
                (ring_data, ring_data(), state()) -> state();
                (leader, boolean(), state()) -> state();
                (range, intervals:interval(), state()) -> state();
                (request, boolean(), state()) -> state();
                (requestor, none | comm:mypid(), state()) -> state();
                (no_of_buckets, non_neg_integer(), state()) -> state();
                (round, round(), state()) -> state();
                (merged, non_neg_integer(), state()) -> state();
                (convergence_count, non_neg_integer(), state()) -> state().
state_set(Key, Value, State) when is_record(State, state) ->
    case Key of
        status -> State#state{status = Value};
        instance -> State#state{instance = Value};
        load_data_list -> State#state{load_data_list = Value};
        ring_data -> State#state{ring_data = Value};
        leader -> State#state{leader = Value};
        range -> State#state{range = Value};
        request -> State#state{request = Value};
        requestor -> State#state{requestor = Value};
        no_of_buckets -> State#state{no_of_buckets = Value};
        round -> State#state{round = Value};
        merged -> State#state{merged = Value};
        convergence_count -> State#state{convergence_count = Value}
    end.

-spec state_set([ {status, status()} | {instance, instance()} |
                  {load_data_list, load_data_list()} | {ring_data, ring_data()} |
                  {leader, boolean()} | {range, intervals:interval()} |
                  {request, boolean()} | {requestor, none | comm:mypid()} |
                  {no_of_buckets, non_neg_integer()} | {round, round()} |
                  {merged, non_neg_integer()} | {convergence_count, non_neg_integer()}, ...], State::state()) -> state().
state_set(KeyValueTupleList, State) when is_record(State, state) andalso is_list(KeyValueTupleList) ->
    Fun = fun ({Key, Value}, OldState) -> state_set(Key, Value, OldState) end,
    lists:foldl(Fun, State, KeyValueTupleList).

-spec state_get (any(), unknown) -> unknown;
                (status, state()) -> status();
                (instance, state()) -> instance();
                (load_data_list, state()) -> load_data_list();
                (ring_data, state()) -> ring_data();
                (leader, state()) -> boolean();
                (range, state()) -> intervals:interval();
                (request, state()) -> boolean();
                (requestor, state()) -> none | comm:mypid();
                (no_of_buckets, state()) -> non_neg_integer();
                (round, state()) -> non_neg_integer();
                (merged, state()) -> non_neg_integer();
                (convergence_count, state()) -> non_neg_integer().
state_get(_Key, unknown) -> unknown;
state_get(status, #state{status=Status}) -> Status;
state_get(instance, #state{instance=Instance}) -> Instance;
state_get(load_data_list, #state{load_data_list=LoadDataList}) -> LoadDataList;
state_get(ring_data, #state{ring_data=RingData}) -> RingData;
state_get(leader, #state{leader=Leader}) -> Leader;
state_get(range, #state{range=Range}) -> Range;
state_get(request, #state{request=Request}) -> Request;
state_get(requestor, #state{requestor=Requestor}) -> Requestor;
state_get(no_of_buckets, #state{no_of_buckets=NoOfBuckets}) -> NoOfBuckets;
state_get(round, #state{round=Round}) -> Round;
state_get(merged, #state{merged=Merged}) -> Merged;
state_get(convergence_count, #state{convergence_count=ConvergenceCount}) -> ConvergenceCount.


-spec state_update(Key::state_key(), Fun::fun(), State::state()) -> state().
state_update(Key, Fun, State) ->
    Value = state_get(Key, State),
    NewValue = apply(Fun, [Value]),
    state_set(Key, NewValue, State).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Load Data and Ring Data
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [load_data_new/1, load_data_list_new/0, ring_data_new/0,
                   divide2/2, merge_failed_exch_data/2, merge_load_data/3, merge_avg/3]}).

%% @doc creates a new load_data (for internal use by gossip_load.erl)
-spec load_data_new(Module::atom()) -> load_data_uninit().
load_data_new(Module) ->
    #load_data{name = Module}.

-spec load_data_list_new() -> [load_data_uninit(), ...].
load_data_list_new() ->
    AdditionalModules = config:read(gossip_load_additional_modules),
    Modules = [?DEFAULT_MODULE | AdditionalModules],
    [load_data_new(Module) || Module <- Modules].

-spec ring_data_new() -> ring_data_uninit().
ring_data_new() ->
    #ring_data{}.

-spec get_default_load_data(unknown) -> unknown;
                           (load_data_list()) -> load_data() | load_data_skipped().
get_default_load_data(unknown) -> unknown;
get_default_load_data(LoadDataList) ->
    %% name is the second element of record tuple
    case lists:keyfind(?DEFAULT_MODULE, 2, LoadDataList) of
        false    -> throw(default_load_module_not_available);
        LoadData -> LoadData
    end.

%% @doc Gets information from a load_data record.
%%      Allowed keys include:
%%      <ul>
%%        <li>avg = average load,</li>
%%        <li>min = minimum load,</li>
%%        <li>max = maximum load,</li>
%%        <li>avg2 = average of load^2,</li>
%%        <li>size_inv = 1 / size_ldr,</li>
%%        <li>avg_kr = average key range (distance between nodes in the address space),</li>
%%      </ul>
%%      See type spec for details on which keys are allowed on which records.
-spec data_get(any(), unknown) -> unknown;
             (name, load_data() | load_data_skipped()) -> atom();
             (avg, load_data()) -> avg();
             (avg2, load_data()) -> avg();
             (min, load_data()) -> min();
             (max, load_data()) -> max();
             (histo, load_data()) -> histogram();
             (size_inv, ring_data()) -> avg();
             (avg_kr, ring_data()) -> avg_kr();
             (avg | avg2 | min | max | histo | size_inv | avg_kr, load_data_skipped()) -> unknown.
data_get(_Key, unknown) -> unknown;
data_get(name, {load_data, Name, skip}) ->
    Name;
data_get(_Key, {load_data, _Module, skip}) -> unknown;
data_get(name, LoadData=#load_data{name = Name}) when is_record(LoadData, load_data) ->
    Name;
data_get(avg, #load_data{avg = Avg}) ->
    Avg;
data_get(avg2, #load_data{avg2 = Avg2}) ->
    Avg2;
data_get(min, #load_data{min = Min}) ->
    Min;
data_get(max, #load_data{max = Max}) ->
    Max;
data_get(histo, #load_data{histo = Histo}) ->
    Histo;
data_get(size_inv, #ring_data{size_inv = SizeInv}) ->
    SizeInv;
data_get(avg_kr, #ring_data{avg_kr = AvgKr}) ->
    AvgKr.

-spec get_current_estimate(Key::avg | avg2,
                           unknown | load_data() | load_data_skipped()) -> float() | unknown;
                          (Key::size_inv | avg_kr,
                           unknown | ring_data()) -> float() | unknown.
get_current_estimate(_Key, unknown) -> unknown;
get_current_estimate(_Key, {load_data, _Module, skip}) -> unknown;
get_current_estimate(avg, #load_data{avg=Avg}) ->
    calc_current_estimate(Avg);
get_current_estimate(avg2, #load_data{avg2=Avg2}) ->
    calc_current_estimate(Avg2);
get_current_estimate(size_inv, #ring_data{size_inv = SizeInv}) ->
    calc_current_estimate(SizeInv);
get_current_estimate(avg_kr, #ring_data{avg_kr = AvgKr}) ->
    calc_current_estimate(AvgKr).


%% @doc Sets information in a load_data record.
%%      Allowed keys are:
%%      <ul>
%%        <li>avg = average load,</li>
%%        <li>avg2 = average of load^2,</li>
%%        <li>size_inv = 1 / size_ldr,</li>
%%        <li>avg_kr = average key range (distance between nodes in the address space),</li>
%%        <li>min = minimum load,</li>
%%        <li>max = maximum load,</li>
%%      </ul>
-spec data_set(avg, avg(), load_data()) -> load_data();
              (min, min(), load_data()) -> load_data();
              (max, max(), load_data()) -> load_data();
              (avg2, avg(), load_data()) -> load_data();
              (histo, histogram(), load_data()) -> load_data();
              (size_inv, avg(), ring_data()) -> ring_data();
              (avg_kr, avg_kr(), ring_data()) -> ring_data().
data_set(Key, Value, LoadData) when is_record(LoadData, load_data) ->
    case Key of
        avg -> LoadData#load_data{avg = Value};
        avg2 -> LoadData#load_data{avg2 = Value};
        min -> LoadData#load_data{min = Value};
        max -> LoadData#load_data{max = Value};
        histo -> LoadData#load_data{histo = Value}
    end;
data_set(Key, Value, RingData) when is_record(RingData, ring_data) ->
    case Key of
         size_inv -> RingData#ring_data{size_inv = Value};
         avg_kr -> RingData#ring_data{avg_kr = Value}
    end.

%% @doc Prepares a load_data record for sending it to a peer and updates the
%%      load_data of self accordingly. Skips load data skipped in the given
%%      state or from the MoreSkippedModules list.
-spec prepare_data(MoreSkippedModules::[atom()], state())
        -> {{load_data_list(), ring_data()}, state()}.
prepare_data(MoreSkippedModules, State) ->
    LoadDataNew =
        [begin
             LoadName = data_get(name, LoadData),
             LoadSkipped = {load_data, LoadName, skip},
             case LoadData =:= LoadSkipped orelse
                      lists:member(LoadName, MoreSkippedModules) of
                  true -> LoadData;
                  false ->
                     LoadData1 = divide2(avg, LoadData),
                     LoadData2 = divide2(avg2, LoadData1),

                     % Min and Max
                     % do_nothing,

                     % Histogram
                     _LoadData3 = divide2(LoadData2)
             end
         end || LoadData <- state_get(load_data_list, State)],

    State1 = state_set(load_data_list, LoadDataNew, State),

    % Averages ring data
    RingData1 = divide2(size_inv, state_get(ring_data, State1)),
    RingData2 = divide2(avg_kr, RingData1),

    State2 = state_set(ring_data, RingData2, State1),

    {{LoadDataNew, RingData2}, State2}.


%% @doc Cut the average values and associated weights in half.
-spec divide2(AvgType:: avg | avg2, MyData::load_data()) -> load_data();
             (AvgType:: size_inv | avg_kr, MyData::ring_data()) -> ring_data().
divide2(AvgType, MyData) ->
    case data_get(AvgType, MyData) of
        unknown -> MyData;
        {Avg, Weight} ->
            NewAvg = Avg/2,
            NewWeight = Weight/2,
            data_set(AvgType, {NewAvg, NewWeight}, MyData)
    end.


%% @doc Merges the load_data from a failed exchange back into self's load_data.
%%      Does not update the merge and convergence count.
-spec merge_failed_exch_data(Data::data(), CurState::state()) -> {data(), state()}.
merge_failed_exch_data(Data, State) ->
    merge_load_data(noupdate, Data, State).


%% @doc Merges the given load_data into self's load_data of the current or
%%      previous round. State can be current or previous state.
-spec merge_load_data(OtherData::data(), CurState::state()) -> {data(), state()}.
merge_load_data(OtherData, State) ->
    merge_load_data(update, OtherData, State).

%% @doc Helper function. Merge the given load_data with the given states load_data.
%%      Set update is set, the merge and convergence count is updated, otherwise not.
-spec merge_load_data(Update::update | noupdate, OtherData::data(),
    CurState::state()) -> {data(), state()}.
merge_load_data(Update, {OtherLoadList, OtherRing}, State) ->

    MyLoadList = state_get(load_data_list, State),
    LoadDataListNew =
        [begin
             LoadName = data_get(name, MyLoad1),
             ?ASSERT(LoadName =:= data_get(name, OtherLoad)),
             LoadSkipped = {load_data, LoadName, skip},
             case MyLoad1 =:= LoadSkipped orelse OtherLoad =:= LoadSkipped of
                  true -> MyLoad1;
                  false ->
                     % Averages load
                     MyLoad2 = merge_avg(avg, MyLoad1, OtherLoad),
                     MyLoad3 = merge_avg(avg2, MyLoad2, OtherLoad),

                     % Min
                     MyNewMin = erlang:min(data_get(min, MyLoad3),
                                           data_get(min, OtherLoad)),
                     MyLoad4 = data_set(min, MyNewMin, MyLoad3),

                     % Max
                     MyNewMax = erlang:max(data_get(max, MyLoad4),
                                           data_get(max, OtherLoad)),
                     MyLoad5 = data_set(max, MyNewMax, MyLoad4),

                     % Histogram
                     _MyLoad5 = merge_histo(MyLoad5, OtherLoad)
             end
         end || {MyLoad1, OtherLoad} <- lists:zip(MyLoadList, OtherLoadList)],

    State1 = state_set(load_data_list, LoadDataListNew, State),

    %% Averages ring
    MyRing1 = state_get(ring_data, State1),
    MyRing2 = merge_avg(size_inv, MyRing1, OtherRing),
    MyRing3 = merge_avg(avg_kr, MyRing2, OtherRing),
    State2 = state_set(ring_data, MyRing3, State1),

    NewData = {LoadDataListNew, MyRing3},
    State4 = case Update of
        update ->
            State3 = state_update(merged, fun inc/1, State2),
            _State4 = update_convergence_count({MyLoadList, MyRing1}, NewData, State3);
        noupdate -> State2
    end,
    {NewData, State4}.


%% @doc Helper function. Merges the given type of average from the given
%%      load_data records.
-spec merge_avg(AvgType:: avg | avg2, Data1::load_data_uninit(), Data2::load_data_uninit())
                -> load_data_uninit();
               (AvgType:: size_inv | avg_kr, Data1::ring_data_uninit(), Data2::ring_data_uninit())
                -> ring_data_uninit().
merge_avg(AvgType, Data1, Data2) ->
    %% avg values can be 'unknown' instead of {Load, Weight}
    %% unknown values are ignored
    NewAvg = case {data_get(AvgType, Data1), data_get(AvgType, Data2)} of
                 {unknown, unknown} -> unknown;
                 {Avg1,    unknown} -> Avg1;
                 {unknown, Avg2   } -> Avg2;
                 {Avg1,    Avg2   } -> {AvgLoad1, AvgWeight1} = Avg1,
                                       {AvgLoad2, AvgWeight2} = Avg2,
                                       {AvgLoad1 + AvgLoad2, AvgWeight1 + AvgWeight2}
             end,
    data_set(AvgType, NewAvg, Data1).


%% @doc Returns the previous load data if the current load data has not
%%      sufficiently converged, otherwise returns the current load data.
%%      In round 0 (when no previous load data exists) always the current load
%%      data is returned.
%%
%%      PrevState           CurState                            Return
%%      -----------------------------------------------------------------
%%      unknown             not initialised                     CurState
%%      unknown             initialized (but not converged)     CurState
%%      unknown             converged                           CurState
%%      converged           not initialised                     PrevState
%%      converged           initialized (but not converged)     PrevState
%%      converged           converged                           CurState
-spec previous_or_current(PrevState::state() | unknown, CurState::state()) -> BestState::state().
previous_or_current(unknown, CurState) when is_record(CurState, state) ->
    CurState;

previous_or_current(PrevState, CurState) when is_record(PrevState, state) andalso is_record(CurState, state) ->
    case has_converged(convergence_count_best_values(), CurState) of
        false -> PrevState;
        true -> CurState
    end.


%% @doc Replaces skipped metrics in My load list with non-skipped values from
%%      the other load list if possible.
-spec replace_skipped(Other::LoadDataList, My::LoadDataList) -> LoadDataList
        when is_subtype(LoadDataList, [load_data() | load_data_skipped()]).
replace_skipped([], []) ->
    [];
replace_skipped([H | OtherL], [{load_data, Metric, skip} | MyL]) ->
    ?ASSERT(Metric =:= data_get(name, H)),
    [H | replace_skipped(OtherL, MyL)];
replace_skipped([_OtherH | OtherL], [MyH | MyL]) ->
    ?ASSERT(data_get(name, _OtherH) =:= data_get(name, MyH)),
    [MyH | replace_skipped(OtherL, MyL)].


%%---------------------------- Histogram ---------------------------%%

-compile({inline, [init_histo/3, divide2/1, merge_histo/2, merge_bucket/2]}).

-spec init_histo(Module::module(), node_details:node_details(),
                 CurState::state()) -> histogram().
init_histo(Module, NodeDetails, CurState) ->
    NumberOfBuckets = state_get(no_of_buckets, CurState),
    Module:init_histo(NodeDetails, NumberOfBuckets).

-spec divide2(LoadData::load_data()) -> NewLoadData::load_data();
             (Bucket::bucket()) -> NewBucket::bucket().
% divide the histogram in the given load_data
divide2(LoadData) when is_record(LoadData, load_data) ->
    Histogram = data_get(histo, LoadData),
    Histogram1 = lists:map(fun divide2/1, Histogram),
    data_set(histo, Histogram1, LoadData);

% divide a single bucket of a histogram
divide2({Interval, unknown}) ->
    {Interval, unknown};

% divide a single bucket of a histogram
divide2({Interval, {Value, Weight}}) ->
    {Interval, {Value/2, Weight/2}}.


-spec merge_histo(MyData::load_data(), OtherData::load_data()) -> load_data().
merge_histo(MyData, OtherData) when is_record(MyData, load_data)
        andalso is_record(OtherData, load_data)->
    MyHisto = data_get(histo, MyData),
    OtherHisto = data_get(histo, OtherData),
    MergedHisto = lists:zipwith(fun merge_bucket/2, MyHisto, OtherHisto),
    data_set(histo, MergedHisto, MyData).




%% @doc Merge two buckets of a histogram.
-spec merge_bucket(Bucket1::bucket(), Bucket2::bucket()) -> bucket().
merge_bucket({Key, unknown}, {Key, unknown}) ->
    {Key, unknown};

merge_bucket({Key, unknown}, {Key, Avg2}) ->
    {Key, Avg2};

merge_bucket({Key, Avg1}, {Key, unknown}) ->
    {Key, Avg1};

merge_bucket({Key, {Value1, Weight1}}, {Key, {Value2, Weight2}}) ->
    {Key, {Value1+Value2, Weight1+Weight2}}.



%%---------------------------- Load Info ---------------------------%%

%% @doc Builds a load_info record from the given state (current or previous state)
-spec get_load_info(State::state()) -> load_info().
get_load_info(State) ->
    LoadDataList = state_get(load_data_list, State),
    %% select default load data for output
    {value, DefaultLoadData, OtherLoadData} =
        lists:keytake(?DEFAULT_MODULE, 2, LoadDataList),
    RingData = state_get(ring_data, State),
    OtherLoadInfo = [#load_info_other {
                                       name   = data_get(name, LoadData),
                                       avg    = get_current_estimate(avg, LoadData),
                                       stddev = calc_stddev(LoadData),
                                       min    = data_get(min, LoadData),
                                       max    = data_get(max, LoadData)
                                      }
                    || LoadData <- OtherLoadData],
    _LoadInfo =
        #load_info{ avg       = get_current_estimate(avg, DefaultLoadData),
                    stddev    = calc_stddev(DefaultLoadData),
                    size_ldr  = get_current_estimate(size_inv, RingData),
                    size_kr   = calc_size_kr(RingData),
                    min       = data_get(min, DefaultLoadData),
                    max       = data_get(max, DefaultLoadData),
                    merged    = state_get(merged, State),
                    other     = OtherLoadInfo
                  }.


%% @doc Gets the value to the given key from the given load_info record.
-spec load_info_get(Key::avgLoad, LoadInfoRecord::load_info()) -> unknown | float();
                   (Key::stddev, LoadInfoRecord::load_info()) -> unknown | float();
                   (Key::size_ldr, LoadInfoRecord::load_info()) -> unknown | float();
                   (Key::size_kr, LoadInfoRecord::load_info()) -> unknown | float();
                   (Key::size, LoadInfoRecord::load_info()) -> unknown | float();
                   (Key::minLoad, LoadInfoRecord::load_info()) -> unknown | min();
                   (Key::maxLoad, LoadInfoRecord::load_info()) -> unknown | max();
                   (Key::merged, LoadInfoRecord::load_info()) -> unknown | merged();
                   (Key::other, LoadInfoRecord::load_info()) -> [load_info_other()].
load_info_get(Key, #load_info{avg=Avg, stddev=Stddev, size_ldr=SizeLdr,
        size_kr=SizeKr, min=Min, max=Max, merged=Merged, other = Other}) ->
    case Key of
        avgLoad -> Avg;
        stddev -> Stddev;
        size_ldr -> SizeLdr;
        size_kr -> SizeKr;
        size ->
            % favor key range based calculations over leader-based
            case SizeKr of
                unknown -> SizeLdr;
                _       -> SizeKr
            end;
        minLoad -> Min;
        maxLoad -> Max;
        merged -> Merged;
        other  -> Other
    end.

%% @doc Gets values from a load_info_other record which can be found in the load_info record.
-spec load_info_other_get(Key::avgLoad, Module::atom(), LoadInfoRecord::load_info()) -> unknown | float();
                         (Key::stddev, Module::atom(), LoadInfoRecord::load_info()) -> unknown | float();
                         (Key::minLoad, Module::atom(), LoadInfoRecord::load_info()) -> unknown | min();
                         (Key::maxLoad, Module::atom(), LoadInfoRecord::load_info()) -> unknown | max().
load_info_other_get(Key, Module, #load_info{other = Other}) ->
    case lists:keyfind(Module, 2, Other) of
        false -> unknown;
        LoadInfoOther -> load_info_other_get(Key, LoadInfoOther)
    end.

-spec load_info_other_get(Key::name, load_info_other()) -> atom();
                   (Key::avgLoad, load_info_other()) -> unknown | float();
                   (Key::stddev, load_info_other()) -> unknown | float();
                   (Key::minLoad, load_info_other()) -> unknown | min();
                   (Key::maxLoad, load_info_other()) -> unknown | max().
load_info_other_get(Key, #load_info_other{name = Name, avg=Avg,
                                          stddev=Stddev, min=Min, max=Max}) ->
    case Key of
        name -> Name;
        avgLoad -> Avg;
        stddev -> Stddev;
        minLoad -> Min;
        maxLoad -> Max
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Calculations on Load Data Values
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [calc_current_estimate/1, calc_current_estimate/2]}).

%% @doc Calculate the current estimate from a given {Value, Weight} tuple.
-spec calc_current_estimate(unknown | avg()) -> float() | unknown.
calc_current_estimate(unknown) -> unknown;

calc_current_estimate({Value, Weight}) ->
    calc_current_estimate(Value, Weight).


%% @doc Calculate the current estimate from the given value and, weight.
-spec calc_current_estimate(Value::float(), Weight::float()) -> float() | unknown.
calc_current_estimate(Value, Weight) ->
    try Value / Weight
    catch
        error:badarith -> unknown
    end.


%% @doc Calculates the change in percent from the Old value to the New value.
-spec calc_change(OldValue::float() | unknown, NewValue::float() | unknown) -> float().
calc_change(unknown, _NewValue) -> 100.0;
calc_change(_OldValue, unknown) -> 100.0;
calc_change(Value, Value)       -> 0.0;
%calc_change(0, _NewValue)       -> 100.0; we only have floats!
calc_change(0.0, _NewValue)     -> 100.0;
calc_change(OldValue, NewValue) ->
    ((OldValue + abs(NewValue - OldValue)) * 100.0 / OldValue) - 100.


%% @doc Calculates the difference between the key of a node and its
%%      predecessor. If the second is larger than the first it wraps around and
%%      thus the difference is the number of keys from the predecessor to the
%%      end (of the ring) and from the start to the current node.
%%      Pre: MyRange is continuous
-spec calc_initial_avg_kr(MyRange::intervals:interval()) -> avg_kr() | unknown.
calc_initial_avg_kr(MyRange) ->
    try
        % get_bounds([]) throws 'no bounds in empty interval'
        {_, PredKey, MyKey, _} = intervals:get_bounds(MyRange),
        % we don't know whether we can subtract keys of type ?RT:key()
        % -> try it and if it fails, return unknown
        {?RT:get_range(PredKey, MyKey), 1.0}
    catch
        throw:not_supported -> unknown;
        throw:'no bounds in empty interval' -> unknown
    end.


%% @doc Extracts and calculates the standard deviation from the load_data record
-spec calc_stddev(load_data() | load_data_skipped()) -> unknown | float().

calc_stddev({load_data, _Module, skip}) -> unknown;

calc_stddev(Data) when is_record(Data, load_data) ->
    Avg = get_current_estimate(avg, Data),
    Avg2 = get_current_estimate(avg2, Data),
    case (Avg =:= unknown) orelse (Avg2 =:= unknown) of
        true -> unknown;
        false ->
            Tmp = Avg2 - (Avg * Avg),
            case (Tmp >= 0) of
                true  -> math:sqrt(Tmp);
                false -> unknown
            end
    end.


%% @doc Extracts and calculates the size_kr field from the load_data record
-spec calc_size_kr(unknown | ring_data()) -> unknown | float().
calc_size_kr(unknown) -> unknown;

calc_size_kr(Data) when is_record(Data, ring_data) ->
    AvgKR = get_current_estimate(avg_kr, Data),
    try
        if
            AvgKR =< 0        -> 1.0;
            AvgKR =:= unknown -> unknown;
            true              -> ?RT:n() / AvgKR
        end
    catch % ?RT:n() might throw
        throw:not_supported -> unknown
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Misc
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, inc/1}).

%% @doc Increments given value with one.
-spec inc(Value::integer()) -> integer().
inc(Value) ->
    Value+1.


-spec to_string(unknown) -> unknown;
               (Histogram::histogram()) -> string();
               (LoadInfo::load_info()) -> string();
               (Instance::instance()) -> string().
to_string(unknown) -> unknown;

to_string(#load_info{avg=Avg, stddev=Stddev, size_ldr=SizeLdr, size_kr=SizeKr,
        min=Min, max=Max, merged=Merged}) ->
    Labels1 = ["avg", "stddev", "size_ldr", "size_kr"],
    Values1 = [Avg, Stddev, SizeLdr, SizeKr],
    LVZib = lists:zip(Labels1, Values1),
    Fun = fun({Label, unknown}, AccIn) -> AccIn ++ Label ++ ": unknown, ";
             ({Label, Value}, AccIn) ->
                AccIn ++ io_lib:format("~s: ~.2f, ", [Label, Value]) end,
    L1 = lists:foldl(Fun, "", LVZib),
    L2 = io_lib:format("min: ~w, max: ~w, merged: ~w", [Min, Max, Merged]),
    lists:flatten(L1++L2);

to_string(Histogram) when is_list(Histogram) ->
    Values = [ calc_current_estimate(VWTuple) || { _, VWTuple } <- Histogram ],
    Fun = fun (unknown, AccIn) -> AccIn ++ "    unknown";
              (Value, AccIn) ->  AccIn ++ io_lib:format(" ~10.2. f", [Value]) end,
    HistoString = lists:foldl(Fun, "[ ", Values) ++ " ]",
    lists:flatten(HistoString);

to_string({ModuleName, Id}) ->
    lists:flatten(io_lib:format("~w:~w", [ModuleName, Id])).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% For Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%% @doc Creates a state record with greatly reduced variety in the round numbers
%%      to reduce warings.
%%      Used as value_creator in tester.erl (property testing).
-spec tester_create_state(status(), instance(), load_data_list(), ring_data(),
                          Leader::boolean(), Range::intervals:non_empty_interval(),
                          Request::boolean(), NoOfBuckets::histogram_size(), round(),
                          Merged::non_neg_integer(),
                          ConvergenceCount::non_neg_integer()) -> state().
tester_create_state(Status, Instance, LoadDataList, RingData, Leader, Range,
                    Request, NoOfBuckets, Round, Merged, ConvergenceCountRound) ->
    #state{status = Status,
            instance = Instance,
            load_data_list = LoadDataList,
            ring_data = RingData,
            leader = Leader,
            range = Range,
            request = Request,
            requestor = comm:this(),
            no_of_buckets = NoOfBuckets,
            round = Round,
            merged = Merged,
            convergence_count = ConvergenceCountRound
           }.

%% @doc Creates round values with greatly reduces variance, so that more rounds
%%      are valid rounds (i.e. rounds from messages and from the state match).
-spec tester_create_round(Round::0..10) -> round().
tester_create_round(0) -> 0;
tester_create_round(_) -> 1.


%% @doc Creates a histogram() within the specifications of this modules, i.e.
%%      in particular that all histograms need to have the same keys
%%      (keyrange/no_of_buckets).
-spec tester_create_histogram(ListOfAvgs::[avg() | unknown]) -> histogram().
tester_create_histogram([]) ->
    tester_create_histogram([unknown]);
tester_create_histogram(ListOfAvgs) ->
    Buckets = intervals:split(intervals:all(), no_of_buckets()),
    Histo1 = [ {BucketInterval, {}} || BucketInterval <- Buckets],
    Fun = fun ({BucketInterval, {}}) -> {BucketInterval,
                lists:nth(randoms:uniform(length(ListOfAvgs)), ListOfAvgs)} end,
    lists:map(Fun, Histo1).

-spec tester_create_histogram_size(1..50) -> histogram_size().
tester_create_histogram_size(Size) -> Size.


%% @doc Creates a fixed sized list of load_data every time. Size must not change once created.
%% Tester fails with massive memory leak here when input type is load_data_list().
-spec tester_create_load_data_list({load_data(), load_data(), load_data(), load_data()}) -> load_data_list().
tester_create_load_data_list(LoadDataTuple) ->
    % make first LoadData the default
    case erlang:get(load_data_list) of
        undefined ->
            LoadDataList = erlang:tuple_to_list(LoadDataTuple),
            LoadDataListNew =
                case LoadDataList of
                    [First | Rest] ->
                        Default = First#load_data{name = ?DEFAULT_MODULE},
                        [Default | Rest]
                end,
            erlang:put(load_data_list, LoadDataListNew),
            LoadDataListNew;
        Existing -> Existing
    end.

%% @doc Checks if a given list is a valid histogram.
%%      Used as type_checker in tester.erl (property testing).
-spec is_histogram([{intervals:interval(), avg()}]) -> boolean().
is_histogram([]) ->
    false;

is_histogram(Histogram) ->
    Buckets = intervals:split(intervals:all(), no_of_buckets()),
    EmptyHisto = [ {BucketInterval, {}} || BucketInterval <- Buckets],
    compare(Histogram, EmptyHisto).


%% @doc Compares to histograms for identical keys, helper function for
%%      is_histogram().
-spec compare(histogram(), histogram()) -> boolean().
compare([], []) -> true;
compare([], _List) -> false;
compare(_List, []) -> false;
compare([{BucketInterval, _Avg1} | List1], [{BucketInterval, _Avg2} | List2]) ->
    compare(List1, List2);
compare([_|_], [_|_]) ->
    false.


-spec init_feeder({NoOfBuckets::non_neg_integer(), Requestor::comm:mypid(),
                   Random1::boolean(), Random2::boolean()}) -> {[proplists:property()]}.
init_feeder({NoOfBuckets, Requestor, Random1, Random2}) ->
    Prop1 = if Random1 -> [{no_of_buckets, NoOfBuckets}];
               not Random1 -> []
            end,
    Prop2 = if Random2 -> [{requestor, Requestor}];
               not Random2 -> []
            end,
    {lists:append([Prop1, Prop2, [{instance, {gossip_load,default}}]])}.

-spec get_values_best_feeder({Options::[proplists:property()], Secs::0..1, Millisecs::0..1000}) ->
    {[proplists:property()]}.
get_values_best_feeder({Options, Secs, Millisecs}) ->
    Options1 = case proplists:get_value(send_after, Options) of
        true ->
            NewOptions1 = proplists:delete(send_after, Options),
            [ {send_after, Millisecs} | NewOptions1 ];
        _ ->
            Options
    end,
    Options2 = case proplists:get_value(msg_delay, Options1) of
        true ->
            NewOptions2 = proplists:delete(msg_delay, Options),
            [ {msg_delay, Secs} | NewOptions2 ];
        _ ->
            Options1
    end,
    {Options2}.


%% @doc For testing: ensure, that only buckets with identical keys are feeded to
%%      merge_bucket().
-compile({nowarn_unused_function, {merge_bucket_feeder, 2}}).
-spec merge_bucket_feeder(Bucket1::bucket(), Bucket2::bucket()) -> {bucket(), bucket()}.
merge_bucket_feeder({Key1, Val1}, {_Key2, Val2}) ->
    {{Key1, Val1}, {Key1, Val2}}.

% hack to be able to suppress warnings when testing
-spec warn() -> log:log_level().
warn() ->
    case config:read(gossip_log_level_warn) of
        failed -> warn;
        Level -> Level
    end.


