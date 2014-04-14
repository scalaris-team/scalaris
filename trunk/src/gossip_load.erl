%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%  @copyright 2008-2014 Zuse Institute Berlin

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
-export([request_histogram/2, load_info_get/2, load_info_other_get/3]).

% gossip_beh
-export([init/1, init/2, init/3, check_config/0, trigger_interval/0, fanout/0,
        select_node/1, select_data/1, select_reply_data/5, integrate_data/4,
        handle_msg/2, notify_change/3, min_cycles_per_round/0, max_cycles_per_round/0,
        round_has_converged/1, get_values_best/1, get_values_all/1, web_debug_info/1,
        shutdown/1]).

%% for testing
-export([tester_create_histogram/1, is_histogram/1, tester_create_state/11,
         is_state/1, tester_create_load_data_list/1]).

-ifdef(with_export_type_support).
-export_type([state/0, histogram/0, bucket/0]). %% for config gossip_load_*.erl
-export_type([avg/0, avg_kr/0, min/0, max/0]). %% for config gossip_load_*.erl
-export_type([load_info/0, load_info_other/0, merged/0]).
-endif.

%% -define(SHOW, config:read(log_level)).
-define(SHOW, debug).

-define(DEFAULT_MODULE, gossip_load_default).

%% List of module names for additional aggregation
%% (Modules need to implement gossip_load_beh)
%-define(ADDITIONAL_MODULES, [gossip_load_requests]).
-define(ADDITIONAL_MODULES, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state() :: ets:tab().
-type state_key() :: convergence_count | instance | leader | load_data | ring_data | merged |
    no_of_buckets | prev_state | range | request | requestor | round | status.
-type avg() :: {Value::float(), Weight::float()}.
-type avg_kr() :: {Value::number(), Weight::float()}.
-type min() :: non_neg_integer().
-type max() :: non_neg_integer().
-type merged() :: non_neg_integer().
-type bucket() :: {Interval::intervals:interval(), Avg::avg()|unknown}.
-type histogram() :: [bucket()].
-type instance() :: {Module :: gossip_load, Id :: atom() | uid:global_uid()}.

%% Record for ring related gossipping data
-record(ring_data, {
            size_inv  = unknown :: unknown | avg(), % 1 / size
            avg_kr    = unknown :: unknown | avg_kr() % average key range (distance between nodes in the address space)})
    }).

-type ring_data_uninit() :: #ring_data{}.
-type ring_data() :: {ring_data, SizeInv::avg(), AvgKr::avg_kr()}.

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
-type load_data() :: {load_data, Name::atom(), Avg::avg(), Avg2::avg(),
                      Min::non_neg_integer(), Max::non_neg_integer(),
                      Histogram::histogram()}.

-type load_data_list() :: [load_data(), ...].

-type data() :: {load_data_list(), ring_data()}.

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

-spec discard_old_rounds() -> boolean().
discard_old_rounds() ->
    config:read(gossip_load_discard_old_rounds).

-spec no_of_buckets() -> pos_integer().
no_of_buckets() ->
    config:read(gossip_load_number_of_buckets).

-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(gossip_load_interval) andalso
    config:cfg_is_greater_than(gossip_load_interval, 0) andalso

    config:cfg_is_integer(gossip_load_min_cycles_per_round) andalso
    config:cfg_is_greater_than_equal(gossip_load_min_cycles_per_round, 0) andalso

    config:cfg_is_integer(gossip_load_max_cycles_per_round) andalso
    config:cfg_is_greater_than_equal(gossip_load_max_cycles_per_round, 1) andalso

    config:cfg_is_float(gossip_load_convergence_epsilon) andalso
    config:cfg_is_in_range(gossip_load_convergence_epsilon, 0.0, 100.0) andalso

    config:cfg_is_bool(gossip_load_discard_old_rounds) andalso

    config:cfg_is_integer(gossip_load_convergence_count_best_values) andalso
    config:cfg_is_greater_than(gossip_load_convergence_count_best_values, 0) andalso

    config:cfg_is_integer(gossip_load_convergence_count_new_round) andalso
    config:cfg_is_greater_than(gossip_load_convergence_count_new_round, 0) andalso

    config:cfg_is_integer(gossip_load_number_of_buckets) andalso
    config:cfg_is_greater_than(gossip_load_number_of_buckets, 0),

    config:cfg_is_integer(gossip_load_fanout) andalso
    config:cfg_is_greater_than(gossip_load_fanout, 0).

-spec no_of_buckets(State::state()) -> pos_integer().
no_of_buckets(State) ->
    state_get(no_of_buckets, State).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Request a histogram with Size number of Buckets. <br/>
%%      The resulting histogram will be sent to SourceId, when all values have
%%      properly converged.
-spec request_histogram(Size::pos_integer(), SourceId::comm:mypid()) -> ok.
request_histogram(Size, SourcePid) when Size < 1 ->
    erlang:error(badarg, [Size, SourcePid]);

request_histogram(Size, SourcePid) ->
    gossip:start_gossip_task(?MODULE, [Size, SourcePid]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initiate the gossip_load module. <br/>
%%      Called by the gossip module upon startup. <br/>
%%      Instance makes the module aware of its own instance id, which is saved
%%      in the state of the module.
-spec init(Instance::instance()) -> {ok, state()}.
init(Instance) ->
    init(Instance, no_of_buckets(), none).

%% @doc Initiate the gossip_load module. <br/>
%%      Used by gossip:start_gossip_task().
%%      NoOfBuckets defines the size of the histogram calculated.
-spec init(Instance::instance(), NoOfBuckets::non_neg_integer()) -> {ok, state()}.
init(Instance, NoOfBuckets) ->
    init(Instance, NoOfBuckets, none).

%% @doc Initiate the gossip_load module. <br/>
%%      Used for request_histogram/1 (called by the gossip module). <br/>
%%      the calculated histogram will be sent to the requestor.
-spec init(Instance::instance(), NoOfBuckets::non_neg_integer(),
                Requestor::comm:mypid()|none) -> {ok, state()}.
init(Instance, NoOfBuckets, Requestor) ->
    log:log(debug, "[ ~w ] CBModule initiated. NoOfBuckets: ~w, Requestor: ~w",
        [Instance, NoOfBuckets, Requestor]),
    State = state_new(),
    state_set(prev_state, unknown, State),
    state_set(no_of_buckets, NoOfBuckets, State),
    state_set(request, Requestor =/= none, State),
    state_set(requestor, Requestor, State),
    state_set(instance, Instance, State),
    {ok, State}.


%% @doc Returns false, i.e. peer selection is done by gossip module.
%%      State: the state of the gossip_load module
-spec select_node(State::state()) -> {boolean(), state()}.
select_node(State) ->
    {false, State}.

%% @doc Select and prepare the load information to be sent to the peer. <br/>
%%      Called by the gossip module at the beginning of every cycle. <br/>
%%      The selected exchange data is sent back to the gossip module as a message
%%      of the form {selected_data, Instance, ExchangeData}.
%%      State: the state of the gossip_load module
-spec select_data(State::state()) -> {ok, state()}.
select_data(State) ->
    log:log(debug, "[ ~w ] select_data: State: ~w", [state_get(instance, State), State]),
    case state_get(status, State) of
        uninit ->
            request_dht_state(State);
        init ->
            {LoadData, RingData} = prepare_data(State),
            Pid = pid_groups:get_my(gossip),
            comm:send_local(Pid, {selected_data, state_get(instance, State), {LoadData, RingData}})
    end,
    {ok, State}.


%% @doc Process the data from the requestor and select reply data. <br/>
%%      Called by the behaviour module upon a p2p_exch message. <br/>
%%      PData: exchange data from the p2p_exch request <br/>
%%      Ref: used by the gossip module to identify the request <br/>
%%      RoundStatus / Round: round information used for special handling of
%%          messages from previous rounds <br/>
%%      State: the state of the gossip_load module
-spec select_reply_data(PData::data(), Ref::pos_integer(),
    RoundStatus::gossip_beh:round_status(), Round::non_neg_integer(),
    State::state()) -> {discard_msg | ok | retry | send_back, state()}.
select_reply_data(PData, Ref, RoundStatus, Round, State) ->

    IsValidRound = is_valid_round(RoundStatus, Round, State),

    case state_get(status, State) of
        uninit ->
            log:log(?SHOW, "[ ~w ] select_reply_data in uninit", [?MODULE]),
            {retry,State};
        init when not IsValidRound ->
            log:log(warn(), "[ ~w ] Discarded data in select_reply_data. Reason: invalid round.",
                [state_get(instance, State)]),
            {send_back, State};
        init when RoundStatus =:= current_round ->
            Data1 = prepare_data(State),
            Pid = pid_groups:get_my(gossip),
            comm:send_local(Pid, {selected_reply_data, state_get(instance, State), Data1, Ref, Round}),

            _ = merge_load_data(current_round, PData, State),

            log:log(debug, "[ ~w ] Data after select_reply_data: ~w", [state_get(instance, State), get_load_info(State)]),
            {ok, State};
        init when RoundStatus =:= old_round ->
            case discard_old_rounds() of
                true ->
                    log:log(?SHOW, "[ ~w ] select_reply_data in old_round", [?MODULE]),
                    % This only happens when entering a new round, i.e. the values have
                    % already converged. Consequently, discarding messages does
                    % not cause mass loss.
                    {discard_msg, State};
                false ->
                    log:log(?SHOW, "[ ~w ] select_reply_data in old_round", [?MODULE]),
                    PrevState = state_get(prev_state, State),
                    Data1 = prepare_data(PrevState),
                    Pid = pid_groups:get_my(gossip),
                    comm:send_local(Pid, {selected_reply_data, state_get(instance, State), Data1, Ref, Round}),
                    _ = merge_load_data(prev_round, PData, State),
                    {ok, State}
            end
    end.

%% @doc Integrate the reply data. <br/>
%%      Called by the behaviour module upon a p2p_exch message. <br/>
%%      QData: the reply data from the peer <br/>
%%      RoundStatus / Round: round information used for special handling of
%%          messages from previous rounds <br/>
%%      State: the state of the gossip_load module <br/>
%%      Upon finishing the processing of the data, a message of the form
%%      {integrated_§data, Instance, RoundStatus} is to be sent to the gossip module.
-spec integrate_data(QData::data(), RoundStatus::gossip_beh:round_status(),
    Round::non_neg_integer(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
integrate_data(QData, RoundStatus, Round, State) ->
    log:log(debug, "[ ~w ] Reply-Data: ~w~n", [state_get(instance, State), QData]),

    IsValidRound = is_valid_round(RoundStatus, Round, State),

    case state_get(status, State) of
        _ when not IsValidRound ->
            log:log(warn(), "[ ~w ] Discarded data in integrate_data. Reason: invalid round. ", [?MODULE]),
            {send_back, State};
        uninit ->
            log:log(?SHOW, "[ ~w ] integrate_data in uninit", [?MODULE]),
            {retry,State};
        init ->
            integrate_data_init(QData, RoundStatus, State)
    end.


%% @doc Handle get_state_response messages from the dht_node. <br/>
%%      The received load information is stored and the status is set to init,
%%      allowing the start of a new gossip round.
%%      State: the state of the gossip_load module <br/>
-spec handle_msg(Message::{get_state_response, DHTNodeState::dht_node_state:state()},
    State::state()) -> {ok, state()}.
handle_msg({get_state_response, DHTNodeState}, State) ->

    LoadDataNew =
        [ begin
              Module = data_get(name, LoadData),
              Load = Module:get_load(DHTNodeState),
              log:log(?SHOW, "[ ~w ] Load: ~w", [state_get(instance, State), Load]),

              LoadData1 = data_set(avg, {float(Load), 1.0}, LoadData),
              LoadData2 = data_set(min, Load, LoadData1),
              LoadData3 = data_set(max, Load, LoadData2),
              LoadData4 = data_set(avg2, {float(Load*Load), 1.0}, LoadData3),
              % histogram
              Histo = init_histo(Module, DHTNodeState, State),
              log:log(?SHOW,"[ ~w ] Histo: ~s", [state_get(instance, State), to_string(Histo)]),
              _LoadData5 = data_set(histo, Histo, LoadData4)
          end
        || LoadData <- state_get(load_data, State)],

    state_set(load_data, LoadDataNew, State),

    RingData = state_get(ring_data, State),
    RingData1 = case (state_get(leader, State)) of
                    true ->
                        data_set(size_inv, {1.0, 1.0}, RingData);
                    false ->
                        data_set(size_inv, {1.0, 0.0}, RingData)
                end,
    AvgKr = calc_initial_avg_kr(state_get(range, State)),
    RingData2 = data_set(avg_kr, AvgKr, RingData1),
    state_set(ring_data, RingData2, State),

    {NewLoadData, NewRingData} = prepare_data(State),
    state_set(status, init, State),

    % send PData to BHModule
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {selected_data, state_get(instance, State), {NewLoadData, NewRingData}}),
    {ok, State}.

%% @doc Checks if the current round has converged yer <br/>
%%      Returns true if the round has converged, false otherwise.
-spec round_has_converged(State::state()) -> {boolean(), state()}.
round_has_converged(State) ->
    ConvergenceCount = convergence_count_new_round(),
    {has_converged(ConvergenceCount, State), State}.


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
-spec notify_change(Keyword::new_round, NewRound::non_neg_integer(), State::state()) -> {ok, state()};
    (Keyword::leader, {MsgTag::is_leader | no_leader, NewRange::intervals:interval()},
            State::state()) -> {ok, state()};
    (Keyword::exch_failure, {_MsgTag::atom(), Data::data(), _Round::non_neg_integer()},
             State::state()) -> {ok, state()}.
notify_change(new_round, NewRound, State) ->
    log:log(debug, "[ ~w ] new_round notification. NewRound: ~w", [state_get(instance, State), NewRound]),
    case state_get(request, State) of
        true ->
            finish_request(State);
        false ->
            new_round(NewRound, State)
    end;

notify_change(leader, {MsgTag, NewRange}, State) when MsgTag =:= is_leader ->
    log:log(?SHOW, "[ ~w ] I'm the leader", [?MODULE]),
    state_set(leader, true, State),
    state_set(range, NewRange, State),
    {ok, State};

notify_change(leader, {MsgTag, NewRange}, State) when MsgTag =:= no_leader ->
    log:log(?SHOW, "[ ~w ] I'm no leader", [?MODULE]),
    state_set(leader, false, State),
    state_set(range, NewRange, State),
    {ok, State};


notify_change(exch_failure, {_MsgTag, Data, Round}, State) ->
    RoundFromState = state_get(round, State),
    RoundStatus = if Round =:= RoundFromState -> current_round;
        Round =/= RoundFromState -> old_round
    end,
    IsValidRound = is_valid_round(RoundStatus, Round, State),
    _ = case Data of
        _ when not IsValidRound ->
            log:log(?SHOW, " [ ~w ] exch_failure in invalid round", [?MODULE]),
            do_nothing;
        undefined -> do_nothing;
        Data ->
            log:log(debug, " [ ~w ] exch_failure in valid round", [?MODULE]),
            merge_failed_exch_data(Data, State)
    end,
    {ok, State}.


%% @doc Returns the best aggregation result. <br/>
%%      Called by the gossip module upon {get_values_best} messages.
%%      Returns either the aggregation restult from the current or previous round,
%%      depending on convergence_count_best_values. <br/>
%%      State: the state of the gossip_load module.
-spec get_values_best(state()) -> {load_info(), state()}.
get_values_best(State) ->
    BestState = previous_or_current(State),
    LoadInfo = get_load_info(BestState),
    {LoadInfo, State}.


%% @doc Returns all aggregation results. <br/>
%%      Called by the gossip module upon {get_values_all} messages.
%%      Returns the aggregation restult from a) the previous, b) the current
%%      and c) the best round (current or previous).
%%      State: the state of the gossip_load module.
-spec get_values_all(state()) ->
    { {PreviousInfo::load_info(), CurrentInfo::load_info(), BestInfo::load_info()},
        NewState::state() }.
get_values_all(State) ->
    BestState = previous_or_current(State),
    InfosAll = lists:map(fun get_load_info/1, [state_get(prev_state, State), State, BestState]),
    {list_to_tuple(InfosAll), State}.


%% @doc Returns a key-value list of debug infos for the Web Interface. <br/>
%%      Called by the gossip module upon {web_debug_info} messages.
%%      State: the state of the gossip_load module.
-spec web_debug_info(state()) ->
    {KeyValueList::[{Key::string(), Value::any()},...], NewState::state()}.
web_debug_info(State) ->
    PreviousState = state_get(prev_state, State),
    PreviousLoadData = prev_state_get(load_data, State),
    CurrentLoadData = state_get(load_data, State),
    PreviousRingData = prev_state_get(ring_data, State),
    CurrentRingData = state_get(ring_data, State),
    BestState = previous_or_current(State),
    Best = if BestState =:= State -> current_data;
              BestState =:= PreviousState -> previous_data
        end,
    KeyValueList =
        [{to_string(state_get(instance, State)), ""},
         {"best",                Best},
         {"leader",              state_get(leader, State)},
         %% previous round
         {"prev_round",          state_get(round, PreviousState)},
         {"prev_merged",         state_get(merged, PreviousState)},
         {"prev_conv_avg_count", state_get(convergence_count, PreviousState)},
         {"prev_size_ldr",       get_current_estimate(size_inv, PreviousRingData)},
         {"prev_size_kr",        calc_size_kr(PreviousRingData)}
        ]
        ++
            lists:flatten(
          [ begin
                [
                 {"prev_name",           data_get(name, LoadDataModule)},
                 {"prev_avg",            get_current_estimate(avg, LoadDataModule)},
                 {"prev_min",            data_get(min, LoadDataModule)},
                 {"prev_max",            data_get(max, LoadDataModule)},
                 {"prev_stddev",         calc_stddev(LoadDataModule)},
                 {"prev_histo",          to_string(data_get(histo, LoadDataModule))}
                ]
            end
            || PreviousLoadData =/= unknown, LoadDataModule <- PreviousLoadData])
        ++
        [
         %% current round
         {"cur_round",           state_get(round, State)},
         {"cur_merged",          state_get(merged, State)},
         {"cur_conv_avg_count",  state_get(convergence_count, State)},
         {"cur_size_ldr",        get_current_estimate(size_inv, CurrentRingData)},
         {"cur_size_kr",         calc_size_kr(CurrentRingData)}
        ]
        ++
            lists:flatten(
          [ begin
                [
                 {"cur_name",           data_get(name, LoadDataModule)},
                 {"cur_avg",            get_current_estimate(avg, LoadDataModule)},
                 {"cur_min",            data_get(min, LoadDataModule)},
                 {"cur_max",            data_get(max, LoadDataModule)},
                 {"cur_stddev",         calc_stddev(LoadDataModule)},
                 {"cur_histo",          to_string(data_get(histo, LoadDataModule))}
                ]
            end
            || LoadDataModule <- CurrentLoadData]),
    {KeyValueList, State}.


%% @doc Shutd down the gossip_load module. <br/>
%%      Called by the gossip module upon stop_gossip_task(CBModule).
%%      Deletes both the current and the previous state.
-spec shutdown(State::state()) -> {ok, state_deleted}.
shutdown(State) ->
    delete_state(state_get(prev_state, State)),
    delete_state(State),
    {ok, state_deleted}.


%%------------------- Callback Functions: Helpers ------------------%%

-spec request_dht_state(State::state()) -> ok.
request_dht_state(State) ->
    % get state of dht node
    DHT_Node = pid_groups:get_my(dht_node),
    EnvPid = comm:reply_as(comm:this(), 3, {cb_reply, state_get(instance, State), '_'}),
    % send_local doesn't work
    comm:send(comm:make_global(DHT_Node), {get_state, EnvPid}).

-spec is_valid_round(RoundStatus::gossip_beh:round_status(),
    RoundFromMessage::non_neg_integer(), State::state()) -> boolean().
is_valid_round(RoundStatus, RoundFromMessage, State) ->
    RoundFromState = case RoundStatus of
        current_round -> state_get(round, State);
        old_round -> prev_state_get(round, State)
    end,

    if RoundFromState =:= RoundFromMessage -> true;
       true ->
           log:log(debug, "[ ~w ] Invalid ~w. RoundFromState: ~w, RoundFromMessage: ~w",
                   [state_get(instance, State), RoundStatus, RoundFromState, RoundFromMessage]),
           false
    end.


-spec integrate_data_init(QData::data(), RoundStatus::gossip_beh:round_status(),
    State::state()) -> {ok, state()}.
integrate_data_init(QData, RoundStatus, State) ->
    case RoundStatus of
        current_round ->
            {_NewLoad, _NewRing} = merge_load_data(current_round, QData, State),
            %% _PrevState = state_get(prev_state, State),
            %% _PrevLoadInfo = get_load_info(_PrevState),
            %% _ValuesBest = get_values_best(State),
            %% _ValuesAll = get_values_all(State),
            %% log:log(debug, "[ ~w ] Values best: ~n\t~w", [state_get(instance, State), _ValuesBest]),
            %% log:log(debug, "[ ~w ] Values all: ~n\t~w~n", [state_get(instance, State), _ValuesAll]),
            %% log:log(debug, "[ ~w ] Values prev round: ~n\t~w", [state_get(instance, State), _PrevLoadInfo]),
            _LoadInfo = get_load_info(State),
            _Histo = data_get(histo, get_default_load_data(_NewLoad)),
            log:log(?SHOW, "[ ~w ] Data at end of cycle: ~n\t~s~n\tHisto: ~s~n",
                [state_get(instance, State), to_string(_LoadInfo), to_string(_Histo)]);
        old_round ->
             case discard_old_rounds() of
                true ->
                    log:log(debug, "[ ~w ] integrate_data in old_round", [?MODULE]),
                    % This only happens when entering a new round, i.e. the values have
                    % already converged. Consequently, discarding messages does
                    % not cause mass loss.
                    do_nothing;
                false ->
                    _ = merge_load_data(prev_round, QData, State),
                    log:log(?SHOW, "[ ~w ] integrate_data in old_round", [?MODULE])
            end
    end,
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {integrated_data, state_get(instance, State), RoundStatus}),
    {ok, State}.


-spec new_round(NewRound, State) -> {ok, State} when
    is_subtype(NewRound, non_neg_integer()),
    is_subtype(State, state()).
new_round(NewRound, State) ->
    % Only replace prev round with current round if current has converged.
    % Cases in which current round has not converged: e.g. late joining, sleeped/paused.
    % ConvergenceTarget should be less than covergence_count_new_round,
    % otherwise non leader groups seldom replace prev_load_data
    NewState = case has_converged(convergence_count_best_values(), State) of
        true ->
            % make the current state the previous state and delete the old prev state
            OldPrevState = state_get(prev_state, State),
            delete_state(OldPrevState),
            state_new(State, State);
        false ->
            % make the old previous state the new previous state and discard the current state
            log:log(warn(), "[ ~w ] Entering new round, but old round has not converged", [?MODULE]),
            PrevState = state_get(prev_state, State),
            NewState1 = state_new(State, PrevState),
            delete_state(State),
            NewState1
    end,
    state_set(round, NewRound, NewState),
    {ok, NewState}.


-spec has_converged(TargetConvergenceCount::pos_integer(), State::state()) -> boolean().
has_converged(TargetConvergenceCount, State) ->
    CurrentConvergenceCount = state_get(convergence_count, State),
    CurrentConvergenceCount >= TargetConvergenceCount.


-spec finish_request(State) -> {ok, State} when
    is_subtype(State, state()).
finish_request(State) ->
    LoadDataList = state_get(load_data, State),
    Histo = data_get(histo, get_default_load_data(LoadDataList)),
    case state_get(leader, State) of
        true ->
            Requestor = state_get(requestor, State),
            comm:send(Requestor, {histogram, Histo}),
            gossip:stop_gossip_task(state_get(instance, State));
        false -> do_nothing
    end,
    {ok, State}.


-spec update_convergence_count(OldData::data(), NewData::data(), State::state()) -> true.
update_convergence_count({OldLoadList, OldRing}, {NewLoadList, NewRing}, State) ->

    % check whether all average based values changed less than epsilon percent
    AvgChangeEpsilon = convergence_epsilon(),

    %% Ring convergence
    Fun = fun(AvgType) ->
            OldValue = calc_current_estimate(data_get(AvgType, OldRing)),
            NewValue = calc_current_estimate(data_get(AvgType, NewRing)),
            log:log(debug, "[ ~w ] ~w: OldValue: ~w, New Value: ~w",
                [state_get(instance, State), AvgType, OldValue, NewValue]),
            _HasConverged = calc_change(OldValue, NewValue) < AvgChangeEpsilon
    end,
    RingConverged = lists:all(Fun, [size_inv, avg_kr]),

    %% Load convergence
    LoadModulesConverged =
        [begin
             ?ASSERT(data_get(name, OldLoad) =:= data_get(name, NewLoad)),
             OldValue = calc_current_estimate(data_get(AvgType, OldLoad)),
             NewValue = calc_current_estimate(data_get(AvgType, NewLoad)),
             log:log(debug, "[ ~w ] ~w: OldValue: ~w, New Value: ~w",
                     [state_get(instance, State), AvgType, OldValue, NewValue]),
             _HasConverged = calc_change(OldValue, NewValue) < AvgChangeEpsilon
         end
         || {OldLoad, NewLoad} <- lists:zip(OldLoadList, NewLoadList),
            AvgType            <- [avg, avg2]
        ],

    LoadConverged = lists:all(fun(X) -> X end, LoadModulesConverged),

    HaveConverged1 = RingConverged andalso LoadConverged,
    log:log(debug, "Averages have converged: ~w", [HaveConverged1]),

    HistoModulesConverged =
        [ begin
              ?ASSERT(data_get(name, OldLoad) =:= data_get(name, NewLoad)),
              % Combine the avg values of the buckets of two histograms into one tuple list
              Combine = fun ({Interval, AvgOld}, {Interval, AvgNew}) -> {AvgOld, AvgNew} end,
              CompList = lists:zipwith(Combine,
                                       data_get(histo, OldLoad), data_get(histo, NewLoad)),

              % check that all the buckets of histogram have changed less than epsilon percent
              Fun1 = fun({OldAvg, NewAvg}) ->
                             OldEstimate = calc_current_estimate(OldAvg),
                             NewEstimate = calc_current_estimate(NewAvg),
                             _HasConverged = calc_change(OldEstimate, NewEstimate) < AvgChangeEpsilon
                     end,
              _HaveConverged2 = lists:all(Fun1, CompList)
          end
          || {OldLoad, NewLoad} <- lists:zip(OldLoadList, NewLoadList)
        ],

    HaveConverged2 = lists:all(fun(X) -> X end, HistoModulesConverged),
    log:log(debug, "Histogram has converged: ~w", [HaveConverged2]),

    HaveConverged = HaveConverged1 andalso HaveConverged2,
    case HaveConverged of
        true ->
            state_update(convergence_count, fun inc/1, State);
        false ->
            state_set(convergence_count, 0, State)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% State of gossip_load: Getters, Setters and Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [delete_state/1, state_get/2, state_set/3, state_update/3,
                   prev_state_get/2]}).

%% @doc Create a new state table with some initial values.
-spec state_new() -> state().
state_new() ->
    NewState = ets:new(cbstate, [set, protected]),
    state_set(status, uninit, NewState),
    state_set(load_data, load_data_list_new(), NewState),
    state_set(ring_data, ring_data_new(), NewState),
    state_set(leader, unknown, NewState),
    state_set(range, unknown, NewState),
    state_set(prev_state, unknown, NewState),
    state_set(request, false, NewState),
    state_set(requestor, none, NewState),
    Fun = fun (Key) -> state_set(Key, 0, NewState) end,
    lists:foreach(Fun, [round, merged, reqs_send, reqs_recv, replies_recv, convergence_count]),
    NewState.


%% @doc Create a new state table with information from other state tables.
%%      This is used to build a new state from the current state when a new round
%%      is entered. Some values (mainly counters) are resetted, others are taken
%%      from the Parent state. The given PrevState is set as prev_state of the new
%%      state. The PrevState can (but does not have to) be the same as Parent.
-spec state_new(Parent::state(), PrevState::state()) -> state().
state_new(Parent, PrevState) ->
    NewState = state_new(), % new state with default values
    state_set(prev_state, PrevState, NewState),
    state_set(leader, state_get(leader, Parent), NewState),
    state_set(range, state_get(range, Parent), NewState),
    state_set(no_of_buckets, state_get(no_of_buckets, Parent), NewState),
    state_set(request, state_get(request, Parent), NewState),
    state_set(requestor, state_get(requestor, Parent), NewState),
    state_set(instance, state_get(instance, Parent), NewState),
    NewState.



-spec delete_state(State::unknown | state()) -> true.
delete_state(unknown) -> true;

delete_state(Name) ->
    ets:delete(Name).


-spec state_get(Key::state_key(), State::state() | unknown) -> any().
state_get(_Key, unknown) ->
    unknown;

state_get(Key, State) ->
    case ets:lookup(State, Key) of
        [] ->
            log:log(error, "[ ~w ] Lookup of ~w in ~w failed~n", [state_get(instance, State), Key, State]),
            erlang:error(lookup_failed, [Key, State]);
        [{Key, Value}] -> Value
    end.


-spec state_set(Key::state_key(), Value::any(), State::state()) -> true.
state_set(Key, Value, State) ->
    ets:insert(State, {Key, Value}).

-spec state_update(Key::state_key(), Fun::fun(), State::state()) -> true.
state_update(Key, Fun, State) ->
    Value = state_get(Key, State),
    NewValue = apply(Fun, [Value]),
    state_set(Key, NewValue, State).


-spec prev_state_get(Key::state_key(), State::state()) -> any().
prev_state_get(Key, State) ->
    PrevState = state_get(prev_state, State),
    state_get(Key, PrevState).


%% prev_state_set(Key, Value, State) ->
%%     PrevState = state_get(prev_state, State),
%%     state_set(Key, Value, PrevState).


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
    Modules = [?DEFAULT_MODULE | ?ADDITIONAL_MODULES],
    [load_data_new(Module) || Module <- Modules].

-spec ring_data_new() -> ring_data_uninit().
ring_data_new() ->
    #ring_data{}.

-spec get_default_load_data(unknown) -> unknown;
                           (load_data_list()) -> load_data().
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
-spec data_get(_, unknown) -> unknown;
             (name, load_data()) -> atom();
             (avg, load_data()) -> avg();
             (avg2, load_data()) -> avg();
             (min, load_data()) -> min();
             (max, load_data()) -> max();
             (histo, load_data()) -> histogram();
             (size_inv, ring_data()) -> avg();
             (avg_kr, ring_data()) -> avg_kr().
data_get(_Key, unknown) -> unknown;
data_get(_Key, []) ->
    [];
data_get(Key, [LoadData | Other]) ->
    [data_get(Key, LoadData) | data_get(Key, Other)];
data_get(name, #load_data{name = Name}) ->
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
                           unknown | load_data()) -> float() | unknown;
                          (Key::size_inv | avg_kr,
                           unknown | ring_data()) -> float() | unknown.
get_current_estimate(_Key, unknown) -> unknown;
get_current_estimate(avg, #load_data{avg=Avg}) ->
    calc_current_estimate(Avg);
get_current_estimate(avg2, #load_data{avg2=Avg2}) ->
    calc_current_estimate(Avg2);
get_current_estimate(size_inv, #ring_data{size_inv = SizeInv}) ->
    calc_current_estimate(SizeInv);
get_current_estimate(avg_kr, #ring_data{avg_kr = AvgKr}) ->
    calc_current_estimate(AvgKr).

%% get_size(Data) ->
%%     Size_kr = calc_size_kr(Data),
%%     Size_ldr = load_data_get(size_inv, Data),
%%     % favor key range based calculations over leader-based
%%     case Size_kr of
%%         unknown -> Size_ldr;
%%         _       -> Size_kr
%%     end.


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
%%      load_data of self accordingly.
-spec prepare_data(State::state()) -> {load_data_list(), ring_data()}.
prepare_data(State) ->
    % Averages
    Fun = fun (AvgType, MyData) -> divide2(AvgType, MyData) end,
    LoadDataNew =
        [begin
             LoadData2 = lists:foldl(Fun, LoadData, [avg, avg2]),

             % Min and Max
             do_nothing,

             % Histogram
             _LoadData3 = divide2(LoadData2)
         end
    || LoadData <- state_get(load_data, State)],

    state_set(load_data, LoadDataNew, State),

    % Averages ring data
    RingData1 = state_get(ring_data, State),
    RingData2 = lists:foldl(Fun, RingData1, [size_inv, avg_kr]),

    state_set(ring_data, RingData2, State),

    {LoadDataNew, RingData2}.


%% @doc Cut the average values and associated weights in half.
-spec divide2(AvgType:: avg | avg2, MyData::load_data()) -> load_data();
             (AvgType:: size_inv | avg_kr, MyData::ring_data()) -> ring_data().
divide2(AvgType, MyData) ->
    {Avg, Weight} = data_get(AvgType, MyData),
    NewAvg = Avg/2,
    NewWeight = Weight/2,
    data_set(AvgType, {NewAvg, NewWeight}, MyData).


%% @doc Merges the load_data from a failed exchange back into self's load_data.
%%      Does not update the merge and convergence count.
-spec merge_failed_exch_data(Data::data(), State::state()) -> data().
merge_failed_exch_data(Data, State) ->
    merge_load_data1(noupdate, Data, State).


%% @doc Merges the given load_data into self's load_data of the current or
%%      previous round.
-spec merge_load_data(current_round, OtherData::data(), State::state()) -> data();
        (prev_round, OtherData::data(), State::state()) -> data().
merge_load_data(current_round, OtherData, State) ->
    merge_load_data1(update, OtherData, State);

merge_load_data(prev_round, OtherData, State) ->
    PrevState = state_get(prev_state, State),
    merge_load_data1(update, OtherData, PrevState).


%% @doc Helper function. Merge the given load_data with the given states load_data.
%%      Set update is set, the merge and convergence count is updated, otherwise not.
-spec merge_load_data1(Update::update | noupdate, OtherData::data(),
    State::state()) -> data().
merge_load_data1(Update, {OtherLoadList, OtherRing}, State) ->

    MyLoadList = state_get(load_data, State),
    LoadDataNew =
        [begin
             ?ASSERT(data_get(name, MyLoad1) =:= data_get(name, OtherLoad)),
             % Averages load
             Fun = fun (AvgType, MyData) -> merge_avg(AvgType, MyData, OtherLoad) end,
             MyLoad2 = lists:foldl(Fun, MyLoad1, [avg, avg2]),

             % Min
             MyMin = data_get(min, MyLoad2),
             OtherMin = data_get(min, OtherLoad),
             MyLoad3 =
                 if  MyMin =< OtherMin -> data_set(min, MyMin, MyLoad2);
                     MyMin > OtherMin -> data_set(min, OtherMin, MyLoad2)
                 end,

             % Max
             MyMax = data_get(max, MyLoad3),
             OtherMax = data_get(max, OtherLoad),
             MyLoad4 =
                 if  MyMax =< OtherMax -> data_set(max, OtherMax, MyLoad3);
                     MyMax > OtherMax -> data_set(max, MyMax, MyLoad3)
                 end,

             % Histogram
             _MyLoad5 = merge_histo(MyLoad4, OtherLoad)
         end
         || {MyLoad1, OtherLoad} <- lists:zip(MyLoadList, OtherLoadList)],

    state_set(load_data, LoadDataNew, State),

    %% Averages ring
    MyRing1 = state_get(ring_data, State),
    Fun2 = fun (AvgType, MyData) -> merge_avg(AvgType, MyData, OtherRing) end,
    MyRing2 = lists:foldl(Fun2, MyRing1, [size_inv, avg_kr]),
    state_set(ring_data, MyRing2, State),

    NewData = {LoadDataNew, MyRing2},
    case Update of
        update ->
            state_update(merged, fun inc/1, State),
            update_convergence_count({MyLoadList, MyRing1}, NewData, State);
        noupdate -> do_nothing
    end,
    NewData.


%% @doc Helper function. Merges the given type of average from the given
%%      load_data records.
-spec merge_avg(AvgType:: avg | avg2, Data1::load_data(), Data2::load_data())
                -> load_data();
               (AvgType:: size_inv | avg_kr, Data1::ring_data(), Data2::ring_data())
                -> ring_data().
merge_avg(AvgType, Data1, Data2) ->
    {AvgLoad1, AvgWeight1} = data_get(AvgType, Data1),
    {AvgLoad2, AvgWeight2} = data_get(AvgType, Data2),
    NewAvg = {AvgLoad1+AvgLoad2, AvgWeight1+AvgWeight2},
    data_set(AvgType, NewAvg, Data1).

%% @doc Returns the previous load data if the current load data has not
%%      sufficiently converged, otherwise returns the current load data.
%%      In round 0 (when no previous load data exists) always the current load
%%      data is returned.
-spec previous_or_current(state()) -> state().
previous_or_current(State) when is_atom(State) orelse is_integer(State) ->
    CurrentInitialized = case state_get(status, State) of
                            init -> true;
                            uninit -> false
                        end,
    HasConverged = has_converged(convergence_count_best_values(), State),
    Round = state_get(round, State),
    _BestValue =
        case (not CurrentInitialized) orelse (not HasConverged) of
            true when Round =/= 0 -> state_get(prev_state, State);
            true -> State;
            false -> State
        end.



%%---------------------------- Histogram ---------------------------%%

-compile({inline, [init_histo/3, divide2/1, merge_histo/2, merge_bucket/2]}).

-spec init_histo(Module::module(), DHTNodeState::dht_node_state:state(),
                 State::state()) -> histogram().
init_histo(Module, DHTNodeState, State) ->
    NumberOfBuckets = no_of_buckets(State),
    Module:init_histo(DHTNodeState, NumberOfBuckets).

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
    LoadDataList = state_get(load_data, State),
    %% select default load data for output
    {DefaultLoadData, OtherLoadData} =
        case lists:keytake(?DEFAULT_MODULE, 2, LoadDataList) of
            false -> throw(default_load_module_not_available);
            {value, LoadData, Other} -> {LoadData, Other}
        end,
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
-spec calc_stddev(unknown | load_data()) -> unknown | float().
calc_stddev(unknown) -> unknown;

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

%% @doc Creates a random state() table within the given type specifications.
%%      Used as value_creator in tester.erl (property testing).
-spec tester_create_state(ConvCount, Leader, LoadData, RingData, Merged, Range, Round, Status,
        NoOfBuckets, Request, Instance) -> state() when
    is_subtype(ConvCount, non_neg_integer()),
    is_subtype(Leader, boolean()),
    is_subtype(LoadData, load_data_list()),
    is_subtype(RingData, ring_data()),
    is_subtype(Merged, non_neg_integer()),
    is_subtype(Range, intervals:non_empty_interval()),
    is_subtype(Round, non_neg_integer()),
    is_subtype(Status, init|uninit),
    is_subtype(NoOfBuckets, 1..50),
    is_subtype(Request, boolean()),
    is_subtype(Instance, instance()).
tester_create_state(ConvCount, Leader, LoadData, RingData, Merged, Range, Round, Status,
        NoOfBuckets, Request, Instance) ->
    NewState = ets:new(cbstate, [set, protected]),
    state_set(convergence_count, ConvCount, NewState),
    state_set(leader, Leader, NewState),
    state_set(load_data, LoadData, NewState),
    state_set(ring_data, RingData, NewState),
    state_set(merged, Merged, NewState),
    state_set(prev_state, NewState, NewState),
    state_set(range, Range, NewState),
    state_set(round, Round, NewState),
    state_set(status, Status, NewState),
    state_set(no_of_buckets, NoOfBuckets, NewState),
    state_set(request, Request, NewState),
    state_set(requestor, comm:this(), NewState),
    state_set(instance, Instance, NewState),
    % make the created state the prev state of a new state
    NewState2 = state_new(NewState, NewState),
    % state_new only creates load_data_uninit, so we reuse the given LoadData
    state_set(load_data, LoadData, NewState2),
    state_set(ring_data, RingData, NewState2),
    NewState2.


%% @doc Checks if a given ets table is a valid state.
%%      Used as type_checker in tester.erl (property testing).
-spec is_state(State::ets:tab()) -> boolean().
is_state(State) ->
    case lists:member(State, ets:all()) of
        false ->
            %% check shortly after: this is a bug in ets that we reported
            %% and that will be fixed in Erlang 17.0
            %% see: http://erlang.org/pipermail/erlang-bugs/2014-February/004054.html
            timer:sleep(15),
            case lists:member(State, ets:all()) of
                false -> false;
                true ->
                    Keys = [convergence_count, leader, load_data, merged,
                            prev_state, range, round, status],
                    lists:all(fun(Key) -> ets:member(State, Key) end, Keys)
            end;
        true ->
            Keys = [convergence_count, leader, load_data, merged,
                    prev_state, range, round, status],
            lists:all(fun(Key) -> ets:member(State, Key) end, Keys)
    end.


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
                lists:nth(random:uniform(length(ListOfAvgs)), ListOfAvgs)} end,
    lists:map(Fun, Histo1).

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

compare([H1|List1], [H2|List2]) when is_tuple(H1) andalso is_tuple(H2) ->
        {BucketInterval1, _Avg1} = H1,
        {BucketInterval2, _Avg2} = H2,
        case BucketInterval1 =:= BucketInterval2 of
            true -> compare(List1, List2);
            false -> false
        end.

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


