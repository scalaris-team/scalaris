%  @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc    Gossiping behaviour
%% @end
%% @version $Id$
-module(gossip_load).
-behaviour(gossip_beh).
-vsn('$Id$').

-include("scalaris.hrl").

-export([load_info_get/2]).

-export([init/0, init_delay/0, trigger_interval/0, select_node/1, select_data/1,
        select_reply_data/5, integrate_data/4, handle_msg/2, notify_change/3,
        min_cycles_per_round/0, max_cycles_per_round/0, round_has_converged/1,
        get_values_best/1, get_values_all/1, web_debug_info/1]).

-export_type([load_info/0]).

-define(SHOW, config:read(log_level)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% Gossip types
-type state() :: ets:tab().
-type state_key() :: convergence_count | leader | load_data | merged |
    prev_state | range | round | status.
-type avg() :: {float(), float()} | unknown.
-type size() :: float() | unknown.
-type avg_kr() :: {number(), float()} | unknown.
-type min() :: non_neg_integer() | unknown.
-type max() :: non_neg_integer() | unknown.
-type merged() :: non_neg_integer().

% record of load data values for gossiping
-record(load_data, {
            avg       = unknown :: avg(), % average load
            avg2      = unknown :: avg(), % average of load^2
            size_inv  = unknown :: avg(), % 1 / size
            avg_kr    = unknown :: avg_kr(), % average key range (distance between nodes in the address space)
            min       = unknown :: min(),
            max       = unknown :: max()
    }).
-type(load_data() :: #load_data{}).

% record of load data values for use by other modules
-record(load_info, {
            avg       = unknown :: avg(),  % average load
            stddev    = unknown :: float(), % standard deviation of the load
            size_ldr  = unknown :: size(), % estimated ring size: 1/(average of leader=1, others=0)
            size_kr   = unknown :: size(), % estimated ring size based on average key ranges
            min       = unknown :: min(), % minimum load
            max       = unknown :: max(), % maximum load
            merged    = 0 :: merged() % how often the data was merged since the node entered/created the round
    }).
-type(load_info() :: #load_info{}). %% @todo: mark as opaque (dialyzer currently crashes if set)


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Config Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% External config function (called by bh module) %%

%% @doc Defines how many ms the initialization of gossip_load is delayed in
%%      relation to the bh module (non-blocking).
%%      Some metrics (e.g. size) wont work properly in first round when started
%%      without delay.
-spec init_delay() -> non_neg_integer().
init_delay() -> % in ms
    1000.

%% @doc The time interval in ms in which message exchanges are initiated.
-spec trigger_interval() -> pos_integer().
trigger_interval() -> % in ms
    1000.

%% @doc The minimum number of cycles per round.
%%      Only full cycles (i.e. received replies) are counted (ignored triggers
%%      do not count as cycle).
%%      Only relevant for leader, all other nodes enter rounds when told to do so.
-spec min_cycles_per_round() -> non_neg_integer().
min_cycles_per_round() ->
    10.

%% @doc The maximum number of cycles per round.
%%      Only full cycles (i.e. received replies) are counted (ignored triggers
%%      do not count as cycle).
%%      Only relevant for leader, all other nodes enter rounds when told to do so.
-spec max_cycles_per_round() -> pos_integer().
max_cycles_per_round() ->
    1000.

%% Private config functions %%

-spec convergence_count_best_values() -> pos_integer().
convergence_count_best_values() ->
% convergence is counted (on average) twice every cycle:
% 1) When receiving the answer to an request (integrate_data()) (this happens
%       once every cycle).
% 2) When receiving a request (select_reply_data()) (this happens randomly,
%       but *on average* once per round).
    10.

-spec convergence_count_new_round() -> pos_integer().
convergence_count_new_round() ->
    20.

-spec convergence_epsilon() -> float().
convergence_epsilon() ->
    5.0.

-spec discard_old_rounds() -> boolean().
discard_old_rounds() ->
    false.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init() -> {ok, state()}.
init() ->
    log:log(debug, "[ ~w ] CBModule initiated", [?MODULE]),
    State = state_new(),
    state_set(prev_state, unknown, State),
    {ok, State}.


-spec select_node(State::state()) -> {boolean(), state()}.
select_node(State) ->
    {false, State}.

-spec select_data(State::state()) -> {ok, state()}.
select_data(State) ->
    log:log(debug, "[ ~w ] select_data: State: ~w", [?MODULE, State]),
    case state_get(status, State) of
        uninit ->
            request_local_info();
        init ->
            LoadData = prepare_load_data(State),
            Pid = pid_groups:get_my(gossip2),
            comm:send_local(Pid, {selected_data, ?MODULE, LoadData})
    end,
    {ok, State}.

-spec select_reply_data(PData::load_data(), Ref::reference(),
    RoundStatus::gossip_beh:round_status(), Round::non_neg_integer(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
select_reply_data(PData, Ref, RoundStatus, Round, State) ->

    IsValidRound = is_valid_round(RoundStatus, Round, State),

    case state_get(status, State) of
        uninit ->
            log:log(?SHOW, "[ ~w ] select_reply_data in uninit", [?MODULE]),
            {retry,State};
        init when not IsValidRound ->
            log:log(warn, "[ ~w ] Discarded Data. PData: ~w", [?MODULE, PData]),
            {send_back, State};
        init when RoundStatus =:= current_round ->
            Data1 = prepare_load_data(State),
            Pid = pid_groups:get_my(gossip2),
            comm:send_local(Pid, {selected_reply_data, ?MODULE, Data1, Ref, Round}),

            _ = merge_load_data(current_round, PData, State),

            log:log(debug, "[ ~w ] Data after select_reply_data: ~w", [?MODULE, get_load_info(State)]),
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
                    Data1 = prepare_load_data(PrevState),
                    Pid = pid_groups:get_my(gossip2),
                    comm:send_local(Pid, {selected_reply_data, ?MODULE, Data1, Ref, Round}),
                    _ = merge_load_data(prev_round, PData, State),
                    {ok, State}
            end
    end.

-spec integrate_data(QData::load_data(), RoundStatus::gossip_beh:round_status(),
    Round::non_neg_integer(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
integrate_data(QData, RoundStatus, Round, State) ->
    log:log(debug, "[ ~w ] Reply-Data: ~w~n", [?MODULE, QData]),

    IsValidRound = is_valid_round(RoundStatus, Round, State),

    case state_get(status, State) of
        _ when not IsValidRound ->
            log:log(warn, "[ ~w ] Discarded Data. QData: ~w", [?MODULE, QData]),
            {send_back, State};
        uninit ->
            log:log(?SHOW, "[ ~w ] integrate_data in uninit", [?MODULE]),
            {retry,State};
        init ->
            integrate_data_init(QData, RoundStatus, State)
    end.


-spec integrate_data_init(QData::load_data(), RoundStatus::gossip_beh:round_status(),
    State::state()) -> {ok, state()}.
integrate_data_init(QData, RoundStatus, State) ->
    case RoundStatus of
        current_round ->
            _NewData = merge_load_data(current_round, QData, State),
            %% _PrevState = state_get(prev_state, State),
            %% _PrevLoadInfo = get_load_info(_PrevState),
            %% _ValuesBest = get_values_best(State),
            %% _ValuesAll = get_values_all(State),
            %% log:log(debug, "[ ~w ] Values best: ~n\t~w", [?MODULE, _ValuesBest]),
            %% log:log(debug, "[ ~w ] Values all: ~n\t~w~n", [?MODULE, _ValuesAll]),
            %% log:log(debug, "[ ~w ] Values prev round: ~n\t~w", [?MODULE, _PrevLoadInfo]),
            _LoadInfo = get_load_info(State),
            log:log(?SHOW, "[ ~w ] Data at end of cycle: ~w~n", [?MODULE, _LoadInfo]);
        old_round ->
             case discard_old_rounds() of
                true ->
                    log:log(debug, "[ ~w ] integrate_data in old_round", [?MODULE]),
                    % This only happens when entering a new round, i.e. the values have
                    % already converged. Consequently, discarding messages does
                    % not cause mass loss.
                    do_nothing;
                false ->
                    log:log(?SHOW, "[ ~w ] integrate_data in old_round", [?MODULE]),
                    merge_load_data(prev_round, QData, State)
            end
    end,
    Pid = pid_groups:get_my(gossip2),
    comm:send_local(Pid, {integrated_data, ?MODULE, RoundStatus}),
    {ok, State}.


-spec handle_msg(Message::{get_node_details_response, NodeDetails::node_details:node_details()},
    State::state()) -> {ok, state()}.
handle_msg({get_node_details_response, NodeDetails}, State) ->
    Load = node_details:get(NodeDetails, load),
    log:log(?SHOW, "[ ~w ] Load: ~w", [?MODULE, Load]),

    Data = state_get(load_data, State),
    Data1 = load_data_set(avg, {float(Load), 1.0}, Data),
    Data2 = load_data_set(min, Load, Data1),
    Data3 = load_data_set(max, Load, Data2),
    Data4 = load_data_set(avg2, {float(Load*Load), 1.0}, Data3),
    Data5 = case (state_get(leader, State)) of
        true ->
            load_data_set(size_inv, {1.0, 1.0}, Data4);
        false ->
            load_data_set(size_inv, {1.0, 0.0}, Data4)
    end,
    AvgKr = calc_initial_avg_kr(state_get(range, State)),
    Data6 = load_data_set(avg_kr, AvgKr, Data5),
    state_set(load_data, Data6, State),

    NewData = prepare_load_data(State),

    state_set(status, init, State),

    % send PData to BHModule
    Pid = pid_groups:get_my(gossip2),
    comm:send_local(Pid, {selected_data, ?MODULE, NewData}),
    {ok, State}.

-spec round_has_converged(State::state()) -> {boolean(), state()}.
round_has_converged(State) ->
    ConvergenceCount = convergence_count_new_round(),
    {has_converged(ConvergenceCount, State), State}.

-spec has_converged(TargetConvergenceCount::pos_integer(), State::state()) -> boolean().
has_converged(TargetConvergenceCount, State) ->
    CurrentConvergenceCount = state_get(convergence_count, State),
    CurrentConvergenceCount >= TargetConvergenceCount.


-spec notify_change(Keyword::new_round, NewRound::non_neg_integer(),
            State::state()) -> {ok, state()};
    (Keyword::leader, {MsgTag::is_leader | no_leader,
            NewRange::intervals:interval()}, State::state()) -> {ok, state()};
    (Keyword::exch_failure, {_MsgTag::atom(), Data::load_data(),
            _Round::non_neg_integer()}, State::state()) -> {ok, state()}.
notify_change(new_round, NewRound, State) ->
    log:log(debug, "[ ~w ] new_round notification. NewRound: ~w", [?MODULE, NewRound]),

    % only replace prev round with current round if current has converged
    % cases in which current round has not converged: e.g. late joining, sleeped/paused
    % ConvergenceTarget should be less than covergence_count_new_round,
    % otherwise non leader groups seldom replace prev_load_data
    NewState = case has_converged(convergence_count_best_values(), State) of
        true ->
            % make the current state the previous state and delete the old prev state
            OldPrevState = state_get(prev_state, State),
            state_delete(OldPrevState),
            state_new(State);
        false ->
            % make the old previous state the new previous state and discard the current state
            log:log(warn, "[ ~w ] Entering new round, but old round has not converged", [?MODULE]),
            PrevState = state_get(prev_state, State),
            NewState1 = state_new(PrevState),
            state_set(leader, state_get(leader, State), NewState1),
            state_set(range, state_get(range, State), NewState1),
            state_delete(State),
            NewState1
    end,
    state_set(round, NewRound, NewState),
    {ok, NewState};

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


%% @doc atm, returns unknown if no previous exists (todo change??)
-spec get_values_best(state()) -> {load_info(), state()}.
get_values_best(State) ->
    BestState = previous_or_current(State),
    LoadInfo = get_load_info(BestState),
    {LoadInfo, State}.


-spec get_values_all(state()) ->
    { {PreviousInfo::load_info(), CurrentInfo::load_info(), BestInfo::load_info()},
        NewState::state() }.
get_values_all(State) ->
    BestState = previous_or_current(State),
    InfosAll = lists:map(fun get_load_info/1, [state_get(prev_state, State), State, BestState]),
    {list_to_tuple(InfosAll), State}.


-spec web_debug_info(state()) ->
    {KeyValueList::[{Key::any(),Value::any()},...], NewState::state()}.
web_debug_info(State) ->
    PreviousState = state_get(prev_state, State),
    PreviousData = prev_state_get(load_data, State),
    CurrentData = state_get(load_data, State),
    BestState = previous_or_current(State),
    Best = if BestState =:= State -> current_data;
              BestState =:= PreviousState -> previous_data
        end,
    KeyValueList =
        [{"gossip_load", ""},
         {"best", Best},
         {"leader", state_get(leader, State)},
         {"prev_round",          state_get(round, PreviousState)},
         {"prev_merged",         state_get(merged, PreviousState)},
         {"prev_conv_avg_count",  state_get(convergence_count, PreviousState)},
         {"prev_avg",            get_current_estimate(avg, PreviousData)},
         {"prev_min",            load_data_get(min, PreviousData)},
         {"prev_max",            load_data_get(max, PreviousData)},
         {"prev_stddev",         calc_stddev(PreviousData)},
         {"prev_size_ldr",       get_current_estimate(size_inv, PreviousData)},
         {"prev_size_kr",        calc_size_kr(PreviousData)},

         {"cur_round",           state_get(round, State)},
         {"cur_merged",          state_get(merged, State)},
         {"cur_conv_avg_count",  state_get(convergence_count, State)},
         {"cur_avg",             get_current_estimate(avg, CurrentData)},
         {"cur_min",             load_data_get(min, CurrentData)},
         {"cur_max",             load_data_get(max, CurrentData)},
         {"cur_stddev",          calc_stddev(CurrentData)},
         {"cur_size_ldr",        get_current_estimate(size_inv, CurrentData)},
         {"cur_size_kr",         calc_size_kr(CurrentData)}],
    {KeyValueList, State}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers / Private Methods
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


-spec request_local_info() -> ok.
request_local_info() ->
    % ask for local load:
    DHT_Node = pid_groups:get_my(dht_node),
    EnvPid = comm:reply_as(comm:this(), 3, {cb_reply, ?MODULE, '_'}),
    comm:send(comm:make_global(DHT_Node), {get_node_details, EnvPid, [load]}).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State of gossip_load: Getters, Setters and Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Create a new state table with some initial values.
-spec state_new() -> state().
state_new() ->
    NewState = ets:new(cbstate, [set, protected]),
    state_set(status, uninit, NewState),
    state_set(load_data, load_data_new(), NewState),
    Fun = fun (Key) -> state_set(Key, 0, NewState) end,
    lists:foreach(Fun, [round, merged, reqs_send, reqs_recv, replies_recv, convergence_count]),
    NewState.


%% @doc Create a new state table with information from another state table.
%%      This is used to build a new state from the current state when a new round
%%      is entered. Some values (mainly counters) are resetted, others are taken
%%      from the given state. The given state is set as prev_state of the new
%%      state.
-spec state_new(PrevState::state()) -> state().
state_new(PrevState) ->
    NewState = state_new(),
    state_set(prev_state, PrevState, NewState),
    state_set(leader, state_get(leader, PrevState), NewState),
    state_set(range, state_get(range, PrevState), NewState),
    NewState.


-spec state_delete(State::unknown | state()) -> true.
state_delete(unknown) -> do_nothing;

state_delete(Name) ->
    ets:delete(Name).


-spec state_get(Key::state_key(), State::state() | unknown) -> any().
state_get(_Key, unknown) ->
    unknown;

state_get(Key, State) ->
    case ets:lookup(State, Key) of
        [] ->
            log:log(error, "[ ~w ] Lookup of ~w in ~w failed~n", [?MODULE, Key, State]),
            error(lookup_failed, [Key, State]);
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


-spec update_convergence_count(OldData::load_data(), NewData::load_data(), State::state()) -> true.
update_convergence_count(OldData, NewData, State) ->

    % check whether all average based values changed less than epsilon percent
    AvgChangeEpsilon = convergence_epsilon(),
    Fun = fun(AvgType, Acc) ->
            {OldAvg, OldWeight} = load_data_get(AvgType, OldData),
            {NewAvg, NewWeight} = load_data_get(AvgType, NewData),
            OldValue = calc_current_estimate(OldAvg, OldWeight),
            NewValue = calc_current_estimate(NewAvg, NewWeight),
            log:log(debug, "[ ~w ] ~w: OldValue: ~w, New Value: ~w",
                [?MODULE, AvgType, OldValue, NewValue]),
            HasConverged = calc_change(OldValue, NewValue) < AvgChangeEpsilon,
            Acc andalso HasConverged
    end,
    HaveConverged = lists:foldl(Fun, true, [avg, avg2, size_inv, avg_kr]),

    case HaveConverged of
        true ->
            state_update(convergence_count, fun inc/1, State);
        false ->
            state_set(convergence_count, 0, State)
    end.


-spec is_valid_round(RoundStatus::gossip_beh:round_status(),
    RoundFromMessage::non_neg_integer(), State::state()) -> boolean().
is_valid_round(RoundStatus, RoundFromMessage, State) ->
    RoundFromState = case RoundStatus of
        current_round -> state_get(round, State);
        old_round -> prev_state_get(round, State)
    end,

    case RoundFromState =:= RoundFromMessage of
        true -> true;
        false ->
            log:log(debug, "[ ~w ] Invalid ~w. RoundFromState: ~w, RoundFromMessage: ~w",
                [?MODULE, RoundStatus, RoundFromState, RoundFromMessage]),
            false
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Load Data
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc creates a new load_data (for internal use by gossip_load.erl)
-spec load_data_new() -> load_data().
load_data_new() ->
    #load_data{}.

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
-spec load_data_get(_, unknown) -> unknown;
         (avg, load_data()) -> avg();
         (avg2, load_data()) -> avg();
         (size_inv, load_data()) -> avg();
         (avg_kr, load_data()) -> avg_kr();
         (min, load_data()) -> min();
         (max, load_data()) -> max().
load_data_get(_Key, unknown) -> unknown;

load_data_get(Key, #load_data{avg=Avg, avg2=Avg2, size_inv=Size_inv, avg_kr=AvgKR,
        min=Min, max=Max}) ->
    case Key of
        avg -> Avg;
        avg2 -> Avg2;
        size_inv -> Size_inv;
        avg_kr -> AvgKR;
        min -> Min;
        max -> Max
    end.


-spec get_current_estimate(Key::avg | avg2 | size_inv | avg_kr,
    LoadData::load_data()) -> float() | unknown.
get_current_estimate(_Key, unknown) -> unknown;
get_current_estimate(Key, #load_data{avg=Avg, avg2=Avg2, size_inv=Size_inv,
        avg_kr=AvgKR}) ->
    case Key of
        avg -> calc_current_estimate(Avg);
        avg2 -> calc_current_estimate(Avg2);
        size_inv ->  calc_current_estimate(Size_inv);
        avg_kr ->  calc_current_estimate(AvgKR)
    end.

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
-spec load_data_set(avg, avg(), load_data()) -> load_data();
         (min, min(), load_data()) -> load_data();
         (max, max(), load_data()) -> load_data();
         (avg2, avg(), load_data()) -> load_data();
         (size_inv, avg(), load_data()) -> load_data();
         (avg_kr, avg_kr(), load_data()) -> load_data();
         (round, current_round|prev_round, load_data()) -> load_data().
load_data_set(Key, Value, LoadData) when is_record(LoadData, load_data) ->
    case Key of
        avg -> LoadData#load_data{avg = Value};
        avg2 -> LoadData#load_data{avg2 = Value};
        size_inv -> LoadData#load_data{size_inv = Value};
        avg_kr -> LoadData#load_data{avg_kr = Value};
        min -> LoadData#load_data{min = Value};
        max -> LoadData#load_data{max = Value}
    end.


%% @doc Prepares a load_data record for sending it to a peer and updates the
%%      load_data of self accordingly.
-spec prepare_load_data(State::state()) -> load_data().
prepare_load_data(State) ->
    % Averages
    Data1 = state_get(load_data, State),
    Fun = fun (AvgType, MyData) -> prepare_avg(AvgType, MyData) end,
    NewData = lists:foldl(Fun, Data1, [avg, avg2, size_inv, avg_kr]),

    % Min and Max
    do_nothing,

    state_set(load_data, NewData, State),
    NewData.


%% @doc Cut the average values and associated weights in half.
-spec prepare_avg(AvgType:: avg | avg2 | size_inv | avg_kr, MyData::load_data()) ->
    load_data().
prepare_avg(AvgType, MyData) ->
    {Avg, Weight} = load_data_get(AvgType, MyData),
    NewAvg = Avg/2,
    NewWeight = Weight/2,
    load_data_set(AvgType, {NewAvg, NewWeight}, MyData).


%% @doc Merges the load_data from a failed exchange back into self's load_data.
%%      Does not update the merge and convergence count.
-spec merge_failed_exch_data(Data::load_data(), State::state()) -> load_data().
merge_failed_exch_data(Data, State) ->
    merge_load_data1(noupdate, Data, State).


%% @doc Merges the given load_data into self's load_data of the current or
%%      previous round.
-spec merge_load_data(current_round, OtherData::load_data(), State::state()) -> load_data();
        (prev_round, OtherData::load_data(), State::state()) -> load_data().
merge_load_data(current_round, OtherData, State) ->
    merge_load_data1(update, OtherData, State);

merge_load_data(prev_round, OtherData, State) ->
    PrevState = state_get(prev_state, State),
    merge_load_data1(update, OtherData, PrevState).


%% @doc Helper function. Merge the given load_data with the given states load_data.
%%      Set update is set, the merge and convergence count is updated, otherwise not.
-spec merge_load_data1(Update::update | noupdate, OtherData::load_data(),
    State::state()) -> load_data().
merge_load_data1(Update, OtherData, State) ->

    MyData1 = state_get(load_data, State),

    % Averages
    Fun = fun (AvgType, MyData) -> merge_avg(AvgType, MyData, OtherData) end,
    MyData2 = lists:foldl(Fun, MyData1, [avg, avg2, size_inv, avg_kr]),

    % Min
    MyMin = load_data_get(min, MyData2),
    OtherMin = load_data_get(min, OtherData),
    MyData3 =
        if  MyMin =< OtherMin -> load_data_set(min, OtherMin, MyData2);
            MyMin > OtherMin -> load_data_set(min, MyMin, MyData2)
        end,

    % Max
    MyMax = load_data_get(max, MyData3),
    OtherMax = load_data_get(max, OtherData),
    MyData4 =
        if  MyMax =< OtherMax -> load_data_set(max, MyMax, MyData3);
            MyMax > OtherMax -> load_data_set(max, OtherMax, MyData3)
        end,

    NewData = MyData4,
    case Update of
        update ->
            state_update(merged, fun inc/1, State),
            update_convergence_count(MyData1, NewData, State);
        noupdate -> do_nothing
    end,
    state_set(load_data, NewData, State),
    NewData.


%% @doc Helper function. Merges the given type of average from the given
%%      load_data records.
-spec merge_avg(AvgType:: avg | avg2 | size_inv | avg_kr,
    Data1::load_data(), Data2::load_data()) -> load_data().
merge_avg(AvgType, Data1, Data2) ->
    {AvgLoad1, AvgWeight1} = load_data_get(AvgType, Data1),
    {AvgLoad2, AvgWeight2} = load_data_get(AvgType, Data2),
    NewAvg = {AvgLoad1+AvgLoad2, AvgWeight1+AvgWeight2},
    load_data_set(AvgType, NewAvg, Data1).


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


%% @doc Builds a load_info record from the given state (current or previous state)
-spec get_load_info(State::state()) -> load_info().
get_load_info(State) ->
    Data = state_get(load_data, State),
    _LoadInfo =
        #load_info{ avg       = get_current_estimate(avg, Data),
                    stddev    = calc_stddev(Data),
                    size_ldr  = get_current_estimate(size_inv, Data),
                    size_kr   = calc_size_kr(Data),
                    min       = load_data_get(min, Data),
                    max       = load_data_get(max, Data),
                    merged    = state_get(merged, State)
            }.


%% @doc Gets the value to the given key from the given load_info record.
-spec load_info_get(Key::avgLoad, LoadInfoRecord::load_info()) -> avg();
                   (Key::stddev, LoadInfoRecord::load_info()) -> float();
                   (Key::size_ldr, LoadInfoRecord::load_info()) -> size();
                   (Key::size_kr, LoadInfoRecord::load_info()) -> size();
                   (Key::minLoad, LoadInfoRecord::load_info()) -> min();
                   (Key::maxLoad, LoadInfoRecord::load_info()) -> max();
                   (Key::merged, LoadInfoRecord::load_info()) -> merged().
load_info_get(Key, #load_info{avg=Avg, stddev=Stddev, size_ldr=SizeLdr,
        size_kr=SizeKr, min=Min, max=Max, merged=Merged}) ->
    case Key of
        avgLoad -> Avg;
        stddev -> Stddev;
        size_ldr -> SizeLdr;
        size_kr -> SizeKr;
        minLoad -> Min;
        maxLoad -> Max;
        merged -> Merged
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Calculations on Load Data Values
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculate the current estimate from a given {Value, Weight} tuple.
-spec calc_current_estimate(unknown | avg()) -> float() | unknown.
calc_current_estimate(unknown) -> unknown;

calc_current_estimate({Value, Weight}) ->
    calc_current_estimate(Value, Weight).


%% @doc Calculate the current estimate from the given value and, weight.
-spec calc_current_estimate(Value::float(), Weight::float()) -> float() | unknown.
calc_current_estimate(Value, Weight) ->
    try Value/Weight
    catch
        error:badarith -> unknown
    end.


%% @doc Calculates the change in percent from the Old value to the New value.
-spec calc_change(OldValue::float(), NewValue::float()) -> float().
calc_change(OldValue, NewValue) ->
    if
        (OldValue =/= unknown) andalso (OldValue =:= NewValue) -> 0.0;
        (OldValue =:= unknown) orelse (NewValue =:= unknown) orelse (OldValue == 0) -> 100.0;
        true -> ((OldValue + abs(NewValue - OldValue)) * 100.0 / OldValue) - 100
    end.


%% @doc Calculates the difference between the key of a node and its
%%      predecessor. If the second is larger than the first it wraps around and
%%      thus the difference is the number of keys from the predecessor to the
%%      end (of the ring) and from the start to the current node.
%%      Pre: MyRange is continuous
-spec calc_initial_avg_kr(MyRange::intervals:interval()) -> avg_kr() | unknown.
calc_initial_avg_kr(MyRange) ->
    {_, PredKey, MyKey, _} = intervals:get_bounds(MyRange),
    % we don't know whether we can subtract keys of type ?RT:key()
    % -> try it and if it fails, return unknown
    try {?RT:get_range(PredKey, MyKey), 1}
    catch
        throw:not_supported -> unknown
    end.


%% @doc Extracts and calculates the standard deviation from the load_data record
-spec calc_stddev(Data::unknown | load_data()) -> float().
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
-spec calc_size_kr(unknown | load_data()) -> size().
calc_size_kr(unknown) -> unknown;

calc_size_kr(Data) when is_record(Data, load_data) ->
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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Increments given value with one.
-spec inc(Value::integer()) -> integer().
inc(Value) ->
    Value+1.
