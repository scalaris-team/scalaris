%  @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc    Framework for estimating aggregated global properties using
%%         gossip techniques.
%%  
%%  Gossiping is organized in rounds. At the start of each round, a node's
%%  gossip process has to ask its dht_node for information about its state,
%%  i.e. its load and the IDs of itself and its predecessor. It will not
%%  participate in any state exchanges until this information has been
%%  received and postpone such messages, i.e. 'get_state', if received. All
%%  other messages, e.g. requests for estimated values, are either ignored
%%  (only applies to "gossip_trigger") or answered with information from the previous
%%  round, e.g. requests for (estimated) system properties.
%%  
%%  When these values are successfully integrated into the process state,
%%  and the node entered some round, it will continuously ask the cyclon
%%  process for a random node to exchange its state with and update the local
%%  estimates (the interval is defined in gossip_interval given by
%%  scalaris.cfg).
%%  
%%  New rounds are started by the leader which is identified as the node for
%%  which
%%  intervals:in(?RT:hash_key("0"), node_details:get(NodeDetails, my_range))
%%  is true. It will propagate its round with its state so that gossip
%%  processes of other nodes can join this round. Several parameters (added
%%  to scalaris.cfg) influence the decision about when to start a new round:
%%  <ul>
%%   <li>gossip_max_triggers_per_round: a new round is started if this many
%%       triggers have been received during the round</li>
%%   <li>gossip_min_triggers_per_round: a new round is NOT started until this
%%       many triggers have been received during the round</li>
%%   <li>gossip_converge_avg_count_start_new_round: if the estimated values
%%       did not change by more than gossip_converge_avg_epsilon percent this
%%       many times, assume the values have converged and start a new round
%%   </li>
%%   <li>gossip_converge_avg_epsilon: (see
%%       gossip_converge_avg_count_start_new_round)</li>
%%  </ul>
%%  
%%  Each process stores the estimates of the current round (which might not
%%  have been converged yet) and the previous estimates. If another process
%%  asks for gossip's best values it will favor the previous values but return
%%  the current ones if they have not changes by more than
%%  gossip_converge_avg_epsilon percent gossip_converge_avg_count times.
%% @end
%% @reference M. Jelasity, A. Montresor, O. Babaoglu: Gossip-based aggregation
%% in large dynamic networks. ACM Trans. Comput. Syst. 23(3), 219-252 (2005)
%% @version $Id$
-module(gossip).
-author('kruber@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).

% functions gen_component, the trigger and the config module use
-export([init/1, on_inactive/2, on_active/2,
         activate/1, deactivate/0,
         get_base_interval/0, check_config/0]).

% interaction with the ring maintenance:
-export([rm_my_range_changed/3, rm_send_new_range/4]).

-type state() :: gossip_state:state().

%% Full state of the gossip process:
%% {PreviousState, CurrentState, QueuedMessages, TriggerState}
%% -> previous and current state, queued messages (get_state messages received
%% before local values are known) and the state of the trigger.
-type full_state_active() :: {PreviousState::state(), CurrentState::state(),
                              MessageQueue::msg_queue:msg_queue(),
                              TriggerState::trigger:state(),
                              Range::intervals:interval()}.
-type full_state_inactive() :: {uninit, QueuedMessages::msg_queue:msg_queue(),
                                TriggerState::trigger:state(),
                                PreviousState::state()}.
%% -type(full_state() :: full_state_active() | full_state_inactive()).

% accepted messages of gossip processes
-type(message() ::
    {gossip_trigger} |
    {get_node_details_response, node_details:node_details()} |
    {update_range, NewRange::intervals:interval()} |
    {get_state, comm:mypid(), gossip_state:values_internal()} |
    {get_state_response, gossip_state:values_internal()} |
    {cy_cache, RandomNodes::[node:node_type()]} |
    {get_values_all, SourcePid::comm:erl_local_pid()} | 
    {get_values_best, SourcePid::comm:erl_local_pid()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

% prevent warnings in the log by mis-using comm:send_with_shepherd/3
%-define(SEND_TO_GROUP_MEMBER(Pid, Process, Msg), comm:send_to_group_member(Pid, Process, Msg)).
-define(SEND_TO_GROUP_MEMBER(Pid, Process, Msg), comm:send_with_shepherd(Pid, {send_to_group_member, Process, Msg} , self())).

%% @doc Activates the gossip process. If not activated, the gossip process will
%%      queue most messages without processing them.
-spec activate(MyRange::intervals:interval()) -> ok.
activate(MyRange) ->
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {activate_gossip, MyRange}).

%% @doc Deactivates the gossip process.
-spec deactivate() -> ok.
deactivate() ->
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {deactivate_gossip}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions that create and send messages to nodes requesting information.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a response message to a request for the best stored values.
-spec msg_get_values_best_response(comm:erl_local_pid(), gossip_state:values()) -> ok.
msg_get_values_best_response(Pid, BestValues) ->
    comm:send_local(Pid, {gossip_get_values_best_response, BestValues}).

%% @doc Sends a response message to a request for all stored values.
-spec msg_get_values_all_response(comm:erl_local_pid(), gossip_state:values(), gossip_state:values(), gossip_state:values()) -> ok.
msg_get_values_all_response(Pid, PreviousValues, CurrentValues, BestValues) ->
    comm:send_local(Pid, {gossip_get_values_all_response, PreviousValues, CurrentValues, BestValues}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the gossip process, registers it with the process dictionary and
%%      returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(gossip_trigger),
    gen_component:start_link(?MODULE, Trigger, [{pid_groups_join_as, DHTNodeGroup, gossip}]).

%% @doc Initialises the module with an empty state.
-spec init(module()) -> {'$gen_component', [{on_handler, Handler::on_inactive}], State::full_state_inactive()}.
init(Trigger) ->
    TriggerState = trigger:init(Trigger, fun get_base_interval/0, gossip_trigger),
    gen_component:change_handler({uninit, msg_queue:new(), TriggerState, gossip_state:new_state()},
                                 on_inactive).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler during start up phase (will change to on_active/2 when a
%%      'activate_gossip' message is received). Queues getter-messages for
%%      faster startup of dependent processes. 
-spec on_inactive(message(), full_state_inactive()) -> full_state_inactive();
                 ({activate_gossip, MyRange::intervals:interval()}, full_state_inactive()) -> {'$gen_component', [{on_handler, Handler::on_active}], State::full_state_active()}.
on_inactive({activate_gossip, MyRange},
            {uninit, QueuedMessages, TriggerState, PreviousState}) ->
    log:log(info, "[ Gossip ~.0p ] activating...~n", [comm:this()]),
    TriggerState2 = trigger:now(TriggerState),
    rm_loop:subscribe(self(), ?MODULE,
                      fun gossip:rm_my_range_changed/3,
                      fun gossip:rm_send_new_range/4, inf),
    State = gossip_state:new_state(),
    msg_queue:send(QueuedMessages),
    gen_component:change_handler({PreviousState, State, [], TriggerState2, MyRange}, on_active);

on_inactive(Msg = {get_values_all, _SourcePid},
            {uninit, QueuedMessages, TriggerState, PreviousState}) ->
    {uninit, msg_queue:add(QueuedMessages, Msg), TriggerState, PreviousState};

on_inactive(Msg = {get_values_best, _SourcePid},
            {uninit, QueuedMessages, TriggerState, PreviousState}) ->
    {uninit, msg_queue:add(QueuedMessages, Msg), TriggerState, PreviousState};

on_inactive({web_debug_info, Requestor},
            {uninit, QueuedMessages, _TriggerState, PreviousState} = State) ->
    % get a list of up to 50 queued messages to display:
    MessageListTmp = [{"", lists:flatten(io_lib:format("~p", [Message]))}
                  || Message <- lists:sublist(QueuedMessages, 50)],
    MessageList = case length(QueuedMessages) > 50 of
                      true -> lists:append(MessageListTmp, [{"...", ""}]);
                      _    -> MessageListTmp
                  end,
    KeyValueList =
        [{"", ""}, {"inactive gossip process", ""},
         {"prev_round",          gossip_state:get(PreviousState, round)},
         {"prev_triggered",      gossip_state:get(PreviousState, triggered)},
         {"prev_msg_exch",       gossip_state:get(PreviousState, msg_exch)},
         {"prev_conv_avg_count", gossip_state:get(PreviousState, converge_avg_count)},
         {"prev_avg",            gossip_state:get(PreviousState, avgLoad)},
         {"prev_min",            gossip_state:get(PreviousState, minLoad)},
         {"prev_max",            gossip_state:get(PreviousState, maxLoad)},
         {"prev_stddev",         gossip_state:calc_stddev(PreviousState)},
         {"prev_size_ldr",       gossip_state:calc_size_ldr(PreviousState)},
         {"prev_size_kr",        gossip_state:calc_size_kr(PreviousState)},
         {"", ""},
         {"queued messages:", ""} | MessageList],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on_inactive(_Msg, State) ->
    State.

%% @doc Message handler when the process is activated.
-spec on_active(message(), full_state_active()) -> full_state_active();
               ({deactivate_gossip}, full_state_active()) -> {'$gen_component', [{on_handler, Handler::on_inactive}], State::full_state_inactive()}.
on_active({deactivate_gossip},
          {PreviousState, _State, _QueuedMessages, TriggerState, _MyRange}) ->
    log:log(info, "[ Gossip ~.0p ] deactivating...~n", [comm:this()]),
    rm_loop:unsubscribe(self(), ?MODULE),
    gen_component:change_handler({uninit, msg_queue:new(), TriggerState, PreviousState},
                                 on_inactive);

% Only integrate the new range on activate_gossip messages in active state.
% note: remove this if the gossip process is to be deactivated on leave (see
% dht_node_move.erl). In the current implementation we can not distinguish
% between the first join and a re-join but after every join, the process is
% (re-)activated.
on_active({activate_gossip, NewRange},
          {PreviousState, State, QueuedMessages, TriggerState, _OldMyRange}) ->
    {PreviousState, State, QueuedMessages, TriggerState, NewRange};

on_active({gossip_trigger},
          {PreviousState, State, QueuedMessages, TriggerState, MyRange}) ->
    % this message is received continuously when the Trigger calls
    % see gossip_trigger and gossip_interval in the scalaris.cfg file
    NewTriggerState = trigger:next(TriggerState),
    NewState1 = gossip_state:inc_triggered(State),
    % request a check whether we are the leader and can thus decide whether to
    % start a new round
    {NewPreviousState, NewState2} =
        check_round(PreviousState, NewState1, MyRange),
    Round = gossip_state:get(NewState2, round),
    Initialized = gossip_state:get(NewState2, initialized),
    % only participate in (active) gossiping if we entered some valid round and
    % we have information about our node's load and key range
    case (Round > 0) andalso Initialized of
        true -> request_random_node();
        false -> ok
    end,
    {NewPreviousState, NewState2, QueuedMessages, NewTriggerState, MyRange};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Responses to requests for information about the local node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on_active({get_node_details_response, NodeDetails},
          {PreviousState, State, QueuedMessages, TriggerState, MyRange}) ->
    % this message is received when the (local) node was asked to tell us its
    % load and key range
%%     io:format("gossip: got get_node_details_response: ~p~n",[NodeDetails]),
    Initialized = gossip_state:get(State, initialized),
    {NewQueuedMessages, NewState} =
        case Initialized of
            true -> {msg_queue:new(), State};
            false ->
                msg_queue:send(QueuedMessages),
                Load = node_details:get(NodeDetails, load),
                {msg_queue:new(), integrate_local_info(State, Load)}
        end,
    {PreviousState, NewState, NewQueuedMessages, TriggerState, MyRange};

on_active({update_range, NewRange},
          {PreviousState, State, QueuedMessages, TriggerState, _OldMyRange}) ->
    {PreviousState, State, QueuedMessages, TriggerState, NewRange};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State exchange
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on_active({get_state, Source_PID, OtherValues} = Msg,
          {MyPreviousState, MyState, QueuedMessages, TriggerState, MyRange}) ->
    % This message is received when a node asked our node for its state.
    % The piggy-backed other node's state will be used to update our own state
    % if we have already initialized it (otherwise postpone the message). A
    % get_state_response message is sent with our state in the first case.
    Initialized = gossip_state:get(MyState, initialized),
    {NewQueuesMessages, {MyNewPreviousState, MyNewState}} =
        case Initialized of
            true ->
                {QueuedMessages,
                 integrate_state(OtherValues, MyPreviousState, MyState, true,
                                 Source_PID, MyRange)};
            false ->
                {msg_queue:add(QueuedMessages, Msg),
                 enter_round(MyPreviousState, MyState, OtherValues, MyRange)}
            end,
    {MyNewPreviousState, MyNewState, NewQueuesMessages, TriggerState, MyRange};

on_active({send_error, _Target, {send_to_group_member, gossip, {get_state, _, _}}}, State) ->
    % ignore (node availability is not that important to gossip)
    State;

on_active({get_state_response, OtherValues},
          {MyPreviousState, MyState, QueuedMessages, TriggerState, MyRange}) ->
    % This message is received as a response to a get_state message and contains
    % another node's state. We will use it to update our own state
    % if both are valid.
    {MyNewPreviousState, MyNewState} =
        integrate_state(OtherValues, MyPreviousState, MyState, false, none, MyRange),
    {MyNewPreviousState, MyNewState, QueuedMessages, TriggerState, MyRange};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Contacting random nodes (response from cyclon)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% ignore empty node list from cyclon
on_active({cy_cache, []}, FullState)  ->
    FullState;

on_active({cy_cache, [Node] = _Cache},
          {_PreviousState, State, _QueuedMessages, _TriggerState, _MyRange} = FullState) ->
    % This message is received as a response to a get_subset message to the
    % cyclon process and should contain a random node. We will then contact this
    % random node and ask for a state exchange.
%%     io:format("gossip: got random node from Cyclon: ~p~n",[_Cache]),
    % do not exchange states with itself
    case node:is_me(Node) of
        false -> ?SEND_TO_GROUP_MEMBER(
                   node:pidX(Node), gossip,
                   {get_state, comm:this(), gossip_state:get(State, values)});
        true  -> ok
    end,
    FullState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getter messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on_active({get_values_best, SourcePid},
          {PreviousState, State, _QueuedMessages, _TriggerState, _MyRange} = FullState) ->
    BestState = previous_or_current(PreviousState, State),
    BestValues = gossip_state:conv_state_to_extval(BestState),
    msg_get_values_best_response(SourcePid, BestValues),
    FullState;

on_active({get_values_all, SourcePid},
          {PreviousState, State, _QueuedMessages, _TriggerState, _MyRange} = FullState) ->
    PreviousValues = gossip_state:conv_state_to_extval(PreviousState),
    CurrentValues = gossip_state:conv_state_to_extval(State),
    BestState = previous_or_current(PreviousState, State),
    BestValues = gossip_state:conv_state_to_extval(BestState),
    msg_get_values_all_response(SourcePid, PreviousValues, CurrentValues, BestValues),
    FullState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Web interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on_active({web_debug_info, Requestor},
          {PreviousState, State, _QueuedMessages, _TriggerState, _MyRange} = FullState) ->
    BestValues = gossip_state:conv_state_to_extval(previous_or_current(PreviousState, State)),
    KeyValueList =
        [{"prev_round",          gossip_state:get(PreviousState, round)},
         {"prev_triggered",      gossip_state:get(PreviousState, triggered)},
         {"prev_msg_exch",       gossip_state:get(PreviousState, msg_exch)},
         {"prev_conv_avg_count", gossip_state:get(PreviousState, converge_avg_count)},
         {"prev_avg",            gossip_state:get(PreviousState, avgLoad)},
         {"prev_min",            gossip_state:get(PreviousState, minLoad)},
         {"prev_max",            gossip_state:get(PreviousState, maxLoad)},
         {"prev_stddev",         gossip_state:calc_stddev(PreviousState)},
         {"prev_size_ldr",       gossip_state:calc_size_ldr(PreviousState)},
         {"prev_size_kr",        gossip_state:calc_size_kr(PreviousState)},
         
         {"cur_round",           gossip_state:get(State, round)},
         {"cur_triggered",       gossip_state:get(State, triggered)},
         {"cur_msg_exch",        gossip_state:get(State, msg_exch)},
         {"cur_conv_avg_count",  gossip_state:get(State, converge_avg_count)},
         {"cur_avg",             gossip_state:get(State, avgLoad)},
         {"cur_min",             gossip_state:get(State, minLoad)},
         {"cur_max",             gossip_state:get(State, maxLoad)},
         {"cur_stddev",          gossip_state:calc_stddev(State)},
         {"cur_size_ldr",        gossip_state:calc_size_ldr(State)},
         {"cur_size_kr",         gossip_state:calc_size_kr(State)},
         
         {"best_avg",             gossip_state:get(BestValues, avgLoad)},
         {"best_min",             gossip_state:get(BestValues, minLoad)},
         {"best_max",             gossip_state:get(BestValues, maxLoad)},
         {"best_stddev",          gossip_state:get(BestValues, stddev)},
         {"best_size",            gossip_state:get(BestValues, size)},
         {"best_size_ldr",        gossip_state:get(BestValues, size_ldr)},
         {"best_size_kr",         gossip_state:get(BestValues, size_kr)}],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    FullState.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the previous state if the current state has not sufficiently
%%      converged yet otherwise returns the current state.
-spec previous_or_current(state(), state()) -> state().
previous_or_current(PreviousState, CurrentState) ->
    CurrentInitialized = gossip_state:get(CurrentState, initialized),
    MinConvergeAvgCount = get_converge_avg_count(),
    CurrentEpsilonCount_Avg = gossip_state:get(CurrentState, converge_avg_count),
    _BestValue =
        case (not CurrentInitialized) orelse (CurrentEpsilonCount_Avg < MinConvergeAvgCount) of
            true -> PreviousState;
            false -> CurrentState
        end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State update
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec integrate_state
        (OtherValues::gossip_state:values_internal(), MyPrevState::state(),
         MyState::state(), SendBack::true, Source_PID::comm:mypid(),
         MyRange::intervals:interval()) -> {state(), state()};
        (OtherValues::gossip_state:values_internal(), MyPrevState::state(),
         MyState::state(), SendBack::false, Source_PID::any(),
         MyRange::intervals:interval()) -> {state(), state()}.
integrate_state(OtherValues, MyPreviousState, MyState, SendBack, Source_PID, MyRange) ->
%%     io:format("gossip:integrate_state: ~p~n",[{OtherValues, MyState}]),
    MyValues = gossip_state:get(MyState, values),
    Initialized = gossip_state:get(MyState, initialized),
    MyRound = gossip_state:get(MyValues, round),
    OtherRound = gossip_state:get(OtherValues, round),
    {MyNewPreviousState, MyNewState} =
        if
            % The other node's round is higher -> enter this (new) round (use
            % the other node's values until the own load and key range are
            % received).
            (OtherRound > MyRound) ->
                ShouldSend = false,
                enter_round(MyPreviousState, MyState, OtherValues, MyRange);
            
            % We are in a higher round than the requesting node -> send it our
            % values if we have information about our own node. Do not update
            % using the other node's state! 
            OtherRound < MyRound ->
                % only send if we have full information!
                ShouldSend = Initialized,
                {MyPreviousState, MyState};

            % Same rounds but we haven't got any load information yet ->
            % update estimates with the other values but do not send them until
            % the own load and key range are received).
            (not Initialized) ->
                ShouldSend = false,
                MyTempState1 = update(MyState, OtherValues),
                MyTempState2 = gossip_state:inc_msg_exch(MyTempState1),
                {MyPreviousState, MyTempState2};
    
            % Both nodes have load information and are in the same round
            % -> send the other node our state and update our state with the
            % information from the other node's state.
            true ->
                ShouldSend = true,
                MyTempState3 = update(MyState, OtherValues),
                MyTempState4 = gossip_state:inc_msg_exch(MyTempState3),
                {MyPreviousState, MyTempState4}
        end,
    if
        (ShouldSend andalso SendBack) ->
            comm:send(Source_PID, {get_state_response, MyValues});
        true -> ok
    end,
    {MyNewPreviousState, MyNewState}. 

%% @doc Updates MyState with the information from OtherState if both share the
%%      same round, otherwise MyState is used as is.
-spec update(state(), gossip_state:values_internal()) -> state().
update(MyState, OtherValues) ->
%%     io:format("gossip:update ~p~n",[{MyState, OtherValues}]),
    MyValues = gossip_state:get(MyState, values),
    MyNewValues = 
        case gossip_state:get(MyValues, round) =:= gossip_state:get(OtherValues, round) of
            true ->
                V1 = update_value(avgLoad, MyValues, OtherValues),
                V2 = update_value(minLoad, V1, OtherValues),
                V3 = update_value(maxLoad, V2, OtherValues),
                V4 = update_value(size_inv, V3, OtherValues),
                V5 = update_value(avgLoad2, V4, OtherValues),
                _V6 = update_value(avg_kr, V5, OtherValues);
            false ->
                % this case should not happen since the on_active/2 handlers should only
                % call update/2 if the rounds match
                log:log(error,"[ Node | ~w ] gossip:update rounds not equal (ignoring):~nstacktrace: ~p", [comm:this(),util:get_stacktrace()]),
                MyValues
        end,
    % now check whether all average-based values changed less than epsilon percent:
    Epsilon_Avg = get_converge_avg_epsilon(),
    MyNewState =
        case (calc_change(avgLoad, MyValues, MyNewValues) < Epsilon_Avg) andalso
                 (calc_change(size_inv, MyValues, MyNewValues) < Epsilon_Avg) andalso
                 (calc_change(avgLoad2, MyValues, MyNewValues) < Epsilon_Avg) andalso
                 (calc_change(avg_kr, MyValues, MyNewValues)  < Epsilon_Avg) of
            true -> gossip_state:inc_converge_avg_count(MyState); 
            false -> gossip_state:reset_converge_avg_count(MyState)
        end,
    Result = gossip_state:set_values(MyNewState, MyNewValues),
    Result.

%% @doc Calculates the change in percent from the Old value to the New value.
-spec calc_change(Key::minLoad | maxLoad | avgLoad | size_inv | avg_kr | avgLoad2,
                   OldValues::gossip_state:values_internal(), NewValues::gossip_state:values_internal()) -> Change::float().
calc_change(Key, OldValues, NewValues) ->
    Old = gossip_state:get(OldValues, Key),
    New = gossip_state:get(NewValues, Key),
    if
        (Old =/= unknown) andalso (Old =:= New) -> 0.0;
        (Old =:= unknown) orelse (New =:= unknown) orelse (Old == 0) -> 100.0;
        true -> ((Old + abs(New - Old)) * 100.0 / Old) - 100
    end.

%% @doc Updates a node's new value of the given key using its old value and
%%      another node's value.
%% @see calc_new_value/3
-spec update_value(Key::minLoad | maxLoad | avgLoad | size_inv | avg_kr | avgLoad2,
                    gossip_state:values_internal(), gossip_state:values_internal()) -> gossip_state:values_internal().
update_value(Key, MyValues, OtherValues) ->
    MyValue = gossip_state:get(MyValues, Key),
    OtherValue = gossip_state:get(OtherValues, Key),
    MyNewValue = calc_new_value(Key, MyValue, OtherValue),
    gossip_state:set(MyValues, Key, MyNewValue).

%% @doc Calculates a node's new value of the given key using its old value and
%%      another node's value.
%% @see update_value/3
-spec calc_new_value
        (Key::minLoad | maxLoad | avgLoad | size_inv | avg_kr | avgLoad2, MyValue::T, OtherValue::T) -> MyNewValue::T when is_subtype(T, number());
        (Key::minLoad | maxLoad | avgLoad | size_inv | avg_kr | avgLoad2, MyValue::T, OtherValue::unknown) -> MyNewValue::T when is_subtype(T, number());
        (Key::minLoad | maxLoad | avgLoad | size_inv | avg_kr | avgLoad2, MyValue::unknown, OtherValue::T) -> MyNewValue::T when is_subtype(T, number());
        (Key::minLoad | maxLoad | avgLoad | size_inv | avg_kr | avgLoad2, MyValue::unknown, OtherValue::unknown) -> MyNewValue::unknown.
calc_new_value(minLoad, unknown, OtherMinLoad) ->
    OtherMinLoad;
calc_new_value(minLoad, MyMinLoad, unknown) ->
    MyMinLoad;
calc_new_value(minLoad, MyMinLoad, OtherMinLoad) when (MyMinLoad < OtherMinLoad) ->
    MyMinLoad;
calc_new_value(minLoad, _MyMinLoad, OtherMinLoad) ->
    OtherMinLoad;

calc_new_value(maxLoad, unknown, OtherMaxLoad) ->
    OtherMaxLoad;
calc_new_value(maxLoad, MyMaxLoad, unknown) ->
    MyMaxLoad;
calc_new_value(maxLoad, MyMaxLoad, OtherMaxLoad) when (MyMaxLoad > OtherMaxLoad) ->
    MyMaxLoad;
calc_new_value(maxLoad, _MyMaxLoad, OtherMaxLoad) ->
    OtherMaxLoad;

calc_new_value(avgLoad, MyAvg, OtherAvg) ->
    calc_avg(MyAvg, OtherAvg);
calc_new_value(size_inv, MySize_inv, OtherSize_inv) ->
    calc_avg(MySize_inv, OtherSize_inv);
calc_new_value(avg_kr, MyAvg_kr, OtherAvg_kr) ->
    calc_avg(MyAvg_kr, OtherAvg_kr);
calc_new_value(avgLoad2, MyAvg2, OtherAvg2) ->
    calc_avg(MyAvg2, OtherAvg2).

%% @doc Calculates the average of the two given values. If MyValue is unknown,
%%      OtherValue will be returned and vice versa.
-spec calc_avg(number(), number()) -> float();
              (T, unknown) -> T when is_subtype(T, number());
              (unknown, T) -> T when is_subtype(T, number());
              (unknown, unknown) -> unknown.
calc_avg(MyValue, OtherValue) -> 
    _MyNewValue =
        case MyValue of
            unknown -> OtherValue;
            X when is_number(X) ->
                case OtherValue of
                    unknown -> MyValue;
                    Y when is_number(Y) -> (X + Y) / 2.0
                end
        end.

%% @doc Creates a (temporary) state a node would have by knowing its own load
%%      and updates the according fields (avg, avg2, min, max) in its real
%%      state and setting it as 'initialized'. This should (only) be called
%%      when a load information message from the local node has been received
%%      for the first time in a round.
-spec integrate_local_info(state(), node_details:load()) -> state().
integrate_local_info(MyState, Load) ->
    MyValues = gossip_state:get(MyState, values),
    ValuesWithNewInfo =
        gossip_state:new_internal(float(Load), % Avg
                                  float(Load * Load), % Avg2
                                  unknown, % 1/size (will be set when entering/creating a round)
                                  unknown, % average key range
                                  Load, % Min
                                  Load, % Max
                                  gossip_state:get(MyValues, round)),
    V1 = update_value(avgLoad, MyValues, ValuesWithNewInfo),
    V2 = update_value(avgLoad2, V1, ValuesWithNewInfo),
    V3 = update_value(minLoad, V2, ValuesWithNewInfo),
    V4 = update_value(maxLoad, V3, ValuesWithNewInfo),
    MyNewValues = update_value(avg_kr, V4, ValuesWithNewInfo),
    S1 = gossip_state:set_values(MyState, MyNewValues),
    _MyNewState = gossip_state:set_initialized(S1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Round handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Enters the given round and thus resets the current state with
%%      information from another node's state. Also requests the own node's
%%      load and key range to integrate into its own state.
-spec enter_round(OldPrevState::state(), OldState::state(),
                  OtherValues::gossip_state:values_internal(),
                  MyRange::intervals:interval())
        -> {OldState::state(), NewState::state()}.
enter_round(OldPreviousState, OldState, OtherValues, MyRange) ->
    MyRound = gossip_state:get(OldState, round),
    OtherRound = gossip_state:get(OtherValues, round),
    case (MyRound =:= OtherRound) of 
        true -> {OldPreviousState, OldState};
        false ->
            % set a size_inv value of 0 (only the leader sets 1)
            NewState = new_state(OtherRound, MyRange, 0.0),
            request_local_info(),
            {OldState, NewState}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Requests send to other processes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether a new round should be started and starts a new round
%%      if ?RT:hash_key("0") is in the interval between our predecessor and our
%%      node.
-spec check_round(PreviousState::state(), State::state(), MyRange::intervals:interval())
        -> {NewPreviousState::state(), NewState::state()}.
check_round(PreviousState, State, MyRange) ->
    Round = gossip_state:get(State, round),
    TriggerCount = gossip_state:get(State, triggered),
    ConvAvgCount = gossip_state:get(State, converge_avg_count),
    ConvAvgCountNewRound = get_converge_avg_count_start_new_round(),
    % decides when to ask whether we are the leader
    case (Round =:= 0) orelse
         ((TriggerCount > get_min_tpr()) andalso (
             (TriggerCount > get_max_tpr()) orelse
             (ConvAvgCount >= ConvAvgCountNewRound))) of
        true ->
            % condition to start new round met -> start new round if the node is the leader
            % note: no need to cache the neighborhood table, this call is quite rare
            case is_leader(MyRange) of
                false -> {PreviousState, State};
                _ ->
                    % log:log(info, "gossip: start new round, I am the leader ~n"),
                    % the leader must set the size_inv to 1 (and only the leader)
                    NewState = new_state(Round + 1, MyRange, 1.0),
                    request_local_info(),
                    {State, NewState}
            end;
        false ->
            {PreviousState, State}
    end.

%% @doc Sends the local node's dht_node a request to tell us some information
%%      about itself. The node will respond with a
%%      {get_node_details_response, NodeDetails} message.
-spec request_local_info() -> ok.
request_local_info() ->
    % ask for local load:
    DHT_Node = pid_groups:get_my(dht_node),
    comm:send_local(DHT_Node, {get_node_details, comm:this(), [load]}).

%% @doc Creates a new state. 
-spec new_state(Round::gossip_state:round(), MyRange::intervals:interval(),
                SizeInv::gossip_state:size_inv()) -> state().
new_state(Round, MyRange, SizeInv) ->
    NewValues1 = gossip_state:set(
                   gossip_state:new_internal(), size_inv, SizeInv),
    NewValues2 = gossip_state:set(
                   NewValues1, avg_kr, calc_initial_avg_kr(MyRange)),
    NewValues3 = gossip_state:set(NewValues2, round, Round),
    gossip_state:new_state(NewValues3).
    

%% @doc Sends the local node's cyclon process a request for a random node.
%%      on_active({cy_cache, Cache},State) will handle the response
-spec request_random_node() -> ok.
request_random_node() ->
    cyclon:get_subset_rand(1),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculates the difference between the key of a node and its
%%      predecessor. If the second is larger than the first it wraps around and
%%      thus the difference is the number of keys from the predecessor to the
%%      end (of the ring) and from the start to the current node.
%%      Pre: MyRange is continuous
-spec calc_initial_avg_kr(MyRange::intervals:interval()) -> integer() | unknown.
calc_initial_avg_kr(MyRange) ->
    {_, PredKey, MyKey, _} = intervals:get_bounds(MyRange),
    % we don't know whether we can subtract keys of type ?RT:key()
    % -> try it and if it fails, return unknown
    try ?RT:get_range(PredKey, MyKey)
    catch
        throw:not_supported -> unknown
    end.

%% @doc Checks whether the node is the current leader.
-spec is_leader(MyRange::intervals:interval()) -> boolean().
is_leader(MyRange) ->
    intervals:in(?RT:hash_key("0"), MyRange).

%% @doc Checks whether the node's range has changed, i.e. either the node
%%      itself or its pred changed.
-spec rm_my_range_changed(OldNeighbors::nodelist:neighborhood(),
                          NewNeighbors::nodelist:neighborhood(),
                          IsSlide::rm_loop:slide()) -> boolean().
rm_my_range_changed(OldNeighbors, NewNeighbors, _IsSlide) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors) orelse
        nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors).

%% @doc Notifies the node's gossip process of a changed range.
%%      Used to subscribe to the ring maintenance. 
-spec rm_send_new_range(Subscriber::pid(), Tag::?MODULE,
                        OldNeighbors::nodelist:neighborhood(),
                        NewNeighbors::nodelist:neighborhood()) -> ok.
rm_send_new_range(Pid, ?MODULE, _OldNeighbors, NewNeighbors) ->
    NewRange = node:mk_interval_between_nodes(nodelist:pred(NewNeighbors),
                                              nodelist:node(NewNeighbors)),
    comm:send_local(Pid, {update_range, NewRange}).

%% @doc Checks whether config parameters of the gossip process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_module(gossip_trigger) and
    
    config:cfg_is_integer(gossip_interval) and
    config:cfg_is_greater_than(gossip_interval, 0) and
    
    config:cfg_is_integer(gossip_min_triggers_per_round) and
    config:cfg_is_greater_than_equal(gossip_min_triggers_per_round, 0) and
    
    config:cfg_is_integer(gossip_max_triggers_per_round) and
    config:cfg_is_greater_than_equal(gossip_max_triggers_per_round, 1) and
    
    config:cfg_is_float(gossip_converge_avg_epsilon) and
    config:cfg_is_in_range(gossip_converge_avg_epsilon, 0.0, 100.0) and
    
    config:cfg_is_integer(gossip_converge_avg_count) and
    config:cfg_is_greater_than(gossip_converge_avg_count, 0) and
    
    config:cfg_is_integer(gossip_converge_avg_count_start_new_round) and
    config:cfg_is_greater_than(gossip_converge_avg_count_start_new_round, 0).
    
%% @doc Gets the gossip interval set in scalaris.cfg.
-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(gossip_interval).

%% @doc Gets the number of minimum triggers a round should have (set in
%%      scalaris.cfg). A new round will not be started as long as this number
%%      has not been reached at the leader.
-spec get_min_tpr() -> pos_integer().
get_min_tpr() ->
    config:read(gossip_min_triggers_per_round).

%% @doc Gets the number of maximum triggers a round should have (set in
%%      scalaris.cfg). A new round will be started when this number is reached
%%      at the leader.
-spec get_max_tpr() -> pos_integer().
get_max_tpr() ->
    config:read(gossip_max_triggers_per_round).

%% @doc Gets the epsilon parameter that defines the maximum change of
%%      average-based values (in percent) to be considered as "converged", i.e.
%%      stable.
-spec get_converge_avg_epsilon() -> float().
get_converge_avg_epsilon() ->
    config:read(gossip_converge_avg_epsilon).

%% @doc Gets the count parameter that defines how often average-based values
%%      should change within an epsilon in order to be considered as
%%      "converged", i.e. stable.
-spec get_converge_avg_count() -> pos_integer().
get_converge_avg_count() ->
    config:read(gossip_converge_avg_count).

%% @doc Gets the count parameter that defines how often average-based values
%%      should change within an epsilon in order to start a new round.
-spec get_converge_avg_count_start_new_round() -> pos_integer().
get_converge_avg_count_start_new_round() ->
    config:read(gossip_converge_avg_count_start_new_round).
