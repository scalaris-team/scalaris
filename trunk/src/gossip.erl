%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    gossip.erl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Framework for estimating aggregated global properties using
%%%         gossip techniques.
%%%  
%%%  Gossiping is organized in rounds. At the start of each round, a node's
%%%  gossip process has to ask its dht_node for information about its state,
%%%  i.e. its load and the IDs of itself and its predecessor. It will not
%%%  participate in any state exchanges until this information has been
%%%  received and postpone such messages, i.e. 'get_state', if received. All
%%%  other messages, e.g. requests for estimated values, are either ignored
%%%  (only applies to "trigger") or answered with information from the previous
%%%  round, e.g. requests for (estimated) system properties.
%%%  
%%%  When these values are successfully integrated into the process state,
%%%  and the node entered some round, it will continuously ask the cyclon
%%%  process for a random node to exchange its state with and update the local
%%%  estimates (the interval is defined in gossip_interval given by
%%%  scalaris.cfg).
%%%  
%%%  New rounds are started by the leader which is identified as the node for
%%%  which util:is_between(PredId, 0, MyId) is true. It will propagate its
%%%  round with its state so that gossip processes of other nodes can join this
%%%  round. Several parameters (added to scalaris.cfg) influence the decision
%%%  about when to start a new round:
%%%  <ul>
%%%   <li>gossip_max_triggers_per_round: a new round is started if this many
%%%       triggers have been received during the round</li>
%%%   <li>gossip_min_triggers_per_round: a new round is NOT started until this
%%%       many triggers have been received during the round</li>
%%%   <li>gossip_converge_avg_count_start_new_round: if the estimated values
%%%       did not change by more than gossip_converge_avg_epsilon percent this
%%%       many times, assume the values have converged and start a new round
%%%   </li>
%%%   <li>gossip_converge_avg_epsilon: (see
%%%       gossip_converge_avg_count_start_new_round)</li>
%%%  </ul>
%%%  
%%%  Each process stores the estimates of the current round (which might not
%%%  have been converged yet) and the previous estimates. If another process
%%%  asks for gossip's best values it will favor the previous values but return
%%%  the current ones if they have not changes by more than
%%%  gossip_converge_avg_epsilon percent gossip_converge_avg_count times.
%%% @end
%%% Created : 19 Feb 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
%% @reference M. Jelasity, A. Montresor, O. Babaoglu: Gossip-based aggregation
%% in large dynamic networks. ACM Trans. Comput. Syst. 23(3), 219-252 (2005)
-module(gossip).

-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).

% functions gen_component, the trigger and the config module use
-export([on/2, init/1, get_base_interval/0, check_config/0]).

% helpers for creating getter messages:
-export([get_values_best/0, get_values_best/1,
		 get_values_all/0, get_values_all/1]).

-type(load() :: integer()).

-type(state() :: gossip_state:state()).
-type(values() :: gossip_state:values()).
-type(values_internal() :: gossip_state:values_internal()).
%% -type(values() :: gossip_state:values()).
-type(avg_kr() :: gossip_state:avg_kr()).

%% Full state of the gossip process:
%% {PreviousState, CurrentState, QueuedMessages, TriggerState}
%% -> previous and current state, queued messages (get_state messages received
%% before local values are known) and the state of the trigger.
-type(full_state() :: {state(), state(), list(), trigger:state()}).

% accepted messages of gossip processes
-type(message() ::
	{trigger} |
	{{get_node_details_response, node_details:node_details()}, local_info} |
	{{get_node_details_response, node_details:node_details()}, leader_start_new_round} |
	{get_state, cs_send:mypid(), values_internal()} |
	{get_state_response, values_internal()} |
	{cy_cache, [node:node_type()]} |
	{get_values_all, cs_send:erl_local_pid()} | 
    {get_values_best, cs_send:erl_local_pid()} |
    {'$gen_cast', {debug_info, Requestor::cs_send:erl_local_pid()}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions that create and send messages to nodes requesting information.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a response message to a request for the best stored values.
-spec msg_get_values_best_response(cs_send:erl_local_pid(), values()) -> ok.
msg_get_values_best_response(Pid, BestValues) ->
    cs_send:send_local(Pid, {gossip_get_values_best_response, BestValues}).

%% @doc Sends a response message to a request for all stored values.
-spec msg_get_values_all_response(cs_send:erl_local_pid(), values(), values(), values()) -> ok.
msg_get_values_all_response(Pid, PreviousValues, CurrentValues, BestValues) ->
    cs_send:send_local(Pid, {gossip_get_values_all_response, PreviousValues, CurrentValues, BestValues}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getters
%
% Functions that other processes can call to receive information from the gossip
% process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for the best values of the stored information.
%%      see on({get_values_best, SourcePid}, FullState) and
%%      msg_get_values_best_response/2
-spec get_values_best() -> ok.
get_values_best() ->
    get_values_best(self()).

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for the best values of the stored information to
%%      be send to Pid.
%%      see on({get_values_best, SourcePid}, FullState) and
%%      msg_get_values_best_response/2
-spec get_values_best(cs_send:erl_local_pid()) -> ok.
get_values_best(Pid) ->
    GossipPid = process_dictionary:get_group_member(gossip),
    cs_send:send_local(GossipPid, {get_values_best, Pid}).

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for all stored information.
%%      see on({get_values_all, SourcePid}, FullState) and
%%      msg_get_values_all_response/4
-spec get_values_all() -> ok.
get_values_all() ->
    get_values_all(self()).

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for all stored information to be send to Pid.
%%      see on({get_values_all, SourcePid}, FullState) and
%%      msg_get_values_all_response/4
-spec get_values_all(cs_send:erl_local_pid()) -> ok.
get_values_all(Pid) ->
    GossipPid = process_dictionary:get_group_member(gossip),
    cs_send:send_local(GossipPid, {get_values_all, Pid}).

%% @doc Returns the previous state if the current state has not sufficiently
%%      converged yet otherwise returns the current state.
-spec previous_or_current(state(), state()) -> state().
previous_or_current(PreviousState, CurrentState) ->
    CurrentInitialized = gossip_state:get(CurrentState, initialized),
    MinConvergeAvgCount = get_converge_avg_count(),
    CurrentEpsilonCount_Avg = gossip_state:get(CurrentState, converge_avg_count),
    _BestValue =
        case (not CurrentInitialized) or (CurrentEpsilonCount_Avg < MinConvergeAvgCount) of
            true -> PreviousState;
            false -> CurrentState
        end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the gossip process, registers it with the process dictionary and
%%      returns its pid for use by a supervisor.
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    Trigger = config:read(gossip_trigger),
    gen_component:start_link(?MODULE, Trigger, [{register, InstanceId, gossip}]).

%% @doc Initialises the module with an empty state.
-spec init(module()) -> full_state().
init(Trigger) ->
    log:log(info,"[ Gossip ~p ] starting~n", [cs_send:this()]),
    TriggerState = trigger:init(Trigger, ?MODULE),
    TriggerState2 = trigger:first(TriggerState),
	PreviousState = gossip_state:new_state(),
	State = gossip_state:new_state(),
    {PreviousState, State, [], TriggerState2}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec on(message(), full_state()) -> full_state() | unknown_event.
on({trigger}, {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message is received continuously when the Trigger calls
	% see gossip_trigger and gossip_interval in the scalaris.cfg file
%% 	io:format("{trigger_gossip}: ~p~n", [State]),
    NewTriggerState = trigger:next(TriggerState),
	NewState = gossip_state:inc_triggered(State),
	% request a check whether we are the leader and can thus decide whether to
	% start a new round
    request_new_round_if_leader(NewState),
	Round = gossip_state:get(NewState, round),
	Initialized = gossip_state:get(State, initialized),
	% only participate in (active) gossiping if we entered some valid round and
	% we have information about our node's load and key range
    case (Round > 0) andalso (Initialized) of
        true -> request_random_node();
        false -> ok
    end,
    {PreviousState, NewState, QueuedMessages, NewTriggerState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Responses to requests for information about the local node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({{get_node_details_response, NodeDetails}, local_info},
   {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message is received when the (local) node was asked to tell us its
	% load and key range
%%     io:format("gossip: got get_node_details_response: ~p~n",[NodeDetails]),
	Initialized = gossip_state:get(State, initialized),
	{NewQueuedMessages, NewState} =
		case Initialized of
			true -> {[], State};
			false ->
                send_queued_messages(QueuedMessages),
				{[], integrate_local_info(State, node_details:get(NodeDetails, load), calc_initial_avg_kr(node_details:get(NodeDetails, my_range)))}
			end,
    {PreviousState, NewState, NewQueuedMessages, TriggerState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Leader election
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({{get_node_details_response, NodeDetails}, leader_start_new_round},
   {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message can only be received after being requested by
	% request_new_round_if_leader/1 which only asks for this if the condition to
	% start a new round has already been met
%%     io:format("gossip: got get_node_details_response, leader_start_new_round: ~p~n",[NodeDetails]),
	{PredId, MyId} = node_details:get(NodeDetails, my_range),
	{NewPreviousState, NewState} = 
		case util:is_between(PredId, 0, MyId) of
	        % not the leader -> continue as normal with the old state
			false -> {PreviousState, State};
			% leader -> start a new round
			true -> new_round(State)
	end,
    {NewPreviousState, NewState, QueuedMessages, TriggerState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State exchange
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state, Source_PID, OtherValues} = Msg,
   {MyPreviousState, MyState, QueuedMessages, TriggerState}) ->
	% This message is received when a node asked our node for its state.
	% The piggy-backed other node's state will be used to update our own state
	% if we have already initialized it (otherwise postpone the message). A
	% get_state_response message is sent with our state in the first case.
	Initialized = gossip_state:get(MyState, initialized),
	{NewQueuesMessages, {MyNewPreviousState, MyNewState}} =
		case Initialized of
			true -> {QueuedMessages, integrate_state(OtherValues, MyPreviousState, MyState, true, Source_PID)};
			false ->
				{[Msg | QueuedMessages], enter_round(MyPreviousState, MyState, OtherValues)}
			end,
    {MyNewPreviousState, MyNewState, NewQueuesMessages, TriggerState};

on({get_state_response, OtherValues},
   {MyPreviousState, MyState, QueuedMessages, TriggerState}) ->
	% This message is received as a response to a get_state message and contains
    % another node's state. We will use it to update our own state
	% if both are valid.
	{MyNewPreviousState, MyNewState} =
		integrate_state(OtherValues, MyPreviousState, MyState, false, none),
    {MyNewPreviousState, MyNewState, QueuedMessages, TriggerState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Contacting random nodes (response from cyclon)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% ignore empty node list from cyclon
on({cy_cache, []}, FullState)  ->
    FullState;

on({cy_cache, [Node] = _Cache},
    {_PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
    % This message is received as a response to a get_subset message to the
    % cyclon process and should contain a random node. We will then contact this
    % random node and ask for a state exchange.
%%     io:format("gossip: got random node from Cyclon: ~p~n",[_Cache]),
    % do not exchange states with itself
    case node:is_me(Node) of
        false ->
            cs_send:send_to_group_member(node:pidX(Node), gossip,
                                         {get_state, cs_send:this(),
                                          gossip_state:get(State, values)});
        true -> ok
    end,
    FullState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getter messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_values_best, SourcePid},
    {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	BestState = previous_or_current(PreviousState, State),
	BestValues = gossip_state:conv_state_to_extval(BestState),
	msg_get_values_best_response(SourcePid, BestValues),
	FullState;

on({get_values_all, SourcePid},
    {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	PreviousValues = gossip_state:conv_state_to_extval(PreviousState),
	CurrentValues = gossip_state:conv_state_to_extval(State),
	BestState = previous_or_current(PreviousState, State),
	BestValues = gossip_state:conv_state_to_extval(BestState),
	msg_get_values_all_response(SourcePid, PreviousValues, CurrentValues, BestValues),
	FullState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Web interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({'$gen_cast', {debug_info, Requestor}},
   {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
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
    cs_send:send_local(Requestor, {debug_info_response, KeyValueList}),
    FullState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Unknown events
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on(_Message, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State update
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec integrate_state(values_internal(), state(), state(), true,
                       cs_send:mypid()) -> {state(), state()}
                    ; (values_internal(), state(), state(), false,
                       any()) -> {state(), state()}.
integrate_state(OtherValues, MyPreviousState, MyState, SendBack, Source_PID) ->
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
    			enter_round(MyPreviousState, MyState, OtherValues);
		    
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
			cs_send:send(Source_PID, {get_state_response, MyValues});
		true -> ok
	end,
	{MyNewPreviousState, MyNewState}. 

%% @doc Updates MyState with the information from OtherState if both share the
%%      same round, otherwise MyState is used as is.
-spec update(state(), values_internal()) -> state().
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
				% this case should not happen since the on/2 handlers should only
				% call update/2 if the rounds match
            	log:log(error,"[ Node | ~w ] gossip:update rounds not equal (ignoring): ~p", [cs_send:this(),util:get_stacktrace()]),
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
                   OldValues::values_internal(), NewValues::values_internal()) -> Change::float().
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
                    values_internal(), values_internal()) -> values_internal().
update_value(Key, MyValues, OtherValues) ->
    MyValue = gossip_state:get(MyValues, Key),
    OtherValue = gossip_state:get(OtherValues, Key),
    MyNewValue = calc_new_value(Key, MyValue, OtherValue),
    gossip_state:set(MyValues, Key, MyNewValue).

%% @doc Calculates a node's new value of the given key using its old value and
%%      another node's value.
%% @see update_value/3
-spec calc_new_value(Key::minLoad | maxLoad | avgLoad | size_inv | avg_kr | avgLoad2, MyValue::T, OtherValue::T) -> MyNewValue::T.
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
%%      OtherValue will be returned.
-spec calc_avg(number() | unknown, number() | unknown) -> number() | unknown.
calc_avg(MyValue, OtherValue) -> 
	_MyNewValue =
		case MyValue of
			unknown -> OtherValue;
			X when is_number(X) ->
				case OtherValue of
					unknown -> unknown;
					Y when is_number(Y) -> (X + Y) / 2.0
				end
		end.

%% @doc Creates a (temporary) state a node would have by knowing its own load
%%      and key range and updates the according fields (avg, avg2, min, max,
%%      avg_kr) in its real state. This should (only) be called when a load
%%      and key range information message from the local node has been received
%%      for the first time in a round.
-spec integrate_local_info(state(), load(), avg_kr()) -> state().
integrate_local_info(MyState, Load, Avg_kr) ->
	MyValues = gossip_state:get(MyState, values),
	ValuesWithNewInfo =
		gossip_state:new_internal(Load, % Avg
								  Load*Load, % Avg2
								  unknown, % 1/size (will be set when entering/creating a round)
								  Avg_kr, % average key range
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

%% @doc Starts a new round (this should only be done by the leader!).
-spec new_round(state()) -> {state(), state()}.
new_round(OldState) ->
%%     io:format("gossip: start new round, I am the leader ~n"),
	% the leader must set the size_inv to 1 (and only the leader)
	NewValues1 = gossip_state:set(gossip_state:new_internal(), size_inv, 1.0),
	OldRound = gossip_state:get(OldState, round),
	NewState =
		gossip_state:new_state(
		  gossip_state:set(NewValues1, round, (OldRound+1))),
    request_local_info(),
	{OldState, NewState}.

%% @doc Enters the given round and thus resets the current state with
%%      information from another node's state. Also requests the own node's
%%      load and key range to integrate into its own state.
-spec enter_round(state(), state(), values_internal()) -> {state(), state()}.
enter_round(OldPreviousState, OldState, OtherValues) ->
	MyRound = gossip_state:get(OldState, round),
	OtherRound = gossip_state:get(OtherValues, round),
	case (MyRound =:= OtherRound) of 
		true -> {OldPreviousState, OldState};
		false ->
			% set a size_inv value of 0 (only the leader sets 1)
			S1 = gossip_state:new_state(),
			NewState = gossip_state:set(gossip_state:set(S1, round, OtherRound),
                                        size_inv, 0.0),
    		request_local_info(),
			{OldState, NewState}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Requests send to other processes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends the local node's dht_node a request to tell us its successor and
%%      predecessor if a new round should be started. A new round will then only
%%      be started if we are the leader, i.e. we are responsible for key 0.
%%      The node will respond with a
%%      {{get_node_details_response, NodeDetails}, leader_start_new_round}
%%      message.
-spec request_new_round_if_leader(state()) -> ok.
request_new_round_if_leader(State) ->
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
			DHT_Node = process_dictionary:get_group_member(dht_node),
    		cs_send:send_local(DHT_Node, {get_node_details, cs_send:this_with_cookie(leader_start_new_round), [my_range]}),
			ok;
		false ->
			ok
	end.

%% @doc Sends the local node's dht_node a request to tell us some information
%%      about itself.
%%      The node will respond with a
%%      {{get_node_details_response, NodeDetails}, local_info} message.
-spec request_local_info() -> ok.
request_local_info() ->
	% ask for local load and key range:
	DHT_Node = process_dictionary:get_group_member(dht_node),
    cs_send:send_local(DHT_Node, {get_node_details, cs_send:this_with_cookie(local_info), [my_range, load]}).

%% @doc Sends the local node's cyclon process a request for a random node.
%%      on({cy_cache, Cache},State) will handle the response
-spec request_random_node() -> ok.
request_random_node() ->
    cyclon:get_subset_rand(1),
	ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends queued messages to the gossip process itself in the order they
%%      have been received.
-spec send_queued_messages(list()) -> ok.
send_queued_messages(QueuedMessages) ->
    lists:foldr(fun(Msg, _) -> cs_send:send_local(self(), Msg) end, ok, QueuedMessages).

%% @doc Gets the total number of keys available.
-spec get_addr_size() -> number().
get_addr_size() ->
	rt_simple:n().

%% @doc Calculates the difference between the key of a node and its
%%      predecessor. If the second is larger than the first it wraps around and
%%      thus the difference is the number of keys from the predecessor to the
%%      end (of the ring) and from the start to the current node.
-spec calc_initial_avg_kr({T, T}) -> avg_kr().
calc_initial_avg_kr({PredKey, MyKey} = _Range) ->
    try
		if
			PredKey =:= MyKey -> get_addr_size(); % I am the only node
			MyKey >= PredKey  -> MyKey - PredKey;
			MyKey < PredKey   -> (get_addr_size() - PredKey - 1) + MyKey
		end
	catch
		error:_ -> unknown
	end.

%% @doc Checks whether config parameters of the gossip process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_atom(gossip_trigger) and
    
    config:is_integer(gossip_interval) and
    config:is_greater_than(gossip_interval, 0) and
    
    config:is_integer(gossip_min_triggers_per_round) and
    config:is_greater_than_equal(gossip_min_triggers_per_round, 0) and
    
    config:is_integer(gossip_max_triggers_per_round) and
    config:is_greater_than_equal(gossip_max_triggers_per_round, 1) and
    
    config:is_float(gossip_converge_avg_epsilon) and
    config:is_in_range(gossip_converge_avg_epsilon, 0.0, 100.0) and
    
    config:is_integer(gossip_converge_avg_count) and
    config:is_greater_than(gossip_converge_avg_count, 0) and
    
    config:is_integer(gossip_converge_avg_count_start_new_round) and
    config:is_greater_than(gossip_converge_avg_count_start_new_round, 0).
    
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
