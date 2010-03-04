%  Copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : gossip.erl
%%% Author  : Nico Kruber <kruber@zib.de>
%%% Description : Framework for estimating aggregated global properties using
%%%               gossip techniques.
%%%
%%% Created : 19 Feb 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @author Nico Kruber <kruber@zib.de>
%% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(gossip,[Trigger]).

-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

%% -define(GOSSIP_REQUEST_LEADER_DEBUG_OUTPUT(), request_leader_debug_output()).
-define(GOSSIP_REQUEST_LEADER_DEBUG_OUTPUT(), ok).

-export([start_link/1]).

-export([on/2, init/1, get_base_interval/0]).

% helpers for creating getter messages:
-export([get_avgLoad/0,
		 get_minLoad/0,
		 get_maxLoad/0,
		 get_size/0,
		 get_size_kr/0,
		 get_stddev/0,
		 get_all/0
]).

-type(load() :: integer()).

-type(state() :: gossip_state:state()).
-type(values_internal() :: gossip_state:values_internal()).
%% -type(values() :: gossip_state:values()).
-type(size_kr() :: gossip_state:size_kr()).

% {PreviousState, CurrentState, QueuedMessages, TriggerState}
-type(full_state() :: {state(), state(), list(), any()}).

% accepted messages of gossip processes
-type(message() ::
	{trigger} |
	{get_load_response, cs_send:mypid(), load()} |
	{get_pred_succ_me_response, node:node_type(), node:node_type(), node:node_type(), size_kr} |
	{get_pred_me_response, node:node_type(), node:node_type(), leader_start_new_round} |
	{get_state, cs_send:mypid(), values_internal()} |
	{get_state_response, values_internal()} |
	{cache, [node:node_type()]} |
	{get_avgLoad, cs_send:mypid()} |
	{get_minLoad, cs_send:mypid()} |
	{get_maxLoad, cs_send:mypid()} |
	{get_size, cs_send:mypid()} |
	{get_size_kr, cs_send:mypid()} |
	{get_stddev, cs_send:mypid()} |
	{get_all, cs_send:mypid()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc initialises the module
-spec init([any()]) -> full_state().
init([_InstanceId, []]) ->
%%     io:format("gossip start ~n"),
    TriggerState = Trigger:init(?MODULE:new(Trigger)),
    TriggerState2 = Trigger:trigger_first(TriggerState,1),
	PreviousState = gossip_state:new_state(),
	State = gossip_state:new_state(),
    {PreviousState, State, [], TriggerState2}.

%% @doc starts the gossip module and returns its pid for use by a supervisor
-spec start_link(term()) -> {ok, pid()}.
start_link(InstanceId) ->
    gen_component:start_link(?MODULE:new(Trigger), [InstanceId, []], [{register, InstanceId, gossip}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec on(message(), full_state()) -> full_state().
on({trigger}, {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message is received continuously when the Trigger calls
	% see gossip_trigger and gossip_interval in the scalaris.cfg file
%% 	io:format("{trigger_gossip}: ~p~n", [State]),
    NewTriggerState = Trigger:trigger_next(TriggerState,1),
	NewState = gossip_state:inc_triggered(State),
	% request a check whether we are the leader and can thus decide whether to
	% start a new round
    request_new_round_if_leader(NewState),
	Round = gossip_state:get_round(NewState),
	Initialized = gossip_state:is_initialized(State),
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

on({get_node_details_response, local_info, NodeDetails},
   {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message is received when the (local) node was asked to tell us its
	% load and key range
%%     io:format("gossip: got get_node_details_response: ~p~n",[NodeDetails]),
	Initialized = gossip_state:is_initialized(State),
	{NewQueuedMessages, NewState} =
		case Initialized of
			true -> {QueuedMessages, State};
			false ->
				[cs_send:send_local(self(), Message) || Message <- QueuedMessages],
				{[], integrate_local_info(State, node_details:load(NodeDetails), calc_initial_size_kr(node_details:my_range(NodeDetails)))}
			end,
    {PreviousState, NewState, NewQueuedMessages, TriggerState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Leader election
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_node_details_response, leader_start_new_round, NodeDetails},
   {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message can only be received after being requested by
	% request_new_round_if_leader/1 which only asks for this if the condition to
	% start a new round has already been met
%%     io:format("gossip: got get_node_details_response, leader_start_new_round: ~p~n",[NodeDetails]),
	{PredId, MyId} = node_details:my_range(NodeDetails),
	{NewPreviousState, NewState} = 
		case intervals:is_between(PredId, 0, MyId) of
	        % not the leader -> continue as normal with the old state
			false -> {PreviousState, State};
			% leader -> start a new round
			true -> new_round(State)
	end,
    {NewPreviousState, NewState, QueuedMessages, TriggerState};

%% Prints some debug information if the current node is the leader.
on({get_node_details_response, leader_debug_output, NodeDetails},
   {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message can only be received after being requested by
	% request_leader_debug_output/0
%%     io:format("gossip: got get_node_details_response, leader_debug_output: ~p~n",[NodeDetails]),
	{PredId, MyId} = node_details:my_range(NodeDetails),
	case intervals:is_between(PredId, 0, MyId) of
        % not the leader
		false -> ok;
		% leader -> provide debug information
		true ->
			io:format("gossip:~n    prv: ~p, ~p, ~p, ~p:~n         ~p, ~p, ~p, ~p,~n         ~p, ~p~n    cur: ~p, ~p, ~p, ~p:~n         ~p, ~p, ~p, ~p,~n         ~p, ~p~n    usr: ~p, ~p, ~p, ~p,~n         ~p, ~p~n",
				[{round,gossip_state:get_round(PreviousState)},
				 {triggered,gossip_state:get_triggered(PreviousState)},
				 {msg_exch,gossip_state:get_msg_exch(PreviousState)},
				 {converge_avg_count,gossip_state:get_converge_avg_count(PreviousState)},
				 {avg,gossip_state:get_avgLoad(PreviousState)},
				 {min,gossip_state:get_minLoad(PreviousState)},
				 {max,gossip_state:get_maxLoad(PreviousState)},
				 {stddev,gossip_state:calc_stddev(PreviousState)},
				 {size,gossip_state:calc_size(PreviousState)},
				 {size_kr,gossip_state:calc_size_kr(PreviousState)},
				 
				 {round,gossip_state:get_round(State)},
				 {triggered,gossip_state:get_triggered(State)},
				 {msg_exch,gossip_state:get_msg_exch(State)},
				 {converge_avg_count,gossip_state:get_converge_avg_count(State)},
				 {avg,gossip_state:get_avgLoad(State)},
				 {min,gossip_state:get_minLoad(State)},
				 {max,gossip_state:get_maxLoad(State)},
				 {stddev,gossip_state:calc_stddev(State)},
				 {size,gossip_state:calc_size(State)},
				 {size_kr,gossip_state:calc_size_kr(State)},
				 
				 {avg,previous_or_current(PreviousState, State,
				     fun gossip_state:get_avgLoad/1)},
				 {min,previous_or_current(PreviousState, State,
				     fun gossip_state:get_minLoad/1)},
				 {max,previous_or_current(PreviousState, State,
				     fun gossip_state:get_maxLoad/1)},
				 {stddev,previous_or_current(PreviousState, State,
				     fun gossip_state:calc_stddev/1)},
				 {size,previous_or_current(PreviousState, State,
				     fun gossip_state:calc_size/1)},
				 {size_kr,previous_or_current(PreviousState, State,
				     fun gossip_state:calc_size_kr/1)}]),
			ok
	end,
    {PreviousState, State, QueuedMessages, TriggerState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State exchange
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state, Source_PID, OtherValues} = Msg,
   {MyPreviousState, MyState, QueuedMessages, TriggerState}) ->
	% This message is received when a node asked our node for its state.
	% The piggy-backed other node's state will be used to update our own state
	% if we have already initialized it (otherwise postpone the message). A
	% get_state_response message is sent with our state in the first case.
	Initialized = gossip_state:is_initialized(MyState),
	{NewQueuesMessages, {MyNewPreviousState, MyNewState}} =
		case Initialized of
			true -> {QueuedMessages, integrate_state(OtherValues, MyPreviousState, MyState, true, Source_PID)};
			false ->
				{[Msg | QueuedMessages], enter_round(MyPreviousState, MyState, OtherValues)}
			end,
    {MyNewPreviousState, MyNewState, NewQueuesMessages, TriggerState};

on({get_state_response, OtherState},
   {MyPreviousState, MyState, QueuedMessages, TriggerState}) ->
	% This message is received as a response to a get_state message and contains
    % another node's state. We will use it to update our own state
	% if both are valid.
	{MyNewPreviousState, MyNewState} =
		integrate_state(OtherState, MyPreviousState, MyState, false, none),
    {MyNewPreviousState, MyNewState, QueuedMessages, TriggerState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Contacting random nodes (response from cyclon)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({cache, Cache}, {_PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	% This message is received as a response to a get_subset message to the
    % cyclon process and should contain a random node. We will then contact this
    % random node and ask for a state exchange.
%%     io:format("gossip: got random node from Cyclon: ~p~n",[Cache]),
    case Cache of
        [Node] ->
			NodePid = node:pidX(Node),
			SelfPid = cs_send:make_global(process_dictionary:get_group_member(cs_node)),
			% do not exchange states with itself
			if
				(NodePid =/= SelfPid) ->
            		cs_send:send_to_group_member(node:pidX(Node), gossip,
						{get_state, cs_send:this(), gossip_state:get_values(State)});
				true -> ok
			end,
            FullState;
        [] -> FullState
    end;

% responds to ping messages with a pong message
%on({ping, Pid}, State) ->
%    %log:log(info, "ping ~p", [Pid]),
%    cs_send:send(Pid, {pong, cs_send:this()}),
%    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getter messages (need to update!)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_avgLoad, SourcePid}, {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	Avg = previous_or_current(PreviousState, State,
		fun gossip_state:get_avgLoad/1),
    cs_send:send(SourcePid, {gossip_get_avgLoad_response, Avg}),
    FullState;

on({get_minLoad, SourcePid}, {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	Min = previous_or_current(PreviousState, State,
		fun gossip_state:get_minLoad/1),
    cs_send:send(SourcePid, {gossip_get_minLoad_response, Min}),
    FullState;

on({get_maxLoad, SourcePid}, {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	Max = previous_or_current(PreviousState, State,
		fun gossip_state:get_maxLoad/1),
    cs_send:send(SourcePid, {gossip_get_maxLoad_response, Max}),
    FullState;

on({get_size, SourcePid}, {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	Size = previous_or_current(PreviousState, State,
		fun gossip_state:calc_size/1),
    cs_send:send(SourcePid, {gossip_get_size_response, Size}),
    FullState;

on({get_size_kr, SourcePid}, {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	Size_kr = previous_or_current(PreviousState, State,
		fun gossip_state:calc_size_kr/1),
    cs_send:send(SourcePid, {gossip_get_size_kr_response, Size_kr}),
    FullState;

on({get_stddev, SourcePid}, {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	Stddev = previous_or_current(PreviousState, State,
		fun gossip_state:calc_stddev/1),
    cs_send:send(SourcePid, {gossip_get_stddev_response, Stddev}),
    FullState;

on({get_all, SourcePid}, {PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
	PreviousStateConverted = gossip_state:conv_state_to_extval(PreviousState),
	StateConverted = gossip_state:conv_state_to_extval(State),
    cs_send:send(SourcePid, {gossip_get_all_response, PreviousStateConverted, StateConverted}),
    FullState.

%on(_Message, _State) ->
%    unknown_event.

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
	MyValues = gossip_state:get_values(MyState),
	Initialized = gossip_state:is_initialized(MyState),
	MyRound = gossip_state:get_round(MyValues),
	OtherRound = gossip_state:get_round(OtherValues),
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
	MyValues = gossip_state:get_values(MyState),
	MyNewValues = 
		case gossip_state:get_round(MyValues) =:= gossip_state:get_round(OtherValues) of
			true ->
				V1 = update_avg(MyValues, OtherValues),
				V2 = update_min(V1, OtherValues),
				V3 = update_max(V2, OtherValues),
				V4 = update_size_n(V3, OtherValues),
				V5 = update_avg2(V4, OtherValues),
				_V6 = update_size_kr(V5, OtherValues);
			false ->
				% this case should not happen since the on/2 handlers should only
				% call update/2 if the rounds match
            	log:log(error,"[ Node | ~w ] gossip:update rounds not equal (ignoring): ~p", [self(),util:get_stacktrace()]),
    			MyValues
		end,
	% now check whether all average-based values changed less than epsilon percent:
	Epsilon_Avg = get_converge_avg_epsilon(),
	AvgChanged = calc_change(gossip_state:get_avgLoad(MyValues), gossip_state:get_avgLoad(MyNewValues)),
	SizeNChanged = calc_change(gossip_state:get_size_n(MyValues), gossip_state:get_size_n(MyNewValues)),
	Avg2Changed = calc_change(gossip_state:get_avgLoad2(MyValues), gossip_state:get_avgLoad2(MyNewValues)),
	SizeKRChanged = calc_change(gossip_state:get_size_kr(MyValues), gossip_state:get_size_kr(MyNewValues)),
	MyNewState =
		case (AvgChanged < Epsilon_Avg) and
             (SizeNChanged < Epsilon_Avg) and
             (Avg2Changed < Epsilon_Avg) and
             (SizeKRChanged < Epsilon_Avg) of
			true -> gossip_state:inc_converge_avg_count(MyState); 
			false -> gossip_state:reset_converge_avg_count(MyState)
		end,
	Result = gossip_state:set_values(MyNewState, MyNewValues),
	?GOSSIP_REQUEST_LEADER_DEBUG_OUTPUT(),
	Result.

-spec calc_change(number() | unknown, number() | unknown) -> float() | unknown.
calc_change(Old, New) ->
	if
		(Old =/= unknown) and (Old =:= New) -> 0.0;
		(Old =:= unknown) orelse (New =:= unknown) orelse (Old == 0) -> 100.0;
		true -> ((Old + abs(New - Old)) * 100.0 / Old) - 100
	end.

%% @doc Updates the min load field of the state record with the min load
%%      of an other node's state.
-spec update_min(values_internal(), values_internal()) -> values_internal().
update_min(MyValues, OtherValues) ->
	MyMinLoad = gossip_state:get_minLoad(MyValues),
	OtherMinLoad = gossip_state:get_minLoad(OtherValues),
	MyNewMinLoad =
		if
			MyMinLoad =:= unknown
				-> OtherMinLoad;
			(MyMinLoad < OtherMinLoad) orelse (OtherMinLoad =:= unknown)
				-> MyMinLoad;
			true
				-> OtherMinLoad
		end,
	gossip_state:set_minLoad(MyValues, MyNewMinLoad).

%% @doc Updates the max load field of the state record with the max load
%%      of an other node's state.
-spec update_max(values_internal(), values_internal()) -> values_internal().
update_max(MyValues, OtherValues) ->
	MyMaxLoad = gossip_state:get_maxLoad(MyValues),
	OtherMaxLoad = gossip_state:get_maxLoad(OtherValues),
	MyNewMaxLoad =
		if
			MyMaxLoad =:= unknown
				-> OtherMaxLoad;
			(MyMaxLoad > OtherMaxLoad) orelse (OtherMaxLoad =:= unknown)
				-> MyMaxLoad;
			true
				-> OtherMaxLoad
		end,
	gossip_state:set_maxLoad(MyValues, MyNewMaxLoad).

%% @doc Updates the average load field of the state record with the average load
%%      of an other node's state.
-spec update_avg(values_internal(), values_internal()) -> values_internal().
update_avg(MyValues, OtherValues) ->
	MyAvg = gossip_state:get_avgLoad(MyValues), 
	OtherAvg = gossip_state:get_avgLoad(OtherValues),
	gossip_state:set_avgLoad(MyValues, calc_avg(MyAvg, OtherAvg)).

%% @doc Updates the size_n field of the state record with the size_n field of an
%%      other node's state.
-spec update_size_n(values_internal(), values_internal()) -> values_internal().
update_size_n(MyValues, OtherValues) ->
	MySize_n = gossip_state:get_size_n(MyValues), 
	OtherSize_n = gossip_state:get_size_n(OtherValues),
	gossip_state:set_size_n(MyValues, calc_avg(MySize_n, OtherSize_n)).

%% @doc Updates the size_kr field of the state record with the size_kr field of
%%      an other node's state.
-spec update_size_kr(values_internal(), values_internal()) -> values_internal().
update_size_kr(MyValues, OtherValues) ->
	MySize_kr = gossip_state:get_size_kr(MyValues), 
	OtherSize_kr = gossip_state:get_size_kr(OtherValues),
	gossip_state:set_size_kr(MyValues, calc_avg(MySize_kr, OtherSize_kr)).

%% @doc Updates the avg2 field of the state record with the avg2 field of an
%%      other node's state.
-spec update_avg2(values_internal(), values_internal()) -> values_internal().
update_avg2(MyValues, OtherValues) ->
	MyAvg2 = gossip_state:get_avgLoad2(MyValues), 
	OtherAvg2 = gossip_state:get_avgLoad2(OtherValues),
	gossip_state:set_avgLoad2(MyValues, calc_avg(MyAvg2, OtherAvg2)).

%% @doc Calculates the average of the two given values. If MyValue is unknown,
%%      OtherValue will be returned.
-spec calc_avg(number() | unknown, number()) -> number().
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
%%      size_kr) in its real state. This should (only) be called when a load
%%      and key range information message from the local node has been received
%%      for the first time in a round.
-spec integrate_local_info(state(), load(), size_kr()) -> state().
integrate_local_info(MyState, Load, Size_kr) ->
	MyValues = gossip_state:get_values(MyState),
	ValuesWithNewInfo =
		gossip_state:new_internal(Load, % Avg
								  Load*Load, % Avg2
								  unknown, % Size_n (will be set when entering/creating a round)
								  Size_kr, % Size_kr (see integrate_size_kr/2)
								  Load, % Min
								  Load, % Max
								  gossip_state:get_round(MyValues)),
	V1 = update_avg(MyValues, ValuesWithNewInfo),
	V2 = update_avg2(V1, ValuesWithNewInfo),
	V3 = update_min(V2, ValuesWithNewInfo),
	V4 = update_max(V3, ValuesWithNewInfo),
	MyNewValues = update_size_kr(V4, ValuesWithNewInfo),
	S1 = gossip_state:set_values(MyState, MyNewValues),
	_MyNewState = gossip_state:set_initialized(S1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Round handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a new round (this should only be done by the leader!).
-spec new_round(state()) -> {state(), state()}.
new_round(OldState) ->
%%     io:format("gossip: start new round, I am the leader ~n"),
	% the leader must set the size_n to 1 (and only the leader)
	NewValues1 = gossip_state:set_size_n(gossip_state:new_internal(), 1.0),
	OldRound = gossip_state:get_round(OldState),
	NewState =
		gossip_state:new_state(
		  gossip_state:set_round(NewValues1, (OldRound+1))),
    request_local_info(),
	{OldState, NewState}.

%% @doc Enters the given round and thus resets the current state with
%%      information from another node's state. Also requests the own node's
%%      load and key range to integrate into its own state.
-spec enter_round(state(), state(), values_internal()) -> {state(), state()}.
enter_round(OldPreviousState, OldState, OtherValues) ->
	MyRound = gossip_state:get_round(OldState),
	OtherRound = gossip_state:get_round(OtherValues),
	case (MyRound =:= OtherRound) of 
		true -> {OldPreviousState, OldState};
		false ->
			% never copy the size_n value!
%% 			S1 = gossip_state:new_state(gossip_state:set_size_n(OtherValues, 0.0)),
%% 			NewState = gossip_state:inc_msg_exch(S1),
			S1 = gossip_state:new_state(),
			NewState = gossip_state:set_size_n(
						 gossip_state:set_round(S1, OtherRound), 0),
    		request_local_info(),
			{OldState, NewState}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a message to the gossip process of the requesting process' group
%%      asking for the average load.
%%      see on({get_avgLoad, SourcePid}, FullState)
get_avgLoad() ->
	cs_send:send_local(process_dictionary:get_group_member(gossip),
		{get_avgLoad, cs_send:this()}).

%% @doc Sends a message to the gossip process of the requesting process' group
%%      asking for the minimum load.
%%      see on({get_minLoad, SourcePid}, FullState)
get_minLoad() ->
	cs_send:send_local(process_dictionary:get_group_member(gossip),
		{get_minLoad, cs_send:this()}).

%% @doc Sends a message to the gossip process of the requesting process' group
%%      asking for the maximum load.
%%      see on({get_maxLoad, SourcePid}, FullState)
get_maxLoad() ->
	cs_send:send_local(process_dictionary:get_group_member(gossip),
		{get_maxLoad, cs_send:this()}).

%% @doc Sends a message to the gossip process of the requesting process' group
%%      asking for the size estimated by the leader adding a value of 1 and all
%%      other nodes adding a value of 0 and calculating the average over it
%%      (leads to 1/size - the response will contain the actual size though).
%%      see on({get_size, SourcePid}, FullState)
get_size() ->
	cs_send:send_local(process_dictionary:get_group_member(gossip),
		{get_size, cs_send:this()}).

%% @doc Sends a message to the gossip process of the requesting process' group
%%      asking for the size estimated by using the key ranges of the nodes.
%%      see on({get_size_kr, SourcePid}, FullState)
get_size_kr() ->
	cs_send:send_local(process_dictionary:get_group_member(gossip),
		{get_size_kr, cs_send:this()}).

%% @doc Sends a message to the gossip process of the requesting process' group
%%      asking for the standard deviation of the load.
%%      see on({get_stddev, SourcePid}, FullState)
get_stddev() ->
	cs_send:send_local(process_dictionary:get_group_member(gossip),
		{get_stddev, cs_send:this()}).

%% @doc Sends a message to the gossip process of the requesting process' group
%%      asking for all stored information.
%%      see on({get_all, SourcePid}, FullState)
get_all() ->
	cs_send:send_local(process_dictionary:get_group_member(gossip),
		{get_all, cs_send:this()}).

%% @doc Returns the value from the previous state if the current state has not
%%      sufficiently converged yet.
-spec previous_or_current(any(), any(), fun((state()) -> any())) -> any().
previous_or_current(PreviousState, CurrentState, GetFun) ->
	CurrentInitialized = gossip_state:is_initialized(CurrentState),
	MinConvergeAvgCount = get_converge_avg_count(),
	CurrentEpsilonCount_Avg = gossip_state:get_converge_avg_count(CurrentState),
	_BestValue =
		case (not CurrentInitialized) or (CurrentEpsilonCount_Avg < MinConvergeAvgCount) of
			true -> GetFun(PreviousState);
			false -> GetFun(CurrentState)
		end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Requests send to other processes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends the local node's cs_node a request to tell us its successor and
%%      predecessor if a new round should be started. A new round will then only
%%      be started if we are the leader, i.e. we are responsible for key 0.
%%      The node will respond with a get_pred_me_response message.
-spec request_new_round_if_leader(state()) -> ok.
request_new_round_if_leader(State) ->
	Round = gossip_state:get_round(State),
	TriggerCount = gossip_state:get_triggered(State),
	ConvAvgCount = gossip_state:get_converge_avg_count(State),
	TPR_max = get_max_tpr(), % max triggers per round
	TPR_min = get_min_tpr(), % min triggers per round
	ConvAvgCountNewRound = get_converge_avg_count_start_new_round(),
	% decides when to ask whether we are the leader
	% on({get_pred_me_response, Pred, Me, leader_start_new_round}, State) will then handle the
	% response
	case (Round =:= 0) orelse
         ((TriggerCount > TPR_min) andalso (
             (TriggerCount > TPR_max) orelse
             (ConvAvgCount >= ConvAvgCountNewRound)))
		of
		true ->
			CS_Node = process_dictionary:get_group_member(cs_node),
    		cs_send:send_local(CS_Node, {get_node_details, cs_send:this(), [my_range], leader_start_new_round}),
			ok;
		false ->
			ok
	end.

%% @doc Sends the local node's cs_node a request to tell us its successor and
%%      predecessor in order to allow debug output at the leader only.
%%      The node will respond with a get_pred_me_response message.
-spec request_leader_debug_output() -> ok.
request_leader_debug_output() ->
	CS_Node = process_dictionary:get_group_member(cs_node),
	cs_send:send_local(CS_Node, {get_node_details, cs_send:this(), [my_range], leader_debug_output}).

%% @doc Sends the local node's cs_node a request to tell us some information
%%      about itself.
-spec request_local_info() -> ok.
request_local_info() ->
	% ask for local load and key range:
	% on({get_node_details_response, local_info, NodeDetails}, State) will handle
	% the response
	CS_Node = process_dictionary:get_group_member(cs_node),
    cs_send:send_local(CS_Node, {get_node_details, cs_send:this(), [my_range, load], local_info}).

%% @doc Sends the local node's cyclon process a request for a random node.
%%      on({cache, Cache},State) will handle the response
-spec request_random_node() -> ok.
request_random_node() ->
	CyclonPid = process_dictionary:get_group_member(cyclon),
	cs_send:send_local(CyclonPid,{get_subset, 1, self()}),
	ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the total number of keys available.
-spec get_addr_size() -> number().
get_addr_size() ->
	rt_simple:n().

%% @doc Calculates the initial estimate of the ring size based on distance to 
%%      predecessor and successor.
-spec calc_initial_size_kr({any(), any()}) -> number().
calc_initial_size_kr({PredKey, MyKey} = _Range) ->
    if
		PredKey =:= MyKey ->
	    	% I am the only node
	    	get_addr_size();
		true ->
			try calc_dist(MyKey, PredKey)
			catch
				error:_ -> unknown
			end
	end.

%% @doc Calculates the difference between the keys of a node and its
%%      predecessor. If the second is larger than the first it wraps around and
%%      thus the difference is the number of keys from the predecessor to the
%%      end (of the ring) and from the start to the current node.
-spec calc_dist(any(), any()) -> number().
calc_dist(MyKey, PredKey) ->
	case MyKey >= PredKey of
		true  -> MyKey - PredKey;
		false -> (get_addr_size() - PredKey - 1) + MyKey
	end.

%% @doc Gets the gossip interval set in scalaris.cfg.
-spec get_base_interval() -> non_neg_integer().
get_base_interval() ->
    _GossipInterval = 
    	case config:read(gossip_interval) of 
        	failed ->
            	log:log(warning,"gossip_interval not defined (see scalaris.cfg), using default (1000)~n"),
				1000;
			X -> X
		end.

%% @doc Gets the number of minimum triggers a round should have (set in
%%      scalaris.cfg). A new round will not be started as long as this number
%%      has not been reached at the leader.
-spec get_min_tpr() -> pos_integer().
get_min_tpr() ->
    _MinTPR = 
	    case config:read(gossip_min_triggers_per_round) of 
    	    failed ->
        	    log:log(warning,"gossip_min_triggers_per_round not defined (see scalaris.cfg), using default (10)~n"),
				10;
			X -> X
		end.

%% @doc Gets the number of maximum triggers a round should have (set in
%%      scalaris.cfg). A new round will be started when this number is reached
%%      at the leader.
-spec get_max_tpr() -> pos_integer().
get_max_tpr() ->
    _MaxTPR = 
	    case config:read(gossip_max_triggers_per_round) of 
    	    failed ->
        	    log:log(warning,"gossip_max_triggers_per_round not defined (see scalaris.cfg), using default (1000)~n"),
				1000;
			X -> X
		end.

%% @doc Gets the epsilon parameter that defines the maximum change of
%%      average-based values (in percent) to be considered as "converged", i.e.
%%      stable.
-spec get_converge_avg_epsilon() -> float().
get_converge_avg_epsilon() ->
    _ConvergeAvgEpsilon = 
	    case config:read(gossip_converge_avg_epsilon) of 
    	    failed ->
        	    log:log(warning,"gossip_converge_avg_epsilon not defined (see scalaris.cfg), using default (5.0)~n"),
				5.0;
			X -> X
		end.

%% @doc Gets the count parameter that defines how often average-based values
%%      should change within an epsilon in order to be considered as
%%      "converged", i.e. stable.
-spec get_converge_avg_count() -> pos_integer().
get_converge_avg_count() ->
    _ConvergeAvgCount = 
	    case config:read(gossip_converge_avg_count) of 
    	    failed ->
        	    log:log(warning,"gossip_converge_avg_count not defined (see scalaris.cfg), using default (10)~n"),
				10;
			X -> X
		end.

%% @doc Gets the count parameter that defines how often average-based values
%%      should change within an epsilon in order to start a new round.
-spec get_converge_avg_count_start_new_round() -> pos_integer().
get_converge_avg_count_start_new_round() ->
    _ConvergeAvgCountStartNewRound = 
	    case config:read(gossip_converge_avg_count_start_new_round) of 
    	    failed ->
        	    log:log(warning,"gossip_converge_avg_count_start_new_round not defined (see scalaris.cfg), using default (20)~n"),
				20;
			X -> X
		end.
