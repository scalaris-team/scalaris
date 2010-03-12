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
%%%  gossip process has to ask its cs_node for information about its state,
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
%%%  which intervals:is_between(PredId, 0, MyId) is true. It will propagate its
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
-module(gossip,[Trigger]).

-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

%% -define(GOSSIP_REQUEST_LEADER_DEBUG_OUTPUT(), request_leader_debug_output()).
-define(GOSSIP_REQUEST_LEADER_DEBUG_OUTPUT(), ok).

-export([start_link/1]).

-export([on/2, init/1, get_base_interval/0]).

% helpers for creating getter messages:
-export([get_values_best/0, get_values_best/1,
		 get_values_all/0, get_values_all/1
]).

-type(load() :: integer()).

-type(state() :: gossip_state:state()).
-type(values() :: gossip_state:values()).
-type(values_internal() :: gossip_state:values_internal()).
%% -type(values() :: gossip_state:values()).
-type(avg_kr() :: gossip_state:avg_kr()).

% {PreviousState, CurrentState, QueuedMessages, TriggerState}
-type(full_state() :: {state(), state(), list(), any()}).

% accepted messages of gossip processes
-type(message() ::
	{trigger} |
	{{get_node_details_response, node_details:node_details()}, local_info} |
	{{get_node_details_response, node_details:node_details()}, leader_start_new_round} |
	{{get_node_details_response, node_details:node_details()}, leader_debug_output} |
	{get_state, cs_send:mypid(), values_internal()} |
	{get_state_response, values_internal()} |
	{cache, [node:node_type()]} |
	{get_avgLoad, cs_send:mypid()} |
	{get_minLoad, cs_send:mypid()} |
	{get_maxLoad, cs_send:mypid()} |
	{get_size, cs_send:mypid()} |
	{get_stddev, cs_send:mypid()} |
	{get_all, cs_send:mypid()}).

%% @doc Sends a response message to a request for the best stored values.
-spec msg_get_values_best_response(cs_send:mypid(), values()) -> ok.
msg_get_values_best_response(Pid, BestValues) ->
    cs_send:send(Pid, {get_values_all_response, BestValues}).

%% @doc Sends a response message to a request for all stored values.
-spec msg_get_values_all_response(cs_send:mypid(), values(), values(), values()) -> ok.
msg_get_values_all_response(Pid, PreviousValues, CurrentValues, BestValues) ->
    cs_send:send(Pid, {get_values_all_response, PreviousValues, CurrentValues, BestValues}).

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
-spec on(message(), full_state()) -> full_state() | unknown_event.
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

on({{get_node_details_response, NodeDetails}, local_info},
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
		case intervals:is_between(PredId, 0, MyId) of
	        % not the leader -> continue as normal with the old state
			false -> {PreviousState, State};
			% leader -> start a new round
			true -> new_round(State)
	end,
    {NewPreviousState, NewState, QueuedMessages, TriggerState};

%% Prints some debug information if the current node is the leader.
on({{get_node_details_response, NodeDetails}, leader_debug_output},
   {PreviousState, State, QueuedMessages, TriggerState}) ->
	% this message can only be received after being requested by
	% request_leader_debug_output/0
%%     io:format("gossip: got get_node_details_response, leader_debug_output: ~p~n",[NodeDetails]),
	{PredId, MyId} = node_details:get(NodeDetails, my_range),
	case intervals:is_between(PredId, 0, MyId) of
        % not the leader
		false -> ok;
		% leader -> provide debug information
		true ->
			BestState = previous_or_current(PreviousState, State),
			io:format("gossip:~n    prv: ~p, ~p, ~p, ~p:~n         ~p, ~p, ~p, ~p,~n         ~p, ~p~n    cur: ~p, ~p, ~p, ~p:~n         ~p, ~p, ~p, ~p,~n         ~p, ~p~n    usr: ~p, ~p, ~p, ~p,~n         ~p, ~p~n",
				[{round,gossip_state:get_round(PreviousState)},
				 {triggered,gossip_state:get_triggered(PreviousState)},
				 {msg_exch,gossip_state:get_msg_exch(PreviousState)},
				 {converge_avg_count,gossip_state:get_converge_avg_count(PreviousState)},
				 {avg,gossip_state:get_avgLoad(PreviousState)},
				 {min,gossip_state:get_minLoad(PreviousState)},
				 {max,gossip_state:get_maxLoad(PreviousState)},
				 {stddev,gossip_state:calc_stddev(PreviousState)},
				 {size_ldr,gossip_state:calc_size_ldr(PreviousState)},
				 {size_kr,gossip_state:calc_size_kr(PreviousState)},
				 
				 {round,gossip_state:get_round(State)},
				 {triggered,gossip_state:get_triggered(State)},
				 {msg_exch,gossip_state:get_msg_exch(State)},
				 {converge_avg_count,gossip_state:get_converge_avg_count(State)},
				 {avg,gossip_state:get_avgLoad(State)},
				 {min,gossip_state:get_minLoad(State)},
				 {max,gossip_state:get_maxLoad(State)},
				 {stddev,gossip_state:calc_stddev(State)},
				 {size_ldr,gossip_state:calc_size_ldr(State)},
				 {size_kr,gossip_state:calc_size_kr(State)},
				 
				 {avg,gossip_state:get_avgLoad(BestState)},
				 {min,gossip_state:get_minLoad(BestState)},
				 {max,gossip_state:get_maxLoad(BestState)},
				 {stddev,gossip_state:calc_stddev(BestState)},
				 {size_ldr,gossip_state:calc_size_ldr(BestState)},
				 {size_kr,gossip_state:calc_size_kr(BestState)}]),
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

on({cache, Cache},
    {_PreviousState, State, _QueuedMessages, _TriggerState} = FullState) ->
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
    BestState = gossip_state:conv_state_to_extval(previous_or_current(PreviousState, State)),
    KeyValueList =
        [{"prev_round",          gossip_state:get_round(PreviousState)},
         {"prev_triggered",      gossip_state:get_triggered(PreviousState)},
         {"prev_msg_exch",       gossip_state:get_msg_exch(PreviousState)},
         {"prev_conv_avg_count", gossip_state:get_converge_avg_count(PreviousState)},
         {"prev_avg",            gossip_state:get_avgLoad(PreviousState)},
         {"prev_min",            gossip_state:get_minLoad(PreviousState)},
         {"prev_max",            gossip_state:get_maxLoad(PreviousState)},
         {"prev_stddev",         gossip_state:calc_stddev(PreviousState)},
         {"prev_size_ldr",       gossip_state:calc_size_ldr(PreviousState)},
         {"prev_size_kr",        gossip_state:calc_size_kr(PreviousState)},
         
         {"cur_round",           gossip_state:get_round(State)},
         {"cur_triggered",       gossip_state:get_triggered(State)},
         {"cur_msg_exch",        gossip_state:get_msg_exch(State)},
         {"cur_conv_avg_count",  gossip_state:get_converge_avg_count(State)},
         {"cur_avg",             gossip_state:get_avgLoad(State)},
         {"cur_min",             gossip_state:get_minLoad(State)},
         {"cur_max",             gossip_state:get_maxLoad(State)},
         {"cur_stddev",          gossip_state:calc_stddev(State)},
         {"cur_size_ldr",        gossip_state:calc_size_ldr(State)},
         {"cur_size_kr",         gossip_state:calc_size_kr(State)},
         
         {"best_avg",             gossip_state:get_avgLoad(BestState)},
         {"best_min",             gossip_state:get_minLoad(BestState)},
         {"best_max",             gossip_state:get_maxLoad(BestState)},
         {"best_stddev",          gossip_state:get_stddev(BestState)},
         {"best_size",            gossip_state:get_size(BestState)},
         {"best_size_ldr",        gossip_state:get_size_ldr(BestState)},
         {"best_size_kr",         gossip_state:get_size_kr(BestState)}],
    cs_send:send_local(Requestor , {debug_info_response, KeyValueList}),
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
				V4 = update_size_inv(V3, OtherValues),
				V5 = update_avg2(V4, OtherValues),
				_V6 = update_avg_kr(V5, OtherValues);
			false ->
				% this case should not happen since the on/2 handlers should only
				% call update/2 if the rounds match
            	log:log(error,"[ Node | ~w ] gossip:update rounds not equal (ignoring): ~p", [self(),util:get_stacktrace()]),
    			MyValues
		end,
	% now check whether all average-based values changed less than epsilon percent:
	Epsilon_Avg = get_converge_avg_epsilon(),
	AvgChanged = calc_change(gossip_state:get_avgLoad(MyValues), gossip_state:get_avgLoad(MyNewValues)),
	SizeNChanged = calc_change(gossip_state:get_size_inv(MyValues), gossip_state:get_size_inv(MyNewValues)),
	Avg2Changed = calc_change(gossip_state:get_avgLoad2(MyValues), gossip_state:get_avgLoad2(MyNewValues)),
	AvgKRChanged = calc_change(gossip_state:get_avg_kr(MyValues), gossip_state:get_avg_kr(MyNewValues)),
	MyNewState =
		case (AvgChanged < Epsilon_Avg) and
             (SizeNChanged < Epsilon_Avg) and
             (Avg2Changed < Epsilon_Avg) and
             (AvgKRChanged < Epsilon_Avg) of
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

%% @doc Updates the size_inv field of the state record with the size_inv field
%%      of an other node's state.
-spec update_size_inv(values_internal(), values_internal()) -> values_internal().
update_size_inv(MyValues, OtherValues) ->
	MySize_inv = gossip_state:get_size_inv(MyValues), 
	OtherSize_inv = gossip_state:get_size_inv(OtherValues),
	gossip_state:set_size_inv(MyValues, calc_avg(MySize_inv, OtherSize_inv)).

%% @doc Updates the avg_kr field of the state record with the avg_kr field of
%%      an other node's state.
-spec update_avg_kr(values_internal(), values_internal()) -> values_internal().
update_avg_kr(MyValues, OtherValues) ->
	MyAvg_kr = gossip_state:get_avg_kr(MyValues), 
	OtherAvg_kr = gossip_state:get_avg_kr(OtherValues),
	gossip_state:set_avg_kr(MyValues, calc_avg(MyAvg_kr, OtherAvg_kr)).

%% @doc Updates the avg2 field of the state record with the avg2 field of an
%%      other node's state.
-spec update_avg2(values_internal(), values_internal()) -> values_internal().
update_avg2(MyValues, OtherValues) ->
	MyAvg2 = gossip_state:get_avgLoad2(MyValues), 
	OtherAvg2 = gossip_state:get_avgLoad2(OtherValues),
	gossip_state:set_avgLoad2(MyValues, calc_avg(MyAvg2, OtherAvg2)).

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
	MyValues = gossip_state:get_values(MyState),
	ValuesWithNewInfo =
		gossip_state:new_internal(Load, % Avg
								  Load*Load, % Avg2
								  unknown, % 1/size (will be set when entering/creating a round)
								  Avg_kr, % average key range
								  Load, % Min
								  Load, % Max
								  gossip_state:get_round(MyValues)),
	V1 = update_avg(MyValues, ValuesWithNewInfo),
	V2 = update_avg2(V1, ValuesWithNewInfo),
	V3 = update_min(V2, ValuesWithNewInfo),
	V4 = update_max(V3, ValuesWithNewInfo),
	MyNewValues = update_avg_kr(V4, ValuesWithNewInfo),
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
	NewValues1 = gossip_state:set_size_inv(gossip_state:new_internal(), 1.0),
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
			% set a size_inv value of 0 (only the leader sets 1)
			S1 = gossip_state:new_state(),
			NewState = gossip_state:set_size_inv(
						 gossip_state:set_round(S1, OtherRound), 0.0),
    		request_local_info(),
			{OldState, NewState}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for the best values of the stored information.
%%      see on({get_values_best, SourcePid}, FullState) and
%%      msg_get_values_best_response/2
-spec get_values_best() -> ok.
get_values_best() ->
    get_values_best(cs_send:this()).

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for the best values of the stored information to
%%      be send to Pid.
%%      see on({get_values_best, SourcePid}, FullState) and
%%      msg_get_values_best_response/2
-spec get_values_best(cs_send:erl_local_pid()) -> ok.
get_values_best((Pid)) ->
    cs_send:send_local(process_dictionary:get_group_member(gossip),
        {get_values_best, (Pid)}).

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for all stored information.
%%      see on({get_values_all, SourcePid}, FullState) and
%%      msg_get_values_all_response/4
-spec get_values_all() -> ok.
get_values_all() ->
    get_values_all(cs_send:this()).

%% @doc Sends a (local) message to the gossip process of the requesting
%%      process' group asking for all stored information to be send to Pid.
%%      see on({get_values_all, SourcePid}, FullState) and
%%      msg_get_values_all_response/4
-spec get_values_all(cs_send:erl_local_pid()) -> ok.
get_values_all(Pid) ->
    cs_send:send_local(process_dictionary:get_group_member(gossip),
        {get_values_all, Pid}).

%% @doc Returns the previous state if the current state has not sufficiently
%%      converged yet otherwise returns the current state.
-spec previous_or_current(state(), state()) -> state().
previous_or_current(PreviousState, CurrentState) ->
	CurrentInitialized = gossip_state:is_initialized(CurrentState),
	MinConvergeAvgCount = get_converge_avg_count(),
	CurrentEpsilonCount_Avg = gossip_state:get_converge_avg_count(CurrentState),
	_BestValue =
		case (not CurrentInitialized) or (CurrentEpsilonCount_Avg < MinConvergeAvgCount) of
			true -> PreviousState;
			false -> CurrentState
		end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Requests send to other processes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends the local node's cs_node a request to tell us its successor and
%%      predecessor if a new round should be started. A new round will then only
%%      be started if we are the leader, i.e. we are responsible for key 0.
%%      The node will respond with a
%%      {{get_node_details_response, NodeDetails}, leader_start_new_round}
%%      message.
-spec request_new_round_if_leader(state()) -> ok.
request_new_round_if_leader(State) ->
	Round = gossip_state:get_round(State),
	TriggerCount = gossip_state:get_triggered(State),
	ConvAvgCount = gossip_state:get_converge_avg_count(State),
	TPR_max = get_max_tpr(), % max triggers per round
	TPR_min = get_min_tpr(), % min triggers per round
	ConvAvgCountNewRound = get_converge_avg_count_start_new_round(),
	% decides when to ask whether we are the leader
	case (Round =:= 0) orelse
         ((TriggerCount > TPR_min) andalso (
             (TriggerCount > TPR_max) orelse
             (ConvAvgCount >= ConvAvgCountNewRound)))
		of
		true ->
			CS_Node = process_dictionary:get_group_member(cs_node),
    		cs_send:send_local(CS_Node, {get_node_details, cs_send:this_with_cookie(leader_start_new_round), [my_range]}),
			ok;
		false ->
			ok
	end.

%% @doc Sends the local node's cs_node a request to tell us its successor and
%%      predecessor in order to allow debug output at the leader only.
%%      The node will respond with a
%%      {{get_node_details_response, NodeDetails}, leader_debug_output}
%%      message.
-spec request_leader_debug_output() -> ok.
request_leader_debug_output() ->
	CS_Node = process_dictionary:get_group_member(cs_node),
	cs_send:send_local(CS_Node, {get_node_details, cs_send:this_with_cookie(leader_debug_output), [my_range]}).

%% @doc Sends the local node's cs_node a request to tell us some information
%%      about itself.
%%      The node will respond with a
%%      {{get_node_details_response, NodeDetails}, local_info} message.
-spec request_local_info() -> ok.
request_local_info() ->
	% ask for local load and key range:
	CS_Node = process_dictionary:get_group_member(cs_node),
    cs_send:send_local(CS_Node, {get_node_details, cs_send:this_with_cookie(local_info), [my_range, load]}).

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
