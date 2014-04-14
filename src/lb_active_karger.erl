%  @copyright 2013-2014 Zuse Institute Berlin

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

%% @author Maximilian Michels <michels@zib.de>
%% @doc Implementation of Karger and Ruhl's item balancing load balancing algorithm.
%%      Modified to sample N nodes and use gossip information.
%% @reference D. R. Karger and M. Ruhl,
%%            "Simple efficient load balancing algorithms for peer-to-peer systems,"
%%            in Proceedings of the sixteenth annual ACM symposium on Parallelism in algorithms and architectures,
%%            2004, pp. 36-43.
%% @version $Id$
-module(lb_active_karger).
-author('michels@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

%-define(TRACE(X,Y), ok).
-define(TRACE(X,Y), io:format(X,Y)).


-behavior(lb_active_beh).

-export([init/0, check_config/0]).
-export([handle_msg/2, handle_dht_msg/2]).
-export([get_web_debug_kv/1]).

-record(state, {epsilon          = ?required(state, epsilon) :: float(),
                rnd_node         = nil                       :: node:node_type() | nil
               }).

-type(state() :: #state{}).

-type(my_message() ::
           %% trigger messages
           {lb_trigger} |
           %% random node from cyclon
           {cy_cache, [node:node_type()]} |
           %% load response from dht node
           {my_dht_response, DhtNode :: comm:mypid(), {get_state_response, Load :: lb_info:load()}} |
           %% Result from slide or jump
           dht_node_move:result_message()).

-type(dht_message() ::
		   %% phase1
		   {lb_active, phase1, Epsilon :: float(), NodeX :: lb_info:lb_info()} |
		   %% phase2
		   {lb_active, phase2, HeavyNode :: lb_info:lb_info(), LightNode :: lb_info:lb_info()}).
		   %% TODO final phase (handled by lb_active module)
		   %{lb_active, balance_with, LightNode :: lb_info:lb_info()}).

%%%%%%%%%%%%%%%
%%  Startup   %
%%%%%%%%%%%%%%%

%% @doc Initialization of module called by lb_active
-spec init() -> state().
init() ->
    %msg_delay:send_trigger(get_base_interval(), {lb_trigger}),
    Epsilon = config:read(lb_active_karger_epsilon),
    #state{epsilon = Epsilon}.

%%%%%%%%%%%%%%%
%%  Trigger   %
%%%%%%%%%%%%%%%

-spec handle_msg(my_message(), state()) -> state().
handle_msg({lb_trigger}, State) ->
    msg_delay:send_trigger(get_base_interval(), {lb_trigger}),
    %% Request 1 random node from cyclon
    %% TODO Request more to have a bigger sample size
    cyclon:get_subset_rand(1),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  Handling of lb process related messages  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% No random nodes available via cyclon
handle_msg({cy_cache, []}, State) ->
    ?TRACE("Cyclon returned no random node~n", []),
    State;

%% Got a random node via cyclon
handle_msg({cy_cache, [RandomNode]}, State) ->
    ?TRACE("Got random node~n", []),
    Envelope = comm:reply_as(comm:this(), 2, {my_dht_response, '_'}),
    comm:send(comm:this(), {get_node_details, Envelope}, [{group_member, dht_node}]),
    State#state{rnd_node = RandomNode};

%% Got load from my node
handle_msg({my_dht_response, {get_node_details_response, NodeDetails}}, State) ->
	?TRACE("Received node details for own node~n", []),
	RndNode = State#state.rnd_node,
	Epsilon = State#state.epsilon,
	comm:send(node:pidX(RndNode), {lb_active, phase1, Epsilon, lb_info:new(NodeDetails)}, [{?quiet}]),
	State#state{rnd_node = nil}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  Static methods called by dht_node message handler  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec handle_dht_msg(dht_message(), dht_node_state:state()) -> dht_node_state:state().
%% First phase: We were contacted by another node who chose
%% us as a random node. In this phase we'll determine if
%% load balancing is necessary. If so, we'll try to balance
%% assuming the two nodes are neighbors. If not we'll contact
%% the light node's successor for more load information.
handle_dht_msg({lb_active, phase1, Epsilon, NodeX}, DhtState) ->
	MyLBInfo = lb_info:new(dht_node_state:details(DhtState)),
	MyLoad = lb_info:get_load(MyLBInfo),
	LoadX = lb_info:get_load(NodeX),
	case MyLoad =/= 0 orelse LoadX =/= 0 of
		true ->
			if
    % first check if load balancing is necessary
    % TODO gossip here to improve load balancing
				MyLoad =< Epsilon * LoadX ->
					?TRACE("My node is light~n", []),
					balance_adjacent(NodeX, MyLBInfo);
				LoadX =< Epsilon * MyLoad ->
					?TRACE("My node is heavy~n", []),
					balance_adjacent(MyLBInfo, NodeX);
				true ->
					%% no balancing
					?TRACE("Won't balance~n", [])
			end;
		_ -> ok
	end,
	DhtState;

%% Second phase: We are LightNode's successor. We might hold
%% more load than the HeavyNode. If so, we'll slide with the
%% LightNode. Otherwise we instruct the HeavyNode to set up
%% a jump operation with the Lightnode.
handle_dht_msg({lb_active, phase2, HeavyNode, LightNode}, DhtState) ->
	?TRACE("In phase 2~n", []),
	MyLBInfo = lb_info:new(dht_node_state:details(DhtState)),
	MyLoad = lb_info:get_load(MyLBInfo),
	LoadHeavyNode = lb_info:get_load(HeavyNode),
	case MyLoad > LoadHeavyNode of
		true ->
			balance_adjacent(MyLBInfo, LightNode);
		_ ->
            lb_active:balance_nodes(LightNode, HeavyNode)
	end,
	DhtState.

%%%%%%%%%%%%%%%%%%%%
%% Helper methods  %
%%%%%%%%%%%%%%%%%%%%

%% @doc Balance if the two nodes are adjacent, otherwise ask the light node's neighbor
-spec balance_adjacent(lb_info:lb_info(), lb_info:lb_info()) -> ok.
balance_adjacent(HeavyNode, LightNode) ->
	case lb_info:is_succ(HeavyNode, LightNode) of
		 %orelse node_details:get(HeavyNode, node) =:= node_details:get(LightNode, pred) of
		true ->
			% neighbors, thus sliding
			?TRACE("We're neighbors~n", []),
            %% slide in phase1 or phase2
            lb_active:balance_nodes(LightNode, HeavyNode);
		_ ->
			% ask the successor of the light node how much load he carries
			?TRACE("Nodes not adjacent, requesting information about neighbors~n", []),
            LightNodeSucc = lb_info:get_succ(LightNode),
			comm:send(node:pidX(LightNodeSucc), {lb_active, phase2, HeavyNode, LightNode})
	end.

%% @doc Key/Value List for web debug
-spec get_web_debug_kv(state()) -> [{string(), string()}].
get_web_debug_kv(State) ->
    [{"state", webhelpers:html_pre(State)}].

-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(lb_active_interval) div 1000.

-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(lb_active_interval) andalso
    config:cfg_is_greater_than(lb_active_interval, 0) andalso
    config:cfg_is_float(lb_active_karger_epsilon) andalso
    config:cfg_is_greater_than(lb_active_karger_epsilon, 0.0) andalso
    config:cfg_is_less_than(lb_active_karger_epsilon, 0.25).
