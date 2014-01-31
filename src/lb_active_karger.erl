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
%% @reference D. R. Karger and M. Ruhl,
%%            "Simple efficient load balancing algorithms for peer-to-peer systems,"
%%            in Proceedings of the sixteenth annual ACM symposium on Parallelism in algorithms and architectures,
%%            2004, pp. 36-43.
%% @version $Id$
-module(lb_active_karger).
-author('michels@zib.de').
-vsn('$Id$').

-behavior(gen_component).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-define(TRACE(X,Y), ok).
%%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X), ?TRACE(X, [])).

-export([start_link/1, init/1]).
-export([on/2, process_lb_msg/2]).
-export([check_config/0]).


-type(load() :: integer()).

-record(lb_info, {node = ?required(lb_info, node) :: node:node_type(),
                  load = ?required(lb_info, load) :: load(),
                  succ = ?required(lb_info, succ) :: node:node_type()
                 }).

-type(lb_info() :: #lb_info{}).

-record(state, {epsilon          = ?required(state, epsilon) :: float(),
                rnd_node         = nil                       :: node:node_type() | nil
               }).

-type(state() :: #state{}).

-type(my_message() ::
           %% trigger messages
           {lb_trigger} |
           %% actions for trigger
           {trigger_periodic} |
           %% random node from cyclon
           {cy_cache, [node:node_type()]} |
           %% load response from dht node
           {my_dht_response, DhtNode :: comm:mypid(), {get_state_response, Load :: load()}} |
           %% Result from slide or jump
           dht_node_move:result_message()).

-type(dht_message() ::
		   %% phase1
		   {balance_load, phase1, Epsilon :: float(), NodeX :: lb_info()} |
		   %% phase2
		   {balance_load, phase2, HeavyNode :: lb_info(), LightNode :: lb_info()} |
		   %% final phase
		   {balance_load, jump | slide, LightNode :: lb_info()}).

%%%%%%%%%%%%%%%
%%  Startup   %
%%%%%%%%%%%%%%%

%% @doc Start this process as a gen component and register it in the dht node group
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, lb_active_karger}]).

%% @doc Initialization of process called by gen_component.
-spec init([]) -> state().
init([]) ->
    msg_delay:send_trigger(get_base_interval(), {lb_trigger}),
    Epsilon = config:read(lb_active_karger_epsilon),
    #state{epsilon = Epsilon}.

%%%%%%%%%%%%%%%
%%  Trigger   %
%%%%%%%%%%%%%%%

-spec on(my_message(), state()) -> state().
on({lb_trigger}, State) ->
    msg_delay:send_trigger(get_base_interval(), {lb_trigger}),
    gen_component:post_op({trigger_periodic}, State);

on({trigger_periodic}, State) ->
    %% Request N random nodes from cyclon
    cyclon:get_subset_rand(1),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  Handling of lb process related messages  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% No random nodes available via cyclon
on({cy_cache, []}, State) ->
    ?TRACE("Cyclon returned no random node~n"),
    State;

%% Got a random node via cyclon
on({cy_cache, [RandomNode]}, State) ->
    ?TRACE("Got random node~n"),
    Envelope = comm:reply_as(comm:this(), 2, {my_dht_response, '_'}),
    comm:send(comm:this(), {get_node_details, Envelope}, [{group_member, dht_node}]),
    State#state{rnd_node = RandomNode};

%% Got load from my node
on({my_dht_response, {get_node_details_response, NodeDetails}}, State) ->
	?TRACE("Received node details for own node~n"),
	RndNode = State#state.rnd_node,
	Epsilon = State#state.epsilon,
	comm:send(node:pidX(RndNode), {balance_load, phase1, Epsilon, serialize(NodeDetails)}, [{?quiet}]),
	State#state{rnd_node = nil};

on({move, result, {tag, _JumpOrSlide}, _Status}, State) ->
	?TRACE("~p status: ~p~n", [_JumpOrSlide, _Status]),
	State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  Static methods called by dht_node message handler  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec process_lb_msg(dht_message(), dht_node_state:state()) -> dht_node_state:state().
%% First phase: We were contacted by another node who chose
%% us as a random node. In this phase we'll determine if
%% load balancing is necessary. If so, we'll try to balance
%% assuming the two nodes are neighbors. If not we'll contact
%% the light node's successor for more load information.
process_lb_msg({balance_load, phase1, Epsilon, NodeX}, DhtState) ->
	MyLBInfo = serialize(dht_node_state:details(DhtState)),
	MyLoad = get_load(MyLBInfo),
	LoadX = get_load(NodeX),
	case MyLoad =/= 0 orelse LoadX =/= 0 of
		true ->
			if
    % first check if load balancing is necessary
				MyLoad =< Epsilon * LoadX ->
					?TRACE("My node is light~n"),
					balance_adjacent(NodeX, MyLBInfo);
				LoadX =< Epsilon * MyLoad ->
					?TRACE("My node is heavy~n"),
					balance_adjacent(MyLBInfo, NodeX);
				true ->
					%% no balancing
					?TRACE("Won't balance~n")
			end;
		_ -> ok
	end,
	DhtState;

%% Second phase: We are LightNode's successor. We might hold
%% more load than the HeavyNode. If so, we'll slide with the
%% LightNode. Otherwise we instruct the HeavyNode to set up
%% a jump operation with the Lightnode.
process_lb_msg({balance_load, phase2, HeavyNode, LightNode}, DhtState) ->
	?TRACE("In phase 2~n"),
	MyLBInfo = serialize(dht_node_state:details(DhtState)),
	MyLoad = get_load(MyLBInfo),
	LoadHeavyNode = get_load(HeavyNode),
	case MyLoad > LoadHeavyNode of
		true ->
			balance_adjacent(MyLBInfo, LightNode);
		_ ->
			Node = get_node(HeavyNode),
			comm:send(node:pidX(Node), {balance_load, jump, LightNode})
	end,
	DhtState;

%% We received a jump or slide operation from a LightNode.
%% In either case, we'll compute the target id and send out
%% the jump or slide message to the LightNode.
process_lb_msg({balance_load, JumpOrSlide, LightNode}, DhtState) ->
	?TRACE("Before ~p Heavy: ~p Light: ~p~n",
            [JumpOrSlide, dht_node_state:get(DhtState, node_id), node:id(get_node(LightNode))]),
	MyNode = serialize(dht_node_state:details(DhtState)),
	TargetLoad = get_target_load(JumpOrSlide, MyNode, LightNode),
    From = dht_node_state:get(DhtState, pred_id),
	To = dht_node_state:get(DhtState, node_id),
	{SplitKey, _TakenLoad} = dht_node_state:get_split_key(DhtState, From, To, TargetLoad, forward),
	?TRACE("TargetLoad: ~p TakenLoad: ~p~n",
            [TargetLoad, _TakenLoad]),
	LbModule = comm:make_global(pid_groups:get_my(?MODULE)),
	case JumpOrSlide of
		jump ->
			Node = get_node(LightNode),
			comm:send(node:pidX(Node), {move, start_jump, SplitKey, {tag, JumpOrSlide}, LbModule});
		slide ->
			Node = get_node(LightNode),
			comm:send(node:pidX(Node),{move, start_slide, succ, SplitKey, {tag, JumpOrSlide}, LbModule})
	end,
	DhtState.

%%%%%%%%%%%%%%%%%%%%
%% Helper methods  %
%%%%%%%%%%%%%%%%%%%%

%% @doc Balance if the two nodes are adjacent, otherwise ask the light node's neighbor
-spec balance_adjacent(lb_info(), lb_info()) -> ok.
balance_adjacent(HeavyNodeDetails, LightNodeDetails) ->
	HeavyNode = get_node(HeavyNodeDetails),
	LightNodeSucc = get_succ(LightNodeDetails),
	case HeavyNode =:= LightNodeSucc of
		 %orelse node_details:get(HeavyNode, node) =:= node_details:get(LightNode, pred) of
		true ->
			% neighbors, thus sliding
			?TRACE("We're neighbors~n"),
            %% slide in phase1 or phase2
			comm:send(node:pidX(HeavyNode), {balance_load, slide, LightNodeDetails});
		_ ->
			% ask the successor of the light node how much load he carries
			?TRACE("Nodes not adjacent, requesting information about neighbors~n"),
			comm:send(node:pidX(LightNodeSucc), {balance_load, phase2, HeavyNodeDetails, LightNodeDetails})
	end.

%% Convert node details to lb_info
-spec serialize(node_details:node_details()) -> lb_info().
serialize(NodeDetails) ->
    #lb_info{node = node_details:get(NodeDetails, node),
             load = node_details:get(NodeDetails, load),
             succ = node_details:get(NodeDetails, succ)}.

-spec get_load(lb_info()) -> load() | node:node_type().
get_load(#lb_info{load = Load}) -> Load.
get_node(#lb_info{node = Node}) -> Node.
get_succ(#lb_info{succ = Succ}) -> Succ.

-spec get_target_load(slide | jump, lb_info(), lb_info()) -> non_neg_integer().
get_target_load(slide, HeavyNode, LightNode) ->
	(get_load(HeavyNode) + get_load(LightNode)) div 2;
get_target_load(jump, HeavyNode, _LightNode) ->
	 get_load(HeavyNode) div 2.

-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(lb_active_interval) div 1000.

-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(lb_active_interval) andalso
    config:cfg_is_greater_than(lb_active_interval, 0) andalso
    config:cfg_is_float(lb_active_karger_epsilon) andalso
    config:cfg_is_greater_than(lb_active_karger_epsilon, 0.0),
    config:cfg_is_less_than(lb_active_karger_epsilon, 0.25).
