%  @copyright 2013-2015 Zuse Institute Berlin

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
%% @end
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

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), io:format("lb_active_karger: " ++ X,Y)).

-behaviour(lb_active_beh).

-export([init/0, check_config/0]).
-export([handle_msg/2, handle_dht_msg/2]).
-export([get_web_debug_kv/1]).

-record(state, {epsilon          = ?required(state, epsilon) :: float(),
                rnd_node         = []                        :: [node:node_type()],
                best_candidate   = []                        :: [{items | requests, {LoadChange::non_neg_integer(), node:node_type()}}],
                round_id         = nil                       :: non_neg_integer() | nil,
                my_lb_info       = nil                       :: lb_info:lb_info() | nil,
                req_ids          = []                        :: [{integer(), node:node_type()}]
               }).

-type state() :: #state{}.

-type(my_message() ::
           %% trigger messages
           {lb_active_karger_trigger} |
           %% random node from cyclon
           {cy_cache, [node:node_type()]} |
           %% load response from dht node
           {my_dht_response, DhtNode :: comm:mypid(), {get_state_response, Load :: number()}} |
           %% Result from slide or jump
           dht_node_move:result_message() |
           %% simulation
           {simulation_result, Id::integer(), ReqId::integer(), {items | requests, LoadChange::non_neg_integer()}} |
           {pick_best_candidate, Id::integer()}).

-type options() :: [{epsilon, float()} | {id, integer()} | {simulate} | {reply_to, comm:mypid()}].

-type dht_message() ::
		   %% phase1
		   {lb_active, phase1, NodeX :: lb_info:lb_info(), options()} |
		   %% phase2
		   {lb_active, phase2, HeavyNode :: lb_info:lb_info(), LightNode :: lb_info:lb_info()}.


%%%%%%%%%%%%%%%
%%  Startup   %
%%%%%%%%%%%%%%%

%% @doc Initialization of module called by lb_active
-spec init() -> state().
init() ->
    trigger(),
    Epsilon = config:read(lb_active_karger_epsilon),
    #state{epsilon = Epsilon}.

%%%%%%%%%%%%%%%
%%  Trigger   %
%%%%%%%%%%%%%%%

-spec handle_msg(my_message(), state()) -> state().
handle_msg({lb_active_karger_trigger}, State) ->
    trigger(),
    %% Request N random nodes from cyclon
    NumNodes = config:read(lb_active_karger_rnd_nodes),
    gossip_cyclon:get_subset_rand(NumNodes),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  Handling of lb process related messages  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% No random nodes available via cyclon
handle_msg({cy_cache, []}, State) ->
    ?TRACE("Cyclon returned no random node~n", []),
    State;

%% Got a random node via cyclon
handle_msg({cy_cache, RandomNodes}, State) ->
    ?TRACE("Got random node~n", []),
    case config:read(lb_active_karger_epsilon) of
        self_tuning -> request_my_gossip_values();
        _ -> request_my_node_details()
    end,
    State#state{rnd_node = RandomNodes};

%% Process gossip values from gossip process to determine epsilon
%% in case lb_active_karger_epsilon is set to self_tuning.
handle_msg({gossip_get_values_best_response, LoadInfo}, State) ->
    Module = lb_active_gossip_load_metric,
    Avg = gossip_load:load_info_other_get(avgLoad, Module, LoadInfo),
    Stddev = gossip_load:load_info_other_get(stddev, Module, LoadInfo),
    Max = gossip_load:load_info_other_get(maxLoad, Module, LoadInfo),
    Epsilon =
        try
            lb_info:bound(0.01, Avg / erlang:max(Avg + Stddev, Max - Stddev), 0.24)
        catch
            error:badarith -> 0.24
        end,
    request_my_node_details(),
    State#state{epsilon = Epsilon};

%% Got load from my node
handle_msg({my_dht_response, {get_node_details_response, NodeDetails}}, State) ->
	?TRACE("Received node details for own node~n", []),
	RandomNodes = State#state.rnd_node,
	Epsilon = State#state.epsilon,
    ?TRACE("Epsilon: ~p~n", [Epsilon]),
    %% If we deal only with one random node, we don't have
    %% any choice but to go to the next phase.
    %% Otherwise, we ask all random nodes for their load
    %% and calculate the load changes before going to the
    %% next phase.
    MyLBInfo = lb_info:new(NodeDetails),
    IsValid = lb_info:is_valid(MyLBInfo),
    Id = randoms:getRandomInt(), %%uid:get_global_uid(),
    Options = [{id, Id}, {epsilon, Epsilon}],
    case RandomNodes of
        [RndNode] when IsValid ->
            comm:send(node:pidX(RndNode), {lb_active, phase1, MyLBInfo, Options}, [{?quiet}]),
            State#state{rnd_node = []};
        RndNodes when IsValid ->
            This = comm:this(),
            ReqIds =
                [begin
                      ReqId = randoms:getRandomInt(),
                      ?TRACE("Sending out simulate request with ReqId ~p to ~.0p~n", [ReqId, node:pidX(RndNode)]),
                      OptionsNew = [{simulate, ReqId}, {reply_to, This} | Options],
                      comm:send(node:pidX(RndNode), {lb_active, phase1, MyLBInfo, OptionsNew}, [{?quiet}]),
                      {ReqId, RndNode}
                 end || RndNode <- RndNodes],
            Timeout = config:read(lb_active_karger_simulation_timeout) div 1000,
            msg_delay:send_local(Timeout, self(), {pick_best_candidate, Id}),
            State#state{round_id = Id, my_lb_info = MyLBInfo, req_ids = ReqIds};
        _ ->
            State
    end;

%% collect all the load change responses and save the best candidate
handle_msg({simulation_result, Id, ThisReqId, {Metric, LoadChange}}, State) ->
    ?TRACE("Received load change ~p in round ~p~n", [LoadChange, Id]),
    case State#state.round_id of
        Id ->
            ReqIds = State#state.req_ids,
            ReqIdsNew = lists:keydelete(ThisReqId, 1, ReqIds),
            case ReqIdsNew of
                [] -> comm:send_local(self(), {pick_best_candidate, Id});
                _  -> ok
            end,
            {ThisReqId, NodeX} = lists:keyfind(ThisReqId, 1, ReqIds),

            Best = State#state.best_candidate,
            {BestLoadChange, _Node} = proplists:get_value(Metric, Best, {0, nil}),

            case LoadChange < BestLoadChange of
                true  ->
                    NewBest = lists:keystore(Metric, 1, Best, {Metric, {LoadChange, NodeX}}),
                    State#state{req_ids = ReqIdsNew,
                                best_candidate = NewBest};
                _ ->
                    State#state{req_ids = ReqIdsNew}
            end;
        _ ->
           ?TRACE("Discarding old round with Id ~p~n", [Id]),
           State
    end;

%% In case we have a best candidate, start the actual
%% load balancing algorithm.
handle_msg({pick_best_candidate, Id}, State) ->
    ?TRACE("Deciding in round ~p~n",[Id]),
    case State#state.round_id of
        Id ->
            Best = State#state.best_candidate,
            BestCandidate =
                case lists:keyfind(requests, 1, Best) of
                    {requests, {_LoadChange, Node}} -> Node;
                    _ ->
                        case lists:keyfind(items, 1, Best) of
                            {items, {_LoadChange, Node}} -> Node;
                            _ -> nil
                        end
                end,
            case BestCandidate of
                nil -> ?TRACE("No best candidate in Round ~p~n", [Id]);
                BestCandidate ->
                    BestPid = node:pidX(BestCandidate),
                    Epsilon = State#state.epsilon,
                    MyLBInfo = State#state.my_lb_info,
                    ?TRACE("Sending out decision in round ~p: BestCandidate: ~w~n", [Id, BestCandidate]),
                    Options = [{id, Id}, {epsilon, Epsilon}],
                    comm:send(BestPid, {lb_active, phase1, MyLBInfo, Options})
            end,
            State#state{best_candidate = [], round_id = nil};
        _ ->
            ?TRACE("Old decision message for round ~p~n", [Id]),
            State
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  Static methods called by dht_node message handler  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec handle_dht_msg(dht_message(), dht_node_state:state()) -> dht_node_state:state().
%% First phase: We were contacted by another node who chose
%% us as a random node. In this phase we'll determine if
%% load balancing is necessary. If so, we'll try to balance
%% assuming the two nodes are neighbors. If not we'll contact
%% the light node's successor for more load information.
handle_dht_msg({lb_active, phase1, NodeX, Options}, DhtState) ->
    {epsilon, Epsilon} = lists:keyfind(epsilon, 1, Options),
	MyLBInfo = lb_info:new(dht_node_state:details(DhtState)),
    IsValid = lb_info:is_valid(MyLBInfo),
	MyLoad = lb_info:get_load(MyLBInfo),
	LoadX = lb_info:get_load(NodeX),
    if IsValid andalso (MyLoad =/= 0 orelse LoadX =/= 0) ->
           if
               % first check if load balancing is necessary
               MyLoad =< Epsilon * LoadX ->
                   ?TRACE("My node is light~n", []),
                   balance_adjacent(NodeX, MyLBInfo, Options);
               LoadX =< Epsilon * MyLoad ->
                   ?TRACE("My node is heavy~n", []),
                   balance_adjacent(MyLBInfo, NodeX, Options);
               true ->
                   %% no balancing
                   ?TRACE("Won't balance~n", []),
                   lb_active:balance_noop(Options)
           end;
       true -> lb_active:balance_noop(Options)
    end,
	DhtState;

%% Second phase: We are LightNode's successor. We might hold
%% more load than the HeavyNode. If so, we'll slide with the
%% LightNode. Otherwise we instruct the HeavyNode to set up
%% a jump operation with the Lightnode.
handle_dht_msg({lb_active, phase2, HeavyNode, LightNode, Options}, DhtState) ->
	?TRACE("In phase 2~n", []),
	MyLBInfo = lb_info:new(dht_node_state:details(DhtState)),
    IsValid = lb_info:is_valid(MyLBInfo),
	MyLoad = lb_info:get_load(MyLBInfo),
    LoadHeavyNode = lb_info:get_load(HeavyNode),
    if IsValid andalso MyLoad > LoadHeavyNode ->
           % slide
           lb_active:balance_nodes(HeavyNode, LightNode, Options);
       IsValid ->
           % jump
           lb_active:balance_nodes(HeavyNode, LightNode, MyLBInfo, Options);
       true -> ok
    end,
	DhtState.

%%%%%%%%%%%%%%%%%%%%
%% Helper methods  %
%%%%%%%%%%%%%%%%%%%%

%% @doc Balance if the two nodes are adjacent, otherwise ask the light node's neighbor
-spec balance_adjacent(lb_info:lb_info(), lb_info:lb_info(), options()) -> ok.
balance_adjacent(HeavyNode, LightNode, Options) ->
	case lb_info:neighbors(HeavyNode, LightNode) of %%lb_info:is_succ(HeavyNode, LightNode) of
		true ->
			% neighbors, thus sliding
			?TRACE("We're neighbors~n", []),
            %% slide in phase1 or phase2
            lb_active:balance_nodes(HeavyNode, LightNode, Options);
		_ ->
			% ask the successor of the light node how much load he carries
			?TRACE("Nodes not adjacent, requesting information about neighbors~n", []),
            LightNodeSucc = lb_info:get_succ(LightNode),
			comm:send(node:pidX(LightNodeSucc), {lb_active, phase2, HeavyNode, LightNode, Options})
	end.

-spec request_my_node_details() -> ok.
request_my_node_details() ->
    MyDhtNode = pid_groups:get_my(dht_node),
    Envelope = comm:reply_as(comm:this(), 2, {my_dht_response, '_'}),
    comm:send_local(MyDhtNode, {get_node_details, Envelope}).

-spec request_my_gossip_values() -> ok.
request_my_gossip_values() ->
    gossip_load:get_values_best([]).

-spec trigger() -> ok.
trigger() ->
    Interval = config:read(lb_active_karger_interval) div 1000,
    msg_delay:send_trigger(Interval, {lb_active_karger_trigger}).

%% @doc Key/Value List for web debug
-spec get_web_debug_kv(state()) -> [{string(), string()}].
get_web_debug_kv(State) ->
    [{"state", webhelpers:html_pre("~p", [State])}].

-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(lb_active_karger_interval) and
    config:cfg_is_greater_than(lb_active_karger_interval, 0) and

    (config:read(lb_active_karger_epsilon) =/= self_tuning orelse
     lb_active:check_gossip_modules(lb_active_gossip_load_metric, lb_active_karger_epsilon)
    ) and

    (config:read(lb_active_karger_epsilon) =:= self_tuning orelse
     config:cfg_is_float(lb_active_karger_epsilon) and
     config:cfg_is_greater_than(lb_active_karger_epsilon, 0.0) and
     config:cfg_is_less_than(lb_active_karger_epsilon, 0.25)
    ) and

    config:cfg_is_integer(lb_active_karger_rnd_nodes) and
    config:cfg_is_greater_than_equal(lb_active_karger_rnd_nodes, 1) and

    config:cfg_is_integer(lb_active_karger_simulation_timeout) and
    config:cfg_is_greater_than(lb_active_karger_simulation_timeout, 0).
