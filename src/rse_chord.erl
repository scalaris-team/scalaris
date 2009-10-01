%%%------------------------------------------------------------------------------
%%% File    : rse_chord.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : implements a ring size estimator (RSE) for Chord-like overlays
%%%
%%% Created : 18 Dec 2008 by Thorsten Schuett <schuett@zib.de>
%%%------------------------------------------------------------------------------
%% @doc implements a ring size estimator (RSE) for Chord-like overlays
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
%% @reference Tallat Shafaat, Ali Ghodsi, Seif Haridi. <em>A Practical
%% Approach to Network Size Estimation for Structured
%% Overlays</em>. IWSOS 2008.
-module(rse_chord).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-export([start_link/1]).
-export([init/1, on/2]).

% state of an rse_chord process
-type(state() :: {float()}).

% accepted messages of rse_chord processes
-type(message() :: {init_shuffle} 
      | {get_pred_succ_response, Pred::node:node_type(), Succ::node:node_type()}
      | {shuffle_trigger}
      | {cache, Cache::[node:node_type()]}
      | {get_rse_chord_response, RemoteEstimate::float()}
      | {get_rse_chord, Pid::cs_send:mypid()}).

shuffle_interval() ->
    25000.

shuffle_reset_interval() ->
    180000.

%% @doc start_link for supervisor
-spec(start_link/1 :: (any()) -> {ok, pid()}).
start_link(InstanceId) ->
    gen_component:start_link(?MODULE, [InstanceId], [{register, InstanceId, rse_chord}]).

%% @doc initializes component
-spec(init/1 :: ([any()]) -> state()).
init([_]) ->
    %process_dictionary:register_process(InstanceId, rse_chord, self()),
    erlang:send_after(shuffle_interval(), self(), {shuffle_trigger}),
    erlang:send_after(shuffle_reset_interval(), self(), {init_shuffle}),
    self() ! {init_shuffle},
    {0.0}.
    
%================================================================================
% ring size estimator
%================================================================================
%% @doc message handler
-spec(on/2 :: (message(), state()) -> state()).

%% trigger new estimation round
on({init_shuffle}, State) ->
    %io:format("last guess ~p~n", [State]),
    erlang:send_after(shuffle_reset_interval(), self(), {init_shuffle}),
    get_node_pid() ! {get_pred_succ, cs_send:this()},
    State;

%% new estimation round: initialize with pred and succ info
on({get_pred_succ_response, Pred, Succ}, _State) ->
    % init with distances to pred and succ
    Id = cs_keyholder:get_key(),
    Estimate = get_initial_estimate(node:id(Pred), Id, node:id(Succ)),
    {Estimate};

%% trigger new shuffle with a random node
on({shuffle_trigger}, State) ->
    erlang:send_after(shuffle_interval(), self(), {shuffle_trigger}),
    get_cyclon_pid() ! {get_subset, 1, self()},
    State;

%% shuffle with a random node (got random node from cyclon)
on({cache, Cache}, State) ->
    case Cache of
	[Node] ->
	    cs_send:send_to_group_member(node:pidX(Node), rse_chord, 
					 {get_rse_chord, cs_send:this()});
	_ ->
	    ok
    end,
    State;

%% got shuffle data from random node
on({get_rse_chord_response, RemoteEstimation}, {LocalEstimation} = _State) ->
    % average of the two estimates
    {(RemoteEstimation + LocalEstimation) / 2.0};

%% get estimate
on({get_rse_chord, Pid}, {LocalEstimation} = State) ->
    cs_send:send(Pid, {get_rse_chord_response, LocalEstimation}),
    State;

%% unknown message
on(_UnknownMessage, _State) ->
    unknown_event.
    

%================================================================================
% helper routines
%================================================================================

% @private
get_node_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cs_node).

get_cyclon_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cyclon).

%% @doc Calculate initial estimate for ring size based on distance to 
%%      predecessor and successor
-spec(get_initial_estimate/3 :: (rt_simple:key(), rt_simple:key(), rt_simple:key()) -> float()).
get_initial_estimate(Pred, Id, Succ) ->
    if
	Pred == Id andalso Id == Succ ->
	    % I am the only node
	    1.0;
	true ->
	    AvgDist = (normalize(Id - Pred) + normalize(Succ - Id)) / 2,
	    16#100000000000000000000000000000000 / AvgDist
    end.

%% @doc normalize Chord identifier
-spec(normalize/1 :: (rt_simple:key()) -> rt_simple:key()).
normalize(Key) ->
    if
	Key < 0 ->
	    normalize(Key + 16#100000000000000000000000000000000);
	Key > 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF ->
	    normalize(Key - 16#100000000000000000000000000000000);
	true ->
	    Key
    end.

% -*-  indent-tabs-mode:nil;  -*-
