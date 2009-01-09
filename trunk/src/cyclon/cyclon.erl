%%%-------------------------------------------------------------------
%%% File    : cyclon.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : cyclon main file
%%%
%%% Created :  1 Dec 2008 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
%% @reference S. Voulgaris, D. Gavidia, M. van Steen. CYCLON: 
%% Inexpensive Membership Management for Unstructured P2P Overlays. 
%% Journal of Network and Systems Management, Vol. 13, No. 2, June 2005.

-module(cyclon.cyclon).
-author('hennig@zib.de').
-vsn('$Id $ ').

-import(boot_server).
-import(config).
-import(cs_send).
-import(io).
-import(io_lib).
-import(lists).
-import(process_dictionary).
-import(erlang).
-import(node).
%% API
-export([start_link/1, start/1]).

-export([simple_shuffle/2, enhanced_shuffle/2]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(InstanceId) ->
	case config:read(cyclon_enable) of
		true -> 
			 {ok, spawn_link(?MODULE, start, [InstanceId])};
		_ ->
			ignore
	end.
   
start(InstanceId) ->
    boot_server:connect(),
    process_dictionary:register_process(InstanceId, cyclon, self()),
    %process_dictionary:register_process(InstanceId, cyclonNode, cs_send:this()),
    %{observer,'me@csr-pc38.zib.de' } ! {cyclon, self()},
    get_pid() ! {get_pred_succ, cs_send:this()},
    erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
    io:format("Cyclon spawn: ~p~n", [cs_send:this()]),
    loop(cache:new(),cs_send:get(get_pid(), cs_send:this()),0).


%% Cache
%% Node: the scalaris node of this cyclon-task
%% State: {preshuffle|shuffle}
%% Cycles: the amount of shuffle-cycles

loop(Cache,Node,Cycles) ->
    receive
	{getcache,Pid} ->
		Pid ! {cache,cs_send:this(),Cycles,cache:get_list_of_cyclons(Cache)},
		loop(Cache,Node,Cycles);
	{flush_cache} ->
		get_pid() ! {get_pred_succ, cs_send:this()},
		loop(cache:new(),Node,0);	
	{start_shuffling} ->
		erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
		loop(Cache,Node,Cycles);
			
	{get_pred_succ_response, Pred, Succ} ->
	    %Me = cs_send:get(get_pid(), cs_send:this()),
	   	%io:format("Pred ~p~n Succ~p~n ~n", [node:pidX(Pred),pidX(Succ)]),
		case node:pidX(Pred) /= Node of
 	 		true ->
				NewCache =  cache:add_list([node:pidX(Pred),node:pidX(Succ)], Cache),
				loop(cache:update(NewCache),Node,Cycles);
			false ->
				loop(Cache,Node,Cycles)
		end;
		
	{'$gen_cast', {debug_info, Requestor}}  ->
	    %io:format("gen_cast~n", []),
		% io:format("~p~n", [lists:flatten(io_lib:format("~p", [State]))]),
	    Requestor ! {debug_info_response, [
					       	{"cs_node", lists:flatten(io_lib:format("~p", [get_pid()]))},
						   	{"cache-items", lists:flatten(io_lib:format("~p", [cache:size(Cache)]))},
							{"cache", lists:flatten(io_lib:format("~p", [Cache])) }
					      ]},
	    loop(Cache,Node,Cycles);
	{shuffle} 	->
	    %io:format("shuffle~n", []),
		case Node of	
		    nil ->
			loop(Cache,Node,Cycles);
		    _	->
			NewCache = case cache:size(Cache) of
				       0 -> 
					   Cache;	
				       _  ->
					   enhanced_shuffle(Cache,Node)
				   end,
			erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
			loop(NewCache,Node,Cycles+1)
		end;
	{subset,P,Subset} ->
		%io:format("subset~n", []),
		ForSend=cache:get_random_subset(get_L(Cache),Cache),
		%io:format("<",[]),
		cs_send:send(P,{subset_response,ForSend,Subset}),
		N2=cache:minus(Subset,Cache),
		NewCache = cache:merge(Cache,N2,ForSend),
		loop(NewCache,Node,Cycles);
	{subset_response,Subset,OldSubset} ->
		%io:format("subset_response~n", []),
		N1=cache:delete({{nil,Node},nil},Subset),
		N2=cache:minus(N1,Cache),
		NewCache=cache:merge(Cache,N2,OldSubset),
		loop(NewCache,Node,Cycles);
	{random_element,Pid} ->
		Pid ! {random_element_response,cache:get_random_element(Cache)},
		loop(Cache,Node,Cycles);
	X ->
		io:format("%% Unhandle Message: ~p~n", [X]),
	    loop(Cache,Node,Cycles)
    end.

%% @doc enhanced shuffle with age
enhanced_shuffle(Cache, Node) ->
    Cache_1= cache:inc_age(Cache),
    Q=cache:get_oldest(Cache_1),
    Subset=cache:get_random_subset(get_L(Cache_1),Cache_1),
    {{QCyclon,_},_} = Q,
    case (QCyclon /= cs_send:this()) of
	true ->
	    NSubset_pre=cache:delete(Q,Subset),
	    NSubset = case cache:size(NSubset_pre) == config:read(cyclon_shuffle_length) of
			  true ->
			      cache:delete(cache:get_random_element(NSubset_pre),NSubset_pre);
			  false ->
			      NSubset_pre
		      end,
	    ForSend=cache:add_element({{cs_send:this(),Node},0},NSubset),
	    %io:format(">",[]),
	    cs_send:send(QCyclon,{subset,cs_send:this(),ForSend}),
	    cache:delete(Q,Cache);	
	false -> 
	    error
    end.

%% @doc simple shuffle without age
simple_shuffle(Cache, Node) ->
    Subset=cache:get_random_subset(get_L(Cache),Cache),
    Q=cache:get_random_element(Subset),
    {{QCyclon,_},_} = Q,
    case (QCyclon /= cs_send:this()) of
	true ->
	    NSubset=cache:delete(Q,Subset),
	    ForSend=cache:add_element({{cs_send:this(),Node},0},NSubset),
	    %io:format("~p",[length(ForSend)]),
	    cs_send:send(QCyclon,{subset,cs_send:this(),ForSend}),
	    cache:delete(Q,Cache);	
	false -> 
	    Cache
    end.


get_L(Cache) ->
	Cl= config:read(cyclon_shuffle_length),
	Size =  cache:size(Cache),
	if
		Cl < Size ->
			L=Cl;
		true ->
			L=cache:size(Cache)
	end,
	L.
	
get_pid() ->
    InstanceId = erlang:get(instance_id),
    if
	InstanceId == undefined ->
	    io:format("~p~n", [util:get_stacktrace()]);
	true ->
	    ok
    end,
    process_dictionary:lookup_process(InstanceId, cs_node).

