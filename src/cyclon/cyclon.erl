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

-module(cyclon).
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
-import(log).
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
    process_dictionary:register_process(InstanceId, cyclon, self()),
   	get_pid() !	{get_node, cs_send:this(),2.71828183},
    get_pid() ! {get_pred_succ, cs_send:this()},
    receive
    	{get_node_response, 2.71828183, Me} ->
          	Node = Me
		end,
    receive
    {get_pred_succ_response, Pred, Succ} ->
	    %Me = cs_send:get(get_pid(), cs_send:this()),
	   	%io:format("Pred ~p~n Succ~p~n ~n", [node:pidX(Pred),pidX(Succ)]),
		case Pred /= Node of
 	 		true ->
				Cache =  cache:add_list([Pred,Succ], cache:new());
			false ->
				Cache =  cache:new()
		end
    end,
    erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
    log:log(info,"[ CY ] Cyclon spawn: ~p~n", [cs_send:this()]),
    loop(Cache,Node,0).


%% Cache
%% Node: the scalaris node of this cyclon-task
%% State: {preshuffle|shuffle}
%% Cycles: the amount of shuffle-cycles

loop(Cache,Node,Cycles) ->
    receive
        {get_ages,Pid} ->
            Pid ! {ages,cache:ages(Cache)},
            loop(Cache,Node,Cycles);
        
        {get_subset,all,Pid} ->
            Pid ! {cache,cache:get_youngest(config:read(cyclon_cache_size),Cache)},
            loop(Cache,Node,Cycles);
        
        {get_subset_max_age,Age,Pid} ->
            Pid ! {cache,cache:get_subset_max_age(Age,Cache)},
            loop(Cache,Node,Cycles);    
        
        {get_subset,N,Pid} ->
            Pid ! {cache,cache:get_youngest(N,Cache)},
            loop(Cache,Node,Cycles);
        
        {get_cache,Pid} ->
            Pid ! {cache,cache:get_list_of_nodes(Cache)},
            loop(Cache,Node,Cycles);
        
        {flush_cache} ->
            get_pid() ! {get_pred_succ, cs_send:this()},
            loop(cache:new(),Node,0);	

        {start_shuffling} ->
            erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
            loop(Cache,Node,Cycles);
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
	 	case Node of	
			nil ->
				loop(Cache,Node,Cycles);
		  _	->
				NewCache = 
        	case cache:size(Cache) of
						0 ->
                        %log:log(warn,"[ CY | ~p] Cache is empty",[self()]),
					  	Cache;	
				    _  ->
					   enhanced_shuffle(Cache,Node)
					end,
				erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
				loop(NewCache,Node,Cycles+1)
		end;
	{cy_subset,P,Subset} ->
                                                %io:format("subset~n", []),
            ForSend=cache:get_random_subset(get_L(Cache),Cache),
                                                %io:format("<",[]),
            cs_send:send_to_group_member(node:pidX(P),cyclon,{cy_subset_response,ForSend,Subset}),
            N2=cache:minus(Subset,Cache),
            NewCache = cache:merge(Cache,N2,ForSend),
            loop(NewCache,Node,Cycles);
	{cy_subset_response,Subset,OldSubset} ->
                                                %io:format("subset_response~n", []),
            N1=cache:delete({Node,nil},Subset),
            N2=cache:minus(N1,Cache),
            NewCache=cache:merge(Cache,N2,OldSubset),
            loop(NewCache,Node,Cycles);
	X ->
            log:log(warn,"[ CY | ~p ] Unhandle Message: ~p", [self(),X]),
	    loop(Cache,Node,Cycles)
    end.

%% @doc enhanced shuffle with age
enhanced_shuffle(Cache, Node) ->
    Cache_1= cache:inc_age(Cache),
    Q=cache:get_oldest(Cache_1),
    Subset=cache:get_random_subset(get_L(Cache_1),Cache_1),
    {QCyclon,_} = Q,
    case (QCyclon /= Node) of
	true ->
	    NSubset_pre=cache:delete(Q,Subset),
	    NSubset = case cache:size(NSubset_pre) == config:read(cyclon_shuffle_length) of
			  true ->
			      cache:delete(cache:get_random_element(NSubset_pre),NSubset_pre);
			  false ->
			      NSubset_pre
		      end,
	    ForSend=cache:add_element({Node,0},NSubset),
                                                %io:format(">",[]),
	    cs_send:send_to_group_member(node:pidX(QCyclon),cyclon,{cy_subset,Node,ForSend}),
	    cache:delete(Q,Cache_1);	
	false -> 
	    error
    end.

%% @doc simple shuffle without age
simple_shuffle(Cache, Node) ->
    Subset=cache:get_random_subset(get_L(Cache),Cache),
    Q=cache:get_random_element(Subset),
    {{QCyclon},_} = Q,
    case (QCyclon /= Node) of
	true ->
	    NSubset=cache:delete(Q,Subset),
	    ForSend=cache:add_element({Node,0},NSubset),
            %% io:format("~p",[length(ForSend)]),
	    cs_send:send_to_group_member(node:pidX(QCyclon),cyclon,{cy_subset,Node,ForSend}),
	    cache:delete(Q,Cache);	
	false ->
	    Cache
    end.
	


get_L(Cache) ->
    Cl= config:read(cyclon_shuffle_length),
    Size = cache:size(Cache),
    if
        Cl < Size ->
            L = Cl;
        true ->
            L = cache:size(Cache)
    end,
    L.

get_pid() ->
    InstanceId = erlang:get(instance_id),
    if
        InstanceId == undefined ->
            log:log(error,"[ CY | ~w ] ~p", [self(),util:get_stacktrace()]);
        true ->
            ok
    end,
    process_dictionary:lookup_process(InstanceId, cs_node).

