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

-export([]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(InstanceId) ->
    {ok, spawn_link(?MODULE, start, [InstanceId])}.

start(InstanceId) ->
    boot_server:connect(),
    process_dictionary:register_process(InstanceId, cyclon, self()),

	get_pid() ! {get_pred_succ, cs_send:this()},
   	erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
    %io:format("after send after~n", []),
    loop(cache:new(),cs_send:get(get_pid(), cs_send:this())).

pidX({_,A,_,_}) ->
	A.


loop(Cache,Node) ->
    receive
	{get_pred_succ_response, Pred, Succ} ->
	    %Me = cs_send:get(get_pid(), cs_send:this()),
	   	io:format("Pred ~p~n Succ~p~n ~n", [node:pidX(Pred),pidX(Succ)]),
		
 	 	NewCache =  cache:add_list([pidX(Pred),pidX(Succ)], Cache),
		loop(cache:update(NewCache),Node);
		
	{'$gen_cast', {debug_info, Requestor}}  ->
	    %io:format("gen_cast~n", []),
		% io:format("~p~n", [lists:flatten(io_lib:format("~p", [State]))]),
	    Requestor ! {debug_info_response, [
					       	{"cs_node", lists:flatten(io_lib:format("~p", [get_pid()]))},
						   	{"cache-items", lists:flatten(io_lib:format("~p", [cache:size(Cache)]))},
							{"cache", lists:flatten(io_lib:format("~p", [Cache])) }
					      ]},
	    loop(Cache,Node);
	{shuffle} 	->
	    %io:format("shuffle~n", []),
		case Node of	
		nil ->
			erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
			loop(Cache,Node);
		_	->
			case cache:size(Cache) of
				0 -> 
					NewCache=Cache;
				_  ->
					NewCache=shuffle(Cache,Node)
			end,
			erlang:send_after(config:read(cyclon_interval), self(), {shuffle}),
			loop(NewCache,Node)
		end;
	{subset,P,Subset} ->
		ForSend=cache:get_random_subset(get_L(Cache),Cache),
		cs_send:send(P,{subset_response,ForSend}),
		N2=cache:minus(Subset,Cache),
		NewCache = cache:merge(Cache,N2,ForSend),
		loop(NewCache,Node);
	X ->
		io:format("%% Unhandle Message: ~p~n", [X]),
	    loop(Cache,Node)
    end.

shuffle(Cache, Node) ->

Subset=cache:get_random_subset(get_L(Cache),Cache),
Q=cache:get_random_element(Subset),
{{QCyclon,_},_} = Q,
io:format("QCyclon: ~p~n",[QCyclon]),
io:format("Q:       ~p~n",[Q]),
io:format("this:    ~p~n",[cs_send:this()]),
case (QCyclon /= cs_send:this()) of
true ->
	NSubset=cache:delete(Q,Subset),
	ForSend=cache:add_element({{cs_send:this(),Node},0},NSubset),
	io:format(">#>"),
	cs_send:send(QCyclon,{subset,cs_send:this(),ForSend}),
	receive 
	{subset_response,Othersubset} ->
		N1=cache:delete({{nil,Node},nil},Othersubset),
		N2=cache:minus(N1,Cache),
		cache:merge(Cache,N2,ForSend)	
	after 300 ->
		Cache
	end;
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
