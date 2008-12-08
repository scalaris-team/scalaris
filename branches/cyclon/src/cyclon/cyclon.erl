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
    process_dictionary:register_process(InstanceId, cyclon, cs_send:this()),

	get_pid() ! {get_pred, cs_send:this()},
    get_pid() ! {get_succ_list, cs_send:this()},
    	erlang:send_after(config:read(cyclon_interval), {shuffle}),
    loop(cache:new(),nil).

loop(Cache,Node) ->
    receive
	{get_pred_response, Pred, Me} ->
		{_,NewNode,_,_} = Me,
		NewCache =  cache:add_list([Pred], Cache),
		loop(NewCache,NewNode);
	{get_succ_list_response, Me, Succ_list} ->
		{_,NewNode,_,_} = Me,
		NewCache =  cache:add_list(Succ_list, Cache),
		loop(cache:update(NewCache),NewNode);
		
	{'$gen_cast', {debug_info, Requestor}} when Node/=nil ->
		% io:format("~p~n", [lists:flatten(io_lib:format("~p", [State]))]),
	    Requestor ! {debug_info_response, [
					       	{"cs_node", lists:flatten(io_lib:format("~p", [get_pid()]))},
						   	{"cache-items", lists:flatten(io_lib:format("~p", [cache:size(Cache)]))},
							{"cache", lists:flatten(io_lib:format("~p", [Cache])) }
					      ]},
	    loop(Cache,Node);
	{shuffle} when Node/=nil	->
		case cache:size(Cache) of
			0 -> 
				NewCache=Cache;
			_  ->
				NewCache = shuffle(Cache,Node)
		end,
		timer:send_after(config:read(cyclon_interval), {shuffle}),
		loop(NewCache,Node);
	X ->
		io:format("%% Unhandle Message: ~p~n",X),
	    loop(Cache,Node)
    end.

shuffle(Cache, Node) ->
io:format("<#>"),
Cl= config:read(cyclon_shuffle_length),
Size =  cache:size(Cache),
if
	Cl < Size ->
		L=Cl;
	true ->
		L=cache:size(Cache)
end,
Subset=cache:get_random_subset(L,Cache),
Q=cache:get_random_element(Subset),
io:format("Subset: ~p~n",[Subset]),
NSubset=cache:delete(Q,Subset),
io:format("NSubset: ~p~n",[NSubset]),
ForSend=cache:add_element({{cs_send:this(),Node},0},NSubset),
io:format("ForSent: ~p~n",[ForSend]),
Cache.


	
get_pid() ->
    InstanceId = erlang:get(instance_id),
    if
	InstanceId == undefined ->
	    io:format("~p~n", [util:get_stacktrace()]);
	true ->
	    ok
    end,
    process_dictionary:lookup_process(InstanceId, cs_node).
