%%%-------------------------------------------------------------------
%%% File    : cyclon.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : cyclon main file
%%%
%%% Created :  1 Dec 2008 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum f�r Informationstechnik Berlin
%% @version $Id $
%% @reference S. Voulgaris, D. Gavidia, M. van Steen. CYCLON:
%% Inexpensive Membership Management for Unstructured P2P Overlays.
%% Journal of Network and Systems Management, Vol. 13, No. 2, June 2005.

-module(cyclon,[Trigger]).
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
-import(gen_component).

-behaviour(gen_component).

%% API
-export([start_link/1,init/1,on/2,get_base_interval/0]).

%-export([simple_shuffle/2, enhanced_shuffle/2]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------

start_link(InstanceId) ->
    start_link(InstanceId, []).

start_link(InstanceId, _Options) ->
    case config:read(cyclon_enable) of
        true ->
             gen_component:start_link(?MODULE:new(Trigger), [InstanceId], [{register, InstanceId, cyclon}]);
        _ ->
            ignore
    end.

init(_Args) ->
    cs_send:send_local(get_pid() , {get_node, cs_send:this(),2.71828183}),
    cs_send:send_local(get_pid() , {get_pred_succ, cs_send:this()}),
    TriggerState = Trigger:init(?MODULE:new(Trigger)),
    TriggerState2 = Trigger:trigger_first(TriggerState,1),
    log:log(info,"[ CY ] Cyclon spawn: ~p~n", [cs_send:this()]),
    {[],null,0,TriggerState2}.

%% Cache
%% Node: the scalaris node of this cyclon-task
%% State: {preshuffle|shuffle}
%% Cycles: the amount of shuffle-cycles


% state of cyclon
%-type(state() :: {cyclon.cache:cache(),null|cs_node(),integer()}).
-type(state() :: {any(),any(),integer()}).

% accepted messages of cs_node processes
-type(message() :: any()).

%% @doc message handler
-spec(on/2 :: (message(), state()) -> state()).
on({get_ages,Pid},{Cache,Node,Cycles,TriggerState}) ->
    cs_send:send_local(Pid , {ages,cache:ages(Cache)}),
    {Cache,Node,Cycles,TriggerState};

on({get_node_response, 2.71828183, Me},{Cache,null,Cycles,TriggerState}) ->
    {Cache,Me,Cycles,TriggerState};
on({get_pred_succ_response, Pred, Succ},{_,Node,Cycles,TriggerState}) ->
    case Pred /= Node of
            true ->
                Cache =  cache:add_list([Pred,Succ], cache:new());
            false ->
                Cache =  cache:new()
    end,
    {Cache,Node,Cycles,TriggerState};

on({get_subset,all,Pid},{Cache,Node,Cycles,TriggerState}) ->
    cs_send:send_local(Pid , {cache,cache:get_youngest(config:read(cyclon_cache_size),Cache)}),
    {Cache,Node,Cycles,TriggerState};

on({get_subset_max_age,Age,Pid},{Cache,Node,Cycles,TriggerState}) ->
    cs_send:send_local(Pid , {cache,cache:get_subset_max_age(Age,Cache)}),
    {Cache,Node,Cycles,TriggerState};

on({get_subset,N,Pid},{Cache,Node,Cycles,TriggerState}) ->
    cs_send:send_local(Pid , {cache,cache:get_youngest(N,Cache)}),
    {Cache,Node,Cycles,TriggerState};

on({get_cache,Pid},{Cache,Node,Cycles,TriggerState}) ->
    cs_send:send_local(Pid , {cache,cache:get_list_of_nodes(Cache)}),
    {Cache,Node,Cycles,TriggerState};

on({flush_cache},{_Cache,Node,_Cycles,TriggerState}) ->
    cs_send:send_local(get_pid() , {get_pred_succ, cs_send:this()}),
    {cache:new(),Node,0,TriggerState};
on({start_shuffling},{Cache,Node,Cycles,TriggerState}) ->
    cs_send:send_after(config:read(cyclon_interval), self(), {shuffle}),
    {Cache,Node,Cycles,TriggerState};
on({'$gen_cast', {debug_info, Requestor}},{Cache,Node,Cycles,TriggerState})  ->
    cs_send:send_local(Requestor , {debug_info_response, [
                                                          {"cs_node", lists:flatten(io_lib:format("~p", [get_pid()]))},
                                                          {"cache-items", lists:flatten(io_lib:format("~p", [cache:size(Cache)]))},
                                                          {"cache", lists:flatten(io_lib:format("~p", [Cache])) }
                                                         ]}),
    {Cache,Node,Cycles,TriggerState};

on({trigger},{Cache,null,Cycles,TriggerState}) ->
    %io:format("[ CY ] Trigger on null~n"),
    TriggerState2 = Trigger:trigger_next(TriggerState,1),
     {Cache,null,Cycles,TriggerState2};
on({trigger},{Cache,Node,Cycles,TriggerState}) 	->
    NewCache =	case cache:size(Cache) of
                    0 ->
                        %log:log(warn,"[ CY | ~p] Cache is empty",[self()]),
                        Cache;	
                    _  ->
                        enhanced_shuffle(Cache,Node)
                end,
    TriggerState2 = Trigger:trigger_next(TriggerState,1),
    {NewCache,Node,Cycles+1,TriggerState2};

on({cy_subset,P,Subset},{Cache,Node,Cycles,TriggerState}) ->
    %io:format("subset~n", []),
    ForSend=cache:get_random_subset(get_L(Cache),Cache),
    %io:format("<",[]),
    cs_send:send_to_group_member(node:pidX(P),cyclon,{cy_subset_response,ForSend,Subset}),
    N2=cache:minus(Subset,Cache),
    NewCache = cache:merge(Cache,N2,ForSend),
    {NewCache,Node,Cycles,TriggerState};

on({cy_subset_response,Subset,OldSubset},{Cache,Node,Cycles,TriggerState}) ->
    %io:format("subset_response~n", []),
    N1=cache:delete({Node,nil},Subset),
    N2=cache:minus(N1,Cache),
    NewCache=cache:merge(Cache,N2,OldSubset),
    {NewCache,Node,Cycles,TriggerState};

on(_, _State) ->
    unknown_event.

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
            Cache
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
            %io:format("~p",[length(ForSend)]),
            cs_send:send_to_group_member(node:pidX(QCyclon),cyclon,{cy_subset,Node,ForSend}),
            cache:delete(Q,Cache);	
        false ->
            Cache
    end.

get_L(Cache) ->
    Cl = config:read(cyclon_shuffle_length),
    case Cl < cache:size(Cache) of
        true ->
            Cl;
        false ->
            cache:size(Cache)
    end.

% get Pid of assigned cs_node
get_pid() ->
    InstanceId = erlang:get(instance_id),
    if
        InstanceId == undefined ->
            log:log(error,"[ CY | ~w ] ~p", [self(),util:get_stacktrace()]);
        true ->
            ok
    end,
    process_dictionary:lookup_process(InstanceId, cs_node).

get_base_interval() ->
    config:read(cyclon_interval).
