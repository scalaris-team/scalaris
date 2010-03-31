%  Copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : rm_tman.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : T-Man ring maintenance
%%%
%%% Created :  12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(rm_tman, [Trigger]).

-author('hennig@zib.de').
-vsn('$Id$ ').

-include("../include/scalaris.hrl").

-export([init/1,on/2]).
-behavior(gen_component).
-behavior(ring_maintenance).
-behavior(self_man).
-export([start_link/1, 
         get_base_interval/0,get_min_interval/0,get_max_interval/0]).

% unit testing
-export([merge/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc spawns a chord-like ring maintenance process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    start_link(InstanceId, []).

start_link(InstanceId,Options) ->
    gen_component:start_link(THIS, [InstanceId, Options], [{register, InstanceId, ring_maintenance}]).

init(_Args) ->
    log:log(info,"[ RM ~p ] starting ring maintainer TMAN~n", [self()]),
    cs_send:send_local(get_pid_dnc() , {subscribe, self()}),
    cs_send:send_local(get_cs_pid(), {init_rm,self()}),
    uninit.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type(state() :: {Id             :: ?RT:key(),
                  Me             :: node:node_type(),
                  Preds          :: list(node:node_type()),
                  Succs          :: list(node:node_type()),
                  RandomViewSize :: pos_integer(),
                  Interval       :: pos_integer(),
                  TriggerState   :: any(),
                  Cache          :: list(node:node_type()), % random cyclon nodes
                  Churn          :: boolean()}
     | uninit).

% @doc the Token takes care, that there is only one timermessage for stabilize
-spec(on/2 :: (any(), state()) -> state()).
on({init, Id, Me, Predecessor, SuccList}, uninit) ->
    ring_maintenance:update_preds_and_succs([Predecessor], SuccList),
    fd:subscribe(lists:usort([node:pidX(Node) || Node <- [Predecessor | SuccList]])),
    cyclon:get_subset_rand_next_interval(1),
    TriggerState = trigger:init(Trigger, fun get_base_interval/0),
    TriggerState2 = trigger:first(TriggerState,make_utility(1)),
    {Id, Me, [Predecessor], SuccList, config:read(cyclon_cache_size),
     stabilizationInterval_min(), TriggerState2, [], true};
on(Msg, uninit) ->
    cs_send:send_local_after(100, self(), Msg),
    uninit;

on({get_successorlist, Pid}, {_Id, Me, _Preds, [], _RandViewSize, _Interval,
                              _TriggerState, _Cache, _Churn} = State)  ->
    cs_send:send_local(Pid , {get_successorlist_response, [Me]}),
    State;
on({get_successorlist, Pid}, {_Id, _Me, _Preds, Succs, _RandViewSize, _Interval,
                              _TriggerState, _Cache, _Churn} = State)  ->
    cs_send:send_local(Pid, {get_successorlist_response, Succs}),
    State;

on({get_predlist, Pid}, {_Id, Me, [], _Succs, _RandViewSize, _Interval,
                         _TriggerState, _Cache, _Churn} = State)  ->
    cs_send:send_local(Pid , {get_predlist_response, [Me]}),
    State;
on({get_predlist, Pid},{_Id, _Me, Preds, _Succs, _RandViewSize, _Interval,
                        _TriggerState, _Cache, _Churn} = State)  ->
    cs_send:send_local(Pid , {get_predlist_response, Preds}),
    State;

% start gossip
on({trigger},{Id, Me, Preds, Succs, RandViewSize, Interval, TriggerState, Cache,
              Churn})  ->
    % Triger an update of the Random view
    %
    RndView= get_RndView(RandViewSize, Cache),
    %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
    {Pred,Succ} = get_safe_pred_succ(Preds,Succs,RndView,Me),
            %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
            %Test for being alone
    NewTriggerState =
        case ((Pred == Me) and (Succ == Me)) of
            true ->
                ring_maintenance:update_preds([Me]),
                ring_maintenance:update_succs([Me]),
                TriggerState;
            false ->
                Message = {rm_buffer, Me, Succs++Preds++[Me]},
                cs_send:send_to_group_member(node:pidX(Succ), ring_maintenance,
                                                 Message),
                cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance,
                                             Message),
                trigger:next(TriggerState,make_utility(1))
        end,
   {Id, Me, Preds, Succs, RandViewSize, Interval, NewTriggerState, Cache, Churn};
% got empty cyclon cache
on({cy_cache, []}, {_Id, _Me, _Preds, _Succs, RandViewSize, _Interval,
                 _TriggerState, _Cache, _Churn} = State)  ->
    % ignore empty cache from cyclon
    cyclon:get_subset_rand_next_interval(RandViewSize),
    State;

% got cyclon cache
on({cy_cache, NewCache},{Id, Me, OldPreds, OldSuccs, RandViewSize, Interval,
                     TriggerState, _Cache, Churn})  ->
             %inc RandViewSize (no error detected)
    RandViewSizeNew = case (RandViewSize < config:read(cyclon_cache_size)) of
                          true ->
                              RandViewSize+1;
                          false ->
                              RandViewSize
                      end,
    % trigger new cyclon cache request
    cyclon:get_subset_rand_next_interval(RandViewSizeNew),
    RndView = get_RndView(RandViewSizeNew, NewCache),
    {NewPreds, NewSuccs, NewInterval, NewChurn} =
        update_view(OldPreds, OldSuccs, NewCache, RndView,
                    Me, Interval, Churn),
    {Id, Me, NewPreds, NewSuccs, RandViewSizeNew, NewInterval, TriggerState,
     NewCache, NewChurn};
% got shuffle request
on({rm_buffer, OtherNode, OtherBuffer}, {Id, Me, OldPreds, OldSuccs, RandViewSize, Interval,
                                         TriggerState ,Cache, Churn})  ->
    RndView=get_RndView(RandViewSize, Cache),
    cs_send:send_to_group_member(node:pidX(OtherNode), ring_maintenance,
                                 {rm_buffer_response, OldSuccs++OldPreds++[Me]}),
    {NewPreds, NewSuccs, NewInterval, NewChurn} =
        update_view(OldPreds, OldSuccs, OtherBuffer, RndView,
                    Me, Interval, Churn),
        NewTriggerState = trigger:next(TriggerState,make_utility(NewInterval)),
    {Id, Me, NewPreds, NewSuccs, RandViewSize, NewInterval, NewTriggerState, Cache, NewChurn};
on({rm_buffer_response, OtherBuffer}, {Id, Me, OldPreds, OldSuccs, RandViewSize, Interval,
                                        TriggerState, Cache, Churn})  ->
    RndView=get_RndView(RandViewSize, Cache),
    {NewPreds, NewSuccs, NewInterval, NewChurn} =
        update_view(OldPreds, OldSuccs, OtherBuffer, RndView,
                    Me, Interval, Churn),
    %inc RandViewSize (no error detected)
    NewRandViewSize = case RandViewSize < config:read(cyclon_cache_size) of
                          true ->
                              RandViewSize+1;
                          false ->
                              RandViewSize
                      end,
    NewTriggerState = trigger:next(TriggerState,make_utility(NewInterval)),
    {Id, Me, NewPreds, NewSuccs, NewRandViewSize, NewInterval, NewTriggerState, Cache, NewChurn};
% dead-node-cache reported dead node to be alive again
on({zombie, Node}, {Id, Me, Preds, Succs, RandViewSize, _Interval, TriggerState, Cache, Churn})  ->
    NewTriggerState = trigger:next(TriggerState,make_utility(3)),
    cs_send:send_local(self_man:get_pid(), {update, ?MODULE, stabilizationInterval,
                                            self(), stabilizationInterval_min()}),
    {Id, Me, Preds, Succs, RandViewSize, stabilizationInterval_min(), NewTriggerState,
     [Node|Cache], Churn};
% failure detector reported dead node
on({crash, DeadPid},{Id, Me, OldPreds, OldSuccs, _RandViewSize, _Interval, TriggerState, Cache, Churn})  ->
    NewPreds = filter(DeadPid, OldPreds),
    NewSuccs = filter(DeadPid, OldSuccs),
    NewCache = filter(DeadPid, Cache),
    update_cs_node(OldPreds, NewPreds, OldSuccs, NewSuccs),
    update_failuredetector(OldPreds, NewPreds, OldSuccs, NewSuccs),
    NewTriggerState = trigger:next(TriggerState,make_utility(3)),
    {Id, Me, NewPreds, NewSuccs, 0, stabilizationInterval_min(), NewTriggerState,
     NewCache,Churn};
on({'$gen_cast', {debug_info, Requestor}},{_Id, _Me, Preds, Succs, _RandViewSize, _Interval,
                                           _TriggerState, _Cache, _Churn} = State)  ->
    cs_send:send_local(Requestor , {debug_info_response, [{"pred", lists:flatten(io_lib:format("~p", [Preds]))},
                                                          {"succs", lists:flatten(io_lib:format("~p", [Succs]))}]}),
    State;
% trigger by admin:dd_check_ring
on({check_ring, Token, Master}, {_Id,  Me, Preds, Succs, _RandViewSize, _Interval,
                            _TriggerState, _Cache, _Churn} = State)  ->
    case {Token, Master} of
        {0, Me} ->
            io:format(" [RM ] CheckRing   OK  ~n");
        {0, _} ->
            io:format(" [RM ] CheckRing  reach TTL in Node ~p not in ~p~n",[Master, Me]);
        {Token, Me} ->
            io:format(" [RM ] Token back with Value: ~p~n",[Token]);
        {Token, _} ->
            {Pred, _Succ} = get_safe_pred_succ(Preds, Succs, [], Me),
            cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance,
                                         {check_ring, Token-1, Master})
    end,
    State;

% trigger by admin:dd_check_ring
on({init_check_ring,Token}, {_Id, Me, Preds, Succs, _RandViewSize, _Interval,
                            _TriggerState, _Cache, _Churn} = State)  ->
    {Pred, _Succ} = get_safe_pred_succ(Preds, Succs, [], Me),
    cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance,
                                 {check_ring, Token - 1, Me}),
    State;

% @TODO: handle information properly
on({notify_new_pred, _Pred}, State) ->
    State;
on({notify_new_succ, _Succ}, State) ->
    State;
on(_, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc merge two successor lists into one
%%      and sort by identifier
merge(L1, L2, Id) ->
    MergedList = lists:append(L1, L2),
    Order = fun(A, B) ->
                    node:id(A) =< node:id(B)
            end,
    Larger  = lists:usort(Order, [X || X <- MergedList, node:id(X) >  Id]),
    Equal   = lists:usort(Order, [X || X <- MergedList, node:id(X) == Id]),
    Smaller = lists:usort(Order, [X || X <- MergedList, node:id(X) <  Id]),
    A = lists:append([Larger, Smaller]),
    case A of
        [] -> Equal;
        _  -> A
    end.

%-spec(filter/2 :: (cs_send:mypid(), list(node:node_type()) -> list(node:node_type()).
filter(_Pid, []) ->
    [];
filter(Pid, [Succ | Rest]) ->
    case Pid == node:pidX(Succ) of
        true ->
            %Hook for DeadNodeCache
            dn_cache:add_zombie_candidate(Succ),
            filter(Pid, Rest);
        false ->
            [Succ | filter(Pid, Rest)]
    end.

%% @doc get a peer form the cycloncache which is alive
get_RndView(N,Cache) ->
    lists:sublist(Cache, N).

% @doc Check if change of failuredetector is necessary
-spec(update_failuredetector/4 :: (list(), list(), list(), list()) ->
              ok).
update_failuredetector(OldPreds, NewPreds, OldSuccs, NewSuccs) ->
    OldView=lists:usort(OldPreds++OldSuccs),
    NewView=lists:usort(NewPreds++NewSuccs),
    case (NewView /= OldView) of
        true ->
            NewNodes = util:minus(NewView,OldView),
            OldNodes = util:minus(OldView,NewView),
            fd:unsubscribe([node:pidX(Node) || Node <- OldNodes]),
            fd:subscribe([node:pidX(Node) || Node <- NewNodes]),
            ok;
        false ->
            ok
    end.

% @doc inform the cs_node of new [succ|pred] if necessary
update_cs_node(OldPreds, NewPreds, OldSuccs, NewSuccs) ->
    %io:format("UCN: ~p ~n",[{PredsNew,SuccsNew,ShuffelBuddy,AktPred,AktSucc}]),
    case NewPreds =/= [] andalso OldPreds =/= NewPreds of
        true -> ring_maintenance:update_preds(NewPreds);
        false -> ok
    end,
    case NewSuccs =/= [] andalso OldSuccs =/= NewSuccs of
        true -> ring_maintenance:update_succs(NewSuccs);
        false -> ok
    end,
    {NewPreds,NewSuccs}.

get_safe_pred_succ(Preds, Succs, RndView, Me) ->
    case (Preds == []) or (Succs == []) of
        true ->
            Buffer = merge(Preds ++ Succs, RndView,node:id(Me)),
            %io:format("Buffer: ~p~n",[Buffer]),
            case Buffer == [] of
                false ->
                    SuccsNew = lists:sublist(Buffer, 1,  config:read(succ_list_length)),
                    PredsNew = lists:sublist(lists:reverse(Buffer), 1,  config:read(pred_list_length)),
                    {hd(PredsNew), hd(SuccsNew)};
                true ->
                    {Me, Me}
            end;
        false ->
            {hd(Preds), hd(Succs)}
    end.

% @doc adaptize the Tman-interval
new_interval(OldPreds, NewPreds, OldSuccs, NewSuccs,_Interval,Churn) ->
    case (((OldPreds++OldSuccs)=:=(NewPreds++NewSuccs)) andalso (Churn==0)) of
        true ->                 % increasing the timer
            0;
        false ->
            2
    end.

% @doc is there churn in the system
has_churn(OldPreds, NewPreds, OldSuccs, NewSuccs) ->
    OldPreds =:= NewPreds andalso OldSuccs =:= NewSuccs.

get_pid_dnc() ->
    process_dictionary:get_group_member(dn_cache).

% get Pid of assigned cs_node
get_cs_pid() ->
    process_dictionary:get_group_member(cs_node).

get_base_interval() ->
    config:read(stabilization_interval_base).

get_min_interval() ->
    config:read(stabilization_interval_min).

get_max_interval() ->
    config:read(stabilization_interval_max).

make_utility(Intervall) ->
    %Now = erlang:now(),
    fun (_T, _C) ->   Intervall end.

update_view(OldPreds, OldSuccs, OtherBuffer, RndView, Me, Interval, Churn) ->
    Buffer=merge(OldSuccs++OldPreds, OtherBuffer++RndView, node:id(Me)),
    NewSuccs=lists:sublist(Buffer, config:read(succ_list_length)),
    NewPreds=lists:sublist(lists:reverse(Buffer), config:read(pred_list_length)),
    update_cs_node(OldPreds, NewPreds, OldSuccs, NewSuccs),
    update_failuredetector(OldPreds, NewPreds, OldSuccs, NewSuccs),
    NewInterval = new_interval(OldPreds, NewPreds, OldSuccs, NewSuccs, Interval, Churn),
    NewChurn = has_churn(OldPreds, NewPreds, OldSuccs, NewSuccs),
    {NewPreds, NewSuccs, NewInterval, NewChurn}.

%% @doc the interval between two stabilization runs Min
%% @spec stabilizationInterval_min() -> integer() | failed
stabilizationInterval_min() ->
    config:read(stabilization_interval_min).
