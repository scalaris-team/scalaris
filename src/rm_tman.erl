%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%% @copyright 2007-2009 Konrad-Zuse-Zentrum f�r Informationstechnik Berlin
%% @version $Id$
-module(rm_tman, [Trigger]).

-author('hennig@zib.de').
-vsn('$Id$ ').





-export([init/1,on/2]).
-behavior(gen_component).
-behavior(ring_maintenance).
-behavior(self_man).
-export([start_link/1, 
	 get_base_interval/0,get_min_interval/0,get_max_interval/0
	]).

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
    gen_component:start_link(?MODULE:new(Trigger), [InstanceId, Options], [{register, InstanceId, ring_maintenance}]).

init(_Args) ->
    log:log(info,"[ RM ~p ] starting ring maintainer TMAN~n", [self()]),
    cs_send:send_local(get_pid_dnc() , {subscribe, self()}),
    cs_send:send_local(get_cs_pid(), {init_rm,self()}),
    uninit.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% @doc the Token takes care, that there is only one timermessage for stabilize 

on({init, NewId, NewMe, NewPred, NewSuccList, _CSNode},uninit) ->
    ring_maintenance:update_succ_and_pred(NewPred, hd(NewSuccList)),
    failuredetector2:subscribe(lists:usort([node:pidX(Node) || Node <- [NewPred | NewSuccList]])),
    TriggerState = Trigger:init(?MODULE:new(Trigger)),
    TriggerState2 = Trigger:trigger_first(TriggerState,1),
    {NewId, NewMe, [NewPred], NewSuccList,config:read(cyclon_cache_size),config:stabilizationInterval_min(),TriggerState2,NewPred,hd(NewSuccList),[],1};
on(_,uninit) ->
    uninit;

on({get_successorlist, Pid},{Id, Me, Preds, [],RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    cs_send:send_local(Pid , {get_successorlist_response, [Me]}),
    {Id, Me, Preds, [],RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({get_successorlist, Pid},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->

    case Succs of
        []  ->  cs_send:send_local(Pid , {get_successorlist_response, [Me]});
        _   ->  cs_send:send_local(Pid , {get_successorlist_response, Succs})
    end,
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({get_predlist, Pid},{Id, Me, [], Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    cs_send:send_local(Pid , {get_predlist_response, [Me]}),
    {Id, Me, [], Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({get_predlist, Pid},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    case Preds of
        []  -> cs_send:send_local(Pid , {get_predlist_response, [Me]});
        _   -> cs_send:send_local(Pid , {get_predlist_response, Preds})
    end,
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};



on({trigger},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  -> % new stabilization interval
    
    % Triger an update of the Random view
    cs_send:send_local(get_cyclon_pid() , {get_subset_max_age,RandViewSize,self()}),
    RndView=get_RndView(RandViewSize,Cache),
            %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
    {Pred,Succ} =get_safe_pred_succ(Preds,Succs,RndView,Me),
            %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
            %Test for being alone
    case ((Pred == Me) and (Succ == Me)) of
        true ->
            ring_maintenance:update_pred(Me),
            ring_maintenance:update_succ(Me),
            NewAktSucc =Me,
            NewAktPred =Me,
            NewTriggerState=TriggerState;
        false ->
            cs_send:send_to_group_member(node:pidX(Succ), ring_maintenance, {rm_buffer,Me,Succs++Preds++[Me]}),
            cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance, {rm_buffer,Me,Succs++Preds++[Me]}),
            NewAktSucc =AktSucc,
            NewAktPred =AktPred,
            %TODO: is this send_after necassary? Yes if all nodes in view are death
	    NewTriggerState = Trigger:trigger_next(TriggerState,1)
    end,
   {Id, Me, Preds, Succs,RandViewSize,Interval,NewTriggerState,NewAktPred,NewAktSucc,Cache,Churn};
on({cache,NewCache},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,_Cache,Churn})  ->
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,NewCache,Churn};
on({rm_buffer,Q,Buffer_q},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    RndView=get_RndView(RandViewSize,Cache),
    cs_send:send_to_group_member(node:pidX(Q),ring_maintenance,{rm_buffer_response,Succs++Preds++[Me]}),
    Buffer=merge(Succs++Preds,Buffer_q++RndView,node:id(Me)),
    SuccsNew=lists:sublist(Buffer, config:read(succ_list_length)),
    PredsNew=lists:sublist(lists:reverse(Buffer), config:read(pred_list_length)),
    {NewAktPred,NewAktSucc} = update_cs_node(PredsNew,SuccsNew,AktPred,AktSucc),
    update_failuredetector(Preds,Succs,PredsNew,SuccsNew),
    NewInterval = new_interval(Preds,Succs,PredsNew,SuccsNew,Interval,Churn),
    NewChurn = new_churn(Preds,Succs,PredsNew,SuccsNew),
    NewTriggerState = Trigger:trigger_next(TriggerState,2),
    {Id, Me, PredsNew, SuccsNew,RandViewSize,NewInterval,NewTriggerState,NewAktPred,NewAktSucc,Cache,NewChurn};
on({rm_buffer_response,Buffer_p},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    RndView=get_RndView(RandViewSize,Cache),
            %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
    Buffer=merge(Succs++Preds,Buffer_p++RndView,node:id(Me)),
    SuccsNew=lists:sublist(Buffer, 1, config:read(succ_list_length)),
    PredsNew=lists:sublist(lists:reverse(Buffer), 1, config:read(pred_list_length)),
    {NewAktPred,NewAktSucc} = update_cs_node(PredsNew,SuccsNew,AktPred,AktSucc),
    update_failuredetector(Preds,Succs,PredsNew,SuccsNew ),
    NewInterval = new_interval(Preds,Succs,PredsNew,SuccsNew,Interval,Churn),
    NewChurn = new_churn(Preds,Succs,PredsNew,SuccsNew),
            %inc RandViewSize (no error detected)
    RandViewSizeNew = case RandViewSize < config:read(cyclon_cache_size) of
                          true ->
            RandViewSize+1;
                          false ->
            RandViewSize
                      end,
    NewTriggerState = Trigger:trigger_next(TriggerState,1),
    {Id, Me, PredsNew, SuccsNew,RandViewSizeNew,NewInterval,NewTriggerState,NewAktPred,NewAktSucc,Cache,NewChurn};
on({zombie,Node},{Id, Me, Preds, Succs,RandViewSize,_Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    NewTriggerState = Trigger:trigger_next(TriggerState,2),
    cs_send:send_local(self_man:get_pid(),{update,?MODULE,stabilizationInterval,self(),config:stabilizationInterval_min()}),
    {Id, Me, Preds, Succs,RandViewSize,config:stabilizationInterval_min(),NewTriggerState,AktPred,AktSucc,[Node|Cache],Churn};
on({crash, DeadPid},{Id, Me, Preds, Succs,_RandViewSize,_Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    PredsNew = filter(DeadPid, Preds),
    SuccsNew = filter(DeadPid, Succs),
    NewCache = filter(DeadPid, Cache),
    update_failuredetector(Preds,Succs,PredsNew,SuccsNew ),
    NewTriggerState = Trigger:trigger_next(TriggerState,2),
    {Id, Me, PredsNew ,SuccsNew,0,config:stabilizationInterval_min(),NewTriggerState,AktPred,AktSucc ,NewCache,Churn};
on({'$gen_cast', {debug_info, Requestor}},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    cs_send:send_local(Requestor , {debug_info_response, [{"pred", lists:flatten(io_lib:format("~p", [Preds]))},
                                                          {"succs", lists:flatten(io_lib:format("~p", [Succs]))}]}),
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({check_ring,0,Me},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    io:format(" [RM ] CheckRing   OK  ~n"),
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({check_ring,Token,Me},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    io:format(" [RM ] Token back with Value: ~p~n",[Token]),
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({check_ring,0,Master},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    io:format(" [RM ] CheckRing  reach TTL in Node ~p not in ~p~n",[Master,Me]),
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({check_ring,Token,Master},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    cs_send:send_to_group_member(node:pidX(AktPred), ring_maintenance, {check_ring,Token-1,Master}),
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
on({init_check_ring,Token},{Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn})  ->
    cs_send:send_to_group_member(node:pidX(AktPred), ring_maintenance, {check_ring,Token-1,Me}),
    {Id, Me, Preds, Succs,RandViewSize,Interval,TriggerState,AktPred,AktSucc,Cache,Churn};
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
        []	-> Equal;
        _	-> A
    end.

%-spec(filter/2 :: (cs_send:mypid(), list(node:node_type()) -> list(node:node_type()).
filter(_Pid, []) ->
    [];
filter(Pid, [Succ | Rest]) ->
    case Pid == node:pidX(Succ) of
	true ->
        
        %Hook for DeadNodeCache
            cs_send:send_local(get_pid_dnc() , {add_zombie_candidate, Succ}),

	    filter(Pid, Rest);
	false ->
	    [Succ | filter(Pid, Rest)]
    end.

%% @doc get a peer form the cycloncache which is alive 
get_RndView(N,Cache) ->
    lists:sublist(Cache, N).
     
     
     

% @doc Check if change of failuredetector is necessary
update_failuredetector(Preds,Succs,PredsNew,SuccsNew) ->
    OldView=lists:usort(Preds++Succs),
    NewView=lists:usort(PredsNew++SuccsNew),
    case (NewView /= OldView) of
        true ->
	    NewNodes = util:minus(NewView,OldView),
	    OldNodes = util:minus(OldView,NewView),
	    update_fd([node:pidX(Node) || Node <- OldNodes],fun failuredetector2:unsubscribe/1),
	    update_fd([node:pidX(Node) || Node <- NewNodes],fun failuredetector2:subscribe/1);
	false ->
	    _NewNodes = util:minus(NewView,OldView),
	    _OldNodes = util:minus(OldView,NewView),
	    ok
    end,
    ok.
             
             
    

update_fd([], _) ->
    ok;
update_fd(Nodes, F) ->
    F(Nodes).             
           
	
% @doc informed the cs_node for new [succ|pred] if necessary
update_cs_node(PredsNew,SuccsNew,AktPred,AktSucc) ->
    %io:format("UCN: ~p ~n",[{PredsNew,SuccsNew,ShuffelBuddy,AktPred,AktSucc}]),
    case has_changed(PredsNew,AktPred) of
    	false ->
            NewAktPred = AktPred;
        {ok, NewAktPred} ->
            ring_maintenance:update_pred(NewAktPred)
    end,
    case has_changed(SuccsNew,AktSucc) of
    	false ->
            NewAktSucc = AktSucc;
        {ok, NewAktSucc} ->
            ring_maintenance:update_succ(NewAktSucc)
    end,
    {NewAktPred,NewAktSucc}.

get_safe_pred_succ(Preds,Succs,RndView,Me) ->
    case (Preds == []) or (Succs == []) of
        true ->
            Buffer = merge(Preds ++ Succs, RndView,node:id(Me)),
            %io:format("Buffer: ~p~n",[Buffer]),
            case Buffer == [] of
                false ->
                    SuccsNew=lists:sublist(Buffer, 1,  config:read(succ_list_length)),
                    PredsNew=lists:sublist(lists:reverse(Buffer), 1,  config:read(pred_list_length)),
                    {hd(PredsNew), hd(SuccsNew)};
                true ->
                    {Me, Me}
            end;
        false ->
            {hd(Preds), hd(Succs)}
    end.
% @doc adaptize the Tman-interval
new_interval(Preds,Succs,PNew,SNew,Interval,Churn) ->
   
    case (((Preds++Succs)==(PNew++SNew)) and (Churn==0)) of
        true ->                 % incresing the timer
            cs_send:send_local(self_man:get_pid(),{no_churn}),
            case (Interval >= config:stabilizationInterval_max() ) of
                true -> config:stabilizationInterval_max();
                false -> Interval + ((config:stabilizationInterval_max() - config:stabilizationInterval_min()) div 10)

            end;
            
            %config:stabilizationInterval_max();
        false ->
%					case (Interval - (config:stabilizationInterval_max()-config:stabilizationInterval_min()) div 2) =< (config:stabilizationInterval_min()  ) of
%						true -> config:stabilizationInterval_min() ;
%						false -> Interval - (config:stabilizationInterval_max()-config:stabilizationInterval_min()) div 2
%					end
            config:stabilizationInterval_min()
    end.
new_churn(Preds,Succs,PNew,SNew) ->
    case((Preds++Succs)=:=(PNew++SNew)) of
        true  -> 0;
        false -> 1
    end.




%% @doc has_changed(NewView, AktNode)
-spec(has_changed/2 :: (list(), node:node_type()) -> {ok, node:node_type()} | false).
has_changed([X | _], X) ->
    false;
has_changed([X | _], _) ->
    {ok, X}.


get_cyclon_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cyclon).




get_pid_dnc() ->
    process_dictionary:lookup_process(erlang:get(instance_id), dn_cache).


% get Pid of assigned cs_node
get_cs_pid() ->
    InstanceId = erlang:get(instance_id),
    if
	InstanceId == undefined ->
            log:log(error,"[ RM | ~w ] ~p", [self(),util:get_stacktrace()]);
	true ->
	    ok
    end,
    process_dictionary:lookup_process(InstanceId, cs_node).

get_base_interval() ->
    config:read(stabilization_interval_base).

get_min_interval() ->
    config:read(stabilization_interval_min).

get_max_interval() ->
    config:read(stabilization_interval_max).