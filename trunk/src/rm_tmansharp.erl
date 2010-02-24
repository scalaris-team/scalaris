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
-module(rm_tmansharp).

-author('hennig@zib.de').
-vsn('$Id$ ').



-export([init/1,on/2]).

-behavior(ring_maintenance).
-behavior(gen_component).

-export([start_link/1, 
	 get_successorlist/0, get_predlist/0, succ_left/1, pred_left/1, 
         update_succ/1, update_pred/1, 
	 get_as_list/0]).

% unit testing
-export([ merge/2, rank/2,get_pred/1,get_succ/1,get_preds/1,get_succs/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc spawns a chord-like ring maintenance process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    start_link(InstanceId, []).

start_link(InstanceId,Options) ->
   gen_component:start_link(?MODULE, [InstanceId, Options], [{register, InstanceId, ring_maintenance}]).

init(_Args) ->
    log:log(info,"[ RM ~p ] starting ring maintainer TMAN~n", [self()]),
    dn_cache:subscribe(),
     cs_send:send_local(get_cs_pid(), {init_rm,self()}),
    uninit.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


get_successorlist() ->
    cs_send:send_local(get_pid() , {get_successorlist, self()}).

get_predlist() ->
    cs_send:send_local(get_pid() , {get_predlist, self()}).
    

%% @doc notification that my succ left
%%      parameter is his current succ list
succ_left(_Succ) ->
    %% @TODO
    ok.

%% @doc notification that my pred left
%%      parameter is his current pred
pred_left(_PredsPred) ->
    %% @TODO
    ok.

%% @doc notification that my succ changed
%%      parameter is potential new succ
update_succ(_Succ) ->
    %% @TODO
    ok.

%% @doc notification that my pred changed
%%      parameter is potential new pred
update_pred(_Pred) ->
    %% @TODO
    ok.


get_as_list() ->
    get_successorlist().
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


on({init, NewId, NewMe, NewPred, NewSuccList, _CSNode},uninit) ->
        ring_maintenance:update_succ_and_pred(NewPred, hd(NewSuccList)),
        fd:subscribe(lists:usort([node:pidX(Node) || Node <- [NewPred | NewSuccList]])),
        Token = 0,
        cs_send:send_after(0, self(), {stabilize,Token}),
        {NewId, NewMe, [NewPred]++NewSuccList,config:read(cyclon_cache_size),config:stabilizationInterval_min(),Token,NewPred,hd(NewSuccList),[]};
on(_,uninit) ->
        uninit;

% @doc the Token takes care, that there is only one timermessage for stabilize 
on({get_successorlist, Pid},{Id, Me, [] ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            cs_send:send_local(Pid , {get_successorlist_response,[Me]}),
	    	{Id, Me, [] ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({get_predlist, Pid},{Id, Me, [],RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
			cs_send:send_local(Pid , {get_predlist_response, [Me]}),
            {Id, Me, [],RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({get_successorlist, Pid},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            cs_send:send_local(Pid , {get_successorlist_response, get_succs(View)}),
            {Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({get_predlist, Pid},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            cs_send:send_local(Pid , {get_predlist_response, get_preds(View)}),
            {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};


on({stabilize,AktToken},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) -> % new stabilization interval
              
            % Triger an update of the Random view
            cs_send:send_local(get_cyclon_pid() , {get_subset_max_age,RandViewSize,self()}),
            RndView=get_RndView(RandViewSize,RandomCache),
            %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
			P =selectPeer(rank(View++RndView,node:id(Me)),Me),
            %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
            %Test for being alone
            case (P == Me) of 
                true ->
                    ring_maintenance:update_pred(Me),
		      		ring_maintenance:update_succ(Me);
          		false ->
                    cs_send:send_to_group_member(node:pidX(P), ring_maintenance, {rm_buffer,Me,extractMessage(View++[Me]++RndView,P)})
        	end,
			cs_send:send_after(Interval, self(), {stabilize,AktToken}),
            {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({stabilize,_},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({cache,NewCache},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,_RandomCache}) ->
            {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,NewCache};
on({rm_buffer,Q,Buffer_q},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            RndView=get_RndView(RandViewSize,RandomCache),
            cs_send:send_to_group_member(node:pidX(Q),ring_maintenance,{rm_buffer_response,extractMessage(View++[Me]++RndView,Q)}),
            %io:format("after_send~p~n",[self()]),
            NewView=rank(View++Buffer_q++RndView,node:id(Me)),
            %io:format("after_rank~p~n",[self()]),
   	        %SuccsNew=get_succs(NewView),
            %PredsNew=get_preds(NewView),
            {NewAktPred,NewAktSucc} = update_cs_node(NewView,AktPred,AktSucc),
            update_failuredetector(View,NewView),
            NewInterval = new_interval(View,NewView,Interval),
            cs_send:send_after(NewInterval , self(), {stabilize,AktToken+1}),
            %io:format("loop~p~n",[self()]),   
            {Id, Me, NewView,RandViewSize,NewInterval,AktToken+1,NewAktPred,NewAktSucc,RandomCache};	
on({rm_buffer_response,Buffer_p},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache})->
            
            RndView=get_RndView(RandViewSize,RandomCache),
            %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
            Buffer=rank(View++Buffer_p++RndView,node:id(Me)),
            %io:format("after_rank~p~n",[self()]),
            NewView=lists:sublist(Buffer,config:read(succ_list_length)+config:read(pred_list_length)),
            {NewAktPred,NewAktSucc} = update_cs_node(View,AktPred,AktSucc),
            update_failuredetector(View,NewView),
            NewInterval = new_interval(View,NewView,Interval),
            %inc RandViewSize (no error detected)
            RandViewSizeNew = case RandViewSize < config:read(cyclon_cache_size) of
                true ->
                    RandViewSize+1;
                false ->
                    RandViewSize
            end,
            cs_send:send_after(NewInterval , self(), {stabilize,AktToken+1}),
            
            {Id, Me, NewView,RandViewSizeNew,NewInterval,AktToken+1,NewAktPred,NewAktSucc,RandomCache};
on({zombie,Node},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            erlang:send(self(), {stabilize,AktToken+1}),
            %Inform Cyclon !!!!
            {Id, Me, View,RandViewSize,Interval,AktToken+1,AktPred,AktSucc,[Node|RandomCache]};
on({crash, DeadPid},{Id, Me, View ,_RandViewSize,_Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            NewView = filter(DeadPid, View),
            NewCache = filter(DeadPid, RandomCache),
            update_failuredetector(View,NewView),
            erlang:send(self(), {stabilize,AktToken+1}),
		 	{Id, Me, NewView,0,config:stabilizationInterval_min(),AktToken+1,AktPred,AktSucc,NewCache};
on({'$gen_cast', {debug_info, Requestor}},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
	    	cs_send:send_local(Requestor , {debug_info_response, [{"pred", lists:flatten(io_lib:format("~p", [get_preds(View)]))}, 
					       {"succs", lists:flatten(io_lib:format("~p", [get_succs(View)]))}]}),
	    	{Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
        
on({check_ring,0,Me},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            io:format(" [RM ] CheckRing   OK  ~n"),
             {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({check_ring,Token,Me},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            io:format(" [RM ] Token back with Value: ~p~n",[Token]),
            {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({check_ring,0,Master},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
            io:format(" [RM ] CheckRing  reach TTL in Node ~p not in ~p~n",[Master,Me]),
            {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({check_ring,Token,Master},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
             cs_send:send_to_group_member(node:pidX(AktPred), ring_maintenance, {check_ring,Token-1,Master}),
             {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on({init_check_ring,Token},{Id, Me, View ,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache}) ->
             cs_send:send_to_group_member(node:pidX(AktPred), ring_maintenance, {check_ring,Token-1,Me}),
             {Id, Me, View,RandViewSize,Interval,AktToken,AktPred,AktSucc,RandomCache};
on(_, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc merge two successor lists into one
%%      and sort by identifier


rank(MergedList,Id) ->
    %io:format("--------------------------------- ~p ~n",[Id]),
    %io:format("in: ~p ~p ~n",[self(),MergedList]),
    Order = fun(A, B) ->
            node:id(A) =< node:id(B)
            %A=<B
        end,
    Larger  = lists:usort(Order, [X || X <- MergedList, node:id(X) >  Id]),
    Equal   = lists:usort(Order, [X || X <- MergedList, node:id(X) == Id]),
    Smaller = lists:usort(Order, [X || X <- MergedList, node:id(X) <  Id]),
    
    H1 = Larger++Smaller,
    Half = length(H1) div 2,
    {Succs,Preds} = lists:split(Half,H1),
    Return=lists:sublist(merge(Succs,lists:reverse(Preds)),10), %config:read(succ_list_length)+config:read(pred_list_length)
   
    %io:format("return: ~p ~p ~n",[self(),Return]),
    A =case Return of
        []  -> Equal;
        _   -> Return
    end,
    %io:format("out: ~p ~p ~n",[self(),A]),
    A.

selectPeer([],Me) ->
    Me;
selectPeer(View,_) ->
    NTH = randoms:rand_uniform(1, 3),
    case (NTH=<length(View)) of
        true -> lists:nth( NTH,View);
        false -> lists:nth(length(View),View)
    end.
            

extractMessage(View,P) ->
    lists:sublist(rank(View,node:id(P)),10).
    

merge([H1|T1],[H2|T2]) ->
    [H1,H2]++merge(T1,T2);
merge([],[T|H]) ->
    [T|H];
merge([],X) ->
    X;
merge(X,[]) ->
    X;
merge([],[]) ->
    [].


get_succs([T]) ->
    [T];
get_succs(View) ->
    get_every_nth(View,1,0).
get_preds([T]) ->
    [T];
get_preds(View) ->
    get_every_nth(View,1,1).

get_succ([H|_]) ->
    H.

get_pred([H|T]) ->
    case T of
        []  -> H;
        _   -> get_succ(T)
    end.
    

get_every_nth([],_,_) ->
    [];
get_every_nth([H|T],Nth,Offset) ->
    case Offset of
        0 ->  [H|get_every_nth(T,Nth,Nth)];
        _ ->  get_every_nth(T,Nth,Offset-1)
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
update_failuredetector(OldView,NewView) ->
    
		case (NewView /= OldView) of
        true ->
            	
                
                NewNodes = util:minus(NewView,OldView),
                OldNodes = util:minus(OldView,NewView),
                            
                update_fd([node:pidX(Node) || Node <- OldNodes],fun fd:unsubscribe/1),
           		update_fd([node:pidX(Node) || Node <- NewNodes],fun fd:subscribe/1);
                
        		
		false ->
            	
              	ok
		end,
    	ok.
             
             
    

update_fd([], _) ->
    ok;
update_fd(Nodes, F) ->
    F(Nodes).             
           
	
% @doc informed the cs_node for new [succ|pred] if necessary
update_cs_node(View,_AktPred,_AktSucc) ->
        NewAktPred=get_pred(View),
        NewAktSucc=get_succ(View),
      	ring_maintenance:update_pred(NewAktPred),
      	ring_maintenance:update_succ(NewAktSucc),
{NewAktPred,NewAktSucc}.

% @doc adaptize the Tman-interval
new_interval(View,NewView,Interval) ->
			case (View==NewView) of
            	true ->
					case (Interval >= config:stabilizationInterval_max() ) of
						true -> config:stabilizationInterval_max();
						false -> Interval + ((config:stabilizationInterval_max() - config:stabilizationInterval_min()) div 10)
                                 
					end;
            	false ->
					case (Interval - (config:stabilizationInterval_max()-config:stabilizationInterval_min()) div 2) =< (config:stabilizationInterval_min()  ) of
						true -> config:stabilizationInterval_min() ;
						false -> Interval - (config:stabilizationInterval_max()-config:stabilizationInterval_min()) div 2
					end
    		end.








get_cyclon_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cyclon).

% print_view(Me,View) ->
%     io:format("[~p] -> ",[node:pidX(Me)]),
%     [io:format("~p",[node:pidX(Node)]) || Node <- View],
%     io:format("~n").



% @private
get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), ring_maintenance).

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
