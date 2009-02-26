%  Copyright 2007-2008 Konrad-Zuse-Zentrum f√ºr Informationstechnik Berlin
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
%% @copyright 2007-2009 Konrad-Zuse-Zentrum f¸r Informationstechnik Berlin
%% @version $Id$
-module(rm_tman).

-author('hennig@zib.de').
-vsn('$Id$ ').



-export([start/2]).

-behavior(ring_maintenance).

-export([start_link/1, initialize/4, 
	 get_successorlist/0, get_predlist/0, succ_left/1, pred_left/1, 
         update_succ/1, update_pred/1, 
	 get_as_list/0]).

% unit testing
-export([merge/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a chord-like ring maintenance process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    Link = spawn_link(?MODULE, start, [InstanceId, self()]),
    receive
	start_done ->
	    ok
    end,
    {ok, Link}.

%% @doc called once by the cs_node when joining the ring
initialize(Id, Me, Pred, Succ) ->
    get_pid() ! {init, Id, Me, Pred, [Succ], self()},
    receive
	{init_done} ->
	    ok
    end.

get_successorlist() ->
    get_pid() ! {get_successorlist, self()},
    receive
	{get_successorlist_response, SuccList} ->
	    SuccList
    end.

get_predlist() ->
    get_pid() ! {get_predlist, self()},
    receive
	{get_predlist_response, PredList} ->
	    PredList
    end.

%% @doc notification that my succ left
%%      parameter is his current succ list
succ_left(_SuccsSuccList) ->
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

start() ->
    receive
	{init, NewId, NewMe, NewPred, NewSuccList, CSNode} -> %set info for cs_node
        %io:format("NewId ~p~n NewMe ~p~n NewPred ~p~n NewSuccList ~p~n CSNode ~p~n",[NewId, NewMe, NewPred, NewSuccList, CSNode]),
 	    ring_maintenance:update_succ_and_pred(NewPred, hd(NewSuccList)),
  	    failuredetector2:subscribe(lists:usort([node:pidX(Node) || Node <- [NewPred | NewSuccList]])),
	    CSNode ! {init_done},
        Token = 0,
    	erlang:send_after(config:stabilizationInterval_min(), self(), {stabilize,Token}),
	    loop(NewId, NewMe, [NewPred], NewSuccList,config:read(cyclon_cache_size),config:stabilizationInterval_min(),Token,NewPred,hd(NewSuccList),[])
    end.
% @doc the Token takes care, that there is only one timermessage for stabilize 
loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken,AktPred,AktSucc,Cache) ->
 	case Succs == merge(Succs, [], Id) of
        false ->
            log:log(error,"[ RM | ~p] should never ever happen (succs): ~p", [self(),Succs]);
        true ->
            ok
    end,
 	case Preds == lists:reverse(merge(Preds, [], Id)) of
        false ->
           log:log(error,"[RM | ~p]should never ever happen (preds): ~p", [self(),Preds]);
        true ->
            ok
    end,
    
    
    receive
    {get_successorlist, Pid} ->
        case length(Succs) > 1 of
            true ->
                Pid ! {get_successorlist_response, util:uniq([AktSucc|lists:nthtail(1,Succs)])};
            false ->
                Pid ! {get_successorlist_response, [AktSucc]}
        end,
	    loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken,AktPred,AktSucc,Cache);
    {get_predlist, Pid} ->
        case length(Preds) > 1 of
            true ->
                Pid ! {get_predlist_response, util:uniq([AktPred|lists:nthtail(1,Preds)])};
            false ->
                Pid ! {get_predlist_response, [AktPred]}
        end,
	    loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken,AktPred,AktSucc,Cache);
    {stabilize,AktToken} -> % new stabilization interval
       %io:format("."),
        % Triger an update of the Random view

        get_cyclon_pid() ! {get_subset,all,self()},
        
        RndView=get_RndView(RandViewSize,Cache),
		{Pred,Succ} =get_safe_pred_succ(Preds,Succs,RndView,Me),
        
        
        %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
        %Test for being alone
        case ((Pred == Me) and (Succ == Me)) of 
            true ->
                ring_maintenance:update_pred(Me),
				ring_maintenance:update_succ(Me),
            	NewAktSucc =Me,
				NewAktPred =Me;
             false ->
        		cs_send:send_to_group_member(node:pidX(Succ), ring_maintenance, {rm_buffer,Me,Succs++Preds++[Me]++RndView}),
    			cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance, {rm_buffer,Me,Succs++Preds++[Me]++RndView}),
        		NewAktSucc =AktSucc,
				NewAktPred =AktPred
	    end,
        RandViewSizenew = case RandViewSize < config:read(cyclon_cache_size) of
            true ->
                RandViewSize+1;
            false ->
                RandViewSize
        end,
        %io:format("~p ~p ~n", [Interval,RandViewSize]),
       	erlang:send_after(Interval, self(), {stabilize,AktToken}),
        loop(Id, Me, Preds, Succs,RandViewSizenew,Interval,AktToken,NewAktPred,NewAktSucc,Cache);
	{stabilize,_} ->
		loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken,AktPred,AktSucc,Cache);
    {cache,NewCache} ->
        loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken,AktPred,AktSucc,NewCache);
    {rm_buffer,Q,Buffer_q} ->
        RndView=get_RndView(RandViewSize,Cache),
        cs_send:send_to_group_member(node:pidX(Q),ring_maintenance,{rm_buffer_response,Me,Succs++Preds++[Me]++RndView}),
        Buffer=merge(Succs++Preds,Buffer_q,node:id(Me)),
   	    SuccsNew=lists:sublist(Buffer, 1, config:read(succ_list_length)),
        PredsNew=lists:sublist(lists:reverse(Buffer), 1, config:read(pred_list_length)),
        
        
		{NewAktPred,NewAktSucc} = update_cs_node(PredsNew,SuccsNew,AktPred,AktSucc,Q),
        update_failuredetector(Preds,Succs,PredsNew,SuccsNew),
      	NewInterval = new_interval(Preds,Succs,PredsNew,SuccsNew,Interval),
        erlang:send_after(NewInterval , self(), {stabilize,AktToken+1}),
        loop(Id, Me, PredsNew, SuccsNew,RandViewSize,NewInterval,AktToken+1,NewAktPred,NewAktSucc,Cache);	
    {rm_buffer_response,Q,Buffer_p} ->	
    	Buffer=merge(Succs++Preds,Buffer_p,node:id(Me)),
		SuccsNew=lists:sublist(Buffer, 1, config:read(succ_list_length)),
        PredsNew=lists:sublist(lists:reverse(Buffer), 1, config:read(pred_list_length)),
        {NewAktPred,NewAktSucc} = update_cs_node(PredsNew,SuccsNew,AktPred,AktSucc,Q),
        update_failuredetector(Preds,Succs,PredsNew,SuccsNew ),
       
        NewInterval = new_interval(Preds,Succs,PredsNew,SuccsNew,Interval),
        erlang:send_after(NewInterval , self(), {stabilize,AktToken+1}),
        loop(Id, Me, PredsNew, SuccsNew,RandViewSize,NewInterval,AktToken+1,NewAktPred,NewAktSucc,Cache);
    {zombie,Node} ->
        erlang:send(self(), {stabilize,AktToken+1}),
        loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken+1,AktPred,AktSucc,[Node|Cache]);
	{crash, DeadPid} ->
        %io:format("~p RM | ~p crashed~n",[self(),DeadPid]),
        
                
        PredsNew = filter(DeadPid, Preds),
        SuccsNew = filter(DeadPid, Succs),
        %io:format("~p~n", [{PredsNew,SuccsNew}]),
     	
        %update_failuredetector(Preds,Succs,PredsNew,SuccsNew ),
        erlang:send(self(), {stabilize,AktToken+1}),
		%erlang:send_after(config:stabilizationInterval_min(), self(), {stabilize,AktToken+1}),
        loop(Id, Me, PredsNew ,SuccsNew,1,config:stabilizationInterval_min(),AktToken+1,AktPred,AktSucc ,Cache);
    	
	{'$gen_cast', {debug_info, Requestor}} ->
	    Requestor ! {debug_info_response, [{"pred", lists:flatten(io_lib:format("~p", [Preds]))}, 
					       {"succs", lists:flatten(io_lib:format("~p", [Succs]))}]},
	    loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken,AktPred,AktSucc,Cache);
    
	X ->
	   log:log(warn,"@rm_tman unknown message ~p", [X]),
	    loop(Id, Me, Preds, Succs,RandViewSize,Interval,AktToken,AktPred,AktSucc,Cache)
    end.

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
        dn_cache:add_zombie_candidate(Succ),

	    filter(Pid, Rest);
	false ->
	    [Succ | filter(Pid, Rest)]
    end.

%% @doc get a peer form the cycloncache which is alive 
get_RndView(N,Cache) ->
     lists:sublist(Cache, N).     
    %[].

% @doc Check if change of failuredetector is necessary
update_failuredetector(Preds,Succs,PredsNew,SuccsNew) ->
    OldView=lists:usort(Preds++Succs),
    NewView=lists:usort(PredsNew++SuccsNew),
    %AktFD=length(failuredetector2:getmytargets()),
	%AktPS=length(lists:usort(Preds++Succs)),
	%io:format("RM ~p | Old ~p ~p~n",[self(),AktFD,AktPS]),
	case (NewView /= OldView) of
        true ->
            	
                %io:format("#~n"),
                %NewNodes = lists:usort(minus(PredsNew,Preds)++minus(SuccsNew,Succs)),
                %OldNodes = lists:usort(minus(Preds,PredsNew)++minus(Succs,SuccsNew)),
                NewNodes = util:minus(NewView,OldView),
                OldNodes = util:minus(OldView,NewView),
                %io:format("~p : in: ~p | out: ~p~n",[self(),length(NewNodes),length(OldNodes)]),
                %io:format("~p : in: ~p | out: ~p~n",[self(),NewNodes,OldNodes]),
                failuredetector2:unsubscribe([node:pidX(Node) || Node <- OldNodes]),
                
                failuredetector2:subscribe([node:pidX(Node) || Node <- NewNodes]);
                
        		
		false ->
            	NewNodes = util:minus(NewView,OldView),
                OldNodes = util:minus(OldView,NewView),
              	ok
	end,
    NewAktFD=length(failuredetector2:getmytargets()),
    NewAktPS=length(NewView),
    case (NewAktFD == NewAktPS) of 
        true ->
            %io:format("RM ~p | New ~p ~p~n",[self(),NewAktFD,NewAktPS]),
            ok;
        false ->
             log:log(error,"[ RM | ~p]  : in: ~p | out: ~p",[self(),NewNodes,OldNodes]),
             log:log(error,"[ RM | ~p] ~p ~p~n~p",[self(),NewAktFD,NewAktPS,{Preds,Succs,PredsNew,SuccsNew}])
	end.
             
             
             
           
	
% @doc informed the cs_node for new [succ|pred] if necessary
update_cs_node(PredsNew,SuccsNew,AktPred,AktSucc,ShuffelBuddy) ->
    %io:format("UCN: ~p ~n",[{PredsNew,SuccsNew,ShuffelBuddy,AktPred,AktSucc}]),
     case has_changed(PredsNew,ShuffelBuddy,AktPred) of
     	false ->
            NewAktPred = AktPred;
         {ok, NewAktPred} ->
             %io:format("RM ~p, Send P: ~p to Node~n",[self(),{NewAktPred}]),
             ring_maintenance:update_pred(NewAktPred)
     end,
     case has_changed(SuccsNew,ShuffelBuddy,AktSucc) of
     	false ->
            NewAktSucc = AktSucc;
         {ok, NewAktSucc} ->
             %io:format("RM ~p, Send S: ~p to Node~n",[self(),{NewAktSucc}]),
             ring_maintenance:update_succ(NewAktSucc)
     end,
     %io:format("~p~n",[{NewAktPred,NewAktSucc}]),
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
new_interval(Preds,Succs,PNew,SNew,Interval) ->
			case (Preds++Succs==PNew++SNew) of
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






%% @doc has_changed(CurrentView, ShuffleBuddy, LastReported)
-spec(has_changed/3 :: (list(), node:node_type(), node:node_type()) -> {ok, node:node_type()} | false).
has_changed([ShuffleBuddy | _], ShuffleBuddy, ShuffleBuddy) ->
	false;
has_changed([ShuffleBuddy | _], ShuffleBuddy, _) ->
	{ok, ShuffleBuddy};
has_changed(_A, _B, _C) ->
    %io:format(": ~p~n", [{A,B,C}]),
	false.


get_cyclon_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cyclon).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc starts ring maintenance
start(InstanceId, Sup) ->
    process_dictionary:register_process(InstanceId, ring_maintenance, self()),
    dn_cache:subscribe(),
   	log:log(info,"[ RM ~p ] starting ring maintainer T-MAN", [self()]),
    Sup ! start_done,
    start().

% @private
get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), ring_maintenance).
