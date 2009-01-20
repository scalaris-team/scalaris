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
	 get_successorlist/0, succ_left/1, pred_left/1, 
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
  	    failuredetector2:subscribe([node:pidX(Node) || Node <- [NewPred | NewSuccList]]),
	    CSNode ! {init_done},
	    loop(NewId, NewMe, [NewPred], NewSuccList)
    end.

loop(Id, Me, Preds, Succs) ->
    receive
	{stabilize} -> % new stabilization interval
        %io:format("-"),
		{Pred,Succ} =get_safe_pred_succ(Preds,Succs,Me),
        RndView=get_RndView(Me),
        cs_send:send_to_group_member(node:pidX(Succ), ring_maintenance, {rm_buffer,cs_send:this(),Succs++Preds++[Me]++RndView}),
    	cs_send:send_to_group_member(node:pidX(Pred), ring_maintenance, {rm_buffer,cs_send:this(),Succs++Preds++[Me]++RndView}),
	    loop(Id, Me, Preds, Succs);
    {rm_buffer,Q,Buffer_q} ->
        RndView=get_RndView(Me),
        cs_send:send(Q,{rm_buffer_response,Succs++Preds++[Me]++RndView}),
        Buffer=merge(Succs++Preds,Buffer_q,node:id(Me)),
   	    SuccsNew=lists:sublist(Buffer, 1, 5),
        PredsNew=lists:sublist(lists:reverse(Buffer), 1, 5),
        {PNew,SNew} = update(Preds,Succs,PredsNew,SuccsNew),
        loop(Id, Me, PNew, SNew);
    {rm_buffer_response,Buffer_p} ->	
    	Buffer=merge(Succs++Preds,Buffer_p,node:id(Me)),
		SuccsNew=lists:sublist(Buffer, 1, 5),
        PredsNew=lists:sublist(lists:reverse(Buffer), 1, 5),
		{PNew,SNew} = update(Preds,Succs,PredsNew,SuccsNew),
        loop(Id, Me, PNew, SNew);	
	{crash, DeadPid} ->
        FN = lists:flatten(io_lib:format(" ~p | ~p crashed~n",[self(),DeadPid])),
        log:log2file("RM",FN),
        PredsNew = filter(DeadPid, Preds),
        SuccsNew = filter(DeadPid, Succs),
     	{PNew,SNew} = update(Preds,Succs,PredsNew,SuccsNew),
        
        loop(Id, Me, PNew, SNew);
	    
	{'$gen_cast', {debug_info, Requestor}} ->
	    Requestor ! {debug_info_response, [{"pred", lists:flatten(io_lib:format("~p", [Preds]))}, 
					       {"succs", lists:flatten(io_lib:format("~p", [Succs]))}]},
	    loop(Id, Me, Preds, Succs);
    {get_successorlist, Pid} ->
	    Pid ! {get_successorlist_response, Succs},
	    loop(Id, Me, Preds, Succs);
	X ->
	    io:format("@rm_tman unknown message ~p~n", [X]),
	    loop(Id, Me, Preds, Succs)
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
    Larger  = util:uniq(lists:sort(Order, [X || X <- MergedList, node:id(X) >  Id])),
    Equal   = util:uniq(lists:sort(Order, [X || X <- MergedList, node:id(X) == Id])),
    Smaller = util:uniq(lists:sort(Order, [X || X <- MergedList, node:id(X) <  Id])),
    A = lists:append([Larger, Smaller]),
    case A of
        []	-> Equal;
        _	-> A
    end.

filter(_Pid, []) ->
    [];
filter(Pid, [Succ | Rest]) ->
    case Pid == node:pidX(Succ) of
	true ->
	    filter(Pid, Rest);
	false ->
	    [Succ | filter(Pid, Rest)]
    end.

%% @doc get a peer form the cycloncache which is alive 
get_RndView(Node) ->
    cs_send:send_to_group_member(node:pidX(Node),cyclon,{get_youngest_element,self()}),
    receive
        {cache,_,_,RndView} ->
            RndView
    end.
    
update(Preds,Succs,PredsNew,SuccsNew) ->
    
    case ((PredsNew /= Preds) or (SuccsNew /= Succs )) of
        	true ->
                
                NewNodes = util:uniq(minus(PredsNew,Preds)++minus(SuccsNew,Succs)),
                OldNodes = util:uniq(minus(Preds,PredsNew)++minus(Succs,SuccsNew)),
               
                
                failuredetector2:subscribe([node:pidX(Node) || Node <- NewNodes]),
               	failuredetector2:unsubscribe([node:pidX(Node) || Node <- OldNodes]);
            false ->
              	ok
	end,
  	
    
		case (((PredsNew /= [])   ) andalso (hd(PredsNew) /= hd(Preds)) ) of
			true ->
        		PNew = PredsNew,        
				ring_maintenance:update_pred(hd(PredsNew));
			false ->
            	case (PredsNew /= Preds) and (PredsNew /= []) of
                    true ->
                        PNew = PredsNew;
                    false ->
    					PNew= Preds
				end
		end,
    
    	case (((SuccsNew /= [])  ) andalso (hd(SuccsNew) /= hd(Succs)) ) of
			true ->
                SNew = SuccsNew,
				ring_maintenance:update_succ(hd(SuccsNew));
			false ->
				case (SuccsNew /= Succs) and (SuccsNew /= []) of
                    true ->
                        SNew = SuccsNew; 
                    false ->
    					SNew= Succs
				end
		end,
    {PNew,SNew}.

get_safe_pred_succ(Preds,Succs,Me) ->
    case (Preds == []) or (Succs == []) of
        true ->
    		Buffer = merge(Preds ++ Succs, get_RndView(Me),node:id(Me)),
            case Buffer == [] of
                false ->
		    		SuccsNew=lists:sublist(Buffer, 1, 5),
    				PredsNew=lists:sublist(lists:reverse(Buffer), 1, 5),
	 				{hd(PredsNew), hd(SuccsNew)};
                true ->
                    io:format("NYI~n", []),
                    {Me, Me}
            end;
        false ->
	 		{hd(Preds), hd(Succs)}
    end.

minus([],_N) ->
    [];
minus([H|T],N) ->
    case is_element(H,N) of
 	true -> 
	    minus(T,N);
	false ->
	    [H]++minus(T,N)
    end.

is_element(Element, Cache) ->
    lists:any(fun(SomeElement) -> (Element==SomeElement) end, Cache).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc starts ring maintenance
start(InstanceId, Sup) ->
    process_dictionary:register_process(InstanceId, ring_maintenance, self()),
    io:format("[ I | RM     | ~p ] starting ring maintainer T-MAN~n", [self()]),
    timer:send_interval(config:stabilizationInterval(), self(), {stabilize}),
    Sup ! start_done,
    start().

% @private
get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), ring_maintenance).
