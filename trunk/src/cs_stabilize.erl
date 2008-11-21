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
%%% File    : stabilize.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Ring Stabilization
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_stabilize.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_stabilize).

-author('schuett@zib.de').
-vsn('$Id: cs_stabilize.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([notify/2, stabilize/1, stabilize2/3, stabilize/3, update_succ/2, update_failuredetector/1, succ_left/2, pred_left/2, update_range/3]).

%logging on
%-define(LOG(S, L), io:format(S, L)).
%logging off
-define(LOG(S, L), ok).

% first call
stabilize(State) ->
    timer:send_after(config:stabilizationInterval(), self(), {stabilize_ring}),
    cs_send:send(cs_state:succ_pid(State), {get_pred, cs_send:this()}).

% second call    
stabilize2(State, SuccsPred, Succ) ->
    Is_Null = node:is_null(SuccsPred),
    UpdatedState = cs_state:update_succ(State, Succ),
    if 
	not Is_Null ->
	    Is_Between = util:is_between_stab(cs_state:id(UpdatedState), node:id(SuccsPred), cs_state:succ_id(UpdatedState)),
	    if 
		Is_Between -> 
		    cs_send:send(node:pidX(SuccsPred), {get_succ_list, cs_send:this()}),
		    NewSuccList = [SuccsPred | cs_state:succ_list(UpdatedState)],
		    NewState = cs_state:set_succlist(UpdatedState, NewSuccList),
		    NewState;
		true ->
		    cs_send:send(cs_state:succ_pid(UpdatedState), {get_succ_list, cs_send:this()}),
		    UpdatedState
	    end;
	true ->
	    cs_send:send(cs_state:succ_pid(UpdatedState), {get_succ_list, cs_send:this()}),
	    UpdatedState
    end.

stabilize(State, Succ, Succ_SuccList) ->
    SuccList = util:trunc(cs_state:filterDeadNodes([Succ | Succ_SuccList], State), config:succListLength()),
    SuccListLength = length(SuccList),
    Limit = config:succListLength() / 2,
    if
	SuccListLength < Limit ->
	    cs_send:send(node:pidX(Succ), {get_succ_list, cs_send:this()});
	true ->
	    void
    end,
    cs_send:send(node:pidX(Succ), {notify, cs_state:me(State)}),
    cs_state:set_succlist(State, SuccList).


notify(State, P) -> 
    ?LOG("[ I | Node   | ~w ] notify ~w~n",[self(), P]),    
    ?LOG("[ I | Node   | ~w ] notify ~w~n",[self(), cs_state:pred(State)]),    
    IsOldNull = node:is_null(cs_state:pred(State)),
    if
	IsOldNull -> 
	    failuredetector:add_node(node:uniqueId(P), node:id(P), node:pidX(P)),
	    cs_state:update_pred(State, P);
	true ->
	    Pred_Id = cs_state:pred_id(State),
	    Id = cs_state:id(State),
	    P_Id = node:id(P),
	    Is_Between = util:is_between_stab(Pred_Id, P_Id, Id),
	    if
		Is_Between ->
		    failuredetector:add_node(node:uniqueId(P), node:id(P), node:pidX(P)),
		    cs_state:update_pred(State, P);
		true ->
		    State
	    end
    end.
	    
succ_left(State, []) ->
    State;
succ_left(State, SuccList) ->
    cs_state:set_succlist(State, SuccList).

pred_left(State, Pred) ->
    PredIsNull = node:is_null(Pred),
    if 
	PredIsNull ->
	    State;
	true ->
	    cs_state:update_pred(State, Pred)
    end.

update_succ(State, Succ) ->
    IsBetween = util:is_between(cs_state:id(State), node:id(Succ), cs_state:succ_id(State)),
    if
	IsBetween ->
	    SuccList = util:trunc([Succ | cs_state:succ_list(State)], config:succListLength()),
	    %failuredetector:add_node(node:uniqueId(Succ), node:id(), node:pidX(Succ)),
	    cs_state:set_succlist(State, SuccList);
	true ->
	    State
    end.

update_failuredetector(State) ->
    timer:send_after(config:failureDetectorUpdateInterval(), self(), {stabilize_failuredetector}),
    Nodes = util:uniq(lists:sort(cs_state:get_nodes(State))),
    failuredetector:set_nodes(Nodes).

update_range(State, _OldPred, NewPred) ->
    case node:is_null(NewPred) of
	true ->
	    State;
	false ->
	    {_From, To} = cs_state:get_my_range(State),
	    cs_state:set_my_range(State, {node:id(NewPred), To})
    end.
    
