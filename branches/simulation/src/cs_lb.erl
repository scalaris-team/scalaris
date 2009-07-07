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
%%% File    : cs_lb.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Load balancing
%%%
%%% Created : 26 Mar 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_lb).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("chordsharp.hrl").

-export([new/0, balance_load/1, check_balance/3, get_middle_key/1, move_load/3, 
	 get_loadbalance_flag/1, reset_loadbalance_flag/1]).

-record(lb, {loadbalance_flag, reset_ref, last_keys}).

new() ->
    ResetRef=cs_send:send_after(config:loadBalanceInterval(), self(), {reset_loadbalance_flag}),
    #lb{loadbalance_flag=true, reset_ref=ResetRef, last_keys=gb_sets:new()}.

balance_load(State) ->
    RT = cs_state:rt(State),
    Fingers = ?RT:to_pid_list(RT),
    lists:foreach(fun(Node) -> cs_send:send(Node, {get_load, cs_send:this()}) end, Fingers),    
    cs_send:send_after(config:loadBalanceInterval(), self(), {stabilize_loadbalance}).

check_balance(State, Source_PID, Load) ->
    MyLoad = ?DB:get_load(cs_state:get_db(State)),
    if
	(MyLoad * 2 < Load) and (Load > 1) ->
	    cs_send:send(Source_PID, {get_middle_key, cs_send:this()}),
	    ok;
	true ->
	    ok
    end.

get_middle_key(State) ->
    LB = cs_state:get_lb(State),
    AmLoadbalancing = get_loadbalance_flag(LB),
    LastKeys = last_keys(LB),
    Load = ?DB:get_load(cs_state:get_db(State)),
    if
	AmLoadbalancing or (Load < 20) ->
	    {nil, State};
	true ->
	    %Keys = gb_trees:keys(cs_state:get_data(State)),
	    %Middle = length(Keys) div 2 + 1,
	    %lists:nth(Middle, Keys),
	    MiddleKey = ?DB:get_middle_key(cs_state:get_db(State)),
	    IsReservedKey = gb_sets:is_element(MiddleKey, LastKeys),
	    if
		IsReservedKey ->
		    {nil, State};
		true ->
		    NewLB = add_reserved_key(MiddleKey, set_loadbalance_flag(LB)),
		    {MiddleKey, cs_state:set_lb(State, NewLB)}
	    end
    end.

move_load(State, _, nil) ->
    State;

move_load(State, _, NewId) ->
    cancel_reset(cs_state:get_lb(State)),
    Succ = cs_state:succ_pid(State),
    Pred = cs_state:pred(State),
    % TODO: needs to be fixed
    drop_data(State),
    cs_keyholder:set_key(NewId),
    PredIsNull = node:is_null(Pred),
    cs_send:send_local(self() , {kill}),
    cs_send:send(Succ, {pred_left, Pred}),
    if 
	not PredIsNull ->
	    PredPid = cs_state:pred_pid(State),
	    cs_send:send(PredPid, {succ_left, cs_state:succ_list(State)});
	true ->
	    void
    end,
    State.

drop_data(State) ->
    cs_send:send(cs_state:succ_pid(State), {drop_data, ?DB:get_data(cs_state:get_db(State)), cs_send:this()}),
    receive
	{drop_data_ack} ->
	    ok
    after 
	10000 ->
	    drop_data(State)
    end.
    
reset_loadbalance_flag(State) ->
    LB = cs_state:get_lb(State),
    NewLB = LB#lb{loadbalance_flag=false},
    cs_state:set_lb(State, NewLB).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%filterData(Dump) ->
%    {Local, Remote}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_loadbalance_flag(#lb{loadbalance_flag=Bool}) ->
    Bool;

get_loadbalance_flag(State) ->
    get_loadbalance_flag(cs_state:get_lb(State)).

set_loadbalance_flag(LB) ->
    ResetRef=cs_send:send_after(config:loadBalanceFlagResetInterval(), self(), {reset_loadbalance_flag}),
    LB#lb{loadbalance_flag=true, reset_ref=ResetRef}.

cancel_reset(#lb{reset_ref=ResetRef}) ->
    erlang:cancel_timer(ResetRef),
    receive
	{reset_loadbalance_flag} ->
	    ok
    after 50 ->
	    ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

last_keys(#lb{last_keys=LastKeys}) ->
    LastKeys.

add_reserved_key(Key, #lb{last_keys=LastKeys}=LB) ->
    LB#lb{last_keys=gb_sets:add_element(Key, LastKeys)}.
