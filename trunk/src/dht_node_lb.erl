%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    dht_node_lb.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Load balancing
%%% @end
%%% Created : 26 Mar 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(dht_node_lb).

-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new/0, balance_load/1, check_balance/3, get_middle_key/1, move_load/3, 
	 get_loadbalance_flag/1, reset_loadbalance_flag/1]).

-record(lb, {loadbalance_flag, reset_ref, last_keys}).
-type lb() :: #lb{}.

-spec new() -> lb().
new() ->
    ResetRef=comm:send_local_after(loadBalanceInterval(), self(), {reset_loadbalance_flag}),
    #lb{loadbalance_flag=true, reset_ref=ResetRef, last_keys=gb_sets:new()}.

balance_load(State) ->
    RT = dht_node_state:get(State, rt),
    Fingers = ?RT:to_pid_list(RT),
    lists:foreach(fun(Node) -> comm:send(Node, {get_load, comm:this()}) end, Fingers),    
    comm:send_local_after(loadBalanceInterval(), self(), {stabilize_loadbalance}).

check_balance(State, Source_PID, Load) ->
    MyLoad = dht_node_state:get(State, load),
    if
	(MyLoad * 2 < Load) andalso (Load > 1) ->
	    comm:send(Source_PID, {get_middle_key, comm:this()}),
	    ok;
	true ->
	    ok
    end.

get_middle_key(State) ->
    LB = dht_node_state:get(State, lb),
    AmLoadbalancing = get_loadbalance_flag(LB),
    LastKeys = last_keys(LB),
    Load = dht_node_state:get(State, load),
    if
	AmLoadbalancing orelse (Load < 20) ->
	    {nil, State};
	true ->
	    %Keys = gb_trees:keys(dht_node_state:get_data(State)),
	    %Middle = length(Keys) div 2 + 1,
	    %lists:nth(Middle, Keys),
	    MiddleKey = ?DB:get_middle_key(dht_node_state:get(State, db)),
	    IsReservedKey = gb_sets:is_element(MiddleKey, LastKeys),
	    if
		IsReservedKey ->
		    {nil, State};
		true ->
		    NewLB = add_reserved_key(MiddleKey, set_loadbalance_flag(LB)),
		    {MiddleKey, dht_node_state:set_lb(State, NewLB)}
	    end
    end.

move_load(State, _, nil) ->
    State;

move_load(State, _, NewId) ->
    cancel_reset(dht_node_state:get(State, lb)),
    Succ = dht_node_state:get(State, succ_pid),
    Pred = dht_node_state:get(State, pred),
    % TODO: needs to be fixed
    drop_data(State),
    OldIdVersion = node:id_version(dht_node_state:get(State, node)),
    idholder:set_id(NewId, OldIdVersion + 1),
    comm:send_local(self() , {kill}),
    comm:send(Succ, {pred_left, Pred}),
    PredPid = dht_node_state:get(State, pred_pid),
    comm:send(PredPid, {succ_left, dht_node_state:get(State, node)}),
    State.

drop_data(State) ->
    comm:send(dht_node_state:get(State, succ_pid), {drop_data, ?DB:get_data(dht_node_state:get(State, db)), comm:this()}),
    receive
	{drop_data_ack} ->
	    ok
    after 
	10000 ->
	    drop_data(State)
    end.
    
reset_loadbalance_flag(State) ->
    LB = dht_node_state:get(State, lb),
    NewLB = LB#lb{loadbalance_flag=false},
    dht_node_state:set_lb(State, NewLB).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%filterData(Dump) ->
%    {Local, Remote}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_loadbalance_flag(#lb{loadbalance_flag=Bool}) ->
    Bool;

get_loadbalance_flag(State) ->
    get_loadbalance_flag(dht_node_state:get(State, lb)).

set_loadbalance_flag(LB) ->
    ResetRef=comm:send_local_after(loadBalanceFlagResetInterval(), self(), {reset_loadbalance_flag}),
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

%% @doc interval between two load balance rounds
%% @spec loadBalanceInterval() -> integer() | failed
loadBalanceInterval() ->
    config:read(load_balance_interval).

%% @doc interval between two load balance rounds
%% @spec loadBalanceStartupInterval() -> integer() | failed
loadBalanceStartupInterval() ->
    config:read(load_balance_startup_interval).

%% @doc interval between two flag reset events
%% @spec loadBalanceFlagResetInterval() -> integer() | failed
loadBalanceFlagResetInterval() ->
    config:read(load_balance_flag_reset_interval).
