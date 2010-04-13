% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Chord-like ring maintenance
%%% @end
%% @version $Id$
-module(rm_chord).
-author('schuett@zib.de').
-vsn('$Id$ ').

-include("scalaris.hrl").

-behavior(rm_beh).
-behavior(gen_component).

-export([init/1,on/2]).

-export([start_link/1,
	 get_successorlist/1, succ_left/1, pred_left/1,
	 notify/1, update_succ/1, update_pred/1,
	 get_predlist/0, check_config/0]).

% unit testing
-export([merge/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a chord-like ring maintenance process, registers it with the
%%      process dictionary and returns its pid for use by a supervisor.
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    Trigger = config:read(ringmaintenance_trigger),
    gen_component:start_link(?MODULE, Trigger, [{register, InstanceId, ring_maintenance}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(module()) -> {uninit, TriggerState::trigger:state()}.
init(Trigger) ->
    log:log(info,"[ RM ~p ] starting ring maintainer chord~n", [cs_send:this()]),
    TriggerState = trigger:init(Trigger, fun stabilizationInterval/0, stabilize),
    cs_send:send_local(get_cs_pid(), {init_rm,self()}),
    TriggerState2 = trigger:next(TriggerState),
    {uninit, TriggerState2}.

get_successorlist(Source) ->
    cs_send:send_local(get_pid(), {get_successorlist, Source, Source}).

get_predlist() ->
    log:log(error, "[ RM-CHORD] OLD FUNCTION use broke with gen_component"),
    cs_send:send_local(get_pid(), {get_predlist, self()}),
    receive
        {get_predlist_response, PredList} ->
            PredList
    end.

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

notify(Pred) ->
    cs_send:send_local(get_pid(), {notify, Pred}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%set info for dht_node
on({init, NewId, NewMe, NewPred, NewSuccList, _DHTNode}, {uninit, TriggerState}) ->
    rm_beh:update_succ_and_pred(NewPred, hd(NewSuccList)),
    cs_send:send(node:pidX(hd(NewSuccList)), {get_succ_list, cs_send:this()}),
    fd:subscribe([node:pidX(Node) || Node <- [NewPred | NewSuccList]]),
    {NewId, NewMe, NewPred, NewSuccList, TriggerState};

on(_, {uninit, _TriggerState} = State) ->
    State;

on({get_successorlist, Pid}, {_Id, _Me, _Pred, Succs, _TriggerState} = State) ->
    cs_send:send_local(Pid, {get_successorlist_response, Succs}),
    State;

on({get_successorlist, Pid, S}, {_Id, _Me, _Pred, Succs, _TriggerState} = State) ->
    cs_send:send_local(Pid, {get_successorlist_response, Succs, S}),
    State;

on({get_predlist, Pid}, {_Id, _Me, Pred, _Succs, _TriggerState} = State) ->
    cs_send:send_local(Pid, {get_predlist_response, [Pred]}),
    State;

on({stabilize}, {Id, Me, Pred, Succs, TriggerState}) -> % new stabilization interval
    case Succs of
        [] ->
            ok;
        _  ->
            cs_send:send(node:pidX(hd(Succs)), {get_pred, cs_send:this()})
    end,
    NewTriggerState = trigger:next(TriggerState),
    {Id, Me, Pred, Succs, NewTriggerState};

on({get_pred_response, SuccsPred}, {Id, Me, Pred, Succs, TriggerState} = State)  ->
    case node:is_null(SuccsPred) of
        false ->
            case util:is_between_stab(Id, node:id(SuccsPred), node:id(hd(Succs))) of
                true ->
                    cs_send:send(node:pidX(SuccsPred), {get_succ_list, cs_send:this()}),
                    rm_beh:update_succ_and_pred(Pred, SuccsPred),
                    fd:subscribe(node:pidX(SuccsPred)),
                    {Id, Me, Pred, [SuccsPred | Succs], TriggerState};
                false ->
                    cs_send:send(node:pidX(hd(Succs)), {get_succ_list, cs_send:this()}),
                    State
            end;
        true ->
            cs_send:send(node:pidX(hd(Succs)), {get_succ_list, cs_send:this()}),
            State
    end;

on({get_succ_list_response, Succ, SuccsSuccList}, {Id, Me, Pred, Succs, TriggerState}) ->
    NewSuccs = util:trunc(merge([Succ | SuccsSuccList], Succs, Id), succListLength()),
    %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
    cs_send:send(node:pidX(hd(NewSuccs)), {notify, Me}),
    rm_beh:update_succ_and_pred(Pred, hd(NewSuccs)),
    fd:subscribe([node:pidX(Node) || Node <- NewSuccs]),
    {Id, Me, Pred, NewSuccs, TriggerState};

on({notify, NewPred}, {Id, Me, Pred, Succs, TriggerState} = State)  ->
    case node:is_null(Pred) of
        true ->
            rm_beh:update_succ_and_pred(NewPred, hd(Succs)),
            fd:subscribe(node:pidX(NewPred)),
            {Id, Me, NewPred, Succs, TriggerState};
        false ->
            case util:is_between_stab(node:id(Pred), node:id(NewPred), Id) of
                true ->
                    rm_beh:update_succ_and_pred(NewPred, hd(Succs)),
                    fd:subscribe(node:pidX(NewPred)),
                    {Id, Me, NewPred, Succs, TriggerState};
                false ->
                    State
            end
    end;

on({crash, DeadPid}, {Id, Me, Pred, Succs, TriggerState})  ->
    case node:is_null(Pred) orelse DeadPid == node:pidX(Pred) of
        true ->
            {Id, Me, node:null(), filter(DeadPid, Succs), TriggerState};
        false ->
            {Id, Me, Pred, filter(DeadPid, Succs), TriggerState}
    end;

on({'$gen_cast', {debug_info, Requestor}}, {_Id, _Me, Pred, Succs, _TriggerState} = State)  ->
    cs_send:send_local(Requestor,
                       {debug_info_response,
                        [{"pred", lists:flatten(io_lib:format("~p", [Pred]))},
                         {"succs", lists:flatten(io_lib:format("~p", [Succs]))}]}),
    State;

on(_, _State) ->
    unknown_event.

%% @doc Checks whether config parameters of the rm_chord process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_atom(ringmaintenance_trigger) and
    
    config:is_integer(stabilization_interval_max) and
    config:is_greater_than(stabilization_interval_max, 0) and
    
    config:is_integer(succ_list_length) and
    config:is_greater_than_equal(succ_list_length, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
    lists:append([Larger, Smaller, Equal]).

filter(_Pid, []) ->
    [];
filter(Pid, [Succ | Rest]) ->
    case Pid == node:pidX(Succ) of
	true ->
	    filter(Pid, Rest);
	false ->
	    [Succ | filter(Pid, Rest)]
    end.



% @private
get_pid() ->
    process_dictionary:get_group_member(ring_maintenance).

% get Pid of assigned dht_node
get_cs_pid() ->
    process_dictionary:get_group_member(dht_node).

%% @doc the length of the successor list
%% @spec succListLength() -> integer() | failed
succListLength() ->
    config:read(succ_list_length).

%% @doc the interval between two stabilization runs Max
%% @spec stabilizationInterval() -> integer() | failed
stabilizationInterval() ->
    config:read(stabilization_interval_max).
