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

-export([start_link/1, check_config/0]).

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

%% @doc Sends a message to the remote node's ring_maintenance process asking for
%%      it list of successors.
-spec get_successorlist(cs_send:mypid()) -> ok.
get_successorlist(RemoteDhtNodePid) ->
    cs_send:send_to_group_member(RemoteDhtNodePid, ring_maintenance, {get_succlist, cs_send:this()}).

%% @doc Sends a message to the remote node's ring_maintenance process notifying
%%      it of a new predecessor.
-spec notify_new_pred(cs_send:mypid(), node:node_type()) -> ok.
notify_new_pred(RemoteDhtNodePid, NewPred) ->
    cs_send:send_to_group_member(RemoteDhtNodePid, ring_maintenance, {notify_new_pred, NewPred}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%set info for dht_node
on({init, NewId, NewMe, NewPred, NewSuccList, _DHTNode}, {uninit, TriggerState}) ->
    rm_beh:update_preds_and_succs([NewPred], NewSuccList),
    get_successorlist(node:pidX(hd(NewSuccList))),
    fd:subscribe([node:pidX(Node) || Node <- [NewPred | NewSuccList]]),
    {NewId, NewMe, NewPred, NewSuccList, TriggerState};

on(_, {uninit, _TriggerState} = State) ->
    State;

on({get_succlist, Source_Pid}, {_Id, Me, _Pred, Succs, _TriggerState} = State) ->
    % note: Source_Pid can be local or remote here
    % see get_successorlist/1
    cs_send:send(Source_Pid, {get_succlist_response, Me, Succs}),
    State;

on({get_predlist, Pid}, {_Id, _Me, Pred, _Succs, _TriggerState} = State) ->
    cs_send:send_local(Pid, {get_predlist_response, [Pred]}),
    State;

on({stabilize}, {Id, Me, Pred, [] = Succs, TriggerState}) -> % new stabilization interval
    {Id, Me, Pred, Succs, trigger:next(TriggerState)};

on({stabilize}, {Id, Me, Pred, Succs, TriggerState}) -> % new stabilization interval
    cs_send:send(node:pidX(hd(Succs)), {get_node_details, cs_send:this(), [pred]}),
    {Id, Me, Pred, Succs, trigger:next(TriggerState)};

on({get_node_details_response, NodeDetails}, {Id, Me, Pred, Succs, TriggerState} = State)  ->
    SuccsPred = node_details:get(NodeDetails, pred),
    case node:is_valid(SuccsPred) of
        true ->
            case util:is_between_stab(Id, node:id(SuccsPred), node:id(hd(Succs))) of
                true ->
                    get_successorlist(node:pidX(SuccsPred)),
                    rm_beh:update_preds_and_succs([Pred], [SuccsPred]),
                    fd:subscribe(node:pidX(SuccsPred)),
                    {Id, Me, Pred, [SuccsPred | Succs], TriggerState};
                false ->
                    get_successorlist(node:pidX(hd(Succs))),
                    State
            end;
        false ->
            get_successorlist(node:pidX(hd(Succs))),
            State
    end;

on({get_succlist_response, Succ, SuccsSuccList}, {Id, Me, Pred, Succs, TriggerState}) ->
    NewSuccs = util:trunc(merge([Succ | SuccsSuccList], Succs, Id), succListLength()),
    %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
    notify_new_pred(node:pidX(hd(NewSuccs)), Me),
    rm_beh:update_preds_and_succs([Pred], NewSuccs),
    fd:subscribe([node:pidX(Node) || Node <- NewSuccs]),
    {Id, Me, Pred, NewSuccs, TriggerState};

on({notify_new_pred, NewPred}, {Id, Me, Pred, Succs, TriggerState} = State) ->
    case node:is_valid(Pred) of
        true ->
            case util:is_between_stab(node:id(Pred), node:id(NewPred), Id) of
                true ->
                    rm_beh:update_preds_and_succs([NewPred], Succs),
                    fd:subscribe(node:pidX(NewPred)),
                    {Id, Me, NewPred, Succs, TriggerState};
                false ->
                    State
            end;
        false ->
            rm_beh:update_preds_and_succs([NewPred], Succs),
            fd:subscribe(node:pidX(NewPred)),
            {Id, Me, NewPred, Succs, TriggerState}
    end;

on({notify_new_succ, _NewSucc}, State) ->
    %% @TODO use the new successor info
    State;

on({crash, DeadPid}, {Id, Me, Pred, Succs, TriggerState})  ->
    case node:is_valid(Pred) andalso (DeadPid =/= node:pidX(Pred)) of
        true ->
            {Id, Me, Pred, filter(DeadPid, Succs), TriggerState};
        false ->
            {Id, Me, node:null(), filter(DeadPid, Succs), TriggerState}
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
