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
-vsn('$Id$').

-include("scalaris.hrl").

-behavior(rm_beh).
-behavior(gen_component).

-export([init/1,on/2]).

-export([start_link/1, check_config/0]).

-type(state() :: {Id             :: ?RT:key(),
                  Neighborhood   :: nodelist:neighborhood(),
                  TriggerState   :: trigger:state()}
     | {uninit, QueuedMessages::[cs_send:message()], TriggerState :: trigger:state()}).

% accepted messages
-type(message() ::
    {init, Id::?RT:key(), Me::node_details:node_type(), Predecessor::node_details:node_type(), Successor::node:node_type()} |
    {get_succlist, Source_Pid::cs_send:mypid()} |
    {stabilize} |
    {get_node_details_response, NodeDetails::node_details:node_details()} |
    {get_succlist_response, Succ::node:node_type(), SuccsSuccList::nodelist:non_empty_nodelist()} |
    {notify_new_pred, Pred::node:node_type()} |
    {notify_new_succ, Succ::node:node_type()} |
    {crash, DeadPid::cs_send:mypid()} |
    {'$gen_cast', {debug_info, Requestor::cs_send:erl_local_pid()}}).

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
-spec init(module()) -> {uninit, QueuedMessages::[], TriggerState::trigger:state()}.
init(Trigger) ->
    log:log(info,"[ RM ~p ] starting ring maintainer chord~n", [cs_send:this()]),
    TriggerState = trigger:init(Trigger, fun stabilizationInterval/0, stabilize),
    cs_send:send_local(get_cs_pid(), {init_rm, self()}),
    TriggerState2 = trigger:next(TriggerState),
    {uninit, [], TriggerState2}.

%% @doc Sends a message to the remote node's ring_maintenance process asking for
%%      it list of successors.
-spec get_successorlist(cs_send:mypid()) -> ok.
get_successorlist(RemoteDhtNodePid) ->
    cs_send:send_to_group_member(RemoteDhtNodePid, ring_maintenance, {get_succlist, cs_send:this()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
%set info for dht_node
on({init, Id, Me, Predecessor, Successor}, {uninit, QueuedMessages, TriggerState}) ->
    Neighborhood = nodelist:new_neighborhood(Predecessor, Me, Successor),
    get_successorlist(node:pidX(Successor)),
    fd:subscribe(lists:usort([node:pidX(Predecessor), node:pidX(Successor)])),
    cs_send:send_queued_messages(QueuedMessages),
    {Id, Neighborhood, TriggerState};

on(Msg, {uninit, QueuedMessages, TriggerState}) ->
    {uninit, [Msg | QueuedMessages], TriggerState};

on({get_succlist, Source_Pid}, {_Id, Neighborhood, _TriggerState} = State) ->
    cs_send:send(Source_Pid, {get_succlist_response,
                              nodelist:node(Neighborhood),
                              nodelist:succs(Neighborhood)}),
    State;

on({stabilize}, {Id, Neighborhood, TriggerState}) ->
    % new stabilization interval
    case nodelist:has_real_succ(Neighborhood) of
        true ->
            cs_send:send(node:pidX(nodelist:succ(Neighborhood)),
                         {get_node_details, cs_send:this(), [pred]});
        _ -> ok
    end,
    {Id, Neighborhood, trigger:next(TriggerState)};

on({get_node_details_response, NodeDetails}, {Id, Neighborhood, TriggerState} = State)  ->
    SuccsPred = node_details:get(NodeDetails, pred),
    case node:is_valid(SuccsPred) of
        true ->
            % check if the predecessor of our successor is correct:
            SuccId = node:id(nodelist:succ(Neighborhood)),
            case intervals:in(node:id(SuccsPred), intervals:new('(', Id, SuccId, ')')) of
                true ->
                    get_successorlist(node:pidX(SuccsPred)),
                    NewNeighborhood = nodelist:add_nodes(Neighborhood, [SuccsPred], 1, succListLength()),
                    rm_beh:update_neighbors(NewNeighborhood),
                    fd:subscribe(node:pidX(SuccsPred)),
                    {Id, NewNeighborhood, TriggerState};
                false ->
                    get_successorlist(node:pidX(nodelist:succ(Neighborhood))),
                    State
            end;
        false ->
            get_successorlist(node:pidX(nodelist:succ(Neighborhood))),
            State
    end;

on({get_succlist_response, Succ, SuccsSuccList}, {Id, Neighborhood, TriggerState}) ->
    NewNeighborhood = nodelist:add_nodes(Neighborhood, [Succ | SuccsSuccList], 1, succListLength()),
    %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
    rm_beh:notify_new_pred(node:pidX(nodelist:succ(NewNeighborhood)), nodelist:node(NewNeighborhood)),
    rm_beh:update_neighbors(NewNeighborhood),
    % the predecessor might also have changed if the successor knew about a better predecessor
    OldPids = [nodelist:pred(Neighborhood)] ++ [node:pidX(Node) || Node <- nodelist:succs(Neighborhood)],
    NewPids = [nodelist:pred(NewNeighborhood)] ++ [node:pidX(Node) || Node <- nodelist:succs(NewNeighborhood)],
    fd:update_subscriptions(OldPids, NewPids),
    {Id, NewNeighborhood, TriggerState};

on({notify_new_pred, NewPred}, {Id, Neighborhood, TriggerState} = State) ->
    Pred = nodelist:pred(Neighborhood),
    % is the 'new predecessor' really our new predecessor?
    case intervals:in(node:id(NewPred), intervals:new('(', node:id(Pred), Id, ')')) of
        true ->
            NewNeighborhood = nodelist:add_nodes(Neighborhood, [NewPred], 1, succListLength()),
            rm_beh:update_neighbors(NewNeighborhood),
            fd:update_subscriptions([node:pidX(Pred)], [node:pidX(NewPred)]),
            {Id, NewNeighborhood, TriggerState};
        false ->
            State
    end;

on({notify_new_succ, _NewSucc}, State) ->
    %% @TODO use the new successor info
    State;

on({crash, DeadPid}, {Id, Neighborhood, TriggerState})  ->
    NewNeighborhood = nodelist:remove(DeadPid, Neighborhood),
    {Id, NewNeighborhood, TriggerState};

on({'$gen_cast', {debug_info, Requestor}}, {_Id, Neighborhood, _TriggerState} = State)  ->
    cs_send:send_local(Requestor,
                       {debug_info_response,
                        [{"self", lists:flatten(io_lib:format("~p", [nodelist:node(Neighborhood)]))},
                         {"preds", lists:flatten(io_lib:format("~p", [nodelist:preds(Neighborhood)]))},
                         {"succs", lists:flatten(io_lib:format("~p", [nodelist:succs(Neighborhood)]))}]}),
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
    config:is_greater_than(succ_list_length, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @private

%% @doc get Pid of assigned dht_node
-spec get_cs_pid() -> pid().
get_cs_pid() ->
    process_dictionary:get_group_member(dht_node).

%% @doc the length of the successor list
-spec succListLength() -> pos_integer().
succListLength() ->
    config:read(succ_list_length).

%% @doc the interval between two stabilization runs Max
-spec stabilizationInterval() -> pos_integer().
stabilizationInterval() ->
    config:read(stabilization_interval_max).
