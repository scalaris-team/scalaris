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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Chord-like ring maintenance
%% @end
%% @version $Id$
-module(rm_chord).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behavior(rm_beh).

-type(state() :: trigger:state()).

-include("rm_beh.hrl").

% accepted messages of an initialized rm_chord process in addition to rm_loop
-type(custom_message() ::
    {get_succlist, Source_Pid::comm:mypid()} |
    {stabilize} |
    {get_node_details_response, NodeDetails::node_details:node_details()} |
    {get_succlist_response, Succ::node:node_type(), SuccsSuccList::nodelist:non_empty_snodelist()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initialises the state when rm_loop receives an init_rm message.
init(Table, Me, Pred, Succ) ->
    Trigger = config:read(ringmaintenance_trigger),
    TriggerState = trigger:init(Trigger, fun stabilizationInterval/0, stabilize),
    NewTriggerState = trigger:now(TriggerState),
    Neighborhood = nodelist:new_neighborhood(Pred, Me, Succ),
    rm_loop:update_neighbors(Table, Neighborhood),
    get_successorlist(node:pidX(Succ)),
    NewTriggerState.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when the module is fully initialized.
on({get_succlist, Source_Pid}, TriggerState, NeighbTable) ->
    Neighborhood = rm_loop:get_neighbors(NeighbTable),
    comm:send(Source_Pid, {get_succlist_response,
                           nodelist:node(Neighborhood),
                           nodelist:succs(Neighborhood)}),
    TriggerState;

on({stabilize}, TriggerState, NeighbTable) ->
    % new stabilization interval
    Neighborhood = rm_loop:get_neighbors(NeighbTable),
    case nodelist:has_real_succ(Neighborhood) of
        true -> comm:send(node:pidX(nodelist:succ(Neighborhood)),
                          {get_node_details, comm:this(), [pred]});
        _    -> ok
    end,
    trigger:next(TriggerState);

% got node_details from our successor
on({get_node_details_response, NodeDetails}, TriggerState, NeighbTable)  ->
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    SuccsPred = node_details:get(NodeDetails, pred),
    NewNeighborhood = nodelist:add_nodes(OldNeighborhood, [SuccsPred],
                                         predListLength(), succListLength()),
    rm_loop:update_neighbors(NeighbTable, NewNeighborhood),
    get_successorlist(node:pidX(nodelist:succ(NewNeighborhood))),
    TriggerState;

on({get_succlist_response, Succ, SuccsSuccList}, TriggerState, NeighbTable) ->
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    NewNeighborhood = nodelist:add_nodes(OldNeighborhood, [Succ | SuccsSuccList],
                                         predListLength(), succListLength()),
    rm_loop:update_neighbors(NeighbTable, NewNeighborhood),
    %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
    rm_loop:notify_new_pred(node:pidX(nodelist:succ(NewNeighborhood)),
                            nodelist:node(NewNeighborhood)),
    TriggerState.

new_pred(TriggerState, NeighbTable, NewPred) ->
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    NewNeighborhood = nodelist:add_node(OldNeighborhood, NewPred,
                                        predListLength(), succListLength()),
    rm_loop:update_neighbors(NeighbTable, NewNeighborhood),
    TriggerState.

new_succ(TriggerState, NeighbTable, NewSucc) ->
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    NewNeighborhood = nodelist:add_node(OldNeighborhood, NewSucc,
                                        predListLength(), succListLength()),
    rm_loop:update_neighbors(NeighbTable, NewNeighborhood),
    TriggerState.

remove_pred(TriggerState, NeighbTable, OldPred, PredsPred) ->
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    NewNbh1 = nodelist:remove(OldPred, OldNeighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, PredsPred, predListLength(), succListLength()),
    rm_loop:update_neighbors(NeighbTable, NewNbh2),
    TriggerState.

remove_succ(TriggerState, NeighbTable, OldSucc, SuccsSucc) ->
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    NewNbh1 = nodelist:remove(OldSucc, OldNeighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, SuccsSucc, predListLength(), succListLength()),
    rm_loop:update_neighbors(NeighbTable, NewNbh2),
    TriggerState.

updated_node(TriggerState, _NeighbTable, _OldMe, _NewMe) ->
    NewTriggerState = trigger:now(TriggerState), % inform neighbors
    NewTriggerState.

leave(_State, _NeighbTable) -> ok.

% failure detector reported dead node
crashed_node(TriggerState, NeighbTable, DeadPid) ->
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    NewNeighborhood = nodelist:remove(DeadPid, OldNeighborhood),
    rm_loop:update_neighbors(NeighbTable, NewNeighborhood),
    TriggerState.

% dead-node-cache reported dead node to be alive again
zombie_node(TriggerState, NeighbTable, Node) ->
    % this node could potentially be useful as it has been in our state before
    OldNeighborhood = rm_loop:get_neighbors(NeighbTable),
    NewNeighborhood = nodelist:add_node(OldNeighborhood, Node,
                                        predListLength(), succListLength()),
    rm_loop:update_neighbors(NeighbTable, NewNeighborhood),
    TriggerState.

get_web_debug_info(_State, _NeighbTable) -> [].

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

%% @doc Sends a message to the remote node's ring_maintenance process asking for
%%      its list of successors.
-spec get_successorlist(comm:mypid()) -> ok.
get_successorlist(RemoteDhtNodePid) ->
    comm:send_to_group_member(RemoteDhtNodePid, ring_maintenance, {get_succlist, comm:this()}).

%% @doc the length of the successor list
-spec predListLength() -> pos_integer().
predListLength() -> 1.

%% @doc the length of the successor list
-spec succListLength() -> pos_integer().
succListLength() -> config:read(succ_list_length).

%% @doc the interval between two stabilization runs Max
-spec stabilizationInterval() -> pos_integer().
stabilizationInterval() -> config:read(stabilization_interval_max).
