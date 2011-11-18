% @copyright 2008-2011 Zuse Institute Berlin

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

-type(state() :: {Neighbors    :: nodelist:neighborhood(),
                  TriggerState :: trigger:state()}).

% accepted messages of an initialized rm_chord process in addition to rm_loop
-type(custom_message() ::
    {rm_trigger} |
    {rm, get_succlist, Source_Pid::comm:mypid()} |
    {{get_node_details_response, NodeDetails::node_details:node_details()}, rm} |
    {rm, get_succlist_response, Succ::node:node_type(), SuccsSuccList::nodelist:non_empty_snodelist()}).

-define(SEND_OPTIONS, [{channel, prio}]).

% note include after the type definitions for erlang < R13B04!
-include("rm_beh.hrl").

get_neighbors({Neighbors, _TriggerState}) ->
    Neighbors.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initialises the state when rm_loop receives an init_rm message.
init(Me, Pred, Succ) ->
    Trigger = config:read(ringmaintenance_trigger),
    TriggerState = trigger:init(Trigger, fun stabilizationInterval/0, rm_trigger),
    NewTriggerState = trigger:now(TriggerState),
    Neighborhood = nodelist:new_neighborhood(Pred, Me, Succ),
    get_successorlist(node:pidX(Succ)),
    {Neighborhood, NewTriggerState}.

unittest_create_state(Neighbors) ->
    Trigger = config:read(ringmaintenance_trigger),
    TriggerState = trigger:init(Trigger, fun stabilizationInterval/0, rm_trigger),
    {Neighbors, TriggerState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when the module is fully initialized.
on({rm_trigger}, {Neighborhood, TriggerState}) ->
    % new stabilization interval
    case nodelist:has_real_succ(Neighborhood) of
        true -> comm:send(node:pidX(nodelist:succ(Neighborhood)),
                          {get_node_details, comm:this_with_cookie(rm), [pred]},
                          ?SEND_OPTIONS);
        _    -> ok
    end,
    {Neighborhood, trigger:next(TriggerState)};

on({rm, get_succlist, Source_Pid}, {Neighborhood, _TriggerState} = State) ->
    comm:send(Source_Pid, {rm, get_succlist_response,
                           nodelist:node(Neighborhood),
                           nodelist:succs(Neighborhood)},
              ?SEND_OPTIONS),
    State;

% got node_details from our successor
on({{get_node_details_response, NodeDetails}, rm},
   {OldNeighborhood, TriggerState})  ->
    SuccsPred = node_details:get(NodeDetails, pred),
    NewNeighborhood = nodelist:add_nodes(OldNeighborhood, [SuccsPred],
                                         predListLength(), succListLength()),
    get_successorlist(node:pidX(nodelist:succ(NewNeighborhood))),
    {NewNeighborhood, TriggerState};

on({rm, get_succlist_response, Succ, SuccsSuccList},
   {OldNeighborhood, TriggerState}) ->
    NewNeighborhood = nodelist:add_nodes(OldNeighborhood, [Succ | SuccsSuccList],
                                         predListLength(), succListLength()),
    %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
    rm_loop:notify_new_pred(node:pidX(nodelist:succ(NewNeighborhood)),
                            nodelist:node(NewNeighborhood)),
    {NewNeighborhood, TriggerState};

on(_, _State) -> unknown_event.

new_pred({OldNeighborhood, TriggerState}, NewPred) ->
    NewNeighborhood = nodelist:add_node(OldNeighborhood, NewPred,
                                        predListLength(), succListLength()),
    {NewNeighborhood, TriggerState}.

new_succ({OldNeighborhood, TriggerState}, NewSucc) ->
    NewNeighborhood = nodelist:add_node(OldNeighborhood, NewSucc,
                                        predListLength(), succListLength()),
    {NewNeighborhood, TriggerState}.

remove_pred({OldNeighborhood, TriggerState}, OldPred, PredsPred) ->
    NewNbh1 = nodelist:remove(OldPred, OldNeighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, PredsPred, predListLength(), succListLength()),
    {NewNbh2, TriggerState}.

remove_succ({OldNeighborhood, TriggerState}, OldSucc, SuccsSucc) ->
    NewNbh1 = nodelist:remove(OldSucc, OldNeighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, SuccsSucc, predListLength(), succListLength()),
    {NewNbh2, TriggerState}.

update_node({OldNeighborhood, TriggerState}, NewMe) ->
    NewNeighborhood = nodelist:update_node(OldNeighborhood, NewMe),
    NewTriggerState = trigger:now(TriggerState), % inform neighbors
    {NewNeighborhood, NewTriggerState}.

leave(_State) -> ok.

% failure detector reported dead node
crashed_node({OldNeighborhood, TriggerState}, DeadPid) ->
    NewNeighborhood = nodelist:remove(DeadPid, OldNeighborhood),
    {NewNeighborhood, TriggerState}.

% dead-node-cache reported dead node to be alive again
zombie_node({OldNeighborhood, TriggerState}, Node) ->
    % this node could potentially be useful as it has been in our state before
    NewNeighborhood = nodelist:add_node(OldNeighborhood, Node,
                                        predListLength(), succListLength()),
    {NewNeighborhood, TriggerState}.

get_web_debug_info(_State) -> [].

%% @doc Checks whether config parameters of the rm_chord process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_module(ringmaintenance_trigger) and
    
    config:cfg_is_integer(stabilization_interval_max) and
    config:cfg_is_greater_than(stabilization_interval_max, 0) and
    
    config:cfg_is_integer(succ_list_length) and
    config:cfg_is_greater_than(succ_list_length, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @private

%% @doc Sends a message to the remote node's dht_node process asking for
%%      its list of successors.
-spec get_successorlist(comm:mypid()) -> ok.
get_successorlist(RemoteDhtNodePid) ->
    comm:send(RemoteDhtNodePid, {rm, get_succlist, comm:this()}, ?SEND_OPTIONS).

%% @doc the length of the successor list
-spec predListLength() -> pos_integer().
predListLength() -> 1.

%% @doc the length of the successor list
-spec succListLength() -> pos_integer().
succListLength() -> config:read(succ_list_length).

%% @doc the interval between two stabilization runs Max
-spec stabilizationInterval() -> pos_integer().
stabilizationInterval() -> config:read(stabilization_interval_max).
