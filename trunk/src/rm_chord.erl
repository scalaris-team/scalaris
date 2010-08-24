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
-behavior(gen_component).

-export([start_link/1]).
-export([init/1, on/2, leave/0]).

-export([check_config/0]).

-type(state() :: {Neighborhood   :: nodelist:neighborhood(),
                  TriggerState   :: trigger:state()}
     | {uninit, QueuedMessages::msg_queue:msg_queue(), TriggerState :: trigger:state()}).

% accepted messages
-type(message() ::
    {init_rm, Me::node:node_type(), Predecessor::node:node_type(), Successor::node:node_type()} |
    {get_succlist, Source_Pid::comm:mypid()} |
    {stabilize} |
    {get_node_details_response, NodeDetails::node_details:node_details()} |
    {get_succlist_response, Succ::node:node_type(), SuccsSuccList::nodelist:non_empty_snodelist()} |
    {notify_new_pred, Pred::node:node_type()} |
    {notify_new_succ, Succ::node:node_type()} |
    {leave, SourcePid::comm:erl_local_pid()} |
    {pred_left, OldPred::node:node_type(), PredsPred::node:node_type()} |
    {succ_left, OldSucc::node:node_type(), SuccsSucc::node:node_type()} |
    {crash, DeadPid::comm:mypid()} |
    {update_id, NewId::?RT:key()} |
    {'$gen_cast', {debug_info, Requestor::comm:erl_local_pid()}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a chord-like ring maintenance process, registers it with the
%%      process dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(ringmaintenance_trigger),
    gen_component:start_link(?MODULE, Trigger, [{pid_groups_join_as, DHTNodeGroup, ring_maintenance}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(module()) -> {uninit, QueuedMessages::msg_queue:msg_queue(), TriggerState::trigger:state()}.
init(Trigger) ->
    log:log(info,"[ RM ~p ] starting ring maintainer chord", [comm:this()]),
    TriggerState = trigger:init(Trigger, fun stabilizationInterval/0, stabilize),
    comm:send_local(get_cs_pid(), {init_rm, self()}),
    TriggerState2 = trigger:next(TriggerState),
    {uninit, msg_queue:new(), TriggerState2}.

%% @doc Sends a message to the remote node's ring_maintenance process asking for
%%      its list of successors.
-spec get_successorlist(comm:mypid()) -> ok.
get_successorlist(RemoteDhtNodePid) ->
    comm:send_to_group_member(RemoteDhtNodePid, ring_maintenance, {get_succlist, comm:this()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
%set info for dht_node
on({init_rm, Me, Predecessor, Successor}, {uninit, QueuedMessages, TriggerState}) ->
    Neighborhood = nodelist:new_neighborhood(Predecessor, Me, Successor),
    get_successorlist(node:pidX(Successor)),
    fd:subscribe(lists:usort([node:pidX(Predecessor), node:pidX(Successor)])),
    msg_queue:send(QueuedMessages),
    {Neighborhood, TriggerState};

on(Msg, {uninit, QueuedMessages, TriggerState}) ->
    {uninit, msg_queue:add(QueuedMessages, Msg), TriggerState};

on({get_succlist, Source_Pid}, {Neighborhood, _TriggerState} = State) ->
    comm:send(Source_Pid, {get_succlist_response,
                           nodelist:node(Neighborhood),
                           nodelist:succs(Neighborhood)}),
    State;

on({stabilize}, {Neighborhood, TriggerState}) ->
    % new stabilization interval
    case nodelist:has_real_succ(Neighborhood) of
        true ->
            comm:send(node:pidX(nodelist:succ(Neighborhood)),
                      {get_node_details, comm:this(), [pred]});
        _ -> ok
    end,
    {Neighborhood, trigger:next(TriggerState)};

% got node_details from our successor
on({get_node_details_response, NodeDetails}, {OldNeighborhood, TriggerState})  ->
    SuccsPred = node_details:get(NodeDetails, pred),
    NewNeighborhood =
        nodelist:add_nodes(OldNeighborhood, [SuccsPred], predListLength(), succListLength()),
    rm_beh:update_dht_node(OldNeighborhood, NewNeighborhood),
    rm_beh:update_failuredetector(OldNeighborhood, NewNeighborhood),
    get_successorlist(node:pidX(nodelist:succ(NewNeighborhood))),
    {NewNeighborhood, TriggerState};

on({get_succlist_response, Succ, SuccsSuccList}, {Neighborhood, TriggerState}) ->
    NewNeighborhood =
        nodelist:add_nodes(Neighborhood, [Succ | SuccsSuccList], predListLength(), succListLength()),
    %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
    rm_beh:notify_new_pred(node:pidX(nodelist:succ(NewNeighborhood)), nodelist:node(NewNeighborhood)),
    rm_beh:update_dht_node(Neighborhood, NewNeighborhood),
    rm_beh:update_failuredetector(Neighborhood, NewNeighborhood),
    {NewNeighborhood, TriggerState};

on({notify_new_pred, NewPred}, {OldNeighborhood, TriggerState}) ->
    NewNeighborhood =
        nodelist:add_node(OldNeighborhood, NewPred, predListLength(), succListLength()),
    rm_beh:update_dht_node(OldNeighborhood, NewNeighborhood),
    rm_beh:update_failuredetector(OldNeighborhood, NewNeighborhood),
    {NewNeighborhood, TriggerState};

on({notify_new_succ, NewSucc}, {OldNeighborhood, TriggerState}) ->
    NewNeighborhood =
        nodelist:add_node(OldNeighborhood, NewSucc, predListLength(), succListLength()),
    rm_beh:update_dht_node(OldNeighborhood, NewNeighborhood),
    rm_beh:update_failuredetector(OldNeighborhood, NewNeighborhood),
    {NewNeighborhood, TriggerState};

on({crash, DeadPid}, {Neighborhood, TriggerState})  ->
    NewNeighborhood = nodelist:remove(DeadPid, Neighborhood),
    {NewNeighborhood, TriggerState};

on({leave, SourcePid}, {Neighborhood, TriggerState}) ->
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    Succ = nodelist:succ(Neighborhood),
    comm:send_to_group_member(node:pidX(Succ), ring_maintenance, {pred_left, Me, Pred}),
    comm:send_to_group_member(node:pidX(Pred), ring_maintenance, {succ_left, Me, Succ}),
    comm:send_local(SourcePid, {leave_response}),
    {uninit, msg_queue:new(), TriggerState};

on({pred_left, OldPred, PredsPred}, {Neighborhood, TriggerState}) ->
    NewNbh1 = nodelist:remove(OldPred, Neighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, PredsPred, predListLength(), succListLength()),
    rm_beh:update_dht_node(Neighborhood, NewNbh2),
    rm_beh:update_failuredetector(Neighborhood, NewNbh2),
    {NewNbh2, TriggerState};

on({succ_left, OldSucc, SuccsSucc}, {Neighborhood, TriggerState}) ->
    NewNbh1 = nodelist:remove(OldSucc, Neighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, SuccsSucc, predListLength(), succListLength()),
    rm_beh:update_dht_node(Neighborhood, NewNbh2),
    rm_beh:update_failuredetector(Neighborhood, NewNbh2),
    {NewNbh2, TriggerState};

on({update_id, NewId}, {Neighborhood, TriggerState} = State) ->
    try begin
            NewMe = node:update_id(nodelist:node(Neighborhood), NewId),
            NewNeighborhood = nodelist:update_node(Neighborhood, NewMe),
            rm_beh:update_dht_node(Neighborhood, NewNeighborhood),
            {NewNeighborhood, TriggerState}
        end
    catch
        throw:_ ->
            log:log(error, "[ RM ] can't update dht node ~w with id ~w (pred=~w, succ=~w)",
                    [nodelist:node(Neighborhood), NewId,
                     nodelist:pred(Neighborhood), nodelist:succ(Neighborhood)]),
            State
    end;

on({web_debug_info, Requestor}, {Neighborhood, _TriggerState} = State)  ->
    comm:send_local(Requestor,
                    {web_debug_info_reply,
                     [{"algorithm", lists:flatten(io_lib:format("~p", [?MODULE]))},
                      {"self", lists:flatten(io_lib:format("~p", [nodelist:node(Neighborhood)]))},
                      {"preds", lists:flatten(io_lib:format("~p", [nodelist:preds(Neighborhood)]))},
                      {"succs", lists:flatten(io_lib:format("~p", [nodelist:succs(Neighborhood)]))}]}),
    State.

%% @doc Notifies the successor and predecessor that the current dht_node is
%%      going to leave / left. Will reset the ring_maintenance state to uninit
%%      and respond with a {leave_response} message.
%%      Note: only call this method from inside the dht_node process!
-spec leave() -> ok.
leave() ->
    comm:send_local(pid_groups:get_my(ring_maintenance),
                    {leave, self()}).

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
get_cs_pid() -> pid_groups:get_my(dht_node).

%% @doc the length of the successor list
-spec predListLength() -> pos_integer().
predListLength() -> 1.

%% @doc the length of the successor list
-spec succListLength() -> pos_integer().
succListLength() -> config:read(succ_list_length).

%% @doc the interval between two stabilization runs Max
-spec stabilizationInterval() -> pos_integer().
stabilizationInterval() -> config:read(stabilization_interval_max).
