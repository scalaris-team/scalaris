%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc ring maintenance protocol for grouped nodes
%% @end
%% @version $Id$
-module(group_rm).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([trigger/1,
         handle_get_pred/3, handle_get_pred_response/3,
         handle_get_succ/3, handle_get_succ_response/3]).

-spec trigger(group_state:state()) -> group_state:state().
trigger(State) ->
    % @todo
    %group_utils:notify_neighbors(State),
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    GroupNode = group_view:get_group_node(View),
    Pred = group_local_state:get_predecessor(NodeState),
    Succ = group_local_state:get_successor(NodeState),
    {_, _, _, Preds} = Pred,
    {_, _, _, Succs} = Succ,
    _ = [comm:send(P, {rm_get_succ, GroupNode, Pred}) || P <- Preds],
    _ = [comm:send(P, {rm_get_pred, GroupNode, Succ}) || P <- Succs],
    State.


% @doc this message was sent by a potential successor; however his
% predecessor (NodesPred) could be a better successor
-spec handle_get_succ(group_state:state(), group_types:group_node(),
                      group_types:group_node()) -> group_state:state().
handle_get_succ(State, Node, NodesPred) ->
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    Me = group_view:get_group_node(View),
    MySucc = group_local_state:get_successor(NodeState),
    MyRange = group_view:get_interval(View),
    {_, _, _, Nodes} = Node,
    NewNodeState = check_potential_succs(NodeState, MyRange, Node, NodesPred),
    _ = [comm:send(P, {rm_get_succ_response, Me, MySucc}) || P <- Nodes],
    group_state:set_node_state(State, NewNodeState).

% @doc this message was sent by a potential predecessor; however his
% successor (NodesSucc) could be a better predecessor
-spec handle_get_pred(group_state:state(), group_types:group_node(),
                      group_types:group_node()) -> group_state:state().
handle_get_pred(State, Node, NodesSucc) ->
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    Me = group_view:get_group_node(View),
    MyPred = group_local_state:get_predecessor(NodeState),
    MyRange = group_view:get_interval(View),
    {_, _, _, Nodes} = Node,
    NewNodeState = check_potential_preds(NodeState, MyRange, Node, NodesSucc),
    _ = [comm:send(P, {rm_get_pred_response, Me, MyPred}) || P <- Nodes],
    group_state:set_node_state(State, NewNodeState).

-spec handle_get_pred_response(group_state:state(), group_types:group_node(),
                               group_types:group_node()) -> group_state:state().
handle_get_pred_response(State, Node, NodesPred) ->
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    MyRange = group_view:get_interval(View),
    NewNodeState = check_potential_succs(NodeState, MyRange, Node, NodesPred),
    group_state:set_node_state(State, NewNodeState).

-spec handle_get_succ_response(group_state:state(), group_types:group_node(),
                               group_types:group_node()) -> group_state:state().
handle_get_succ_response(State, Node, NodesSucc) ->
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    MyRange = group_view:get_interval(View),
    NewNodeState = check_potential_preds(NodeState, MyRange, Node, NodesSucc),
    group_state:set_node_state(State, NewNodeState).

% check whether Node or NodesPred could be my successor
check_potential_succs(NodeState, MyRange, Node, NodesPred) ->
    {_, NodesRange, _, _} = Node,
    {_, NodesPredRange, _, _} = NodesPred,
    case intervals:is_left_of(MyRange, NodesRange) of
        % Node is still my successor
        true ->
            group_local_state:update_succ(NodeState, Node, MyRange);
        % maybe NodesPred is my successor
        false ->
            case intervals:is_left_of(MyRange, NodesPredRange) of
                % NodesPred is my successor
                true ->
                    group_local_state:update_succ(NodeState, NodesPred, MyRange);
                % who is my successor?
                false ->
                    % @todo
                    NodeState
            end
    end.

% check whether Node or NodesSucc could be my predecessor
check_potential_preds(NodeState, MyRange, Node, NodesSucc) ->
    {_, NodesRange, _, _} = Node,
    {_, NodesSuccRange, _, _} = NodesSucc,
    case intervals:is_left_of(NodesRange, MyRange) of
        % Node is still my predecessor
        true ->
            group_local_state:update_pred(NodeState, Node, MyRange);
        % maybe NodesSucc is my predecessor
        false ->
            case intervals:is_left_of(NodesSuccRange, MyRange) of
                % NodesSucc is my predecessor
                true ->
                    group_local_state:update_pred(NodeState, NodesSucc, MyRange);
                % who is my predecessor?
                false ->
                    % @todo
                    NodeState
            end
    end.
