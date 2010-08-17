%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc Ring maintenance behaviour
%% @end
%% @version $Id$
-module(rm_beh).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([behaviour_info/1,
         notify_new_pred/2, notify_new_succ/2,
         update_dht_node/1, update_dht_node/2, update_failuredetector/2,
         update_id/1]).

-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {start_link, 1},
     {leave, 0},
     {check_config, 0}
    ];

behaviour_info(_Other) ->
    undefined.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a message to the remote node's ring_maintenance process notifying
%%      it of a new successor.
-spec notify_new_succ(Node::comm:mypid(), NewSucc::node:node_type()) -> ok.
notify_new_succ(Node, NewSucc) ->
    comm:send_to_group_member(Node, ring_maintenance,
                              {notify_new_succ, NewSucc}).

%% @doc Sends a message to the remote node's ring_maintenance process notifying
%%      it of a new predecessor.
-spec notify_new_pred(Node::comm:mypid(), NewPred::node:node_type()) -> ok.
notify_new_pred(Node, NewPred) ->
    comm:send_to_group_member(Node, ring_maintenance,
                              {notify_new_pred, NewPred}).

% @doc Check if change of failuredetector is necessary.
-spec update_failuredetector(OldNeighborhood::nodelist:neighborhood(),
                             NewNeighborhood::nodelist:neighborhood()) -> ok.
update_failuredetector(OldNeighborhood, NewNeighborhood) ->
    % Note: nodelist:to_list/1 would provide similar functionality to determine
    % the view but at a higher cost and we need neither unique nor sorted lists.
    OldView = lists:append(nodelist:preds(OldNeighborhood),
                           nodelist:succs(OldNeighborhood)),
    NewView = lists:append(nodelist:preds(NewNeighborhood),
                           nodelist:succs(NewNeighborhood)),
    OldPids = [node:pidX(Node) || Node <- OldView],
    NewPids = [node:pidX(Node) || Node <- NewView],
    fd:update_subscriptions(OldPids, NewPids).

%% @doc Inform the dht_node of a new neighborhood.
-spec update_dht_node(OldNeighborhood::nodelist:neighborhood(),
                      NewNeighborhood::nodelist:neighborhood()) -> ok.
update_dht_node(OldNeighborhood, NewNeighborhood) ->
    case OldNeighborhood =/= NewNeighborhood of
        true -> update_dht_node(NewNeighborhood);
        _    -> ok
    end.

%% @doc Notifies the dht_node that its neighbors changed
%%      (to be used by the rm_*.erl modules).
-spec update_dht_node(Neighbors::nodelist:neighborhood()) -> ok.
update_dht_node(Neighbors) ->
    comm:send_local(pid_groups:get_my(dht_node),
                    {rm_update_neighbors, Neighbors}).

%% @doc Updates a dht node's id and sends the ring maintenance a message about
%%      the change.
%%      Beware: the only allowed node id changes are between the node's
%%      predecessor and successor!
-spec update_id(NewId::?RT:key()) -> ok.
update_id(NewId) ->
    comm:send_local(pid_groups:get_my(ring_maintenance),
                    {update_id, NewId}).
