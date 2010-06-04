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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    ring maintenance behaviour
%%% @end

%% @version $Id$
-module(rm_beh).
-author('schuett@zib.de').
-vsn('$Id$').

-export([behaviour_info/1,
         succ_left/1, pred_left/1,
         update_neighbors/1,
         notify_new_pred/2, notify_new_succ/2]).

behaviour_info(callbacks) ->
    [
     {start_link, 1},
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
    comm:send_to_group_member(Node, ring_maintenance, {notify_new_succ, NewSucc}).

%% @doc Sends a message to the remote node's ring_maintenance process notifying
%%      it of a new predecessor.
-spec notify_new_pred(Node::comm:mypid(), NewPred::node:node_type()) -> ok.
notify_new_pred(Node, NewPred) ->
    comm:send_to_group_member(Node, ring_maintenance, {notify_new_pred, NewPred}).


%% @doc notification that my succ left
%%      parameter is his current succ list
succ_left(_SuccsSuccList) ->
    %% @TODO implement notification
    ok.

%% @doc notification that my pred left
%%      parameter is his current pred
pred_left(_PredsPred) ->
    %% @TODO implement notification
    ok.

%% @doc Notifies the dht_node that (at least one of) his neighbors changed
%%      (to be used in the rm_*.erl modules).
-spec update_neighbors(Neighbors::nodelist:neighborhood()) -> ok.
update_neighbors(Neighbors) ->
    Pid = process_dictionary:get_group_member(dht_node),
    comm:send_local(Pid, {rm_update_neighbors, Neighbors}).
