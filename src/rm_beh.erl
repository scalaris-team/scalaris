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

-export([behaviour_info/1, update_preds_and_succs/2,
        get_successorlist/0, get_predlist/0, succ_left/1, pred_left/1,
        update_succs/1, update_preds/1,
        notify_new_pred/1, notify_new_succ/1]).

behaviour_info(callbacks) ->
    [
     % start
     {start_link, 1},
     {check_config, 0}
    ];

behaviour_info(_Other) ->
    undefined.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends the current process' ring_maintenance process a request for its
%%      successor list.
-spec get_successorlist() -> ok.
get_successorlist() ->
    cs_send:send_local(get_my_rm_pid(), {get_succlist, self()}).

%% @doc Sends the current process' ring_maintenance process a request for its
%%      predecessor list.
-spec get_predlist() -> ok.
get_predlist() ->
    cs_send:send_local(get_my_rm_pid(), {get_predlist, self()}).

%% @doc Notifies the current process' ring_maintenance process of a changed
%%      predecessor.
-spec notify_new_pred(node:node_type()) -> ok.
notify_new_pred(Pred) ->
    cs_send:send_local(get_my_rm_pid(), {notify_new_pred, Pred}).

%% @doc Notifies the current process' ring_maintenance process of a changed
%%      successor.
-spec notify_new_succ(node:node_type()) -> ok.
notify_new_succ(Succ) ->
    cs_send:send_local(get_my_rm_pid(), {notify_new_succ, Succ}).


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


%% @doc Notifies the dht_node that his predecessors and successors changed
%%      (to be used in the rm_*.erl modules).
-spec update_preds_and_succs(Preds::[node:node_type(),...], Succs::[node:node_type(),...]) -> ok.
update_preds_and_succs(Preds, Succs) ->
    Pid = process_dictionary:get_group_member(dht_node),
    cs_send:send_local(Pid, {rm_update_preds_succs, Preds, Succs}),
    ok.

%% @doc Notifies the dht_node that his predecessors changed
%%      (to be used in the rm_*.erl modules).
-spec update_preds(Preds::[node:node_type(),...]) -> ok.
update_preds(Preds) ->
    Pid = process_dictionary:get_group_member(dht_node),
    cs_send:send_local(Pid, {rm_update_preds, Preds}).

%% @doc Notifies the dht_node that his successors changed
%%      (to be used in the rm_*.erl modules).
-spec update_succs(Succs::[node:node_type(),...]) -> ok.
update_succs(Succs) ->
    Pid = process_dictionary:get_group_member(dht_node),
    cs_send:send_local(Pid, {rm_update_succs, Succs}).

% @private
get_my_rm_pid() ->
    process_dictionary:get_group_member(ring_maintenance).
