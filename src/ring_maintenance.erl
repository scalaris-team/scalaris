%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : ring_maintenance.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : ring maintenance behaviour
%%%
%%% Created :  27 Nov 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(ring_maintenance).

-author('schuett@zib.de').
-vsn('$Id').

-export([behaviour_info/1, update_preds_and_succs/2,
        get_successorlist/0, get_predlist/0, succ_left/1, pred_left/1,  get_as_list/0,
         update_succs/1, update_preds/1,
        notify_new_pred/1, notify_new_succ/1]).

behaviour_info(callbacks) ->
    [
     % start
     {start_link, 1}
    ];

behaviour_info(_Other) ->
    undefined.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_successorlist() ->
    cs_send:send_local(get_pid(), {get_successorlist, self()}).

get_predlist() ->
    cs_send:send_local(get_pid(), {get_predlist, self()}).

notify_new_pred(Pred) ->
    cs_send:send_local(get_pid(), {notify_new_pred, Pred}).

notify_new_succ(Succ) ->
    cs_send:send_local(get_pid(), {notify_new_succ, Succ}).


%% @doc notification that my succ left
%%      parameter is his current succ list
succ_left(_SuccsSuccList) ->
    %% @TODO
    ok.

%% @doc notification that my pred left
%%      parameter is his current pred
pred_left(_PredsPred) ->
    %% @TODO
    ok.


get_as_list() ->
    get_successorlist().


%% @doc functions for rm_*.erl modules to notify the cs_node
%%      that his pred/succ changed
-spec(update_preds_and_succs/2 :: (list(node:node_type()), list(node:node_type())) ->
             ok).
update_preds_and_succs(Preds, Succs) ->
    Pid = process_dictionary:get_group_member(cs_node),
    cs_send:send_local(Pid , {rm_update_preds_succs, Preds, Succs}),
    ok.

%% @doc functions for rm_*.erl modules to notify the cs_node
%%      that his pred/succ changed
update_preds(Preds) ->
    Pid = process_dictionary:get_group_member(cs_node),
    cs_send:send_local(Pid , {rm_update_preds, Preds}).


%% @doc functions for rm_*.erl modules to notify the cs_node
%%      that his pred/succ changed
update_succs(Succs) ->
    Pid = process_dictionary:get_group_member(cs_node),
    cs_send:send_local(Pid , {rm_update_succs, Succs}).

% @private
get_pid() ->
    process_dictionary:get_group_member(ring_maintenance).
