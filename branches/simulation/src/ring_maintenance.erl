%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(ring_maintenance).

-author('schuett@zib.de').
-vsn('$Id').

-export([behaviour_info/1, update_succ_and_pred/2, update_succ/1,  update_pred/1]).

behaviour_info(callbacks) ->
    [
     % start
     {start_link, 1},
     % initialize
     {initialize, 4},
     % succ_left (you're succ left)
     {succ_left, 1},
     % pred_left (you're pred left)
     {pred_left, 1},
     % update succ (you're successor changed)
     {update_succ, 1},
     % update pred (you're predecessor changed)
     {update_pred, 1},
     % get internal state as list, e.g. the successor list
     {get_as_list, 0}
    ];

behaviour_info(_Other) ->
    undefined.

%% @doc functions for rm_*.erl modules to notify the cs_node
%%      that his pred/succ changed
update_succ_and_pred(Pred, Succ) ->    
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    cs_send:send_local(Pid , {rm_update_pred_succ, Pred, Succ}).


%% @doc functions for rm_*.erl modules to notify the cs_node
%%      that his pred/succ changed
update_pred(Pred) ->    
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    cs_send:send_local(Pid , {rm_update_pred, Pred}).


%% @doc functions for rm_*.erl modules to notify the cs_node
%%      that his pred/succ changed
update_succ(Succ) ->    
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    cs_send:send_local(Pid , {rm_update_succ, Succ}).