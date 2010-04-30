% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc routing table behaviour
%% @end

%% @version $Id$
-module(rt_beh).
-author('schuett@zib.de').
-vsn('$Id$ ').

% for behaviour
-export([behaviour_info/1]).

% for routing table implementation
-export([initialize/3, to_html/1]).

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Behaviour definition
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin routingtable:behaviour
behaviour_info(callbacks) ->
    [
     % create a default routing table
     {empty, 1},
     % mapping: key space -> identifier space
     {hash_key, 1}, {getRandomNodeId, 0},
     % routing
     {next_hop, 2},
     % trigger for new stabilization round
     {init_stabilize, 3},
     % dead nodes filtering
     {filterDeadNode, 2},
     % statistics
     {to_pid_list, 1}, {get_size, 1},
     % for symmetric replication
     {get_keys_for_replicas, 1},
     % for debugging
     {dump, 1},
     % for bulkowner
     {to_dict, 1},
     % convert from internal representation to version for dht_node
     {export_rt_to_dht_node, 4},
     % update pred/succ in routing stored in dht_node
     {update_pred_succ_in_dht_node, 3},
     % for web interface
     {to_html, 1},
     % handle messages specific to a certain routing-table implementation
     {handle_custom_message, 2},
     {check_config, 0}
    ];
%% userdevguide-end routingtable:behaviour

behaviour_info(_Other) ->
    undefined.

%% see rt_simple.erl for simple standard implementation

initialize(Id, Pred, Succ) ->
    Pid = process_dictionary:get_group_member(routing_table),
    cs_send:send_local(Pid , {init, Id, Pred, Succ}).

to_html(RT) ->
    ?RT:to_html(RT).
