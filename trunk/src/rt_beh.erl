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
-vsn('$Id$').

% for behaviour
-export([behaviour_info/1]).

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Behaviour definition
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_beh:behaviour
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     % create a default routing table
     {empty, 1}, {empty_ext, 1},
     % mapping: key space -> identifier space
     {hash_key, 1}, {get_random_node_id, 0},
     % routing
     {next_hop, 2},
     % trigger for new stabilization round
     {init_stabilize, 2},
     % adapt RT to changed neighborhood
     {update, 3},
     % dead nodes filtering
     {filter_dead_node, 2},
     % statistics
     {to_pid_list, 1}, {get_size, 1},
     % gets all (replicated) keys for a given (hashed) key
     % (for symmetric replication)
     {get_replica_keys, 1},
     % address space size (throws 'throw:not_supported' if unsupported by the RT)
     {n, 0},
     % for debugging and web interface
     {dump, 1},
     % for bulkowner
     {to_list, 1},
     % convert from internal representation to version for dht_node
     {export_rt_to_dht_node, 2},
     % handle messages specific to a certain routing-table implementation
     {handle_custom_message, 2},
     % common methods
     {check, 4}, {check, 5},
     {check_config, 0}
    ];
%% userdevguide-end rt_beh:behaviour

behaviour_info(_Other) ->
    undefined.
