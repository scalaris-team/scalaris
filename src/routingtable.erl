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
%%% File    : routingtable.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : routing table behaviour
%%%
%%% Created :  14 Apr 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: routingtable.erl 463 2008-05-05 11:14:22Z schuett $
-module(routingtable).

-author('schuett@zib.de').
-vsn('$Id: routingtable.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     % create a default routing table
     {empty, 1}, 
     % key space -> indentifier space
     {hash_key, 1}, {getRandomNodeId, 0},
     % routing
     {next_hop, 2}, 
     % trigger for new stabilization round
     {stabilize, 1}, 
     % dead nodes filtering
     {filterDeadNodes, 2}, 
     % statistics
     {to_pid_list, 1}, {to_node_list, 1}, {get_size, 1},
     % for symmetric replication
     {get_keys_for_replicas, 1},
     % for debugging
     {dump, 1}
    ];

behaviour_info(_Other) ->
    undefined.

%% see rt_simple.erl for simple standard implementation
