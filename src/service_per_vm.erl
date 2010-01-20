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
%%% File    : service_per_vm.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description :
%%%
%%% Created : 20 Jan 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @doc
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(service_per_vm).

-author('schuett@zib.de').
-vsn('$Id').

-behaviour(gen_component).

-export([dump_node_states/0]).

-export([start_link/0, init/1, on/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc ask all local nodes for their state
dump_node_states() ->
    [gen_component:get_state(Pid)
     || Pid <-  process_dictionary:find_all_cs_nodes()].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Server process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_component:start_link(?MODULE, [], [{register_native, service_per_vm}]).

init(_Arg) ->
    ok.

on({get_cs_nodes, Pid}, ok) ->
    Nodes = [cs_send:make_global(Node)
             || Node <- process_dictionary:find_all_cs_nodes()],
    cs_send:send(Pid, {get_cs_nodes_response, Nodes}),
    ok;

on(_, _State) ->
    unknown_event.
