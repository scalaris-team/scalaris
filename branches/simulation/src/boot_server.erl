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
%%% File    : boot_server.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : maintains a list of chord# nodes for bootstrapping
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
%% @doc The boot server maintains a list of chord# nodes and checks the 
%%  availability using a failure_detector. It also exports a webpage 
%%  on port 8000 containing some statistics. Its main purpose is to 
%%  give new chord# nodes a list of nodes already in the system.

-module(boot_server).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([start_link/1, number_of_nodes/0, node_list/0, connect/0]).

-behaviour(gen_component).

-export([init/1, on/2]).

-ifdef(types_are_builtin).
-type(boot_node_list()::gb_set()).
-else.
-type(boot_node_list()::gb_sets:gb_set()).
-endif.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc returns the number of nodes known to the boot server
%% @spec number_of_nodes() -> integer()
-spec(number_of_nodes/0 :: () -> pos_integer()).
number_of_nodes() ->
    cs_send:send(config:bootPid(), {get_list_length, cs_send:this()}),
    ok.

connect() ->
    cs_send:send(config:bootPid(), {connect}).

%% @doc returns all nodes known to the boot server
%% @spec node_list() -> list(pid())
-spec(node_list/0 :: () -> list()).
node_list() ->
    cs_send:send(config:bootPid(), {get_list, cs_send:this()}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc the main loop of the bootstrapping server
%% @spec loop(gb_sets:gb_set(pid())) -> gb_sets:gb_set(pid())


on({crash, PID},Nodes) ->
	    NewNodes = gb_sets:delete_any(PID, Nodes),
	    NewNodes;
on({ping, Ping_PID, Cookie},Nodes) ->
	    cs_send:send(Ping_PID, {pong, Cookie}),
	    Nodes;
on({ping, Ping_PID},Nodes) ->
	    cs_send:send(Ping_PID, {pong, Ping_PID}),
	    Nodes;
on({get_list, Ping_PID},Nodes) ->
	    cs_send:send(Ping_PID, {get_list_response, gb_sets:to_list(Nodes)}),
	    Nodes;
on({get_list_length,Ping_PID},Nodes) ->
        cs_send:send(Ping_PID, {get_list_length_response, length(gb_sets:to_list(Nodes))}),
        
	    Nodes;
on({register, Ping_PID},Nodes) ->
	    failuredetector2:subscribe(Ping_PID),
	    gb_sets:add(Ping_PID, Nodes);
on({connect},Nodes) ->
	    % ugly work around for finding the local ip by setting up a socket first
	    Nodes;

on(_, _State) ->
    unknown_event.
%% @doc starts the mainloop of the boot server
%% @spec start(term()) -> gb_sets:gb_set(pid())
%-spec(start/1 :: (any()) -> no_return()).
init(_Arg) ->
    log:log(info,"[ Boot | ~w ] Starting Bootserver",[self()]),
    gb_sets:empty().

%% @doc starts the server; called by the boot supervisor
%% @see boot_sup
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
     gen_component:start_link(?MODULE, [InstanceId, []], [{register_native, boot}]).
