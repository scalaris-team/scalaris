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
%% @version $Id: boot_server.erl 522 2008-07-14 12:49:59Z schuett $
%% @doc The boot server maintains a list of chord# nodes and checks the 
%%  availability using a failure_detector. It also exports a webpage 
%%  on port 8000 containing some statistics. Its main purpose is to 
%%  give new chord# nodes a list of nodes already in the system.

-module(boot_server).

-author('schuett@zib.de').
-vsn('$Id: boot_server.erl 522 2008-07-14 12:49:59Z schuett $ ').

-export([start_link/1, start/1, number_of_nodes/0, node_list/0, ping/0, ping/1, connect/0]).

%logging on
%-define(LOG(S, L), io:format(S, L)).
%logging off
-define(LOG(S,L), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc returns the number of nodes known to the boot server
%% @spec number_of_nodes() -> integer()
number_of_nodes() ->
    cs_send:send(config:bootPid(), {get_list, cs_send:this()}),
    receive
	{get_list_response, Nodes} ->
	    util:lengthX(Nodes)
    end.

connect() ->
    cs_send:send(config:bootPid(), {connect}).

%% @doc returns all nodes known to the boot server
%% @spec node_list() -> list(pid())
node_list() ->
    cs_send:send(config:bootPid(), {get_list, cs_send:this()}),
    receive
	{get_list_response, Nodes} ->
	    Nodes
    end.

%% @doc pings all known nodes
%% @spec ping() -> list(int)
ping() ->
    Nodes = node_list(),
    lists:map(fun (PID) ->
		      {Time, _ } = timer:tc(boot_server, ping, [PID]),
		      Time
	      end,
	      Nodes).

ping(PID) ->
    Me = cs_send:this(),
    cs_send:send(PID, {ping, Me}),
    receive
	{pong, Me} ->
	    ok
    after 2000 ->
	  fail
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc the main loop of the bootstrapping server
%% @spec loop(gb_sets:gb_set(pid())) -> gb_sets:gb_set(pid())
loop(Nodes) ->
    %?LOG("[ I | Boot   | ~w ] nodes <~w>~n",
    	%      [self(), length(gb_trees:values(Nodes))]),
    receive
	{crash, Id, _, _PID} ->
	    %?LOG("[ I | Boot   | ~w ] crashed <~w/~w>~n",
	    %	      [self(), _PID, gb_trees:values(Nodes)]),
	    NewNodes = gb_trees:delete_any(Id, Nodes),
	    loop(NewNodes);
	{ping, Ping_PID, Cookie} ->
	    cs_send:send(Ping_PID, {pong, Cookie}),
	    loop(Nodes);
	{ping, Ping_PID} ->
	    %?LOG("[ I | Boot   | ~w ] ping ~w ~n",[self(), Ping_PID]),
	    cs_send:send(Ping_PID, {pong, Ping_PID}),
	    loop(Nodes);
	{get_list, Ping_PID} ->
	    ?LOG("[ I | Boot   | ~w ] get_list ~w ~n",[self(), Ping_PID]),
	    cs_send:send(Ping_PID, {get_list_response, gb_sets:to_list(gb_sets:from_list(gb_trees:values(Nodes)))}),
	    loop(Nodes);
	{register, Ping_PID, Id} ->
	    %?LOG("[ I | Boot   | ~w ] register ~w ~s~n",[self(), Ping_PID, Id]),
	    failuredetector:add_node(Id, nil, Ping_PID),
	    loop(gb_trees:enter(Id, Ping_PID, Nodes));
	{connect} ->
	    % ugly work around for finding the local ip by setting up a socket first
	    loop(Nodes);
	_X ->
	    io:format("[ I | Boot   | ~w ] unknown message: ~w~n",[self(), _X]),
	    loop(Nodes)
    end.

%% @doc starts a failure_detector, the embedded webserver and the mainloop
%% @see failure_detector
%% @spec start(term()) -> gb_sets:gb_set(pid())
start(InstanceId) ->
    register(boot, self()),
    process_dictionary:register_process(InstanceId, boot_server, self()),
    %error_logger:add_report_handler(cs_error_logger),
    failuredetector:set_owner(self()),
    loop(gb_trees:empty()).

%% @doc starts the server; called by the boot supervisor
%% @see boot_sup
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    {ok, spawn_link(?MODULE, start, [InstanceId])}.
