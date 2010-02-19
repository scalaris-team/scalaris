%  Copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
%% @doc The boot server maintains a list of chord# nodes and checks the 
%%  availability using a failure_detector. It also exports a webpage 
%%  on port 8000 containing some statistics. Its main purpose is to 
%%  give new chord# nodes a list of nodes already in the system.

-module(boot_server).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([start_link/1, number_of_nodes/0, node_list/0, connect/0, be_the_first/0]).

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
%% @doc trigger a message with  the number of nodes known to the boot server
%% @spec number_of_nodes() -> ok
-spec(number_of_nodes/0 :: () -> ok).
number_of_nodes() ->
    cs_send:send(config:bootPid(), {get_list_length, cs_send:this()}),
    ok.

be_the_first() ->
    cs_send:send(config:bootPid(), {be_the_first, cs_send:this()}),
    ok.

connect() ->
    % @todo we have to improve the startup process!
    cs_send:send(config:bootPid(), {connect}).

%% @doc trigger a message with all nodes known to the boot server
%% @spec node_list() -> ok
-spec(node_list/0 :: () -> ok).
node_list() ->
    cs_send:send(config:bootPid(), {get_list, cs_send:this()}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



on({crash, PID},{Nodes,First,Subscriber}) ->
    NewNodes = gb_sets:delete_any(PID, Nodes),
    {NewNodes,First,Subscriber};
on({ping, Ping_PID, Cookie},{Nodes,First,Subscriber}) ->
    cs_send:send(Ping_PID, {pong, Cookie}),
    {Nodes,First,Subscriber};
on({ping, Ping_PID},{Nodes,First,Subscriber}) ->
    cs_send:send(Ping_PID, {pong, Ping_PID}),
    {Nodes,First,Subscriber};
on({get_list, Ping_PID},{Nodes,First,Subscriber}) ->
    case gb_sets:is_empty(Nodes) of
        true ->
            {Nodes,First,[Ping_PID|Subscriber]};
        _ -> cs_send:send(Ping_PID, {get_list_response, gb_sets:to_list(Nodes)}),
             {Nodes,First,Subscriber}
    end;
on({be_the_first,Ping_PID},{Nodes,First,Subscriber}) ->
    cs_send:send(Ping_PID, {be_the_first_response,First}),
    {Nodes,false,Subscriber};

on({get_list_length,Ping_PID},{Nodes,First,Subscriber}) ->
    cs_send:send(Ping_PID, {get_list_length_response, length(gb_sets:to_list(Nodes))}),
    {Nodes,First,Subscriber};
on({register, Ping_PID},{Nodes,First,Subscriber}) ->
    fd:subscribe(Ping_PID),
    NewNodes = gb_sets:add(Ping_PID, Nodes),
    case Subscriber of
        [] ->
            ok;
        _ ->
            [cs_send:send(Node,{get_list_response,gb_sets:to_list(NewNodes)} )|| Node <- Subscriber]
    end,
    {NewNodes,First,[]};
on({connect},State) ->
    % ugly work around for finding the local ip by setting up a socket first
    State;
on(_, _State) ->
    unknown_event.

init(_Arg) ->
    log:log(info,"[ Boot | ~w ] Starting Bootserver",[self()]),
    {gb_sets:empty(),true,[]}.

%% @doc starts the server; called by the boot supervisor
%% @see boot_sup
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
     gen_component:start_link(?MODULE, [InstanceId, []], [{register_native, boot}]).
