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
%%% File    : cs_lookup.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Public Lookup API; Reliable Lookup
%%%
%%% Created : 10 Apr 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_lookup.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_lookup).

-author('schuett@zib.de').
-vsn('$Id: cs_lookup.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([reliable_get_node/3, reliable_get_node_service/3, unreliable_lookup/2,
	unreliable_get_key/1]).

%logging on
%-define(LOG(S, L), io:format(S, L)).
%logging off
-define(LOG(S, L), ok).

-include("chordsharp.hrl").

%%%-----------------------Public API----------------------------------

reliable_get_node(InstanceId, Id, Timeout) ->
    spawn(cs_lookup, reliable_get_node_service, [InstanceId, Id, cs_send:this()]),
    receive
	{get_node_response, Id, Node} ->
	    ?LOG("[ ~w | I | Node   | ~w ] reliable_get_node succeeded~n",[calendar:universal_time(), self()]),
	    {ok, Node}
    after
	Timeout ->
	    io:format("[ ~w | I | Node   | ~w ] reliable_get_node failed ~p~n",[calendar:universal_time(), self(), boot_server:node_list()]),
	    error
    end.

unreliable_lookup(Key, Msg) ->
    get_pid(cs_node) ! {lookup_aux, Key, 0, Msg}.

unreliable_get_key(Key) ->
    unreliable_lookup(Key, {get_key, cs_send:this(), Key}).
%%%-----------------------Implementation------------------------------

reliable_get_node_service(InstanceId, Id, Node) ->
    reliable_get_node_service_using_boot(InstanceId, Id, Node).
%    cs_send:send(cs_node, {lookup_aux, Id, 0, {get_node, self(), Id}}),
%    receive
%	{get_node_response, Id, Response} ->
%	    cs_send:send(Node, {get_node_response, Id, Response})
%    after
%	3000 ->
%	    ?LOG("[ ~w | I | Node   | ~w ] reliable_get_node_service failed~n",[calendar:universal_time(), self()]),
%	    reliable_get_node_service_using_boot(Id, Node)
%    end.

reliable_get_node_service_using_boot(InstanceId, Id, Node) ->
    erlang:put(instance_id, InstanceId),
    reliable_get_node_service_using_boot(Id, Node).

reliable_get_node_service_using_boot(Id, Node) ->
    NodeList = boot_server:node_list(),
    reliable_get_node_service_using_boot_iter(NodeList, [], Id, Node).

reliable_get_node_service_using_boot_iter([First | Rest], Suspected, Id, Node) ->
    cs_send:send(First, {lookup_aux, Id, 0, {get_node, cs_send:this(), Id}}),
    receive
	{get_node_response, Id, Response} ->
	    cs_send:send(Node, {get_node_response, Id, Response})
    after
	3000 ->
	    ?LOG("[ ~w | I | Node   | ~w ] reliable_get_node_using_boot failed~n",[calendar:universal_time(), self()]),
	    reliable_get_node_service_using_boot_iter(Rest, [First | Suspected], Id, Node)
    end;
reliable_get_node_service_using_boot_iter([], _Suspected, Id, Node) ->
    %Suspected, [], 
    reliable_get_node_service_using_boot(Id, Node).

get_pid(Id) ->
    InstanceId = erlang:get(instance_id),
    if
  	InstanceId == undefined ->
  	    io:format("~p~n", [util:get_stacktrace()]);
  	true ->
  	    ok
    end,
    process_dictionary:lookup_process(InstanceId, Id).
