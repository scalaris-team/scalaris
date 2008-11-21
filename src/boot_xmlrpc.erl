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
%%% File    : boot_xmlrpc.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : XMLRPC Interface
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(boot_xmlrpc).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([start_link/1]).
-export([handler/2]).

handler(InstanceId, {call, ping, [Cookie]}) ->
    erlang:put(instance_id, InstanceId),
    {false, {response, [Cookie]}};


handler(InstanceId, {call, transtest, [_Cookie, NumElems]}) ->
    erlang:put(instance_id, InstanceId),
    {Time, _Result} = timer:tc(transaction_test, test, [NumElems]),
    {false, {response, [Time]}};

handler(InstanceId, {call, test1, [_Cookie]}) ->
    erlang:put(instance_id, InstanceId),
    {Time, _Result} = timer:tc(transaction_test, test1, []),
    {false, {response, [Time]}};

handler(InstanceId, {call, test2, [_Cookie]}) ->
    erlang:put(instance_id, InstanceId),
    {Time, _Result} = timer:tc(transaction_test, test2, []),
    {false, {response, [Time]}};

handler(InstanceId, {call, test3, [_Cookie]}) ->
    erlang:put(instance_id, InstanceId),
    {Time, _Result} = timer:tc(transaction_test, test3, []),
    {false, {response, [Time]}};

handler(InstanceId, {call, test4, [_Cookie]}) ->
    erlang:put(instance_id, InstanceId),
    {Time, _Result} = timer:tc(transaction_test, test4, []),
    {false, {response, [Time]}};

handler(InstanceId, {call, test5, [_Cookie]}) ->
    erlang:put(instance_id, InstanceId),
    {Time, _Result} = timer:tc(transaction_test, test5, []),
    {false, {response, [Time]}};


handler(InstanceId, Payload) ->
    erlang:put(instance_id, InstanceId),
    FaultString = lists:flatten(io_lib:format("Unknown call: ~p", [Payload])),
    io:format("unknown xmlrpc request: ~w~n", [FaultString]),
    {false, {response, {fault, -4, FaultString}}}.

start_link(InstanceId) ->
    xmlrpc:start_link(6666, 100, 600 * 1000, {?MODULE, handler}, InstanceId).
    
    
