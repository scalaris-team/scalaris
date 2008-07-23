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
%%% File    : cs_xmlrpc.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : XMLRPC Interface
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_xmlrpc.erl 463 2008-05-05 11:14:22Z schuett $

-module(cs_xmlrpc).

-author('schuett@zib.de').
-vsn('$Id: cs_xmlrpc.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([start_link/0]).
-export([handler/2]).

handler(_, {call, ping, [Cookie]}) ->
    {false, {response, [Cookie]}};
handler(_, Payload) ->
    FaultString = lists:flatten(io_lib:format("Unknown call: ~p", [Payload])),
    io:format("~w~n", [FaultString]),
    {false, {response, {fault, -4, FaultString}}}.

start_link() ->
    xmlrpc:start_link(6666, 100, 60 * 1000, {?MODULE, handler}, undefined).
    
