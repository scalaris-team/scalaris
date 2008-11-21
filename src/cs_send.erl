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
%%% File    : cs_send.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Message Sending
%%%
%%% Created :  15 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_send).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("transstore/trecords.hrl").

-export([send/2, this/0, get/2, here/1]).
%-export([send/2, this/0, get/2]).

-define(TCP_LAYER, true).
%-define(BUILTIN, true).

-ifdef(TCP_LAYER).
this() ->
    %self().
    comm_layer.comm_layer:this().

send(Pid, Message) ->
    %Pid ! Message.
    comm_layer.comm_layer:send(Pid, Message).

% get process Name on node Node
get(Name, {IP, Port, _Pid}=_Node) ->
    {IP, Port, Name}.

-spec(here/1 :: (pid()) -> {any(), integer(), pid() | atom()}).
here(Pid) ->
    comm_layer.comm_layer:here(Pid).
    
-endif.

-ifdef(BUILTIN).
this() ->
    self().

send(Pid, Message) ->
    Pid ! Message.

get(Name, {_Pid,Host}) ->
    {Name, Host};
get(Name, Pid) ->
    {Name, node(Pid)}.

% here(Pid) ->
%     {Pid, self()}. 
-endif.
