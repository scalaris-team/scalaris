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
%%% Description : Message Sending. This module allows to configure 
%%%           Scalaris for using Distributed Erlang or TCP for inter-node
%%%           communication.
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
-include("../include/scalaris.hrl").

-export([send/2,send_after/3 , this/0, get/2, send_to_group_member/3,
         send_local/2, make_global/1, is_valid/1]).

send_to_group_member(Csnodepid,Processname,Mesg) ->
    send(Csnodepid,{send_to_group_member,Processname,Mesg}).

-ifdef(TCP_LAYER).
-type(mypid() :: {inet:ip_address(), integer(), pid()}).
-spec(this/0 :: () -> mypid()).
this() ->
    %self().
    comm_layer:this().

-spec(send/2 :: (mypid(), any()) -> ok).
send(Pid, Message) ->
    %Pid ! Message.
    comm_layer:send(Pid, Message).

send_local(Pid, Message) ->
    Pid ! Message.

send_after(Delay,Pid, Message) ->
    erlang:send_after(Delay,Pid,Message).

% get process Name on node Node
get(Name, {IP, Port, _Pid}=_Node) ->
    {IP, Port, Name}.

-spec(is_valid/1 :: (mypid()) -> bool()).
is_valid(Pid) ->
    comm_layer:is_valid(Pid).

-endif.
-ifdef(BUILTIN).
-type(mypid() :: pid()).
this() ->
    self().

send_after(Delay,Pid, Message) ->
    erlang:send_after(Delay,Pid,Message).

send(Pid, Message) ->
    Pid ! Message.

send_local(Pid, Message) ->
    Pid ! Message.

%get(Name, {_Pid,Host}) ->
%    {Name, Host};
get(Name, {_, Host}) when is_atom(Name) ->
    {Name, Host};
get(Name, Pid) when is_atom(Name) ->
    %io:format("CS: ~p ~p ~n ",[Name,Pid]),
    {Name, node(Pid)};
get(Name, Pid) ->
    A = node(Name),
    A = node(Pid), % we assume that you only call get with local pids
    Name.

is_valid(Pid) ->
    true.

-endif.

-ifdef(SIMULATION).
this() ->
    self().

send(Pid, Message) ->
    %Pid ! Message.
    scheduler:send(Pid,Message).

send_local(Pid, Message) ->
    scheduler:send(0, Pid , Message).

send_after(Delay,Pid, Message) ->
    scheduler:send(Delay,Pid,Message).

get(Name, {_Pid,Host}) ->
    {Name, Host};
get(Name, Pid) ->
    Name.

is_valid(Pid) ->
    true.

-endif.

-spec(make_global/1 :: (pid()) -> mypid()).
make_global(Pid) ->
    get(Pid, cs_send:this()).

