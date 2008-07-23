%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : comm_layer.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created :  04 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(comm_layer.comm_layer).

-author('schuett@zib.de').
-vsn('$Id: comm_layer.erl 523 2008-07-14 13:38:34Z schintke $ ').

-export([start_link/0, send/2, this/0]).

-import(io).
-import(util).

%%====================================================================
%% public functions
%%====================================================================

%% @doc starts the communication port (for supervisor)
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
start_link() ->
    comm_port_sup:start_link().

%% @doc a process descriptor has to specify the erlang vm 
%%      + the process inside. {IP address, port, pid}
%% @type process_id() = {inet:ip_address(), int(), pid()}.
%% @spec send(process_id(), term()) -> ok
send({{_IP1, _IP2, _IP3, _IP4} = _IP, _Port, _Pid} = Target, Message) ->
    MyIP = comm_port:get_local_address(),
    MyPort = comm_port:get_local_port(),
    %io:format("send: ~p:~p -> ~p:~p : ~p\n", [MyIP, MyPort, _IP, _Port, Message]),
    IsLocal = (MyIP == _IP) and (MyPort == _Port),
    if
 	IsLocal ->
 	    _Pid ! Message;
 	true ->
	    comm_port:send(Target, Message)
 	    %communication_port_server ! {send, Target, Message}
    end;

send(Target, Message) ->
    io:format("wrong call to cs_send:send: ~w ! ~w~n", [Target, Message]),
    io:format("stacktrace: ~w~n", [util:get_stacktrace()]),
    ok.

%% @doc returns process descriptor for the calling process
%% @spec this() -> process_id()
this() ->
    {comm_port:get_local_address(), comm_port:get_local_port(), self()}.


