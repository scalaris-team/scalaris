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
%%% File    : comm_connection.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created : 18 Apr 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(comm_layer.comm_connection).

-export([open_new/4, new/3]).

-import(gen_tcp).
-import(inet).
-import(io).
-import(io_lib).
-import(log).

-include("comm_layer.hrl").

%% @doc new accepted connection. called by comm_acceptor
%% @spec new(inet:ip_address(), int(), socket()) -> pid()
new(Address, Port, Socket) ->
    spawn(fun () -> loop(Socket, Address, Port) end).

%% @doc open new connection
%% @spec open_new(inet:ip_address(), int(), inet:ip_address(), int()) -> pid()
open_new(Address, Port, undefined, MyPort) ->
    Myself = self(),
    LocalPid = spawn(fun () ->
			     S = new_connection(Address, Port, MyPort),
			     {ok, {MyIP, _MyPort}} = inet:sockname(S),
			     Myself ! {new_connection_started, MyIP, MyPort},
			     loop(S, Address, Port)
		     end),
    receive
	{new_connection_started, MyIP, MyPort} ->
	    {local_ip, MyIP, MyPort, LocalPid}
    end;
open_new(Address, Port, _MyAddress, MyPort) ->
    spawn(fun () -> 
		  S = new_connection(Address, Port, MyPort), 
		  loop(S, Address, Port) 
	  end).

loop(fail, Address, Port) ->
    comm_port:unregister_connection(Address, Port),
    ok;
loop(Socket, Address, Port) ->
    receive
	{tcp_closed, Socket} ->
	    	comm_port:unregister_connection(Address, Port),
	    	gen_tcp:close(Socket);
	{tcp, Socket, Data} ->
	    case binary_to_term(Data) of
	        {deliver, Process, Message} ->
		    Process ! Message,
		    inet:setopts(Socket, [{active, once}]),
		    loop(Socket, Address, Port);
		{user_close} ->
		    comm_port:unregister_connection(Address, Port),
		    gen_tcp:close(Socket);
		Unknown ->
		    log:log2file(comm_connection, io_lib:format("unknown message ~p", [Unknown]) ),
		    inet:setopts(Socket, [{active, once}]),
		    loop(Socket, Address, Port)
	    end;
	{send, Pid, Message} ->
	    BinaryMessage = term_to_binary({deliver, Pid, Message}),
	    case gen_tcp:send(Socket, BinaryMessage) of
		{error, closed} ->
  	    	    comm_port:unregister_connection(Address, Port),
	    	    gen_tcp:close(Socket);
		    %gen_tcp:close(Socket),
		    %self() ! Request,
		    %loop(new_connection(Address, Port, comm_port:get_local_address(), 
			%		comm_port:get_local_port()), 
			% Address, Port);
		{error, Reason} ->
		    log:log2file(comm_connection, io_lib:format("couldn't send to ~p:~p (~p)~n", [Address, Port, Reason])),
  	    	    comm_port:unregister_connection(Address, Port),
	    	    gen_tcp:close(Socket);
		ok ->
		    ?LOG_MESSAGE(erlang:element(1, Message), byte_size(BinaryMessage)),
		    loop(Socket, Address, Port)
	    end;

        Unknown ->
	    log:log2file(comm_connection, io_lib:format("unknown message2 ~p", [Unknown]) ),
	    loop(Socket, Address, Port)
    end.

new_connection(Address, Port, MyPort) ->
    case gen_tcp:connect(Address, Port, [binary, {packet, 4}, {active, once}, {nodelay, true}, {send_timeout, 60000}], 60000) of
        {ok, Socket} ->
                                                % send end point data
	    {ok, {MyAddress, _MyPort}} = inet:sockname(Socket),
            gen_tcp:send(Socket, term_to_binary({endpoint, MyAddress, MyPort})),
	    {ok, {RemoteIP, RemotePort}} = inet:peername(Socket),
            gen_tcp:send(Socket, term_to_binary({youare, RemoteIP, RemotePort})),
	    
            Socket;
        {error, Reason} ->
            log:log2file(comm_connection, io_lib:format("couldn't connect to ~p:~p (~p)~n", [Address, Port, Reason])),
	    comm_port:unregister_connection(Address, Port),
	    fail
    end.
    
