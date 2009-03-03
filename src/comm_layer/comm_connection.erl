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
%%% Description : creates and destroys connections and represents the 
%%%           endpoint of a connection where messages are received and 
%%            send from/to the network.
%%%
%%% Created : 18 Apr 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(comm_layer.comm_connection).

-export([send/3, open_new/4, new/3]).

-import(config).
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
%% @spec open_new(inet:ip_address(), int(), inet:ip_address(), int()) -> 
%%       {local_ip, inet:ip_address(), int(), pid(), inet:socket()} 
%%     | fail 
%%     | {connection, pid(), inet:socket()}
open_new(Address, Port, undefined, MyPort) ->
    Myself = self(),
    LocalPid = spawn(fun () ->
  			     case new_connection(Address, Port, MyPort) of
				 fail -> 
				     Myself ! {new_connection_failed};
				 Socket ->
				     {ok, {MyIP, _MyPort}} = inet:sockname(Socket),
				     Myself ! {new_connection_started, MyIP, MyPort, Socket},
				     loop(Socket, Address, Port)
			     end
  		     end),
    receive
  	{new_connection_failed} ->
	    fail;
  	{new_connection_started, MyIP, MyPort, S} ->
  	    {local_ip, MyIP, MyPort, LocalPid, S}
    end;
open_new(Address, Port, _MyAddress, MyPort) ->
    Owner = self(),
    LocalPid = spawn(fun () ->
			     case new_connection(Address, Port, MyPort) of
				 fail ->
				     Owner ! {new_connection_failed};
				 Socket ->
				     Owner ! {new_connection_started, Socket},
				     loop(Socket, Address, Port) 
			     end
		     end),
    receive
	{new_connection_failed} ->
	    fail;
	{new_connection_started, Socket} ->
	    {connection, LocalPid, Socket}
    end.

send({Address, Port, Socket}, Pid, Message) ->
    BinaryMessage = term_to_binary({deliver, Pid, Message}),
    case gen_tcp:send(Socket, BinaryMessage) of
	ok ->
	    ?LOG_MESSAGE(erlang:element(1, Message), byte_size(BinaryMessage)),
	    ok;
	{error, closed} ->
	    comm_port:unregister_connection(Address, Port),
	    gen_tcp:close(Socket);
	{error, Reason} ->
	    log:log(error,"[ CC ] couldn't send to ~p:~p (~p)", [Address, Port, Reason]),
	    comm_port:unregister_connection(Address, Port),
	    gen_tcp:close(Socket)
    end.

loop(fail, Address, Port) ->
    comm_port:unregister_connection(Address, Port),
    ok;
loop(Socket, Address, Port) ->
    receive
	{send, Pid, Message} ->
	    case send({Address, Port, Socket}, Pid, Message) of
		ok -> loop(Socket, Address, Port);
		_ -> ok
	    end;
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
		{youare, _Address, _Port} ->
		    %% @TODO what do we get from this information?
		    inet:setopts(Socket, [{active, once}]),
		    loop(Socket, Address, Port);
		Unknown ->
		    log:log(warn,"[ CC ] unknown message ~p", [Unknown]),
		    inet:setopts(Socket, [{active, once}]),
		    loop(Socket, Address, Port)
	    end;

	{youare, _IP, _Port} ->
	    loop(Socket, Address, Port);

        Unknown ->
	    log:log(warn,"[ CC ] unknown message2 ~p", [Unknown]) ,
	    loop(Socket, Address, Port)
    end.

%% @spec new_connection(inet:ip_address(), int(), int()) -> inet:socket() | fail
new_connection(Address, Port, MyPort) ->
    case gen_tcp:connect(Address, Port, [binary, {packet, 4}, {nodelay, true}, {active, once}, 
					 {send_timeout, config:read(tcp_send_timeout)}], 
			 config:read(tcp_connect_timeout)) of
        {ok, Socket} ->
                                                % send end point data
	    case inet:sockname(Socket) of
		{ok, {MyAddress, _MyPort}} ->
	            gen_tcp:send(Socket, term_to_binary({endpoint, MyAddress, MyPort})),
		    case inet:peername(Socket) of
			{ok, {RemoteIP, RemotePort}} ->
		            gen_tcp:send(Socket, term_to_binary({youare, RemoteIP, RemotePort})),
		            Socket;
			{error, Reason} ->
			   log:log(error,"[ CC ] reconnect to ~p because socket is ~p", [Address, Reason]),
			    gen_tcp:close(Socket),
	    		    new_connection(Address, Port, MyPort)
		    end;
		{error, Reason} ->
		    log:log(error,"[ CC ] reconnect to ~p because socket is ~p", [Address, Reason]),
		    gen_tcp:close(Socket),
	            new_connection(Address, Port, MyPort)
	    end;
        {error, Reason} ->
            log:log(error,"[ CC ] couldn't connect to ~p:~p (~p)", [Address, Port, Reason]),
	    fail
    end.
    
