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
%%% File    : comm_ssl_acceptor.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : SSL Acceptor
%%%           This module accepts new SSL connections and starts corresponding 
%%%           comm_connection processes.
%%%
%%% Created : 16 Nov 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(comm_ssl_acceptor).

-export([start_link/1, init/2]).

% start a new acceptor for SSL connections
start_link(InstanceId) ->
    case config:read(listen_ssl_port) of
        none ->
            ignore;
        _ ->
            Pid = spawn_link(comm_ssl_acceptor, init, [InstanceId, self()]),
            receive
                {started} ->
                    {ok, Pid}
            end
    end.

init(InstanceId, Supervisor) ->
    process_dictionary:register_process(InstanceId, ssl_acceptor, self()),
    erlang:register(comm_layer_ssl_acceptor, self()),
    log:log(info,"[ CC ] ssl listening on ~p:~p", [config:listenIP(), config:read(listen_ssl_port)]),
    LS = case config:listenIP() of
             undefined ->
                 open_listen_port(config:read(listen_ssl_port), first_ip());
             _ ->
                 open_listen_port(config:read(listen_ssl_port), config:listenIP())
         end,
    Supervisor ! {started},
    log:log(info,"[ CC ] ssl listener started", []),
    server(LS).

% the server loop
% accepts one ssl connection after the other
server(LS) ->
    case ssl:transport_accept(LS) of
	{ok, TransportSocket} ->
            case ssl:ssl_accept(TransportSocket) of
                ok ->
                    Verifier = config:read(ssl_verifier),
                    case Verifier:verify(ssl:peercert(TransportSocket)) of
                        deny ->
                            log:log(info, "[ CC ] denied connection from: ~p",
                                    [ssl:peername(TransportSocket)]),
                            ssl:close(TransportSocket);
                        accept ->
                            case comm_port:get_local_address_port() of
                                {undefined, LocalPort} ->
                                    {ok, {MyIP, _LocalPort}} = inet:sockname(TransportSocket),
                                    comm_port:set_local_address(MyIP, LocalPort);
                                _ ->
                                    ok
                            end,
                            receive
                                {tcp, TransportSocket, Msg} ->
                                    {endpoint, Address, Port} = binary_to_term(Msg),
                                    % auto determine remote address, when not sent correctly
                                    NewAddress = if Address =:= {0,0,0,0} orelse Address =:= {127,0,0,1} ->  
                                                         case inet:peername(TransportSocket) of
                                                             {ok, {PeerAddress, _Port}} -> 
                                                                 PeerAddress;
                                                             {error, _Why} ->
                                                                 Address
                                                         end;
                                                    true ->
                                                         Address
                                                 end,
                                    NewPid = comm_connection:new(NewAddress, Port, TransportSocket),
                                    gen_tcp:controlling_process(TransportSocket, NewPid),
                                    inet:setopts(TransportSocket, comm_connection:tcp_options()),
                                    comm_port:register_connection(NewAddress, Port, NewPid, TransportSocket)
                            end
                    end,
                    server(LS);
                Other ->
                    log:log(warn,"[ CC ] ssl_accept failed: ~p", [Other])
            end;
        Other ->
            log:log(warn,"[ CC ] transport_accept failed: ~p", [Other])
    end.

open_listen_port({From, To}, IP) ->
    open_listen_port(lists:seq(From, To), IP);
open_listen_port([Port | Rest], IP) ->
    CACertFile = config:read_required(ca_cert_file),
    Res = try
              ssl:listen(Port, [binary, {ssl_imp, new}, {packet, 4},
                                {reuseaddr, true}, {active, once}, {ip, IP},
                                % @TODO SSL options
                                {cacertfile, CACertFile}])
          catch Ex ->
                  Ex
          end,
    case Res of
        {ok, Socket} ->
            Socket;
        {error, Reason} ->
            log:log(error,"[ CC ] can't listen on ~p: ~p~n", [Port, Reason]),
            open_listen_port(Rest, IP)
    end;
open_listen_port([], _) ->
    abort;
open_listen_port(Port, IP) ->
    open_listen_port([Port], IP).

-include_lib("kernel/include/inet.hrl").

first_ip() ->
    {ok, Hostname} = inet:gethostname(),
    {ok, HostEntry} = inet:gethostbyname(Hostname),
    erlang:hd(HostEntry#hostent.h_addr_list).

