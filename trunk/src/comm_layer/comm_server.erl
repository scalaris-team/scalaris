% @copyright 2008-2011 Zuse Institute Berlin

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

%% @doc: CommLayer: Management of comm_connection processes.
%% @author Thorsten Schuett <schuett@zib.de>
%% @author Florian Schintke <schintke@zib.de>
%% @version $Id$
-module(comm_server).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([tcp_port/0]).
-endif.

-export([start_link/1, init/1, on/2]).
-export([send/3, tcp_options/0]).
-export([unregister_connection/2, create_connection/3,
         set_local_address/2, get_local_address_port/0]).

-type tcp_port() :: 0..65535.
-type message() ::
    {create_connection, Address::inet:ip_address(), Port::tcp_port(), Socket::inet:socket() | notconnected, Client::pid()} |
    {send, Address::inet:ip_address(), Port::tcp_port(), Pid::pid(), Message::comm:message()} |
    {unregister_conn, Address::inet:ip_address(), Port::tcp_port(), Client::pid()} |
    {set_local_address, Address::inet:ip_address(), Port::tcp_port(), Client::pid()}.

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(CommLayerGroup) ->
    gen_component:start_link(?MODULE,
                             [],
                             [ {erlang_register, ?MODULE},
                               {pid_groups_join_as, CommLayerGroup, ?MODULE}
                             ]).

%% @doc initialize: return initial state.
-spec init([]) -> null.
init([]) ->
    _ = ets:new(?MODULE, [set, protected, named_table]),
    _State = null.

-spec tcp_options() -> [{term(), term()}].
tcp_options() ->
%    [{active, once}, {nodelay, true}, {send_timeout, config:read(tcp_send_timeout)}].
    [{active, once},
     {nodelay, true},
     {keepalive, true},
     {reuseaddr, true},
     {delay_send, true},
     {send_timeout, config:read(tcp_send_timeout)}
].

-spec send({inet:ip_address(), tcp_port(), pid()}, term(), comm:send_options()) -> ok.
send({Address, Port, Pid}, Message, Options) ->
    ?MODULE ! {send, Address, Port, Pid, Message, Options},
    ok.

%% @doc Synchronous call to create (or get) a connection for the given Address+Port using Socket.
-spec create_connection(Address::inet:ip_address(), Port::tcp_port(), Socket::inet:socket()) -> pid().
create_connection(Address, Port, Socket) ->
    ?MODULE ! {create_connection, Address, Port, Socket, self()},
    receive {create_connection_done, ConnPid} -> ConnPid end.

%% @doc Synchronous call to de-register a connection with the comm server.
-spec unregister_connection(inet:ip_address(), tcp_port()) -> ok.
unregister_connection(Adress, Port) ->
    ?MODULE ! {unregister_conn, Adress, Port, self()},
    receive {unregister_conn_done} -> ok end.

-spec set_local_address(inet:ip_address() | undefined, tcp_port()) -> ok.
set_local_address(Address, Port) ->
    ?MODULE ! {set_local_address, Address, Port, self()},
    receive {set_local_address_done} -> ok end.

%% @doc returns the local ip address and port
-spec(get_local_address_port() -> {inet:ip_address(), tcp_port()}
                                      | undefined
                                      | {undefined, tcp_port()}).
get_local_address_port() ->
    case erlang:get(local_address_port) of
        undefined ->
            case ets:lookup(?MODULE, local_address_port) of
                [{local_address_port, Value = {undefined, _MyPort}}] ->
                    Value;
                [{local_address_port, Value}] ->
                    erlang:put(local_address_port, Value),
                    Value;
                [] ->
                    undefined
            end;
        Value -> Value
    end.

%% @doc Gets or creates a connection for the given Socket or address/port.
%%      Only a single connection to any IP+Port combination is created.
%%      Socket is the initial socket when a connection needs to be created.
-spec get_connection(Address::inet:ip_address(), Port::tcp_port(),
                     Socket::inet:socket() | notconnected) -> pid().
get_connection(Address, Port, Socket) ->
    case erlang:get({Address, Port}) of
        undefined ->
            %% start Erlang process responsible for the connection
            {ok, ConnPid} = comm_connection:start_link(
                              pid_groups:my_groupname(), Address, Port, Socket),
            erlang:put({Address, Port}, ConnPid),
            ok;
        ConnPid -> ok
    end,
    ConnPid.

%% @doc message handler
-spec on(message(), State::null) -> null.
on({create_connection, Address, Port, Socket, Client}, State) ->
    % helper for comm_acceptor as we need to synchronise the creation of
    % connections in order to prevent multiple connections to/from a single IP
    ConnPid = get_connection(Address, Port, Socket),
    Client ! {create_connection_done, ConnPid},
    State;
on({send, Address, Port, Pid, Message, Options}, State) ->
    ConnPid = get_connection(Address, Port, notconnected),
    ConnPid ! {send, Pid, Message, Options},
    State;

on({unregister_conn, Address, Port, Client}, State) ->
    erlang:erase({Address, Port}),
    Client ! {unregister_conn_done},
    State;

on({set_local_address, Address, Port, Client}, State) ->
    ets:insert(?MODULE, {local_address_port, {Address, Port}}),
    Client ! {set_local_address_done},
    State.
