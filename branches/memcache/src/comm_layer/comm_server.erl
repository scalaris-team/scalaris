% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-export([start_link/1, init/1, on/2]).
-export([send/2, tcp_options/0]).
-export([unregister_connection/2, register_connection/4,
        set_local_address/2, get_local_address_port/0]).

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(CommLayerGroup) ->
    gen_component:start_link(?MODULE,
                             [],
                             [ {erlang_register, ?MODULE},
                               {pid_groups_join_as, CommLayerGroup, ?MODULE}
                             ]).

%% @doc initialize: return initial state.
-spec init([]) -> any().
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

-spec send({inet:ip_address(), integer(), pid()}, term()) -> ok.
send({Address, Port, Pid}, Message) ->
    ConnPid =
        case ets:lookup(?MODULE, {Address, Port}) of
            [{{Address, Port}, X}] -> X;
            [] ->
                %% start Erlang process responsible for the connection
                ?MODULE ! {comm_server_create_connection,
                           Address, Port, self()},
                receive {comm_server_create_connection_done, X} -> ok end,
                X
    end,
    ConnPid ! {send, Pid, Message},
    ok.

-spec unregister_connection(inet:ip_address(), integer()) -> ok.
unregister_connection(Adress, Port) ->
    ?MODULE ! {unregister_conn, Adress, Port, self()},
    receive {unregister_conn_done} ->  ok end.

-spec register_connection(inet:ip_address(), integer(),
                          pid(), inet:socket()) -> ok.
register_connection(Adress, Port, Pid, Socket) ->
    ?MODULE ! {register_conn, Adress, Port, Pid, Socket, self()},
    receive {register_conn_done} -> ok end.

-spec set_local_address(inet:ip_address() | undefined, integer()) -> ok.
set_local_address(Address, Port) ->
    ?MODULE ! {set_local_address, Address, Port, self()},
    receive {set_local_address_done} -> ok end.

%% @doc returns the local ip address and port
-spec(get_local_address_port() -> {inet:ip_address(),integer()}
                                      | undefined
                                      | {undefined, integer()}).
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

%% @doc message handler
-spec on(term(), term()) -> term().
on({comm_server_create_connection, Address, Port, ClientPid}, State) ->
    case ets:lookup(?MODULE, {Address, Port}) of
        [] ->
            {ok, Pid} = comm_connection:start_link(pid_groups:my_groupname(),
                                                   Address, Port),
            ets:insert(?MODULE, {{Address, Port}, Pid});
        [{_, Pid}] -> ok
    end,
    ClientPid ! {comm_server_create_connection_done, Pid},
    State;

on({unregister_conn, Address, Port, Client}, State) ->
    ets:delete(?MODULE, {Address, Port}),
    Client ! {unregister_conn_done},
    State;

on({register_conn, Address, Port, Pid, _Socket, Client}, State) ->
    case ets:lookup(?MODULE, {Address, Port}) of
        [] -> ets:insert(?MODULE, {{Address, Port}, Pid});
        _ -> ok
    end,
    Client ! {register_conn_done},
    State;

on({set_local_address, Address, Port, Client}, State) ->
    ets:insert(?MODULE, {local_address_port, {Address, Port}}),
    Client ! {set_local_address_done},
    State.
