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
%% @version $Id $
-module(comm_server).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1, init/1, on/2]).

-export([send/2]).

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
    ets:new(?MODULE, [set, protected, named_table]),
    _State = null.

-spec send({inet:ip_address(), integer(), pid()}, term()) -> ok.
send({Address, Port, Pid}, Message) ->
    ?MODULE ! {send, Address, Port, Pid, Message}, ok.

-spec unregister_connection(inet:ip_address(), integer()) -> ok.
unregister_connection(Adress, Port) ->
    ?MODULE ! {unregister_conn, Adress, Port}, ok.

-spec register_connection(inet:ip_address(), integer(),
                          pid(), inet:socket()) -> ok.
register_connection(Adress, Port, Pid, Socket) ->
    ?MODULE ! {register_conn, Adress, Port, Pid, Socket}, ok.

-spec set_local_address(inet:ip_address() | undefined, integer()) -> ok.
set_local_address(Address, Port) ->
    ?MODULE ! {set_local_address, Address, Port}, ok.

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
on({send, Address, Port, Pid, Message}, State) ->
    send(Address, Port, Pid, Message, State);

on({unregister_conn, Address, Port}, State) ->
    ets:delete(?MODULE, {Address, Port}),
    State;

on({register_conn, Address, Port, Pid, Socket}, State) ->
    case ets:lookup(?MODULE, {Address, Port}) of
        [] -> ets:insert(?MODULE, {{Address, Port}, {Pid, Socket}});
        _ -> ok
    end,
    State;

on({set_local_address, Address, Port}, State) ->
    ets:insert(?MODULE, {local_address_port, {Address, Port}}),
    State.


%% Internal functions

send(Address, Port, Pid, Message, State) ->
    {DepAddr,DepPort} = get_local_address_port(),
    if
        DepAddr =:= undefined ->
            open_sync_connection(Address, Port, Pid, Message, State);
        true ->
            case ets:lookup(?MODULE, {Address, Port}) of
                [{{Address, Port}, {ConnPid, _Socket}}] ->
                    ConnPid ! {send, Pid, Message};
                [] ->
                    ConnPid = comm_connection:open_new_async(Address, Port,
                                                             DepAddr, DepPort),
                    ets:insert(?MODULE, {{Address, Port}, {ConnPid, undef}}),
                    ConnPid ! {send, Pid, Message}
            end
    end,
    State.

open_sync_connection(Address, Port, Pid, Message, State) ->
    {DepAddr,DepPort} = get_local_address_port(),
    case comm_connection:open_new(Address, Port, DepAddr, DepPort) of
        {local_ip, MyIP, MyPort, MyPid, MySocket} ->
            comm_connection:send({Address, Port, MySocket}, Pid, Message),
            log:log(info,"[ CC ] this() == ~w", [{MyIP, MyPort}]),
                                                %                   set_local_address(t, {MyIP,MyPort}}),
                                                %                   register_connection(Address, Port, MyPid, MySocket),
            ets:insert(?MODULE, {local_address_port, {MyIP,MyPort}}),
            ets:insert(?MODULE, {{Address, Port}, {MyPid, MySocket}}),
            {reply, ok, State};
        fail ->
                                                % drop message (remote node not reachable, failure detector will notice)
            {reply, fail, State};
        {connection, LocalPid, NewSocket} ->
            comm_connection:send({Address, Port, NewSocket}, Pid, Message),
            ets:insert(?MODULE, {{Address, Port}, {LocalPid, NewSocket}}),
                                                %                   register_connection(Address, Port, LPid, NewSocket),
            {reply, ok, State}
    end.
