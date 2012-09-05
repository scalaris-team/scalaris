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

%% @doc CommLayer: Management of comm_connection processes,
%%      generic functions to send messages.  Distinguishes on runtime
%%      whether the destination is in the same Erlang virtual machine
%%      (use ! for sending) or on a remote site (send through comm_connection).
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

-export([send/3, this/0, is_valid/1, is_local/1, make_local/1,
         get_ip/1, get_port/1, report_send_error/4]).

-export([start_link/1, init/1, on/2]).
-export([tcp_options/1]).
-export([unregister_connection/2, create_connection/4,
         set_local_address/2, get_local_address_port/0]).

-type tcp_port() :: 0..65535.
-type message() ::
    {create_connection, Address::inet:ip_address(), Port::tcp_port(), Socket::inet:socket() | notconnected, Channel::main | prio, Client::pid()} |
    {send, Address::inet:ip_address(), Port::tcp_port(), Pid::pid(), Message::comm:message()} |
    {unregister_conn, Address::inet:ip_address(), Port::tcp_port(), Client::pid()} |
    {set_local_address, Address::inet:ip_address(), Port::tcp_port(), Client::pid()}.

-type process_id() ::
        {inet:ip_address(), tcp_port(), comm:erl_local_pid_plain()}.

%% @doc send message via tcp, if target is not in same Erlang VM.
-spec send(process_id(), comm:message(), comm:send_options()) -> ok.
send({{_IP1, _IP2, _IP3, _IP4} = TargetIP, TargetPort, TargetPid} = Target,
     Message, Options) ->
    % integrated is_local/1 and make_local/1:
    if is_pid(TargetPid) andalso node(TargetPid) =:= node() ->
           case is_process_alive(TargetPid) of
               true ->
                   %% minor gap of error reporting, if PID
                   %% dies at this moment, but better than a
                   %% false positive reporting when first
                   %% sending and then checking, if message
                   %% leads to process termination (as in the
                   %% RPCs of the Java binding)
                   TargetPid ! Message, ok;
               false ->
                   report_send_error(Options, Target, Message,
                                     local_target_not_alive)
           end;
       is_pid(TargetPid) ->
           ?LOG_MESSAGE('send', Message, proplists:get_value(channel, Options, main)),
           ?MODULE ! {send, TargetIP, TargetPort, TargetPid, Message, Options};
       is_atom(TargetPid) -> % named process
           {MyIP, MyPort} = get_local_address_port(),
           if MyIP =:= TargetIP andalso MyPort =:= TargetPort ->
                  case whereis(TargetPid) of
                      undefined ->
                          log:log(warn,
                                  "[ CC ] Cannot locally send msg to unknown named"
                                      " process ~p: ~.0p~n", [TargetPid, Message]),
                          report_send_error(Options, Target, Message, unknown_named_process);
                      PID ->
                          % assume that whereis/1 does not report dead PIDs
                          PID ! Message, ok
                  end;
              true ->
                  ?LOG_MESSAGE('send', Message, proplists:get_value(channel, Options, main)),
                  ?MODULE ! {send, TargetIP, TargetPort, TargetPid, Message, Options}
           end
    end.

%% @doc returns process descriptor for the calling process
-spec this() -> process_id().
this() ->
    %% Note: We had caching enabled here, but the eshell takes over
    %% the process dictionary to a new pid in case of failures, so we
    %% got outdated pid info here.
    %% case erlang:get(comm_this) of
    %%    undefined ->
    {LocalIP, LocalPort} = get_local_address_port(),
    _This1 = {LocalIP, LocalPort, self()}
    %% , case LocalIP of
    %%     undefined -> ok;
    %%     _         -> erlang:put(comm_this, This1)
    %% end,
    %% This1;
    %%     This -> This
    %% end
    .

-spec is_valid(process_id() | any()) -> boolean().
is_valid({{_IP1, _IP2, _IP3, _IP4} = _IP, _Port, _Pid}) -> true;
is_valid(_) -> false.

-spec is_local(process_id()) -> boolean().
is_local({IP, Port, _Pid}) ->
    {MyIP, MyPort} = get_local_address_port(),
    IP =:= MyIP andalso Port =:= MyPort.

-spec make_local(process_id()) -> comm:erl_local_pid_plain().
make_local({_IP, _Port, Pid}) -> Pid.

%% @doc Gets the IP address of the given process id.
-spec get_ip(process_id()) -> inet:ip_address().
get_ip({IP, _Port, _Pid}) -> IP.

%% @doc Gets the port of the given process id.
-spec get_port(process_id()) -> tcp_port().
get_port({_IP, Port, _Pid}) -> Port.

-spec report_send_error(comm:send_options(), process_id(), comm:message(), atom()) -> ok.
report_send_error(Options, Target, Message, Reason) ->
    case proplists:get_value(shepherd, Options) of
        undefined ->
            case proplists:get_bool(quiet, Options) of
                false ->
                    log:log(warn, "~p (name: ~.0p) Send to ~.0p failed, drop message ~.0p due to ~p",
                            [self(), pid_groups:my_pidname(), Target, Message, Reason]);
                _ -> ok
            end,
            ok;
        ShepherdPid ->
            comm:send_local(ShepherdPid, {send_error, Target, Message, Reason})
    end,
    ok.

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(CommLayerGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [],
                             [ {erlang_register, ?MODULE},
                               {pid_groups_join_as, CommLayerGroup, ?MODULE}
                             ]).

%% @doc initialize: return initial state.
-spec init([]) -> null.
init([]) ->
    _ = ets:new(?MODULE, [set, protected, named_table]),
    _State = null.

%% @doc message handler
-spec on(message(), State::null) -> null.
on({create_connection, Address, Port, Socket, Channel, Client}, State) ->
    % helper for comm_acceptor as we need to synchronise the creation of
    % connections in order to prevent multiple connections to/from a single IP
    {Channel, Dir} = case Channel of
                         main -> {main, 'rcv'};
                         prio -> {prio, 'both'}
                     end,
    ConnPid = get_connection(Address, Port, Socket, Channel, Dir),
    Client ! {create_connection_done, ConnPid},
    State;
on({send, Address, Port, Pid, Message, Options}, State) ->
    {Channel, Dir} = case proplists:get_value(channel, Options, main) of
                         main -> {main, 'send'};
                         prio -> {prio, 'both'}
                     end,
    ConnPid = get_connection(Address, Port, notconnected, Channel, Dir),
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

-spec tcp_options(Channel::main | prio) -> [{term(), term()}].
tcp_options(Channel) ->
    TcpSendTimeout = case Channel of
                         prio -> config:read(tcp_send_timeout);
                         main -> infinity
                     end,
    [{active, once},
     {nodelay, true},
     {keepalive, true},
     {reuseaddr, true},
     {delay_send, true},
     {send_timeout, TcpSendTimeout}].

%% @doc Synchronous call to create (or get) a connection for the given Address+Port using Socket.
-spec create_connection(Address::inet:ip_address(), Port::tcp_port(),
                        Socket::inet:socket(), Channel::main | prio) -> pid().
create_connection(Address, Port, Socket, Channel) ->
    ?MODULE ! {create_connection, Address, Port, Socket, Channel, self()},
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
                     Socket::inet:socket() | notconnected,
                     Channel::main | prio, Dir::'rcv' | 'send' | 'both') -> pid().
get_connection(Address, Port, Socket, Channel, Dir) ->
    case erlang:get({Address, Port, Channel, Dir}) of
        undefined ->
            %% start Erlang process responsible for the connection
            {ok, ConnPid} = comm_connection:start_link(
                              pid_groups:my_groupname(), Address, Port, Socket, Channel, Dir),
            erlang:put({Address, Port, Channel, Dir}, ConnPid),
            ok;
        ConnPid -> ok
    end,
    ConnPid.
