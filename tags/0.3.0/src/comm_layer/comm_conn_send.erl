%% @copyright 2007-2011 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @author Florian Schintke <schintke@zib.de>

%% @doc creates and destroys connections and represents the endpoint
%%      of a connection where messages are send to the network.
%% @end
%% @version $Id$
-module(comm_conn_send).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), ct:pal(X,Y)).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/3, init/1, on/2]).

-type state() ::
    {DestIP               :: inet:ip_address(),
     DestPort             :: comm_server:tcp_port(),
     LocalListenPort      :: comm_server:tcp_port(),
     Socket               :: inet:socket() | notconnected,
     StartTime            :: util:time(),
     SentMsgCount         :: non_neg_integer(),
     MsgQueue             :: [{DestPid::pid(), Message::comm:message()}],
     MsgQueueLen          :: non_neg_integer(),
     DesiredBundleSize    :: non_neg_integer(),
     MsgsSinceBundleStart :: non_neg_integer()}.
-type message() ::
    {send, DestPid::pid(), Message::comm:message()} |
    {tcp, Socket::inet:socket(), Data::binary()} |
    {tcp_closed, Socket::inet:socket()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%% be startable via supervisor, use gen_component

-spec start_link(pid_groups:groupname(), inet:ip_address(),
                 comm_server:tcp_port()) -> {ok, pid()}.
start_link(CommLayerGroup, DestIP, DestPort) ->
    {IP1, IP2, IP3, IP4} = DestIP,
    {_, LocalListenPort} = comm_server:get_local_address_port(),
    PidName = "to > " ++ integer_to_list(IP1) ++ "."
        ++ integer_to_list(IP2) ++ "." ++ integer_to_list(IP3) ++ "."
        ++ integer_to_list(IP4) ++ ":" ++ integer_to_list(DestPort),
    gen_component:start_link(?MODULE,
                             {DestIP, DestPort, LocalListenPort},
                             [{pid_groups_join_as, CommLayerGroup, PidName}]).

%% @doc initialize: return initial state.
-spec init({DestIP::inet:ip_address(), DestPort::comm_server:tcp_port(),
            LocalListenPort::comm_server:tcp_port()}) -> state().
init({DestIP, DestPort, LocalListenPort}) ->
    state_new(DestIP, DestPort, LocalListenPort).

%% @doc message handler
-spec on(message(), state()) -> state().
on({send, DestPid, Message, Shepherd}, State) ->
    Socket = case socket(State) of
                 notconnected ->
                     log:log(info, "Connecting to ~.0p:~.0p", [dest_ip(State), dest_port(State)]),
                     new_connection(dest_ip(State),
                                    dest_port(State),
                                    local_listen_port(State));
                 S -> S
             end,
    case Socket of
        fail ->
            comm_layer:report_send_error(Shepherd,
                                         {dest_ip(State), dest_port(State), DestPid},
                                         Message),
            %%reconnect
            set_socket(State, notconnected);
        _ ->
            case msg_queue_len(State) of
                0 ->
                    {_, MQL} = process_info(self(), message_queue_len),
                    if MQL > 5 ->
                            %% start message bundle for sending
                            %% io:format("MQL ~p~n", [MQL]),
                            MaxBundle = util:max(200, MQL div 100),
                            T1 = set_msg_queue(State, {[{DestPid, Message}], [Shepherd]}),
                            T2 = set_msg_queue_len(T1, 1),
                            set_desired_bundle_size(T2, util:min(MQL,MaxBundle));
                       true ->
                            NewSocket =
                                send({dest_ip(State), dest_port(State), Socket},
                                     DestPid, Message, Shepherd),
                            T1 = set_socket(State, NewSocket),
                            inc_s_msg_count(T1)
                    end;
                QL ->
                    DBS = desired_bundle_size(State),
                    MSBS = msgs_since_bundle_start(State),
                    case (QL + MSBS) >= DBS of
                        true ->
                            {MsgQueue, ShepherdQueue} = msg_queue(State),
                            MQueue = [{DestPid, Message} | MsgQueue],
                            SQueue = [Shepherd | ShepherdQueue],
%%                            io:format("Bundle Size: ~p~n", [length(MQueue)]),
                            NewSocket =
                                send({dest_ip(State), dest_port(State), Socket},
                                     unpack_msg_bundle, MQueue, SQueue),
                            T1State = set_socket(State, NewSocket),
                            T2State = inc_s_msg_count(T1State),
                            T3State = set_msg_queue(T2State, {[], []}),
                            T4State = set_msg_queue_len(T3State, 0),
                            _T5State = set_msgs_since_bundle_start(T4State,0);
                        false ->
                            %% add to message bundle
                            {MsgQueue, ShepherdQueue} = msg_queue(State),
                            T1 = set_msg_queue(State, {[{DestPid, Message} | MsgQueue],
                                                       [Shepherd | ShepherdQueue]}),
                            set_msg_queue_len(T1, QL + 1)
                    end
            end
    end;

on({tcp, Socket, Data}, State) ->
    NewState =
        case binary_to_term(Data) of
            {user_close} ->
                log:log(warn,"[ CC ] tcp user_close request", []),
                gen_tcp:close(Socket),
                set_socket(State, notconnected);
            Unknown ->
                log:log(warn,"[ CC ] unknown message ~.0p", [Unknown]),
                %% may fail, when tcp just closed
                _ = inet:setopts(Socket, [{active, once}]),
                State
    end,
    send_bundle_if_ready(NewState);

on({tcp_closed, Socket}, State) ->
    log:log(warn,"[ CC ] tcp closed info", []),
    gen_tcp:close(Socket),
    set_socket(State, notconnected);

on({web_debug_info, Requestor}, State) ->
    Now = erlang:now(),
    Runtime = timer:now_diff(Now, started(State)) / 1000000,
    KeyValueList =
        [
         {"status",
          lists:flatten(io_lib:format("~p", [status(State)]))},
         {"running since (s)",
          lists:flatten(io_lib:format("~p", [Runtime]))},
         {"sent_tcp_messages",
          lists:flatten(io_lib:format("~p", [s_msg_count(State)]))},
         {"~ sent messages/s",
          lists:flatten(io_lib:format("~p", [s_msg_count(State) / Runtime]))}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    send_bundle_if_ready(State);

on(UnknownMessage, State) ->
    %% we want to count messages, so we need this default handler.
    log:log(error,"unknown message: ~.0p~n in Module: ~p and handler ~p~n in State ~.0p",[UnknownMessage,?MODULE,on,State]),
    send_bundle_if_ready(State).

-spec send({inet:ip_address(), comm_server:tcp_port(), inet:socket()}, pid(), comm:message(), comm:erl_local_pid() | unknown) ->
                   notconnected | inet:socket();
          ({inet:ip_address(), comm_server:tcp_port(), inet:socket()}, unpack_msg_bundle, [{pid(), comm:message()}], list(comm:erl_local_pid() | unknown)) ->
                   notconnected | inet:socket().
send({Address, Port, Socket}, Pid, Message, Shepherd) ->
    BinaryMessage = term_to_binary({deliver, Pid, Message},
                                   [{compressed, 2}, {minor_version, 1}]),
    NewSocket =
        case gen_tcp:send(Socket, BinaryMessage) of
            ok ->
                ?TRACE("~.0p Sent message ~.0p~n",
                       [pid_groups:my_pidname(), Message]),
                ?LOG_MESSAGE(Message, byte_size(BinaryMessage)),
                Socket;
            {error, closed} ->
                report_bundle_error(Shepherd,
                                    {Address, Port, Pid},
                                    Message),
                log:log(warn,"[ CC ] sending closed connection", []),
                gen_tcp:close(Socket),
                notconnected;
            {error, timeout} ->
                log:log(error,"[ CC ] couldn't send to ~.0p:~.0p (~.0p). retrying.",
                        [Address, Port, timeout]),
                send({Address, Port, Socket}, Pid, Message, Shepherd);
            {error, Reason} ->
                report_bundle_error(Shepherd,
                                    {Address, Port, Pid},
                                    Message),
                log:log(error,"[ CC ] couldn't send to ~.0p:~.0p (~.0p). closing connection",
                        [Address, Port, Reason]),
                gen_tcp:close(Socket),
                notconnected
    end,
    NewSocket.

-spec new_connection(inet:ip_address(), comm_server:tcp_port(), comm_server:tcp_port()) -> inet:socket() | fail.
new_connection(Address, Port, MyPort) ->
    case gen_tcp:connect(Address, Port, [binary, {packet, 4}]
                         ++ comm_server:tcp_options(),
                         config:read(tcp_connect_timeout)) of
        {ok, Socket} ->
            % send end point data
            case inet:sockname(Socket) of
                {ok, {MyAddress, _SocketPort}} ->
                    case comm_server:get_local_address_port() of
                        {undefined,_} ->
                            comm_server:set_local_address(MyAddress, MyPort);
                        _ -> ok
                    end,
                    case inet:peername(Socket) of
                        {ok, {_RemoteIP, _RemotePort}} ->
                            Socket;
                        {error, Reason} ->
                            log:log(error,"[ CC ] reconnect to ~.0p because socket is ~.0p",
                                    [Address, Reason]),
                            gen_tcp:close(Socket),
                            new_connection(Address, Port, MyPort)
                    end;
                {error, Reason} ->
                    log:log(error,"[ CC ] reconnect to ~.0p because socket is ~.0p",
                            [Address, Reason]),
                    gen_tcp:close(Socket),
                    new_connection(Address, Port, MyPort)
            end;
        {error, Reason} ->
            log:log(info,"[ CC ] couldn't connect to ~.0p:~.0p (~.0p)",
                    [Address, Port, Reason]),
            fail
    end.

send_bundle_if_ready(InState) ->
    QL = msg_queue_len(InState),
    case QL of
        0 -> InState;
        _ ->
            State = inc_msgs_since_bundle_start(InState),
            DBS = desired_bundle_size(State),
            MSBS = msgs_since_bundle_start(State),
            case (QL + MSBS) >= DBS of
                true ->
                    Socket = socket(State),
                    %% io:format("Sending packet with ~p msgs~n", [length(msg_queue(State))]),
                    {MQueue, SQueue} = msg_queue(State),
                    NewSocket =
                        send({dest_ip(State), dest_port(State), Socket},
                             unpack_msg_bundle, MQueue, SQueue),
                    T1State = set_socket(State, NewSocket),
                    T2State = inc_s_msg_count(T1State),
                    T3State = set_msg_queue(T2State, {[], []}),
                    T4State = set_msg_queue_len(T3State, 0),
                    _T5State = set_msgs_since_bundle_start(T4State, 0);
                false -> State
            end
    end.

-spec state_new(DestIP::inet:ip_address(), DestPort::comm_server:tcp_port(),
                LocalListenPort::comm_server:tcp_port()) -> state().
state_new(DestIP, DestPort, LocalListenPort) ->
    {DestIP, DestPort, LocalListenPort, notconnected,
     _StartTime = erlang:now(), _SentMsgCount = 0,
     _MsgQueue = [], _Len = 0,
     _DesiredBundleSize = 0, _MsgsSinceBundleStart = 0}.

dest_ip(State)                -> element(1, State).
dest_port(State)              -> element(2, State).
local_listen_port(State)      -> element(3, State).
socket(State)                 -> element(4, State).
set_socket(State, Val)        -> setelement(4, State, Val).
started(State)                -> element(5, State).
s_msg_count(State)            -> element(6, State).
inc_s_msg_count(State)        -> setelement(6, State, s_msg_count(State) + 1).
msg_queue(State)              -> element(7, State).
set_msg_queue(State, Val)     -> setelement(7, State, Val).
msg_queue_len(State)          -> element(8, State).
set_msg_queue_len(State, Val) -> setelement(8, State, Val).
desired_bundle_size(State)    -> element(9, State).
set_desired_bundle_size(State, Val) -> setelement(9, State, Val).
msgs_since_bundle_start(State) ->
    element(10, State).
inc_msgs_since_bundle_start(State) ->
    setelement(10, State, msgs_since_bundle_start(State) + 1).
set_msgs_since_bundle_start(State, Val) ->
    setelement(10, State, Val).

status(State) ->
     case socket(State) of
         notconnected -> notconnected;
         _            -> connected
     end.

report_bundle_error(Shepherd, {Address, Port, Pid}, Message) ->
    case is_list(Shepherd) of
        true ->
            zip_and_foldr(fun (ShepherdX, {DestPid, MessageX}) ->
                                  comm_layer:report_send_error(ShepherdX,
                                                               {Address, Port, DestPid},
                                                               MessageX)
                          end, Shepherd, Message);
        false ->
            comm_layer:report_send_error(Shepherd,
                                         {Address, Port, Pid},
                                         Message)
    end.

zip_and_foldr(_F, [], []) ->
    ok;
zip_and_foldr(F, [El1 | R1] , [El2 | R2]) ->
    zip_and_foldr(F, R1, R2),
    F(El1, El2).
