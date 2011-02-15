%% @copyright 2007-2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%%      of a connection where messages are received and send from/to the
%%      network.
%% @end
%% @version $Id$
-module(comm_connection).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), ct:pal(X,Y)).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/3, % comm_server send
         start_link/4, % for accepted connections with established Socket
         init/1, on/2]).

-export([send/3]).%, open_new/4, new/3, open_new_async/4]).

%% be startable via supervisor, use gen_component

-spec start_link(pid_groups:groupname(),
                 inet:ip_address(), integer()) -> {ok, pid()}.
start_link(CommLayerGroup, DestIP, DestPort) ->
  start_link(CommLayerGroup, DestIP, DestPort, notconnected).

-spec start_link(pid_groups:groupname(),
                 inet:ip_address(), integer(), inet:socket() | notconnected) -> {ok, pid()}.
start_link(CommLayerGroup, DestIP, DestPort, Socket) ->
    {IP1, IP2, IP3, IP4} = DestIP,
    {_, LocalListenPort} = comm_server:get_local_address_port(),
    PidNamePrefix = case Socket of
                        notconnected -> "to > ";
                        _ -> "from > "
                    end,
    PidName = PidNamePrefix ++ integer_to_list(IP1) ++ "."
        ++ integer_to_list(IP2) ++ "." ++ integer_to_list(IP3) ++ "."
        ++ integer_to_list(IP4) ++ ":" ++ integer_to_list(DestPort),
    gen_component:start_link(?MODULE,
                             [DestIP, DestPort, LocalListenPort, Socket],
                             [ {pid_groups_join_as, CommLayerGroup, PidName}
                             ]).

%% @doc initialize: return initial state.
-spec init([inet:ip_address()| integer()| inet:socket()| notconnected]) -> any().
init([DestIP, DestPort, LocalListenPort, Socket]) ->
    state_new(DestIP, DestPort, LocalListenPort, Socket).

%% @doc message handler
-spec on(term(), term()) -> term().
on({send, DestPid, Message}, State) ->
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
            log:log(warn, "~.0p Connection failed, drop message ~.0p",
                    [pid_groups:my_pidname(), Message]),
            %%reconnect
            set_socket(State, notconnected);
        _ ->
            {_, MQL} = process_info(self(), message_queue_len),
            QL = msg_queue_len(State),
            case QL of
                0 ->
                    if MQL > 4 ->
                            %% start message bundle for sending
                            %% io:format("MQL ~p~n", [MQL]),
                            T1 = set_msg_queue(State, [{DestPid, Message}]),
                            T2 = set_msg_queue_len(T1, QL + 1),
                            set_desired_bundle_size(T2, util:min(MQL,200));
                       true ->
                            NewSocket =
                                send({dest_ip(State), dest_port(State), Socket},
                                     DestPid, Message),
                            T1 = set_socket(State, NewSocket),
                            inc_s_msg_count(T1)
                    end;
                _ ->
                    case QL >= desired_bundle_size(State) orelse 0 =:= MQL of
                        true ->
                            MQueue = [{DestPid, Message} | msg_queue(State)],
                            NewSocket =
                                send({dest_ip(State), dest_port(State), Socket},
                                     unpack_msg_bundle, MQueue),
                            T1State = set_socket(State, NewSocket),
                            T2State = inc_s_msg_count(T1State),
                            T3State = set_msg_queue(T2State, []),
                            _T4State = set_msg_queue_len(T3State, 0);
                        false ->
                            %% add to message bundle
                            T1 = set_msg_queue(State, [{DestPid, Message}
                                                       | msg_queue(State)]),
                            set_msg_queue_len(T1, QL + 1)
                    end
            end
    end;

on({tcp, Socket, Data}, State) ->
    NewState =
        case binary_to_term(Data) of
            {deliver, unpack_msg_bundle, Message} ->
                lists:foldr(fun({DestPid, Msg}, _) -> DestPid ! Msg, ok end,
                            ok, Message),
                ok = inet:setopts(Socket, [{active, once}]),
                inc_r_msg_count(State);
            {deliver, Process, Message} ->
                PID = case is_pid(Process) of
                          true -> Process;
                          false -> whereis(Process)
                      end,
                case PID of
                    undefined ->
                        log:log(warn,
                                "[ CC ] Cannot accept msg for unknown named"
                                " process ~p: ~.0p~n", [Process, Message]);
                    _ -> PID ! Message
                end,
                ok = inet:setopts(Socket, [{active, once}]),
                inc_r_msg_count(State);
            {user_close} ->
                log:log(warn,"[ CC ] tcp user_close request", []),
                gen_tcp:close(Socket),
                set_socket(State, notconnected);
            {youare, _Address, _Port} ->
                %% @TODO what info do we get from this message?
                ok = inet:setopts(Socket, [{active, once}]),
                State;
            Unknown ->
                log:log(warn,"[ CC ] unknown message ~.0p", [Unknown]),
                ok = inet:setopts(Socket, [{active, once}]),
                State
    end,
    NewState;

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
          lists:flatten(io_lib:format("~p", [s_msg_count(State) / Runtime]))},
         {"received_tcp_messages",
          lists:flatten(io_lib:format("~p", [r_msg_count(State)]))},
         {"~ received messages/s",
          lists:flatten(io_lib:format("~p", [r_msg_count(State) / Runtime]))}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

-spec send({inet:ip_address(), integer(), inet:socket()}, pid(), term()) ->
                   notconnected | port();
          ({inet:ip_address(), integer(), inet:socket()}, unpack_msg_bundle, [{pid(), term()}]) ->
                   notconnected | port().
send({Address, Port, Socket}, Pid, Message) ->
    BinaryMessage = term_to_binary({deliver, Pid, Message}, [{compressed, 2}]),
    NewSocket =
        case gen_tcp:send(Socket, BinaryMessage) of
            ok ->
                ?TRACE("~.0p Sent message ~.0p~n",
                       [pid_groups:my_pidname(), Message]),
                ?LOG_MESSAGE(Message, byte_size(BinaryMessage)),
                Socket;
            {error, closed} ->
                log:log(warn,"[ CC ] sending closed connection", []),
                gen_tcp:close(Socket),
                notconnected;
            {error, timeout} ->
                log:log(error,"[ CC ] couldn't send to ~.0p:~.0p (~.0p). retrying.",
                        [Address, Port, timeout]),
                send({Address, Port, Socket}, Pid, Message);
            {error, Reason} ->
                log:log(error,"[ CC ] couldn't send to ~.0p:~.0p (~.0p). closing connection",
                        [Address, Port, Reason]),
                gen_tcp:close(Socket),
                notconnected
    end,
    NewSocket.

-spec(new_connection(inet:ip_address(), integer(), integer()) -> inet:socket() | fail).
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
                    Message = term_to_binary({endpoint, MyAddress, MyPort}),
                    _ = gen_tcp:send(Socket, Message),
                    case inet:peername(Socket) of
                        {ok, {RemoteIP, RemotePort}} ->
                            YouAre = term_to_binary({youare, RemoteIP, RemotePort}),
                            _ = gen_tcp:send(Socket, YouAre),
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

state_new(DestIP, DestPort, LocalListenPort, Socket) ->
    {DestIP, DestPort, LocalListenPort, Socket,
     _StartTime = erlang:now(), _SentMsgCount = 0,
     _ReceivedMsgCount = 0, _MsgQueue = [], _Len = 0,
    _DesiredBundleSize = 0}.

dest_ip(State)           -> element(1, State).
dest_port(State)         -> element(2, State).
local_listen_port(State) -> element(3, State).
socket(State)            -> element(4, State).
set_socket(State, Val)   -> setelement(4, State, Val).
started(State)           -> element(5, State).
s_msg_count(State)         -> element(6, State).
inc_s_msg_count(State)     -> setelement(6, State, s_msg_count(State) + 1).
r_msg_count(State)         -> element(7, State).
inc_r_msg_count(State)     -> setelement(7, State, r_msg_count(State) + 1).
msg_queue(State)           -> element(8, State).
set_msg_queue(State, Val)  -> setelement(8, State, Val).
msg_queue_len(State)       -> element(9, State).
set_msg_queue_len(State, Val) -> setelement(9, State, Val).
desired_bundle_size(State)    -> element(10, State).
set_desired_bundle_size(State, Val) -> setelement(10, State, Val).

status(State) ->
     case socket(State) of
         notconnected ->
              notconnected;
         _ -> connected
     end.
