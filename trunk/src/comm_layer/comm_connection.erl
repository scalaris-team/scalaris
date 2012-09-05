%% @copyright 2007-2012 Zuse Institute Berlin

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
%%      of a connection where messages are received from and send to the
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

-export([start_link/6, init/1, on/2]).

-type state() ::
    {DestIP               :: inet:ip_address(),
     DestPort             :: comm_server:tcp_port(),
     LocalListenPort      :: comm_server:tcp_port(),
     Channel              :: main | prio,
     Socket               :: inet:socket() | notconnected,
     StartTime            :: util:time(),
     SentMsgCount         :: non_neg_integer(),
     ReceivedMsgCount     :: non_neg_integer(),
     MsgQueue             :: {MQueue::[{DestPid::pid(), Message::comm:message()}],
                              OQueue::[comm:send_options()]},
     MsgQueueLen          :: non_neg_integer(),
     DesiredBundleSize    :: non_neg_integer(),
     MsgsSinceBundleStart :: non_neg_integer(),
     LastStatReport       :: {RcvCnt::non_neg_integer(), RcvBytes::non_neg_integer(),
                              SendCnt::non_neg_integer(), SendBytes::non_neg_integer()}}.
-type message() ::
    {send, DestPid::pid(), Message::comm:message()} |
    {tcp, Socket::inet:socket(), Data::binary()} |
    {tcp_closed, Socket::inet:socket()} |
    {report_stats} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%% be startable via supervisor, use gen_component

-spec start_link(pid_groups:groupname(), DestIP::inet:ip_address(),
                 comm_server:tcp_port(), inet:socket() | notconnected,
                 Channel::main | prio, Dir::'rcv' | 'send' | 'both') -> {ok, pid()}.
start_link(CommLayerGroup, {IP1, IP2, IP3, IP4} = DestIP, DestPort, Socket, Channel, Dir) ->
    {_, LocalListenPort} = comm_server:get_local_address_port(),
    DirStr = case Dir of
                 'rcv'  -> " <-  ";
                 'send' -> "  -> ";
                 'both' -> " <-> "
             end,
    PidName = atom_to_list(Channel) ++ DirStr ++ integer_to_list(IP1) ++ "."
        ++ integer_to_list(IP2) ++ "." ++ integer_to_list(IP3) ++ "."
        ++ integer_to_list(IP4) ++ ":" ++ integer_to_list(DestPort),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             {DestIP, DestPort, LocalListenPort, Channel, Socket},
                             [{pid_groups_join_as, CommLayerGroup, PidName}]).

%% @doc initialize: return initial state.
-spec init({DestIP::inet:ip_address(), DestPort::comm_server:tcp_port(),
            LocalListenPort::comm_server:tcp_port(), Channel::main | prio,
            Socket::inet:socket() | notconnected}) -> state().
init({DestIP, DestPort, LocalListenPort, Channel, Socket}) ->
    msg_delay:send_local(10, self(), {report_stats}),
    state_new(DestIP, DestPort, LocalListenPort, Channel, Socket).

%% @doc Forwards a message to the given PID or named process.
%%      Logs a warning if a named process does not exist.
-spec forward_msg(Process::pid() | atom(), Message::comm:message(), State::state()) -> ok.
forward_msg(Process, Message, _State) ->
    ?LOG_MESSAGE('rcv', Message, channel(_State)),
    case is_pid(Process) of
        true ->
            % TODO: report error if process is not alive?
            Process ! Message, ok;
        false ->
            case whereis(Process) of
                undefined ->
                    log:log(warn,
                            "[ CC ] Cannot accept msg for unknown named"
                                " process ~p: ~.0p~n", [Process, Message]);
                PID -> PID ! Message, ok
            end
    end.

%% @doc message handler
-spec on(message(), state()) -> state().
on({send, DestPid, Message, Options}, State) ->
    Socket = case socket(State) of
                 notconnected ->
                     log:log(info, "Connecting to ~.0p:~.0p", [dest_ip(State), dest_port(State)]),
                     new_connection(dest_ip(State),
                                    dest_port(State),
                                    local_listen_port(State),
                                    proplists:get_value(channel, Options, main));
                 S -> S
             end,
    case Socket of
        fail ->
            comm_server:report_send_error(Options,
                                         {dest_ip(State), dest_port(State), DestPid},
                                         Message, tcp_connect_failed),
            %%reconnect
            set_socket(State, notconnected);
        _ ->
            case msg_queue_len(State) of
                0 ->
                    {_, MQL} = process_info(self(), message_queue_len),
                    if MQL > 5 ->
                            %% start message bundle for sending
                            %% io:format("MQL ~p~n", [MQL]),
                            MaxBundle = erlang:max(200, MQL div 100),
                            T1 = set_msg_queue(State, {[{DestPid, Message}], [Options]}),
                            T2 = set_msg_queue_len(T1, 1),
                            set_desired_bundle_size(T2, erlang:min(MQL,MaxBundle));
                       true ->
                            NewSocket =
                                send({dest_ip(State), dest_port(State), Socket},
                                     DestPid, Message, Options, State),
                            T1 = set_socket(State, NewSocket),
                            inc_s_msg_count(T1)
                    end;
                QL ->
                    DBS = desired_bundle_size(State),
                    MSBS = msgs_since_bundle_start(State),
                    case (QL + MSBS) >= DBS of
                        true ->
                            {MsgQueue, OptionQueue} = msg_queue(State),
                            MQueue = [{DestPid, Message} | MsgQueue],
                            OQueue = [Options | OptionQueue],
%%                            io:format("Bundle Size: ~p~n", [length(MQueue)]),
                            NewSocket =
                                send({dest_ip(State), dest_port(State), Socket},
                                     unpack_msg_bundle, MQueue, OQueue, State),
                            T1State = set_socket(State, NewSocket),
                            T2State = inc_s_msg_count(T1State),
                            T3State = set_msg_queue(T2State, {[], []}),
                            T4State = set_msg_queue_len(T3State, 0),
                            _T5State = set_msgs_since_bundle_start(T4State,0);
                        false ->
                            %% add to message bundle
                            {MsgQueue, OptionQueue} = msg_queue(State),
                            T1 = set_msg_queue(State, {[{DestPid, Message} | MsgQueue],
                                                       [Options | OptionQueue]}),
                            set_msg_queue_len(T1, QL + 1)
                    end
            end
    end;

on({tcp, Socket, Data}, State) ->
    NewState =
        case binary_to_term(Data) of
            {deliver, unpack_msg_bundle, Message} ->
                ?LOG_MESSAGE_SOCK('rcv', Data, byte_size(Data), channel(State)),
                ?TRACE("Received message ~.0p", [Message]),
                lists:foldr(fun({DestPid, Msg}, _) -> forward_msg(DestPid, Msg, State) end,
                            ok, Message),
                %% may fail, when tcp just closed
                _ = inet:setopts(Socket, [{active, once}]),
                inc_r_msg_count(State);
            {deliver, Process, Message} ->
                ?TRACE("Received message ~.0p", [Message]),
                ?LOG_MESSAGE_SOCK('rcv', Data, byte_size(Data), channel(State)),
                forward_msg(Process, Message, State),
                %% may fail, when tcp just closed
                _ = inet:setopts(Socket, [{active, once}]),
                inc_r_msg_count(State);
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

on({report_stats}, State) ->
    %% re-trigger
    msg_delay:send_local(10, self(), {report_stats}),
    case socket(State) of
        notconnected -> State;
        Socket ->
            case inet:getstat(Socket, [recv_cnt, recv_oct,
                                       send_cnt, send_oct]) of
                {ok, [{recv_cnt, RcvCnt}, {recv_oct, RcvBytes},
                      {send_cnt, SendCnt}, {send_oct, SendBytes}]} ->
                    PrevStat = last_stat_report(State),
                    NewStat = {RcvCnt, RcvBytes, SendCnt, SendBytes},
                    case PrevStat of
                        NewStat -> State;
                        _ ->
                            {PrevRcvCnt, PrevRcvBytes, PrevSendCnt, PrevSendBytes} = PrevStat,
                            comm:send_local(comm_stats,
                                            {report_stat, RcvCnt - PrevRcvCnt,
                                             RcvBytes - PrevRcvBytes,
                                             SendCnt - PrevSendCnt,
                                             SendBytes - PrevSendBytes}),
                            set_last_stat_report(State, NewStat)
                    end;
                {error, _Reason} -> State
            end
    end;

on({web_debug_info, Requestor}, State) ->
    Now = erlang:now(),
    Runtime = timer:now_diff(Now, started(State)) / 1000000,
    {SentPerS, ReceivedPerS} =
        if Runtime =< 0 -> {"n/a", "n/a"};
           true         -> {s_msg_count(State) / Runtime,
                            r_msg_count(State) / Runtime}
        end,
    case socket(State) of
        notconnected ->
            MyAddress = MyPort = "n/a",
            PeerAddress = PeerPort = "n/a",
            RcvAgv = RcvCnt = RcvBytes = "n/a",
            SendAgv = SendCnt = SendBytes = "n/a",
            ok;
        Socket ->
            case inet:sockname(Socket) of
                {ok, {MyAddress, MyPort}} -> ok;
                {error, _Reason1}          -> MyAddress = MyPort = "n/a"
            end,
            case inet:peername(Socket) of
                {ok, {PeerAddress, PeerPort}} -> ok;
                {error, _Reason2}              -> PeerAddress = PeerPort = "n/a"
            end,
            case inet:getstat(Socket, [recv_avg, recv_cnt, recv_oct,
                                       send_avg, send_cnt, send_oct]) of
                {ok, [{recv_avg, RcvAgv}, {recv_cnt, RcvCnt}, {recv_oct, RcvBytes},
                      {send_avg, SendAgv}, {send_cnt, SendCnt}, {send_oct, SendBytes}]} -> ok;
                {error, _Reason} ->
                    RcvAgv = RcvCnt = RcvBytes = "n/a",
                    SendAgv = SendCnt = SendBytes = "n/a"
            end
    end,
    KeyValueList =
        [
         {"status",
          webhelpers:safe_html_string("~p", [status(State)])},
         {"my IP:",
          webhelpers:safe_html_string("~p", [MyAddress])},
         {"my port",
          webhelpers:safe_html_string("~p", [MyPort])},
         {"peer IP:",
          webhelpers:safe_html_string("~p", [PeerAddress])},
         {"channel:",
          webhelpers:safe_html_string("~p", [channel(State)])},
         {"peer port",
          webhelpers:safe_html_string("~p", [PeerPort])},
         {"running since (s)",
          webhelpers:safe_html_string("~p", [Runtime])},
         {"sent_tcp_messages",
          webhelpers:safe_html_string("~p", [s_msg_count(State)])},
         {"sent_tcp_packets",
          webhelpers:safe_html_string("~p", [SendCnt])},
         {"~ sent messages/s",
          webhelpers:safe_html_string("~p", [SentPerS])},
         {"~ sent avg packet size",
          webhelpers:safe_html_string("~p", [SendAgv])},
         {"sent total bytes",
          webhelpers:safe_html_string("~p", [SendBytes])},
         {"recv_tcp_messages",
          webhelpers:safe_html_string("~p", [r_msg_count(State)])},
         {"recv_tcp_packets",
          webhelpers:safe_html_string("~p", [RcvCnt])},
         {"~ recv messages/s",
          webhelpers:safe_html_string("~p", [ReceivedPerS])},
         {"~ recv avg packet size",
          webhelpers:safe_html_string("~p", [RcvAgv])},
         {"recv total bytes",
          webhelpers:safe_html_string("~p", [RcvBytes])}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    send_bundle_if_ready(State);

on(UnknownMessage, State) ->
    %% we want to count messages, so we need this default handler.
    log:log(error,"unknown message: ~.0p~n in Module: ~p and handler ~p~n in State ~.0p",[UnknownMessage,?MODULE,on,State]),
    send_bundle_if_ready(State).

-spec send({inet:ip_address(), comm_server:tcp_port(), inet:socket()}, pid(), comm:message(), comm:send_options(), state()) ->
                   notconnected | inet:socket();
          ({inet:ip_address(), comm_server:tcp_port(), inet:socket()}, unpack_msg_bundle, [{pid(), comm:message()}], [comm:send_options()], state()) ->
                   notconnected | inet:socket().
send({Address, Port, Socket}, Pid, Message, Options, State) ->
    BinaryMessage = term_to_binary({deliver, Pid, Message},
                                   [{compressed, 2}, {minor_version, 1}]),
    NewSocket =
        case gen_tcp:send(Socket, BinaryMessage) of
            ok ->
                ?TRACE("~.0p Sent message ~.0p~n",
                       [pid_groups:my_pidname(), Message]),
                ?LOG_MESSAGE_SOCK('send', Message, byte_size(BinaryMessage), channel(State)),
                Socket;
            {error, closed} ->
                report_bundle_error(Options,
                                    {Address, Port, Pid},
                                    Message, socket_closed),
                log:log(warn,"[ CC ] sending closed connection", []),
                gen_tcp:close(Socket),
                notconnected;
            {error, timeout} ->
                log:log(error,"[ CC ] couldn't send to ~.0p:~.0p (~.0p). retrying.",
                        [Address, Port, timeout]),
                send({Address, Port, Socket}, Pid, Message, Options, State);
            {error, Reason} ->
                report_bundle_error(Options,
                                    {Address, Port, Pid},
                                    Message, Reason),
                log:log(error,"[ CC ] couldn't send to ~.0p:~.0p (~.0p). closing connection",
                        [Address, Port, Reason]),
                gen_tcp:close(Socket),
                notconnected
    end,
    NewSocket.

-spec new_connection(inet:ip_address(), comm_server:tcp_port(),
                     comm_server:tcp_port(), Channel::main | prio | unknown)
        -> inet:socket() | fail.
new_connection(Address, Port, MyPort, Channel) ->
    case gen_tcp:connect(Address, Port, [binary, {packet, 4}]
                         ++ comm_server:tcp_options(Channel),
                         config:read(tcp_connect_timeout)) of
        {ok, Socket} ->
            % send end point data (the other node needs to know my listen port
            % in order to have only a single connection to me)
            case inet:sockname(Socket) of
                {ok, {MyAddress, _SocketPort}} ->
                    case comm_server:get_local_address_port() of
                        {undefined,_} ->
                            comm_server:set_local_address(MyAddress, MyPort);
                        _ -> ok
                    end,
                    Message = term_to_binary({endpoint, MyAddress, MyPort, Channel},
                                             [{compressed, 2}, {minor_version, 1}]),
                    _ = gen_tcp:send(Socket, Message),
                    Socket;
                {error, Reason} ->
                    % note: this should not occur since the socket was just created with 'ok'
                    log:log(error,"[ CC ] reconnect to ~.0p because socket is ~.0p",
                            [Address, Reason]),
                    gen_tcp:close(Socket),
                    new_connection(Address, Port, MyPort, Channel)
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
                    {MQueue, OQueue} = msg_queue(State),
                    NewSocket =
                        send({dest_ip(State), dest_port(State), Socket},
                             unpack_msg_bundle, MQueue, OQueue, State),
                    T1State = set_socket(State, NewSocket),
                    T2State = inc_s_msg_count(T1State),
                    T3State = set_msg_queue(T2State, {[], []}),
                    T4State = set_msg_queue_len(T3State, 0),
                    _T5State = set_msgs_since_bundle_start(T4State, 0);
                false -> State
            end
    end.

-spec state_new(DestIP::inet:ip_address(), DestPort::comm_server:tcp_port(),
                LocalListenPort::comm_server:tcp_port(), Channel::main | prio,
                Socket::inet:socket() | notconnected) -> state().
state_new(DestIP, DestPort, LocalListenPort, Channel, Socket) ->
    {DestIP, DestPort, LocalListenPort, Channel, Socket,
     _StartTime = os:timestamp(), _SentMsgCount = 0, _ReceivedMsgCount = 0,
     _MsgQueue = {[], []}, _Len = 0,
     _DesiredBundleSize = 0, _MsgsSinceBundleStart = 0,
     _LastStatReport = {0, 0, 0, 0} }.

dest_ip(State)                 -> element(1, State).
dest_port(State)               -> element(2, State).
local_listen_port(State)       -> element(3, State).
channel(State)                 -> element(4, State).
socket(State)                  -> element(5, State).
set_socket(State, Val)         -> setelement(5, State, Val).
started(State)                 -> element(6, State).
s_msg_count(State)             -> element(7, State).
inc_s_msg_count(State)         -> setelement(7, State, s_msg_count(State) + 1).
r_msg_count(State)             -> element(8, State).
inc_r_msg_count(State)         -> setelement(8, State, r_msg_count(State) + 1).
msg_queue(State)               -> element(9, State).
set_msg_queue(State, Val)      -> setelement(9, State, Val).
msg_queue_len(State)           -> element(10, State).
set_msg_queue_len(State, Val)  -> setelement(10, State, Val).
desired_bundle_size(State)     -> element(11, State).
set_desired_bundle_size(State, Val) -> setelement(11, State, Val).
msgs_since_bundle_start(State) ->
    element(12, State).
inc_msgs_since_bundle_start(State) ->
    setelement(12, State, msgs_since_bundle_start(State) + 1).
set_msgs_since_bundle_start(State, Val) ->
    setelement(12, State, Val).
last_stat_report(State)          -> element(13, State).
set_last_stat_report(State, Val) -> setelement(13, State, Val).

status(State) ->
     case socket(State) of
         notconnected -> notconnected;
         _            -> connected
     end.

-spec report_bundle_error
        (comm:send_options(), {inet:ip_address(), comm_server:tcp_port(), pid()},
         comm:message(), socket_closed | inet:posix()) -> ok;
        ([comm:send_options()], {inet:ip_address(), comm_server:tcp_port(), unpack_msg_bundle},
         [{pid(), comm:message()}], socket_closed | inet:posix()) -> ok.
report_bundle_error(Options, {Address, Port, unpack_msg_bundle}, Message, Reason) ->
    zip_and_foldr(
      fun (OptionsX, {DestPid, MessageX}) ->
               comm_server:report_send_error(
                 OptionsX, {Address, Port, DestPid}, MessageX, Reason)
      end, Options, Message);
report_bundle_error(Options, {Address, Port, Pid}, Message, Reason) ->
    comm_server:report_send_error(Options, {Address, Port, Pid}, Message, Reason).

zip_and_foldr(_F, [], []) ->
    ok;
zip_and_foldr(F, [El1 | R1] , [El2 | R2]) ->
    zip_and_foldr(F, R1, R2),
    F(El1, El2).
