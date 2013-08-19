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

%-define(TRACE(X,Y), log:pal(X,Y)).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-behaviour(gen_component).

-compile({inline, [dest_ip/1, dest_port/1, local_listen_port/1, channel/1,
                   socket/1, set_socket/2,
                   started/1,
                   s_msg_count/1, inc_s_msg_count/1,
                   r_msg_count/1, inc_r_msg_count/1,
                   msg_queue/1, set_msg_queue/2,
                   msg_queue_len/1, set_msg_queue_len/2,
                   desired_bundle_size/1, set_desired_bundle_size/2,
                   msgs_since_bundle_start/1, inc_msgs_since_bundle_start/1,
                   set_msgs_since_bundle_start/2,
                   last_stat_report/1, set_last_stat_report/2]}).

-include("scalaris.hrl").

-export([start_link/6, init/1, on/2]).

-type msg_queue() :: {MQueue::[{DestPid::pid(), Message::comm:message()}],
                      OQueue::[comm:send_options()]}.
-type stat_report() :: {RcvCnt::non_neg_integer(), RcvBytes::non_neg_integer(),
                        SendCnt::non_neg_integer(), SendBytes::non_neg_integer()}.
-type state() ::
    {DestIP               :: inet:ip_address(),
     DestPort             :: comm_server:tcp_port(),
     LocalListenPort      :: comm_server:tcp_port(),
     Channel              :: comm:channel(),
     Socket               :: inet:socket() | notconnected,
     StartTime            :: erlang_timestamp(),
     SentMsgCount         :: non_neg_integer(),
     ReceivedMsgCount     :: non_neg_integer(),
     MsgQueue             :: msg_queue(),
     MsgQueueLen          :: non_neg_integer(),
     DesiredBundleSize    :: non_neg_integer(),
     MsgsSinceBundleStart :: non_neg_integer(),
     LastStatReport       :: stat_report(),
     NumberOfTimeouts	  :: non_neg_integer()}.
-type message() ::
    {send, DestPid::pid(), Message::comm:message(), Options::comm:send_options()} |
    {tcp, Socket::inet:socket(), Data::binary()} |
    {tcp_closed, Socket::inet:socket()} |
    {report_stats} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%% be startable via supervisor, use gen_component

-spec start_link(pid_groups:groupname(), DestIP::inet:ip_address(),
                 comm_server:tcp_port(), inet:socket() | notconnected,
                 Channel::comm:channel(), Dir::'rcv' | 'send' | 'both') -> {ok, pid()}.
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
            LocalListenPort::comm_server:tcp_port(), Channel::comm:channel(),
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
            comm:send_local(Process, Message), ok;
        false ->
            case whereis(Process) of
                undefined ->
                    log:log(warn,
                            "[ CC ~p (~p) ] Cannot accept msg for unknown named"
                                " process ~p: ~.0p~n",
                            [self(), pid_groups:my_pidname(), Process, Message]);
                PID -> comm:send_local(PID, Message), ok
            end
    end.

%% @doc message handler
-spec on(message(), state()) -> state().
on({send, DestPid, Message, Options}, State) ->
    case socket(State) of
        notconnected ->
            log:log(info, "Connecting to ~.0p:~.0p", [dest_ip(State), dest_port(State)]),
            Socket = new_connection(dest_ip(State), dest_port(State),
                                    local_listen_port(State),
                                    proplists:get_value(channel, Options, main)),
            case Socket of
                fail ->
                    comm_server:report_send_error(Options,
                                                  {dest_ip(State), dest_port(State), DestPid},
                                                  Message, tcp_connect_failed),
                    State;
                _ ->
                    State1 = set_socket(State, Socket),
                    State2 = set_last_stat_report(State1, {0, 0, 0, 0}),
                    send_or_bundle(DestPid, Message, Options, State2)
            end;
        _ -> send_or_bundle(DestPid, Message, Options, State)
    end;

on({tcp, Socket, Data}, State) ->
    DeliverMsg = ?COMM_DECOMPRESS_MSG(Data, State),
    NewState =
        case DeliverMsg of
            {?deliver, ?unpack_msg_bundle, Message} ->
                ?LOG_MESSAGE_SOCK('rcv', Data, byte_size(Data), channel(State)),
                ?TRACE("Received message ~.0p", [Message]),
                lists:foldr(fun({DestPid, Msg}, _) -> forward_msg(DestPid, Msg, State) end,
                            ok, Message),
                %% may fail, when tcp just closed
                _ = inet:setopts(Socket, [{active, once}]),
                inc_r_msg_count(State);
            {?deliver, Process, Message} ->
                ?TRACE("Received message ~.0p", [Message]),
                ?LOG_MESSAGE_SOCK('rcv', Data, byte_size(Data), channel(State)),
                forward_msg(Process, Message, State),
                %% may fail, when tcp just closed
                _ = inet:setopts(Socket, [{active, once}]),
                inc_r_msg_count(State);
            {user_close} ->
                log:log(warn,"[ CC ~p (~p) ] tcp user_close request", [self(), pid_groups:my_pidname()]),
                close_connection(Socket, State);
            Unknown ->
                log:log(warn,"[ CC ~p (~p) ] unknown message ~.0p", [self(), pid_groups:my_pidname(), Unknown]),
                %% may fail, when tcp just closed
                _ = inet:setopts(Socket, [{active, once}]),
                State
    end,
    send_bundle_if_ready(NewState);

on({tcp_closed, Socket}, State) ->
    log:log(warn,"[ CC ~p (~p) ] tcp closed", [self(), pid_groups:my_pidname()]),
    close_connection(Socket, State);

on({tcp_error, Socket, Reason}, State) ->
    % example Reason: etimedout, ehostunreach
    log:log(warn,"[ CC ~p (~p) ] tcp error: ~p", [self(), pid_groups:my_pidname(), Reason]),
    %% may fail, when tcp just closed
    _ = inet:setopts(Socket, [{active, once}]),
    send_bundle_if_ready(State);

on({report_stats}, State) ->
    %% re-trigger
    msg_delay:send_local(10, self(), {report_stats}),
    NewState = report_stats(State),
    send_bundle_if_ready(NewState);

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

-spec report_stats(State::state()) -> state().
report_stats(State) ->
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
                            comm:send_local(
                              comm_stats,
                              {report_stat,
                               overflow_aware_diff(RcvCnt, PrevRcvCnt),
                               overflow_aware_diff(RcvBytes, PrevRcvBytes),
                               overflow_aware_diff(SendCnt, PrevSendCnt),
                               overflow_aware_diff(SendBytes, PrevSendBytes)}),
                            set_last_stat_report(State, NewStat)
                    end;
                {error, _Reason} -> State
            end
    end.

%% @doc Diff between A and B taking an overflow at 2^32 or 2^64 into account,
%%      otherwise <tt>A - B</tt>. Fails if A &lt; B and B &lt;= 2^32 or 2^64.
-spec overflow_aware_diff(number(), number()) -> number().
overflow_aware_diff(A, B) when A >= B ->
    A - B;
overflow_aware_diff(A, B) when A < B andalso B < 4294967296 -> % 2^32
    4294967296 - B + A;
overflow_aware_diff(A, B) when A < B andalso B < 18446744073709551616 -> % 2^64
    18446744073709551616 - B + A.

% PRE: connected socket
-spec send_or_bundle(DestPid::pid(), Message::comm:message(), Options::comm:send_options(), State::state()) -> state().
send_or_bundle(DestPid, Message, Options, State) ->
    case msg_queue_len(State) of
        0 ->
            {_, MQL} = process_info(self(), message_queue_len),
            if MQL > 0 ->
                   %% start message bundle for sending
                   %% io:format("MQL ~p~n", [MQL]),
                   MaxBundle = erlang:max(200, MQL div 100),
                   T1 = set_msg_queue(State, {[{DestPid, Message}], [Options]}),
                   T2 = set_msg_queue_len(T1, 1),
                   % note: need to set the bundle size equal to MQL
                   % -> to process this 1 msg + MQL messages (see below)
                   set_desired_bundle_size(T2, erlang:min(MQL, MaxBundle));
               true ->
                   NewState = send(DestPid, Message, Options, State),
                   inc_s_msg_count(NewState)
            end;
        QL ->
            {MsgQueue0, OptionQueue0} = msg_queue(State),
            MQueue = [{DestPid, Message} | MsgQueue0],
            OQueue = [Options | OptionQueue0],
            DBS = desired_bundle_size(State),
            MSBS = msgs_since_bundle_start(State),
            % can check for QL instead of QL+1 here due to DBS init above
            case (QL + MSBS) >= DBS of
                true ->
                    %%                            io:format("Bundle Size: ~p~n", [length(MQueue)]),
                    send_msg_bundle(State, MQueue, OQueue, QL + 1);
                false ->
                    %% add to message bundle
                    T1 = set_msg_queue(State, {MQueue, OQueue}),
                    set_msg_queue_len(T1, QL + 1)
            end
    end.

-spec send(pid(), comm:message(), comm:send_options(), state())
            -> state();
          (?unpack_msg_bundle, [{pid(), comm:message()}], [comm:send_options()], state())
            -> state().
send(Pid, Message, Options, State) ->
    DeliverMsg = {?deliver, Pid, Message},
    BinaryMessage = ?COMM_COMPRESS_MSG(DeliverMsg, State),
    send_internal(Pid, Message, Options, BinaryMessage, State).

-spec send_internal
    (pid(), comm:message(), comm:send_options(), BinMsg::binary(), state())
        -> state();
    (?unpack_msg_bundle, [{pid(), comm:message()}], [comm:send_options()], BinMsg::binary(), state())
        -> state().
send_internal(Pid, Message, Options, BinaryMessage, State) ->
    Socket = socket(State),
    case gen_tcp:send(Socket, BinaryMessage) of
        ok ->
            ?TRACE("~.0p Sent message ~.0p~n",
                   [pid_groups:my_pidname(), Message]),
            ?LOG_MESSAGE_SOCK('send', Message, byte_size(BinaryMessage), channel(State)),
            set_number_of_timeouts(State, 0);
        {error, closed} ->
            Address = dest_ip(State),
            Port = dest_port(State),
            report_bundle_error(Options, {Address, Port, Pid}, Message,
                                socket_closed),
            log:log(warn,"[ CC ~p (~p) ] sending closed connection", [self(), pid_groups:my_pidname()]),
            close_connection(Socket, set_number_of_timeouts(State, 0));
        {error, timeout} ->
            NumberOfTimeouts = number_of_timeouts(State),
            if  % retry 5 times
                NumberOfTimeouts < 5 ->
                    log:log(error,"[ CC ~p (~p) ] couldn't send message (~.0p). retrying.",
                            [self(), pid_groups:my_pidname(), timeout]),
                    send_internal(Pid, Message, Options, BinaryMessage, set_number_of_timeouts(State, NumberOfTimeouts + 1));
                true ->
                    log:log(error,"[ CC ~p (~p) ] couldn't send message (~.0p). retried 5 times, now closing the connection.",
                            [self(), pid_groups:my_pidname(), timeout]),
                    Address = dest_ip(State),
                    Port = dest_port(State),
                    report_bundle_error(Options, {Address, Port, Pid}, Message,
                                        socket_timeout),
                    close_connection(Socket, set_number_of_timeouts(State, 0))
            end;
        {error, Reason} ->
            Address = dest_ip(State),
            Port = dest_port(State),
            report_bundle_error(Options, {Address, Port, Pid}, Message,
                                Reason),
            log:log(error,"[ CC ~p (~p) ] couldn't send message (~.0p). closing connection",
                    [self(), pid_groups:my_pidname(), Reason]),
            close_connection(Socket, set_number_of_timeouts(State, 0))
    end.

-spec new_connection(inet:ip_address(), comm_server:tcp_port(),
                     comm_server:tcp_port(), Channel::comm:channel() | unknown)
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
                    log:log(error,"[ CC ~p (~p) ] reconnect because socket is ~.0p",
                            [self(), pid_groups:my_pidname(), Reason]),
                    gen_tcp:close(Socket),
                    new_connection(Address, Port, MyPort, Channel)
            end;
        {error, Reason} ->
            log:log(info,"[ CC ~p (~p) ] couldn't connect (~.0p)",
                    [self(), pid_groups:my_pidname(), Reason]),
            fail
    end.

-spec close_connection(Socket::inet:socket(), State::state()) -> state().
close_connection(Socket, State) ->
    gen_tcp:close(Socket),
    case socket(State) of
        Socket ->
            % report stats out of the original schedule
            % (these would otherwise be lost)
            set_socket(report_stats(State), notconnected);
        _      -> State
    end.

-spec send_bundle_if_ready(state()) -> state().
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
                    %% io:format("Sending packet with ~p msgs~n", [length(msg_queue(State))]),
                    {MQueue, OQueue} = msg_queue(State),
                    send_msg_bundle(State, MQueue, OQueue, QL);
                false -> State
            end
    end.

-spec send_msg_bundle(state(),
                      MQueue::[{DestPid::pid(), Message::comm:message()}],
                      OQueue::[comm:send_options()], QL::pos_integer()) -> state().
send_msg_bundle(State, MQueue, OQueue, QL) ->
    case socket(State) of
        notconnected ->
            % should not occur often - just in case a new MQueue, OQueue was given:
            T1 = set_msg_queue(State, {MQueue, OQueue}),
            set_msg_queue_len(T1, QL);
        _ ->
            T1State = send(?unpack_msg_bundle, MQueue, OQueue, State),
            T2State = inc_s_msg_count(T1State),
            T3State = set_msg_queue(T2State, {[], []}),
            T4State = set_msg_queue_len(T3State, 0),
            _T5State = set_msgs_since_bundle_start(T4State,0)
    end.

-spec state_new(DestIP::inet:ip_address(), DestPort::comm_server:tcp_port(),
                LocalListenPort::comm_server:tcp_port(), Channel::comm:channel(),
                Socket::inet:socket() | notconnected) -> state().
state_new(DestIP, DestPort, LocalListenPort, Channel, Socket) ->
    {DestIP, DestPort, LocalListenPort, Channel, Socket,
     _StartTime = os:timestamp(), _SentMsgCount = 0, _ReceivedMsgCount = 0,
     _MsgQueue = {[], []}, _Len = 0,
     _DesiredBundleSize = 0, _MsgsSinceBundleStart = 0,
     _LastStatReport = {0, 0, 0, 0},
     _NumberOfTimeouts = 0}.

-spec dest_ip(state()) -> inet:ip_address().
dest_ip(State)                 -> element(1, State).

-spec dest_port(state()) -> comm_server:tcp_port().
dest_port(State)               -> element(2, State).

-spec local_listen_port(state()) -> comm_server:tcp_port().
local_listen_port(State)       -> element(3, State).

-spec channel(state()) -> comm:channel().
channel(State)                 -> element(4, State).

-spec socket(state()) -> inet:socket() | notconnected.
socket(State)                  -> element(5, State).

-spec set_socket(state(), inet:socket() | notconnected) -> state().
set_socket(State, Val)         -> setelement(5, State, Val).

-spec started(state()) -> erlang_timestamp().
started(State)                 -> element(6, State).

-spec s_msg_count(state()) -> non_neg_integer().
s_msg_count(State)             -> element(7, State).
-spec inc_s_msg_count(state()) -> state().
inc_s_msg_count(State)         -> setelement(7, State, s_msg_count(State) + 1).

-spec r_msg_count(state()) -> non_neg_integer().
r_msg_count(State)             -> element(8, State).
-spec inc_r_msg_count(state()) -> state().
inc_r_msg_count(State)         -> setelement(8, State, r_msg_count(State) + 1).

-spec msg_queue(state()) -> msg_queue().
msg_queue(State)               -> element(9, State).
-spec set_msg_queue(state(), msg_queue()) -> state().
set_msg_queue(State, Val)      -> setelement(9, State, Val).

-spec msg_queue_len(state()) -> non_neg_integer().
msg_queue_len(State)           -> element(10, State).
-spec set_msg_queue_len(state(), non_neg_integer()) -> state().
set_msg_queue_len(State, Val)  -> setelement(10, State, Val).

-spec desired_bundle_size(state()) -> non_neg_integer().
desired_bundle_size(State)     -> element(11, State).
-spec set_desired_bundle_size(state(), non_neg_integer()) -> state().
set_desired_bundle_size(State, Val) -> setelement(11, State, Val).

-spec msgs_since_bundle_start(state()) -> non_neg_integer().
msgs_since_bundle_start(State) -> element(12, State).
-spec inc_msgs_since_bundle_start(state()) -> state().
inc_msgs_since_bundle_start(State) ->
    setelement(12, State, msgs_since_bundle_start(State) + 1).
-spec set_msgs_since_bundle_start(state(), non_neg_integer()) -> state().
set_msgs_since_bundle_start(State, Val) ->
    setelement(12, State, Val).

-spec last_stat_report(state()) -> stat_report().
last_stat_report(State)          -> element(13, State).
-spec set_last_stat_report(state(), stat_report()) -> state().
set_last_stat_report(State, Val) -> setelement(13, State, Val).

-spec number_of_timeouts(state()) -> non_neg_integer().
number_of_timeouts(State) -> element(14, State).
-spec set_number_of_timeouts(state(), non_neg_integer()) -> state().
set_number_of_timeouts(State, N) -> setelement(14, State, N).

-spec status(State::state()) -> notconnected | connected.
status(State) ->
     case socket(State) of
         notconnected -> notconnected;
         _            -> connected
     end.

-spec report_bundle_error
        (comm:send_options(), {inet:ip_address(), comm_server:tcp_port(), pid()},
         comm:message(), socket_closed | inet:posix()) -> ok;
        ([comm:send_options()], {inet:ip_address(), comm_server:tcp_port(), ?unpack_msg_bundle},
         [{pid(), comm:message()}], socket_closed | inet:posix()) -> ok.
report_bundle_error(Options, {Address, Port, ?unpack_msg_bundle}, Message, Reason) ->
    zip_and_foldr(
      fun (OptionsX, {DestPid, MessageX}) ->
               comm_server:report_send_error(
                 OptionsX, {Address, Port, DestPid}, MessageX, Reason)
      end, Options, Message);
report_bundle_error(Options, {Address, Port, Pid}, Message, Reason) ->
    comm_server:report_send_error(Options, {Address, Port, Pid}, Message, Reason).

-spec zip_and_foldr(fun((E1, E2) -> any()), [E1], [E2]) -> ok.
zip_and_foldr(_F, [], []) ->
    ok;
zip_and_foldr(F, [El1 | R1] , [El2 | R2]) ->
    zip_and_foldr(F, R1, R2),
    F(El1, El2).
