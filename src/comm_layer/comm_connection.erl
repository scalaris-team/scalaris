%% @copyright 2007-2017 Zuse Institute Berlin

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

%% number of tags or messages to keep (for debugging)
-define(NUM_KEEP, 10).
%% keep only tag (uncomment to keep msg)
-define(KEEP_TAG, true).

-define(COMM, (config:read(comm_backend))). %% comm_layer backend

-compile({inline, [dest_ip/1, dest_port/1, local_listen_port/1, channel/1,
                   socket/1, set_socket/2,
                   started/1,
                   s_msg_count/1, inc_s_msg_count/2,
                   r_msg_count/1, inc_r_msg_count/2,
                   msg_queue/1, set_msg_queue/2,
                   msg_queue_len/1, set_msg_queue_len/2,
                   desired_bundle_size/1, set_desired_bundle_size/2,
                   msgs_since_bundle_start/1, inc_msgs_since_bundle_start/1,
                   set_msgs_since_bundle_start/2,
                   last_stat_report/1, set_last_stat_report/2]}).

-include("scalaris.hrl").

-ifdef(KEEP_TAG).
    -define(GET_MSG_OR_TAG(Msg), comm:get_msg_tag(Msg)).
    -define(PRETTY_PRINT_MSG(Tag), util:extint2atom(Tag)).
-else.
    -define(GET_MSG_OR_TAG(Msg), Msg).
    -define(PRETTY_PRINT_MSG(Msg), setelement(1, Msg, util:extint2atom(element(1, Msg)))).
-endif.

-export([start_link/6, init/1, on/2]).

-include("gen_component.hrl").

-type msg_queue() :: {MQueue::[{DestPid::pid(), Message::comm:message()}],
                      OQueue::[comm:send_options()]}.
-type stat_report() :: {RcvCnt::non_neg_integer(), RcvBytes::non_neg_integer(),
                        SendCnt::non_neg_integer(), SendBytes::non_neg_integer()}.
-type time_last_msg() :: erlang_timestamp().
-type msg_or_tag() :: comm:message() | comm:msg_tag().

-type socket() :: inet:socket() | ssl:sslsocket().

-type state() ::
    {DestIP                  :: inet:ip_address(),
     DestPort                :: comm_server:tcp_port(),
     LocalListenPort         :: comm_server:tcp_port(),
     Channel                 :: comm:channel(),
     Socket                  :: socket() | notconnected,
     StartTime               :: erlang_timestamp(),
     SentMsgCount            :: non_neg_integer(),
     ReceivedMsgCount        :: non_neg_integer(),
     MsgQueue                :: msg_queue(),
     MsgQueueLen             :: non_neg_integer(),
     DesiredBundleSize       :: non_neg_integer(),
     MsgsSinceBundleStart    :: non_neg_integer(),
     LastStatReport          :: stat_report(),
     TimeLastMsgSent         :: time_last_msg(),
     TimeLastMsgReceived     :: time_last_msg(),
     SentMsgCountSession     :: non_neg_integer(),
     ReceivedMsgCountSession :: non_neg_integer(),
     SessionCount            :: non_neg_integer(),
     LastMsgSent             :: [msg_or_tag()],
     LastMsgReceived         :: [msg_or_tag()]
    }.
-type message() ::
    {send, DestPid::pid(), Message::comm:message(), Options::comm:send_options()} |
    {tcp, Socket::socket(), Data::binary()} |
    {tcp_closed, Socket::socket()} |
    {report_stats} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%% be startable via supervisor, use gen_component

-spec start_link(pid_groups:groupname(), DestIP::inet:ip_address(),
                 comm_server:tcp_port(), socket() | notconnected,
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
                             [{pid_groups_join_as, CommLayerGroup, PidName},
                              {spawn_opts, [{fullsweep_after, 0},
                                            {min_heap_size, 131071}]}]).

%% @doc initialize: return initial state.
-spec init({DestIP::inet:ip_address(), DestPort::comm_server:tcp_port(),
            LocalListenPort::comm_server:tcp_port(), Channel::comm:channel(),
            Socket::socket() | notconnected}) -> state().
init({DestIP, DestPort, LocalListenPort, Channel, Socket}) ->
    msg_delay:send_trigger(10, {report_stats}),
    msg_delay:send_trigger(10, {check_idle}),
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
                                    channel(State)),
            case Socket of
                notconnected ->
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
    handle_data(Socket, Data, State);

on({ssl, Socket, Data}, State) -> %% copy of tcp ...
    handle_data(Socket, Data, State);

on({tcp_closed, Socket}, State) ->
    log:log(info,"[ CC ~p (~p) ] tcp closed", [self(), pid_groups:my_pidname()]),
    close_connection(Socket, State);

on({ssl_closed, Socket}, State) ->
    log:log(info,"[ CC ~p (~p) ] ssl closed", [self(), pid_groups:my_pidname()]),
    close_connection(Socket, State);

on({tcp_error, Socket, Reason}, State) ->
    % example Reason: etimedout, ehostunreach
    log:log(warn,"[ CC ~p (~p) ] tcp error: ~p", [self(), pid_groups:my_pidname(), Reason]),
    %% may fail, when tcp just closed
    _ = inet:setopts(Socket, [{active, once}]),
    send_bundle_if_ready(State);

on({report_stats}, State) ->
    %% re-trigger
    msg_delay:send_trigger(10, {report_stats}),
    NewState = report_stats(State),
    send_bundle_if_ready(NewState);

%% checks if the connection hasn't been used recently
on({check_idle}, State) ->
    msg_delay:send_trigger(10, {check_idle}),
    NewState = send_bundle_if_ready(State),

    Timeout = config:read(tcp_idle_timeout),
    TimeLastMsgSent = time_last_msg_seen(NewState),
    case (notconnected =/= socket(NewState)) andalso
        timer:now_diff(os:timestamp(), TimeLastMsgSent) div 1000 > Timeout of
        true ->
            %% we timed out
            ?TRACE("Closing idle connection: ~p~n", [NewState]),
            %% TODO: check whether data was received on this socket?
            %% (maybe a part of a huge message that takes longer
            %% than the tcp_idle_timeout to receive?)
            close_connection(socket(NewState), NewState);
        _ ->
            ?TRACE("Connection not idle~n", []),
            NewState
            %% send_bundle_if_ready() is called in the
            %% beginning of this on handler to make the
            %% decision on tcp_idle_timeout on the newest
            %% possible state NewState
    end;

on({web_debug_info, Requestor}, State) ->
    Now = os:timestamp(),
    TimeLastMsgSent = time_last_msg_seen(State),
    TimeLastMsgReceived = time_last_msg_received(State),
    SecondsAgoSent = seconds_ago(Now, TimeLastMsgSent),
    SecondsAgoReceived = seconds_ago(Now, TimeLastMsgReceived),
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
            case sockname(Socket) of
                {ok, {MyAddress, MyPort}} -> ok;
                {error, _Reason1}          -> MyAddress = MyPort = "n/a"
            end,
            case peername(Socket) of
                {ok, {PeerAddress, PeerPort}} -> ok;
                {error, _Reason2}              -> PeerAddress = PeerPort = "n/a"
            end,
            case getstat(Socket, [recv_avg, recv_cnt, recv_oct,
                                       send_avg, send_cnt, send_oct]) of
                {ok, [{recv_avg, RcvAgv}, {recv_cnt, RcvCnt}, {recv_oct, RcvBytes},
                      {send_avg, SendAgv}, {send_cnt, SendCnt}, {send_oct, SendBytes}]} -> ok;
                {error, _Reason} ->
                    RcvAgv = RcvCnt = RcvBytes = "n/a",
                    SendAgv = SendCnt = SendBytes = "n/a"
            end
    end,
    LastMsgsSent = [?PRETTY_PRINT_MSG(X) || X <- last_msg_sent(State)],
    LastMsgsReceived = [?PRETTY_PRINT_MSG(X) || X <- last_msg_received(State)],
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
          webhelpers:safe_html_string("~p", [RcvBytes])},
         {"time last message sent",
          webhelpers:safe_html_string("~p sec ago", [SecondsAgoSent])},
         {"time last message received",
          webhelpers:safe_html_string("~p sec ago", [SecondsAgoReceived])},
         {"last message sent:",
          webhelpers:html_pre("~0p", [LastMsgsSent])},
         {"last message received",
          webhelpers:html_pre("~0p", [LastMsgsReceived])},
         {"session variables:", ""},
         {"num sessions",
          webhelpers:safe_html_string("~p", [session_count(State)])},
         {"sent_tcp_messages",
          webhelpers:safe_html_string("~p", [s_msg_count_session(State)])},
         {"recv_tcp_messages",
          webhelpers:safe_html_string("~p", [r_msg_count_session(State)])}
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
            case getstat(Socket, [recv_cnt, recv_oct,
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
            erlang:yield(), % give other processes a chance to enqueue more messages
            {_, MQL} = process_info(self(), message_queue_len),
            if MQL > 0 ->
                   %% start message bundle for sending
                   %% log:log("MQL ~p~n", [MQL]),
                   MaxBundle = erlang:max(200, MQL div 100),
                   T1 = set_msg_queue(State, {[{DestPid, Message}], [Options]}),
                   T2 = set_msg_queue_len(T1, 1),
                   % note: need to set the bundle size equal to MQL
                   % -> to process this 1 msg + MQL messages (see below)
                   set_desired_bundle_size(T2, erlang:min(MQL, MaxBundle));
               true ->
                   send(DestPid, Message, Options, State)
            end;
        QL ->
            {MsgQueue0, OptionQueue0} = msg_queue(State),
            MQueue = [{DestPid, Message} | MsgQueue0],
            OQueue = [Options | OptionQueue0],
            % similar to send_bundle_if_ready/1 (keep in sync!)
            DBS = desired_bundle_size(State),
            MSBS = msgs_since_bundle_start(State),
            % can check for QL instead of QL+1 here due to DBS init above
            case (QL + MSBS) >= DBS of
                true when DBS >= 100 -> % quick path without message_queue_len check
                    %% log:log("Bundle Size: ~p~n", [length(MQueue)]),
                    send_msg_bundle(State, MQueue, OQueue, QL + 1);
                true ->
                    erlang:yield(), % give other processes a chance to enqueue more messages
                    {_, MQL} = process_info(self(), message_queue_len),
                    if MQL > 0 ->
                           %% add to message bundle
                           T1 = set_msg_queue(State, {MQueue, OQueue}),
                           T2 = set_msg_queue_len(T1, QL + 1),
                           set_desired_bundle_size(T2, erlang:min(100, DBS + MQL));
                       true ->
                           %% log:log("Bundle Size: ~p~n", [length(MQueue)]),
                           send_msg_bundle(State, MQueue, OQueue, QL + 1)
                    end;
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
    send_internal(Pid, Message, Options, BinaryMessage, State, 0, 0).

-spec send_internal
    (pid(), comm:message(), comm:send_options(), BinMsg::binary(), state(), Timeouts::non_neg_integer(), Errors::non_neg_integer())
        -> state();
    (?unpack_msg_bundle, [{pid(), comm:message()}], [comm:send_options()], BinMsg::binary(), state(), Timeouts::non_neg_integer(), Errors::non_neg_integer())
        -> state().
send_internal(Pid, Message, Options, BinaryMessage, State, Timeouts, Errors) ->
    Socket = socket(State),
    case Socket of
        notconnected ->
            log:log(error, "[ CC ~p (~p) ] couldn't send message (tcp connect failed)",
                    [self(), pid_groups:my_pidname()]),
            set_socket(reset_msg_counters(State), notconnected);
        Socket ->
            ?LOG_MESSAGE_SOCK('send', Message, byte_size(BinaryMessage), channel(State)),
            case ?COMM:send(Socket, BinaryMessage) of
                ok ->
                    ?TRACE("~.0p Sent message ~.0p~n",
                           [pid_groups:my_pidname(), Message]),
                    SendMsgCount = s_msg_count_session(State),
                    State2 = save_n_msgs(Message, fun set_last_msg_sent/2, State),
                    %% only close in case of no_keep_alive if the
                    %% connection was solely initiated for this send
                    case SendMsgCount =< 1 andalso
                        lists:member({no_keep_alive}, Options) of
                        true -> close_connection(Socket, State2);
                        _    -> State2
                    end;
                {error, closed} ->
                    case Errors < 1 of
                        true ->
                            State2 = close_connection(Socket, State),
                            State3 = set_socket(State2, reconnect(State2)),
                            send_internal(Pid, Message, Options, BinaryMessage, State3, Timeouts, Errors + 1);
                        _    ->
                            Address = dest_ip(State),
                            Port = dest_port(State),
                            report_bundle_error(Options, {Address, Port, Pid}, Message,
                                                socket_closed),
                            log:log(warn,"[ CC ~p (~p) ] sending closed connection", [self(), pid_groups:my_pidname()]),
                            close_connection(Socket, State)
                    end;
                {error, timeout} ->
                    if  % retry 5 times
                        Timeouts < 5 ->
                            log:log(error,"[ CC ~p (~p) ] couldn't send message (~.0p). retrying.",
                                    [self(), pid_groups:my_pidname(), timeout]),
                            send_internal(Pid, Message, Options, BinaryMessage, State, Timeouts + 1, Errors);
                        true ->
                            log:log(error,"[ CC ~p (~p) ] couldn't send message (~.0p). retried 5 times, now closing the connection.",
                                    [self(), pid_groups:my_pidname(), timeout]),
                            Address = dest_ip(State),
                            Port = dest_port(State),
                            report_bundle_error(Options, {Address, Port, Pid}, Message,
                                                socket_timeout),
                            close_connection(Socket, State)
                    end;
                {error, Reason} ->
                    case Errors < 1 of
                        true ->
                            State2 = close_connection(Socket, State),
                            State3 = set_socket(State2, reconnect(State2)),
                            send_internal(Pid, Message, Options, BinaryMessage, State3, Timeouts, Errors + 1);
                        _    ->
                            Address = dest_ip(State),
                            Port = dest_port(State),
                            report_bundle_error(Options, {Address, Port, Pid}, Message,
                                                Reason),
                            log:log(error,"[ CC ~p (~p) ] couldn't send message (~.0p). closing connection",
                                    [self(), pid_groups:my_pidname(), Reason]),
                            close_connection(Socket, State)
                    end
            end
    end.

-spec new_connection(inet:ip_address(), comm_server:tcp_port(),
                     comm_server:tcp_port(), Channel::comm:channel() | unknown)
        -> socket() | notconnected.
new_connection(Address, Port, MyPort, Channel) ->
    new_connection(Address, Port, MyPort, Channel, 0).

-spec new_connection(inet:ip_address(), comm_server:tcp_port(),
                     comm_server:tcp_port(), Channel::comm:channel() | unknown,
                     non_neg_integer())
        -> socket() | notconnected.
new_connection(Address, Port, MyPort, Channel, Retries) ->
    StrictOpts =
        case config:read(ssl_mode) of
            strict -> [{cacertfile, config:read(cacertfile)},
                       {password, config:read(ssl_password)}];
            normal -> []
        end,
    SSLOpts = case ?COMM of
                  ssl -> [{certfile, config:read(certfile)},
                          {keyfile, config:read(keyfile)},
                          {secure_renegotiate, true}
                         ];
                  gen_tcp -> []
              end,
    case ?COMM:connect(Address, Port, [binary, {packet, 4}]
                         ++ comm_server:tcp_options(Channel) ++ StrictOpts ++ SSLOpts,
                         config:read(tcp_connect_timeout)) of
        {ok, Socket} ->
            % send end point data (the other node needs to know my listen port
            % in order to have only a single connection to me)
            case sockname(Socket) of
                {ok, {MyAddress, _SocketPort}} ->
                    case comm_server:get_local_address_port() of
                        {undefined,_} ->
                            comm_server:set_local_address(MyAddress, MyPort);
                        _ -> ok
                    end,
                    Message = term_to_binary({endpoint, MyAddress, MyPort, Channel},
                                             [{compressed, 2}, {minor_version, 1}]),
                    _ = ?COMM:send(Socket, Message),
                    Socket;
                {error, Reason} ->
                    % note: this should not occur since the socket was just created with 'ok'
                    log:log(error,"[ CC ~p (~p) ] reconnect because socket is ~.0p",
                            [self(), pid_groups:my_pidname(), Reason]),
                    ?COMM:close(Socket),
                    new_connection(Address, Port, MyPort, Channel, Retries + 1)
            end;
        {error, Reason} ->
            log:log(info,"[ CC ~p (~p) ] couldn't connect (~.0p)",
                    [self(), pid_groups:my_pidname(), Reason]),
            case Retries >= 3 of
                true -> notconnected;
                _    -> timer:sleep(config:read(tcp_connect_timeout) * (Retries + 1)),
                        new_connection(Address, Port, MyPort, Channel, Retries + 1)
            end
    end.

-spec reconnect(state()) -> socket() | notconnected.
reconnect(State) ->
    Address = dest_ip(State),
    Port = dest_port(State),
    MyPort = local_listen_port(State),
    Channel = channel(State),
    new_connection(Address, Port, MyPort, Channel, 0).

-spec close_connection(Socket::socket(), State::state()) -> state().
close_connection(Socket, State) ->
    ?COMM:close(Socket),
    case socket(State) of
        Socket ->
            % report stats out of the original schedule
            % (these would otherwise be lost)
            StateNew = report_stats(State),
            set_socket(reset_msg_counters(StateNew), notconnected);
        _ -> State
    end.

-spec send_bundle_if_ready(state()) -> state().
send_bundle_if_ready(InState) ->
    QL = msg_queue_len(InState),
    case QL of
        0 -> InState;
        _ ->
            % similar to send_or_bundle/4 (keep in sync!)
            State = inc_msgs_since_bundle_start(InState),
            DBS = desired_bundle_size(State),
            MSBS = msgs_since_bundle_start(State),
            case (QL + MSBS) >= DBS of
                true when DBS >= 100 -> % quick path without message_queue_len check
                    %% log:log("Sending packet with ~p msgs~n", [length(element(1, msg_queue(State)))]),
                    {MQueue, OQueue} = msg_queue(State),
                    send_msg_bundle(State, MQueue, OQueue, QL);
                true ->
                    erlang:yield(), % give other processes a chance to enqueue more messages
                    {_, MQL} = process_info(self(), message_queue_len),
                    if MQL > 0 ->
                           set_desired_bundle_size(State, erlang:min(100, DBS + MQL));
                       true ->
                           %% log:log("Sending packet with ~p msgs~n", [length(element(1, msg_queue(State)))]),
                           {MQueue, OQueue} = msg_queue(State),
                           send_msg_bundle(State, MQueue, OQueue, QL)
                    end;
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
            T2State = set_msg_queue(T1State, {[], []}),
            T3State = set_msg_queue_len(T2State, 0),
            _T4State = set_msgs_since_bundle_start(T3State,0)
    end.

-spec state_new(DestIP::inet:ip_address(), DestPort::comm_server:tcp_port(),
                LocalListenPort::comm_server:tcp_port(), Channel::comm:channel(),
                Socket::socket() | notconnected) -> state().
state_new(DestIP, DestPort, LocalListenPort, Channel, Socket) ->
    {DestIP, DestPort, LocalListenPort, Channel, Socket,
     _StartTime = os:timestamp(), _SentMsgCount = 0, _ReceivedMsgCount = 0,
     _MsgQueue = {[], []}, _Len = 0,
     _DesiredBundleSize = 0, _MsgsSinceBundleStart = 0,
     _LastStatReport = {0, 0, 0, 0},
     _TimeLastMsgSent = {0, 0, 0},
     _TimeLastMsgReceived = {0, 0, 0},
     _SentMsgCountSession = 0, _ReceivedMsgCountSession = 0,
     _SessionCount = 0,
     _LastMsgsSent = [], _LastMsgsReceived = []
    }.

-spec dest_ip(state()) -> inet:ip_address().
dest_ip(State)                 -> element(1, State).

-spec dest_port(state()) -> comm_server:tcp_port().
dest_port(State)               -> element(2, State).

-spec local_listen_port(state()) -> comm_server:tcp_port().
local_listen_port(State)       -> element(3, State).

-spec channel(state()) -> comm:channel().
channel(State)                 -> element(4, State).

-spec socket(state()) -> socket() | notconnected.
socket(State)                  -> element(5, State).

-spec set_socket(state(), socket() | notconnected) -> state().
set_socket(State, Val)         -> setelement(5, State, Val).

-spec started(state()) -> erlang_timestamp().
started(State)                 -> element(6, State).

-spec s_msg_count(state()) -> non_neg_integer().
s_msg_count(State)             -> element(7, State).
-spec inc_s_msg_count(state(), pos_integer()) -> state().
inc_s_msg_count(State, N)      -> State2 = setelement(7, State, s_msg_count(State) + N),
                                  inc_s_msg_count_session(State2, N).

-spec r_msg_count(state()) -> non_neg_integer().
r_msg_count(State)             -> element(8, State).
-spec inc_r_msg_count(state(), pos_integer()) -> state().
inc_r_msg_count(State, N)      -> State2 = setelement(8, State, r_msg_count(State) + N),
                                  inc_r_msg_count_session(State2, N).

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

-spec time_last_msg_seen(state()) -> erlang_timestamp().
time_last_msg_seen(State) -> element(14, State).
-spec set_time_last_msg_seen(state()) -> state().
set_time_last_msg_seen(State) -> setelement(14, State, os:timestamp()).

-spec time_last_msg_received(state()) -> erlang_timestamp().
time_last_msg_received(State) -> element(15, State).
-spec set_time_last_msg_received(state()) -> state().
set_time_last_msg_received(State) -> setelement(15, State, os:timestamp()).

-spec s_msg_count_session(state()) -> non_neg_integer().
s_msg_count_session(State)         -> element(16, State).
-spec inc_s_msg_count_session(state(), pos_integer()) -> state().
inc_s_msg_count_session(State, N)  -> setelement(16, State, s_msg_count_session(State) + N).

-spec r_msg_count_session(state()) -> non_neg_integer().
r_msg_count_session(State)         -> element(17, State).
-spec inc_r_msg_count_session(state(), pos_integer()) -> state().
inc_r_msg_count_session(State, N)  -> setelement(17, State, r_msg_count_session(State) + N).

-spec reset_msg_counters(state()) -> state().
reset_msg_counters(State) -> State1 = setelement(16, State, 0),
                             State2 = setelement(17, State1, 0),
                             inc_session_count(State2).

-spec session_count(state()) -> non_neg_integer().
session_count(State) -> element(18, State).
-spec inc_session_count(state()) -> state().
inc_session_count(State) -> setelement(18, State, session_count(State) + 1).

-spec last_msg_sent(state()) -> [msg_or_tag()].
last_msg_sent(State) -> element(19, State).
-spec set_last_msg_sent(state(), [msg_or_tag()]) -> state().
set_last_msg_sent(State, MsgList) ->
    MsgListLen = length(MsgList),
    OldList = last_msg_sent(State),
    NewList = case erlang:max(?NUM_KEEP - MsgListLen, 0) of
                  0         -> MsgList;
                  NumToKeep -> MsgList ++ lists:sublist(OldList, NumToKeep)
              end,
    State2 = set_time_last_msg_seen(State),
    State3 = inc_s_msg_count(State2, MsgListLen),
    setelement(19, State3, NewList).

-spec last_msg_received(state()) -> [msg_or_tag()].
last_msg_received(State) -> element(20, State).
-spec set_last_msg_received(state(), [msg_or_tag()]) -> state().
set_last_msg_received(State, MsgList) ->
    MsgListLen = length(MsgList),
    OldList = last_msg_received(State),
    NewList = case erlang:max(?NUM_KEEP - MsgListLen, 0) of
                  0         -> MsgList;
                  NumToKeep -> MsgList ++ lists:sublist(OldList, NumToKeep)
              end,
    State2 = set_time_last_msg_received(State),
    State3 = inc_r_msg_count(State2, MsgListLen),
    setelement(20, State3, NewList).

-spec save_n_msgs(comm:message() | [{any(), comm:message()}],
                  fun((state(), MsgList::[msg_or_tag()]) -> state()),
                  state()) -> state().
save_n_msgs(Msg, SaveFun, State) when is_tuple(Msg) ->
    save_n_msgs([{single, Msg}], SaveFun, State);
save_n_msgs(Msgs, SaveFun, State) when is_list(Msgs) ->
    List = [get_msg_tag(M) || M <- lists:sublist(Msgs, ?NUM_KEEP)],
    SaveFun(State, List).

-spec get_msg_tag({any(), comm:message()}) -> msg_or_tag().
get_msg_tag(Msg) ->
    {_Pid, ActualMessage} = Msg,
    ?GET_MSG_OR_TAG(ActualMessage).

-spec seconds_ago(time_last_msg(), time_last_msg()) -> non_neg_integer().
seconds_ago(Now, Time) ->
    case Time of
        {0,0,0} -> infinity;
        _ -> timer:now_diff(Now, Time) div 1000000
    end.


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

sockname(Socket) ->
    case ?COMM of
        ssl ->
            ssl:sockname(Socket);
        gen_tcp ->
            inet:sockname(Socket)
    end.

-spec peername(Socket::socket()) -> term().
peername(Socket) ->
    case ?COMM of
        ssl ->
            ssl:peername(Socket);
        gen_tcp ->
            inet:peername(Socket)
    end.

getstat(Socket, Options) ->
    case ?COMM of
        ssl ->
            ssl:getstat(Socket, Options);
        gen_tcp ->
            inet:getstat(Socket, Options)
    end.

-spec setopts(Socket::socket(), list()) -> term().
setopts(Socket, Options) ->
    case ?COMM of
        ssl ->
            ssl:setopts(Socket, Options);
        gen_tcp ->
            inet:setopts(Socket, Options)
    end.

handle_data(Socket, Data, State) ->
    DeliverMsg = ?COMM_DECOMPRESS_MSG(Data, State),
    NewState =
        case DeliverMsg of
            {?deliver, ?unpack_msg_bundle, Message} ->
                ?LOG_MESSAGE_SOCK('rcv', Data, byte_size(Data), channel(State)),
                ?TRACE("Received message ~.0p", [Message]),
                lists:foldr(fun({DestPid, Msg}, _) -> forward_msg(DestPid, Msg, State) end,
                            ok, Message),
                %% may fail, when tcp just closed
                _ = setopts(Socket, [{active, once}]),
                save_n_msgs(Message, fun set_last_msg_received/2, State);
            {?deliver, Process, Message} ->
                ?TRACE("Received message ~.0p", [Message]),
                ?LOG_MESSAGE_SOCK('rcv', Data, byte_size(Data), channel(State)),
                forward_msg(Process, Message, State),
                %% may fail, when tcp just closed
                _ = setopts(Socket, [{active, once}]),
                save_n_msgs(Message, fun set_last_msg_received/2, State);
            {user_close} ->
                log:log(warn,"[ CC ~p (~p) ] tcp user_close request", [self(), pid_groups:my_pidname()]),
                close_connection(Socket, State);
            Unknown ->
                log:log(warn,"[ CC ~p (~p) ] unknown message ~.0p", [self(), pid_groups:my_pidname(), Unknown]),
                %% may fail, when tcp just closed
                _ = setopts(Socket, [{active, once}]),
                State
        end,
    New2State = set_time_last_msg_seen(NewState),
    send_bundle_if_ready(New2State).
