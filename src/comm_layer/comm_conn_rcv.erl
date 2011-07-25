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
%%      of a connection where messages are received from the network.
%% @end
%% @version $Id$
-module(comm_conn_rcv).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), ct:pal(X,Y)).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/2, init/1, on/2]).

-type state() ::
    {Socket               :: inet:socket() | notconnected,
     StartTime            :: util:time(),
     ReceivedMsgCount     :: non_neg_integer()}.
-type message() ::
    {tcp, Socket::inet:socket(), Data::binary()} |
    {tcp_closed, Socket::inet:socket()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%% be startable via supervisor, use gen_component

-spec start_link(pid_groups:groupname(), inet:socket() | notconnected)
        -> {ok, pid()}.
start_link(CommLayerGroup, Socket) ->
    {Address, Port} =
        case inet:peername(Socket) of
            {ok, X} -> X;
            {error, Reason} ->
                log:log(warn, "[ CC ] cannot get IP address of incoming connection: ~.0p",
                        [Reason]),
                {{0, 0, 0, 0}, 0}
        end,
    {IP1, IP2, IP3, IP4} = Address,
    PidName = "from < " ++ integer_to_list(IP1) ++ "."
        ++ integer_to_list(IP2) ++ "." ++ integer_to_list(IP3) ++ "."
        ++ integer_to_list(IP4) ++ ":" ++ integer_to_list(Port),
    gen_component:start_link(?MODULE, Socket,
                             [{pid_groups_join_as, CommLayerGroup, PidName}]).

%% @doc initialize: return initial state.
-spec init(Socket::inet:socket()) -> state().
init(Socket) -> state_new(Socket).

%% @doc Forwards a message to the given PID or named process.
%%      Logs a warning if the process does not exist.
-spec forward_msg(Process::pid() | atom(), Message::comm:message()) -> ok.
forward_msg(Process, Message) ->
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
    ok.

%% @doc message handler
-spec on(message(), state()) -> state().
on({tcp, Socket, Data}, State) ->
    case binary_to_term(Data) of
        {deliver, unpack_msg_bundle, Message} ->
            ?TRACE("Received message ~.0p", [Message]),
            lists:foldr(fun({DestPid, Msg}, _) -> forward_msg(DestPid, Msg) end,
                        ok, Message),
            %% may fail, when tcp just closed
            _ = inet:setopts(Socket, [{active, once}]),
            inc_r_msg_count(State);
        {deliver, Process, Message} ->
            ?TRACE("Received message ~.0p", [Message]),
            forward_msg(Process, Message),
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
    end;

on({tcp_closed, Socket}, State) ->
    gen_tcp:close(Socket),
    %% receiving processes do not need to survive
    gen_component:kill(self()),
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
         {"received_tcp_messages",
          lists:flatten(io_lib:format("~p", [r_msg_count(State)]))},
         {"~ received messages/s",
          lists:flatten(io_lib:format("~p", [r_msg_count(State) / Runtime]))}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

-spec state_new(Socket::inet:socket()) -> state().
state_new(Socket) ->
    {Socket, _StartTime = erlang:now(), _ReceivedMsgCount = 0}.

socket(State)                 -> element(1, State).
set_socket(State, Val)        -> setelement(1, State, Val).
started(State)                -> element(2, State).
r_msg_count(State)            -> element(3, State).
inc_r_msg_count(State)        -> setelement(3, State, r_msg_count(State) + 1).

status(State) ->
     case socket(State) of
         notconnected -> notconnected;
         _            -> connected
     end.
