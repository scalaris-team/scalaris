%  @copyright 2007-2012 Zuse Institute Berlin

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
%% @doc    Message Sending.
%%
%%  This module allows to configure Scalaris for using Distributed Erlang
%%  (if the macro BUILTIN is defined) or TCP (macro TCP_LAYER) for inter-node
%%  communication and wraps message sending and process identifiers.
%%
%%  Messages consist of a tuple of which the first element is the message's
%%  tag, i.e. an atom. Process identifiers depend on the messaging back-end
%%  being used but can also wrap up an arbitrary cookie that can be used to
%%  tell messages with the same tag apart, e.g. when they are used for
%%  different purposes.
%%
%%  Sending messages to so-tagged process identifiers works seamlessly, e.g.
%%  a server receiving message {tag, SourcePid} can reply with
%%  comm:send(SourcePid, {tag_response}). On the receiving side (a client),
%%  a message of the form {Message, Cookie} will then be received, e.g.
%%  {{tag_response}, Cookie}. Pids with cookies can be created using
%%  this_with_cookie/1.
%% @end
%% @version $Id$
-module(comm).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

%% initialization
-export([init_and_wait_for_valid_pid/0]).
%% Sending messages
-export([send/2, send/3, send_local/2, send_local_after/3]).
%% Pid manipulation
-export([make_global/1, make_local/1]).
-export([this/0, get/2, this_with_cookie/1, self_with_cookie/1]).
-export([is_valid/1, is_local/1]).
%%-export([node/1]).
%% Message manipulation
-export([get_msg_tag/1]).
-export([unpack_cookie/2]).

-ifdef(with_export_type_support).
-export_type([message/0, message_tag/0, mypid/0,
              erl_local_pid/0, erl_local_pid_with_cookie/0,
              send_options/0]).
% for comm_layer
-export_type([erl_pid_plain/0]).
% for tester_scheduler
-export_type([mypid_plain/0, erl_local_pid_plain/0]).
-endif.

-type cookie() :: any().
-type reg_name() :: atom().
-type erl_local_pid_plain() :: pid() | reg_name().
-type erl_local_pid_with_cookie() :: {erl_local_pid_plain(), c, cookie()}.
-type erl_local_pid() :: erl_local_pid_plain() | erl_local_pid_with_cookie().
-type erl_pid_plain() :: erl_local_pid_plain() | {reg_name(), node()}. % port()

% NOTE: Cannot move these type specs to the appropriate include files as they
%       need to be in this order for Erlang R13.
-ifdef(TCP_LAYER).
-type mypid_plain() :: {inet:ip_address(), integer(), erl_pid_plain()}.
-endif.
-ifdef(BUILTIN).
-type mypid_plain() :: erl_pid_plain().
-endif.

%% -type erl_pid_with_cookie() :: {erl_pid_plain(), c, cookie()}.
%% -type erl_pid() :: erl_pid_plain() | erl_pid_with_cookie().
-type mypid_with_cookie() :: {mypid_plain(), c, cookie()}.
-opaque mypid() :: mypid_plain() | mypid_with_cookie().
-type message_tag() :: atom().
% there is no variable length-tuple definition for types -> declare messages with up to 10 parameters here:
-type message_plain() :: {message_tag()} |
                         {message_tag(), any()} |
                         {message_tag(), any(), any()} |
                         {message_tag(), any(), any(), any()} |
                         {message_tag(), any(), any(), any(), any()} |
                         {message_tag(), any(), any(), any(), any(), any()} |
                         {message_tag(), any(), any(), any(), any(), any(), any()} |
                         {message_tag(), any(), any(), any(), any(), any(), any(), any()} |
                         {message_tag(), any(), any(), any(), any(), any(), any(), any(), any()} |
                         {message_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
                         {message_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()}.
-type message_with_cookie() :: {message_plain(), any()}.
-type message() :: message_plain() | message_with_cookie().
-type group_message() :: {send_to_group_member, atom(), message()}.
-type send_options() :: [{shepherd, Pid::erl_local_pid()} |
                         {group_member, Process::atom()} |
                         {channel, main | prio} | quiet].

-ifdef(TCP_LAYER).
-include("comm_tcp.hrl").
-endif.
-ifdef(BUILTIN).
-include("comm_builtin.hrl").
-endif.

%% @doc Sends a message to a process given by its pid.
-spec send(mypid(), message() | group_message()) -> ok.
send(Pid, Message) ->
    send(Pid, Message, []).

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()).
-spec send_local(erl_local_pid(), message()) -> ok.
send_local(Pid, Message) ->
    {RealPid, RealMessage} = unpack_cookie(Pid, Message),
    case erlang:get(trace_mpath) of
        undefined ->
            RealPid ! RealMessage;
        Logger ->
            LogEpidemicMsg =
                trace_mpath:log_send(Logger, self(), RealPid, RealMessage),
            RealPid ! LogEpidemicMsg
    end,
    ok.

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()) after the given delay in milliseconds.
-spec send_local_after(non_neg_integer(), erl_local_pid(), message()) -> reference().
send_local_after(Delay, Pid, Message) ->
    {RealPid, RealMessage} = unpack_cookie(Pid, Message),
    erlang:send_after(Delay, RealPid, RealMessage).

%% @doc Returns the pid of the current process.
-spec this() -> mypid().
this() -> this_().

%% @doc Encapsulates the current process' pid (as returned by this/0)
%%      and the given cookie for seamless use of cookies with send/2.
%%      A message Msg to the pid with cookie Cookie will be deliverd as:
%%      {Msg, Cookie} to the Pid.
-spec this_with_cookie(any()) -> mypid().
this_with_cookie(Cookie) -> {this_(), c, Cookie}.

%% @doc Encapsulates the current process' pid (as returned by self/0) and the
%%      given cookie for seamless use of cookies with send_local/2 and
%%      send_local_after/3.
%%      A message Msg to the pid with cookie Cookie will be deliverd as:
%%      {Msg, Cookie} to the Pid.
-spec self_with_cookie(any()) -> erl_local_pid_with_cookie().
self_with_cookie(Cookie) -> {self(), c, Cookie}.

%% @doc Gets the tag of a message (the first element of its tuple - should be an
%%      atom).
-spec get_msg_tag(message() | group_message()) -> atom().
get_msg_tag({Message, _Cookie})
  when is_tuple(Message) andalso is_atom(erlang:element(1, Message)) ->
    get_msg_tag(Message);
get_msg_tag({send_to_group_member, _ProcessName, Message})
  when is_tuple(Message) andalso is_atom(erlang:element(1, Message)) ->
    get_msg_tag(Message);
get_msg_tag(Message)
  when is_tuple(Message) andalso is_atom(erlang:element(1, Message)) ->
    erlang:element(1, Message).

% note: cannot simplify to the following spec -> this lets dialyzer crash
%-spec unpack_cookie(mypid(), message()) -> {mypid(), message()}.
-spec unpack_cookie(mypid(), message()) -> {mypid_plain(), message()};
                   (erl_local_pid(), message()) -> {erl_local_pid_plain(), message()}.
unpack_cookie({Pid, c, Cookie}, Message) -> {Pid, {Message, Cookie}};
unpack_cookie(Pid, Message) -> {Pid, Message}.

%% @doc Creates a group member message and filter out the send options for the
%%      comm_layer process.
-spec pack_group_member(message(), send_options()) -> message().
pack_group_member(Message, []) ->
    Message;
pack_group_member(Message, [{shepherd, _Shepherd}]) ->
    Message;
pack_group_member(Message, Options) ->
    case lists:keyfind(group_member, 1, Options) of
        false                   -> Message;
        {group_member, Process} -> {send_to_group_member, Process, Message}
    end.

%% @doc Initializes the comm_layer by sending a message to the known_hosts. A
%%      valid PID for comm:this/0 will be available afterwards.
%%      (ugly hack to get a valid ip-address into the comm-layer)
-spec init_and_wait_for_valid_pid() -> ok.
init_and_wait_for_valid_pid() ->
    KnownHosts1 = config:read(known_hosts),
    % maybe the list of known nodes is empty and we have a mgmt_server?
    MgmtServer = config:read(mgmt_server),
    KnownHosts = case is_valid(MgmtServer) of
                     true -> [MgmtServer | KnownHosts1];
                     _ -> KnownHosts1
                 end,
    % note, comm:this() may be invalid at this moment
    _ = [send(KnownHost, {hi}, [{group_member, service_per_vm}])
        || KnownHost <- KnownHosts],
    timer:sleep(100),
    case is_valid(this()) of
        true  -> ok;
        false -> init_and_wait_for_valid_pid()
    end.
