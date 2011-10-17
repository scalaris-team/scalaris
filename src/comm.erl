%  @copyright 2007-2011 Zuse Institute Berlin

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
-export([send/2, send_local/2, send_local_after/3, send_to_group_member/3,
         send_with_shepherd/3]).
%% Pid manipulation
-export([make_global/1, make_local/1]).
-export([this/0, get/2, this_with_cookie/1, self_with_cookie/1]).
-export([is_valid/1, is_local/1]).
-ifdef(TCP_LAYER).
-export([get_ip/1, get_port/1]).
-endif.
%%-export([node/1]).
%% Message manipulation
-export([get_msg_tag/1]).
-export([unpack_cookie/2]).

-ifdef(with_export_type_support).
-export_type([message/0, message_tag/0, mypid/0,
              erl_local_pid/0, erl_local_pid_with_cookie/0]).
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

%% @doc Sends a message to a process given by its pid.
-spec send(mypid(), message() | group_message()) -> ok.
-ifdef(TCP_LAYER).
send(Pid, Message) ->
    {RealPid, RealMessage} = unpack_cookie(Pid, Message),
    comm_layer:send(RealPid, RealMessage).
-endif.
-ifdef(BUILTIN). %% @hidden
send(Pid, Message) ->
    {RealPid, RealMessage} = unpack_cookie(Pid, Message),
    send_local(RealPid, RealMessage).
-endif.

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()).
-spec send_local(erl_local_pid(), message()) -> ok.
send_local(Pid, Message) ->
    {RealPid, RealMessage} = unpack_cookie(Pid, Message),
    RealPid ! RealMessage,
    ok.

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()) after the given delay in milliseconds.
-spec send_local_after(non_neg_integer(), erl_local_pid(), message()) -> reference().
send_local_after(Delay, Pid, Message) ->
    {RealPid, RealMessage} = unpack_cookie(Pid, Message),
    erlang:send_after(Delay, RealPid, RealMessage).

%% @doc Sends a message to an arbitrary process of another node instructing it
%%      to forward the message to a process in its group with the given name.
-spec send_to_group_member(mypid(), atom(), message()) -> ok.
send_to_group_member(DestNode, Processname, Mesg) ->
    send(DestNode, {send_to_group_member, Processname, Mesg}).

%% @doc Sends a message to an arbitrary process. When the sending fails, the
%%      shepherd process will be informed with a message of the form:
%%       {send_error, Pid, Message}.
-spec send_with_shepherd(mypid(), message() | group_message(), erl_local_pid()) -> ok.
-ifdef(TCP_LAYER).
send_with_shepherd(Pid, Message, Shepherd) ->
    comm_layer:send_with_shepherd(Pid, Message, Shepherd).
-endif.
-ifdef(BUILTIN). %% @hidden
send_with_shepherd(Pid, Message, _Shepherd) ->
    send(Pid, Message).
-endif.

-spec make_global(erl_pid_plain()) -> mypid().
-ifdef(TCP_LAYER).
%% @doc TCP_LAYER: Converts a local erlang pid to a global pid of type mypid()
%%      for use in send/2.
make_global(Pid) -> get(Pid, this()).
-endif.
-ifdef(BUILTIN).
%% @doc BUILTIN: Returns the given pid (with BUILTIN communication, global pids
%%      are the same as local pids) for use in send/2.
make_global(Pid) -> Pid.
-endif.

-spec make_local(mypid()) -> erl_pid_plain().
-ifdef(TCP_LAYER).
%% @doc TCP_LAYER: Converts a global mypid() of the current node to a local
%%      erlang pid.
make_local(Pid) -> comm_layer:make_local(Pid).
-endif.
-ifdef(BUILTIN).
%% @doc BUILTIN: Returns the given pid (with BUILTIN communication, global pids
%%      are the same as local pids).
make_local(Pid) -> Pid.
-endif.

%% @doc Returns the pid of the current process.
-spec this() -> mypid().
this() -> this_().

%% @doc Returns the pid of the current process.
%%      Note: use this_/1 in internal functions (needed for dialyzer).
-spec this_() -> mypid_plain().
-ifdef(TCP_LAYER).
this_() -> comm_layer:this().
-endif.
-ifdef(BUILTIN). %% @hidden
this_() -> self().
-endif.

%% @doc Creates the PID a process with name Name would have on node _Node.
-spec get(erl_pid_plain(), mypid()) -> mypid().
-ifdef(TCP_LAYER).
get(Name, {Pid, c, Cookie} = _Node) -> {get(Name, Pid), c, Cookie};
get(Name, {IP, Port, _Pid} = _Node) -> {IP, Port, Name}.
-endif.
-ifdef(BUILTIN). %% @hidden
get(Name, {Pid, c, Cookie} = _Node) -> {get(Name, Pid), c, Cookie};
get(Name, {_Pid, Host} = _Node) when is_atom(Name) -> {Name, Host};
get(Name, Pid = _Node) when is_atom(Name) -> {Name, node(Pid)};
get(Name, Pid = _Node) ->
    A = node(Name),
    A = node(Pid), % we assume that you only call get with local pids
    Name.
-endif.

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

-spec is_valid(mypid() | any()) -> boolean().
%% @doc Checks if the given pid is valid.
-ifdef(TCP_LAYER).
is_valid({Pid, c, _Cookie}) -> is_valid(Pid);
is_valid(Pid) -> comm_layer:is_valid(Pid).
-endif.
-ifdef(BUILTIN). %% @hidden
is_valid({Pid, c, _Cookie}) -> is_valid(Pid);
is_valid(Pid) when is_pid(Pid) orelse is_atom(Pid) orelse is_port(Pid) -> true;
is_valid({RegName, _Node}) when is_atom(RegName)                       -> true;
is_valid(_) -> false.
-endif.

-spec is_local(mypid()) -> boolean().
-ifdef(TCP_LAYER).
%% @doc TCP_LAYER: Checks whether a global mypid() can be converted to a local
%%      pid of the current node.
is_local(Pid) -> comm_layer:is_local(Pid).
-endif.
-ifdef(BUILTIN).
%% @doc BUILTIN: Checks whether a pid is located at the same node than the
%%      current process.
is_local(Pid) -> erlang:node(Pid) =:= node().
-endif.

% -spec node(
% -ifdef(TCP_LAYER).
% node({Pid, c, _Cookie}) -> node(Pid);
% node(Pid) -> {element(1, Pid), element(2, Pid)}.
% -endif.
% -ifdef(BUILTIN).
% node({Pid, c, _Cookie}) -> node(Pid);
% node(Pid) -> erlang:node(Pid).
% -endif.

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

-ifdef(TCP_LAYER).
-spec get_ip(mypid()) -> inet:ip_address().
%% @doc TCP_LAYER: Gets the IP address of the given (global) mypid().
get_ip(Pid) -> comm_layer:get_ip(Pid).

-spec get_port(mypid()) -> non_neg_integer().
%% @doc TCP_LAYER: Gets the port of the given (global) mypid().
get_port(Pid) -> comm_layer:get_port(Pid).
-endif.

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
    _ = [comm:send_to_group_member(KnownHost, service_per_vm, {hi})
        || KnownHost <- KnownHosts],
    timer:sleep(100),
    case comm:is_valid(comm:this()) of
        true  -> ok;
        false -> init_and_wait_for_valid_pid()
    end.
