%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-include("transstore/trecords.hrl").
-include("scalaris.hrl").

-export([send/2, send_local_after/3 , this/0, get/2, send_to_group_member/3,
         send_local/2, make_global/1, is_valid/1, get_msg_tag/1,
         this_with_cookie/1, self_with_cookie/1]).

-ifdef(with_export_type_support).
-export_type([message/0, message_tag/0, mypid/0, erl_local_pid/0]).
-endif.

-type cookie() :: any().
-type reg_name() :: atom().
-type erl_local_pid_plain() :: pid() | reg_name().
-type erl_local_pid_with_cookie() :: {erl_local_pid_plain(), c, cookie()}.
-type erl_local_pid() :: erl_local_pid_plain() | erl_local_pid_with_cookie().
-type erl_pid_plain() :: erl_local_pid_plain() | {reg_name(), node()}. % port() | 


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

%% @doc Sends a message to an arbitrary process of another node instructing it
%%      to forward the message to a process in its group with the given name.
-spec send_to_group_member(Dest::mypid(), ProcessName::atom(), Message::message()) -> ok.
send_to_group_member(DestNode, Processname, Mesg) ->
    send(DestNode, {send_to_group_member, Processname, Mesg}).

%% @doc Encapsulates the current process' pid (as returned by self/0) and the
%%      given cookie for seamless use of cookies with send_local/2 and
%%      send_local_after/3.
-spec self_with_cookie(any()) -> erl_local_pid_with_cookie().
self_with_cookie(Cookie) ->
    {self(), c, Cookie}.

%% @doc Gets the tag of a message (the first element of its tuple - should be an
%%      atom).
-spec get_msg_tag(message() | group_message()) -> atom().
get_msg_tag({Message, _Cookie}) when is_tuple(Message) andalso is_atom(erlang:element(1, Message)) ->
    get_msg_tag(Message);
get_msg_tag({send_to_group_member, _ProcessName, Message}) when is_tuple(Message) andalso is_atom(erlang:element(1, Message)) ->
    get_msg_tag(Message);
get_msg_tag(Message) when is_tuple(Message) andalso is_atom(erlang:element(1, Message)) ->
    erlang:element(1, Message).

% specs for the functions below which differ depending on the currently defined
% macros
-spec this() -> mypid().
-spec this_with_cookie(any()) -> mypid().
-spec send(Dest::mypid(), Message::message() | group_message()) -> ok.
-spec send_local(Dest::erl_local_pid(), Message::message()) -> ok.
-spec send_local_after(Delay::non_neg_integer(), Dest::erl_local_pid() , Message::message()) -> reference().
-spec make_global(erl_pid_plain()) -> mypid().
-spec get(erl_pid_plain(), mypid()) -> mypid().
-spec is_valid(any()) -> boolean().

-ifdef(TCP_LAYER).

%% @doc TCP_LAYER: Returns the pid of the current process.
this() ->
    comm_layer:this().

%% @doc TCP_LAYER: Encapsulates the current process' pid (as returned by this/0)
%%      and the given cookie for seamless use of cookies with send/2.
this_with_cookie(Cookie) ->
    {comm_layer:this(), c, Cookie}.

%% @doc TCP_LAYER: Sends a message to a process given by its pid.
send({Pid, c, Cookie}, Message) ->
    send(Pid, {Message, Cookie});
send(Pid, Message) ->
    comm_layer:send(Pid, Message).

%% @doc TCP_LAYER: Sends a message to a local process given by its local pid
%%      (as returned by self()).
send_local({Pid, c, Cookie}, Message) ->
    send_local(Pid, {Message, Cookie});
send_local(Pid, Message) ->
    Pid ! Message,
    ok.

%% @doc TCP_LAYER: Sends a message to a local process given by its local pid
%%      (as returned by self()) after the given delay in milliseconds.
send_local_after(Delay, {Pid, c, Cookie}, Message) ->
    send_local_after(Delay, Pid, {Message, Cookie});
send_local_after(Delay, Pid, Message) ->
    erlang:send_after(Delay, Pid, Message).

%% @doc TCP_LAYER: Converts a local erlang pid to a global pid of type mypid()
%%      for use in send/2.
make_global(Pid) ->
    get(Pid, comm:this()).

%% @doc TCP_LAYER: Creates the pid a process with name Name would have on node
%%      _Node.
get(Name, {Pid, c, Cookie} = _Node) ->
    {get(Name, Pid), c, Cookie};
get(Name, {IP, Port, _Pid} = _Node) ->
    {IP, Port, Name}.

%% @doc TCP_LAYER: Checks if the given pid is valid. 
is_valid({Pid, c, _Cookie}) ->
    is_valid(Pid);
is_valid(Pid) ->
    comm_layer:is_valid(Pid).

-endif.
-ifdef(BUILTIN).

%% @doc BUILTIN: Returns the pid of the current process.
this() ->
    self().

%% @doc BUILTIN: Encapsulates the current process' pid (as returned by this/0)
%%      and the given cookie for seamless use of cookies with send/2.
this_with_cookie(Cookie) ->
    {self(), c, Cookie}.

%% @doc BUILTIN: Sends a message to a process given by its pid.
send(Pid, Message) ->
    send_local(Pid, Message).

%% @doc BUILTIN: Sends a message to a local process given by its local pid
%%      (as returned by self()).
send_local({Pid, c, Cookie}, Message) ->
    send_local(Pid, {Message, Cookie});
send_local(Pid, Message) ->
    Pid ! Message,
    ok.

%% @doc BUILTIN: Sends a message to a local process given by its local pid
%%      (as returned by self()) after the given delay in milliseconds.
send_local_after(Delay, {Pid, c, Cookie}, Message) ->
    send_local_after(Delay, Pid, {Message, Cookie});
send_local_after(Delay, Pid, Message) ->
    erlang:send_after(Delay, Pid, Message).

%% @doc BUILTIN: Returns the given pid (with BUILTIN communication, global pids
%%      are the same as local pids) for use in send/2.
make_global(Pid) ->
    Pid.

%% @doc BUILTIN: Creates the pid a process with name Name would have on node
%%      _Node.
get(Name, {Pid, c, Cookie} = _Node) ->
    {get(Name, Pid), c, Cookie};
%get(Name, {_Pid,Host}) ->
%    {Name, Host};
get(Name, {_Pid, Host} = _Node) when is_atom(Name) ->
    {Name, Host};
get(Name, Pid = _Node) when is_atom(Name) ->
    %io:format("CS: ~p ~p ~n ",[Name,Pid]),
    {Name, node(Pid)};
get(Name, Pid = _Node) ->
    A = node(Name),
    A = node(Pid), % we assume that you only call get with local pids
    Name.

%% @doc BUILTIN: Checks if the given pid is valid. 
is_valid({Pid, c, _Cookie}) ->
    is_valid(Pid);
%% -type erl_pid_plain() :: pid() | reg_name() | port() | {reg_name(), node()}.
is_valid(Pid) when is_pid(Pid) orelse is_atom(Pid) orelse is_port(Pid) ->
    true;
is_valid({RegName, _Node}) when is_atom(RegName) ->
    true;
is_valid(_) ->
    false.

-endif.
