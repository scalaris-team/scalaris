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

%% Sending messages
-export([send/2, send/3, send_local/2, send_local_after/3]).

%% Pid manipulation
-export([make_global/1, make_local/1]).
-export([this/0, get/2, this_with_cookie/1, self_with_cookie/1]).
-export([is_valid/1, is_local/1]).
-export([get_ip/1, get_port/1]).

%% Message manipulation
-export([get_msg_tag/1]).
-export([unpack_cookie/2]).

%% initialization
-export([init_and_wait_for_valid_pid/0]).


-ifdef(with_export_type_support).
-export_type([message/0, msg_tag/0, mypid/0,
              erl_local_pid/0, erl_local_pid_with_cookie/0,
              send_options/0]).
% for comm_layer and tester_scheduler
-export_type([erl_local_pid_plain/0]).
% for tester_scheduler
-export_type([mypid_plain/0]).
-endif.

-type cookie()                    :: any().
-type reg_name()                  :: atom().
-type erl_local_pid_plain()       :: pid() | reg_name().
-type erl_local_pid_with_cookie() :: {erl_local_pid_plain(), c, cookie()}.
-type erl_local_pid()             :: erl_local_pid_plain() | erl_local_pid_with_cookie().
-type mypid_plain()               :: {inet:ip_address(), comm_server:tcp_port(), erl_local_pid_plain()}.

-type mypid_with_cookie() :: {mypid_plain(), c, cookie()}.
-type mypid() :: mypid_plain() | mypid_with_cookie().

-type msg_tag() :: atom().
%% there is no variable length-tuple definition for types -> declare
%% messages with up to 10 parameters here:
-type message_plain() ::
        {msg_tag()} |
        {msg_tag(), any()} |
        {msg_tag(), any(), any()} |
        {msg_tag(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()}.
-type message_with_cookie() :: {message_plain(), any()}.
-type message() :: message_plain() | message_with_cookie().
-type group_message() :: {send_to_group_member, atom(), message()}.
-type send_options() :: [{shepherd, Pid::erl_local_pid()} |
                         {group_member, Process::atom()} |
                         {channel, main | prio} | quiet].


%% @doc Sends a message to a process given by its pid.
-spec send(mypid(), message() | group_message()) -> ok.
send(Pid, Msg) -> send(Pid, Msg, []).

%% @doc Send a message to an arbitrary process with the given options.
%%      If a shepherd is given, it will be informed when the sending fails;
%%      with a message of the form:
%%       {send_error, Pid, Msg, Reason}.
%%      If a group_member is given, the message is send to an arbitrary process
%%      of another node instructing it to forward the message to a process in
%%      its group with the given name.
-spec send(mypid(), message() | group_message(), send_options()) -> ok.
send(Pid, Msg, Options) ->
    {RealPid, RealMsg1} = unpack_cookie(Pid, Msg),
    RealMsg = pack_group_member(RealMsg1, Options),
    case erlang:get(trace_mpath) of
        undefined ->
            comm_layer:send(RealPid, RealMsg, Options);
        Logger ->
            LogEpidemicMsg =
                trace_mpath:log_send(Logger, self(), RealPid, RealMsg),
            comm_layer:send(RealPid, LogEpidemicMsg, Options)
    end.

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()).
-spec send_local(erl_local_pid(), message()) -> ok.
send_local(Pid, Msg) ->
    {RealPid, RealMsg} = unpack_cookie(Pid, Msg),
    _ = case erlang:get(trace_mpath) of
            undefined ->
                RealPid ! RealMsg;
            Logger ->
                LogEpidemicMsg =
                    trace_mpath:log_send(Logger, self(), RealPid, RealMsg),
                RealPid ! LogEpidemicMsg
        end,
    ok.

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()) after the given delay in milliseconds.
-spec send_local_after(non_neg_integer(), erl_local_pid(), message()) -> reference().
send_local_after(Delay, Pid, Msg) ->
    {RealPid, RealMsg} = unpack_cookie(Pid, Msg),
    case erlang:get(trace_mpath) of
        undefined ->
            erlang:send_after(Delay, RealPid, RealMsg);
        Logger ->
            LogEpidemicMsg =
                trace_mpath:log_send(Logger, self(), RealPid, RealMsg),
            erlang:send_after(Delay, RealPid, LogEpidemicMsg)
    end.

%% @doc Convert a local erlang pid to a global pid of type mypid() for
%%      use in send/2.
-spec make_global(erl_local_pid_plain()) -> mypid().
make_global(Pid) -> get(Pid, this()).

%% @doc Convert a global mypid() of the current node to a local erlang pid.
-spec make_local(mypid()) -> erl_local_pid_plain().
make_local(Pid) -> comm_layer:make_local(Pid).

%% @doc Returns the global pid of the current process.
-spec this() -> mypid_plain().
this() -> comm_layer:this().

%% @doc Create the PID a process with name Name would have on node _Node.
-spec get(erl_local_pid_plain(), mypid()) -> mypid().
get(Name, {Pid, c, Cookie} = _Node) -> {get(Name, Pid), c, Cookie};
get(Name, {IP, Port, _Pid} = _Node) -> {IP, Port, Name}.

%% @doc Encapsulates the current process' pid (as returned by this/0)
%%      and the given cookie for seamless use of cookies with send/2.
%%      A message Msg to the pid with cookie Cookie will be deliverd as:
%%      {Msg, Cookie} to the Pid.
-spec this_with_cookie(any()) -> mypid().
this_with_cookie(Cookie) -> {this(), c, Cookie}.

%% @doc Encapsulates the current process' pid (as returned by self/0) and the
%%      given cookie for seamless use of cookies with send_local/2 and
%%      send_local_after/3.
%%      A message Msg to the pid with cookie Cookie will be deliverd as:
%%      {Msg, Cookie} to the Pid.
-spec self_with_cookie(any()) -> erl_local_pid_with_cookie().
self_with_cookie(Cookie) -> {self(), c, Cookie}.


%% @doc Check whether the given pid is well formed.
-spec is_valid(mypid() | any()) -> boolean().
is_valid({Pid, c, _Cookie}) -> is_valid(Pid);
is_valid(Pid) -> comm_layer:is_valid(Pid).

%% @doc Check whether a global mypid() can be converted to a local
%%      pid of the current node.
-spec is_local(mypid()) -> boolean().
is_local(Pid) -> comm_layer:is_local(Pid).

%% @doc Gets the IP address of the given (global) mypid().
-spec get_ip(mypid()) -> inet:ip_address().
get_ip(Pid) -> comm_layer:get_ip(Pid).

%% @doc Gets the port of the given (global) mypid().
-spec get_port(mypid()) -> non_neg_integer().
get_port(Pid) -> comm_layer:get_port(Pid).


%% @doc Gets the tag of a message (the first element of its tuple - should be an
%%      atom).
-spec get_msg_tag(message() | group_message()) -> atom().
get_msg_tag({Msg, _Cookie})
  when is_tuple(Msg) andalso is_atom(erlang:element(1, Msg)) ->
    get_msg_tag(Msg);
get_msg_tag({send_to_group_member, _ProcessName, Msg})
  when is_tuple(Msg) andalso is_atom(erlang:element(1, Msg)) ->
    get_msg_tag(Msg);
get_msg_tag(Msg)
  when is_tuple(Msg) andalso is_atom(erlang:element(1, Msg)) ->
    erlang:element(1, Msg).

% note: cannot simplify to the following spec -> this lets dialyzer crash
%-spec unpack_cookie(mypid(), message()) -> {mypid(), message()}.
-spec unpack_cookie(mypid(), message()) -> {mypid_plain(), message()};
                   (erl_local_pid(), message()) -> {erl_local_pid_plain(), message()}.
unpack_cookie({Pid, c, Cookie}, Msg) -> {Pid, {Msg, Cookie}};
unpack_cookie(Pid, Msg)              -> {Pid, Msg}.

%% @doc Creates a group member message and filter out the send options for the
%%      comm_layer process.
-spec pack_group_member(message(), send_options()) -> message().
pack_group_member(Msg, [])                      -> Msg;
pack_group_member(Msg, [{shepherd, _Shepherd}]) -> Msg;
pack_group_member(Msg, Options)                 ->
    case lists:keyfind(group_member, 1, Options) of
        false                   -> Msg;
        {group_member, Process} -> {send_to_group_member, Process, Msg}
    end.

%% @doc Initializes the comm_layer by sending a message to known_hosts. A
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
