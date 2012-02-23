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
%% @doc    Message Sending using TCP.
%% @end
%% @version $Id$

-export([get_ip/1, get_port/1]).

%% @doc TCP_LAYER: Sends a message to an arbitrary process with the given options.
%%      If a shepherd is given, it will be informed when the sending fails;
%%      with a message of the form:
%%       {send_error, Pid, Message, Reason}.
%%      If a group_member is given, the message is send to an arbitrary process
%%      of another node instructing it to forward the message to a process in
%%      its group with the given name.
-spec send(mypid(), message() | group_message(), send_options()) -> ok.
send(Pid, Message, Options) ->
    {RealPid, RealMsg1} = unpack_cookie(Pid, Message),
    RealMessage = pack_group_member(RealMsg1, Options),
    case erlang:get(trace_mpath) of
        undefined ->
            comm_layer:send(RealPid, RealMessage, Options);
        Logger ->
            LogEpidemicMsg =
                trace_mpath:log_send(Logger, self(), RealPid, RealMessage),
            comm_layer:send(RealPid, LogEpidemicMsg, Options)
    end.

-spec make_global(erl_pid_plain()) -> mypid().
%% @doc TCP_LAYER: Converts a local erlang pid to a global pid of type mypid()
%%      for use in send/2.
make_global(Pid) -> get(Pid, this()).

-spec make_local(mypid()) -> erl_pid_plain().
%% @doc TCP_LAYER: Converts a global mypid() of the current node to a local
%%      erlang pid.
make_local(Pid) -> comm_layer:make_local(Pid).

%% @doc TCP_LAYER: Returns the pid of the current process.
%%      Note: use this_/1 in internal functions (needed for dialyzer).
-spec this_() -> mypid_plain().
this_() -> comm_layer:this().

%% @doc TCP_LAYER: Creates the PID a process with name Name would have on node _Node.
-spec get(erl_pid_plain(), mypid()) -> mypid().
get(Name, {Pid, c, Cookie} = _Node) -> {get(Name, Pid), c, Cookie};
get(Name, {IP, Port, _Pid} = _Node) -> {IP, Port, Name}.

%% @doc TCP_LAYER: Checks if the given pid is valid.
-spec is_valid(mypid() | any()) -> boolean().
is_valid({Pid, c, _Cookie}) -> is_valid(Pid);
is_valid(Pid) -> comm_layer:is_valid(Pid).

%% @doc TCP_LAYER: Checks whether a global mypid() can be converted to a local
%%      pid of the current node.
-spec is_local(mypid()) -> boolean().
is_local(Pid) -> comm_layer:is_local(Pid).

% node({Pid, c, _Cookie}) -> node(Pid);
% node(Pid) -> {element(1, Pid), element(2, Pid)}.

-spec get_ip(mypid()) -> inet:ip_address().
%% @doc TCP_LAYER: Gets the IP address of the given (global) mypid().
get_ip(Pid) -> comm_layer:get_ip(Pid).

-spec get_port(mypid()) -> non_neg_integer().
%% @doc TCP_LAYER: Gets the port of the given (global) mypid().
get_port(Pid) -> comm_layer:get_port(Pid).
