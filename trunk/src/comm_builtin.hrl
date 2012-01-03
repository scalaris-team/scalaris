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
%% @doc    Message Sending using Distributed Erlang.
%% @end
%% @version $Id$
-module(comm).
-author('schuett@zib.de').
-vsn('$Id$').

%% @doc BUILTIN: Sends a message to an arbitrary process with the given options.
%%      If a shepherd is given, it will be informed when the sending fails;
%%      with a message of the form:
%%       {send_error, Pid, Message, Reason}.
%%      If a group_member is given, the message is send to an arbitrary process
%%      of another node instructing it to forward the message to a process in
%%      its group with the given name.
-spec send(mypid(), message() | group_message(), send_options()) -> ok.
send(Pid, Message, Options) ->
    {RealPid, RealMsg1} = unpack_cookie(Pid, Message),
    % note: ignore further options, e.g. shepherd, with BUILTIN
    RealPid ! pack_group_member(RealMsg1, Options),
    ok.

-spec make_global(erl_pid_plain()) -> mypid().
%% @doc BUILTIN: Returns the given pid (with BUILTIN communication, global pids
%%      are the same as local pids) for use in send/2.
make_global(Pid) -> Pid.

-spec make_local(mypid()) -> erl_pid_plain().
%% @doc BUILTIN: Returns the given pid (with BUILTIN communication, global pids
%%      are the same as local pids).
make_local(Pid) -> Pid.

%% @doc BUILTIN: Returns the pid of the current process.
%%      Note: use this_/1 in internal functions (needed for dialyzer).
-spec this_() -> mypid_plain().
this_() -> self().

%% @doc BUILTIN: Creates the PID a process with name Name would have on node _Node.
-spec get(erl_pid_plain(), mypid()) -> mypid().
get(Name, {Pid, c, Cookie} = _Node) -> {get(Name, Pid), c, Cookie};
get(Name, {_Pid, Host} = _Node) when is_atom(Name) -> {Name, Host};
get(Name, Pid = _Node) when is_atom(Name) -> {Name, node(Pid)};
get(Name, Pid = _Node) ->
    A = node(Name),
    A = node(Pid), % we assume that you only call get with local pids
    Name.

-spec is_valid(mypid() | any()) -> boolean().
%% @doc BUILTIN: Checks if the given pid is valid.
is_valid({Pid, c, _Cookie}) -> is_valid(Pid);
is_valid(Pid) when is_pid(Pid) orelse is_atom(Pid) orelse is_port(Pid) -> true;
is_valid({RegName, _Node}) when is_atom(RegName)                       -> true;
is_valid(_) -> false.

-spec is_local(mypid()) -> boolean().
%% @doc BUILTIN: Checks whether a pid is located at the same node than the
%%      current process.
is_local(Pid) -> erlang:node(Pid) =:= node().

% node({Pid, c, _Cookie}) -> node(Pid);
% node(Pid) -> erlang:node(Pid).
