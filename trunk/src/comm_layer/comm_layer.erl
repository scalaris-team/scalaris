% @copyright 2008-2011 Zuse Institute Berlin

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

%% @doc Public interface to Communication Layer.
%%      Generic functions to send messages.  Distinguishes on runtime
%%      whether the destination is in the same Erlang virtual machine
%%      (use !  for sending) or on a remote site (use
%%      comm_server:send()).
%% @end
%% @version $Id$
-module(comm_layer).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

-export([send/2, send_with_shepherd/3, this/0, is_valid/1, is_local/1, make_local/1,
         get_ip/1, get_port/1, report_send_error/3]).

-include("scalaris.hrl").

-type process_id() :: {inet:ip_address(), comm_server:tcp_port(), comm:erl_pid_plain()}.

-spec send_with_shepherd(process_id(), comm:message(), comm:erl_local_pid() | unknown) -> ok.
send_with_shepherd(Target, Message, Shepherd) ->
    IsLocal = is_local(Target),
    case is_valid(Target) of
        true when IsLocal ->
            ?LOG_MESSAGE(Message, byte_size(term_to_binary(Message))),
            LocalTarget = make_local(Target),
            PID = case is_pid(LocalTarget) of
                      true -> LocalTarget;
                      false -> whereis(LocalTarget)
                  end,
            case PID of
                undefined ->
                    log:log(warn,
                            "[ CC ] Cannot locally send msg to unknown named"
                            " process ~p: ~.0p~n", [LocalTarget, Message]),
                    report_send_error(Shepherd, Target, Message);
                _ -> PID ! Message
            end,
            ok;
        true ->
            comm_server:send(Target, Message, Shepherd);
        _ ->
            log:log(error,"[ CL ] wrong call to comm:send: ~w ! ~w", [Target, Message]),
            log:log(error,"[ CL ] stacktrace: ~w", [util:get_stacktrace()]),
            ok
    end.

%% @doc send message via tcp, if target is not in same Erlang VM.
-spec send(process_id(), comm:message()) -> ok.
send(Target, Message) ->
    send_with_shepherd(Target, Message, unknown).

%% @doc returns process descriptor for the calling process
-spec this() -> process_id().
this() ->
    %% Note: We had caching enabled here, but the eshell takes over
    %% the process dictionary to a new pid in case of failures, so we
    %% got outdated pid info here.
    %% case erlang:get(comm_this) of
    %%    undefined ->
    {LocalIP, LocalPort} = comm_server:get_local_address_port(),
    _This1 = {LocalIP, LocalPort, self()}
    %% , case LocalIP of
    %%     undefined -> ok;
    %%     _         -> erlang:put(comm_this, This1)
    %% end,
    %% This1;
    %%     This -> This
    %% end
    .

-spec is_valid(process_id() | any()) -> boolean().
is_valid({{_IP1, _IP2, _IP3, _IP4} = _IP, _Port, _Pid}) -> true;
is_valid(_) -> false.

-spec is_local(process_id()) -> boolean().
is_local({IP, Port, _Pid}) ->
    {MyIP, MyPort} = comm_server:get_local_address_port(),
    {IP, Port} =:= {MyIP, MyPort}.

-spec make_local(process_id()) -> comm:erl_pid_plain().
make_local({_IP, _Port, Pid}) ->
    Pid.

%% @doc Gets the IP address of the given process id.
-spec get_ip(process_id()) -> inet:ip_address().
get_ip({IP, _Port, _Pid}) -> IP.

%% @doc Gets the port of the given process id.
-spec get_port(process_id()) -> comm_server:tcp_port().
get_port({_IP, Port, _Pid}) -> Port.

-spec report_send_error(unknown | comm:erl_local_pid(), process_id(), comm:message()) -> ok.
report_send_error(Shepherd, Target, Message) ->
    case Shepherd of
        unknown ->
            ok;
        ShepherdPid ->
            comm:send_local(ShepherdPid, {send_error, Target, Message})
    end,
    ok.
