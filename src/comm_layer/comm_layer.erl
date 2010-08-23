% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-export([send/2, this/0, is_valid/1]).

-include("scalaris.hrl").

-type(process_id() :: {inet:ip_address(), integer(), pid()}).

%% @doc send message via tcp, if target is not in same Erlang VM.
-spec send(process_id(), term()) -> ok.
send({{_IP1, _IP2, _IP3, _IP4} = IP, Port, Pid} = Target, Message) ->
    {MyIP,MyPort} = comm_server:get_local_address_port(),
    case {IP, Port} of
        {MyIP, MyPort} ->
            ?LOG_MESSAGE(Message, byte_size(term_to_binary(Message))),
            Pid ! Message,
            ok;
        _ ->
            comm_server:send(Target, Message)
    end;

send(Target, Message) ->
    log:log(error,"[ CL ] wrong call to comm:send: ~w ! ~w", [Target, Message]),
    log:log(error,"[ CL ] stacktrace: ~w", [util:get_stacktrace()]),
    ok.

%% @doc returns process descriptor for the calling process
-spec(this/0 :: () -> process_id()).
this() ->
    {LocalIP, LocalPort} = comm_server:get_local_address_port(),
    {LocalIP, LocalPort, self()}.

-spec is_valid(process_id()) -> boolean().
is_valid({{_IP1, _IP2, _IP3, _IP4} = _IP, _Port, _Pid}) -> true;
is_valid(_) -> false.
