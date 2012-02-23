% @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Trace what a message triggers in the system by tracing all
%% generated subsequent messages.
%% @version $Id: tracer.erl 1445 2011-02-23 11:34:29Z kruber@zib.de $
-module(trace_mpath).
-author('schintke@zib.de').
-vsn('$Id:$').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% 1. call trace_mpath:on()
%% 2. perform a request like api_tx:read("a")
%% 3. call trace_mpath:off()
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").

-export([on/0, on/1, off/0]).
-export([log_send/4]).
-export([log_recv/4]).
-export([epidemic_reply_msg/4]).

-type logger() ::
        ioformat
%% not yet supported
%%      | ctpal
%%      | {log, Level::term()}
%%      | {log_collector, comm:mypid()}
        .

-ifdef(with_export_type_support).
-export_type([logger/0]).
-endif.

-spec on() -> ok.
on() -> on(ioformat).
-spec on(logger()) -> ok.
on(Logger) ->
    erlang:put(trace_mpath, Logger), ok.

-spec off() -> ok.
off() ->
    erlang:erase(trace_mpath), ok.

epidemic_reply_msg(Logger, FromPid, ToPid, Msg) ->
    From =
        case is_pid(FromPid) of
            true -> {comm:make_global(FromPid),
                     pid_groups:group_and_name_of(FromPid)};
            false -> {FromPid,
                      pid_groups:group_and_name_of(comm:make_local(FromPid))}
        end,
    To = case is_pid(ToPid) of
             true -> {comm:make_global(ToPid),
                      pid_groups:group_and_name_of(ToPid)};
             false ->
                 case comm:is_local(ToPid) of
                     true -> {ToPid, pid_groups:group_and_name_of(comm:make_local(ToPid))};
                     false -> {ToPid, non_local_pid_name_unknown}
                 end
         end,
    {'$gen_component', trace_mpath, Logger, From, To, Msg}.

log_send(Logger, FromPid, ToPid, Msg) ->
    EMsg = {'$gen_component', trace_mpath, Logger, From, To, Msg} =
        epidemic_reply_msg(Logger, FromPid, ToPid, Msg),
    case Logger of
        ioformat ->
            io:format("send ~.0p -> ~.0p:~n  ~.0p.~n",
                      [From, To, Msg])
    end,
    EMsg.

log_recv(Logger, _From, _To, _Msg) ->
    case Logger of
        ioformat ->
%%            io:format("recv ~.0p -> ~.0p:~n  ~.0p.~n",
%%                      [From, To, Msg])
            ok
    end,
    ok.
