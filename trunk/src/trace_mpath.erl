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
-export([log_info/3]).
-export([log_recv/4]).
-export([epidemic_reply_msg/4]).

-type logger() ::
        io_format
      | {log_collector, comm:mypid()}.
%% not yet supported
%%      | ctpal
%%      | {log, Level::term()}

-type pidinfo() :: {comm:mypid(), {pid_groups:groupname(),
                                   pid_groups:pidname()}}.
-type anypid() :: pid() | comm:mypid() | pidinfo().

-type gc_mpath_msg() ::
        {'$gen_component', trace_mpath, logger(), pidinfo(), pidinfo(),
         comm:message()}.

-ifdef(with_export_type_support).
-export_type([logger/0]).
-export_type([pidinfo/0]).
-endif.

-spec on() -> ok.
on() -> on(io_format).

-spec on(logger() | comm:mypid()) -> ok.
on(Logger) ->
    case comm:is_valid(Logger) of
        true -> %% just a pid was given
            erlang:put(trace_mpath, {log_collector, Logger});
        false ->
            erlang:put(trace_mpath, Logger)
    end,
 ok.

-spec off() -> ok.
off() ->
    erlang:erase(trace_mpath), ok.

-spec epidemic_reply_msg(logger(), anypid(), anypid(), comm:message()) ->
                                gc_mpath_msg().
epidemic_reply_msg(Logger, FromPid, ToPid, Msg) ->
    From = normalize_pidinfo(FromPid),
    To = normalize_pidinfo(ToPid),
    {'$gen_component', trace_mpath, Logger, From, To, Msg}.

-spec log_send(logger(), anypid(), anypid(), comm:message()) ->
                      gc_mpath_msg().
log_send(Logger, FromPid, ToPid, Msg) ->
    From = normalize_pidinfo(FromPid),
    To = normalize_pidinfo(ToPid),
    Now = os:timestamp(),
    case Logger of
        io_format ->
            io:format("~p send ~.0p -> ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now), From, To, Msg]);
        {log_collector, LoggerPid} ->
            %% don't log the sending of log messages ...
            RestoreThis = erlang:get(trace_mpath),
            off(),
            comm:send(LoggerPid, {log_send, Now, From, To, Msg}),
            on(RestoreThis)
    end,
    epidemic_reply_msg(Logger, From, To, Msg).

-spec log_info(logger(), anypid(), term()) -> ok.
log_info(Logger, FromPid, Info) ->
    From = normalize_pidinfo(FromPid),
    Now = os:timestamp(),
    case Logger of
        io_format ->
            io:format("~p info ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now), From, Info]);
        {log_collector, LoggerPid} ->
            %% don't log the sending of log messages ...
            RestoreThis = erlang:get(trace_mpath),
            off(),
            comm:send(LoggerPid, {log_info, Now, From, Info}),
            on(RestoreThis)
    end,
    ok.

-spec log_recv(logger(), anypid(), anypid(), comm:message()) -> ok.
log_recv(Logger, FromPid, ToPid, Msg) ->
    From = normalize_pidinfo(FromPid),
    To = normalize_pidinfo(ToPid),
    Now = os:timestamp(),
    case Logger of
        io_format ->
            io:format("~p recv ~.0p -> ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now), From, To, Msg]);
        {log_collector, LoggerPid} ->
            %% don't log the sending of log messages ...
            RestoreThis = erlang:get(trace_mpath),
            off(),
            comm:send(LoggerPid, {log_recv, Now, From, To, Msg}),
            on(RestoreThis)
    end,
    ok.

-spec normalize_pidinfo(anypid()) -> pidinfo().
normalize_pidinfo(Pid) ->
    case is_pid(Pid) of
        true -> {comm:make_global(Pid), pid_groups:group_and_name_of(Pid)};
        false ->
            case comm:is_valid(Pid) of
                true ->
                    case comm:is_local(Pid) of
                        true -> {Pid,
                                 pid_groups:group_and_name_of(
                                   comm:make_local(Pid))};
                        false -> {Pid, non_local_pid_name_unknown}
                    end;
                false -> %% already a pidinfo()
                    Pid
            end
    end.
