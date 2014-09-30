% @copyright 2009-2014 Zuse Institute Berlin

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
%% @doc Simple Profiler for Scalaris.
%% @version $Id$
-module(tracer).
-author('schuett@zib.de').
-vsn('$Id$').

-export([tracer/1, start/0, dump/0,
         tracer_perf/1, start_perf/0, dump_perf/0]).

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 1. put tracer:start() and/or tracer:start_perf() into scalaris_app.erl
%    before application:start(scalaris)
% 2. run benchmark
% 3. call tracer:dump() or tracer:dump_perf()
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start() -> ok.
start() ->
    spawn(?MODULE, tracer, [self()]),
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({done}, ok) end,
    ok.

-spec start_perf() -> ok.
start_perf() ->
    spawn(?MODULE, tracer_perf, [self()]),
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({done}, ok) end,
    ok.

-spec tracer(Pid::comm:erl_local_pid()) -> no_return().
tracer(Pid) ->
    erlang:trace(all, true, [send, procs]),
    comm:send_local(Pid, {done}),
    _ = ets:new(tracer, [set, protected, named_table]),
    loop().

-spec tracer_perf(Pid::comm:erl_local_pid()) -> no_return().
tracer_perf(Pid) ->
    erlang:trace(all, true, [running, timestamp]),
    comm:send_local(Pid, {done}),
    _ = ets:new(tracer_perf, [set, protected, named_table]),
    loop_perf().

-spec loop() -> no_return().
loop() ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {trace, Pid, send_to_non_existing_process, Msg, To}, %% ->
            begin
                log:log(error,"send_to_non_existing_process: ~p -> ~p (~p)", [Pid, To, Msg]),
                loop()
            end
          );
        ?SCALARIS_RECV(
            {trace, Pid, exit, Reason}, %% ->
            case Reason of
                normal ->
                    loop();
                {ok, _Stack,_Num} ->
                    io:format(" EXIT: ~p | ~p~n", [Pid, Reason]),
                    loop();
                _ ->
                    io:format(" EXIT: ~p | ~p~n", [Pid, Reason]),
                    %io:format("~p~n", [dump()]),
                    %log:log(warn,"EXIT: ~p | ~p", [Pid, Reason]),
                    loop()
            end
          );
        ?SCALARIS_RECV(
            {trace, Pid, spawn, Pid2, {M, F, Args}}, %% ->
            begin
                %io:format(" SPAWN: ~p -> ~p in ~p~n", [Pid, Pid2, {M, F, Args}]),
                %log:log(debug "[ TRACER ] SPAWN: ~p -> ~p in ~p", [Pid, Pid2, {M, F, Args}]),
                ets:insert(tracer, {Pid, Pid2, {M, F, Args}}),
                loop()
            end
          );
        _X ->
            loop()
    end.

-spec loop_perf() -> no_return().
loop_perf() ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {trace_ts, Pid, in, _, TS}, %% ->
            begin
                case ets:lookup(tracer_perf, Pid) of
                    [] ->
                        ets:insert(tracer_perf, {Pid, TS, 0});
                    [{Pid, _, Sum}] ->
                        ets:insert(tracer_perf, {Pid, TS, Sum})
                end,
                loop_perf()
            end
          );
        ?SCALARIS_RECV(
            {trace_ts, Pid, out, _, TS}, %% ->
            begin
                case ets:lookup(tracer_perf, Pid) of
                    [] ->
                        ets:insert(tracer_perf, {Pid, ok, 0});
                    [{Pid, In, Sum}] ->
                        ets:insert(tracer_perf, {Pid, ok, timer:now_diff(TS, In) + Sum})
                end,
                loop_perf()
            end
          );
        _X ->
            io:format("unknown message: ~p~n", [_X]),
            loop_perf()
    end.
-spec dump() -> [{Pid::pid(), Pid2::pid(), {M::module(), F::atom(), Args::list()}}].
dump() ->
    ets:tab2list(tracer).

-spec dump_perf() -> [{Pid::pid(), ScheduledIn::{MegaSecs::integer(), Secs::integer(), MicroSecs::integer()} | ok, Runtime::integer()}].
dump_perf() ->
    lists:reverse(lists:keysort(3, ets:tab2list(tracer_perf))).
