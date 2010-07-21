% @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-export([tracer/1, start/0, dump/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 1. put tracer:start() into boot.erl before application:start(boot_cs)
% 2. run benchmark
% 3. call tracer:dump()
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    spawn(?MODULE, tracer, [self()]),
    receive
        {done} -> ok
    end,
    ok.

tracer_perf(Pid) ->
    erlang:trace(all, true, [running, timestamp]),
    comm:send_local(Pid , {done}),
    ets:new(?MODULE, [set, public, named_table]),
    loop([]).

tracer(Pid) ->
    erlang:trace(all, true, [send, procs]),
    comm:send_local(Pid , {done}),
    loop([]).

loop(Ps) ->
    receive
        {trace, Pid, send_to_non_existing_process, Msg, To} ->

            log:log(error,"send_to_non_existing_process: ~p -> ~p (~p)", [Pid, To, Msg]),

            loop(Ps);
        {trace, Pid, exit, Reason} ->
            case Reason of
                normal ->
                    loop(Ps);
                {ok, _Stack,_Num} ->
                    io:format(" EXIT: ~p | ~p~n", [Pid,Reason]),
                    loop(Ps);
                _ ->
                    io:format(" EXIT: ~p | ~p~n", [Pid,Reason]),
                    %io:format("~p~n",Ps),
                    %log:log(warn,"EXIT: ~p | ~p", [Pid,Reason]),
                    loop(Ps)

            end;
        {trace, Pid, spawn, Pid2, {M, F, Args}} ->
            %io:format(" SPAWN: ~p -> ~p in ~p~n", [Pid,Pid2,{M, F, Args}]),
            %log:log2file("TRACER",lists:flatten(io_lib:format(" SPAWN: ~p -> ~p in ~p~n", [Pid,Pid2,{M, F, Args}]))),
            loop([{Pid,Pid2,{M, F, Args}}|Ps]);
        _X ->
            loop(Ps)
    end.



loop_perf(Ps) ->
    receive
        {trace_ts, Pid, in, _, TS} ->
            case ets:lookup(?MODULE, Pid) of
                [] ->
                    ets:insert(?MODULE, {Pid, TS, 0});
                [{Pid, _, Sum}] ->
                    ets:insert(?MODULE, {Pid, TS, Sum})
            end,
            loop_perf(Ps);
        {trace_ts, Pid, out, _, TS} ->
            case ets:lookup(?MODULE, Pid) of
                [] ->
                    ets:insert(?MODULE, {Pid, ok, 0});
                [{Pid, In, Sum}] ->
                    ets:insert(?MODULE, {Pid, ok, timer:now_diff(TS, In) + Sum})
            end,
            loop_perf(Ps);
        _X ->
            io:format("unknown message: ~p~n", [_X]),
            loop_perf(Ps)
    end.

dump() ->
    lists:reverse(lists:keysort(3, ets:tab2list(?MODULE))).
