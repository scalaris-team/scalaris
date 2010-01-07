%%%------------------------------------------------------------------------------
%%% File    : tracer.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Simple Profiler for Scalaris
%%%
%%% Created : 8 Jan 2009 by Thorsten Schuett <schuett@zib.de>
%%%------------------------------------------------------------------------------
%% @doc Simple Profiler for Scalaris
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(tracer).

-author('schuett@zib.de').
-vsn('$Id$ ').

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
	done ->
	    ok
    end,
    ok.

tracer(Pid) ->
    erlang:trace(all, true, [running, timestamp]),
    cs_send:send_local(Pid , done),
    ets:new(?MODULE, [set, public, named_table]),
    loop([]).

loop(Ps) ->
    receive
        {trace_ts, Pid, in, _, TS} ->
            case ets:lookup(?MODULE, Pid) of
                [] ->
                    ets:insert(?MODULE, {Pid, TS, 0});
                [{Pid, _, Sum}] ->
                    ets:insert(?MODULE, {Pid, TS, Sum})
            end,
            loop(Ps);
        {trace_ts, Pid, out, _, TS} ->
            case ets:lookup(?MODULE, Pid) of
                [] ->
                    ets:insert(?MODULE, {Pid, ok, 0});
                [{Pid, In, Sum}] ->
                    ets:insert(?MODULE, {Pid, ok, timer:now_diff(TS, In) + Sum})
            end,
            loop(Ps);
        _X ->
            io:format("unknown message: ~p~n", [_X]),
            loop(Ps)
    end.

dump() ->
    lists:reverse(lists:keysort(3, ets:tab2list(?MODULE))).
