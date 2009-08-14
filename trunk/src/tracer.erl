%%%------------------------------------------------------------------------------
%%% File    : tracer.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created : 8 Jan 2009 by Thorsten Schuett <schuett@zib.de>
%%%------------------------------------------------------------------------------
%% @doc 
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(tracer).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([tracer/1, start_link/0]).

start_link() ->
    Link = spawn_link(?MODULE, tracer, [self()]),
    receive
	done ->
	    ok
    end,
    {ok, Link}.



tracer(Pid) ->
    erlang:trace(all, true, [send, procs]),
    cs_send:send_local(Pid , done),
    loop([]).

loop(Ps) ->
    receive
	{trace, Pid, send_to_non_existing_process, Msg, To} ->
        
        log:log(error,"send_to_non_existing_process: ~p -> ~p (~p)", [Pid, To, Msg]),
        
	    loop(Ps);
	{trace, Pid, exit, Reason} ->
        case Reason  of
			normal ->
					loop(Ps);
            {ok, _Stack,_Num} -> 
					loop(Ps);				
			_ ->
               
        			%io:format(" EXIT: ~p | ~p~n", [Pid,Reason]),
                    %io:format("~p~n",Ps),
        			log:log(warn,"EXIT: ~p | ~p", [Pid,Reason]),
	    			loop(Ps)        	
            
        end;
   {trace, Pid, spawn, Pid2, {M, F, Args}} ->
        %io:format(" SPAWN: ~p -> ~p in  ~p~n", [Pid,Pid2,{M, F, Args}]),
        %log:log2file("TRACER",lists:flatten(io_lib:format(" SPAWN: ~p -> ~p in  ~p~n", [Pid,Pid2,{M, F, Args}]))),
    	loop([{Pid,Pid2,{M, F, Args}}|Ps]);
	_X ->
	    loop(Ps)
    end.
