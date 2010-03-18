%% Author: christian
%% Created: Jun 29, 2009
%% Description: TODO: Add description to scheduler
-module(scheduler).

-include("../../include/scalaris.hrl").

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([send/3, send/2, start/0, init/1, stop/0]).

%%
%% API Functions
%%
send(Pid,M) ->
    Delay = get_delay(self(),Pid),
    send(Delay,Pid,M).    
%send(0,Pid,MSG) ->
%    Pid ! MSG;
%send(Delay,Pid,MSG) ->
%    erlang:send_after(Delay, Pid, MSG).

send(Delay,Pid,MSG) ->
    R = scheduler ! {{ss_mesg},{Delay,Pid,MSG},self()},
    receive 
        {ok} ->
            R
    end.

stop() ->
    %io:format("Stoping Scheduler~n"),
    scheduler ! {stop}.

    
start() ->
    spawn_link(?MODULE, init,[self()]),
    receive
    {started, Pid} ->
        {ok, Pid}
    end.

init(Sup) ->
 
    register(scheduler,self()),
    Sup ! {started, self() },
    Q = message_queue:new(),
    scheduler_loop(Q,false,0,0).
    
%%
%% Local Functions
%%

-spec scheduler_loop(message_queue:message_queue(), boolean(), message_queue:msg_time(), non_neg_integer()) -> any().
scheduler_loop(Q,Once,AkkTime,Ins) ->
    receive 
        %{halt_simulation} ->
        %    MS = stop_loop([]),
        %    [ self() ! M || M <- MS],
        %    scheduler_loop(Q,Once,AkkTime,Ins);
        {stop} ->
                                  io:format("T: ~p ~p~n",[AkkTime,Ins]),
                                  halt(0);
         X -> 
            case X of
                {release_ok} ->
                    {Time,Target,Msg} = message_queue:get_next_message_to_schedule(Q),
                        NewAkkTime = Time,
                        %io:format("T: ~p  ~p ! ~p ~n",[NewAkkTime,Target,Msg]),
                        Target ! Msg,
                        scheduler_loop(message_queue:pop(Q),Once,NewAkkTime,Ins+1);                   
                {yield} ->
                    {Time,Target,Msg} = message_queue:get_next_message_to_schedule(Q),  
                        NewAkkTime = Time,
                        %io:format("~p  ~p ~p ~n",[NewAkkTime,Ins,length(Q)]),
                        Target ! Msg,
                        scheduler_loop(message_queue:pop(Q),Once,NewAkkTime,Ins+1);
                {{ss_mesg},{Delay, Pid, MSG},S} ->
                    %io:format("MSG ~p~n",[{{ss_mesg},{Delay, Pid, MSG}}]),
                    %io:format(" Q: ~p~n",[Q]),
                    Time = Delay + AkkTime,
                    S ! {ok}, 
                   scheduler_loop(message_queue:add(Q,{Time, Pid, MSG}),Once,AkkTime,Ins+1);
                _ -> io:format("No Handle for massage"),
                     halt(-1)
            end
    after 1000 ->
               case Once of
                    false ->
                        {Time,Target,Msg} = message_queue:get_next_message_to_schedule(Q),
                        NewAkkTime = Time,
                        %io:format("T: ~p  ~p ! ~p ~n",[NewAkkTime,Target,Msg]),
                        Target ! Msg,
                        scheduler_loop(message_queue:pop(Q),Once,NewAkkTime,Ins+1);
                    true ->
                         io:format("ERROR: Timeout on() takes over 1000ms~n"),
                         halt(-1)
               end
    end.                    
             
            
stop_loop(M) ->
    receive 
        {continue} -> 
                M;
        {{ss_mesg},{T, Pid, MSG},S} ->
                case T > 60 of
                    true ->
                        io:format("~p~n",[{T, Pid, MSG}]),
                         NewM =  M++[{{ss_mesg},{T, Pid, MSG},S}] ;
                    false ->
                        NewM = M,
                        Pid ! MSG,
                        S ! {ok}
                end,
                stop_loop(NewM);
       X ->
                io:format("~p~n",[X]),
                stop_loop(M++[X])
    end.


get_delay(_,_) ->
    randoms:rand_uniform(30, 60).



    