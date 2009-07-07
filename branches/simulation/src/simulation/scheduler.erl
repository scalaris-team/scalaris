%% Author: christian
%% Created: Jun 29, 2009
%% Description: TODO: Add description to scheduler
-module(scheduler).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([send/3, send/2,start/0,init/1]).

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


    
start() ->
    spawn_link(?MODULE, init,[self()]),
    receive
    {started, Pid} ->
        {ok, Pid}
    end.

init(Sup) ->
    random:seed(9,9,9),
    register(scheduler,self()),
    Sup ! {started, self() },
    Q = message_queue:new(),
    scheduler_loop(Q,false,0,0).
    
%%
%% Local Functions
%%

scheduler_loop(Q,Once,AkkTime,Ins) ->
    receive 
        X -> 
            case X of
                {release_ok} ->
                    {Time,Target,Msg} = message_queue:get_next_message_to_schedule(Q),
                        NewAkkTime = Time,
                        %io:format("T: ~p  ~p ! ~p ~n",[NewAkkTime,Target,Msg]),
                        Target ! Msg,
                        scheduler_loop(message_queue:pop(Q),Once,NewAkkTime,Ins+1);                   
                {unlock} ->
                    {Time,Target,Msg} = message_queue:get_next_message_to_schedule(Q),  
                        NewAkkTime = Time,
                        %io:format("T: ~p  ~p ! ~p ~n",[NewAkkTime,Target,Msg]),
                        case Msg of 
                             {simu_stop} ->
                                  io:format("T: ~p ~p~n",[Time,Ins]);
                             _ -> ok
                        end,
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
             
            

get_delay(_,_) ->
    randoms:rand_uniform(30, 60).



    