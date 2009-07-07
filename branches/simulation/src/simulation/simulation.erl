%% Author: christian
%% Created: Jun 11, 2009
%% Description: TODO: Add description to simulator
-module(simulation).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([start/0]).



%%
%% API Functions
%%

start() ->
    randoms:start_seed({1,1,3}),
    scheduler:start(),
    simu_slave:start(),
    application:start(boot_cs).
   
    

%% run_1() ->
%%     Size = list_to_integer(os:getenv("RING_SIZE")),
%%     io:format("Do ~p~n",[Size]),
%%     Start = erlang:now(),
%%     admin:add_nodes(Size),
%%     %M = lists:flatten(io_lib:format("/work/bzchenni/tman_run_1_Extend_~p.log", [Size])),
%%     %{ok,F} = file:open(M,[write]),
%% 
%%     Ende = erlang:now(),
%%     N = "/work/bzchenni/tman_run_1.log",
%%     {ok,S} = file:open(N,[append]),
%%     io:format(S,"~p ~p~n",[Size,time_diff(Start,Ende)]),
%%     io:format("~p ~p~n",[Size,time_diff(Start,Ende)]),
%%     halt(1).
%% 
%% time_diff({SMe,SSe,SMi},{EMe,ESe,EMi}) ->
%%     (EMe*1000000+ESe+EMi/1000000)-(SMe*1000000+SSe+SMi/1000000).
