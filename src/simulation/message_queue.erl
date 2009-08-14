%% Author: christian
%% Created: Jun 30, 2009
%% Description: TODO: Add description to message_queue
-module(message_queue).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([new/0,add/2,get_next_message_to_schedule/1, pop/1,get_time/1]).

%%
%% API Functions
%%
new() ->
    [].

add([],{Time,Target,Msg}) ->
    [{Time,Target,Msg}];
add([{Time,Target,Msg}|T],{Ti,Ta,Ms}) ->
    case   Ti < Time of
        true ->
            [{Ti,Ta,Ms}]++[{Time,Target,Msg}|T];
        false ->
            [{Time,Target,Msg}|add(T,{Ti,Ta,Ms})]
    end.    

pop([]) ->
    [];
pop([_H|T]) ->
    %timer:sleep(1000),
    T.

get_time([]) ->
    0;
get_time([{Time,_Target,_Msg}|_H]) ->
    Time.

get_next_message_to_schedule([]) ->
    io:format("Mhhh fertig, nix mehr zu tun mmmmh  dürfte eigendlich nicht passieren"),
    null;
get_next_message_to_schedule([{Time,Target,Msg}|_H]) ->
    {Time,Target,Msg}.

%%
%% Local Functions
%%

