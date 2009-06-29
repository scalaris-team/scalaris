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
-export([send/3]).

%%
%% API Functions
%%

send(0,Pid,MSG) ->
    Pid ! MSG;
send(Delay,Pid,MSG) ->
    erlang:send_after(Delay, Pid, MSG).
    

%%
%% Local Functions
%%

