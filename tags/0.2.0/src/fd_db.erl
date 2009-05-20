%% Author: christian
%% Created: Feb 25, 2009
%% Description: TODO: Add description to fd_db
-module(fd_db).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([add_subscription/2, del_subscription/2,
         add_pinger/2, del_pinger/1,
         get_subscribers/1, get_subscriptions/1, get_pinger/1,
         init/0
        ]).

%%
%% API Functions
%%
init() ->
  	ets:new(ts_table, [bag, protected, named_table]),	    
	ets:new(st_table, [bag, protected, named_table]),
    ets:new(pinger_table, [set, protected, named_table]).

-spec(add_subscription/2 :: (pid(),cs_send:mypid()) -> true).
add_subscription(Subscriber,Target) ->
    ets:insert(st_table, {Subscriber, Target}),
	ets:insert(ts_table, {Target, Subscriber}).

-spec(del_subscription/2 :: (pid(),cs_send:mypid()) -> true).
del_subscription(Subscriber,Target) ->
    ets:delete_object(st_table,{Subscriber, Target}),
    ets:delete_object(ts_table,{Target,Subscriber}).

-spec(add_pinger/2 :: (cs_send:mypid(),pid()) -> true).
add_pinger(Target,Pinger) ->
    ets:insert(pinger_table, {Target,Pinger}).

-spec(get_subscribers/1 :: (cs_send:mypid()) -> list(pid())).
get_subscribers(Target) -> 
    [ Result || {_,Result} <- ets:lookup(ts_table, Target)].

-spec(get_subscriptions/1 :: (pid()) -> list(cs_send:mypid())).
get_subscriptions (Subscriber) ->
    [ Result || {_,Result} <- ets:lookup(st_table, Subscriber)].

-spec(get_pinger/1 :: (cs_send:mypid()) -> (none | {ok,pid()} )).
get_pinger(Target) ->
    case ets:lookup(pinger_table, Target) of
    	[] ->
            none;
        [{_,Pinger}] -> 
			{ok,Pinger}
    end.
    

-spec(del_pinger/1 :: (cs_send:mypid()) -> true).
del_pinger(Target) ->
	ets:delete(pinger_table, Target).
    


%%
%% Local Functions
%%

