%  Copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%            2009-2010 onScale solutions GmbH
%
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

%% Author: christian
%% Created: Feb 25, 2009
%% Description: TODO: Add description to fd_db
-module(fd_db).

%% Exported Functions
-export([add_subscription/3, del_subscription/3]).
-export([add_pinger/2, del_pinger/1]).
-export([get_subscribers/1, get_subscribers/2, get_subscriptions/1]).

-export([get_pinger/1, init/0]).

%% API Functions
init() ->
    ets:new(fd_ts_table, [duplicate_bag, protected, named_table]),
    ets:new(fd_st_table, [duplicate_bag, protected, named_table]),
    ets:new(fd_pinger_table, [set, protected, named_table]).

-spec(add_subscription/3 :: (pid(), cs_send:mypid(), any) -> true).
add_subscription(Subscriber, Target, Cookie) ->
    ets:insert(fd_st_table, {Subscriber, {Target, Cookie}}),
    ets:insert(fd_ts_table, {Target, {Subscriber, Cookie}}).

-spec(del_subscription/3 :: (pid(), cs_send:mypid(), any()) -> true).
del_subscription(Subscriber, Target, Cookie) ->
    ets:delete_object(fd_st_table,{Subscriber, {Target, Cookie}}),
    ets:delete_object(fd_ts_table,{Target, {Subscriber, Cookie}}).

-spec(add_pinger/2 :: (cs_send:mypid(),pid()) -> true).
add_pinger(Target, Pinger) ->
    ets:insert(fd_pinger_table, {Target,Pinger}).

-spec(get_subscribers/1 :: (cs_send:mypid()) -> list({pid(), any()})).
get_subscribers(Target) ->
    [ Result || {_,Result} <- ets:lookup(fd_ts_table, Target)].

-spec(get_subscribers/2 :: (cs_send:mypid(), any) -> list({pid(), any()})).
get_subscribers(Target, Cookie) ->
    [ Result || {_,{_, XCookie} = Result} <- ets:lookup(fd_ts_table, Target),
               XCookie =:= Cookie].

-spec(get_subscriptions/1 :: (pid()) -> list(cs_send:mypid())).
get_subscriptions (Subscriber) ->
    [ Result || {_,Result} <- ets:lookup(fd_st_table, Subscriber)].

-spec(get_pinger/1 :: (cs_send:mypid()) -> (none | {ok,pid()} )).
get_pinger(Target) ->
    case ets:lookup(fd_pinger_table, Target) of
        [] ->
            none;
        [{_,Pinger}] ->
            {ok,Pinger}
    end.

-spec(del_pinger/1 :: (cs_send:mypid()) -> true).
del_pinger(Target) ->
    ets:delete(fd_pinger_table, Target).

