%  Copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%            2009 onScale solutions GmbH
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
    ets:new(ts_table, [bag, protected, named_table]),
    ets:new(st_table, [bag, protected, named_table]),
    ets:new(pinger_table, [set, protected, named_table]).

-spec(add_subscription/3 :: (pid(), cs_send:mypid(), any) -> true).
add_subscription(Subscriber, Target, Cookie) ->
    ets:insert(st_table, {Subscriber, {Target, Cookie}}),
    ets:insert(ts_table, {Target, {Subscriber, Cookie}}).


-spec(del_subscription/3 :: (pid(), cs_send:mypid(), any()) -> true).
del_subscription(Subscriber, Target, Cookie) ->
    ets:delete_object(st_table,{Subscriber, {Target, Cookie}}),
    ets:delete_object(ts_table,{Target, {Subscriber, Cookie}}).

-spec(add_pinger/2 :: (cs_send:mypid(),pid()) -> true).
add_pinger(Target, Pinger) ->
    ets:insert(pinger_table, {Target,Pinger}).

-spec(get_subscribers/1 :: (cs_send:mypid()) -> list({pid(), any()})).
get_subscribers(Target) ->
    [ Result || {_,Result} <- ets:lookup(ts_table, Target)].

-spec(get_subscribers/2 :: (cs_send:mypid(), any) -> list({pid(), any()})).
get_subscribers(Target, Cookie) ->
    [ Result || {_,{_, XCookie} = Result} <- ets:lookup(ts_table, Target),
               XCookie =:= Cookie].

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

