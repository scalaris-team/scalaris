%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%             2009-2010 onScale solutions GmbH
%  @end
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
%%%-------------------------------------------------------------------
%%% File    fd.erl
%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    Database operations for the failure detector.
%%% @end
%%%
%%% Created :  25 Feb 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(fd_db).
-author('hennig@zib.de').
-vsn('$Id$ ').

-include("scalaris.hrl").

%% Exported Functions
-export([add_subscription/3, del_subscription/3]).
-export([add_pinger/2, del_pinger/1]).
-export([get_subscribers/1, get_subscribers/2, get_subscriptions/1]).

-export([get_pinger/1, init/0]).

%% API Functions
-spec init() -> ok.
init() ->
    ets:new(fd_ts_table, [duplicate_bag, protected, named_table]),
    ets:new(fd_st_table, [duplicate_bag, protected, named_table]),
    ets:new(fd_pinger_table, [set, protected, named_table]),
    ok.

-spec add_subscription(Subscriber::pid(), Target::cs_send:mypid(), Cookie::fd:cookie()) -> true.
add_subscription(Subscriber, Target, Cookie) ->
    ets:insert(fd_st_table, {Subscriber, {Target, Cookie}}),
    ets:insert(fd_ts_table, {Target, {Subscriber, Cookie}}).

-spec del_subscription(Subscriber::pid(), Target::cs_send:mypid(), Cookie::fd:cookie()) -> true.
del_subscription(Subscriber, Target, Cookie) ->
    ets:delete_object(fd_st_table,{Subscriber, {Target, Cookie}}),
    ets:delete_object(fd_ts_table,{Target, {Subscriber, Cookie}}).

-spec get_subscribers(Target::cs_send:mypid()) -> [{pid(), fd:cookie()}].
get_subscribers(Target) ->
    [ Result || {_, Result} <- ets:lookup(fd_ts_table, Target)].

-spec get_subscribers(Target::cs_send:mypid(), Cookie::fd:cookie()) -> [{pid(), fd:cookie()}].
get_subscribers(Target, Cookie) ->
    [ Result || {_, {_Pid, XCookie} = Result} <- ets:lookup(fd_ts_table, Target),
                XCookie =:= Cookie].

-spec get_subscriptions(Subscriber::pid()) -> [{Target::cs_send:mypid(), Cookie::fd:cookie()}].
get_subscriptions(Subscriber) ->
    [ Result || {_, Result} <- ets:lookup(fd_st_table, Subscriber)].

-spec add_pinger(Target::cs_send:mypid(), Pinger::pid()) -> true.
add_pinger(Target, Pinger) ->
    ets:insert(fd_pinger_table, {Target, Pinger}).

-spec get_pinger(Target::cs_send:mypid()) -> none | {ok,pid()}.
get_pinger(Target) ->
    case ets:lookup(fd_pinger_table, Target) of
        [] ->
            none;
        [{_, Pinger}] ->
            {ok, Pinger}
    end.

-spec del_pinger(Target::cs_send:mypid()) -> true.
del_pinger(Target) ->
    ets:delete(fd_pinger_table, Target).
