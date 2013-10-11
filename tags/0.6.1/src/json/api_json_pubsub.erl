%% @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc JSON API for accessing the Pub/Sub system.
%% @version $Id$
-module(api_json_pubsub).
-author('schintke@zib.de').
-vsn('$Id$').

-export([handler/2]).

% for api_json:
-export([publish/2, subscribe/2, unsubscribe/2, get_subscribers/1]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(publish, [Topic, Content])       -> publish(Topic, Content);
handler(subscribe, [Topic, URL])         -> subscribe(Topic, URL);
handler(unsubscribe, [Topic, URL])       -> unsubscribe(Topic, URL);
handler(get_subscribers, [Topic])        -> get_subscribers(Topic);

handler(notify, [Topic, Value]) ->
    io:format("Got pubsub notify ~p -> ~p~n", [Topic, Value]),
    "ok";

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

%% interface for api_pubsub calls
-spec publish(string(), string()) -> {struct, [{status, string()}]}. %status: "ok"
publish(Topic, Content) ->
    Res = api_pubsub:publish(Topic, Content),
    api_json_tx:result_to_json(Res).

-spec subscribe(string(), string()) -> api_json_tx:commit_result().
subscribe(Topic, URL) ->
    Res = api_pubsub:subscribe(Topic, URL),
    api_json_tx:result_to_json(Res).

-spec unsubscribe(string(), string()) -> api_json_tx:commit_result(). %note: reason may also be "not_found"
unsubscribe(Topic, URL) ->
    Res = api_pubsub:unsubscribe(Topic, URL),
    api_json_tx:result_to_json(Res).

-spec get_subscribers(string()) -> {array, [string()]}.
get_subscribers(Topic) ->
    case api_pubsub:get_subscribers(Topic) of
        [] -> {array, []};
        Any -> {array, Any}
    end.
