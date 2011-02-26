%  @copyright 2007-2011 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Publish/Subscribe API functions
%% @end
%% @version $Id$
-module(api_pubsub).
-author('schuett@zib.de').
-vsn('$Id$').

-export([publish/2, subscribe/2, unsubscribe/2, get_subscribers/1]).

%% @doc publishs an event under a given topic.
%%      called e.g. from the java-interface
-spec publish(string(), string()) -> ok.
publish(Topic, Content) ->
    Subscribers = get_subscribers(Topic),
    [ pubsub_publish:publish(X, Topic, Content) || X <- Subscribers ],
    ok.

%% @doc subscribes a url for a topic.
%%      called e.g. from the java-interface
-spec subscribe(string(), string()) -> ok | {fail, Reason::term()}.
subscribe(Topic, URL) ->
    {TLog, [Res]} = api_tx:req_list(api_tx:new_tlog(), [{read, Topic}]),
    {_TLog2, [_, CommitRes]} =
        case Res of
            {fail, not_found} -> api_tx:req_list(TLog, [{write, Topic, [URL]}, {commit}]);
            {ok, URLs}        -> api_tx:req_list(TLog, [{write, Topic, [URL | URLs]}, {commit}]);
            {fail, timeout}   -> {TLog, [nothing, {fail, timeout}]}
        end,
    case CommitRes of {ok} -> ok; _ -> CommitRes end.

%% @doc unsubscribes a url for a topic.
-spec unsubscribe(string(), string()) -> ok | {fail, any()}.
unsubscribe(Topic, URL) ->
    {TLog, [Res]} = api_tx:req_list(api_tx:new_tlog(), [{read, Topic}]),
    case Res of
        {ok, URLs} ->
            case lists:member(URL, URLs) of
                true ->
                    {_TLog2, [_, CommitRes]} =
                        api_tx:req_list(TLog, [{write, Topic, lists:delete(URL, URLs)}, {commit}]),
                    case CommitRes of {ok} -> ok; _ -> CommitRes end;
                false -> {fail, not_found}
            end;
        _ -> Res
    end.

%% @doc queries the subscribers of a query
%% @spec get_subscribers(string()) -> [string()]
-spec get_subscribers(Topic::string()) -> [string()].
get_subscribers(Topic) ->
    {Res, Value} = api_tx:read(Topic),
    case Res of
        ok -> Value;
        fail -> []
    end.
