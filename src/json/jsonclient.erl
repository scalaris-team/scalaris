%% @copyright 2018 Zuse Institute Berlin

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
%% @doc JSON Client.
-module(jsonclient).
-author('schuett@zib.de').

% for api_json:
-export([get_ring_size/3, wait_for_ring_size/4, doJsonRPC/5]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec get_ring_size(TimeOut::integer(), IP::string(), Port::integer()) -> integer().
get_ring_size(TimeOut, IP, Port) ->
    doJsonRPC(IP, Port, "jsonrpc.yaws", "get_ring_size", [TimeOut]).

-spec wait_for_ring_size(Size::integer(), TimeOut::integer(), IP::string(), Port::integer()) -> string().
wait_for_ring_size(Size, TimeOut, IP, Port) ->
    doJsonRPC(IP, Port, "jsonrpc.yaws", "wait_for_ring_size", [Size, TimeOut]).


-spec doJsonRPC(IP::string(), Port::integer(), Path::string(), Call::string(), Params::list()) -> term().
doJsonRPC(IP, Port, Path, Call, Params) ->
    ContentType = "application/json",
    Json = {struct, [{jsonrpc, "2.0"}, {method, Call}, {params, {array, Params}}, {id, 1}]},
    Body = lists:flatten(json2:encode(Json)),
    Headers = [{"User-Agent", "Wget/1.19.4 (darwin17.3.0)"},
               {"Accept", "*/*"},
               {"Accept-Encoding", "identity"},
               {"Connection", "Keep-Alive"},
               {"Content-Type", ContentType},
               {"Content-Length", length(Body)}],
    Request = { "http://" ++ IP ++ ":" ++ integer_to_list(Port) ++ "/" ++ Path, Headers,
                ContentType, Body},
    HTTPOptions = [{version, "HTTP/1.1"}],
    Options = [{body_format, string}],
    Result = httpc:request(post, Request, HTTPOptions, Options),
    case Result of
        {ok, {_StatusLine, _Headers2, Body2}} ->
            JsonResponse = json2:decode_string(string:trim(Body2)),
            case JsonResponse of
                {ok, {struct, List}} ->
                    case lists:keyfind("result", 1, List) of
                        {"result", TheResult} ->
                            TheResult;
                        false ->
                            failed
                    end;
                X ->
                    io:format("~w~n", [X])
            end;
        {error, Reason} ->
            {error, Reason}
    end.
