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

-export([get_ring_size/4, wait_for_ring_size/5, run_benchmark/3]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec get_ring_size(TimeOut::integer(), IP::{non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}, Port::integer(), SSL::boolean()) -> integer().
get_ring_size(TimeOut, _IP = {A,B,C,D}, Port, SSL) ->
    TheIP = lists:flatten(io_lib:format("~w.~w.~w.~w", [A,B,C,D])),
    doJsonRPC(TheIP, Port, "jsonrpc.yaws", "get_ring_size", [TimeOut], SSL).

-spec wait_for_ring_size(Size::integer(), TimeOut::integer(), IP::{non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}, Port::integer(), SSL::boolean()) -> string().
wait_for_ring_size(Size, TimeOut, _IP = {A,B,C,D}, Port, SSL) ->
    TheIP = lists:flatten(io_lib:format("~w.~w.~w.~w", [A,B,C,D])),
    doJsonRPC(TheIP, Port, "jsonrpc.yaws", "wait_for_ring_size", [Size, TimeOut], SSL).

-spec run_benchmark(IP::{non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}, Port::integer(), SSL::boolean()) -> ok.
run_benchmark(_IP = {A,B,C,D}, Port, SSL) ->
    TheIP = lists:flatten(io_lib:format("~w.~w.~w.~w", [A,B,C,D])),
    io:format("running bench:increment(10, 500)...~n"),
    Incr = doJsonRPC(TheIP, Port, "jsonrpc.yaws", "run_benchmark_incr", [], SSL),
    ResultIncr = bench_json_helper:json_to_result(Incr),
    bench:print_results(ResultIncr, [print, verbose]),
    io:format("running bench:quorum_read(10, 5000)...~n"),
    Read = doJsonRPC(TheIP, Port, "jsonrpc.yaws", "run_benchmark_read", [], SSL),
    ResultRead = bench_json_helper:json_to_result(Read),
    bench:print_results(ResultRead, [print, verbose]),
    ok.

-spec doJsonRPC(IP::string(), Port::integer(), Path::string(), Call::string(), Params::list(), SSL::boolean()) -> term().
doJsonRPC(IP, Port, Path, Call, Params, SSL) ->
    _ = ssl:start(), %% just in case.  ok | {error, Reason}
    ContentType = "application/json",
    Json = {struct, [{jsonrpc, "2.0"}, {method, Call}, {params, {array, Params}}, {id, 1}]},
    Body = lists:flatten(json2:encode(Json)),
    Headers = [{"User-Agent", "Wget/1.19.4 (darwin17.3.0)"},
               {"Accept", "*/*"},
               {"Accept-Encoding", "identity"},
               {"Connection", "Keep-Alive"},
               {"Content-Type", ContentType},
               {"Content-Length", length(Body)}],
    Request = { get_url_prefix(SSL) ++ IP ++ ":" ++ integer_to_list(Port) ++ "/" ++ Path, Headers,
                ContentType, Body},
    io:format("~s~n", [get_url_prefix(SSL) ++ IP ++ ":" ++ integer_to_list(Port) ++ "/" ++ Path]),
    HTTPOptions = [{version, "HTTP/1.1"}],
    Options = [{body_format, string}],
    Result = httpc:request(post, Request, HTTPOptions, Options),
    case Result of
        {ok, {_StatusLine, _Headers2, Body2}} ->
            JsonResponse = json2:decode_string(trim_new_lines(Body2)), % cheap string:trim()
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% util
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
trim_new_lines(S) ->
    trim_new_lines_internal(lists:reverse(S)).

trim_new_lines_internal([]) ->
    [];
trim_new_lines_internal(S = [First | Rest]) ->
    case First of
        [$\r,$\n] -> trim_new_lines_internal(Rest);
        $\n ->trim_new_lines_internal(Rest);
        _ -> lists:reverse(S)
    end.

get_url_prefix(_SSL = true) ->
    "https://";
get_url_prefix(_SSL = false) ->
    "http://".
