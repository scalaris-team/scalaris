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
%% @doc JSON API for using rbr.
-module(api_json_rbr).
-author('schuett@zib.de').

-export([handler/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

% for api_json:
-export([read/1, write/2]).

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(read, [Key])                     -> read(Key);
handler(write, [Key, Value])             -> write(Key, Value);

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

-spec read(client_key()) -> read_result().
read(Key) ->
    %% api_tx:read_result()
    Result = kv_on_cseq:read(Key),
    Json = result_to_json(Result),
    io:format("~w~n", [Json]),
    Json.

-spec write(client_key(), term()) -> write_result().
write(Key, Value) ->
    Result = kv_on_cseq:write(Key, Value),
    result_to_json(Result).

%% TODO copied from api_json_tx

-type value() :: {struct, [{type | string(), string()} | %% {"type", "as_is" | "as_bin"}
                           {value | string(), client_value()} %% {"value", ...}
                          ]}.
-type read_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()}     %% "timeout", "not_found"
                  | {value, value()} ]}.
-type write_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()} ]}. %% "timeout"

-type result() :: read_result() | write_result().

-spec result_to_json(Result::api_tx:result()) -> result().
result_to_json(Result) ->
    {struct,
     case Result of
         {ok}                       -> [{status, "ok"}];
         {ok, Val}                  -> [{status, "ok"},
                                        value_to_json(Val)];
         {fail, Reason}             -> [{status, "fail"},
                                        {reason, atom_to_list(Reason)}]
     end
    }.

-spec value_to_json(client_value()) -> {value, value()}.
value_to_json(Value) when is_binary(Value) ->
    {value, {struct, [{type, "as_bin"}, {value, base64:encode_to_string(Value)}]}};
value_to_json(Value) ->
    {value, {struct, [{type, "as_is"}, {value, Value}]}}.

