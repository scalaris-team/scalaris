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
%% @doc JSON API for transactional, consistent access to replicated DHT items.
%% @version $Id$
-module(api_json_tx).
-author('schintke@zib.de').
-vsn('$Id$').

-export([handler/2]).

% for the other JSON-RPC modules:
-export([value_to_json/1, json_to_value/1, 
         results_to_json/1, result_to_json/1]).

% for api_json:
-export([req_list/1, req_list/2,
         read/1, write/2, add_del_on_list/3, add_on_nr/2, test_and_set/3,
         req_list_commit_each/1]).

-ifdef(with_export_type_support).
-export_type([value/0, request/0,
              read_result/0, write_result/0, commit_result/0, result/0]).
-endif.

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(req_list, [Param])               -> req_list(Param);
handler(req_list, [TLog, ReqList])       -> req_list(TLog, ReqList);
handler(read, [Key])                     -> read(Key);
handler(write, [Key, Value])             -> write(Key, Value);
handler(add_del_on_list, [Key, ToAdd, ToRemove])
                                         -> add_del_on_list(Key, ToAdd, ToRemove);
handler(add_on_nr, [Key, ToAdd])         -> add_on_nr(Key, ToAdd);
handler(test_and_set, [Key, OldV, NewV]) -> test_and_set(Key, OldV, NewV);
handler(req_list_commit_each, [Param])   -> req_list_commit_each(Param);

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.


%% interface for api_tx calls
% Public Interface
-type value() :: {struct, [{type | string(), string()} | %% {"type", "as_is" | "as_bin"}
                           {value | string(), client_value()} %% {"value", ...}
                          ]}.

-type request() :: {string(), client_key()} % {"read", ...}
                 | {string(), {struct, [{client_key(), value()}]}} % {"write", {struct, [..., ...]}}
                 | {string(), any()}. % {"commit", _}

-type read_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()}     %% "timeout", "not_found"
                  | {value, value()} ]}.
-type write_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()} ]}. %% "timeout"
-type commit_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()}     %% "timeout"
                  | {keys, {array, [string()]}} ]}. %% "abort"

-type result() :: read_result() | write_result() | commit_result().

-spec req_list({array, [request()]})
                 -> {struct, [{tlog, string()}
                              | {results, {array, [result()]}}]}.
req_list(ReqList) ->
    JSON_TLog = tlog_to_json(api_tx:new_tlog()),
    req_list(JSON_TLog, ReqList).

-spec req_list(string(), {array, [request()]})
                 -> {struct, [{tlog, string()}
                              | {results, {array, [result()]}}]}.
req_list(JSON_TLog, JSON_ReqList) ->
    TLog = json_to_tlog(JSON_TLog),
    ReqList = json_to_reqlist(JSON_ReqList, true),
    {NewTLog, Res} = api_tx:req_list(TLog, ReqList),
    {struct, [{tlog, tlog_to_json(NewTLog)},
              {results, results_to_json(Res)}]}.

-spec read(client_key()) -> read_result().
read(Key) ->
    Res = api_tx:read(Key),
    result_to_json(Res).

-spec write(client_key(), value()) -> commit_result().
write(Key, Value) ->
    Res = api_tx:write(Key, json_to_value(Value)),
    result_to_json(Res).

-spec add_del_on_list(client_key(), ToAdd::{array, [client_value()]},
                         ToRemove::{array, [client_value()]})
                     -> commit_result() |
                        {struct, [{status, string()}  %% "fail"
                                 | {reason, string()} ]}. %% "not_a_list"
add_del_on_list(Key, {array, ToAdd}, {array, ToRemove}) ->
    Res = api_tx:add_del_on_list(Key, ToAdd, ToRemove),
    result_to_json(Res);
add_del_on_list(_Key, _ToAdd, _ToRemove) ->
    result_to_json({fail, not_a_list}).

-spec add_on_nr(client_key(), ToAdd::number())
                     -> commit_result() |
                        {struct, [{status, string()}  %% "fail"
                                 | {reason, string()} ]}. %% "not_a_number"
add_on_nr(Key, ToAdd) ->
    Res = api_tx:add_on_nr(Key, ToAdd),
    result_to_json(Res).

-spec test_and_set(client_key(),
                      OldValue::value(),
                      NewValue::value())
                     -> commit_result() |
                        {struct, [{status, string()}  %% "fail"
                                 | {reason, string()} %% "key_changed", "not_found"
                                 | {value, value()} ]}. %% for key_changed
test_and_set(Key, OldValue, NewValue) ->
    OldRealValue = json_to_value(OldValue),
    NewRealValue = json_to_value(NewValue),
    Res = api_tx:test_and_set(Key, OldRealValue, NewRealValue),
    result_to_json(Res).

-spec req_list_commit_each({array, [request()]}) -> {array, [result()]}.
req_list_commit_each(JSON_ReqList) ->
    ReqList = json_to_reqlist(JSON_ReqList, false),
    Res = api_tx:req_list_commit_each(ReqList),
    results_to_json(Res).

-spec results_to_json(Results::[api_tx:result()]) -> {array, [result()]}.
results_to_json(Results) ->
    Entries = [ result_to_json(Result) || Result <- Results ],
    {array, Entries}.

-spec result_to_json(Result::api_tx:result()) -> result().
result_to_json(Result) ->
    {struct,
     case Result of
         {ok}                       -> [{status, "ok"}];
         {ok, Val}                  -> [{status, "ok"},
                                        value_to_json(Val)];
         {fail, {key_changed, Val}} -> [{status, "fail"},
                                        {reason, "key_changed"},
                                        value_to_json(Val)];
         {fail, Reason}             -> [{status, "fail"},
                                        {reason, atom_to_list(Reason)}];
         {fail, abort, Keys}        -> [{status, "fail"},
                                        {reason, "abort"},
                                        {keys,   {array, Keys}}]
     end
    }.

-spec json_to_reqlist(JSON_ReqList::{array, [request()]}, AllowCommit::boolean()) -> [api_tx:request()].
json_to_reqlist({array, TmpReqList}, AllowCommit) ->
    [ case Elem of
          {"read", Key} ->
              {read, Key};
          {"write", {struct, [{Key, Val}]}} ->
              {write, Key, json_to_value(Val)};
          % note: struct properties sorted alphabetically
          {"add_del_on_list", {struct, [{"add", {array, ToAdd}}, {"del", {array, ToRemove}}, {"key", Key}]}} ->
              {add_del_on_list, Key, ToAdd, ToRemove};
          {"add_del_on_list", {struct, [{"add", ToAdd}, {"del", ToRemove}, {"key", Key}]}} ->
              {add_del_on_list, Key, ToAdd, ToRemove};
          {"add_on_nr", {struct, [{Key, ToAdd}]}} ->
              {add_on_nr, Key, ToAdd};
          {"test_and_set", {struct, [{"key", Key}, {"new", New}, {"old", Old}]}} ->
              {test_and_set, Key, Old, New};
          {"commit", _} when AllowCommit ->
              {commit}
      end || {struct, [Elem]} <- TmpReqList ].

-spec tlog_to_json(TLog::tx_tlog:tlog()) -> string().
tlog_to_json(TLog) ->
    base64:encode_to_string(term_to_binary(TLog, [compressed, {minor_version, 1}])).

-spec json_to_tlog(JsonTLog::string()) -> tx_tlog:tlog().
json_to_tlog(JsonTLog) ->
    binary_to_term(base64:decode(JsonTLog)).

-spec value_to_json(client_value()) -> {value, value()}.
value_to_json(Value) when is_binary(Value) ->
    {value, {struct, [{type, "as_bin"}, {value, base64:encode_to_string(Value)}]}};
value_to_json(Value) ->
    {value, {struct, [{type, "as_is"}, {value, Value}]}}.

-spec json_to_value(value()) -> client_value().
json_to_value({struct, [{"type", "as_bin"}, {"value", Value}]}) ->
    base64:decode(Value);
json_to_value({struct, [{"type", "as_is"}, {"value", {array, List}}]}) ->
    List;
json_to_value({struct, [{"type", "as_is"}, {"value", Value}]}) ->
    Value.
