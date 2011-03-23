%% @copyright 2011 Zuse Institute Berlin

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
%% @doc JSON API for transactional, consistent access to replicated DHT items
%% @version $Id$
-module(api_json).
-author('schintke@zib.de').
-vsn('$Id$').

-export([tx_req_list/1, tx_req_list/2, tx_read/1, tx_write/2,
         tx_test_and_set/3]).

-export([pubsub_publish/2, pubsub_subscribe/2, pubsub_unsubscribe/2,
         pubsub_get_subscribers/1]).

-export([rdht_delete/1, rdht_delete/2]).

-export([dht_raw_range_read/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% interface for api_tx calls
% Public Interface
-type json_value() :: {struct, [{type, string()} %% "as_is", "as_bin"
                                | {value, client_value()}]}.

-type request() :: {read, client_key()}
                 | {write, client_key(), json_value()}
                 | {commit}.

-type read_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()}     %% "timeout", "not_found"
                  | {value, json_value()} ]}.
-type write_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()} ]}. %% "timeout"
-type commit_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()} ]}. %% "abort", "timeout"

-type result() :: read_result() | write_result() | commit_result().

-spec tx_req_list({array, [request()]})
                 -> {struct, [{tlog, string()}
                              | {results, {array, [result()]}}]}.
tx_req_list(ReqList) ->
    JSON_TLog = tlog_to_json(api_tx:new_tlog()),
    tx_req_list(JSON_TLog, ReqList).

-spec tx_req_list(string(), {array, [request()]})
                 -> {struct, [{tlog, string()}
                              | {results, {array, [result()]}}]}.
tx_req_list(JSON_TLog, JSON_ReqList) ->
    TLog = json_to_tlog(JSON_TLog),
    ReqList = json_to_reqlist(JSON_ReqList),
    {NewTLog, Res} = api_tx:req_list(TLog, ReqList),
    {struct, [{tlog, tlog_to_json(NewTLog)},
              {results, results_to_json(Res)}]}.

-spec tx_read(client_key()) -> read_result().
tx_read(Key) ->
    Res = api_tx:read(Key),
    result_to_json(Res).

-spec tx_write(client_key(),
               json_value()) -> commit_result().
tx_write(Key, Value) ->
    Res = api_tx:write(Key, json_to_value(Value)),
    result_to_json(Res).

-spec tx_test_and_set(client_key(),
                      OldValue::json_value(),
                      NewValue::json_value())
                     -> commit_result() |
                        {struct, [{status, string()}  %% "fail"
                                 | {reason, string()} %% "key_changed", "not_found"
                                 | {value, json_value()} ]}.
tx_test_and_set(Key, OldValue, NewValue) ->
    OldRealValue = json_to_value(OldValue),
    NewRealValue = json_to_value(NewValue),
    Res = api_tx:test_and_set(Key, OldRealValue, NewRealValue),
    result_to_json(Res).

results_to_json(Results) ->
    Entries = [ result_to_json(Result) || Result <- Results ],
    {array, Entries}.

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
                                        {reason, atom_to_list(Reason)}]
     end
    }.

json_to_reqlist(JSON_ReqList) ->
    {array, TmpReqList} = JSON_ReqList,
    [ case Elem of
          {read, Key}                     -> {read, Key};
          {write, {struct, [{Key, Val}]}} -> {write, atom_to_list(Key), json_to_value(Val)};
          {commit, _}                     -> {commit};
          Any                             -> Any
      end || {struct, [Elem]} <- TmpReqList ].

tlog_to_json(TLog) ->
    base64:encode_to_string(term_to_binary(TLog, [compressed, {minor_version, 1}])).

json_to_tlog(JsonTLog) ->
    binary_to_term(base64:decode(JsonTLog)).

value_to_json(Value) when is_binary(Value) ->
    {value, {struct, [{type, "as_bin"}, {value, base64:encode_to_string(Value)}]}};
value_to_json(Value) ->
    {value, {struct, [{type, "as_is"}, {value, Value}]}}.

json_to_value({struct, [{type, "as_bin"}, {value, Value}]}) ->
    base64:decode(Value);
json_to_value({struct, [{type, "as_is"}, {value, {array, List}}]}) ->
    List;
json_to_value({struct, [{type, "as_is"}, {value, Value}]}) ->
    Value.

%% interface for api_pubsub calls
-spec pubsub_publish(string(), string()) -> {struct, [{status, string()}]}. %status: "ok"
pubsub_publish(Topic, Content) ->
    Res = api_pubsub:publish(Topic, json_to_value(Content)),
    result_to_json(Res).

-spec pubsub_subscribe(string(), string()) -> commit_result().
pubsub_subscribe(Topic, URL) ->
    Res = api_pubsub:subscribe(Topic, URL),
    result_to_json(Res).

-spec pubsub_unsubscribe(string(), string()) -> commit_result(). %note: reason may also be "not_found"
pubsub_unsubscribe(Topic, URL) ->
    Res = api_pubsub:unsubscribe(Topic, URL),
    result_to_json(Res).

-spec pubsub_get_subscribers(string()) -> {array, [string()]}.
pubsub_get_subscribers(Topic) ->
    case api_pubsub:get_subscribers(Topic) of
        [] -> {array, []};
        Any -> {array, Any}
    end.

%% interface for api_rdht calls
-spec rdht_delete(client_key()) -> {struct, [{failure, string()}
                                         | {ok, non_neg_integer()}
                                         | {results, {array, [string()]}}]}.
rdht_delete(Key) ->
    rdht_delete(Key, 2000).

-spec rdht_delete(client_key(), Timeout::pos_integer())
        -> {struct, [{failure, string()} |
                     {ok, non_neg_integer()} |
                     {results, {array, [string()]}}]}.
rdht_delete(Key, Timeout) ->
    case api_rdht:delete(Key, Timeout) of
        {fail, Reason, NumOK, StateList} ->
            {struct, [{failure, atom_to_list(Reason)},
                      {ok, NumOK},
                      {results, {array, [atom_to_list(X) || X <- StateList]}}]};
        {ok, NumOk, StateList} ->
            {struct, [{ok, NumOk},
                      {results, {array, [atom_to_list(X) || X <- StateList ]}}]}
    end.

%% interface for api_dht_raw calls
-spec dht_raw_range_read(intervals:key(), intervals:key()) -> result().
dht_raw_range_read(From, To) ->
    {ErrorCode, Data} = api_dht_raw:range_read(From, To),
    {struct, [{status, atom_to_list(ErrorCode)}, {value, data_to_json(Data)}]}.

data_to_json(Data) ->
    {array, [ {struct, [{key, Key},
                        value_to_json(Value),
                        {version, Version}]} ||
              {Key, Value, _WriteLock, _ReadLock, Version} <- Data]}.
