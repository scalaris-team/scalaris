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

-export([handler/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(range_read, [From, To])          -> dht_raw_range_read(From, To);
handler(delete, [Key])                   -> rdht_delete(Key);
handler(delete, [Key, Timeout])          -> rdht_delete(Key, Timeout);
handler(req_list, [Param])               -> tx_req_list(Param);
handler(req_list, [TLog, ReqList])       -> tx_req_list(TLog, ReqList);
handler(read, [Key])                     -> tx_read(Key);
handler(write, [Key, Value])             -> tx_write(Key, Value);
handler(add_del_on_list, [Key, ToAdd, ToRemove])
                                         -> tx_add_del_on_list(Key, ToAdd, ToRemove);
handler(add_on_nr, [Key, ToAdd])         -> tx_add_on_nr(Key, ToAdd);
handler(test_and_set, [Key, OldV, NewV]) -> tx_test_and_set(Key, OldV, NewV);
handler(req_list_commit_each, [Param])   -> tx_req_list_commit_each(Param);

handler(publish, [Topic, Content])       -> pubsub_publish(Topic, Content);
handler(subscribe, [Topic, URL])         -> pubsub_subscribe(Topic, URL);
handler(unsubscribe, [Topic, URL])       -> pubsub_unsubscribe(Topic, URL);
handler(get_subscribers, [Topic])        -> pubsub_get_subscribers(Topic);

handler(get_node_info, [])               -> get_node_info();
handler(get_node_performance, [])        -> get_node_performance();
handler(get_service_info, [])            -> get_service_info();
handler(get_service_performance, [])     -> get_service_performance();

handler(notify, [Topic, Value]) ->
    io:format("Got pubsub notify ~p -> ~p~n", [Topic, Value]),
    "ok";

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~p(~p)~n", [AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.


%% interface for api_tx calls
% Public Interface
-type json_value() :: {struct, [{type | string(), string()} %% {"type", "as_is" | "as_bin"}
                                | {value | string(), client_value()} %% {"value", ...}
                               ]}.

-type request() :: {string(), client_key()} % {"read", ...}
                 | {string(), {struct, [{client_key(), json_value()}]}} % {"write", {struct, [..., ...]}}
                 | {string(), any()}. % {"commit", _}

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
    ReqList = json_to_reqlist(JSON_ReqList, true),
    {NewTLog, Res} = api_tx:req_list(TLog, ReqList),
    {struct, [{tlog, tlog_to_json(NewTLog)},
              {results, results_to_json(Res)}]}.

-spec tx_read(client_key()) -> read_result().
tx_read(Key) ->
    Res = api_tx:read(Key),
    result_to_json(Res).

-spec tx_write(client_key(), json_value()) -> commit_result().
tx_write(Key, Value) ->
    Res = api_tx:write(Key, json_to_value(Value)),
    result_to_json(Res).

-spec tx_add_del_on_list(client_key(), ToAdd::{array, [client_value()]},
                         ToRemove::{array, [client_value()]})
                     -> commit_result() |
                        {struct, [{status, string()}  %% "fail"
                                 | {reason, string()} ]}. %% "not_a_list"
tx_add_del_on_list(Key, {array, ToAdd}, {array, ToRemove}) ->
    Res = api_tx:add_del_on_list(Key, ToAdd, ToRemove),
    result_to_json(Res);
tx_add_del_on_list(_Key, _ToAdd, _ToRemove) ->
    result_to_json({fail, not_a_list}).

-spec tx_add_on_nr(client_key(), ToAdd::number())
                     -> commit_result() |
                        {struct, [{status, string()}  %% "fail"
                                 | {reason, string()} ]}. %% "not_a_number"
tx_add_on_nr(Key, ToAdd) ->
    Res = api_tx:add_on_nr(Key, ToAdd),
    result_to_json(Res).

-spec tx_test_and_set(client_key(),
                      OldValue::json_value(),
                      NewValue::json_value())
                     -> commit_result() |
                        {struct, [{status, string()}  %% "fail"
                                 | {reason, string()} %% "key_changed", "not_found"
                                 | {value, json_value()} ]}. %% for key_changed
tx_test_and_set(Key, OldValue, NewValue) ->
    OldRealValue = json_to_value(OldValue),
    NewRealValue = json_to_value(NewValue),
    Res = api_tx:test_and_set(Key, OldRealValue, NewRealValue),
    result_to_json(Res).

-spec tx_req_list_commit_each({array, [request()]}) -> {array, [result()]}.
tx_req_list_commit_each(JSON_ReqList) ->
    ReqList = json_to_reqlist(JSON_ReqList, false),
    Res = api_tx:req_list_commit_each(ReqList),
    results_to_json(Res).

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

-spec value_to_json(client_value()) -> {value, json_value()}.
value_to_json(Value) when is_binary(Value) ->
    {value, {struct, [{type, "as_bin"}, {value, base64:encode_to_string(Value)}]}};
value_to_json(Value) ->
    {value, {struct, [{type, "as_is"}, {value, Value}]}}.

-spec json_to_value(json_value()) -> client_value().
json_to_value({struct, [{"type", "as_bin"}, {"value", Value}]}) ->
    base64:decode(Value);
json_to_value({struct, [{"type", "as_is"}, {"value", {array, List}}]}) ->
    List;
json_to_value({struct, [{"type", "as_is"}, {"value", Value}]}) ->
    Value.

%% interface for api_pubsub calls
-spec pubsub_publish(string(), string()) -> {struct, [{status, string()}]}. %status: "ok"
pubsub_publish(Topic, Content) ->
    Res = api_pubsub:publish(Topic, Content),
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

-spec data_to_json(Data::[db_entry:entry()]) ->
            {array, [{struct, [{key, ?RT:key()} | 
                               {value, json_value()} |
                               {version, ?DB:version()}]
                     }]}.
data_to_json(Data) ->
    {array, [ {struct, [{key, db_entry:get_key(DBEntry)},
                        value_to_json(db_entry:get_value(DBEntry)),
                        {version, db_entry:get_version(DBEntry)}]} ||
              DBEntry <- Data]}.

%% interface for monitoring calls
get_node_info() ->
    {struct, [{status, "ok"}, {value, {struct, api_monitoring:get_node_info()}}]}.

get_node_performance() ->
    {struct, [{status, "ok"}, {value, {struct, api_monitoring:get_node_performance()}}]}.

get_service_info() ->
    {struct, [{status, "ok"}, {value, {struct, api_monitoring:get_service_info()}}]}.

get_service_performance() ->
    {struct, [{status, "ok"}, {value, {struct, api_monitoring:get_service_performance()}}]}.
