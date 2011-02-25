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

-include("scalaris.hrl").
-include("client_types.hrl").

% Public Interface
-type request() :: {read, client_key()}
                 | {write, client_key(), client_value()}
                 | {commit}.

-type read_result() ::
        {struct, [{status, string()}       %% "ok", "fail"
                  | {reason, string()}     %% "timeout", "not_found"
                  | {value, any()} ]}.
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

-spec tx_write(client_key(), client_value()) -> commit_result().
tx_write(Key, Value) ->
    Res = api_tx:write(Key, Value),
    result_to_json(Res).

-spec tx_test_and_set(client_key(), client_value(), client_value())
                     -> commit_result().
tx_test_and_set(Key, OldValue, NewValue) ->
    Res = api_tx:test_and_set(Key, OldValue, NewValue),
    result_to_json(Res).

results_to_json(Results) ->
    Entries = [ result_to_json(Result) || Result <- Results ],
    {array, Entries}.

result_to_json(Result) ->
    {struct,
     case Result of
         {ok}                       -> [{status, "ok"}];
         {ok, Val}                  -> [{status, "ok"},
                                        {value, Val}];
         {fail, {key_changed, Val}} -> [{status, "fail"},
                                        {reason, "key_changed"},
                                        {value,  lists:flatten(
                                                   io_lib:format("~s", [Val])
                                                  )}];
         {fail, Reason}              -> [{status, "fail"},
                                         {reason, atom_to_list(Reason)}]
     end
    }.

json_to_reqlist(JSON_ReqList) ->
    {array, TmpReqList} = JSON_ReqList,
    [ case Elem of
          {read, Key} ->
              {read, Key};
          {write, {struct, [{Key, Val}]}} ->
              {write, atom_to_list(Key), Val};
          {commit, _} ->
              {commit};
          Any -> Any
      end || {struct, [Elem]} <- TmpReqList ].

tlog_to_json(TLog) ->
    base64:encode_to_string(term_to_binary(TLog, [compressed])).

json_to_tlog(JsonTLog) ->
    binary_to_term(base64:decode(JsonTLog)).


