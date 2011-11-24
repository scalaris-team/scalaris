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

%% @author Florian Schintke <schintke@zib.de>,
%% @author Nico Kruber <kruber@zib.de>
%% @doc API for transactional access to replicated DHT items.
%% For single request/single item operations, we provide read/1,
%% write/2, and test_and_set/3 functions that directly commit.
%%
%% For compound transactions a transaction log has to be passed
%% through all operations and finally has to be committed. This is
%% supported by the functions new_tlog/0, read/2, write/3, req_list/2,
%% and commit/1.
%% @end
%% @version $Id$
-module(api_tx).
-author('schintke@zib.de').
-author('kruber@zib.de').
-vsn('$Id$').

%% Perform a chain of operations (passing a transaction log) and
%% finally commit the transaction.
-export([new_tlog/0, req_list/1, req_list/2,
         read/2, write/3, set_change/4, number_add/3, test_and_set/4, commit/1]).

%% Perform single operation transactions.
-export([req_list_commit_each/1,
         read/1, write/2, set_change/3, number_add/2, test_and_set/3]).

-ifdef(with_export_type_support).
-export_type([request/0, read_result/0, write_result/0, commit_result/0, result/0,
              request_on_key/0]).
-endif.

-include("scalaris.hrl").
-include("client_types.hrl").

% Public Interface
-type request_on_key() ::
          {read, client_key()}
        | {write, client_key(), client_value()}
        | {set_change, client_key(), ToAdd::[client_value()], ToRemove::[client_value()]}
        | {number_add, client_key(), number()}
        | {test_and_set, client_key(), Old::client_value(), New::client_value()}.
-type request() :: request_on_key() | {commit}.
-type read_result() :: {ok, client_value()} | {fail, timeout | not_found}.
-type write_result() :: {ok} | {fail, timeout}.
-type listop_result() :: write_result() | {fail, not_a_list}.
-type numberop_result() :: write_result() | {fail, not_a_number}.
-type commit_result() :: {ok} | {fail, abort | timeout}.
-type testandset_result() :: commit_result() | {fail, not_found | {key_changed, RealOldValue::client_value()}}.
-type result() :: read_result() | write_result() | listop_result() | numberop_result() | testandset_result() | commit_result().

%% @doc Get an empty transaction log to start a new transaction.
-spec new_tlog() -> tx_tlog:tlog().
new_tlog() -> tx_tlog:empty().

%% @doc Perform several requests starting a new transaction.
-spec req_list([request()]) -> {tx_tlog:tlog(), [result()]}.
req_list(ReqList) ->
    req_list(new_tlog(), ReqList).

%% @doc Perform several requests inside a transaction and monitors its
%%      execution time.
-spec req_list(tx_tlog:tlog(), [request()]) -> {tx_tlog:tlog(), [result()]}.
req_list(TLog, ReqList) ->
    {TimeInUs, Result} = util:tc(fun req_list2/2, [TLog, ReqList]),
    monitor:client_monitor_set_value(
      ?MODULE, "req_list",
      fun(Old) ->
              Old2 = case Old of
                         % 10s monitoring interval, only keep newest in the client process
                         undefined -> rrd:create(10 * 1000000, 1, {timing, ms});
                         _ -> Old
                     end,
              rrd:add_now(TimeInUs / 1000, Old2)
      end),
    Result.

%% @doc Perform several requests inside a transaction (internal implementation).
-spec req_list2(tx_tlog:tlog(), [request()]) -> {tx_tlog:tlog(), [result()]}.
req_list2(TLog, ReqList) ->
    rdht_tx0:req_list(TLog, ReqList).

%% @doc Perform a read inside a transaction.
-spec read(tx_tlog:tlog(), client_key())
          -> {tx_tlog:tlog(), read_result()}.
read(TLog, Key) ->
    {NewTLog, [Result]} = req_list(TLog, [{read, Key}]),
    {NewTLog, Result}.

%% @doc Perform a write inside a transaction.
-spec write(tx_tlog:tlog(), client_key(), client_value())
           -> {tx_tlog:tlog(), write_result()}.
write(TLog, Key, Value) ->
    {NewTLog, [Result]} = req_list(TLog, [{write, Key, Value}]),
    {NewTLog, Result}.

%% @doc Perform a set_change operation inside a transaction.
-spec set_change(tx_tlog:tlog(), client_key(), ToAdd::[client_value()], ToRemove::[client_value()])
           -> {tx_tlog:tlog(), listop_result()}.
set_change(TLog, Key, ToAdd, ToRemove) ->
    {NewTLog, [Result]} = req_list(TLog, [{set_change, Key, ToAdd, ToRemove}]),
    {NewTLog, Result}.

%% @doc Perform a set_change operation inside a transaction.
-spec number_add(tx_tlog:tlog(), client_key(), ToAdd::number())
           -> {tx_tlog:tlog(), numberop_result()}.
number_add(TLog, Key, ToAdd) ->
    {NewTLog, [Result]} = req_list(TLog, [{number_add, Key, ToAdd}]),
    {NewTLog, Result}.

%% @doc Atomically compare and set a value for a key inside a transaction.
%%      If the value stored at Key is the same as OldValue, then NewValue will
%%      be stored.
-spec test_and_set(tx_tlog:tlog(), Key::client_key(), OldValue::client_value(), NewValue::client_value())
        -> {tx_tlog:tlog(), testandset_result()}.
test_and_set(TLog, Key, OldValue, NewValue) ->
    {NewTLog, [Result]} = req_list(TLog, [{test_and_set, Key, OldValue, NewValue}]),
    {NewTLog, Result}.

%% @doc Finish a transaction and materialize it atomically on the DHT.
-spec commit(tx_tlog:tlog()) -> commit_result().
commit(TLog) ->
    {_NewTLog, [Result]} = req_list(TLog, [{commit}]),
    Result.

%% @doc Atomically read a single key (not as part of a transaction).
-spec read(client_key()) -> read_result().
read(Key) ->
    ReqList = [{read, Key}],
    {_TLog, [Res]} = req_list(tx_tlog:empty(), ReqList),
    Res.

%% @doc Atomically write and commit a single key (not as part of a transaction).
-spec write(client_key(), client_value()) -> commit_result().
write(Key, Value) ->
    ReqList = [{write, Key, Value}, {commit}],
    {_TLog, [_Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    Res2.

%% @doc Atomically perform a set_change operation and a commit (not as part of a transaction).
-spec set_change(client_key(), ToAdd::[client_value()], ToRemove::[client_value()])
           -> listop_result() | {fail, abort}.
set_change(Key, ToAdd, ToRemove) ->
    ReqList = [{set_change, Key, ToAdd, ToRemove}, {commit}],
    {_TLog, [Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    case Res1 of
        X when erlang:is_tuple(X) andalso erlang:element(1, X) =:= fail -> X;
        _ -> Res2
    end.

%% @doc Atomically perform a set_change operation and a commit (not as part of a transaction).
-spec number_add(client_key(), ToAdd::number())
           -> numberop_result() | {fail, abort}.
number_add(Key, ToAdd) ->
    ReqList = [{number_add, Key, ToAdd}, {commit}],
    {_TLog, [Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    case Res1 of
        X when erlang:is_tuple(X) andalso erlang:element(1, X) =:= fail -> X;
        _ -> Res2
    end.

%% @doc Atomically compare and set a value for a key (not as part of a transaction).
%%      If the value stored at Key is the same as OldValue, then NewValue will
%%      be stored.
-spec test_and_set(Key::client_key(), OldValue::client_value(), NewValue::client_value())
        -> testandset_result() | {fail, abort}.
test_and_set(Key, OldValue, NewValue) ->
    ReqList = [{test_and_set, Key, OldValue, NewValue}, {commit}],
    {_TLog, [Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    case Res1 of
        X when erlang:is_tuple(X) andalso erlang:element(1, X) =:= fail -> X;
        _ -> Res2
    end.

-spec req_list_commit_each([request()]) -> [result()].
req_list_commit_each(ReqList) ->
    [ case Req of
          {read, Key} -> read(Key);
          {write, Key, Value} -> write(Key, Value);
          {set_change, Key, ToAdd, ToRemove} -> set_change(Key, ToAdd, ToRemove);
          {number_add, Key, ToAdd} -> number_add(Key, ToAdd);
          {test_and_set, Key, Old, New} -> test_and_set(Key, Old, New);
          _ -> {fail, abort}
      end || Req <- ReqList].
