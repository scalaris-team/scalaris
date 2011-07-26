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
-vsn('$Id$').

%% Perform a chain of operations (passing a transaction log) and
%% finally commit the transaction.
-export([new_tlog/0, req_list/1, req_list/2, read/2, write/3, commit/1]).

%% Perform single operation transactions.
-export([read/1, write/2, test_and_set/3, req_list_commit_each/1]).

%% conversion between ?DB:value() and client_value():
-export([encode_value/1, decode_value/1]).

-ifdef(with_export_type_support).
-export_type([request/0, read_result/0, write_result/0, commit_result/0, result/0]).
-endif.

-include("scalaris.hrl").
-include("client_types.hrl").

% Public Interface
-type request() :: {read, client_key()}
                 | {write, client_key(), client_value()}
                 | {commit}.
-type read_result() :: {ok, client_value()} | {fail, timeout | not_found}.
-type write_result() :: {ok} | {fail, timeout}.
-type commit_result() :: {ok} | {fail, abort | timeout}.
-type result() :: read_result() | write_result() | commit_result().

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
                         % 10s monitoring interval, only keep newest
                         undefined -> rrd:create(10 * 1000000, 1, timing);
                         _ -> Old
                     end,
              rrd:add_now(TimeInUs, Old2)
      end),
    Result.

%% @doc Perform several requests inside a transaction (internal implementation).
-spec req_list2(tx_tlog:tlog(), [request()]) -> {tx_tlog:tlog(), [result()]}.
req_list2([], [{commit}]) ->
    {[], [{ok}]};
req_list2(TLog, ReqList) ->
    %% @todo should choose a dht_node in the local VM at random or even
    %% better round robin.
    %% replace operations by corresponding module names in ReqList
    %% number requests in ReqList to keep ordering more easily
    RDHT_ReqList = [ case Entry of
                         {read, Key} -> {rdht_tx_read, Key};
                         {write, Key, Value} -> {rdht_tx_write, Key, encode_value(Value)};
                         {commit} -> {commit}
                     end || Entry <- ReqList ],
    %% sanity checks on ReqList:
    %% @TODO Scan for fail in TransLog, then return immediately?
    {NewTLog, Results} = rdht_tx:req_list(TLog, RDHT_ReqList),
    {NewTLog, [decode_rdht_result(Result) || Result <- Results]}.

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

%% @doc Atomically compare and set a value for a key (not as part of a transaction).
%%      If the value stored at Key is the same as OldValue, then NewValue will
%%      be stored.
-spec test_and_set(Key::client_key(), OldValue::client_value(), NewValue::client_value())
        -> commit_result() | {fail, not_found | {key_changed, RealOldValue::client_value()}}.
test_and_set(Key, OldValue, NewValue) ->
    ReadReqList = [{read, Key}],
    WriteReqList = [{write, Key, NewValue}, {commit}],
    {TLog, [Result]} = req_list(tx_tlog:empty(), ReadReqList),
    case Result of
        X = {fail, timeout} -> X;
        Y = {fail, not_found} -> Y;
        {ok, OldValue} -> {_TLog2, [_, Res2]} = req_list(TLog, WriteReqList),
                          Res2;
        {ok, RealOldValue} -> {fail, {key_changed, RealOldValue}}
    end.

-spec req_list_commit_each([request()]) -> [result()].
req_list_commit_each(ReqList) ->
    [ case Req of
          {read, Key} -> read(Key);
          {write, Key, Value} -> write(Key, Value);
          _ -> {fail, abort}
      end || Req <- ReqList].

-spec encode_value(client_value()) -> ?DB:value().
encode_value(Value) when is_atom(Value) orelse is_boolean(Value) orelse
                             is_number(Value) ->
    Value;
encode_value(Value) when is_binary(Value) ->
    % do not compress a binary
    erlang:term_to_binary(Value, [{minor_version, 1}]);
encode_value(Value) ->
    erlang:term_to_binary(Value, [{compressed, 6}, {minor_version, 1}]).

-spec decode_value(?DB:value()) -> client_value().
decode_value(Value) when is_binary(Value) ->
    erlang:binary_to_term(Value);
decode_value(Value) ->
    Value.

-spec decode_rdht_result(rdht_tx:result_entry()) -> result().
decode_rdht_result({ok, Value}) -> {ok, decode_value(Value)};
decode_rdht_result(X) -> X.
