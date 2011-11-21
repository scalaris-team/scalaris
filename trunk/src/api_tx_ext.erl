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
-module(api_tx_ext).
-author('schintke@zib.de').
-vsn('$Id$').

%% Perform a chain of operations (passing a transaction log) and
%% finally commit the transaction.
-export([new_tlog/0, req_list/1, req_list/2,
         read/2, write/3, set_change/4, number_add/3, commit/1]).

%% Perform single operation transactions.
-export([req_list_commit_each/1,
         read/1, write/2, set_change/3, number_add/2, test_and_set/3]).

-ifdef(with_export_type_support).
-export_type([request/0, listop_result/0, numberop_result/0, result/0]).
-endif.

-include("scalaris.hrl").
-include("client_types.hrl").

% Public Interface
-type request() :: api_tx:request()
                 | {set_change, client_key(), ToAdd::[client_value()], ToRemove::[client_value()]}
                 | {number_add, client_key(), number()}.
-type listop_result() :: api_tx:write_result() | {fail, not_a_list}.
-type numberop_result() :: api_tx:write_result() | {fail, not_a_number}.
-type result() :: api_tx:result() | listop_result() | numberop_result().

%% @doc Get an empty transaction log to start a new transaction.
-spec new_tlog() -> tx_tlog:tlog().
new_tlog() -> api_tx:new_tlog().

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

-spec req_list_ext_foldl(Request::request(), {tx_tlog:tlog(), [result()]}) -> {tx_tlog:tlog(), [result()]}.
req_list_ext_foldl(Request, {TLog0, Results0}) ->
    case Request of
        {set_change, _Key, [], []} ->
            {TLog0, [{ok} | Results0]};
        {set_change, Key, ToAdd, ToRemove} ->
            {TLog1, [Result1]} = api_tx:req_list2(TLog0, [{read, Key}]),
            case Result1 of
                {ok, OldValue} when erlang:is_list(OldValue) ->
                    NewValue1 = lists:append(ToAdd, OldValue),
                    NewValue2 = util:minus(NewValue1, ToRemove),
                    {TLog2, [Result2]} = api_tx:req_list2(TLog1, [{write, Key, NewValue2}]),
                    {TLog2, [Result2 | Results0]};
                {fail, not_found} ->
                    NewValue2 = util:minus(ToAdd, ToRemove),
                    {TLog2, [Result2]} = api_tx:req_list2(TLog1, [{write, Key, NewValue2}]),
                    {TLog2, [Result2 | Results0]};
                {ok, _} ->
                    {TLog1, [{fail, not_a_list} | Results0]};
                X when erlang:is_tuple(X) ->
                    {TLog1, [X | Results0]}
            end;
        {number_add, _Key, X} when X == 0 -> % note: tolerate integer and float
            {TLog0, [{ok} | Results0]};
        {number_add, Key, X} ->
            {TLog1, [Result1]} = api_tx:req_list2(TLog0, [{read, Key}]),
            case Result1 of
                {ok, OldValue} when erlang:is_number(OldValue) ->
                    NewValue = OldValue + X,
                    {TLog2, [Result2]} = api_tx:req_list2(TLog1, [{write, Key, NewValue}]),
                    {TLog2, [Result2 | Results0]};
                {fail, not_found} ->
                    NewValue = X,
                    {TLog2, [Result2]} = api_tx:req_list2(TLog1, [{write, Key, NewValue}]),
                    {TLog2, [Result2 | Results0]};
                {ok, _} ->
                    {TLog1, [{fail, not_a_number} | Results0]};
                X when erlang:is_tuple(X) ->
                    {TLog1, [X | Results0]}
            end;
        Op ->
            {TLog1, [Result1]} = api_tx:req_list2(TLog0, [Op]),
            {TLog1, [Result1 | Results0]}
    end.

%% @doc Perform several requests inside a transaction (internal implementation).
-spec req_list2(tx_tlog:tlog(), [request()]) -> {tx_tlog:tlog(), [result()]}.
req_list2([], [{commit}]) ->
    {[], [{ok}]};
req_list2(TLog, ReqList) ->
    {NewTLog, ResultsRev} = lists:foldl(fun req_list_ext_foldl/2, {TLog, []}, ReqList),
    {NewTLog, lists:reverse(ResultsRev)}.

%% @doc Perform a read inside a transaction.
-spec read(tx_tlog:tlog(), client_key())
          -> {tx_tlog:tlog(), api_tx:read_result()}.
read(TLog, Key) ->
    {NewTLog, [Result]} = req_list(TLog, [{read, Key}]),
    {NewTLog, Result}.

%% @doc Perform a write inside a transaction.
-spec write(tx_tlog:tlog(), client_key(), client_value())
           -> {tx_tlog:tlog(), api_tx:write_result()}.
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

%% @doc Finish a transaction and materialize it atomically on the DHT.
-spec commit(tx_tlog:tlog()) -> api_tx:commit_result().
commit(TLog) ->
    {_NewTLog, [Result]} = req_list(TLog, [{commit}]),
    Result.

%% @doc Atomically read a single key (not as part of a transaction).
-spec read(client_key()) -> api_tx:read_result().
read(Key) ->
    ReqList = [{read, Key}],
    {_TLog, [Res]} = req_list(tx_tlog:empty(), ReqList),
    Res.

%% @doc Atomically write and commit a single key (not as part of a transaction).
-spec write(client_key(), client_value()) -> api_tx:commit_result().
write(Key, Value) ->
    ReqList = [{write, Key, Value}, {commit}],
    {_TLog, [_Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    Res2.

%% @doc Atomically perform a set_change operation and a commit (not as part of a transaction).
-spec set_change(client_key(), ToAdd::[client_value()], ToRemove::[client_value()])
           -> listop_result().
set_change(Key, ToAdd, ToRemove) ->
    ReqList = [{set_change, Key, ToAdd, ToRemove}, {commit}],
    {_TLog, [Result]} = req_list(tx_tlog:empty(), ReqList),
    Result.

%% @doc Atomically perform a set_change operation and a commit (not as part of a transaction).
-spec number_add(client_key(), ToAdd::number())
           -> numberop_result().
number_add(Key, ToAdd) ->
    ReqList = [{number_add, Key, ToAdd}, {commit}],
    {_TLog, [Result]} = req_list(tx_tlog:empty(), ReqList),
    Result.

%% @doc Atomically compare and set a value for a key (not as part of a transaction).
%%      If the value stored at Key is the same as OldValue, then NewValue will
%%      be stored.
-spec test_and_set(Key::client_key(), OldValue::client_value(), NewValue::client_value())
        -> api_tx:commit_result() | {fail, not_found | {key_changed, RealOldValue::client_value()}}.
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
          {set_change, Key, ToAdd, ToRemove} -> set_change(Key, ToAdd, ToRemove);
          {number_add, Key, ToAdd} -> number_add(Key, ToAdd);
          _ -> {fail, abort}
      end || Req <- ReqList].
