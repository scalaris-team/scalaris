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
-export([new_tlog/0, req_list/2, read/2, write/3, commit/1]).

%% Perform single operation transactions.
-export([read/1, write/2, test_and_set/3]).

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

%% @doc Perform several requests inside a transaction.
-spec req_list(tx_tlog:tlog(), [request()]) -> {tx_tlog:tlog(), [result()]}.
req_list([], [{commit}]) ->
    {[], [{ok}]};
req_list(TLog, ReqList) ->
    %% @todo should choose a dht_node in the local VM at random or even
    %% better round robin.
    %% replace operations by corresponding module names in ReqList
    %% number requests in ReqList to keep ordering more easily
    RDHT_ReqList = [ case element(1, Entry) of
                         read -> setelement(1, Entry, rdht_tx_read);
                         write -> setelement(1, Entry, rdht_tx_write);
                         commit -> Entry
                     end || Entry <- ReqList ],
    %% sanity checks on ReqList:
    %% @TODO Scan for fail in TransLog, then return immediately?
    {TransLogResult, TmpResultList} =
        rdht_tx:process_request_list(TLog, RDHT_ReqList),
    ResultList = [ case Entry of
                       {rdht_tx_read, _Key, {value, Value}} -> {ok, Value};
                       {rdht_tx_write, _Key, {value, _Value}} -> {ok};
                       {_Operation, _Key, Failure} -> Failure;
                       {commit} -> {ok};
                       {abort} -> {fail, abort}
                   end || Entry <- TmpResultList ],
    %% this returns the NewTLog and an ordered result list
    {TransLogResult, ResultList}.

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
    {_TLog, [Res]} = api_tx:req_list(tx_tlog:empty(), ReqList),
    Res.

%% @doc Atomically write and commit a single key (not as part of a transaction).
-spec write(client_key(), client_value()) -> commit_result().
write(Key, Value) ->
    ReqList = [{write, Key, Value}, {commit}],
    {_TLog, [_Res1, Res2]} = api_tx:req_list(tx_tlog:empty(), ReqList),
    Res2.

%% @doc Atomically compare and swap a value for a key (not as part of a transaction).
-spec test_and_set(client_key(), client_value(), client_value())
        -> commit_result() | {fail, {key_changed, client_value()}}.
test_and_set(Key, OldValue, NewValue) ->
    ReadReqList = [{read, Key}],
    WriteReqList = [{write, Key, NewValue}, {commit}],
    {TLog, [Result]} = req_list(tx_tlog:empty(), ReadReqList),
    case Result of
        {fail, timeout} -> {fail, timeout};
        _ -> if (Result =:= {fail, not_found})
                orelse (Result =:= {ok, OldValue}) ->
                     {_TLog2, [_, Res2]} = req_list(TLog, WriteReqList),
                     Res2;
                true -> {fail, {key_changed, element(2,Result)}}
             end
    end.
