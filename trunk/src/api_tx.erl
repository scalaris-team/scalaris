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
%% @doc API for transactional, consistent access to the replicated DHT items
%% @version $Id$
-module(api_tx).
-author('schintke@zib.de').
-vsn('$Id$').

-export([new_tlog/0, req_list/2, read/1, write/2, test_and_set/3]).

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

-spec new_tlog() -> tx_tlog:tlog().
new_tlog() -> tx_tlog:empty().

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

%% @doc reads the value of a key
-spec read(client_key()) -> read_result().
read(Key) ->
    ReqList = [{read, Key}],
    {_TLog, [Res]} = api_tx:req_list(tx_tlog:empty(), ReqList),
    Res.

%% @doc writes the value of a key
-spec write(client_key(), client_value()) -> commit_result().
write(Key, Value) ->
    ReqList = [{write, Key, Value}, {commit}],
    {_TLog, [_Res1, Res2]} = api_tx:req_list(tx_tlog:empty(), ReqList),
    Res2.

%% @doc atomic compare and swap
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
