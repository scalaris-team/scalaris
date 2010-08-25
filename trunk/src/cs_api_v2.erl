%% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
-module(cs_api_v2).
-author('schintke@zib.de').
-vsn('$Id$').

-export([new_tlog/0, process_request_list/2,
         read/1, write/2, delete/1,
         test_and_set/3, range_read/2]).

-include("scalaris.hrl").

% Public Interface

%% @type key() = term(). Key
-type(key() :: term()).
%% @type value() = term(). Value
-type(value() :: term()).

new_tlog() -> tx_tlog:empty().

process_request_list([], [{commit}]) ->
    {[], {results, [commit]}};
process_request_list(TLog, ReqList) ->
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
    %% @TODO Scan for fail in TransLog, then return imediately?
    {TmpTransLogResult, {results, TmpResultList}} =
        rdht_tx:process_request_list(TLog, RDHT_ReqList),
%%     TransLogResult = [ case element(1, Entry) of
%%                            rdht_tx_read -> setelement(1, Entry, read);
%%                            rdht_tx_write -> setelement(1, Entry, write)
%%                        end || Entry <- TmpTransLogResult ],
    TransLogResult = TmpTransLogResult,
    ResultList = [ case element(1, Entry) of
                       rdht_tx_read -> setelement(1, Entry, read);
                       rdht_tx_write -> setelement(1, Entry, write);
                       Any -> Any %% commit results
                   end || Entry <- TmpResultList ],
    %% this returns the NewTLog and an ordered
    %% result list in the form
    {TransLogResult, {results, ResultList}}.

%% @doc reads the value of a key
%% @spec read(key()) -> value() | {fail, term()}
-spec read(key()) -> value() | {fail, term()}.
read(Key) ->
    ReqList = [{read, Key}],
    case process_request_list(tx_tlog:empty(), ReqList) of
        {_TLog, {results, [{read, Key, {fail, Reason}}]}} -> {fail, Reason};
        {_TLog, {results, [{read, Key, {value, Value}}]}} -> Value
    end.

%% @doc writes the value of a key
%% @spec write(key(), value()) -> ok | {fail, term()}
-spec write(key(), value()) -> ok | {fail, term()}.
write(Key, Value) ->
    ReqList = [{write, Key, Value}, {commit}],
    case process_request_list(tx_tlog:empty(), ReqList) of
        {_TLog, {results, [{write, Key, {value, Value}}, commit]}} -> ok;
%        {_TLog, {results, [{write, Key, {fail, timeout}}, Reason]}} ->
%            {fail, Reason};
        {_TLog, {results, [{write, Key, {value, Value}}, Reason]}} ->
            {fail, Reason}
    end.

delete(Key) ->
    transaction_api:delete(Key, 2000).

%% @doc atomic compare and swap
%% @spec test_and_set(key(), value(), value()) -> ok | {fail, term()}
-spec test_and_set(key(), value(), value()) -> ok | {fail, Reason::term()}.
test_and_set(Key, OldValue, NewValue) ->
    ReadReqList = [{read, Key}],
    WriteReqList = [{write, Key, NewValue}, {commit}],
    {TLog, Results} = process_request_list(tx_tlog:empty(), ReadReqList),
    {results, [{read, Key, Result}]} = Results,
    case Result of
        {fail, timeout} -> {{fail, timeout}, TLog};
        _ -> if (Result =:= {fail, not_found})
                orelse (Result =:= {value, OldValue}) ->
                     {_TLog2, Results2} = process_request_list(TLog, WriteReqList),
                     {results, [_, Result2]} = Results2,
                     case Result2 of
                         commit -> ok;
                         abort -> {fail, write}
                     end;
                true -> {fail, {key_changed, element(2,Result)}}
             end
    end.

%% use exception handling for errors
% new_test_and_set(Key, OldValue, NewValue) ->
%     ReadReqList = [{read, Key}],
%     WriteReqList = [{write, Key, NewValue}, {commit}],
%     {TLog, Results} = process_request_list(tx_tlog:empty(), ReadReqList),
%%     case cs_api:ok(Results) of
%%         true ->
%             {TLog2, Results2} = process_request_list(TLog, WriteReqList),
%             cs_api_result:ok(Results2);
%%         false -> false
%     end.


%@doc read a range of key-value pairs
-spec range_read(intervals:key(), intervals:key()) -> {ok | timeout, [db_entry:entry()]}.
range_read(From, To) ->
    Interval = intervals:new('[', From, To, ']'),
    bulkowner:issue_bulk_owner(Interval,
                               {bulk_read_entry, comm:this()}),
    TimerRef =
        comm:send_local_after(config:read(range_read_timeout), self(), {range_read_timeout}),
    range_read_loop(Interval, intervals:empty(), [], TimerRef).

-spec range_read_loop(Interval::intervals:interval(), Done::intervals:interval(), Data::[db_entry:entry()], TimerRef::reference()) -> {ok | timeout, [db_entry:entry()]}.
range_read_loop(Interval, Done, Data, TimerRef) ->
    receive
        {range_read_timeout} ->
            {timeout, lists:flatten(Data)};
        {bulk_read_entry_response, NowDone, NewData} ->
            Done2 = intervals:union(NowDone, Done),
            case intervals:is_subset(Interval, Done2) of
                false ->
                    range_read_loop(Interval, Done2, [NewData | Data], TimerRef);
                true ->
                    % cancel timeout
                    erlang:cancel_timer(TimerRef),
                    % consume potential timeout message
                    receive
                        {range_read_timeout} -> ok
                    after 0 -> ok
                    end,
                    {ok, lists:flatten(Data, NewData)}
            end
    end.
