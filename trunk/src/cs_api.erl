% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc API for transactional, consistent access to the replicated DHT items
%% @version $Id$
-module(cs_api).
-author('schuett@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-export([new_tlog/0, process_request_list/2,
         read/1, write/2, delete/1,
         test_and_set/3, range_read/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type request() :: {read, client_key()} | {write, client_key(), client_value()} | {commit}.
-type result() :: any(). %% TODO: specify result() type

-spec new_tlog() -> tx_tlog:tlog().
new_tlog() -> txlog:new().

-spec process_request_list(tx_tlog:tlog(), [request()]) -> {tx_tlog:tlog(), {results, [result()]}}.
process_request_list(TLog, ReqList) ->
    ?TRACE("cs_api:process_request_list(~p, ~p)~n", [TLog, ReqList]),
    % should just call transaction_api:process_request_list
    % for parallel quorum reads and scan for commit request to actually do
    % the transaction
    % and there should scan for duplicate keys in ReqList
    % commit is only allowed as last element in ReqList
    {TransLogResult, ReverseResultList} =
        lists:foldl(
          fun(Request, {AccTLog, AccRes}) ->
                  {NewAccTLog, SingleResult} = process_request(AccTLog, Request),
                  {NewAccTLog, [SingleResult | AccRes]}
          end,
          {TLog, []}, ReqList),
    {TransLogResult, {results, lists:reverse(ReverseResultList)}}.

process_request(TLog, Request) ->
    ?TRACE("cs_api:process_request(~p, ~p)~n", [TLog, Request]),
    case Request of
        {read, Key} ->
            case util:tc(transaction_api, read, [Key, TLog]) of
                {_Time, {{value, empty_val}, NTLog}} ->
                    ?LOG_CS_API(read_fail, _Time / 1000.0),
                    {NTLog, {read, Key, {fail, not_found}}};
                {_Time, {{value, Val}, NTLog}} ->
                    ?LOG_CS_API(read_success, _Time / 1000.0),
                    {NTLog, {read, Key, {value, Val}}};
                {_Time, {{fail, Reason}, NTLog}} ->
                    ?LOG_CS_API(read_fail, _Time / 1000.0),
                    {NTLog, {read, Key, {fail, Reason}}}
            end;
        {write, Key, Value} ->
            case util:tc(transaction_api, write, [Key, Value, TLog]) of
                {_Time, {ok, NTLog}} ->
                    ?LOG_CS_API(write_success, _Time / 1000.0),
                    {NTLog, {write, Key, {value, Value}}};
                {_Time, {{fail, Reason}, NTLog}} ->
                    ?LOG_CS_API(write_fail, _Time / 1000.0),
                    {NTLog, {write, Key, {fail, Reason}}}
            end;
        {commit} ->
            case util:tc(transaction_api, commit, [TLog]) of
                {_Time, {ok}} ->
                    ?LOG_CS_API(commit_success, _Time / 1000.0),
                    {TLog, {commit, ok, {value, "ok"}}};
                {_Time, {fail, Reason}} ->
                    ?LOG_CS_API(commit_fail, _Time / 1000.0),
                    {TLog, {commit, fail, {fail, Reason}}}
            end
    end.

%% @doc reads the value of a key
%% @spec read(client_key()) -> client_value() | {fail, term()} 
-spec read(client_key()) -> client_value() | {fail, term()}.
read(Key) ->
    ?TRACE("cs_api:read(~p)~n", [Key]),
    case util:tc(transaction_api, quorum_read, [Key]) of
        {_Time, {empty_val, _}} ->
            ?LOG_CS_API(quorum_read_fail, _Time / 1000.0),
            {fail, not_found};
        {_Time, {fail, Reason}} ->
            ?LOG_CS_API(quorum_read_fail, _Time / 1000.0),
            {fail, Reason};
        {_Time, {Value, _Version}} ->
            ?LOG_CS_API(quorum_read_success, _Time / 1000.0),
            Value
    end.

%% @doc writes the value of a key
%% @spec write(client_key(), client_value()) -> ok | {fail, term()}
-spec write(client_key(), client_value()) -> ok | {fail, term()}.
write(Key, Value) ->
    ?TRACE("cs_api:write(~p, ~p)~n", [Key, Value]),
    case util:tc(transaction_api, single_write, [Key, Value]) of
        {_Time, commit} ->
            ?LOG_CS_API(single_write_success, _Time / 1000.0),
            ok;
        {_Time, {fail, Reason}} ->
            ?LOG_CS_API(single_write_fail, _Time / 1000.0),
            {fail, Reason}
    end.

-spec delete(Key::client_key())
        -> {ok, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, timeout} |
           {fail, timeout, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, node_not_found}.
delete(Key) ->
    ?TRACE("cs_api:delete(~p)~n", [Key]),
    transaction_api:delete(Key, 2000).

%% @doc atomic compare and swap
%% @spec test_and_set(client_key(), client_value(), client_value()) -> ok | {fail, term()}
-spec test_and_set(client_key(), client_value(), client_value()) -> ok | {fail, Reason::term()}.
test_and_set(Key, OldValue, NewValue) ->
    ?TRACE("cs_api:test_and_set(~p, ~p, ~p)~n", [Key, OldValue, NewValue]),
    TFun = fun(TransLog) ->
                   {Result, TransLog1} = transaction_api:read(Key, TransLog),
                   case Result of
                       {value, empty_val} ->
                           %% same as not found
                           {Result2, TransLog2} = transaction_api:write(Key, NewValue, TransLog),
                           if
                               Result2 =:= ok ->
                                   {{ok, done}, TransLog2};
                               true ->
                                   {{fail, write}, TransLog2}
                           end;
                       {value, ReadValue} ->
                           if
                               ReadValue =:= OldValue ->
                                   {Result2, TransLog2} = transaction_api:write(Key, NewValue, TransLog1),
                                   if
                                       Result2 =:= ok ->
                                           {{ok, done}, TransLog2};
                                       true ->
                                           {{fail, write}, TransLog2}
                                   end;
                               true ->
                                   {{fail, {key_changed, ReadValue}}, TransLog1}
                           end;
                       {fail, not_found} ->
                           {Result2, TransLog2} = transaction_api:write(Key, NewValue, TransLog),
                           if
                               Result2 =:= ok ->
                                   {{ok, done}, TransLog2};
                               true ->
                                   {{fail, write}, TransLog2}
                           end;
                       {fail,timeout} ->
                           {{fail, timeout}, TransLog}
                       end
           end,
    SuccessFun = fun(X) -> {success, X} end,
    FailureFun = fun(X) -> {failure, X} end,
    case do_transaction_locally(TFun, SuccessFun, FailureFun, config:read(test_and_set_timeout)) of
        {trans, {success, {commit, done}}} ->
            ok;
        {trans, {failure, Reason}} ->
            {fail, Reason};
        X ->
            log:log(warn, "[ Node ~w ] cs_api:test_and_set unexpected: got ~p", [self(), X]),
            X
    end.


% I know there is a dht_node in this instance so I will use it directly
%@private
do_transaction_locally(TransFun, SuccessFun, Failure, Timeout) ->
    PID = pid_groups:find_a(dht_node),
    comm:send_local(PID , {do_transaction, TransFun, SuccessFun, Failure, comm:this()}),
    receive
        X ->
            X
    after
        Timeout ->
            do_transaction_locally(TransFun, SuccessFun, Failure, Timeout)
    end.

%@doc range a range of key-value pairs
-spec range_read(intervals:key(), intervals:key()) -> {ok | timeout, [db_entry:entry()]}.
range_read(From, To) ->
    ?TRACE("cs_api:range_read(~p, ~p)~n", [From, To]),
    Interval = intervals:new('[', From, To, ']'),
    bulkowner:issue_bulk_owner(Interval,
                               {bulk_read_entry, comm:this()}),
    TimerRef = comm:send_local_after(config:read(range_read_timeout), self(), {range_read_timeout}),
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
