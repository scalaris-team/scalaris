%% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% end

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

%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Scalaris API
%% @author Thorsten Schuett <schuett@zib.de>
%% @version $Id$
-module(cs_api).
-author('schuett@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-export([process_request_list/2, read/1, write/2, delete/1,
         test_and_set/3, range_read/2]).

-include("scalaris.hrl").



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @type key() = term(). Key
-type(key() :: term()).
%% @type value() = term(). Value
-type(value() :: term()).

process_request_list(TLog, ReqList) ->
    ?TRACE("cs_api:process_request_list(~p, ~p)~n", [TLog, ReqList]),
    erlang:put(instance_id, process_dictionary:find_group(dht_node)),
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
    {{translog, TransLogResult}, {results, lists:reverse(ReverseResultList)}}.

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
%% @spec read(key()) -> value() | {fail, term()} 
-spec read(key()) -> value() | {fail, term()}.
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
%% @spec write(key(), value()) -> ok | {fail, term()}
-spec write(key(), value()) -> ok | {fail, term()}.
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

delete(Key) ->
    ?TRACE("cs_api:delete(~p)~n", [Key]),
    transaction_api:delete(Key, 2000).

%% @doc atomic compare and swap
%% @spec test_and_set(key(), value(), value()) -> ok | {fail, term()}
-spec test_and_set(key(), value(), value()) -> ok | {fail, Reason::term()}.
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
            io:format("cs_api:test_and_set unexpected: Node ~w got ~p", [self(),X]),
            log:log(warn,"[ Node ~w ] ~p", [self(),X]),
            X
    end.


% I know there is a dht_node in this instance so I will use it directly
%@private
do_transaction_locally(TransFun, SuccessFun, Failure, Timeout) ->
    {ok, PID} = process_dictionary:find_dht_node(),
    comm:send_local(PID , {do_transaction, TransFun, SuccessFun, Failure, comm:this()}),
    receive
        X ->
            X
    after
        Timeout ->
            do_transaction_locally(TransFun, SuccessFun, Failure, Timeout)
    end.

%@doc range a range of key-value pairs
range_read(From, To) ->
    ?TRACE("cs_api:range_read(~p, ~p)~n", [From, To]),
    Interval = intervals:new(From, To),
    bulkowner:issue_bulk_owner(Interval,
                               {bulk_read_with_version, comm:this()}),
    comm:send_local_after(config:read(range_read_timeout), self(), {timeout}),
    range_read_loop(Interval, intervals:empty(), []).

-spec range_read_loop(Interval::intervals:interval(), Done::intervals:interval(), Data::[any()]) -> {timeout | ok, Data::[any()]}.
range_read_loop(Interval, Done, Data) ->
    receive
        {timeout} ->
            {timeout, lists:flatten(Data)};
        {bulk_read_with_version_response, NowDone, NewData} ->
            Done2 = intervals:union(NowDone, Done),
            case intervals:is_subset(Interval, Done2) of
                false ->
                    range_read_loop(Interval, Done2, [NewData | Data]);
                true ->
                    {ok, lists:flatten([NewData, Data])}
            end
    end.
