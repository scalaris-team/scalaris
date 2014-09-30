%% @copyright 2013 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc Part of replicated DHT implementation.
%%      The add_on_nr operation.
%%      This "two-phase" operation uses rdht_tx_read to first read the value
%%      and then alters the tlog entry so that it appears to be a write
%%      operation from rdht_tx_write. Changes are performed in the context of
%%      the client, not on the responsible node(s)!
%% @version $Id$
-module(rdht_tx_add_on_nr).
-author('kruber@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").
-include("client_types.hrl").

-export([work_phase/3, extract_from_tlog/4]).

% feeder for tester
-export([extract_from_tlog_feeder/4]).


-spec work_phase(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                 api_tx:request()) -> ok.
work_phase(ClientPid, ReqId, Request) ->
    rdht_tx_read:work_phase(ClientPid, ReqId, Request).

-spec extract_from_tlog_feeder
        (tx_tlog:tlog_entry(), client_key(), client_value(), EnDecode::true)
        -> {tx_tlog:tlog_entry(), client_key(), client_value(), EnDecode::true};
        (tx_tlog:tlog_entry(), client_key(), rdht_tx:encoded_value(), EnDecode::false)
        -> {tx_tlog:tlog_entry(), client_key(), rdht_tx:encoded_value(), EnDecode::false}.
extract_from_tlog_feeder(Entry, Key, X, EnDecode) ->
    % the result in the tlog is essentially a read op
    % -> similar to rdht_tx_read:extract_from_tlog_feeder/4
    {ValType, EncVal} = tx_tlog:get_entry_value(Entry),
    NewEntry =
        % transform unknow value types to a valid ?value type:
        case not lists:member(ValType, [?value, ?not_found]) of
            true -> tx_tlog:set_entry_value(Entry, ?value, EncVal);
            _    -> Entry
        end,
    {NewEntry, Key, X, EnDecode}.

%% @doc Simulate a change on a set via read and write requests.
%%      Update the TLog entry accordingly.
-spec extract_from_tlog
        (tx_tlog:tlog_entry(), client_key(), client_value(), EnDecode::true)
            -> {tx_tlog:tlog_entry(), api_tx:numberop_result()};
        (tx_tlog:tlog_entry(), client_key(), rdht_tx:encoded_value(), EnDecode::false)
            -> {tx_tlog:tlog_entry(), api_tx:numberop_result()}.
extract_from_tlog(Entry, _Key, X, true) when (not erlang:is_number(X)) ->
    %% check type of input data
    {tx_tlog:set_entry_status(Entry, ?fail),
     {fail, not_a_number}};
extract_from_tlog(Entry0, Key, X, true) ->
    {Entry, Res0} = rdht_tx_read:extract_from_tlog(Entry0, Key, read, true),
    case Res0 of
        {ok, OldValue} when erlang:is_number(OldValue) ->
            %% types ok
            case X == 0 of %% also accepts 0.0
                true -> {Entry, {ok}}; % no op
                _ ->
                    NewValue = OldValue + X,
                    rdht_tx_write:extract_from_tlog(Entry, Key, NewValue, true)
            end;
        {fail, not_found} -> %% key creation
            rdht_tx_write:extract_from_tlog(Entry, Key, X, true);
        {ok, _} -> %% value is not a number
            {tx_tlog:set_entry_status(Entry, ?fail),
             {fail, not_a_number}}
    end;
extract_from_tlog(Entry, Key, X, false) ->
    % note: we can only work with decoded values here
    extract_from_tlog(Entry, Key, rdht_tx:decode_value(X), true).
