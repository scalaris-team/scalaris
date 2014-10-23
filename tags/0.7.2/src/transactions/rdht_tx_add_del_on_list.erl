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
%%      The add_del_on_list operation.
%%      This "two-phase" operation uses rdht_tx_read to first read the value
%%      and then alters the tlog entry so that it appears to be a write
%%      operation from rdht_tx_write. Changes are performed in the context of
%%      the client, not on the responsible node(s)!
%% @version $Id$
-module(rdht_tx_add_del_on_list).
-author('kruber@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").
-include("client_types.hrl").

-export([work_phase/3, extract_from_tlog/5]).

% feeder for tester
-export([extract_from_tlog_feeder/5]).


-spec work_phase(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                 api_tx:request()) -> ok.
work_phase(ClientPid, ReqId, Request) ->
    rdht_tx_read:work_phase(ClientPid, ReqId, Request).

-spec extract_from_tlog_feeder
        (tx_tlog:tlog_entry(), client_key(), client_value(), client_value(), EnDecode::true)
        -> {tx_tlog:tlog_entry(), client_key(), client_value(), client_value(), EnDecode::true};
        (tx_tlog:tlog_entry(), client_key(), rdht_tx:encoded_value(), rdht_tx:encoded_value(), EnDecode::false)
        -> {tx_tlog:tlog_entry(), client_key(), rdht_tx:encoded_value(), rdht_tx:encoded_value(), EnDecode::false}.
extract_from_tlog_feeder(Entry, Key, ToAdd, ToDel, EnDecode) ->
    % the result in the tlog is essentially a read op
    % -> similar to rdht_tx_read:extract_from_tlog_feeder/4
    {ValType, EncVal} = tx_tlog:get_entry_value(Entry),
    NewEntry =
        % transform unknow value types to a valid ?value type:
        case not lists:member(ValType, [?value, ?not_found]) of
            true -> tx_tlog:set_entry_value(Entry, ?value, EncVal);
            _    -> Entry
        end,
    {NewEntry, Key, ToAdd, ToDel, EnDecode}.

%% @doc Simulate a change on a set via read and write requests.
%%      Update the TLog entry accordingly.
-spec extract_from_tlog
        (tx_tlog:tlog_entry(), client_key(), client_value(), client_value(), EnDecode::true)
            -> {tx_tlog:tlog_entry(), api_tx:listop_result()};
        (tx_tlog:tlog_entry(), client_key(), rdht_tx:encoded_value(), rdht_tx:encoded_value(), EnDecode::false)
            -> {tx_tlog:tlog_entry(), api_tx:listop_result()}.
extract_from_tlog(Entry, _Key, ToAdd, ToDel, true) when
      (not erlang:is_list(ToAdd)) orelse
      (not erlang:is_list(ToDel)) ->
    %% input type error
    {tx_tlog:set_entry_status(Entry, ?fail), {fail, not_a_list}};
extract_from_tlog(Entry0, Key, ToAdd, ToDel, true) ->
    {Entry, Res0} = rdht_tx_read:extract_from_tlog(Entry0, Key, read, true),
    case Res0 of
        {ok, OldValue} when erlang:is_list(OldValue) ->
            %% types ok
            case ToAdd =:= [] andalso ToDel =:= [] of
                true -> {Entry, {ok}}; % no op
                _ ->
                    NewValue1 = lists:append(ToAdd, OldValue),
                    NewValue2 = util:minus_first(NewValue1, ToDel),
                    rdht_tx_write:extract_from_tlog(Entry, Key, NewValue2, true)
            end;
        {fail, not_found} -> %% key creation
            NewValue2 = util:minus_first(ToAdd, ToDel),
            rdht_tx_write:extract_from_tlog(Entry, Key, NewValue2, true);
        {ok, _} -> %% value is not a list
            {tx_tlog:set_entry_status(Entry, ?fail),
             {fail, not_a_list}}
    end;
extract_from_tlog(Entry, Key, ToAdd, ToDel, false) ->
    % note: we can only work with decoded values here
    extract_from_tlog(Entry, Key, rdht_tx:decode_value(ToAdd), rdht_tx:decode_value(ToDel), true).
