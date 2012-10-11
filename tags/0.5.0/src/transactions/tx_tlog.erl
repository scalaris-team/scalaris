% @copyright 2009-2012 Zuse Institute Berlin

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

% @author Florian Schintke <schintke@zib.de>
% @doc operations on the end user transaction log
% @version $Id$
-module(tx_tlog).
-author('schintke@zib.de').
-vsn('$Id$').

-compile({inline, [new_entry/5,
                   get_entry_operation/1, set_entry_operation/2,
                   get_entry_key/1,       set_entry_key/2,
                   get_entry_status/1,    set_entry_status/2,
                   get_entry_value/1,     set_entry_value/2,
                   drop_value/1,
                   get_entry_version/1
                  ]}).

-include("scalaris.hrl").
-include("client_types.hrl").

%% Operations on TLogs
-export([empty/0]).
-export([add_entry/2]).
-export([add_or_update_status_by_key/3]).
-export([update_entry/2]).
-export([sort_by_key/1]).
-export([find_entry_by_key/2]).
-export([is_sane_for_commit/1]).
-export([get_insane_keys/1]).
-export([merge/2, first_req_per_key_not_in_tlog/2, cleanup/1]).

%% Operations on entries of TLogs
-export([new_entry/5]).
-export([get_entry_operation/1, set_entry_operation/2]).
-export([get_entry_key/1,       set_entry_key/2]).
-export([get_entry_status/1,    set_entry_status/2]).
-export([get_entry_value/1,     set_entry_value/2]).
-export([drop_value/1]).
-export([get_entry_version/1]).

-ifdef(with_export_type_support).
-export_type([tlog/0, tlog_entry/0]).
-export_type([tx_status/0]).
-export_type([tx_op/0]).
-endif.

-type tx_status() :: ?value | {fail, abort | not_found}.
-type tx_op()     :: ?read | ?write.

-type tlog_key() :: client_key(). %%| ?RT:key().
%% TLogEntry: {Operation, Key, Status, Value, Version}
%% Sample: {?read,"key3",?value,"value3",0}
-type tlog_entry() ::
          { tx_op(),                  %% operation
            tlog_key(),             %% key
            non_neg_integer() | -1,   %% version
            tx_status(),              %% status
            any()                     %% value
          }.
%% -opaque tlog() :: [tlog_entry()]. % creates a false warning in add_or_update_status_by_key/3
-type tlog() :: [tlog_entry()].

% @doc create an empty list
-spec empty() -> tlog().
empty() -> [].

-spec add_entry(tlog(), tlog_entry()) -> tlog().
add_entry(TransLog, Entry) -> [ Entry | TransLog ].

-spec add_or_update_status_by_key(tlog(), tlog_key(), tx_status()) -> tlog().
add_or_update_status_by_key([], Key, Status) ->
    [new_entry(?write, Key, _Vers = 0, Status, _Val = 0)];
add_or_update_status_by_key([Entry | T], Key, Status)
  when element(2, Entry) =:= Key ->
    [set_entry_status(Entry, Status) | T];
add_or_update_status_by_key([Entry | T], Key, Status) ->
    [Entry | add_or_update_status_by_key(T, Key, Status)].

-spec update_entry(tlog(), tlog_entry()) -> tlog().
update_entry(TLog, Entry) ->
    lists:keyreplace(get_entry_key(Entry), 2, TLog, Entry).

-spec sort_by_key(tlog()) -> tlog().
sort_by_key(TLog) -> lists:keysort(2, TLog).

-spec find_entry_by_key(tlog(), tlog_key()) -> tlog_entry() | false.
find_entry_by_key(TLog, Key) ->
    lists:keyfind(Key, 2, TLog).

-spec entry_is_sane_for_commit(tlog_entry(), boolean()) -> boolean().
entry_is_sane_for_commit(Entry, Acc) ->
    Acc andalso
        not (is_tuple(get_entry_status(Entry))
             andalso fail =:= element(1, get_entry_status(Entry))
             andalso not_found =/= element(2, get_entry_status(Entry))
            ).

-spec is_sane_for_commit(tlog()) -> boolean().
is_sane_for_commit(TLog) ->
    lists:foldl(fun entry_is_sane_for_commit/2, true, TLog).

-spec get_insane_keys(tlog()) -> [client_key()].
get_insane_keys(TLog) ->
    lists:foldl(fun(X, Acc) ->
                        case entry_is_sane_for_commit(X, true) of
                            true -> Acc;
                            false -> [get_entry_key(X) | Acc]
                        end
                end, [], TLog).

%% @doc Merge TLog entries from sorted translogs (see sort_by_key/1), if same
%%      key. Check for version mismatch, take over values.
%%      Duplicate keys are only allowedif the old TLog only read the value!
%%      SortedTlog is old TLog
%%      SortedRTlog is TLog received from newer RDHT operations
-spec merge(tlog(), tlog()) -> tlog().
merge(TLog1, TLog2) ->
    merge_tlogs_iter(TLog1, TLog2, []).

-spec merge_tlogs_iter([tlog_entry()], [tlog_entry()], [tlog_entry()]) -> [tlog_entry()].
merge_tlogs_iter([TEntry | TTail] = SortedTLog,
                 [RTEntry | RTTail] = SortedRTLog,
                 Acc) ->
    TKey = get_entry_key(TEntry),
    RTKey = get_entry_key(RTEntry),
    if TKey < RTKey ->
           merge_tlogs_iter(TTail, SortedRTLog, [TEntry | Acc]);
       TKey > RTKey ->
           merge_tlogs_iter(SortedTLog, RTTail, [RTEntry | Acc]);
       true -> % TKey =:= RTKey ->
           %% key was in TLog, new entry is newer and contains value
           %% for read?
           case get_entry_operation(TEntry) of
               ?read ->
                   %% check versions: if mismatch -> change status to abort
                   TVersion = get_entry_version(TEntry),
                   RTVersion = get_entry_version(RTEntry),
                   NewTLogEntry =
                       if TVersion =:= RTVersion ->
                              Val = get_entry_value(RTEntry),
                              set_entry_value(TEntry, Val);
                          true ->
                              set_entry_status(RTEntry, {fail, abort})
                       end,
                   merge_tlogs_iter(TTail, RTTail, [NewTLogEntry | Acc]);
               _ ->
                   log:log(warn,
                           "Duplicate key in TLog merge should not happen ~p ~p", [TEntry, RTEntry]),
                   merge_tlogs_iter(TTail, RTTail, [ RTEntry | Acc])
           end
    end;
merge_tlogs_iter([], [], Acc)                 -> lists:reverse(Acc);
merge_tlogs_iter([], [_|_] = SortedRTLog, []) -> SortedRTLog;
merge_tlogs_iter([], [_|_] = SortedRTLog, [_|_] = Acc) -> lists:reverse(Acc) ++ SortedRTLog;
merge_tlogs_iter([_|_] = SortedTLog, [], [])  -> SortedTLog;
merge_tlogs_iter([_|_] = SortedTLog, [], [_|_] = Acc)  -> lists:reverse(Acc) ++ SortedTLog.

%% @doc Filters a request list with unique keys so that only operations reside
%%      that need data from the DHT which is not yet present in the TLog.
%%      Note: uses functions from rdht_tx to cope with requests.
-spec first_req_per_key_not_in_tlog(tlog(), [rdht_tx:request_on_key()])
        -> [rdht_tx:request_on_key()].
first_req_per_key_not_in_tlog(SortedTLog, USortedReqList) ->
    %% PRE: no {commit} requests in SortedReqList
    %% POST: returns requests in reverse order, but thats sufficient.
    first_req_per_key_not_in_tlog_iter(SortedTLog, USortedReqList, []).

%% @doc Helper to acquire requests, that need information from the DHT.
-spec first_req_per_key_not_in_tlog_iter(
        [tlog_entry()], [rdht_tx:request_on_key()], [rdht_tx:request_on_key()])
        -> [rdht_tx:request_on_key()].
first_req_per_key_not_in_tlog_iter(_, [], Acc) -> Acc;
first_req_per_key_not_in_tlog_iter([], USortedReqList, Acc) ->
    USortedReqList ++ Acc;
first_req_per_key_not_in_tlog_iter([TEntry | TTail] = USTLog,
                                   [Req | RTail] = USReqList,
                                   Acc) ->
    TKey = get_entry_key(TEntry),
    ReqKey = rdht_tx:req_get_key(Req),
    if TKey =:= ReqKey ->
            %% key is in TLog, rdht op not necessary
            case get_entry_operation(TEntry) of
                ?write ->
                    first_req_per_key_not_in_tlog_iter(USTLog, RTail, Acc);
                _ ->
                    NeedsOp = rdht_tx:req_needs_rdht_op_on_ex_tlog_read_entry(Req),
                    if NeedsOp ->
                           first_req_per_key_not_in_tlog_iter
                             (USTLog, RTail, [Req | Acc]);
                       true ->
                           first_req_per_key_not_in_tlog_iter(USTLog, RTail, Acc)
                    end
            end;
       TKey < ReqKey ->
           %% jump to next Tlog entry
           first_req_per_key_not_in_tlog_iter(TTail, USReqList, Acc);
       true -> % TKey > ReqKey
           first_req_per_key_not_in_tlog_iter(USTLog, RTail, [Req | Acc])
    end.

%% @doc Strips the tlog from read values (sets those values to '$empty').
-spec cleanup(tlog()) -> tlog().
cleanup(TLog) ->
    [ case get_entry_operation(TLogEntry) of
          ?read -> set_entry_value(TLogEntry, '$empty');
          ?write -> TLogEntry
      end || TLogEntry <- TLog ].

%% Operations on Elements of TransLogs (public)
-spec new_entry(tx_op(), client_key() | ?RT:key(),
                non_neg_integer() | -1,
                tx_status(), any()) -> tlog_entry().
new_entry(Op, Key, Vers, Status, Val) ->
    {Op, Key, Vers, Status, Val}.

-spec get_entry_operation(tlog_entry()) -> tx_op().
get_entry_operation(Element) -> element(1, Element).

-spec set_entry_operation(tlog_entry(), tx_op()) -> tlog_entry().
set_entry_operation(Element, Val) -> setelement(1, Element, Val).

-spec get_entry_key(tlog_entry()) -> client_key() | ?RT:key().
get_entry_key(Element)       -> element(2, Element).

-spec set_entry_key(tlog_entry(), client_key() | ?RT:key()) -> tlog_entry().
set_entry_key(Entry, Val)    -> setelement(2, Entry, Val).

-spec get_entry_version(tlog_entry()) -> non_neg_integer() | -1.
get_entry_version(Element)   -> element(3, Element).

-spec get_entry_status(tlog_entry()) -> tx_status().
get_entry_status(Element)    -> element(4, Element).

-spec set_entry_status(tlog_entry(), tx_status()) -> tlog_entry().
set_entry_status(Element, Val) -> setelement(4, Element, Val).

-spec get_entry_value(tlog_entry()) -> any().
get_entry_value(Element)     -> element(5, Element).

-spec set_entry_value(tlog_entry(), any()) -> tlog_entry().
set_entry_value(Element, Val)     -> setelement(5, Element, Val).

-spec drop_value(tlog_entry()) -> tlog_entry().
drop_value(Element)     -> setelement(5, Element, ?value_dropped).
