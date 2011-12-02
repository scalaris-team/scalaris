% @copyright 2009-2011 Zuse Institute Berlin

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

%% Operations on entries of TLogs
-export([new_entry/5]).
-export([get_entry_operation/1, set_entry_operation/2]).
-export([get_entry_key/1,       set_entry_key/2]).
-export([get_entry_status/1,    set_entry_status/2]).
-export([get_entry_value/1,     set_entry_value/2]).
-export([get_entry_version/1]).

-ifdef(with_export_type_support).
-export_type([tlog/0, tlog_entry/0]).
-export_type([tx_status/0]).
-export_type([tx_op/0]).
-endif.

-type tx_status() :: {fail, term()} | value | not_found.
-type tx_op()     :: rdht_tx_read   | rdht_tx_write.

-type tlog_key() :: client_key() | ?RT:key().
%% TLogEntry: {Operation, Key, Status, Value, Version}
%% Sample: {read,"key3",value,"value3",0}
-type tlog_entry() ::
          { tx_op(),                  %% operation
            tlog_key, %% key | hashed and replicated key
            tx_status(),              %% status
            any(),                    %% value
            integer()                 %% version
          }.
-type tlog() :: [tlog_entry()].

% @doc create an empty list
-spec empty() -> tlog().
empty() -> [].

-spec add_entry(tlog(), tlog_entry()) -> tlog().
add_entry(TransLog, Entry) -> [ Entry | TransLog ].

-spec add_or_update_status_by_key(tlog(),
                                  tlog_key(),
                                  tx_status()) -> tlog().
add_or_update_status_by_key(TLog, Key, Status) ->
    case lists:keyfind(Key, 2, TLog) of
        false ->
            Entry = new_entry(rdht_tx_write, Key, Status, 0, 0),
            add_entry(TLog, Entry);
        Entry ->
            NewEntry = set_entry_status(Entry, Status),
            update_entry(TLog, NewEntry)
    end.

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
        not (is_tuple(get_entry_status(Entry)) andalso
                 fail =:= element(1, get_entry_status(Entry))).

-spec is_sane_for_commit(tlog()) -> boolean().
is_sane_for_commit(TLog) ->
    lists:foldl(fun entry_is_sane_for_commit/2, true, TLog).


%% Operations on Elements of TransLogs (public)
-spec new_entry(tx_op(), client_key() | ?RT:key(), tx_status(), any(), integer()) -> tlog_entry().
new_entry(Op, Key, Status, Val, Vers) ->
%    #tlog_entry{op = Op, key = Key, status = Status, val = Val, vers = Vers}.
    {Op, Key, Status, Val, Vers}.

-spec get_entry_operation(tlog_entry()) -> tx_op().
get_entry_operation(Element) -> element(1, Element).

-spec set_entry_operation(tlog_entry(), tx_op()) -> tlog_entry().
set_entry_operation(Element, Val) -> setelement(1, Element, Val).

-spec get_entry_key(tlog_entry()) -> client_key() | ?RT:key().
get_entry_key(Element)       -> element(2, Element).

-spec set_entry_key(tlog_entry(), client_key() | ?RT:key()) -> tlog_entry().
set_entry_key(Entry, Val)    -> setelement(2, Entry, Val).

-spec get_entry_status(tlog_entry()) -> tx_status().
get_entry_status(Element)    -> element(3, Element).

-spec set_entry_status(tlog_entry(), tx_status()) -> tlog_entry().
set_entry_status(Element, Val)    -> setelement(3, Element, Val).

-spec get_entry_value(tlog_entry()) -> any().
get_entry_value(Element)     -> element(4, Element).

-spec set_entry_value(tlog_entry(), any()) -> tlog_entry().
set_entry_value(Element, Val)     -> setelement(4, Element, Val).

-spec get_entry_version(tlog_entry()) -> integer().
get_entry_version(Element)   -> element(5, Element).
