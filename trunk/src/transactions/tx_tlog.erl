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

%% Operations on TransLogs
-export([empty/0]).
-export([add_entry/2]).
-export([filter_by_key/2]).
-export([filter_by_status/2]).
-export([update_status_by_key/3]).
-export([is_sane_for_commit/1]).
%%-export([update_entry/4]).

%% Operations on entries of TransLogs
-export([new_entry/5]).
-export([get_entry_operation/1]).
-export([get_entry_key/1,    set_entry_key/2]).
-export([get_entry_status/1, set_entry_status/2]).
-export([get_entry_value/1]).
-export([get_entry_version/1]).

%% TranslogEntry: {Operation, Key, Status, Value, Version}
%% Sample: {read,"key3",value,"value3",0}

-ifdef(with_export_type_support).
-export_type([tlog/0, tlog_entry/0]).
-export_type([tx_status/0]).
-export_type([tx_op/0]).
-endif.

-type tx_status()    :: {fail, term()} | value | not_found.
-type tx_op()        :: rdht_tx_read | rdht_tx_write.
-opaque tlog_entry() ::
          { tx_op(),                  %% operation
            client_key() | ?RT:key(), %% key | hashed and replicated key
            tx_status(),              %% status
            any(),                    %% value
            integer()                 %% version
          }.

-type tlog() :: [tlog_entry()].

%% Public tlog methods:

% @doc create an empty list
-spec empty() -> tlog().
empty() -> [].

-spec add_entry(tlog(), tlog_entry()) -> tlog().
add_entry(TransLog, Entry) -> [ Entry | TransLog ].

-spec filter_by_key(tlog(), client_key() | ?RT:key()) -> tlog().
filter_by_key(TransLog, Key) ->
    [ X || X <- TransLog, Key =:= get_entry_key(X) ].

-spec filter_by_status(tlog(), tx_status()) -> tlog().
filter_by_status(TransLog, Status) ->
    [ X || X <- TransLog, Status =:= get_entry_status(X) ].

-spec update_status_by_key(tlog(), client_key() | ?RT:key(), tx_status()) -> tlog().
update_status_by_key(TransLog, Key, Status) ->
    [ case get_entry_key(X) of
          Key -> set_entry_status(X, Status);
          _   -> X
      end || X <- TransLog ].

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

-spec get_entry_version(tlog_entry()) -> integer().
get_entry_version(Element)   -> element(5, Element).
