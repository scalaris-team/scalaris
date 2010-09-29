% @copyright 2009, 2010 onScale solutions GmbH

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

% @author Florian Schintke <schintke@onscale.de>
% @doc operations on the end user transaction log
% @version $Id$
-module(tx_tlog).
-author('schintke@onscale.de').
-vsn('$Id$').

-include("scalaris.hrl").

%% Operations on TransLogs
-export([empty/0]).
-export([add_entry/2]).
-export([filter_by_key/2]).
-export([filter_by_status/2]).
-export([update_entry/4]).

%% Operations on entries of TransLogs
-export([new_entry/5]).
-export([get_entry_operation/1]).
-export([get_entry_key/1]).
-export([get_entry_status/1]).
-export([get_entry_value/1]).
-export([get_entry_version/1]).

%% TranslogEntry: {Operation, Key, Status, Value, Version}
%% Sample: {read,"key3",ok,"value3",0}

-ifdef(with_export_type_support).
-export_type([tlog/0, tlog_entry/0]).
-export_type([tx_status/0]).
-endif.

-type tx_status() :: {fail, atom()} | value | not_found.
-type tx_op() :: read | write | rdht_tx_read | rdht_tx_write.
-type tlog_entry() ::
        { tx_op(),     %% operation
          ?RT:key(),   %% key
          tx_status(), %% status
          any(),       %% value
          integer()    %% version
        }.

-type tlog() :: [tlog_entry()].

% @doc create an empty list
-spec empty() -> [].
empty() -> [].

-spec add_entry(tlog(), tlog_entry()) -> tlog().
add_entry(TransLog, Entry) ->
    [ Entry | TransLog ].

-spec filter_by_key(tlog(), ?RT:key()) -> tlog().
filter_by_key(TransLog, Key) ->
    [ X || X <- TransLog, Key =:= get_entry_key(X) ].

-spec filter_by_status(tlog(), tx_status()) -> tlog().
filter_by_status(TransLog, Status) ->
    [ X || X <- TransLog, Status =:= get_entry_status(X) ].

-spec update_entry(tlog(), ?RT:key() | tlog_entry(), tx_op(), any()) -> tlog().
update_entry(TransLog, {read,LogKey,LogSuccess,_,LogVers} = Element,
             write, Value) ->
    UnchangedPart = lists:delete(Element, TransLog),
    add_entry(UnchangedPart,
              new_entry(write, LogKey, LogSuccess, Value, LogVers + 1));

update_entry(TransLog, {write,LogKey,LogSuccess,_,LogVers} = Element,
             write, Value) ->
    UnchangedPart = lists:delete(Element, TransLog),
    add_entry(UnchangedPart,
              new_entry(write, LogKey, LogSuccess, Value, LogVers));

update_entry(TransLog, Key, write, Value) ->
    [Element] = filter_by_key(TransLog, Key),
    update_entry(TransLog, Element, write, Value).


%% Operations on Elements of TransLogs
-spec new_entry(tx_op(), ?RT:key(), tx_status(), any(), integer()) -> tlog_entry().
new_entry(Op, Key, Status, Val, Vers) ->
%    #tlog_entry{op = Op, key = Key, status = Status, val = Val, vers = Vers}.
    {Op, Key, Status, Val, Vers}.
-spec get_entry_operation(tlog_entry()) -> tx_op().
get_entry_operation(Element) -> element(1, Element).
-spec get_entry_key(tlog_entry()) -> ?RT:key().
get_entry_key(Element)       -> element(2, Element).
-spec get_entry_status(tlog_entry()) -> tx_status().
get_entry_status(Element)    -> element(3, Element).
-spec get_entry_value(tlog_entry()) -> any().
get_entry_value(Element)     -> element(4, Element).
-spec get_entry_version(tlog_entry()) -> integer().
get_entry_version(Element)   -> element(5, Element).
