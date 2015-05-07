%% @copyright 2010-2013 Zuse Institute Berlin

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
%% @doc DB for a process internal state (lika a gen_component).
%% This abstraction allows for easy switching between
%% erlang:put/get/erase and ets:insert/lookup/delete
%% @end
%% @version $Id$
-module(pdb_ets).
-author('schintke@zib.de').
-vsn('$Id$').

-behaviour(pdb_beh).

-export_type([tableid/0]).

-export([new/2, get/2, set/2, delete/2, take/2, tab2list/1]).
-include("scalaris.hrl").

-type tableid() :: ets:tid() | atom().

-spec new(TableName::atom(),
          [set | ordered_set | bag | duplicate_bag |
           public | protected | private |
           named_table | {keypos, integer()} |
           {heir, pid(), term()} | {heir,none} |
           {write_concurrency, boolean()}]) -> tableid().
new(TableName, Params) ->
    ets:new(TableName, Params).

-spec get(term(), tableid()) -> tuple() | undefined.
get(Key, TableName) ->
    case ets:lookup(TableName, Key) of
        [] -> undefined;
        [Entry] -> Entry
    end.

-spec set(tuple(), tableid()) -> ok.
set(NewTuple, TableName) ->
    ets:insert(TableName, NewTuple),
    ok.

-spec delete(term(), tableid()) -> ok.
delete(Key, TableName) ->
    ets:delete(TableName, Key),
    ok.

-spec take(term(), tableid()) -> term() | undefined.
take(Key, TableName) ->
    Old = get(Key, TableName),
    ets:delete(TableName, Key),
    Old.

-spec tab2list(tableid()) -> [term()].
tab2list(TableName) ->
  ets:tab2list(TableName).
