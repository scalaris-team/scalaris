%% @copyright 2010-2012 Zuse Institute Berlin
%%            and onScale solutions GmbH

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc DB for a process internal state (lika a gen_component).
%% This abstraction allows for easy switching between
%% erlang:put/get/erase and ets:insert/lookup/delete
%% @end
%% @version $Id: pdb.erl 3617 2012-08-27 13:25:33Z kruber@zib.de $
-module(pdb_ets).
-author('schintke@onscale.de').
-vsn('$Id: pdb.erl 3617 2012-08-27 13:25:33Z kruber@zib.de $ ').

-behaviour(pdb_beh).

-ifdef(with_export_type_support).
-export_type([tableid/0]).
-endif.

-export([new/2, get/2, set/2, delete/2, take/2, tab2list/1]).
-include("scalaris.hrl").

-type tableid() :: tid() | atom().

-spec new(TableName::atom() | nonempty_string(),
          [set | ordered_set | bag | duplicate_bag |
           public | protected | private |
           named_table | {keypos, integer()} |
           {heir, pid(), term()} | {heir,none} |
           {write_concurrency, boolean()}]) -> tableid().
new([_|_] = TableName, Params) ->
    new(erlang:list_to_atom(TableName), Params);
new(TableName, Params) when is_atom(TableName) ->
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
