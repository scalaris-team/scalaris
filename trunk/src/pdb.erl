%% @copyright 2010-2011 Zuse Institute Berlin
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
%% @version $Id$
-module(pdb).
-author('schintke@onscale.de').
-vsn('$Id$').

-ifdef(with_export_type_support).
-export_type([tableid/0]).
-endif.

-export([new/2, get/2, set/2, delete/2, tab2list/1]).
-include("scalaris.hrl").

-ifdef(PDB_ERL).
-type tableid() :: atom().
-endif.
-ifdef(PDB_ETS).
-type tableid() :: tid() | atom().
-endif.
-spec new(TableName::atom(), [set | ordered_set | bag | duplicate_bag |
                              public | protected | private |
                              named_table | {keypos, integer()} |
                              {heir, pid(), term()} | {heir,none} |
                              {write_concurrency, boolean()}]) -> tableid().
-ifdef(PDB_ERL).
new(TableName, _Params) ->
    TableName.
-endif.
-ifdef(PDB_ETS).
new(TableName, Params) ->
    ets:new(TableName, Params).
-endif.

-spec get(term(), tableid()) -> tuple() | undefined.
-ifdef(PDB_ERL).
get(Key, _TableName) ->
    erlang:get(Key).
-endif.
-ifdef(PDB_ETS).
get(Key, TableName) ->
    case ets:lookup(TableName, Key) of
        [] -> undefined;
        [Entry] -> Entry
    end.
-endif.

-spec set(tuple(), tableid()) -> ok.
-ifdef(PDB_ERL).
set(NewTuple, _TableName) ->
    erlang:put(element(1, NewTuple), NewTuple),
    ok.
-endif.
-ifdef(PDB_ETS).
set(NewTuple, TableName) ->
    ets:insert(TableName, NewTuple),
    ok.
-endif.

-spec delete(term(), tableid()) -> ok.
-ifdef(PDB_ERL).
delete(Key, _TableName) ->
    erlang:erase(Key),
    ok.
-endif.
-ifdef(PDB_ETS).
delete(Key, TableName) ->
    ets:delete(TableName, Key),
    ok.
-endif.

-spec tab2list(tableid()) -> [term()].
-ifdef(PDB_ERL).
tab2list(_TableName) ->
    [ X || {_,X} <- erlang:get()].
-endif.
-ifdef(PDB_ETS).
tab2list(TableName) ->
  ets:tab2list(TableName).
-endif.
