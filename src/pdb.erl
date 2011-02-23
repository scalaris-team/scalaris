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

%% put/get variant
-type tableid() :: atom().
-spec new(TableName::atom(), [set | ordered_set | bag | duplicate_bag |
                              public | protected | private |
                              named_table | {keypos, integer()} |
                              {heir, pid(), term()} | {heir,none} |
                              {write_concurrency, boolean()}]) -> tableid().
new(TableName, _Params) ->
    TableName.

-spec get(term(), atom()) -> tuple() | undefined.
get(Key, _TableName) ->
    erlang:get(Key).

-spec set(tuple(), atom()) -> ok.
set(NewTuple, _TableName) ->
    erlang:put(element(1, NewTuple), NewTuple),
    ok.

-spec delete(term(), atom()) -> ok.
delete(Key, _TableName) ->
    erlang:erase(Key),
    ok.

-spec tab2list(atom()) -> [term()].
tab2list(_TableName) ->
    [ X || {_,X} <- erlang:get()].

%% %% ets variant (only for debugging! has performance issues with msg_delay)
%% -type tableid() :: tid() | atom().
%% new(TableName, Params) ->
%%     Name = ets:new(TableName, Params).
%%
%% get(Key, TableName) ->
%%     case ets:lookup(TableName, Key) of
%%         [] -> undefined;
%%         [Entry] -> Entry
%%     end.
%%
%% set(NewTuple, TableName) ->
%%     ets:insert(TableName, NewTuple),
%%     ok.
%%
%% delete(Key, TableName) ->
%%     ets:delete(TableName, Key),
%%     ok.
%%
%% tab2list(TableName) ->
%%   ets:tab2list(TableName).
