% @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
%            2009 onScale solutions

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

%%% @author Florian Schintke <schintke@onscale.de>
%%% @doc    In-process database using ets
%%% @end

%% @version $Id$
-module(db_ets).
-author('schintke@onscale.de').
-vsn('$Id').

-include("scalaris.hrl").

-behaviour(db_beh).
-type(db()::atom()).

% note: must include this file AFTER the type definitions for erlang < R13B04
% to work 
-include("db.hrl").

-export([start_link/1]).
-export([new/1, close/1]).
-export([get_entry/2, set_entry/2]).
-export([read/2, write/4, get_version/2]).
-export([delete/2]).
-export([set_write_lock/2, unset_write_lock/2,
         set_read_lock/2, unset_read_lock/2, get_locks/2]).
-export([get_range/3, get_range_with_version/2]).
-export([get_load/1, get_middle_key/1, split_data/3, get_data/1,
         add_data/2]).
-export([get_range_only_with_version/2,
         build_merkle_tree/2,
         update_if_newer/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(instanceid()) -> ignore.
start_link(_InstanceId) ->
    ignore.

%% @doc initializes a new database; returns the DB name.
new(_) ->
    % ets prefix: DB_ + random name
    DBname = list_to_atom(string:concat("db_", randoms:getRandomId())),
    % better protected? All accesses would have to go to DB-process
     ets:new(DBname, [ordered_set, protected, named_table]).
    %ets:new(DBname, [ordered_set, private, named_table]).

%% delete DB (missing function)
close(DB) ->
    ets:delete(DB).

%% @doc returns all keys
get_data(DB) ->
    ets:tab2list(DB).

build_merkle_tree(DB, Range) ->
    {From, To} = intervals:unpack(Range),
    MerkleTree = lists:foldl(fun ({Key, {_, _, _, _Version}}, Tree) ->
                                     case util:is_between(From, Key, To) of
                                         true ->
                                             merkerl:insert({Key, 0}, Tree);
                                         false ->
                                             Tree
                                     end
                             end,
                             undefined, ets:tab2list(DB)),
    MerkleTree.

-define(ETS, ets).
-include("db_generic_ets.hrl").
