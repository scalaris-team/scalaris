%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    In-process Database using tcerl
%%% @end
-module(db_tcerl).
-author('schuett@zib.de').
-vsn('$Id').

-include("../include/scalaris.hrl").
-include("db.hrl").

-behaviour(db_beh).

-type(db()::atom()).

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
new(Id) ->
    Dir = lists:flatten(io_lib:format("~s/~s", [config:read(db_directory),
                                  atom_to_list(node())])),
    file:make_dir(Dir),
    FileName = lists:flatten(io_lib:format("~s/db_~p.tc", [Dir, Id])),
    case tcbdbets:open_file([{file, FileName}, truncate]) of
        {ok, Handle} ->
            Handle;
        {error, Reason} ->
            log:log(error, "[ TCERL ] ~p", [Reason])
    end.

%% delete DB (missing function)
close(DB) ->
    tcbdbets:close(DB).

%% @doc returns all keys
get_data(DB) ->
    tcbdbets:traverse(DB, fun (X) -> {continue, X} end).

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

%%====================================================================
%% internal functions
%%====================================================================

-define(ETS, tcbdbets).
-include("db_generic_ets.hrl").

