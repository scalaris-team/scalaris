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
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).

-type(db()::atom()).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the tcerl service (needed by tcerl).
-ifdef(have_tcerl).
start_per_vm() ->
    tcerl:start().
-else.
start_per_vm() ->
    erlang:error('tcerl_not_available').
-endif.

%% @doc initializes a new database; returns the DB name.
new(Id) ->
    Dir = lists:flatten(io_lib:format("~s/~s", [config:read(db_directory),
                                  atom_to_list(node())])),
    file:make_dir(Dir),
    FileName = lists:flatten(io_lib:format("~s/db_~p.tcb", [Dir, Id])),
    case tcbdbets:open_file([{file, FileName}, truncate]) of
        {ok, Handle} ->
            Handle;
        {error, Reason} ->
            log:log(error, "[ TCERL ] ~p", [Reason])
    end.

%% @doc Deletes all contents of the given DB.
close(DB) ->
    tcbdbets:delete_all_objects(DB),
    tcbdbets:close(DB).

%% @doc Returns all DB entries.
get_data(DB) ->
    tcbdbets:traverse(DB, fun(X) -> {continue, X} end).

-define(ETS, tcbdbets).
-include("db_generic_ets.hrl").
