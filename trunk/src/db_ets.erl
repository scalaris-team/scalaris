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

%% @doc There is nothing to do once per VM with ets.
start_per_vm() ->
    ok.

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

%% @doc Returns all DB entries.
get_data(DB) ->
    ets:tab2list(DB).

-define(ETS, ets).
-include("db_generic_ets.hrl").
