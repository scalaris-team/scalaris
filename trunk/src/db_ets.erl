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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc    In-process database using ets
%% @end
%% @version $Id$
-module(db_ets).
-author('schintke@onscale.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).
-opaque(db() :: {Table::tid() | atom(), RecordChangesInterval::intervals:interval(), ChangedKeysTable::tid() | atom()}).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-define(CKETS, ets).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc initializes a new database; returns the DB name.
new(_NodeId) ->
    % ets prefix: DB_ + random name
    RandomName = randoms:getRandomId(),
    DBname = list_to_atom(string:concat("db_", RandomName)),
    CKDBname = list_to_atom(string:concat("db_ck_", RandomName)), % changed keys
    % better protected? All accesses would have to go to DB-process
    {ets:new(DBname, [ordered_set | ?DB_ETS_ADDITIONAL_OPS]), intervals:empty(), ?CKETS:new(CKDBname, [ordered_set | ?DB_ETS_ADDITIONAL_OPS])}
%%     ets:new(DBname, [ordered_set, protected, named_table])
%%     ets:new(DBname, [ordered_set, private, named_table])
.

%% delete DB (missing function)
close({DB, _CKInt, CKDB}) ->
    ets:delete(DB),
    ?CKETS:delete(CKDB).

%% @doc Returns all DB entries.
get_data({DB, _CKInt, _CKDB}) ->
    ets:tab2list(DB).

-define(ETS, ets).
-include("db_generic_ets.hrl").
