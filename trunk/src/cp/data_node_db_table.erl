%  @copyright 2007-2012 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    data_node database file
%% @end
%% @version $Id$
-module(data_node_db_table).
-author('schuett@zib.de').
-vsn('$Id').

-include("scalaris.hrl").

-export([new/0,
         read/2
         ]).

-ifdef(with_export_type_support).
-export_type([state/0]).
-endif.

-record(state, {db_table         :: any()}).
-opaque state() :: #state{}.

-spec new() -> state().
new() ->
    RandomName = randoms:getRandomString(),
    DBName = "db_" ++ RandomName,
    DBTable = ets:new(list_to_atom(DBName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
    #state{db_table=DBTable}.

% @todo
-spec read(state(), ?RT:key()) -> unknown | any().
read(#state{db_table=DBTable}, HashedKey) ->
    %read(DBTable, HashedKey).
    ok.

% @todo
% - ets -> dets
