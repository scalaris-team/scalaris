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
%% @doc    data_node state file
%% @end
%% @version $Id$
-module(data_node_state).
-author('schuett@zib.de').
-vsn('$Id').

-include("scalaris.hrl").

-export([new/0,
         % db
         db_read/2,
         db_write/3,
         % leases
         is_owner/2,
         get_my_leases/1,
         store_lease/3
         ]).

-ifdef(with_export_type_support).
-export_type([state/0]).
-endif.

-record(state, {lease_table      :: data_node_lease_table:state(),
                db_table         :: data_node_db_table:state()}).
-opaque state() :: #state{}.

%===============================================================================
%
% leases
%
%===============================================================================
-spec is_owner(state(), ?RT:key()) -> boolean().
is_owner(State, HashedKey) ->
    data_node_lease:is_owner(State, HashedKey).

-spec get_my_leases(state()) -> list(data_node_lease:lease()).
get_my_leases(#state{lease_table=Table}) ->
    data_node_lease_table:get_my_leases(Table).

-spec store_lease(state(), data_node_lease:lease_id(), data_node_lease:lease()) -> state().
store_lease(#state{lease_table=Table} = State, LeaseId, Lease) ->
    State#state{lease_table = data_node_lease_table:store_lease(Table, LeaseId, Lease)}.

%===============================================================================
%
% database
%
%===============================================================================
db_read(#state{db_table=DBTable}, HashedKey) ->
    data_node_db_table:read(DBTable, HashedKey).

db_write(#state{db_table=DBTable}, HashedKey, Value) ->
    data_node_db_table:write(DBTable, HashedKey, Value).

%===============================================================================
%
% init and misc.
%
%===============================================================================
-spec new() -> state().
new() ->
    #state{lease_table = data_node_lease_table:new(),
           db_table    = data_node_db_table:new()}.
