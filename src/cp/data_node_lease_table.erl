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
%% @doc    data_node lease-table file
%% @end
%% @version $Id$
-module(data_node_lease_table).
-author('schuett@zib.de').
-vsn('$Id').

-include("scalaris.hrl").

-export([
         % lease table functions
         new/0,
         get_lease/2,
         get_my_leases/1,
         store_lease/3
        ]).

-record(lease_table, {my_leases             :: any(), % persistent???
                      persistent_table      :: any(),
                      volatile_table        :: any()}).
-opaque state() :: #lease_table{}.

-ifdef(with_export_type_support).
-export_type([state/0]).
-endif.

% @todo better definition of pid/owner

%===============================================================================
%
% public lease table functions
%
%===============================================================================

-spec new() -> state().
new() ->
    % ets prefix: leases_ + random name
    RandomName = randoms:getRandomString(),
    DBPName = "leases_p_" ++ RandomName, % persistent
    DBVName = "leases_v_" ++ RandomName, % volatile
    PersistentTable = ets:new(list_to_atom(DBPName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
    VolatileTable   = ets:new(list_to_atom(DBVName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
    #lease_table{my_leases = gb_sets:empty(),
                 persistent_table = PersistentTable,
                 volatile_table = VolatileTable}.

-spec get_lease(state(), data_node_lease:lease_id()) -> unknown | data_node_lease:lease().
get_lease(#lease_table{persistent_table = PersistentTable,
                       volatile_table = VolatileTable}, LeaseId) ->
    case ets:lookup(VolatileTable, LeaseId) of
        [] ->
            unknown;
        [{LeaseId, Version, Timeout}] ->
            case ets:lookup(PersistentTable, LeaseId) of
                [] ->
                    unknown;
                [{LeaseId, Epoch, Owner, Range, Aux}] ->
                    data_node_lease:init(Epoch, Owner, Range, Aux, Version,
                                         Timeout)
            end
    end.

-spec store_lease(state(), data_node_lease:lease_id(), data_node_lease:lease()) -> state().
store_lease(#lease_table{my_leases=MyLeases, persistent_table=PTable,
                         volatile_table=VTable} = LeaseTables,
            LeaseId, Lease) ->
    {Epoch, Owner, Range, Aux, Version, Timeout} = data_node_lease:flatten_lease(Lease),
    ets:insert(PTable, {LeaseId, Epoch, Owner, Range, Aux}),
    ets:insert(VTable, {LeaseId, Version, Timeout}),
    LeaseTables#lease_table{my_leases=gb_sets:add(LeaseId, MyLeases)}.

-spec get_my_leases(state()) -> list(data_node_lease:lease()).
get_my_leases(#lease_table{my_leases=MyLeases} = LeaseTables) ->
    lists:foreach(fun(LeaseId) ->
                          get_lease(LeaseTables, LeaseId)
                  end,
                  MyLeases).
