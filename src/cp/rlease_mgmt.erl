%  @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    data_node replicated lease storage and management
%% @end
%% @version $Id$
-module(rlease_mgmt).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id').
-include("scalaris.hrl").

%% part of data_node on-handler for lease management
-export([on/2]).
-type msg() :: {rlease_mgmt, {renew_lease}}.

%% operations on rlease_mgmt state
-export([new_state/0,   %% create new state
         get_rlease/3,  %% get a lease by its lease_id
         store_rlease/3 %% store a lease (replaced, when id already exists)
        ]).

-type nth() :: non_neg_integer().
-record(rlease, {
          key :: {?RT:key(), nth()}, %% key, nth replica
          lease :: leases:lease()
         }).
-type rlease() :: #rlease{}.
-record(rleasedb, {
          rleases :: [rlease()],
          persistent :: ?DB:db()
         }).
-opaque state() :: #rleasedb{}.

-ifdef(with_export_type_support).
-export_type([msg/0, state/0]).
-endif.

-spec new_state() -> state().
new_state() ->
    % ets prefix: leases_ + random name
    RandomName = randoms:getRandomString(),
    DBPName = "leases_p_" ++ RandomName, % persistent
    PersistentTable = ets:new(list_to_atom(DBPName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
    #rleasedb{rleases = [],
              persistent = PersistentTable}.

%% TODO: replace by quorum read on replicated lease stores
-spec get_rlease(state(), leases:lease_id(), nth()) -> rlease() | unknown.
get_rlease(#rleasedb{rleases = RLeases}, LeaseId, ReplNo) ->
    case lists:keyfind({LeaseId, ReplNo}, #rlease.key, RLeases) of
        false -> unknown;
        RLease -> RLease
    end.

%% TODO: replace by paxos on replicated lease stores
-spec store_rlease(state(), leases:lease(), nth()) -> state().
store_rlease(RLeases, Lease, ReplNo) ->
    VTable = RLeases#rleasedb.rleases,
    PTable = RLeases#rleasedb.persistent,
    LeaseId = leases:get_id(Lease),
    LeaseEpoch = leases:get_epoch(Lease),
    RId = {LeaseId, ReplNo},
    RLease = {RId, Lease},
    PLease = {RId, leases:get_persistent_info(Lease)},
    case lists:keyfind(RId, #rleasedb.rleases, VTable) of
        false -> %% lease is new
            ets:insert(PTable, PLease),
            RLeases#rleasedb{rleases=[RLease | VTable]};
        FoundLease ->
            case leases:get_epoch(FoundLease#rlease.lease) of
                LeaseEpoch ->
                    %% epoch is unchanged -> update in ram
                    NewRLeases = lists:keyreplace(RId, #rleasedb.rleases, VTable, RLease),
                    RLeases#rleasedb{rleases=NewRLeases};
                _ ->
                    %% epoch is changed -> write to disk
                    ets:insert(PTable, PLease),
                    NewRLeases = lists:keyreplace(RId, #rleasedb.rleases, VTable, RLease),
                    RLeases#rleasedb{rleases=NewRLeases}
            end
    end.

%% %% scan DB for own lease entries (used on recovery)
%% -spec get_my_leases(state()) -> [rlease()].
%% get_my_leases(#rleasedb{rleases=MyLeases}) -> MyLeases.

-spec on(msg(), data_node:state()) -> data_node:state().
on({rlease_mgmt, {renew_lease}}, State) ->
    State.
