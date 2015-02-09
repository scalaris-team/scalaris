% @copyright 2012-2013 Zuse Institute Berlin,

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
%% @doc leases.
%% @end
%% @version $Id$
-module(leases).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([is_responsible/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Public API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec is_responsible(dht_node_state:state(), ?RT:key()) -> boolean() | maybe.
is_responsible(State, Key) ->
    LeaseList = dht_node_state:get(State, lease_list),
    is_responsible_(LeaseList, Key).

-spec is_responsible_(lease_list:lease_list(), ?RT:key()) -> boolean() | maybe.
is_responsible_(LeaseList, Key) ->
    ActiveLease = lease_list:get_active_lease(LeaseList),
    %PassiveLease = lease_list:get_passive_leases(LeaseList),
    case ActiveLease of
        empty ->
            PassiveLeases = lease_list:get_passive_leases(LeaseList),
            is_responsible_passive_leases(PassiveLeases, Key);
        _ ->
            RangeMatches = intervals:in(Key, l_on_cseq:get_range(ActiveLease)),
            IsAlive = l_on_cseq:is_valid(ActiveLease),
            if
                IsAlive andalso RangeMatches ->
                    true;
                RangeMatches ->
                    %log:log("returned maybe"),
                    maybe;
                true ->
                    %log:log("returned false"),
                    PassiveLeases = lease_list:get_passive_leases(LeaseList),
                    is_responsible_passive_leases(PassiveLeases, Key)
            end
    end.

-spec is_responsible_passive_leases(list(l_on_cseq:lease_t()), ?RT:key()) -> boolean() | maybe.
is_responsible_passive_leases([], _Key) ->
    false;
is_responsible_passive_leases([Lease|Rest], Key) ->
    case intervals:in(Key, l_on_cseq:get_range(Lease)) of
        true ->
            maybe;
        false ->
            is_responsible_passive_leases(Rest, Key)
    end.
