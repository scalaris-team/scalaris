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
%% @doc Abstract data type of leases for key range responsibility and
%%      operations on lists of leases.
%% @end
%% @version $Id$
-module(leases).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id').
-include("scalaris.hrl").

%% operations for abstract data type 'lease'
-export([new_for_all/0,
         get_id/1,
         get_epoch/1, inc_epoch/1,
         get_owner/1, i_am_owner/1, set_owner_to_self/1,
         get_range/1,
         get_aux/1,
         get_version/1, inc_version/1,
         get_timeout/1, set_timeout/2, update_timeout/1,
         is_valid/1
        ]).
-export([get_persistent_info/1]).

%% operations on lists of the 'lease' type
-export([is_owner/2,  %% has calling process a valid lease for given key
         find_by_id/2]).

-type lease_id() :: ?RT:key().
-record(lease, {
          id      :: lease_id(),
          epoch   :: non_neg_integer(),
          owner   :: comm:mypid(),
          range   :: intervals:interval(),
          aux     :: any(),
          version :: non_neg_integer(),
          timeout :: erlang:timestamp()}).
-type lease() :: #lease{}.
-type leases() :: [lease()].

-ifdef(with_export_type_support).
-export_type([lease/0, lease_id/0, leases/0]).
-endif.

%% operations for abstract data type 'lease'
-spec new_for_all() -> lease().
new_for_all() ->
    Range = intervals:all(),
    #lease{id = ?RT:get_random_node_id(), epoch = 0, owner = comm:this(),
           range = Range, aux = nil, version = 0,
           timeout = calc_timeout(os:timestamp())}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Getter
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_id(lease()) -> lease_id().
get_id(Lease) -> Lease#lease.id.
-spec get_epoch(lease()) -> non_neg_integer().
get_epoch(Lease) -> Lease#lease.epoch.
-spec get_owner(lease()) -> comm:mypid().
get_owner(Lease) -> Lease#lease.owner.
-spec get_range(lease()) -> intervals:interval().
get_range(Lease) -> Lease#lease.range.
-spec get_aux(lease()) -> any().
get_aux(Lease) -> Lease#lease.aux.
-spec get_version(lease()) -> non_neg_integer().
get_version(Lease) -> Lease#lease.version.
-spec get_timeout(lease()) -> erlang:timestamp().
get_timeout(Lease) -> Lease#lease.timeout.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% More complex operations on leases
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec inc_epoch(lease()) -> lease().
inc_epoch(#lease{epoch=Epoch} = Lease) ->
    Lease#lease{epoch=Epoch + 1, version = 1}.

-spec inc_version(lease()) -> lease().
inc_version(#lease{version=Version} = Lease) ->
    Lease#lease{version = Version + 1}.

-spec i_am_owner(lease()) -> boolean().
i_am_owner(#lease{owner=Owner}) ->
    Owner == comm:make_global(pid_groups:get_my(data_node)).

-spec set_owner_to_self(lease()) -> lease().
set_owner_to_self(Lease) ->
    Lease#lease{owner = comm:make_global(pid_groups:get_my(data_node))}.

-spec set_timeout(lease(), erlang:timestamp()) -> lease().
set_timeout(Lease, Timeout) ->
    Lease#lease{timeout = Timeout}.

-spec update_timeout(lease()) -> lease().
update_timeout(Lease) ->
    set_timeout(Lease, calc_timeout(os:timestamp())).


% -spec calc_lease_id(lease()) -> lease_id().
% calc_lease_id(Lease) ->
%     Range = get_range(Lease),
%     {'(', _, UpperBound, ']'} = intervals:get_bounds(Range),
%     UpperBound.
-spec get_persistent_info(lease()) -> {?RT:key(),
                                       non_neg_integer(),
                                       comm:mypid(),
                                       intervals:interval(),
                                       any()}.
get_persistent_info(#lease{id = Id, epoch=Epoch, owner=Owner,
                           range=Range, aux=Aux}) ->
    {Id, Epoch, Owner, Range, Aux}.



%% operations on lists of the 'lease' type

-spec find_by_id(leases(), lease_id()) -> lease() | unknown.
find_by_id(Leases, LeaseId) ->
    case lists:keyfind(LeaseId, #lease.id, Leases) of
        false -> unknown;
        Lease -> Lease
    end.

-spec is_owner(leases(), ?RT:key()) -> boolean().
is_owner(Leases, Key) ->
    lists:any(fun (Lease) ->
                      A = i_am_owner(Lease),
                      B = is_valid(Lease),
                      C = intervals:in(Key, get_range(Lease)),
                      A andalso B andalso C
              end, Leases).



% @doc valid if timeout in future
-spec is_valid(lease()) -> boolean().
is_valid(#lease{timeout=Timeout}) ->
    timer:now_diff(Timeout, precision_time()) > epsilon().

%% private time related functions
% @doc returns the precise current time; could apply delta to erlang:now()
precision_time() ->
    os:timestamp().

-spec calc_timeout(erlang:timestamp()) -> erlang:timestamp().
calc_timeout(Time) ->
    {MegaSecs, Secs, MicroSecs} = Time,
    {MegaSecs, Secs + delta(), MicroSecs}.


% @doc precision of synchronized clocks in microsecs
epsilon() ->
    500 * 1000.

% @doc lifetime of leases in seconds
delta() ->
    30.

