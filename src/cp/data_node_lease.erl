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
%% @doc    data_node lease file
%% @end
%% @version $Id$
-module(data_node_lease).
-author('schuett@zib.de').
-vsn('$Id').

-include("scalaris.hrl").

-export([is_owner/2,
         create_first_lease/0,
         get_lease_id/1,
         get_range/1,
         flatten_lease/1,

         % on-handler
         on/2
         ]).

-record(lease, {epoch, owner, range, aux, version, timeout}).
-opaque lease() :: #lease{}.

-type lease_id() :: ?RT:key().

-ifdef(with_export_type_support).
-export_type([lease/0]).
-endif.

% @todo better definition of pid/owner

%===============================================================================
%
% public lease functions
%
%===============================================================================

get_lease_id(Lease) ->
    Range = get_range(Lease),
    {'(', 0, UpperBound, ']'} = intervals:get_bounds(),
    UpperBound.

is_owner(State, Key) ->
    Leases = data_node_state:get_my_leases(State),
    lists:any(fun (Lease) ->
                      i_am_owner(Lease) andalso is_valid(Lease)
              end, Leases).

create_first_lease() ->
    Range = intervals:new('(', 0, 0, ']'),
    Lease= #lease{epoch   = 0, owner   = comm:this(), range = Range, aux = nil,
                  version = 0, timeout = get_timeout(erlang:now())},
    {0, Lease}.

get_range(#lease{range=Range}) ->
    Range.

flatten_lease(#lease{epoch=Epoch, owner=Owner, range=Range, aux=Aux, version=Version,
                     timeout=Timeout}) ->
    {Epoch, Owner, Range, Aux, Version, Timeout}.

%===============================================================================
%
% on handler
%
%===============================================================================

on({renew_lease}, State) ->
    State;

on(Msg, State) ->
    io:format("unknown lease message: ~p~n", [Msg]),
    unknown_event.

%===============================================================================
%
% private functions
%
%===============================================================================

i_am_owner(#lease{owner=Owner}) ->
    Owner == comm:this().

%===============================================================================
%
% private time related functions
%
%===============================================================================
% @doc valid if timeout in future
is_valid(#lease{timeout=Timeout}) ->
    Epsilon = epsilon(),
    timer:now_diff(Timeout, precision_time()) > epsilon().

% @doc returns the precise current time; could apply delta to erlang:now()
precision_time() ->
    os:timestamp().

% @doc precision of synchronized clocks in microsecs
epsilon() ->
    500 * 1000.

% @doc lifetime of leases in seconds
delta() ->
    500 * 1000.

get_timeout({MegaSecs, Secs, MicroSecs}) ->
    {MegaSecs, Secs + delta(), MicroSecs}.
