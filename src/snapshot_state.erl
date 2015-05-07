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

%% @author Stefan Keidel <keidel@informatik.hu-berlin.de>
%% @doc Local state information needed for the S3 snapshot algorithm
%% @version $Id$
-module(snapshot_state).
-author('keidel@informatik.hu-berlin.de').
-vsn('$Id$').

-export([new/0, new/3, get_number/1, is_in_progress/1, get_leaders/1,
         set_number/2, add_leader/2, start_progress/1, stop_progress/1]).

-export_type([snapshot_state/0]).

-type(snapshot_state() :: {SnapNo::non_neg_integer(), InProgress::boolean(), Leaders::[comm:mypid()]}).

% constructors

-spec new() -> snapshot_state().
new() ->
    erlang:put(local_snap_number, 0),
    {0, false, []}.

-spec new(non_neg_integer(), boolean(), [comm:mypid() | none]) -> snapshot_state().
new(Number, InProgress, Leaders) ->
    erlang:put(local_snap_number, Number),
    {Number, InProgress, [X || X <- Leaders, X =/= none]}.

% getters

-spec get_number(snapshot_state()) -> non_neg_integer().
get_number({Number, _, _}) -> Number.

-spec is_in_progress(snapshot_state()) -> boolean().
is_in_progress({_, InProgress, _}) -> InProgress.

-spec get_leaders(snapshot_state()) -> [comm:mypid()].
get_leaders({_, _, Leaders}) -> Leaders.

% setters

-spec set_number(snapshot_state(), non_neg_integer()) -> snapshot_state().
set_number(SnapInfo, NewVal) ->
    erlang:put(local_snap_number, NewVal),
    setelement(1, SnapInfo, NewVal).

-spec add_leader(snapshot_state(), comm:mypid() | none) -> snapshot_state().
add_leader(State, none) -> State;
add_leader({Number, InProgress, Leaders}, NewLeader) ->
    {Number, InProgress, [NewLeader | Leaders]}.

-spec start_progress(snapshot_state()) -> snapshot_state().
start_progress(SnapInfo) -> setelement(2, SnapInfo, true).

-spec stop_progress(snapshot_state()) -> snapshot_state().
stop_progress(SnapInfo) -> setelement(2, SnapInfo, false).
