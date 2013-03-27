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
-module(snapshot_state).
-author('keidel@informatik.hu-berlin.de').

-export([init_participant/0,
		 init_leader/0,
		 get_snap_number_leader/1,
		 get_snap_number_part/1,
		 set_snap_number_leader/2,
		 set_snap_number_part/2]).

-ifdef(with_export_type_support).
-export_type([snap_part_info/0,snap_leader_info/0]).
-endif.

% record specs

-record(s3_state, {
	number = 0 :: non_neg_integer(),
	in_progress = 'false' :: boolean(),
	leaders = [] :: list()
}).

-record(s3_leader_state, {
	snapshots = [] :: list(),
	in_progress = 'false' :: boolean(),
	number = 0 :: non_neg_integer()
}).

% type specs

-type snap_part_info() :: s3_state.
-type snap_leader_info() :: s3_leader_state.

% basic functions

-spec init_participant() -> snap_part_info().
init_participant() -> #s3_state{}.

-spec init_leader() -> snap_leader_info().
init_leader() -> #s3_leader_state{}.

% getters

-spec get_snap_number_leader(SnapLeaderInfo :: snap_leader_info()) -> non_neg_integer().
get_snap_number_leader(SnapLeaderInfo) -> SnapLeaderInfo#s3_leader_state.number.

-spec get_snap_number_part(SnapPartInfo :: snap_part_info()) -> non_neg_integer().
get_snap_number_part(SnapPartInfo) -> SnapPartInfo#s3_leader_state.number.

% setters

-spec set_snap_number_leader(SnapLeaderInfo :: snap_leader_info(), NewVal :: non_neg_integer()) -> snap_leader_info().
set_snap_number_leader(SnapLeaderInfo,NewVal) -> SnapLeaderInfo#s3_leader_state{number=NewVal}.

-spec set_snap_number_part(SnapPartInfo :: snap_part_info(), NewVal :: non_neg_integer()) -> snap_part_info().
set_snap_number_part(SnapPartInfo,NewVal) -> SnapPartInfo#s3_leader_state{number=NewVal}.

