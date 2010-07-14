%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    database behaviour
%%% @end

%% @version $Id$
-module(db_beh).
-author('schuett@zib.de').
-vsn('$Id$').

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     % init (node identifier)
     {new, 1},
     % close, delete?
     {close, 1},
     % write locks
     {set_write_lock, 2}, {unset_write_lock, 2},
     % read locks
     {set_read_lock, 2}, {unset_read_lock, 2},
     % locks helper
     {get_locks, 2},
     % standard calls
     {read, 2}, {write, 4}, {get_version, 2},
     {get_entry, 2}, {set_entry, 2}, {update_entry, 2}, {delete_entry, 2},
     % dangerous calls
     {delete, 2},
     %load balancing
     {get_load, 1}, {get_middle_key, 1}, {split_data, 2},
     %
     {get_data, 1}, {add_data, 2},
     {get_range, 2}, {get_range_with_version, 2},
     {get_range_only_with_version, 2},
     % bulk op for replica repair
     {update_if_newer, 2}
    ];

behaviour_info(_Other) ->
    undefined.

