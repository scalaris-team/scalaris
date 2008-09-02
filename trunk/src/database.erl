%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : database.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : database behaviour
%%%
%%% Created :  29 Jul 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(database).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     % write locks
     {set_write_lock, 1}, {unset_write_lock, 1},
     % read locks
     {set_read_lock, 1}, {unset_read_lock, 1},
     % locks helper
     {get_locks, 1},
     % standard calls
     {read, 1}, {write, 3}, {get_version, 1},
     %load balancing
     {get_load, 0}, {get_middle_key, 0}, {split_data, 2}, 
     %
     {get_data, 0}, {add_data, 1}, {get_range_with_version, 1}
    ];

behaviour_info(_Other) ->
    undefined.

