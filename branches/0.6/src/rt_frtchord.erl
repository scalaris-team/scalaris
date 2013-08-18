% @copyright 2012 Zuse Institute Berlin

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

%% @author Magnus Mueller <mamuelle@informatik.hu-berlin.de>
%% @doc A flexible routing table algorithm as presented in (Nagao, Shudo, 2011)
%% @end
%% @version $Id$

-module(rt_frtchord).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

% Additional information appended to an rt_frt_helpers:rt_entry()
-type rt_entry_info_t() :: undefined.

% Include after type definitions for R13
-include("rt_frt_common.hrl").

-spec allowed_nodes(RT :: rt()) -> [rt_entry()].
allowed_nodes(RT) ->
    [N || N <- rt_get_nodes(RT), not is_sticky(N) and not is_source(N)].

-spec rt_entry_info(Node :: node:node_type(), Type :: entry_type(),
                    PredId :: key_t(), SuccId :: key_t()) -> rt_entry_info_t().
rt_entry_info(_Node, _Type, _PredId, _SuccId) ->
    undefined.

%% @doc Checks whether config parameters of the rt_frtchord process exist and are
%%      valid.
-spec frt_check_config() -> boolean().
frt_check_config() -> true.
