%% @copyright 2018 Zuse Institute Berlin

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
%% @doc JSON API for querying rings.
-module(api_json_ring).
-author('schuett@zib.de').

% for api_json:
-export([get_ring_size/1, wait_for_ring_size/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec get_ring_size(TimeOut::integer()) -> integer() | failed.
get_ring_size(TimeOut) ->
    api_ring:get_ring_size(TimeOut).

-spec wait_for_ring_size(Size::integer(), TimeOut::integer()) -> [char(),...].
wait_for_ring_size(Size, TimeOut) ->
    api_ring:wait_for_ring_size(Size, TimeOut),
    "ok".
