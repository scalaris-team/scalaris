%  @copyright 2008-2018 Zuse Institute Berlin

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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc State-based CRDT behaviour
%% @end
%%
-module(crdt_beh).
-author('skrzypczak@zib.de').

-ifdef(have_callback_support).
-include("scalaris.hrl").

-type crdt() :: term().

-callback new() -> crdt().

-callback apply_update(crdt:update_fun(), non_neg_integer(), crdt()) -> crdt().
-callback apply_query(crdt:update_fun(), crdt()) -> crdt().
-callback merge(crdt(), crdt()) -> crdt().

-callback eq(crdt(), crdt()) -> boolean().
-callback lt(crdt(), crdt()) -> boolean().
-callback lteq(crdt(), crdt()) -> boolean().

-else.

-export([behaviour_info/1]).
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
        {new, 0},
        {apply_update, 3}, {apply_query, 2},
        {merge, 2},
        {eq, 2}, {lt, 2}, {lteq, 2}
    ];
behaviour_info(_Other) ->
    undefined.

-endif.

