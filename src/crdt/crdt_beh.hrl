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

-author('skrzypczak@zib.de').

-export_type([crdt/0]).

-export([new/0, update/2, query/2, merge/2, eq/2, lt/2, lteq/2]).

-spec update(crdt(), crdt:update_fun()) -> crdt().
update(CRDT, U) -> U(CRDT).

-spec query(crdt(), crdt:query_fun()) -> crdt().
query(CRDT, Q) -> Q(CRDT).

-spec lteq(crdt(), crdt())  -> boolean().
lteq(CRDT1, CRDT2) -> lt(CRDT1, CRDT2) orelse eq(CRDT1, CRDT2).


