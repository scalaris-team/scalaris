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

-export([new/0, apply_update/3, apply_query/2, merge/2, eq/2, lt/2, lteq/2]).

-spec apply_update(crdt:update_fun(), non_neg_integer(), crdt()) -> crdt().
apply_update(U, ReplicaId, CRDT) ->
    case erlang:fun_info(U, arity) of
        {arity, 1} -> U(CRDT);
        {arity, 2} -> U(ReplicaId, CRDT)
    end.

-spec apply_query(crdt:query_fun(), crdt()) -> crdt().
apply_query(Q, CRDT) -> Q(CRDT).

-spec lteq(crdt(), crdt())  -> boolean().
lteq(CRDT1, CRDT2) -> lt(CRDT1, CRDT2) orelse eq(CRDT1, CRDT2).


