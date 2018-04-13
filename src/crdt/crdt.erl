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
%% @doc Some common type defs and functions for CRDTs.
%% @end
-module(crdt).
-author('skrzypczak@zib.de').

-export_type([crdt/0, update_fun/0, query_fun/0]).

-export([update_noop/1, update_noop/2, query_noop/1]).

% for tester
-export([tester_create_update_fun/1, tester_create_query_fun/1]).

-type crdt()        :: term().
-type update_fun()  :: fun((crdt()) -> crdt()) | fun((non_neg_integer(), crdt()) -> crdt()).
-type query_fun()   :: fun((crdt()) -> term()).

-spec update_noop(crdt()) -> crdt().
update_noop(CRDT) -> CRDT.

-spec update_noop(non_neg_integer(), crdt()) -> crdt().
update_noop(_ReplicaId, CRDT) -> CRDT.

-spec query_noop(crdt()) -> term().
query_noop(CRDT) -> CRDT.

-spec tester_create_update_fun(0) -> update_fun().
tester_create_update_fun(0) -> fun update_noop/1.

-spec tester_create_query_fun(0) -> query_fun().
tester_create_query_fun(0) -> fun query_noop/1.
