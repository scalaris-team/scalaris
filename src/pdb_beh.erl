%% @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Behaviour for pdb (see pdb.erl and pdb_ets.erl)
%% @end
%% @version $Id$
-module(pdb_beh).
-author('schintke@zib.de').
-vsn('$Id$').

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-type tableid() :: term().

-callback new(TableName::atom(),
              [set | ordered_set | bag | duplicate_bag |
               public | protected | private |
               named_table | {keypos, integer()} |
               {heir, pid(), term()} | {heir,none} |
               {write_concurrency, boolean()}]) -> tableid().

-callback get(term(), tableid()) -> tuple() | undefined.
-callback set(tuple(), tableid()) -> ok.
-callback delete(term(), tableid()) -> ok.
-callback take(term(), tableid()) -> term() | undefined.
-callback tab2list(tableid()) -> [term()].

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [ {new, 2},
      {get, 2},
      {set, 2},
      {delete, 2},
      {take, 2},
      {tab2list, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.

