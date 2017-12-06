%  @copyright 2017 Zuse Institute Berlin

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
%% @doc    the state of rm_tman
%% @end
-module(rm_tman_state).
-author('schuett@zib.de').

-include("scalaris.hrl").

-export([get_neighbors/1, get_randview_size/1, get_cache/1, get_churn/1,
         set_neighbors/2, set_randview_size/2, set_cache/2, set_churn/2,
         init/4]).

-export_type([state/0]).


-opaque state() :: {Neighbors      :: nodelist:neighborhood(),
                    RandomViewSize :: pos_integer(),
                    Cache          :: [node:node_type()], % random cyclon nodes
                    Churn          :: boolean()}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% getter
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_neighbors(state()) -> nodelist:neighborhood().
get_neighbors({Neighbors, _RandViewSize, _Cache, _Churn}) ->
    Neighbors.

-spec get_randview_size(state()) -> pos_integer().
get_randview_size({_Neighbors, RandViewSize, _Cache, _Churn}) ->
    RandViewSize.

-spec get_cache(state()) -> [node:node_type()].
get_cache({_Neighbors, _RandViewSize, Cache, _Churn}) ->
    Cache.

-spec get_churn(state()) -> boolean().
get_churn({_Neighbors, _RandViewSize, _Cache, Churn}) ->
    Churn.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% setter
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec set_neighbors(nodelist:neighborhood(), state()) -> state().
set_neighbors(Neighbors, {_Neighbors2, RandViewSize2, Cache2, Churn2}) ->
    {Neighbors, RandViewSize2, Cache2, Churn2}.

-spec set_randview_size(pos_integer(), state()) -> state().
set_randview_size(RandViewSize, {Neighbors2, _RandViewSize2, Cache2, Churn2}) ->
    {Neighbors2, RandViewSize, Cache2, Churn2}.

-spec set_cache([node:node_type()], state()) -> state().
set_cache(Cache, {Neighbors2, RandViewSize2, _Cache2, Churn2}) ->
    {Neighbors2, RandViewSize2, Cache, Churn2}.

-spec set_churn(boolean(), state()) -> state().
set_churn(Churn, {Neighbors2, RandViewSize2, Cache2, _Churn2}) ->
    {Neighbors2, RandViewSize2, Cache2, Churn}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% misc.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init(Neighbors      :: nodelist:neighborhood(),
          RandomViewSize :: pos_integer(),
          Cache          :: [node:node_type()], % random cyclon nodes
          Churn          :: boolean()) -> state().
init(Neighbors, RandomViewSize, Cache, Churn) ->
    {Neighbors, RandomViewSize, Cache, Churn}.
