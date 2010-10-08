%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc
%% @end
%% @version $Id$
-module(group_utils).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("group.hrl").

-export([notify_neighbors/3]).

-spec notify_neighbors(NodeState::group_local_state:local_state(),
                       OldGroupState::group_state:group_state(),
                       NewGroupState::group_state:group_state()) -> ok.
notify_neighbors(NodeState, OldGroupState, NewGroupState) ->
    OldGroupNode = group_state:get_group_node(OldGroupState),
    NewGroupNode = group_state:get_group_node(NewGroupState),
    case OldGroupNode == NewGroupNode of
        true ->
            ok;
        false ->
            {_, _, Preds} = group_local_state:get_predecessor(NodeState),
            {_, _, Succs} = group_local_state:get_successor(NodeState),
            [comm:send(P, {succ_update, NewGroupNode}) || P <- Preds],
            [comm:send(P, {pred_update, NewGroupNode}) || P <- Succs],
            ok
    end.
