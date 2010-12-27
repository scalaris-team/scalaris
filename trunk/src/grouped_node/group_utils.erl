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

-export([notify_neighbors/1, notify_neighbors/2, notify_neighbors/3]).

-spec notify_neighbors(NodeState::group_local_state:local_state(),
                       OldView::group_view:view(),
                       NewView::group_view:view()) -> ok.
notify_neighbors(NodeState, OldView, NewView) ->
    OldGroupNode = group_view:get_group_node(OldView),
    NewGroupNode = group_view:get_group_node(NewView),
    case OldGroupNode =:= NewGroupNode of
        true ->
            ok;
        false ->
            notify_neighbors(NodeState, NewView)
    end.

-spec notify_neighbors(NodeState::group_local_state:local_state(),
                       View::group_view:view()) -> ok.
notify_neighbors(NodeState, View) ->
    GroupNode = group_view:get_group_node(View),
    {_, _, _, Preds} = group_local_state:get_predecessor(NodeState),
    {_, _, _, Succs} = group_local_state:get_successor(NodeState),
    _ = [comm:send(P, {succ_update, GroupNode}) || P <- Preds],
    _ = [comm:send(P, {pred_update, GroupNode}) || P <- Succs],
    ok.

-spec notify_neighbors(State::group_state:state()) -> ok.
notify_neighbors(State) ->
    NodeState = group_state:get_node_state(State),
    View = group_state:get_view(State),
    notify_neighbors(NodeState, View).
