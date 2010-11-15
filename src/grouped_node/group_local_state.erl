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
%% @doc    grouped_local_state
%% @end
%% @version $Id$
-module(group_local_state).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new/2,
        get_predecessor/1, get_successor/1,
        update_pred_succ/4, update_pred/3, update_succ/3]).

-record(local_state, {successor::group_types:group_node(),
                      predecessor::group_types:group_node(),
                      rt}).

-opaque local_state() :: #local_state{}.

-spec new(Pred::group_types:group_node(), Succ::group_types:group_node()) ->
    local_state().
new(Pred, Succ) ->
    #local_state{
     predecessor = Pred,
     successor = Succ,
     rt = none}.

-spec get_predecessor(State::local_state()) -> group_types:group_node().
get_predecessor(#local_state{predecessor=Predecessor}) ->
    Predecessor.

-spec get_successor(State::local_state()) -> group_types:group_node().
get_successor(#local_state{successor=Successor}) ->
    Successor.

-spec update_pred_succ(local_state(), group_types:group_node(),
                       group_types:group_node(), Range::intervals:interval()) -> local_state().
update_pred_succ(NodeState, Pred, Succ, Range) ->
    case Pred == Succ of
        true ->
            ct:pal("WARNING");
        false ->
            ok
    end,
    update_pred(update_succ(NodeState, Succ, Range), Pred, Range).

-spec update_succ(local_state(), group_types:group_node(), Range::intervals:interval()) -> local_state().
update_succ(NodeState, {NewGroupId, NewRange, NewVersion, _NewMembers} = Succ, Range) ->
    {OldGroupId, OldRange, OldVersion, _OldMembers} = get_successor(NodeState),
    case
        (intervals:is_left_of(Range, NewRange) andalso
        is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion))
        orelse
        not intervals:is_right_of(Range, OldRange)
        of
        true ->
            NodeState#local_state{successor=Succ};
        false ->
            NodeState
    end.

-spec update_pred(local_state(), group_types:group_node(), Range::intervals:interval()) -> local_state().
update_pred(NodeState, {NewGroupId, NewRange, NewVersion, _NewMembers} = Pred, Range) ->
    {OldGroupId, OldRange, OldVersion, _OldMembers} = get_predecessor(NodeState),
    case
        (intervals:is_right_of(Range, NewRange) andalso
        is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion))
        orelse
        not intervals:is_right_of(Range, OldRange)
        of
        true ->
            NodeState#local_state{predecessor=Pred};
        false ->
            NodeState
    end.

is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion) ->
    (OldGroupId == NewGroupId andalso NewVersion > OldVersion) orelse
        OldGroupId < NewGroupId.
