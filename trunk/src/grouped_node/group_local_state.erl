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
-include("group.hrl").

-export([new/2,
        get_predecessor/1, get_successor/1,
        update_pred_succ/4, update_pred/3, update_succ/3]).

-ifdef(with_export_type_support).
-export_type([local_state/0]).
-endif.

-record(local_state, {successor::group_types:group_node(),
                      predecessor::group_types:group_node(),
                      rt}).

-opaque local_state() :: #local_state{}.

-define(IF(P, S), if P -> S; true -> ok end).

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
                       group_types:group_node(), intervals:interval()) -> local_state().
% @doc update pred and succ if newer
update_pred_succ(NodeState, Pred, Succ, Range) ->
    update_pred(update_succ(NodeState, Succ, Range), Pred, Range).

-spec update_succ(local_state(), group_types:group_node(),
                  intervals:interval()) -> local_state().
% @doc update succ if newer
update_succ(NodeState, Succ, MyRange) ->
    {NewGroupId, SuccRange, NewVersion, _NewMembers} = Succ,
    {OldGroupId, OldSuccRange, OldVersion, _OldMembers} = get_successor(NodeState),
    case MyRange =:= group_types:all() of
        true ->
            % special case when we cover everything
            case is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion) andalso
                MyRange =:= SuccRange of
                true ->
                    NodeState#local_state{successor=Succ};
                false ->
                    NodeState
            end;
        false ->
            case intervals:is_left_of(MyRange, SuccRange) andalso
                (is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion)
                 orelse
                 not intervals:is_left_of(MyRange, OldSuccRange))
                of
                true ->
                    P1 = intervals:is_left_of(MyRange, SuccRange),
                    P2 = is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion),
                    P3 = not intervals:is_left_of(MyRange, OldSuccRange),
                    case MyRange =:= SuccRange of
                        true -> ok;
                        false -> ?LOG("me: ~p is my succ: ~p ~p~n", [MyRange, SuccRange, {P1, P2, P3}])
                    end,
                    NodeState#local_state{successor=Succ};
                false ->
                    P1 = intervals:is_left_of(MyRange, SuccRange),
                    P2 = is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion),
                    P3 = not intervals:is_left_of(MyRange, OldSuccRange),
                    case {OldGroupId, OldVersion} =:= {NewGroupId, NewVersion} of
                        true -> ok;
                        false -> ?LOG("me: ~p not a succ: ~p ~p~n", [MyRange, SuccRange, {P1, P2, P3}])
                    end,
                    NodeState
            end
    end.

-spec update_pred(local_state(), group_types:group_node(),
                  intervals:interval()) -> local_state().
% @doc update pred if newer
update_pred(NodeState, Pred, MyRange) ->
    {NewGroupId, PredRange, NewVersion, _NewMembers} = Pred,
    {OldGroupId, OldPredRange, OldVersion, _OldMembers} = get_predecessor(NodeState),
    case MyRange =:= group_types:all() of
        true ->
            % special case when we cover everything
            case is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion) andalso
                MyRange =:= PredRange of
                true ->
                    NodeState#local_state{predecessor=Pred};
                false ->
                    NodeState
            end;
        false ->
            case
                intervals:is_left_of(PredRange, MyRange) andalso
                (is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion)
                 orelse
                 not intervals:is_left_of(OldPredRange, MyRange))
                of
                true ->
                    NodeState#local_state{predecessor=Pred};
                false ->
                    %io:format("not a pred: ~p me: ~p~n", [PredRange, MyRange]),
                    NodeState
            end
    end.

is_newer(OldGroupId, OldVersion, NewGroupId, NewVersion) ->
    % @todo this is not correct!
    (OldGroupId == NewGroupId andalso NewVersion > OldVersion) orelse
        OldGroupId < NewGroupId.
