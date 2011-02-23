%  @copyright 2007-2011 Zuse Institute Berlin

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
-module(group_node_trigger).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([trigger/1]).

-spec trigger(group_state:state()) -> group_state:state().
trigger(State) ->
    case group_state:get_view(State) of
        undefined -> State;
        View ->
            case length(group_view:get_members(View)) > config:read(group_max_size)  of
                true ->
                    SplitKey = get_split_key(group_view:get_interval(View)),
                    {LeftGroup, RightGroup} = split_group(group_view:get_members(View)),
                    Proposal = {group_split, comm:this(), SplitKey,
                                LeftGroup, RightGroup},
                    comm:send(comm:this(), {ops, Proposal});
                false ->
                    ok
            end
    end,
    NewState = case group_state:get_mode(State) of
                   joined ->
                       group_rm:trigger(State);
                   _ ->
                       State
               end,
    TriggerState = group_state:get_trigger_state(NewState),
    NewTriggerState = trigger:next(TriggerState),
    group_state:set_trigger_state(NewState, NewTriggerState).

get_split_key(Interval) ->
    {'[', LowerBound, UpperBound, _} = intervals:get_bounds(Interval),
    ?RT:get_split_key(LowerBound, UpperBound, {1, 2}).

-spec split_group(list(comm:mypid())) -> {list(comm:mypid()), list(comm:mypid())}.
split_group([]) ->
    {[], []};
split_group([El]) ->
    {[El], []};
split_group([El1, El2|Rest]) ->
    {LRest, RRest} = split_group(Rest),
    {[El1 | LRest], [El2 | RRest]}.


% @todo we assume that keys behave like numbers (+, -, /)
