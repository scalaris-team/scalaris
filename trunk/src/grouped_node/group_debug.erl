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
%% @version $Id$
-module(group_debug).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("group.hrl").

-export([dbg/0, dbg_version/0, dbg3/0, dbg_mode/0]).

-spec dbg() -> any().
dbg() ->
    gb_trees:to_list(lists:foldl(fun (Node, Stats) ->
                        case gen_component:get_state(Node) of
                            {joined, _LocalState, GroupState, _TriggerState} ->
                                inc(Stats, group_state:get_group_id(GroupState));
                            _ ->
                                Stats
                        end
                end, gb_trees:empty(), pid_groups:find_all(group_node))).

inc(Stats, Label) ->
    case gb_trees:lookup(Label, Stats) of
        {value, Counter} ->
            gb_trees:update(Label, Counter + 1, Stats);
        none ->
            gb_trees:insert(Label, 1, Stats)
    end.

-spec dbg_version() -> any().
dbg_version() ->
    lists:map(fun (Node) ->
                      case gen_component:get_state(Node) of
                          {joined, _LocalState, GroupState, _TriggerState} ->
                              {Node, group_state:get_current_paxos_id(GroupState)};
                          _ ->
                              {Node, '_'}
                      end
              end, pid_groups:find_all(group_node)).

-spec dbg3() -> any().
dbg3() ->
    lists:map(fun (Node) ->
                      case gen_component:get_state(Node) of
                          {joined, _LocalState, GroupState, _TriggerState} ->
                              {Node, length(group_state:get_postponed_decisions(GroupState))};
                          _ ->
                              {Node, '_'}
                      end
              end, pid_groups:find_all(group_node)).

-spec dbg_mode() -> any().
dbg_mode() ->
    lists:map(fun (Node) ->
                      {Node, element(1, gen_component:get_state(Node))}
              end, pid_groups:find_all(group_node)).
