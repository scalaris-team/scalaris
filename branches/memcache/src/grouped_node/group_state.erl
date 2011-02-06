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
%% @doc    grouped_node main file
%% @end
%% @version $Id$
-module(group_state).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("group.hrl").

-type(mode_type() :: joining | joining_sent_request | joined).

-record(state, {mode::mode_type(),
                node_state::group_local_state:local_state(),  % succ, pred, rt, ...
                view::group_view:view(),                      % members, version, etc
                db_state::group_db:state(),                   % the local DB
                %wheels_of_fortune::group_wof:state(),
                trigger_state::trigger:state()}).
-opaque state() :: #state{}.

-ifdef(with_export_type_support).
-export_type([state/0]).
-endif.

-export([set_db/2, set_mode/2, set_view/2, set_node_state/2, set_trigger_state/2,
        get_db/1, get_mode/1, get_view/1, get_node_state/1, get_trigger_state/1,
        new_replica/1, new_primary/4]).

-spec get_db(state()) -> group_db:state().
get_db(State) ->
    State#state.db_state.

-spec set_db(state(), group_db:state()) -> state().
set_db(State, DB) ->
    State#state{db_state = DB}.

-spec get_mode(state()) -> mode_type().
get_mode(State) ->
    State#state.mode.

-spec set_mode(state(), mode_type()) -> state().
set_mode(State, Mode) ->
    State#state{mode = Mode}.

-spec get_view(state()) -> group_view:view().
get_view(State) ->
    State#state.view.

-spec set_view(state(), group_view:view()) -> state().
set_view(State, View) ->
    State#state{view = View}.

-spec get_node_state(state()) -> group_local_state:local_state().
get_node_state(State) ->
    State#state.node_state.

-spec set_node_state(state(), group_local_state:local_state()) -> state().
set_node_state(State, NodeState) ->
    State#state{node_state=NodeState}.

-spec get_trigger_state(state()) -> trigger:state().
get_trigger_state(State) ->
    State#state.trigger_state.

-spec set_trigger_state(state(), trigger:state()) -> state().
set_trigger_state(State, TriggerState) ->
    State#state{trigger_state=TriggerState}.

-spec new_replica(trigger:state()) -> state().
new_replica(TriggerState) ->
    #state{mode=joining, trigger_state=TriggerState}.

-spec new_primary(group_local_state:local_state(), group_view:view(),
                  group_db:state(), trigger:state()) -> state().
new_primary(NodeState, View, DB, TriggerState) ->
    #state{mode=joined, node_state=NodeState, view=View, db_state=DB,
           trigger_state=TriggerState}.
