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
%% @doc    Replicated-state machine
%% @end
%% @version $Id$
-module(rsm_state).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new_primary/2, new_replica/2,
         set_mode/2,
         get_rsm_pid/1,
         get_view/1, set_view/2,
         get_app_state/1, set_app_state/2,
         get_app_module/1]).

-type(mode_type() :: joining | joining_sent_request | joined).
-type(rsm_pid_type() :: comm:mypid() | list(comm:mypid())).

-record(state, {mode::mode_type(),
                view::rsm_view:view_type(),
                rsm_pid::rsm_pid_type(),
                app_module::module(),
                app_state::any()}).
-opaque state() :: #state{}.

-type(message() :: any()).

-type(proposal() ::
        {add_node, Pid::comm:mypid(), Acceptor::comm:mypid(), Learner::comm:mypid()}
      | {remove_node, Pid::comm:mypid()}
      | {deliver, Message::message(), Proposer::comm:mypid()}).

-ifdef(with_export_type_support).
-export_type([state/0, rsm_pid_type/0, proposal/0]).
-endif.

-spec new_replica(module(), rsm_pid_type()) -> state().
new_replica(App, RSMPid) ->
    #state{
      mode = joining,
      view = undefined,
      rsm_pid = RSMPid,
      app_module = App,
      app_state = undefined
     }.

-spec new_primary(module(), rsm_view:view_type()) -> state().
new_primary(App, View) ->
    #state{
      mode = joined,
      view = View,
      rsm_pid = undefined,
      app_module = App,
      app_state = App:init_state()
     }.

-spec set_mode(state(), mode_type()) -> state().
set_mode(State, Mode) ->
    State#state{mode = Mode}.

-spec get_rsm_pid(state()) -> rsm_pid_type().
get_rsm_pid(State) ->
    State#state.rsm_pid.

-spec get_view(state()) -> rsm_view:view_type().
get_view(State) ->
    State#state.view.

-spec set_view(state(), rsm_view:view_type()) -> state().
set_view(State, View) ->
    State#state{view = View}.

-spec get_app_state(state()) -> any().
get_app_state(State) ->
    State#state.app_state.

-spec set_app_state(state(), any()) -> state().
set_app_state(State, AppState) ->
    State#state{app_state = AppState}.

-spec get_app_module(state()) -> module().
get_app_module(State) ->
    State#state.app_module.

