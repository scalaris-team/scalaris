%  @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc Ring maintenance behaviour
%% @end
%% @version $Id$
-module(rm_beh).
-author('schuett@zib.de').
-vsn('$Id$').

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-include("scalaris.hrl").
-type state() :: term().
-type custom_message() :: comm:message().

-callback get_neighbors(state()) -> nodelist:neighborhood().
-callback init_first() -> ok.
-callback init(Me::node:node_type(), Pred::node:node_type(), Succ::node:node_type())
        -> state().
-callback trigger_action(State::state())
        -> {ChangeReason::rm_loop:reason(), state()}.
-callback handle_custom_message(custom_message(), state())
        -> {ChangeReason::rm_loop:reason(), state()} | unknown_event.
-callback zombie_node(State::state(), Node::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
-callback fd_notify(State::state(), Event::fd:event(), DeadPid::comm:mypid(), Data::term())
        -> {ChangeReason::rm_loop:reason(), state()}.
-callback new_pred(State::state(), NewPred::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
-callback new_succ(State::state(), NewSucc::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
-callback update_node(State::state(), NewMe::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
-callback contact_new_nodes(NewNodes::[node:node_type()]) -> ok.
-callback trigger_interval() -> pos_integer().

-callback get_web_debug_info(State::state()) -> [{string(), string()}].
-callback check_config() -> boolean().
-callback unittest_create_state(Neighbors::nodelist:neighborhood()) -> state().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {get_neighbors, 1},
     {init_first, 0}, {init, 3}, {trigger_action, 1}, {handle_custom_message, 2},
     {trigger_interval, 0},
     {zombie_node, 2}, {fd_notify, 4},
     {new_pred, 2}, {new_succ, 2},
     {update_node, 2},
     {contact_new_nodes, 1},
     {get_web_debug_info, 1},
     {check_config, 0},
     {unittest_create_state, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
