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

-export([new/3,
         set_db/2,
        get_predecessor/1, get_successor/1, get_db/1,
        update_pred_succ/3, update_pred/2, update_succ/2]).

-record(local_state, {successor::group_types:group_node(),
                      predecessor::group_types:group_node(),
                      rt,
                      db}).

-opaque local_state() :: #local_state{}.

-spec new(Pred::group_types:group_node(), Succ::group_types:group_node(),
          DB::?DB:db()) -> local_state().
new(Pred, Succ, DB) ->
    #local_state{
     predecessor = Pred,
     successor = Succ,
     db = DB,
     rt = none}.

-spec get_db(State::local_state()) -> ?DB:db().
get_db(#local_state{db=DB}) ->
    DB.

-spec set_db(State::local_state(), DB::?DB:db()) -> local_state().
set_db(LocalState, DB) ->
    LocalState#local_state{db=DB}.

-spec get_predecessor(State::local_state()) -> group_types:group_node().
get_predecessor(#local_state{predecessor=Predecessor}) ->
    Predecessor.

-spec get_successor(State::local_state()) -> group_types:group_node().
get_successor(#local_state{successor=Successor}) ->
    Successor.

-spec update_pred_succ(local_state(), group_types:group_node(),
                       group_types:group_node()) -> local_state().
update_pred_succ(NodeState, Pred, Succ) ->
    update_pred(update_succ(NodeState, Succ), Pred).

-spec update_succ(local_state(), group_types:group_node()) -> local_state().
update_succ(NodeState, Succ) ->
    % @todo: check if newer and update
    NodeState.

-spec update_pred(local_state(), group_types:group_node()) -> local_state().
update_pred(NodeState, Pred) ->
    % @todo: check if newer and update
    NodeState.
