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
%% @doc    grouped_node router
%% @end
%% @version $Id$
-module(group_router).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([route/4]).

-type(message() :: any()).

-spec route(?RT:key(), pos_integer(), message(), group_state:state()) -> group_state:state().
route(Key, Hops, Message, State) ->
    View = group_state:get_view(State),
    NodeState = group_state:get_node_state(State),
    Interval = group_view:get_interval(View),
    case intervals:in(Key, Interval) of
        true ->
            group_node:on(Message, State);
        false ->
            {_, _, Successors} = group_local_state:get_successor(NodeState),
            Successor = hd(Successors), % @todo
            comm:send(Successor, {route, Key, Hops + 1, Message}),
            State
    end.
