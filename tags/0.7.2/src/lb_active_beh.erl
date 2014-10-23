%  @copyright 2014 Zuse Institute Berlin

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

%% @author Maximilian Michels <michels@zib.de>
%% @doc    Active load balancing algorithm behavior
%% @end
%% @version $Id$
-module(lb_active_beh).
-author('michels@zib.de').
-vsn('$Id$').

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-include("scalaris.hrl").

%% callbacks

-type state() :: term().

-callback init() -> state().

-callback handle_msg(comm:message(), state()) -> state().

-callback handle_dht_msg(comm:message(), dht_node_state:state())
        -> dht_node_state:state().

-callback get_web_debug_kv(state()) -> [{string(), string()}].

-callback check_config() -> boolean().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init, 0},
     % handle msg from lb_active module
     {handle_msg, 2},
     % process lb message at dht node
     {handle_dht_msg, 2},
     % returns key / value list to be shown in web debug interface
     {get_web_debug_kv, 1},
     % config check performed by lb_active.erl 
     {check_config, 0}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.