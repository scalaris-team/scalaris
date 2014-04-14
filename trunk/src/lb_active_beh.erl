%  @copyright 2010-2011 Zuse Institute Berlin

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

%% TODO add more...

%-callback start_link_per_vm (pid_groups:groupname()) -> {ok, pid()}.
%-callback start_link_per_dht(pid_groups:groupname()) -> {ok, pid()}.

-callback process_lb_msg(lb_active:lb_message(), dht_node_state:state())
        -> dht_node_state:state().

-callback check_config() -> boolean().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     % start link for central service
     %{start_link_per_vm, 1},
     % start link for dht node service
     %{start_link_per_dht, 1},
     % process lb message at dht node
     {process_lb_msg, 2},
     % config check performed by lb_active.erl 
     {check_config, 0}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.