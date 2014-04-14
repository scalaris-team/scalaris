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
%% @doc Active load balancing bootstrap module
%% @version $Id$
-module(lb_active).
-author('michels@zib.de').
-vsn('$Id$').

-export([start_link/1, check_config/0]).
-export([process_lb_msg/2]).

-type lb_message() :: {lb_active, comm:message()}.

-define(MODULES_AVAIL, [lb_active_karger, lb_active_directories]).

%% @doc The load balancing process running inside each dht_node
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    apply(get_lb_module(), start_link, [DHTNodeGroup]).

%% @doc DHT load balance messages
-spec process_lb_msg(lb_message(), dht_node_state:state()) -> dht_node_state:state().
process_lb_msg(Msg, State) ->
    apply(get_lb_module(), process_lb_msg, [Msg, State]).

get_lb_metric() ->
    config:read(lb_active_metric).

-spec get_lb_module() -> any() | failed.
get_lb_module() ->
    config:read(lb_active_module).

%% @doc config check registered in config.erl
-spec check_config() -> boolean().
check_config() ->
    config:cfg_exists(lb_active_module) andalso
    config:cfg_is_in(lb_active_module, ?MODULES_AVAIL) andalso
    config:cfg_exists(lb_active_metric) andalso
    apply(get_lb_module(), check_config, []).
