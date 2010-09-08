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

%% @author Christian Hennig <hennig@zib.de>
%% @doc    Dead node Cache
%% @end
%% @version $Id$
-module(dn_cache).
-author('hennig@zib.de').
-vsn('$Id$').

-behavior(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).
-export([init/1, on/2, get_base_interval/0]).

-export([add_zombie_candidate/1, subscribe/0, unsubscribe/0]).
-type(message() ::
    {trigger} |
    {{pong}, node:node_type() | comm:mypid()} |
    {add_zombie_candidate, node:node_type() | comm:mypid()} |
    {subscribe, comm:erl_local_pid()} |
    {unsubscribe, comm:erl_local_pid()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

-type(state() :: {fix_queue:fix_queue(), gb_set(), trigger:state()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_zombie_candidate(node:node_type() | comm:mypid()) -> ok.
add_zombie_candidate(Node) ->
    comm:send_local(get_pid(), {add_zombie_candidate, Node}).

-spec subscribe() -> ok.
subscribe() ->
    comm:send_local(get_pid(), {subscribe, self()}).

-spec unsubscribe() -> ok.
unsubscribe() ->
    comm:send_local(get_pid(), {unsubscribe, self()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a Dead Node Cache process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(dn_cache_trigger),
    gen_component:start_link(?MODULE, Trigger, [{pid_groups_join_as, DHTNodeGroup, dn_cache}]).

%% @doc Initialises the module with an empty state.
-spec init(module()) -> state().
init(Trigger) ->
    TriggerState = trigger:init(Trigger, ?MODULE),
    TriggerState2 = trigger:now(TriggerState),
    {fix_queue:new(config:read(zombieDetectorSize)), gb_sets:new(), TriggerState2}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc the Token takes care, that there is only one timermessage for stabilize 

-spec on(message(), state()) -> state().
on({trigger}, {Queue, Subscriber, TriggerState}) ->
    fix_queue:map(fun(X) ->
                          Pid = case node:is_valid(X) of
                                    true -> node:pidX(X);
                                    _    -> X
                                end,
                          comm:send(Pid, {ping, comm:this_with_cookie(X)})
                  end, Queue),
    NewTriggerState = trigger:next(TriggerState),
    {Queue, Subscriber, NewTriggerState};

on({{pong}, Zombie}, {Queue, Subscriber, TriggerState}) ->
    log:log(warn,"[ dn_cache ~p ] found zombie ~p", [comm:this(), Zombie]),
    NewQueue =
        case node:is_valid(Zombie) of
            true ->
                gb_sets:fold(fun(X, _) ->
                                     comm:send_local(X, {zombie, Zombie})
                             end, 0, Subscriber),
                fix_queue:remove(Zombie, Queue, fun node:same_process/2);
            _ ->
                gb_sets:fold(fun(X, _) ->
                                     comm:send_local(X, {zombie_pid, Zombie})
                             end, 0, Subscriber),
                fix_queue:remove(Zombie, Queue, fun erlang:'=:='/2)
    end,
    {NewQueue, Subscriber, TriggerState};

on({add_zombie_candidate, Node}, {Queue, Subscriber, TriggerState}) ->
    case node:is_valid(Node) of
        true ->
            {fix_queue:add_unique_head(Node, Queue, fun node:same_process/2, fun node:newer/2),
             Subscriber, TriggerState};
        _ ->
            {fix_queue:add_unique_head(Node, Queue, fun erlang:'=:='/2, fun(_Old, New) -> New end),
             Subscriber, TriggerState}
    end;

on({subscribe, Node}, {Queue, Subscriber, TriggerState}) ->
    {Queue, gb_sets:insert(Node, Subscriber), TriggerState};

on({unsubscribe, Node}, {Queue, Subscriber, TriggerState}) ->
    {Queue, gb_sets:del_element(Node, Subscriber), TriggerState};

on({web_debug_info, Requestor}, {Queue, Subscriber, _TriggerState} = State) ->
    KeyValueList =
        lists:flatten(
          [{"max_length", fix_queue:max_length(Queue)},
           {"queue length", fix_queue:length(Queue)},
           {"queue (node):", ""},
           [{"", lists:flatten(io_lib:format("~p", [Node]))} || Node <- queue:to_list(fix_queue:queue(Queue))],
           {"subscribers", gb_sets:size(Subscriber)},
           {"subscribers (pid):", ""},
           [{"", webhelpers:pid_to_name(Pid)} || Pid <- gb_sets:to_list(Subscriber)]]),
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%% @doc Gets the pid of the dn_cache process in the same group as the calling
%%      process. 
-spec get_pid() -> pid() | failed.
get_pid() ->
    pid_groups:get_my(dn_cache).

%% @doc Gets the zombie detector interval set in scalaris.cfg.
-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(zombieDetectorInterval).
