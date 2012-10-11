%  @copyright 2007-2012 Zuse Institute Berlin

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
    {send_error, Target::comm:mypid(), {ping, ThisWithCookie::comm:mypid()}, Reason::atom()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

-type(state() :: {fix_queue:fix_queue(), Subscribers::gb_set(), trigger:state()}).

-define(SEND_OPTIONS, [{channel, prio}]).

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
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Trigger, [{pid_groups_join_as, DHTNodeGroup, dn_cache}]).

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
on({trigger}, {Queue, Subscribers, TriggerState}) ->
    _ = fix_queue:map(
          fun(X) ->
                  Pid = case node:is_valid(X) of
                            true -> node:pidX(X);
                            _    -> X
                        end,
                  SPid = comm:reply_as(comm:this(), 2, {trigger_reply, '_', X}),
                  comm:send(Pid, {ping, SPid},
                            ?SEND_OPTIONS ++ [{shepherd, self()}])
          end, Queue),
    NewTriggerState = trigger:next(TriggerState),
    {fix_queue:new(config:read(zombieDetectorSize)), Subscribers, NewTriggerState};

on({trigger_reply, {pong}, Zombie}, {Queue, Subscribers, TriggerState}) ->
    log:log(warn,"[ dn_cache ~p ] found zombie ~.0p", [comm:this(), Zombie]),
    report_zombie(Subscribers, Zombie),
    {Queue, Subscribers, TriggerState};

on({add_zombie_candidate, Node}, {Queue, Subscribers, TriggerState}) ->
    {add_to_queue(Queue, Node), Subscribers, TriggerState};

on({subscribe, Node}, {Queue, Subscribers, TriggerState}) ->
    {Queue, gb_sets:insert(Node, Subscribers), TriggerState};

on({unsubscribe, Node}, {Queue, Subscribers, TriggerState}) ->
    {Queue, gb_sets:del_element(Node, Subscribers), TriggerState};

on({send_error, _Target, {ping, ThisWithCookie}, _Reason}, {Queue, Subscribers, TriggerState}) ->
    {_This, {trigger_reply, {null}, Node}} = comm:unpack_cookie(ThisWithCookie, {null}),
    {add_to_queue(Queue, Node), Subscribers, TriggerState};

on({web_debug_info, Requestor}, {Queue, Subscribers, _TriggerState} = State) ->
    KeyValueList =
        lists:flatten(
          [{"max_length", fix_queue:max_length(Queue)},
           {"queue length", fix_queue:length(Queue)},
           {"queue (node):", ""},
           [{"", webhelpers:safe_html_string("~p", [Node])} || Node <- queue:to_list(fix_queue:queue(Queue))],
           {"subscribers", gb_sets:size(Subscribers)},
           {"subscribers (pid):", ""},
           [{"", pid_groups:pid_to_name(Pid)} || Pid <- gb_sets:to_list(Subscribers)]]),
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

-spec add_to_queue(Queue::fix_queue:fix_queue(), Node::node:node_type() | comm:mypid())
        -> fix_queue:fix_queue().
add_to_queue(Queue, Node) ->
    case node:is_valid(Node) of
        true -> fix_queue:add_unique_head(Node, Queue, fun node:same_process/2,
                                          fun node:newer/2);
        _    -> fix_queue:add_unique_head(Node, Queue, fun erlang:'=:='/2,
                                          fun(_Old, New) -> New end)
    end.

-spec report_zombie(Subscribers::gb_set(), Zombie::node:node_type() | comm:mypid()) -> ok.
report_zombie(Subscribers, Zombie) ->
    case node:is_valid(Zombie) of % comm:mypid() or node:node_type()?
        true -> gb_sets:fold(fun(X, _) ->
                                     comm:send_local(X, {zombie, Zombie})
                             end, ok, Subscribers);
        _    -> gb_sets:fold(fun(X, _) ->
                                     comm:send_local(X, {zombie_pid, Zombie})
                             end, ok, Subscribers)
    end,
    ok.

%% @doc Gets the pid of the dn_cache process in the same group as the calling
%%      process. 
-spec get_pid() -> pid() | failed.
get_pid() ->
    pid_groups:get_my(dn_cache).

%% @doc Gets the zombie detector interval set in scalaris.cfg.
-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(zombieDetectorInterval).
