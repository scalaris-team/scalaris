%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    dn_cache.erl
%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    Dead node Cache
%%% @end
%%% Created : 12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
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
    {{pong}, node:node_type()} |
    {add_zombie_candidate, node:node_type()} |
    {subscribe, comm:erl_local_pid()} |
    {unsubscribe, comm:erl_local_pid()}).

-type(state() :: {fix_queue:fix_queue(), gb_set(), trigger:state()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_zombie_candidate(node:node_type()) -> ok.
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
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    Trigger = config:read(dn_cache_trigger),
    gen_component:start_link(?MODULE, Trigger, [{register, InstanceId, dn_cache}]).

%% @doc Initialises the module with an empty state.
-spec init(module()) -> state().
init(Trigger) ->
    log:log(info,"[ DNC ~p ] starting Dead Node Cache", [comm:this()]),
    TriggerState = trigger:init(Trigger, ?MODULE),
    TriggerState2 = trigger:first(TriggerState),
    {fix_queue:new(config:read(zombieDetectorSize)), gb_sets:new(), TriggerState2}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc the Token takes care, that there is only one timermessage for stabilize 

-spec on(message(), state()) -> state() | unknown_event.
on({trigger}, {Queue, Subscriber, TriggerState}) ->
        fix_queue:map(fun(X) -> comm:send(node:pidX(X), {ping, comm:this_with_cookie(X)}) end, Queue), 
        NewTriggerState = trigger:next(TriggerState),
        {Queue, Subscriber, NewTriggerState};

on({{pong}, Zombie}, {Queue, Subscriber, TriggerState}) ->
        gb_sets:fold(fun(X, _) -> comm:send_local(X, {zombie, Zombie}) end, 0, Subscriber),
        {Queue, Subscriber, TriggerState};

on({add_zombie_candidate, Node}, {Queue, Subscriber, TriggerState}) ->
		{fix_queue:add(Node, Queue), Subscriber, TriggerState};

on({subscribe, Node}, {Queue, Subscriber, TriggerState}) ->
		{Queue, gb_sets:insert(Node, Subscriber), TriggerState};

on({unsubscribe, Node}, {Queue, Subscriber, TriggerState}) ->
		{Queue, gb_sets:del_element(Node, Subscriber), TriggerState};

on(_, _State) ->
    unknown_event.

%% @doc Gets the pid of the dn_cache process in the same group as the calling
%%      process. 
-spec get_pid() -> pid() | failed.
get_pid() ->
    process_dictionary:get_group_member(dn_cache).

%% @doc Gets the zombie detector interval set in scalaris.cfg.
-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(zombieDetectorInterval).
