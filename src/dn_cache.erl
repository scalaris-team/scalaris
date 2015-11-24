%  @copyright 2007-2015 Zuse Institute Berlin

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

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).
-export([init/1, on/2]).

-export([add_zombie_candidate/1, subscribe/0, unsubscribe/0]).

-include("gen_component.hrl").

-type(message() ::
    {trigger} |
    {trigger_reply, {pong, PidName::pid_groups:pidname() | undefined}, node:node_type()} |
    {add_zombie_candidate, node:node_type()} |
    {subscribe, comm:erl_local_pid()} |
    {unsubscribe, comm:erl_local_pid()} |
    {send_error, Target::comm:mypid(), {ping, ThisWithCookie::comm:mypid()}, Reason::atom()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

-type(state() :: {fix_queue:fix_queue(node:node_type()), Subscribers::gb_sets:set(comm:erl_local_pid())}).

-define(SEND_OPTIONS, [{channel, prio}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Adds a dht_node PID to the dead node cache for further alive-checks.
-spec add_zombie_candidate(node:node_type()) -> ok.
add_zombie_candidate(Node) ->
    comm:send_local(get_pid(), {add_zombie_candidate, Node}).

-spec subscribe() -> ok.
subscribe() ->
    comm:send_local(get_pid(), {subscribe, self()}).

-spec unsubscribe() -> ok.
unsubscribe() ->
    comm:send_local(get_pid(), {unsubscribe, self()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a Dead Node Cache process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, dn_cache}]).

%% @doc Initialises the module with an empty state.
-spec init([]) -> state().
init([]) ->
    msg_delay:send_trigger(0, {trigger}),
    {fix_queue:new(config:read(zombieDetectorSize)), gb_sets:new()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc the Token takes care, that there is only one timermessage for stabilize

-spec on(message(), state()) -> state().
on({trigger}, {Queue, Subscribers}) ->
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
    msg_delay:send_trigger(get_base_interval(), {trigger}),
    {fix_queue:new(config:read(zombieDetectorSize)), Subscribers};

on({trigger_reply, {pong, dht_node}, Zombie},
   {_Queue, Subscribers} = State) ->
    log:log(warn,"[ dn_cache ~p ] found zombie ~.0p", [comm:this(), Zombie]),
    report_zombie(Subscribers, Zombie),
    State;

on({trigger_reply, {pong, PidName}, Zombie}, State) ->
    log:log(warn,"[ dn_cache ~p ] found zombie ~.0p but no dht_node process (reports as ~.0p)",
            [comm:this(), Zombie, PidName]),
    State;

on({add_zombie_candidate, Node}, {Queue, Subscribers}) ->
    {add_to_queue(Queue, Node), Subscribers};

on({subscribe, Node}, {Queue, Subscribers}) ->
    {Queue, gb_sets:insert(Node, Subscribers)};

on({unsubscribe, Node}, {Queue, Subscribers}) ->
    {Queue, gb_sets:del_element(Node, Subscribers)};

on({send_error, _Target, {ping, ThisWithCookie}, _Reason}, {Queue, Subscribers}) ->
    {_This, {trigger_reply, {null}, Node}} = comm:unpack_cookie(ThisWithCookie, {null}),
    {add_to_queue(Queue, Node), Subscribers};

on({web_debug_info, Requestor}, {Queue, Subscribers} = State) ->
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

-spec add_to_queue(Queue::fix_queue:fix_queue(node:node_type()), Node::node:node_type())
        -> fix_queue:fix_queue(node:node_type()).
add_to_queue(Queue, Node) ->
    case node:is_valid(Node) of
        true -> fix_queue:add_unique_head(Node, Queue, fun node:same_process/2,
                                          fun node:newer/2);
        _    -> fix_queue:add_unique_head(Node, Queue, fun erlang:'=:='/2,
                                          fun(_Old, New) -> New end)
    end.

-spec report_zombie(Subscribers::gb_sets:set(comm:erl_local_pid()), Zombie::node:node_type()) -> ok.
report_zombie(Subscribers, Zombie) ->
    gb_sets:fold(fun(X, _) ->
                         comm:send_local(X, {zombie, Zombie})
                 end, ok, Subscribers),
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
