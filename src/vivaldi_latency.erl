%  @copyright 2009-2017 Zuse Institute Berlin

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
%% @doc Vivaldi helper module for measuring latency between nodes.
%% @end
%% @version $Id$
-module(vivaldi_latency).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1, on/2, init/1]).

-export([measure_latency/3, check_config/0]).

-include("gen_component.hrl").

%% state of the vivaldi loop
-type state() :: {gb_trees:tree(task_id(), task_state()), task_id()}.

-type task_id() :: non_neg_integer().

%% state per task
-type task_state() ::
    {Owner::comm:erl_local_pid(),
     RemotePid::comm:mypid(),
     Token::{gossip_vivaldi:network_coordinate(), gossip_vivaldi:est_error()},
     Start::erlang_timestamp() | unknown,
     Count::non_neg_integer(),
     Latencies::[gossip_vivaldi:latency()]}.

-type token() :: {gossip_vivaldi:network_coordinate(), gossip_vivaldi:est_error()}.

% accepted messages of vivaldi_latency processes
-type message() ::
    {ping_reply, {pong, term()}, TaskId::task_id()} |
    {start_ping, Owner::comm:erl_local_pid(), RemotePid::comm:mypid(), Token::token()} |
    {shutdown, TaskId::task_id()}.

-define(SEND_OPTIONS, [{channel, prio}, {?quiet}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec on(Message::message(), State::state()) -> state().
on({ping_reply, {pong, gossip}, TaskId}, State = {Tasks, NextTask}) ->
    case gb_trees:lookup(TaskId, Tasks) of
        {value, Task} ->
            {Owner, RemotePid, Token, Start, Count, Latencies} = Task,
            Stop = os:timestamp(),
            NewLatencies = [timer:now_diff(Stop, Start) | Latencies],
            case Count =:= config:read(gossip_vivaldi_count_measurements) of
                true ->
                    Msg = {cb_msg, {gossip_vivaldi, default},
                           {update_vivaldi_coordinate, calc_latency(NewLatencies), Token}},
                    comm:send_local(Owner, Msg),
                    {gb_trees:delete(TaskId, Tasks), NextTask};
                false ->
                    SPid = comm:reply_as(comm:this(), 2, {ping_reply, '_', TaskId}),
                    comm:send(RemotePid, {ping, SPid}, ?SEND_OPTIONS),
                    UpdatedTask = {Owner, RemotePid, Token, os:timestamp(),
                                   Count+1, NewLatencies},
                    {gb_trees:enter(TaskId, UpdatedTask, Tasks), NextTask}
            end;
        none ->
            State
    end;

on({ping_reply, {pong, _PidName}, _TaskId}, State) ->
    % ignore unrelated pong messages
    State;

on({start_ping, Owner, RemotePid, Token}, {Tasks, NextTask}) ->
    %%NewCount = Count + 1,
    SPid = comm:reply_as(comm:this(), 2, {ping_reply, '_', NextTask}),
    comm:send(RemotePid, {ping, SPid}, ?SEND_OPTIONS),
    Count = config:read(gossip_vivaldi_count_measurements),
    msg_delay:send_local(Count * config:read(gossip_vivaldi_latency_timeout),
                         self(), {shutdown, NextTask}, [{?quiet}]),
    Task = {Owner, RemotePid, Token, os:timestamp(), 1, []},
    {gb_trees:enter(NextTask, Task, Tasks), NextTask+1};

on({shutdown, TaskId}, {Tasks, NextTask}) ->
    %% log:log(info, "shutdown vivaldi_latency due to timeout", []),
    {gb_trees:delete_any(TaskId, Tasks), NextTask};

on(Msg, State) ->
    io:format("vivaldi unknown message: ~p~n", [Msg]),
    State.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(Args::term()) -> state().
init([]) ->
    %% {Owner, RemotePid, Token, unknown, 0, []}.
    {gb_trees:empty(), 0}.

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, vivaldi_latency},
                              {wait_for_init}]).

-spec measure_latency(comm:mypid(), gossip_vivaldi:network_coordinate(),
                      gossip_vivaldi:est_error()) -> ok.
measure_latency(RemotePid, RemoteCoordinate, RemoteConfidence) ->
    Pid = pid_groups:find_a(vivaldi_latency),
    Token = {RemoteCoordinate, RemoteConfidence},
    comm:send_local(Pid, {start_ping, self(), RemotePid, Token}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec calc_latency([gossip_vivaldi:latency(),...]) -> number().
calc_latency(Latencies) ->
    mathlib:median(Latencies).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters of the vivaldi_latency process exist
%%      and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(gossip_vivaldi_count_measurements) and
    config:cfg_is_greater_than(gossip_vivaldi_count_measurements, 0) and

    config:cfg_is_integer(gossip_vivaldi_measurements_delay) and
    config:cfg_is_greater_than_equal(gossip_vivaldi_measurements_delay, 0) and

    config:cfg_is_integer(gossip_vivaldi_latency_timeout) and
    config:cfg_is_greater_than(gossip_vivaldi_latency_timeout, 0).
