%  @copyright 2009-2014 Zuse Institute Berlin

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

-export([on/2, init/1]).

-export([measure_latency/3, check_config/0]).

% state of the vivaldi loop
-type state() ::
    {Owner::comm:erl_local_pid(),
     RemotePid::comm:mypid(),
     Token::{gossip_vivaldi:network_coordinate(), gossip_vivaldi:est_error()},
     Start::{MegaSecs::non_neg_integer(), Secs::non_neg_integer(), MicroSecs::non_neg_integer()} | unknown,
     Count::non_neg_integer(),
     Latencies::[gossip_vivaldi:latency()]}.

% accepted messages of vivaldi_latency processes
-type message() ::
    {{pong, PidName::pid_groups:pidname() | undefined}, Count::pos_integer()} |
    {start_ping} |
    {shutdown} |
    {'DOWN', MonitorRef::reference(), process, Owner::comm:erl_local_pid(), Info::any()}.

-define(SEND_OPTIONS, [{channel, prio}, {?quiet}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec on(Message::message(), State::state()) -> state().
on({ping_reply, {pong, gossip}, Count},
   {Owner, RemotePid, Token, Start, Count, Latencies})
  when Start =/= unknown ->
    Stop = os:timestamp(),
    NewLatencies = [timer:now_diff(Stop, Start) | Latencies],
    case Count =:= config:read(gossip_vivaldi_count_measurements) of
        true ->
            Msg = {cb_msg, {gossip_vivaldi, default},
                   {update_vivaldi_coordinate, calc_latency(NewLatencies), Token}},
            comm:send_local(Owner, Msg),
            kill;
        false ->
            _ = msg_delay:send_local(config:read(gossip_vivaldi_measurements_delay),
                                      self(), {start_ping}),
            {Owner, RemotePid, Token, unknown, Count, NewLatencies}
    end;

on({ping_reply, {pong, _PidName}, _Count}, State) ->
    % ignore unrelated pong messages
    State;

on({start_ping}, {Owner, RemotePid, Token, _, Count, Latencies}) ->
    NewCount = Count + 1,
    SPid = comm:reply_as(comm:this(), 2, {ping_reply, '_', NewCount}),
    comm:send(RemotePid, {ping, SPid}, ?SEND_OPTIONS),
    {Owner, RemotePid, Token, os:timestamp(), NewCount, Latencies};

on({shutdown}, _State) ->
    log:log(info, "shutdown vivaldi_latency due to timeout", []),
    kill;

on({'DOWN', _MonitorRef, process, Owner, _Info}, {Owner, _RemotePid, _Token, _Start, _Count, _Latencies}) ->
    log:log(info, "shutdown vivaldi_latency due to vivaldi shutting down", []),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init({pid(), comm:mypid(), {gossip_vivaldi:network_coordinate(), gossip_vivaldi:est_error()}}) -> state().
init({Owner, RemotePid, Token}) ->
    _ = comm:send_local_after(config:read(gossip_vivaldi_latency_timeout), self(), {shutdown}),
    comm:send_local(self(), {start_ping}),
    erlang:monitor(process, Owner),
    {Owner, RemotePid, Token, unknown, 0, []}.

-spec measure_latency(comm:mypid(), gossip_vivaldi:network_coordinate(), gossip_vivaldi:est_error()) -> {ok, pid()}.
measure_latency(RemotePid, RemoteCoordinate, RemoteConfidence) ->
    gen_component:start(?MODULE, fun ?MODULE:on/2, {self(), RemotePid, {RemoteCoordinate, RemoteConfidence}}, []).

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
