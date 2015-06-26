% @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Unit tests for src/dht_node_move.erl (slide and jump operations).
%%   Executes tests using the protocol scheduler producing a random interleaving
%%   between the different channels.
%% @end
%% @version $Id$
-module(dht_node_move_proto_sched_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

%% start proto scheduler for this suite
-define(proto_sched(Action), proto_sched_fun(Action)).

%% number of slides without timeouts
-define(NUM_SLIDES, 25).

-include("dht_node_move_SUITE.hrl").

all() ->
    [
     {group, send_to_pred},
     {group, send_to_pred_incremental},
     {group, send_to_succ},
     {group, send_to_succ_incremental},
     {group, send_to_both},
     {group, send_to_both_incremental},
     {group, slide_illegally},
     {group, jump_slide}
    ].

suite() -> [ {timetrap, {seconds, 60}} ].

-spec additional_ring_config() -> [{stabilization_interval_base, 100000},...].
additional_ring_config() ->
    % increase ring stabilisation interval since proto_sched infections get
    % lost if rm subscriptions are triggered instead of continuing due to our
    % direct (and infected) messages!
    [{stabilization_interval_base, 100000}, % ms
     {tman_cyclon_interval, 100} % s
    ].

-spec proto_sched_fun(start | stop | fun(() -> ok)) -> ok.
proto_sched_fun(start) ->
    %% ct:pal("Starting proto scheduler"),
    proto_sched:thread_num(1),
    proto_sched:thread_begin();
proto_sched_fun(stop) ->
    %% is a ring running?
    case erlang:whereis(pid_groups) =:= undefined
             orelse pid_groups:find_a(proto_sched) =:= failed of
        true -> ok;
        false ->
            %% then finalize proto_sched run:
            %% try to call thread_end(): if this
            %% process was running the proto_sched
            %% thats fine, otherwise thread_end()
            %% will raise an exception
            proto_sched:thread_end(),
            proto_sched:wait_for_end(),
            unittest_helper:print_proto_sched_stats(at_end_if_failed),
            proto_sched:cleanup()
    end;
proto_sched_fun(Fun) when is_function(Fun) ->
    Fun().
