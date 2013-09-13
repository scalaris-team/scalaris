% @copyright 2010-2013 Zuse Institute Berlin

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
%%   First executes all tests using the erlang scheduler. Then executes using
%%   the proto scheduler which produces a random interleaving between the
%%   different channels.
%% @end
%% @version $Id$
-module(dht_node_move_proto_sched_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

%% start proto scheduler for this suite
-define(proto_sched(Action),
        fun() ->
                case Action of
                    start ->
                        ct:pal("Starting proto scheduler"),
                        proto_sched:start(),
                        proto_sched:start_deliver();
                    stop ->
                        proto_sched:stop(),
                        case pid_groups:find_a(proto_sched) of
                            failed -> ok;
                            _ -> ct:pal("Proto scheduler stats: ~.2p", proto_sched:get_infos()),
                                 proto_sched:cleanup()
                        end
                end
        end()).

%% number of slides without timeouts
-define(NUM_SLIDES, 25).

-include("dht_node_move_SUITE.hrl").

all() ->
    [
     {group, send_to_pred},
     {group, send_to_pred_incremental},
     {group, send_to_succ},
     {group, send_to_succ_incremental}
    ].

suite() -> [ {timetrap, {seconds, 90}} ].
