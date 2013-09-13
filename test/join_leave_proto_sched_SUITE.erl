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
%% @doc    Unit tests for src/dht_node_join.erl in combination with
%%         src/dht_node_move.erl.
%% @end
%% @version $Id$
-module(join_leave_proto_sched_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

%% start proto scheduler for this suite
-define(proto_sched(Action),
        fun() ->
                case Action of
                    start ->
                        proto_sched:start(),
                        proto_sched:start_deliver();
                    stop ->
                        proto_sched:stop(),
                        ct:pal("Proto scheduler stats: ~.2p", proto_sched:get_infos()),
                        proto_sched:cleanup()
                end
        end()).

suite() -> [ {timetrap, {seconds, 60}} ].

-include("join_leave_SUITE.hrl").
