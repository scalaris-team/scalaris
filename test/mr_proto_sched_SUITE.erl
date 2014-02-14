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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    Unit tests for the map reduce protocol.
%% @end
%% @version $Id$
-module(mr_proto_sched_SUITE).
-author('fajerski@zib.de').
-vsn('$Id$').

-compile([export_all]).

-define(proto_sched(Action),
        fun() ->
                case Action of
                    start ->
                        ct:pal("Starting proto scheduler"),
                        proto_sched:start(),
                        proto_sched:start_deliver();
                    stop ->
                        proto_sched:stop(),
                        case erlang:whereis(pid_groups) =:= undefined orelse pid_groups:find_a(proto_sched) =:= failed of
                            true -> ok;
                            false -> ct:pal("Proto scheduler stats: ~.2p", proto_sched:get_infos()),
                                     proto_sched:cleanup()
                        end
                end
        end()).

-include("mr_SUITE.hrl").

all() ->
    tests_avail().

suite() -> [ {timetrap, {seconds, 90}} ].

