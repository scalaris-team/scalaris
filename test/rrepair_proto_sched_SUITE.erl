%  @copyright 2014 Zuse Institute Berlin

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
%% @doc    Tests for the replica repair modules.
%% @end
%% @version $Id$
-module(rrepair_proto_sched_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

%% start proto scheduler for this suite
-define(proto_sched(Action),
        fun() ->
                case Action of
                    start ->
                        %% ct:pal("Starting proto scheduler"),
                        proto_sched:start(),
                        proto_sched:start_deliver();
                    stop ->
                        proto_sched:stop(),
                        case erlang:whereis(pid_groups) =:= undefined orelse pid_groups:find_a(proto_sched) =:= failed of
                            true -> ok;
                            false -> ct:pal("Proto scheduler stats: ~.2p", [proto_sched:get_infos()]),
                                     proto_sched:cleanup()
                        end
                end
        end()).

-include("rrepair_SUITE.hrl").

%% number of executions per test (sub-) group
-define(NUM_EXECUTIONS, 5).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() ->
    [
     session_ttl,
     {group, repair}
    ].

groups() ->
    [
     {repair, [sequence], [
                           {upd_trivial,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
                           {upd_bloom,    [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
                           {upd_merkle,   [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
                           {upd_art,      [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
                           {regen_trivial,[{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
                           {regen_bloom,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
                           {regen_merkle, [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
                           {regen_art,    [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
                           {mixed_trivial,[{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
                           {mixed_bloom,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
                           {mixed_merkle, [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
                           {mixed_art,    [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()}
                          ]}
    ].

suite() -> [ {timetrap, {seconds, 20}} ].
