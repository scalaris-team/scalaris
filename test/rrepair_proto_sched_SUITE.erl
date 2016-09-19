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
-define(proto_sched(Action), proto_sched_fun(Action)).

-include("rrepair_SUITE.hrl").

%% number of executions per test (sub-) group
-define(NUM_EXECUTIONS, 5).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() ->
    [
     {group, gsession_ttl},
     {group, repair}
    ].

groups() ->
    [
     {gsession_ttl,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], [session_ttl]},
     {repair, [sequence],
      [
       {upd_trivial,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {upd_shash,    [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {upd_bloom,    [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {upd_merkle,   [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {upd_art,      [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {regen_trivial,[{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
       {regen_shash,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
       {regen_bloom,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
       {regen_merkle, [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
       {regen_art,    [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default() ++ regen_special()},
       {mixed_trivial,[{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {mixed_shash,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {mixed_bloom,  [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {mixed_merkle, [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()},
       {mixed_art,    [{repeat_until_any_fail, ?NUM_EXECUTIONS}], repair_default()}
      ]}
    ].

suite() -> [ {timetrap, {seconds, 20}} ].

init_per_group_special(_Group, Config) ->
    Config.

end_per_group_special(_Group, Config) ->
    Config.

-spec proto_sched_fun(start | stop) -> ok.
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
    end.
