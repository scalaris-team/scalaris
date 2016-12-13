% @copyright 2010-2016 Zuse Institute Berlin

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
%% @doc    Unit tests for ets backend.
%% @end
-module(db_ets_SUITE).

-author('fajerski@zib.de').

-compile(export_all).

-define(TEST_DB, db_ets).
-define(CLOSE, close).

-include("db_backend_SUITE.hrl").

groups() ->
    [{tester_tests, [sequence], tests_avail()}].

all() ->
    [
     {group, tester_tests}
    ].

suite() -> [ {timetrap, {seconds, 50}} ].

init_per_suite(Config) ->
    tester:register_type_checker({typedef, db_backend_beh, key, []}, db_backend_beh, tester_is_valid_db_key),
    tester:register_value_creator({typedef, db_backend_beh, key, []}, db_backend_beh, tester_create_db_key, 1),
    Config.

end_per_suite(_Config) ->
    tester:unregister_type_checker({typedef, db_backend_beh, key, []}),
    tester:unregister_value_creator({typedef, db_backend_beh, key, []}),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

rw_suite_runs(N) ->
    erlang:min(N, 10000).
