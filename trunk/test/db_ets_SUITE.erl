% @copyright 2010-2011 Zuse Institute Berlin

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
%% @version $Id$
-module(db_ets_SUITE).

-author('fajerski@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(TEST_DB, db_ets).

-include("db_backend_SUITE.hrl").

all() ->
    tests_avail().

suite() -> [ {timetrap, {seconds, 15}} ].

init_per_suite(Config) ->
    Config1 = unittest_helper:init_per_suite(Config),
    tester:register_type_checker({typedef, backend_beh, key}, backend_beh, tester_is_valid_db_key),
    tester:register_value_creator({typedef, backend_beh, key}, backend_beh, tester_create_db_key, 1),

    tester:register_type_checker({typedef, backend_beh, entry}, backend_beh, tester_is_valid_db_entry),
    tester:register_value_creator({typedef, backend_beh, entry}, backend_beh, tester_create_db_entry, 1),
    Config1.

end_per_suite(Config) ->
    tester:unregister_type_checker({typedef, backend_beh, key}),
    tester:unregister_value_creator({typedef, backend_beh, key}),

    tester:unregister_type_checker({typedef, backend_beh, entry}),
    tester:unregister_value_creator({typedef, backend_beh, entry}),
    unittest_helper:end_per_suite(Config).

rw_suite_runs(N) ->
    erlang:min(N, 10000).
