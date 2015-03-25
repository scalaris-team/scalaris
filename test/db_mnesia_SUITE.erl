% @copyright 2015 Zuse Institute Berlin

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
%% @author Tanguy Racinet <tanracinet@gmail.com>
%% @doc    Unit tests for the mnesia backend.
%% @end
%% @version $Id$
-module(db_mnesia_SUITE).

-author('kruber@zib.de').
-author('tanracinet@gmail.com').
-vsn('$Id$').

-compile(export_all).

-define(TEST_DB, db_mnesia).
-define(CLOSE, close).
-define(EQ, ==).

-include("db_backend_SUITE.hrl").

all() -> tests_avail().

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
    Config1 = unittest_helper:init_per_suite(Config),

    %% cleanup schema generated possibly in earlier run
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD)-1)++
        "/../data/db_mnesia_SUITE_ct@127.0.0.1/",
    file:delete(WorkingDir ++ "schema.DAT"),

    ok = db_mnesia:start(),
    tester:register_type_checker({typedef, db_backend_beh, key}, db_backend_beh, tester_is_valid_db_key),
    tester:register_value_creator({typedef, db_backend_beh, key}, db_backend_beh, tester_create_db_key, 1),

    tester:register_type_checker({typedef, db_backend_beh, entry}, db_backend_beh, tester_is_valid_db_entry),
    tester:register_value_creator({typedef, db_backend_beh, entry}, db_backend_beh, tester_create_db_entry, 1),
    unittest_helper:start_minimal_procs(Config1, [], false).

end_per_suite(Config) ->
    tester:unregister_type_checker({typedef, db_backend_beh, key}),
    tester:unregister_value_creator({typedef, db_backend_beh, key}),

    tester:unregister_type_checker({typedef, db_backend_beh, entry}),
    tester:unregister_value_creator({typedef, db_backend_beh, entry}),
    unittest_helper:stop_minimal_procs(Config),
    unittest_helper:end_per_suite(Config).

rw_suite_runs(N) ->
    erlang:min(N, 200).
