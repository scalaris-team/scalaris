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
    %% need config here to get db path
    Config2 = unittest_helper:start_minimal_procs(Config, [], false),

    %% cleanup schema generated possibly in earlier failed run
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
        "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
    _ = file:delete(WorkingDir ++ "schema.DAT"),

    ok = db_mnesia:start(),
    tester:register_type_checker({typedef, db_backend_beh, key, []}, db_backend_beh, tester_is_valid_db_key),
    tester:register_value_creator({typedef, db_backend_beh, key, []}, db_backend_beh, tester_create_db_key, 1),
    Config2.

end_per_suite(Config) ->
    tester:unregister_type_checker({typedef, db_backend_beh, key, []}),
    tester:unregister_value_creator({typedef, db_backend_beh, key, []}),

    _ = application:stop(mnesia),
    %% cleanup schema generated in this run
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
        "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
    _ = file:delete(WorkingDir ++ "schema.DAT"),

    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

rw_suite_runs(N) ->
    erlang:min(N, 200).
