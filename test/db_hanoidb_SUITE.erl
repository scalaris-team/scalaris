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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Unit tests for hanoidb backend.
%% @end
%% @version $Id$
-module(db_hanoidb_SUITE).

-author('skrzypczak@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(TEST_DB, db_hanoidb).
-define(CLOSE, close_and_delete).
-define(EQ, =:=).

-include("db_backend_SUITE.hrl").

all() ->
    case code:which(hanoidb) of
        non_existing -> [];
        _            -> lists:append(tests_avail(), [tester_reopen])
    end.

suite() -> [ {timetrap, {seconds, 30}} ].

init_per_suite(Config) ->
    tester:register_type_checker({typedef, db_backend_beh, key, []}, db_backend_beh, tester_is_valid_db_key),
    tester:register_value_creator({typedef, db_backend_beh, key, []}, db_backend_beh, tester_create_db_key, 1),
    unittest_helper:start_minimal_procs(Config, [], false).

end_per_suite(Config) ->
    tester:unregister_type_checker({typedef, db_backend_beh, key, []}),
    tester:unregister_value_creator({typedef, db_backend_beh, key, []}),
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

rw_suite_runs(N) ->
    erlang:min(N, 200).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_hanoidb:open/1, db_hanoidb getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_reopen(Key::?RT:key()) -> true.
prop_reopen(Key) ->
    DB = db_hanoidb:new(randoms:getRandomString()),
    FileName = db_hanoidb:get_name(DB),
    check_db(DB, [], "check_db_reopen_1"),
    ?equals(db_hanoidb:get(DB, Key), {}),
    DB1 = db_hanoidb:put(DB, {Key}),
    db_hanoidb:close(DB1),
    DB2 = db_hanoidb:open(FileName),
    check_db(DB2, [{Key}], "check_db_reopen_2"),
    ?equals(db_hanoidb:get(DB2, Key), {Key}),
    db_hanoidb:close_and_delete(DB2),
    true.

tester_reopen(_Config) ->
    tester:test(?MODULE, prop_reopen, 1, rw_suite_runs(10)).


