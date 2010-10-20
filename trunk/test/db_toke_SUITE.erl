% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Unit tests for src/db_toke.erl.
%%% @end
%% @version $Id$
-module(db_toke_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(TEST_DB, db_toke).

-include("db_SUITE.hrl").

-ifdef(have_toke).
all() -> lists:append(tests_avail(), [tester_reopen]).
-else.
all() -> [].
-endif.

%% @doc Specify how often a read/write suite can be executed in order not to
%%      hit a timeout (depending on the speed of the DB implementation).
-spec max_rw_tests_per_suite() -> pos_integer().
max_rw_tests_per_suite() ->
    50.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% db_toke:open/1, db_toke getters
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_reopen(Key::?RT:key()) -> true.
prop_reopen(Key) ->
    DB = db_toke:new(),
    FileName = db_toke:get_name(DB),
    check_db(DB, {true, []}, 0, [], "check_db_new_1"),
    ?equals(db_toke:read(DB, Key), {ok, empty_val, -1}),
    check_entry(DB, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_new_1"),
    db_toke:close(DB, false),
    DB2 = db_toke:open(FileName),
    check_db(DB2, {true, []}, 0, [], "check_db_new_1"),
    ?equals(db_toke:read(DB2, Key), {ok, empty_val, -1}),
    check_entry(DB2, Key, db_entry:new(Key), {ok, empty_val, -1}, false, "check_entry_new_1"),
    db_toke:close(DB2),
    true.

tester_reopen(_Config) ->
    tester:test(?MODULE, prop_reopen, 1, rw_suite_runs(10)).
