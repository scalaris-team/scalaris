% @copyright 2016 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Unit tests for the bitcask backend.
%% @end
%% @version $Id$
-module(db_bitcask_SUITE).

-author('schuett@zib.de').

-compile(export_all).

-define(TEST_DB, db_bitcask).
-define(CLOSE, close).
-define(EQ, =:=).

-include("db_backend_SUITE.hrl").

all() ->
    case code:which(bitcask) of
        non_existing -> [];
        _            -> tests_avail()
    end.

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
    %% need config here to get db path
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir}]}]),
    unittest_helper:check_ring_size_fully_joined(config:read(replication_factor)),

    %% cleanup schema generated possibly in earlier failed run
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
        "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
    cleanup(WorkingDir),

    tester:register_type_checker({typedef, db_backend_beh, key, []}, db_backend_beh, tester_is_valid_db_key),
    tester:register_value_creator({typedef, db_backend_beh, key, []}, db_backend_beh, tester_create_db_key, 1),
    Config.

end_per_suite(_Config) ->
    tester:unregister_type_checker({typedef, db_backend_beh, key, []}),
    tester:unregister_value_creator({typedef, db_backend_beh, key, []}),

    %% cleanup schema generated in this run
    PWD = os:cmd(pwd),
    WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
        "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
    cleanup(WorkingDir),

    _ = unittest_helper:stop_ring(),
    ok.

rw_suite_runs(N) ->
    erlang:min(N, 200).


cleanup(Path) ->
    Re = ".*",
    Files = filelib:fold_files(Path, Re, true, fun(File, Acc) -> [File | Acc] end, []),
    _ = [file:delete(File) || File <- Files],
    ok.
