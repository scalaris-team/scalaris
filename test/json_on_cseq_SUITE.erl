%% @copyright 2013-2017 Zuse Institute Berlin
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
%% @doc    Unit tests for json_on_cseq
%% @end
%% @version $Id$
-module(json_on_cseq_SUITE).
-author('skrzypczak@zib.de').
-vsn('$Id$').

-compile(export_all).


-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all() -> [
          tester_type_check_json_on_cseq,
          test_dotto_avail
         ].

suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir},
                                                   {replication_factor, 4}]}]),
    unittest_helper:check_ring_size_fully_joined(config:read(replication_factor)),
    [{stop_ring, true} | Config].

test_dotto_avail(_Config) ->
    Data = dict:from_list([{name, "bob"}, {age, 29}, {friends, ["sandy", "patrick"]},
                           {data, dict:from_list([{numbers, [10, 11, 12]}])}]),
    Expected = dict:from_list([{name, "bob"}, {age, 29}, {friends, ["sandy"]},
                           {data, dict:from_list([{numbers, [10, 11, 12]}])}]),
    {ok, Result} = dotto:remove(Data, [friends, 1]),
    ?equals(dict:to_list(Expected), dict:to_list(Result)).

tester_type_check_json_on_cseq(_Config) ->
    Count = 250,

    JsonType = {typedef, json_on_cseq, json, []},
    tester:register_type_checker(JsonType, json_on_cseq, is_json),
    tester:register_value_creator(JsonType, json_on_cseq, create_rand_json, 1),

    %% [{modulename, [publicexeludelist = {fun, arity}], [privexcludelist..}]
    %% Excluded these functions because cannot create funs
    Modules = [{json_on_cseq,
                 [{cc_noop,3}],
                 [{read_helper, 2},
                  {write_helper, 3},
                  {write_helper, 5}
                 ]}
              ],

    _ = [tester:type_check_module(Mod, Excl, ExclPriv, Count)
         || {Mod, Excl, ExclPriv} <- Modules],

    tester:unregister_type_checker(JsonType),
    tester:unregister_value_creator(JsonType),
    true.

