%% @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Thorsten Schuett <schuett@zib.de>
%% @author Nico Kruber <kruber@zib.de>
%% @version $Id: api_tx_SUITE.erl 2689 2012-01-11 10:15:52Z schintke $
-module(type_check_SUITE).
-author('schintke@zib.de').
-vsn('$Id: api_tx_SUITE.erl 2689 2012-01-11 10:15:52Z schintke $').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [tester_type_check].
suite() -> [ {timetrap, {seconds, 200}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

tester_type_check_module({Module, InExcludeList}, Count) ->
    ExpFuncs = Module:module_info(exports),
    ExcludeList = [{module_info, 0}, {module_info, 1}] ++ InExcludeList,
    [ begin
          ct:pal("Testing ~p:~p/~p~n", [Module, Fun, Arity]),
          tester:test(Module, Fun, Arity, Count)
      end
     || {Fun, Arity} = FA <- ExpFuncs, not lists:member(FA, ExcludeList) ].

tester_type_check(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules = [
%%               {randoms, []},
%%               {intervals, [{get_bounds, 1}]}, %% throws exception on []
               {api_dht, []},
               {api_dht_raw, [
                              {unreliable_lookup,2}, %% creates arb. messages
                              {unreliable_get_key,3} %% creates arb. IP-adresses
                             ]},
               {api_monitor, []},
               {api_pubsub, []},
               {api_rdht, []},
               {api_tx, []},
               {rdht_tx, [
                          {decode_value, 1} %% not every binary is an erlterm
                         ]}
%%               {util, [
%%                       {collect_while, 1}
%%                      ]}
%%
              ],
    [ tester_type_check_module(Mod, Count) || Mod <- Modules ],
    true.
