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
%% @version $Id$
-module(type_check_SUITE).
-author('schintke@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [
%%            tester_type_check_paxos,
            tester_type_check_api,
            tester_type_check_config,
            tester_type_check_util
           ].
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

tester_type_check_api(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules = [
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
              ],
    [ tester_type_check_module(Mod, Count) || Mod <- Modules ],
    true.

tester_type_check_config(_Config) ->
    Count = 1000,
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules = [
               {config, [
                         {check_config, 0},
                         {cfg_is_tuple, 4}, %% needs a fun as parameter
                         {cfg_is_list, 3}, %% needs a fun as parameter
                         {cfg_test_and_error, 3}, %% needs a fun as parameter
                         {start, 2},
                         {write, 2},
                         {start_link, 1}, {start_link, 2},
                         {start_link2, 0}, {start_link2, 1},
                         {loop, 0}
                        ]}
              ],
    %% These tests generate errors which would be too verbose.
    log:set_log_level(none),
    [ begin
          tester_type_check_module(Mod, Count)
      end || Mod <- Modules ],
    log:set_log_level(config:read(log_level)),
    true.

tester_type_check_util(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules = [
%%               {intervals, [{get_bounds, 1}]}, %% throws exception on []
               {db_entry, []},
               {quorum, []},
               {pdb, []},
               {pid_groups, [
                             {start_link, 0},
                             {init, 1}, %% tries to create existing ets table
                             {on, 2},
                             {pids_to_names, 2}, %% sends remote messages
                             {join_as, 2}, %% tries to join with multiple groups/names
                             {add, 3} %% same as above
                            ]},
               {randoms, [{start, 0}, {stop, 0}]}
%%               {util, [
%%                       {collect_while, 1}
%%                      ]}
%%
              ],
    [ tester_type_check_module(Mod, Count) || Mod <- Modules ],
    true.


tester_type_check_paxos(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    Modules = [
%                {acceptor, [
%                            {msg_accepted, 4} %% tries to send messages
%                           ]},
%               {acceptor_state, []},
%               {learner, []},
%               {learner_state, []},
%               {proposer, []},
%               {proposer_state, []}
              ],
    [ tester_type_check_module(Mod, Count) || Mod <- Modules ],
    true.
