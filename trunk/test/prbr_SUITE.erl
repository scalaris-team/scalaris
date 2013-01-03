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
%% @version $Id: type_check_SUITE.erl 3571 2012-08-21 13:22:25Z kruber@zib.de $
-module(prbr_SUITE).
-author('schintke@zib.de').
-vsn('$Id: type_check_SUITE.erl 3571 2012-08-21 13:22:25Z kruber@zib.de $ ').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [
            tester_type_check_l_on_cseq,
            tester_type_check_rbr
           ].
suite() -> [ {timetrap, {seconds, 400}} ].

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

tester_type_check_rbr(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {kv_on_cseq,
           [ {is_valid_next_req, 3} %% cannot create funs
           ],
           []},
          {prbr,
           [ {on, 2},       %% sends messages
             {set_entry, 2} %% needs valid tid()
          ],
           [ {msg_read_reply, 4},  %% sends messages
             {msg_read_deny, 3},   %% sends messages
             {msg_write_reply, 3}, %% sends messages
             {msg_write_deny, 3},  %% sends messages
             {get_entry, 2}        %% needs valid tid()
            ]},
          {rbrcseq,
           [ {on, 2},         %% sends messages
             {qread, 3},      %% tries to create envelopes
             {qread, 4},      %% needs fun as input
             {start_link, 3}, %% needs fun as input
             {qwrite, 5},     %% needs funs as input
             {qwrite, 7}      %% needs funs as input
           ],
           [ {inform_client, 2}, %% cannot create valid envelopes
             {get_entry, 2},     %% needs valid tid()
             {set_entry, 2}      %% needs valid tid()
           ]
          }

        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_l_on_cseq(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {l_on_cseq,
           [ {add_first_lease_to_db, 2}, %% cannot create DB refs for State
             {on, 2}, %% cannot create dht_node_state
             {split_test, 0} %% requires 1-node-ring
           ],
           [ {read, 2}, %% cannot create pids
             {update_lease_in_dht_node_state, 2}, %% gb_trees not supported by type_checker
             {remove_lease_from_dht_node_state, 2} %% gb_trees not supported by type_checker
           ]}
        ],
    %% join a dht_node group to be able to call lease trigger functions
    pid_groups:join(pid_groups:group_with(dht_node)),
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    true.
