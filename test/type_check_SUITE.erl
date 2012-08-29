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
-vsn('$Id$ ').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [
            tester_type_check_api,
            tester_type_check_config,
            tester_type_check_gossip,
            tester_type_check_math,
            tester_type_check_node,
            tester_type_check_paxos,
            tester_type_check_tx,
            tester_type_check_util
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

tester_type_check_api(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {api_dht, [], []},
          {api_dht_raw,
           [ {unreliable_lookup,2}, %% creates arb. messages
             {unreliable_get_key,3} %% creates arb. IP-adresses
           ],
           [ {range_read,1}, %% bug in range_read?
             {range_read_loop,5}, %% receives msgs
             {delete_and_cleanup_timer,2} %% cannot create reference()
           ]},
          {api_monitor, [], []},
          {api_pubsub, [], []},
          {api_rdht, [], [ {delete_collect_results, 3} ]}, %% receives
          {api_tx, [], []}
        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_config(_Config) ->
    Count = 1000,
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {config,
           [ {cfg_is_list, 3}, %% needs a fun as parameter
             {cfg_is_tuple, 4}, %% needs a fun as parameter
             {cfg_test_and_error, 3}, %% needs a fun as parameter
             {check_config, 0},
             {init, 2},
             {start_link, 1}, {start_link, 2},

             {write, 2}, %% cannot write to config_ets
             {read, 1}, %% cannot write to config
             {system_continue, 3}, %% no return
             {loop, 0} %% no return
           ],
           [ {populate_db, 1}, %% cannot create config filenames
             {process_term, 1} %% cannot write config_ets
           ]}
        ],
    %% These tests generate errors which would be too verbose.
    log:set_log_level(none),
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    log:set_log_level(config:read(log_level)),
    true.

tester_type_check_gossip(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    Modules =
        [ {gossip_state, [], []}
        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_math(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    tester:register_type_checker({typedef, intervals, interval}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, simple_interval}, intervals, is_well_formed_simple),
    tester:register_type_checker({typedef, intervals, continuous_interval}, intervals, is_continuous),
    tester:register_value_creator({typedef, intervals, interval}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, simple_interval}, intervals, tester_create_simple_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval}, intervals, tester_create_continuous_interval, 4),
    Modules =
        [ {intervals,
           [ {get_bounds, 1}, %% throws exception on []
             {new, 4}, %% type spec to wide (would need overlapping contract support)
             {split, 2} %% integers too large; tested via feeder
           ],
           [ {minus_simple2, 2}, %% second is subset of first param
             {p_split, 2}, %% integers too large; tested via feeder
             {split2, 8} %% integers too large; tested via feeder
           ]},
          {mathlib,
           [ {vecWeightedAvg,4}, %% needs same length lists
             {closestPoints, 1}, %% needs same length lists
             {binomial_coeff, 2}, %% needs N > K, done by feeder
             {aggloClustering, 3}, %% needs same length lists
             {vecAdd, 2}, %% needs same length lists
             {vecSub, 2}, %% needs same length lists
             {euclideanDistance, 2}, %% needs same length lists
             {u, 1} %% needs non zero number in list
           ],
           [ {closestPointsForJ, 7}, %% needs same length lists and funs
             {aggloClusteringHelper, 6}, %% spec suspicious (-1 for lists:nth())
             {choose, 4} %% slow for large integers
           ]},
          %% {math_pos, [], []}, %% needs valid pos fields
          {prime, [],
           [ {find_bigger_prime, 3} ]}, %% too slow for large integers
          {randoms, [{start, 0}, {stop, 0}], []}
        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, intervals, interval}),
    tester:unregister_type_checker({typedef, intervals, simple_interval}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval}),
    tester:unregister_value_creator({typedef, intervals, interval}),
    tester:unregister_value_creator({typedef, intervals, simple_interval}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval}),
    true.

tester_type_check_node(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    Modules =
        [
         {node,
          [ {is_newer, 2}, %% throws function clause (same pid as input needed)
            {newer, 2} %% throws function clause (same pid as input needed)
          ], []},
         {node_details,
          [ {get, 2}], %% throws 'not_available' on empty nodelist
          [ {get_list, 2}]}, %% throws 'not_available'
         {nodelist,
          [ {throw_if_newer, 2}, %% throws
            {lremove, 3}, %% cannot create funs
            {lfilter_min_length, 3}, %% cannot create funs
            {filter_min_length, 4}, %% cannot create funs
            {lfilter, 2}, %% cannot create funs
            {lfilter, 3}, %% cannot create funs
            {filter, 2}, %% cannot create funs
            {filter, 3}, %% cannot create funs
            {update_node, 2}, %% needs node in certain interval
            {ets_insert_newer_node,2}, %% cannot create tid()
            {remove, 3} %% cannot create funs
          ],
          [ ]}
        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_paxos(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    Modules =
        [ {acceptor,
           [ {add_learner,3}, %% tries to send messages
             {msg_accepted, 4}, %% tries to send messages
             {on, 2}, %% spec for messages not tight enough
             {start_link,2}, %% tries to spawn processes
             {start_paxosid, 2}, %% tries to send messages
             {start_paxosid, 3}, %% tries to send messages
             {stop_paxosids,2} %% tries to send messages
           ],
           [ {msg_ack,5}, %% sends msgs
             {msg_nack,3}, %% sends msgs
             {msg_naccepted,3}, %% sends msgs
             {get_entry,2}, %% no spec
             {set_entry,2}, %% no spec
             {inform_learners,2}, %% sends msgs
             {inform_learner,3} %% sends msgs
           ]},
          {acceptor_state, [], []},
          {learner,
           [ {on, 2}, %% spec for messages not tight enough
             {start_link,2}, %% tries to spawn processes
             {start_paxosid, 5}, %% tries to send messages
             {stop_paxosids,2} %% tries to send messages
           ],
           [ {msg_decide,4}, %% sends msg.
             {decide, 2} %% no spec & uses msg_decide
           ]},
          {learner_state, [], []},
          {proposer,
           [ {msg_accept, 5}, %% tries to send messages
             {on, 2}, %% spec for messages not tight enough
             {start_link, 2}, %% tries to spawn processes
             {start_paxosid, 6}, %% tries to send messages
             {start_paxosid, 7}, %% tries to send messages
             {stop_paxosids, 2}, %% tries to send messages
             {trigger, 2} %% tries to send messages
           ],
           [ {msg_prepare,4}, %% tries to send messages
             {start_new_higher_round,3}]}, %% tries to send messages
          {proposer_state, [], []}
        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_tx(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    Modules =
        [ {rdht_tx,
           [ {decode_value, 1} ], %% not every binary is an erlterm
           [ {collect_replies,2}, %% recv msgs
             {receive_answer,0}, %% recv msgs
             {do_reqs_on_tlog,3}, %% req keys maybe not in tlog
             {do_reqs_on_tlog_iter,4}, %% req keys maybe not in tlog
             {commit, 1} %% should work, but hangs
           ]},
          {rdht_tx_read,
           [ {abort, 3},
             {commit, 3},
             {init, 1},
             {on,2},
             {start_link, 1},
             {validate_prefilter, 1}, %% TODO: not a list error
             {validate, 2},
             {work_phase, 3}
           ],
           [ {quorum_read, 3}, %% needs collector pid
             {inform_client, 2}, %% needs collector pid
             %% split tlog types for client and rt:keys
             %% use feeder to avoid unknown as key
             {make_tlog_entry, 1}
           ]},
          {rdht_tx_read_state,[], []},
          {rdht_tx_write,
           [ {abort, 3},
             {commit, 3},
             {start_link, 1}, {init, 1}, {on,2},
             {validate_prefilter, 1}, %% TODO: not a list error
             {validate, 2},
             {work_phase, 3}
           ], []},
          {tx_item_state,
           [ {new, 3}, %% TODO: not a list error
             {new, 6} %% cannot create same length lists for zip
           ], []},
          {tx_op_beh,[], []},
          {tx_state, [], []},
          {tx_tlog,
           [ {new_entry, 5}, %% split tlog types for client and rt:keys
             {set_entry_key, 2} %% split tlog types for client and rt:keys
           ], []},
          {tx_tm_rtm,
           [ {commit, 4},
             {get_my, 2},
             {init, 1},
             {msg_commit_reply, 3},
             {on,2},
             {on_init,2},
             {start_link,2}
           ],
           [ {get_paxos_ids, 2}, %% requires item entries in dictionary
             {msg_tp_do_commit_abort,3}, %% tries to send
             {init_RTMs, 2}, %% tries to send
             {init_TPs, 2}, %% tries to send
             {inform_client, 3}, %% tries to send
             {inform_rtms, 3}, %% tries to send
             {inform_tps, 3}, %% tries to send
             {send_to_rtms, 2}, %% tries to send
             {state_subscribe, 2}, %% tries to create pids / envelopes
             {state_unsubscribe, 2} %% tries to create pids / envelopes
           ]}
          %% {tx_tp,[{init, 0}, {on_do_commit_abort_fwd, 6},
          %% {on_do_commit_abort, 3}, {on_init_TP, 2}]},
        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_util(_Config) ->
    Count = 1000,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {comm,
           [ {get_ip, 1}, %% cannot create correct envelopes
             {get_port, 1}, %% cannot create correct envelopes
             {init_and_wait_for_valid_pid, 0}, %% cannot start
             {is_local, 1}, %% cannot create correct envelopes
             {send, 2}, {send, 3}, %% cannot send msgs
             {send_local, 2}, {send_local_after, 3}, %% cannot send msgs
             {unpack_cookie, 2} %% cannot create correct envelopes
           ], []},
          {db_entry, [], []},
          %% {fix_queue, [], []}, %% queue as builtin type not supported yet

          %% {histogram, [], []}, %% error in add?
          {msg_queue, [], []},
          {pdb, [], []},
          {pid_groups,
           [ {add, 3}, %% same as above
             {init, 1}, %% tries to create existing ets table
             {join_as, 2}, %% tries to join with multiple groups/names
             {on, 2},
             {pids_to_names, 2}, %% sends remote messages
             {start_link, 0}
           ], []},
          {quorum, [], []},
          %% {rrd,
          %%  [ {dump, 1}, %% eats memory?!
          %%    {dump_with, 2}, %% needs fun
          %%    {dump_with, 3}, %% needs fun
          %%    {add, 3}, %% to slow for large timestamps?
          %%    {add_now, 2}, %% bad arith
          %%    {add_with, 4}, %% needs fun
          %%    {check_timeslot, 2}, %% to slow for large timestamps?
          %%    {check_timeslot_now, 1}, %% to slow for testing?
          %%    {get_value, 2}, %% returns more than the spec expects
          %%    {get_value_by_offset, 2}, %% returns more than the spec expects
          %%    {timing_with_hist_merge_fun, 3}, %% function_clause
          %%    {merge, 2}, %% needs same rrd type twice
          %%    {add_nonexisting_timeslots, 2} %% needs same rrd type twice
          %%  ],
          %%  [ {update_with, 5} %% needs fun
          %%    ...
          %%  ]},
          %%{statistics, [], []},
          {uid, [], []},
          {util,
           [ {collect_while, 1}, %% cannot create funs
             {debug_info, 0}, %% type spec not valid?
             {debug_info, 1}, %% type spec not valid?
             {dump3, 0}, %% type spec not valid
             {dumpX, 1}, {dumpX, 2}, %% type spec not valid?
             {extint2atom, 1}, %% type spec too wide
             {first_matching, 2}, %% cannot create funs
             {for_to, 3}, %% cannot create funs
             {for_to_ex, 3}, %% cannot create funs
             {for_to_ex, 4}, %% cannot create funs
             {for_to_fold, 5}, %% cannot create funs
             {gb_trees_foldl, 3}, %% cannot create funs
             {log, 2}, %% floats become to large and raise badarith
             {log2, 1}, %% floats become to large and raise badarith
             {logged_exec, 1}, %% not execute random strings
             {par_map, 2}, %% cannot create funs; tested via feeder
             {par_map, 3}, %% cannot create funs; tested via feeder
             {parallel_run, 5}, %% cannot create funs
             {pop_randomelem, 2}, %% list may be too short
             {pow, 2}, %% floats become to large and raise badarith
             {print_bits, 2}, %% cannot create funs
             {readable_utc_time, 1}, %% too slow for big ints; tested via feeder
             {repeat, 3}, {repeat, 4}, %% cannot create funs
             {sets_map, 2}, %% cannot create funs
             {smerge2, 3}, %% cannot create funs
             {smerge2, 4}, %% cannot create funs
             {sleep_for_ever, 0},
             {split_unique, 3}, %% cannot create funs
             {split_unique, 4}, %% cannot create funs
             {ssplit_unique, 3}, %% cannot create funs
             {ssplit_unique, 4}, %% cannot create funs
             {supervisor_terminate, 1}, %% could destroy the system
             {supervisor_terminate_childs, 1}, %% tester not ready for gb_trees
             {tc, 1}, {tc, 2}, {tc, 3}, %% don't call arbitrary functions
             {topDumpX, 1},
             {topDumpX, 3},
             {topDumpXEvery, 3},
             {topDumpXEvery, 5},
             {topDumpXEvery_helper, 4},
             {wait_for, 1}, %% cannot create funs
             {wait_for, 2}, %% cannot create funs
             {wait_for_process_to_die, 1}, %% could wait forever
             {wait_for_table_to_disappear, 1}, %% cannot create tids
             {zipfoldl, 5} %% cannot create funs
           ],
           [ {dump_extract_from_list,2}, %% wrong spec
             {dumpXNoSort,2}, %% needs fun
             {shuffle_helper,4}, %% badarg error? why?
             {gb_trees_largest_smaller_than_iter,3}, %% err: function_clause
             {'=:<_lists', 2}, %% need equal length lists
             {ssplit_unique_helper, 5}, %% needs fun
             {smerge2, 6}, %% needs fun
             {smerge2_helper, 7}, %% needs fun
             {i_repeat,5}, %% needs fun
             {parallel_collect,3}, %% needs fun
             {par_map_recv, 2}, %% receives messages
             {par_map_recv2, 2}, %% receives messages
             {collect_while,2}, %% needs fun
             {gb_trees_foldl_iter,3}, %% needs fun
             {default_dumpX_val_fun,2} %% spec too wide (must be tuple sometimes)
           ]}
        ],
    [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
      || {Mod, Excl, ExclPriv} <- Modules ],
%% feeders are found automatically - sample explicit call would be:
%%    tester:test(util, readable_utc_time, 1, 25, [with_feeder]),

%%    tester_helper:load_with_export_all(util),
%%    ct:pal("testing with export all"),
%%    tester:test(util, escape_quotes_, 2, 25),
%%    tester_helper:load_without_export_all(util),
    true.
