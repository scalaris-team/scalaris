%% @copyright 2012-2016 Zuse Institute Berlin

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
%% @doc    Unit tests varifying type constraints via random testing.
%% @end
%% @version $Id$
-module(type_check_SUITE).
-author('schintke@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [
            tester_type_check_api,
            tester_type_check_config,
%%            tester_type_check_dht_node,
            tester_type_check_math,
            tester_type_check_node,
            tester_type_check_paxos,
            tester_type_check_rrepair,
            tester_type_check_tx,
            tester_type_check_rdht_tx,
            tester_type_check_histogram,
            tester_type_check_util,
            tester_type_check_gossip,
            tester_type_check_mr
           ].
suite() -> [ {timetrap, {seconds, 480}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

tester_type_check_api(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    tester:register_type_checker({typedef, rdht_tx, encoded_value, []}, rdht_tx, is_encoded_value),
    tester:register_value_creator({typedef, rdht_tx, encoded_value, []}, rdht_tx, encode_value, 1),
    Modules =
        [ {api_dht, [], []},
          {api_dht_raw,
           [ {unreliable_lookup,2}, %% creates arb. messages
             {unreliable_get_key,3}, %% creates arb. IP-adresses
             {split_ring, 1} %% needs feeder to limit the input size
           ],
           [ {range_read,1}, %% bug in range_read?
             {range_read_loop,5}, %% receives msgs
             {delete_and_cleanup_timer,2} %% cannot create reference()
           ]},
          {api_monitor, [], []},
          {api_rdht, [], [ {delete_collect_results, 3} ]}, %% receives
          {api_tx,
           [ {get_system_snapshot, 0} %% receives msgs
           ], []},
          {api_mr,
           [ {start_job, 1} %% sends msgs
           ],
           [ {wait_for_results, 3}]} %%receives messages
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, rdht_tx, encoded_value, []}),
    tester:unregister_value_creator({typedef, rdht_tx, encoded_value, []}),
    true.

tester_type_check_config(_Config) ->
    Count = 500,
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {config,
           [ {cfg_is_list, 3}, %% needs a fun as parameter
             {cfg_is_tuple, 4}, %% needs a fun as parameter
             {cfg_test_and_error, 3}, %% needs a fun as parameter
             {init, 1} %% already initialised (config_ets table exists)
           ],
           [ {populate_db, 1} %% cannot create config filenames
           ]}
        ],
    %% These tests generate errors which would be too verbose.
    log:set_log_level(none),
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    log:set_log_level(config:read(log_level)),
    true.

%% tester_type_check_dht_node(_Config) ->
%%     Count = 1000,
%%     config:write(no_print_ring_data, true),
%%     tester:register_type_checker({typedef, intervals, interval}, intervals, is_well_formed),
%%     tester:register_value_creator({typedef, intervals, interval}, intervals, tester_create_interval, 1),
%%     Modules =
%%         [ %% {dht_node, [], [],}
%%           %% {dht_node_join, [], [],}
%%           %% {dht_node_lookup, [], [],}
%%           %% {dht_node_monitor, [], [],}
%%           %% {dht_node_move, [], [],}
%%           %% {dht_node_reregister, [], [],}
%%           {dht_node_state, [], []}
%%         ],
%%     [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
%%       || {Mod, Excl, ExclPriv} <- Modules ],
%%     tester:unregister_type_checker({typedef, intervals, interval}),
%%     tester:unregister_value_creator({typedef, intervals, interval}),
%%    true.

tester_type_check_gossip(_Config) ->
    unittest_helper:wait_for_stable_ring_deep(),
    Group = pid_groups:group_with(gossip),
    pid_groups:join_as(Group, gossip),
    Count = 250,
    config:write(no_print_ring_data, true),
    config:write(gossip_log_level_warn, debug),
    config:write(gossip_log_level_error, debug),
    tester:register_type_checker({typedef, intervals, interval, []}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, simple_interval, []}, intervals, is_well_formed_simple),
    tester:register_type_checker({typedef, intervals, continuous_interval, []}, intervals, is_continuous),
    tester:register_value_creator({typedef, intervals, interval, []}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, simple_interval, []}, intervals, tester_create_simple_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval, []}, intervals, tester_create_continuous_interval, 4),
    tester:register_type_checker({typedef, gossip_load, histogram, []}, gossip_load, is_histogram),
    tester:register_value_creator({typedef, gossip_load, round, []}, gossip_load, tester_create_round, 1),
    tester:register_value_creator({typedef, gossip_load, state, []}, gossip_load, tester_create_state, 11),
    tester:register_value_creator({typedef, gossip_load, histogram, []}, gossip_load, tester_create_histogram, 1),
    tester:register_value_creator({typedef, gossip_load, histogram_size, []}, gossip_load, tester_create_histogram_size, 1),
    tester:register_value_creator({typedef, gossip_load, load_data_list, []}, gossip_load, tester_create_load_data_list, 1),
    tester:register_value_creator({typedef, gossip, cb_module_name, []}, gossip, tester_create_cb_module_names, 1),
    %% tester:register_type_checker({typedef, gossip_cyclon, state, []}, gossip_cyclon, is_state),
    Modules = [
               {gossip,
            % excluded (exported functions)
            [   {start_link, 1}, % would start a lot of processes
                {start_gossip_task, 2}, % spec to wide
                {stop_gossip_task, 1}, % would prohibit subsequent tests
                {on_inactive, 2}, % too much interaction / spec to wide
                {on_active, 2}, % too much interaction / spec to wide
                {start_gen_component,5} %% unsupported types
            ],
            % excluded (private functions)
            [   {handle_msg, 2}, % spec to wide, sends messages
                {start_p2p_exchange, 4}, % would need valid peer
                {init_gossip_tasks, 2}, % Id-Version-Error in gossip_cyclon (see below)
                {init_gossip_task, 3}, % test via feeder
                {cb_init, 2}, % spec to wide (Args)
                {cb_select_data, 2}, % would need valid callback state
                {cb_select_reply_data, 6}, % would need valid callback state
                {cb_integrate_data, 5}, % would need valid callback state
                {cb_handle_msg, 3}, % spec to wide (Msg)
                {cb_web_debug_info, 2}, % would need valid callback state
                {cb_round_has_converged, 2}, % would need valid callback state
                {cb_notify_change, 4}, % would need valid callback state
                {cb_call, 4}, % spec to wide
                {check_round, 3}, % would need valid callback state
                {is_end_of_round, 2}, % would need valid callback state
                {state_update, 3} % tester can not create a value of type fun()
            ]},
          {gossip_load,
            % excluded (exported functions)
            [  {init, 1}, % tested via feeder
               {get_values_best, 1}, % tested via feeder
               {handle_msg, 2}, % would need valid dht_node_state, sends messages
               {select_data, 1}, % sends messages
               {select_reply_data, 4}, % sends messages
               {integrate_data, 3}, % sends messages
               {request_histogram, 2} % tested via feeder
            ],
            % excluded (private functions)
            [  {state_update, 3}, % cannot create funs
               {replace_skipped, 2},
               {init_histo, 3}, % needs DHTNodeState state
               {merge_histo, 2}, % tested via feeder
               {merge_bucket, 2}, % tested via feeder
               {request_node_details, 1} % sends messages
            ]},
            {gossip_cyclon,
            % excluded (exported functions)
            [
             %% Id-Version-Error 1:
             %%     'got two nodes with same IDversion but different ID' in node:is_newer()
             {init, 1}, % needs valid neighborhood / Id-Version-Error
             {select_data, 1}, % tested via feeder
             {select_reply_data, 4}, % Id-Version-Error
             {integrate_data, 3}, % Id-Version-Error
             {handle_msg, 2} % needs valid NodeDetails (in get_node_details_response)
            ],
            % excluded (private functions)
            [
             {request_node_details, 1}, % tested via feeder
             {print_cache_dot, 2} % to much console output
            ]}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, intervals, interval, []}),
    tester:unregister_type_checker({typedef, intervals, simple_interval, []}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval, []}),
    tester:unregister_value_creator({typedef, intervals, interval, []}),
    tester:unregister_value_creator({typedef, intervals, simple_interval, []}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval, []}),
    tester:unregister_value_creator({typedef, gossip_load, round, []}),
    tester:unregister_value_creator({typedef, gossip_load, state, []}),
    tester:unregister_value_creator({typedef, gossip_load, load_data_list, []}),
    tester:unregister_value_creator({typedef, gossip_load, histogram, []}),
    tester:unregister_value_creator({typedef, gossip_load, histogram_size, []}),
    tester:unregister_type_checker({typedef, gossip_load, histogram, []}),
    tester:unregister_value_creator({typedef, gossip, cb_module_name, []}),
    %% tester:unregister_type_checker({typedef, gossip_cyclon, state, []}),
    true.


tester_type_check_math(_Config) ->
    Count = 250,
    config:write(no_print_ring_data, true),
    tester:register_type_checker({typedef, intervals, interval, []}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, simple_interval, []}, intervals, is_well_formed_simple),
    tester:register_type_checker({typedef, intervals, continuous_interval, []}, intervals, is_continuous),
    tester:register_type_checker({typedef, prime, prime_list, []}, prime, tester_is_prime_list),
    tester:register_type_checker({typedef, prime, prime, []}, prime, is_prime),
    tester:register_value_creator({typedef, intervals, interval, []}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, simple_interval, []}, intervals, tester_create_simple_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval, []}, intervals, tester_create_continuous_interval, 4),
    Modules =
        [ {intervals,
           [ {get_bounds, 1}, %% throws exception on []
             {new, 4}, %% type spec to wide (would need overlapping contract support)
             {split, 2} %% integers too large; tested via feeder
           ],
           [ {minus_simple2, 2}, %% second is subset of first param
             {split2, 7} %% special list of split keys; tested via feeder
           ]},
          {mathlib,
           [ {vecWeightedAvg,4}, %% needs same length lists
             {closestPoints, 1}, %% needs same length lists
             {binomial_coeff, 2}, %% needs N > K, done by feeder
             {factorial, 1}, %% slow for large integers, done by feeder
             {aggloClustering, 2}, %% needs same length lists
             {vecAdd, 2}, %% needs same length lists
             {vecSub, 2}, %% needs same length lists
             {euclideanDistance, 2}, %% needs same length lists
             {nearestCentroid, 2}, %% needs proper centroids
             {u, 1}, %% needs non zero number in list
             {zeros, 1} %% slow for large integers, tested via feeder
           ],
           [ {aggloClusteringHelper, 5}, %% spec suspicious (-1 for lists:nth())
             {choose, 4}, %% slow for large integers
             {factorial, 2} %% slow for large integers, done by feeder
           ]},
          %% {math_pos, [], []}, %% needs valid pos fields
          {prime,
           [ {get_nearest, 1}, %% too slow for large integers, tested via feeder
             {is_prime, 1} %% too slow for large integers, tested via feeder
           ],
           [ {init, 0}, %% only once for initialisation
             {sieve_num, 3}, %% throws if no prime is found
             {sieve_filter, 3}, %% slow for large gaps between integers in the list
             {find_in_cache, 2}, %% pre-condition between parameters must be met
             {prime_cache, 0} %% there really is no point in testing this function!
           ]},
          {randoms,
           [ {start, 0},
             {stop, 0},
             {rand_uniform, 2}, % tested via feeder
             {rand_uniform, 3}  % tested via feeder
           ]}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, intervals, interval, []}),
    tester:unregister_type_checker({typedef, intervals, simple_interval, []}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval, []}),
    tester:unregister_type_checker({typedef, prime, prime_list, []}),
    tester:unregister_type_checker({typedef, prime, prime, []}),
    tester:unregister_value_creator({typedef, intervals, interval, []}),
    tester:unregister_value_creator({typedef, intervals, simple_interval, []}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval, []}),
    true.

tester_type_check_node(_Config) ->
    Count = 250,
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
          [ {new_neighborhood, 2}, % the two given nodes must have different PIDs
            {new_neighborhood, 3}, % the pred/succ nodes must have different PIDs than the base node
            {mk_nodelist, 2}, % base node must not be in node list
            {mk_neighborhood, 2}, % base node must not be in node list
            {mk_neighborhood, 4}, % base node must not be in node list
            {add_node, 4}, % base node must not be in node list
            {add_nodes, 4}, % base node must not be in node list
            {lremove, 3}, %% cannot create funs
            {lremove_outdated, 1}, % base node must not be in node list
            {lremove_outdated, 2}, % base node must not be in node list
            {lfilter_min_length, 3}, %% cannot create funs
            {filter_min_length, 4}, %% cannot create funs
            {lfilter, 2}, %% cannot create funs
            {lfilter, 3}, %% cannot create funs
            {filter, 2}, %% cannot create funs
            {filter, 3}, %% cannot create funs
            {update_ids, 2}, % base node must not be in node list
            {lupdate_ids, 2}, % base node must not be in node list
            {update_node, 2}, %% needs node in certain interval
            {merge, 4}, % base node must not be in node list of the other neighbour's object
            {remove, 3}, %% cannot create funs
            {create_pid_to_node_dict, 2} %% needs a dict() of node() objects
          ],
          [ {throw_if_newer, 2}, %% throws
            {lsplit_nodelist, 2}, %% base node must not be in node list
            {lusplit_nodelist, 2}, %% base node must not be in node list
            {lmerge_helper, 5} %% base node must not be in node list
          ]}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_paxos(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    Modules =
        [ {acceptor,
           [ {add_learner,3}, %% tries to send messages
             {msg_accepted, 4}, %% tries to send messages
             {on, 2}, %% spec for messages not tight enough
             {start_link,2}, %% tries to spawn processes
             {start_paxosid, 2}, %% tries to send messages
             {start_paxosid, 3}, %% tries to send messages
             {stop_paxosids,2}, %% tries to send messages
             {start_gen_component,5} %% unsupported types
           ],
           [ {msg_ack,5}, %% sends msgs
             {msg_nack,3}, %% sends msgs
             {msg_naccepted,3}, %% sends msgs
             {initialize,4}, %% sends msgs
             {inform_learners,2}, %% sends msgs
             {inform_learner,3} %% sends msgs
           ]},
          {learner,
           [ {on, 2}, %% spec for messages not tight enough
             {start_link,2}, %% tries to spawn processes
             {start_paxosid, 5}, %% tries to send messages
             {stop_paxosids,2}, %% tries to send messages
             {start_gen_component,5} %% unsupported types
           ],
           [ {msg_decide,4}, %% sends msg.
             {decide, 2} %% no spec & uses msg_decide
           ]},
          {proposer,
           [ {msg_accept, 5}, %% tries to send messages
             {on, 2}, %% spec for messages not tight enough
             {start_link, 2}, %% tries to spawn processes
             {start_paxosid, 6}, %% tries to send messages
             {start_paxosid, 7}, %% tries to send messages
             {stop_paxosids, 2}, %% tries to send messages
             {trigger, 2}, %% tries to send messages
             {start_gen_component,5} %% unsupported types
           ],
           [ {msg_prepare,4}, %% tries to send messages
             {proposer_trigger, 4}, %% tries to send messages
             {start_new_higher_round,3}, %% tries to send messages
             {state_add_ack_msg, 4} %% tested via feeder
           ]}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    true.

tester_type_check_rrepair(_Config) ->
    Count = 250,
    config:write(no_print_ring_data, true),
    tester:register_type_checker({typedef, intervals, interval, []}, intervals, is_well_formed),
    tester:register_type_checker({typedef, intervals, continuous_interval, []}, intervals, is_continuous),
    tester:register_type_checker({typedef, intervals, non_empty_interval, []}, intervals, is_non_empty),
    tester:register_type_checker({typedef, rt_beh, segment, []}, rt_beh, tester_is_segment),
    tester:register_type_checker({typedef, rr_recon, kvi_tree, []}, rr_recon, tester_is_kvi_tree),
    tester:register_value_creator({typedef, random_bias, generator, []},
                                  random_bias, tester_create_generator, 3),
    tester:register_value_creator({typedef, intervals, interval, []}, intervals, tester_create_interval, 1),
    tester:register_value_creator({typedef, intervals, continuous_interval, []}, intervals, tester_create_continuous_interval, 4),
    tester:register_value_creator({typedef, intervals, non_empty_interval, []}, intervals, tester_create_non_empty_interval, 2),
    tester:register_value_creator({typedef, merkle_tree, leaf_hash_fun, []}, merkle_tree, tester_create_hash_fun, 1),
    tester:register_value_creator({typedef, merkle_tree, inner_hash_fun, []}, merkle_tree, tester_create_inner_hash_fun, 1),
    tester:register_value_creator({typedef, hfs_beh, hfs_fun, []}, hfs_beh, tester_create_hfs_fun, 1),
    tester:register_value_creator({typedef, hfs_lhsp, hfs, []}, hfs_lhsp, tester_create_hfs, 1),
    tester:register_value_creator({typedef, hfs_plain, hfs, []}, hfs_plain, tester_create_hfs, 1),
    tester:register_value_creator({typedef, rt_beh, segment, []}, rt_beh, tester_create_segment, 1),
    tester:register_value_creator({typedef, rr_recon, kvi_tree, []}, rr_recon, tester_create_kvi_tree, 1),
    Modules =
        [ {rr_recon_stats, [], []},
          {db_generator,
           [ {get_db, 3}, %% tested via feeder
             {get_db, 4}, %% tested via feeder
             {fill_ring, 3} %% tested via feeder
           ],
           [ {fill_random, 2}, %% tested via feeder
             {fill_wiki, 2}, %% no (suitable) wiki file to import
             {gen_kvv, 3}, %% tested via feeder
             {p_gen_kvv, 6}, %% tested via feeder
             {get_error_key, 2}, %% keys must be replica keys, i.e. one per quadrant!

             {gen_random, 3}, %% needs feeder
             {gen_random_gb_sets, 5}, %% needs feeder
             {uniform_key_list, 3}, %% needs feeder
             {uniform_key_list_no_split, 3}, %% needs feeder
             {non_uniform_key_list, 5}, %% needs feeder
             {non_uniform_key_list_, 7}, %% needs feeder
             {get_non_uniform_probs, 1} %% needs feeder
           ]},
          {hfs_lhsp,
           [ {apply_val, 3} %% tested via feeder
           ],
           [ {apply_val_helper, 3}, %% tested via feeder
             {apply_val_rem_helper, 4} %% tested via feeder
           ]},
          {hfs_plain,
           [ {apply_val, 3}, %% tested via feeder
             {apply_val_rem, 3} %% tested via feeder
           ],
           [ {hash_value, 4}, %% needs bit sizes to fit, already tested via the apply_val* functions
             {split_bin, 3} %% bitstring needs to have a minimum size
           ]},
          {rr_recon,
           [
             {init, 1}, %% registers a monitor (only one allowed per PID)
             {on, 2}, %% tries to send messages, needs valid state with pid
             {start, 2}, %% tries to spawn processes
             {map_rkeys_to_quadrant, 2}, %% keys must be replica keys, i.e. one per quadrant!
             {map_interval, 2}, %% second interval must be in a single quadrant

             {merkle_compress_hashlist, 4}, %% needs merkle nodes with hashes
             {merkle_decompress_hashlist, 3}, %% needs a special binary to correspond to a number of bits
             {pos_to_bitstring, 4}, % needs to fulfil certain preconditions
             {bitstring_to_k_list_k, 3}, % needs a special binary to correspond to a number of Key entries
             {bitstring_to_k_list_kv, 3}, % needs a special binary to correspond to a number of KV entries
             {calc_n_subparts_FR, 2}, %% needs float >= 0
             {calc_n_subparts_FR, 3}, %% needs float >= 0
             {start_gen_component,5} %% unsupported types
           ],
           [
             {check_percent, 1}, %% checks arbitrary config -> too many unnecessary error messages
             {build_struct, 3}, %% tries to send messages, needs valid state with pid
             {build_recon_struct, 5}, %% DB items must be in interval
             {begin_sync, 1}, %% tries to send messages
             {shutdown, 2}, %% tries to send messages
             {merkle_next_signature_sizes, 5}, %% needs float > 0, < 1
             {min_max, 3}, %% tested via feeder
             {trivial_signature_sizes, 4}, %% needs float > 0, < 1
             {trivial_worst_case_failrate, 4}, %% needs 0 =< ExpDelta =< 100
             {shash_signature_sizes, 4}, %% needs float > 0, < 1
             {shash_worst_case_failrate, 4}, %% needs 0 =< ExpDelta =< 100
             {calc_one_m_xpow_one_m_z, 2}, %% needs X =/= 0
             {calc_max_different_hashes_, 3}, %% needs 0 =< ExpDelta =< 100
             {calc_max_different_hashes, 3}, %% needs 0 =< ExpDelta =< 100
             {calc_max_different_items_total, 3}, %% needs 0 =< ExpDelta =< 100
             {calc_max_different_items_node, 3}, %% needs 0 =< ExpDelta =< 100
             {compress_kv_list, 5}, %% needs a fun
             {compress_kv_list_fr, 7}, %% needs float > 0, < 1 and a fun
             {bloom_worst_case_failrate, 3}, %% needs float > 0, < 1, 0 =< ExpDelta =< 100
             {bloom_worst_case_failrate_, 5}, %% needs float > 0, < 1, 0 =< ExpDelta =< 100
             {bloom_target_fp, 4}, %% needs float > 0, < 1
             {merkle_next_fr_targets, 2}, %% needs float > 0, < 1
             {get_diff_with_dupes, 7}, %% needs a function
             {calc_items_in_chunk, 2}, %% needs special input
             {decompress_kv_list, 3}, %% needs a special binary to correspond to a number of bits
             {compress_idx_list, 6}, %% needs a sorted list of positions, also LastPos needs to be smaller than these positions
             {decompress_idx_list, 3}, %% needs a special binary to correspond to a number of bits
             {decompress_idx_list_, 4}, %% needs a special binary to correspond to a number of bits
             {decompress_idx_to_list, 2}, %% needs a special binary to correspond to a number of bits
             {decompress_idx_to_list_, 3}, %% needs a special binary to correspond to a number of bits
             {shash_bloom_perform_resolve, 7}, %% needs a special binary to correspond to a number of bits
             {phase2_run_trivial_on_diff, 6}, %% needs parameters to match
             {merkle_check_node, 22}, %% needs merkle_tree/nodes with hashes
             {merkle_cmp_result, 21}, %% needs matching result and merkle nodes
             {merkle_calc_used_fr, 5}, %% needs floats >= 0, =< 1
             {merkle_resolve_add_leaf_hashes, 14}, %% needs KV-List merkle buckets
             {merkle_resolve_retrieve_leaf_hashes, 17}, %% needs special bitstring
             {merkle_resolve_leaves_send, 2}, % needs only leaf nodes in node list
             {merkle_resolve_leaves_receive, 2}, % needs only leaf nodes in node list
             {merkle_resolve_leaves_ckidx, 8}, % needs same-length lists
             {send_resolve_request, 6}, %% tries to send messages
             {art_get_sync_leaves, 6}, %% needs non-empty bloom filters
             {send, 2}, %% tries to send messages
             {send_local, 2}, %% tries to send messages
             {send_chunk_req, 4}, %% tries to send messages
             {quadrant_intervals_, 3}, %% special pre-conditions, only private to quadrant_intervals/0, tested enough in there
             {replicated_intervals, 1} %% interval must be in a single quadrant
           ]},
          {rr_resolve,
           [
             {init, 1}, %% registers a monitor (only one allowed per PID)
             {on, 2}, %% tries to send messages, needs valid state with pid
             {start, 1}, %% tries to spawn processes
             {start_gen_component,5}, %% unsupported types
             {merge_stats, 2} %% tested via feeder
           ],
           [
             {shutdown, 2}, %% tries to send messages
             {send_request_resolve, 6}, %% tries to send messages
             {send, 2}, %% tries to send messages
             {send_local, 2}, %% tries to send messages

             {start_update_key_entries, 3}, %% tries to send messages
             {map_kvv_list, 2}, %% needs a unique tuple list, e.g. via feeder
             {map_key_list, 2} %% needs a unique key list, e.g. via feeder
           ]}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_value_creator({typedef, merkle_tree, leaf_hash_fun, []}),
    tester:unregister_value_creator({typedef, merkle_tree, inner_hash_fun, []}),
    tester:unregister_value_creator({typedef, random_bias, generator, []}),
    tester:unregister_value_creator({typedef, intervals, interval, []}),
    tester:unregister_value_creator({typedef, intervals, continuous_interval, []}),
    tester:unregister_value_creator({typedef, intervals, non_empty_interval, []}),
    tester:unregister_value_creator({typedef, hfs_beh, hfs_fun, []}),
    tester:unregister_value_creator({typedef, hfs_lhsp, hfs, []}),
    tester:unregister_value_creator({typedef, hfs_plain, hfs, []}),
    tester:unregister_value_creator({typedef, rt_beh, segment, []}),
    tester:unregister_value_creator({typedef, rr_recon, kvi_tree, []}),
    tester:unregister_type_checker({typedef, intervals, interval, []}),
    tester:unregister_type_checker({typedef, intervals, continuous_interval, []}),
    tester:unregister_type_checker({typedef, intervals, non_empty_interval, []}),
    tester:unregister_type_checker({typedef, rt_beh, segment, []}),
    tester:unregister_type_checker({typedef, rr_recon, kvi_tree, []}),
    true.

tester_type_check_tx(_Config) ->
    Count = 250,
    config:write(no_print_ring_data, true),
    tester:register_type_checker({typedef, rdht_tx, encoded_value, []}, rdht_tx, is_encoded_value),
    tester:register_value_creator({typedef, rdht_tx, encoded_value, []}, rdht_tx, encode_value, 1),
    Modules =
        [ {tx_op_beh,[], []},
          {tx_tlog,
           [ {new_entry, 5}, %% TODO: some combinations of value types are not allowed
             {new_entry, 6}, %% TODO: some combinations of value types are not allowed
             {new_entry, 7}, %% TODO: some combinations of value types are not allowed
             {set_entry_key, 2}, %% split tlog types for client and rt:keys
             {set_entry_operation, 2}, %% may violate type spec (?partial_value in ?write op) (TODO: prevent via feeder)
             {set_entry_value, 3} %% may violate type spec (?partial_value in ?write op) (TODO: prevent via feeder)
           ],
           [ {read_op_for_key, 5} %% no type spec available (a 1-element list may not be specified anyway)
           ]},
          {tx_tm_rtm,
           [ {commit, 4},
             {get_my, 2},
             {init, 1},
             {msg_commit_reply, 3},
             {on,2},
             {on_init,2},
             {start_link,2},
             {start_gen_component,5} %% unsupported types
           ],
           [ {get_paxos_ids, 2}, %% requires item entries in dictionary
             {get_failed_keys, 2}, %% needs number of aborts in item list to match numabort
             {msg_tp_do_commit_abort,4}, %% tries to send
             {init_RTMs, 2}, %% tries to send
             {init_TPs, 3}, %% tries to send
             {inform_client, 3}, %% tries to send
             {inform_rtms, 3}, %% tries to send
             {inform_tps, 3}, %% tries to send
             {send_to_rtms, 2}, %% tries to send
             {merge_item_states, 6}, %% needs specially-crafted lists
             {tx_item_new, 3}, %% TODO: not a list error
             {tx_item_new, 5} %% TODO invalid result type
           ]}
          %% {tx_tp,[{init, 0}, {on_do_commit_abort_fwd, 6},
          %% {on_do_commit_abort, 3}, {on_init_TP, 2}]},
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, rdht_tx, encoded_value, []}),
    tester:unregister_value_creator({typedef, rdht_tx, encoded_value, []}),
    true.

tester_type_check_rdht_tx(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    tester:register_type_checker({typedef, rdht_tx, encoded_value, []}, rdht_tx, is_encoded_value),
    tester:register_value_creator({typedef, rdht_tx, encoded_value, []}, rdht_tx, encode_value, 1),
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
           [ {abort, 5},
             {commit, 5},
             {extract_from_value, 3}, %% tested via feeder
             {extract_from_tlog, 4}, %% tested via feeder
             {init, 1},
             {on,2},
             {start_link, 1},
             {start_gen_component,5}, %% unsupported types
             {validate_prefilter, 1}, %% TODO: not a list error
             {validate, 3},
             {work_phase, 3}
           ],
           [ {quorum_read, 4}, %% needs collector pid
             {make_tlog_entry, 2} %% tested via feeder
           ]},
          {rdht_tx_write,
           [ {abort, 5},
             {commit, 5},
             {start_link, 1}, {init, 1}, {on,2},
             {start_gen_component,5}, %% unsupported types
             {validate_prefilter, 1}, %% TODO: not a list error
             {validate, 3},
             {work_phase, 3}
           ], []},
          {rdht_tx_add_del_on_list,
           [ {extract_from_tlog, 5}, %% tested via feeder
             {work_phase, 3}
           ], []},
          {rdht_tx_add_on_nr,
           [ {extract_from_tlog, 4}, %% tested via feeder
             {work_phase, 3}
           ], []},
          {rdht_tx_test_and_set,
           [ {extract_from_tlog, 5}, %% tested via feeder
             {work_phase, 3}
           ], []}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, rdht_tx, encoded_value, []}),
    tester:unregister_value_creator({typedef, rdht_tx, encoded_value, []}),
    true.

tester_type_check_histogram(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    tester:register_type_checker({typedef, histogram, histogram, []}, histogram, tester_is_valid_histogram),
    tester:register_value_creator({typedef, histogram, histogram, []}, histogram, tester_create_histogram, 2),
    tester:register_value_creator({typedef, histogram_rt, histogram, []}, histogram_rt, tester_create_histogram, 2),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {histogram,
           [ {find_smallest_interval, 1}, % private API, needs feeder
             {merge_interval, 2} % private API, needs feeder
           ],
           [ {resize, 1} % needs feeder
           ]},
          {histogram_rt,
           [],
           [ {denormalize, 2} % TODO needs feeder according to ?RT:get_range/2 output
           ]
          }
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, histogram, histogram, []}),
    tester:unregister_value_creator({typedef, histogram, histogram, []}),
    tester:unregister_value_creator({typedef, histogram_rt, histogram, []}),
    true.

tester_type_check_util(_Config) ->
    Count = 250,
    config:write(no_print_ring_data, true),
    %% [{modulename, [excludelist = {fun, arity}]}]
    Modules =
        [ {comm,
           [ {get_ip, 1}, %% cannot create correct envelopes
             {get_port, 1}, %% cannot create correct envelopes
             {init_and_wait_for_valid_IP, 0}, %% cannot start
             {is_local, 1}, %% cannot create correct envelopes
             {send, 2}, {send, 3}, %% cannot send msgs
             {send_local, 2}, {send_local_after, 3}, %% cannot send msgs
             {forward_to_group_member, 2}, %% may forward arbitrary message to any process
             {forward_to_registered_proc, 2}, %% may forward arbitrary message to any process
             {reply_as, 3} %% needs feeder for envelope
           ], []},
          {db_entry,
           [ {inc_version, 1}, % WL -1 is only allowed for empty_val
             {dec_version, 1}, % WL -1 is only allowed for empty_val
             {set_value, 3} % WL -1 is only allowed for empty_val
           ], []},
          {debug,
           [ {get_round_trip, 2}, %% needs gen_component pids
             {dump3, 0}, %% type spec not valid
             {dumpX, 1}, {dumpX, 2}, %% type spec not valid?
             {topDumpX, 1},
             {topDumpX, 3},
             {topDumpXEvery, 3},
             {topDumpXEvery, 5},
             {topDumpXEvery_helper, 4}
           ],
           [ {get_round_trip_helper, 2}, %% needs gen_component pids
             {dump_extract_from_list,2}, %% wrong spec
             {dumpXNoSort,2}, %% needs fun
             {default_dumpX_val_fun,2}, %% spec too wide (must be tuple sometimes),
             {rr_count_old_replicas_data, 1} %% needs dht_node pid
           ]},
          %% {fix_queue, [], []}, %% queue as builtin type not supported yet

          {mymaps,
           [ {get, 2}, % throws if the value does not exist
             {update, 3} % throws if the value does not exist
           ], []},
          {msg_queue, [], []},
          {pdb, [], []},
          {pid_groups,
           [ {add, 3}, %% same as above
             {init, 1}, %% tries to create existing ets table
             {join_as, 2}, %% tries to join with multiple groups/names
             {on, 2},
             {pids_to_names, 2}, %% sends remote messages
             {filename_to_group, 1}, %% not every string is convertible
             {start_link, 0},
             {start_gen_component,5} %% unsupported types
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
             {do_throw, 1}, %% throws exceptions
             {extint2atom, 1}, %% type spec too wide
             {for_to, 3}, %% cannot create funs
             {for_to_ex, 3}, %% cannot create funs
             {for_to_ex, 4}, %% cannot create funs
             {for_to_fold, 5}, %% cannot create funs
             {gb_trees_foldl, 3}, %% cannot create funs
             {lists_takewith, 2}, %% cannot create funs; tested via feeder
             {lists_keystore2, 5}, %% key id may not be larger than the tuple size in the list
             {lists_partition3, 2}, %% cannot create funs; tested via feeder
             {lists_remove_at_indices, 2}, %% indices must exist in list
             {log, 2}, %% tested via feeder
             {log2, 1}, %% tested via feeder
             {log1p, 1}, %% tested via feeder
             {pow1p, 2}, %% tested via feeder
             {logged_exec, 1}, %% not execute random strings
             {map_with_nr, 3}, %% cannot create funs; tested via feeder
             {par_map, 2}, %% cannot create funs; tested via feeder
             {par_map, 3}, %% cannot create funs; tested via feeder
             {parallel_run, 5}, %% cannot create funs
             {pop_randomelem, 2}, %% list may be too short
             {pow, 2}, %% floats become too large and raise badarith
             {print_bits, 2}, %% cannot create funs
             {readable_utc_time, 1}, %% too slow for big ints; tested via feeder
             {repeat, 3}, {repeat, 4}, %% cannot create funs
             {round, 2}, %% floats become too large and raise badarith
             {sets_map, 2}, %% cannot create funs
             {smerge2, 3}, %% cannot create funs
             {smerge2, 4}, %% cannot create funs
             {smerge2, 6}, %% cannot create funs
             {sleep_for_ever, 0},
             {split_unique, 3}, %% cannot create funs
             {split_unique, 4}, %% cannot create funs
             {ssplit_unique, 3}, %% cannot create funs
             {ssplit_unique, 4}, %% cannot create funs
             {tc, 1}, {tc, 2}, {tc, 3}, %% don't call arbitrary functions
             {wait_for, 1}, %% cannot create funs
             {wait_for, 2}, %% cannot create funs
             {wait_for_process_to_die, 1}, %% could wait forever
             {wait_for_ets_table_to_disappear, 2}, %% cannot create tids
             {zipfoldl, 5}, %% cannot create funs
             {rrd_combine_timing_slots, 3}, %% values too big
             {rrd_combine_timing_slots, 4}, %% values too big
             {rrd_combine_gauge_slots, 3}, %% values too big
             {rrd_combine_gauge_slots, 4}, %% values too big
             {rrd_combine_slots, 6} %% values too big
           ],
           [ {lists_takewith_iter, 3}, %% cannot create funs; tested via feeder
             {lists_partition3, 5}, %% cannot create funs; tested via feeder
             {lists_remove_at_indices, 3}, %% indices must exist in list
             {shuffle_helperA, 3}, %% preconds must be fulfilled
             {gb_trees_largest_smaller_than_iter,3}, %% err: function_clause
             {'=:<_lists', 2}, %% need equal length lists
             {ssplit_unique_helper, 5}, %% needs fun
             {smerge2_helper, 6}, %% needs fun
             {i_repeat,5}, %% needs fun
             {parallel_collect,3}, %% needs fun
             {par_map_recv, 2}, %% receives messages
             {par_map_recv2, 2}, %% receives messages
             {sublist_, 4}, %% tested via feeder
             {pow1p_, 4}, %% already tested by root1p/2
             {bin_op, 3}, %% cannot create funs
             {bin_op, 4}, %% cannot create funs
             {wait_for1, 2}, %% cannot create funs
             {wait_for2, 2}, %% cannot create funs
             {collect_while,2}, %% needs fun
             {gb_trees_foldl_iter,3} %% needs fun
           ]}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
%% feeders are found automatically - sample explicit call would be:
%%    tester:test(util, readable_utc_time, 1, 25, [with_feeder]),

%%    tester_helper:load_with_export_all(util),
%%    ct:pal("testing with export all"),
%%    tester:test(util, escape_quotes_, 2, 25),
%%    tester_helper:load_without_export_all(util),
    true.

tester_type_check_mr(_Config) ->
    Count = 500,
    config:write(no_print_ring_data, true),
    tester:register_type_checker({typedef, mr_state, fun_term, []}, mr_state,
                                 tester_is_valid_funterm),
    tester:register_value_creator({typedef, mr_state, fun_term, []}, mr_state,
                                  tester_create_valid_funterm, 2),
    Modules =
        [ {mr_state,
           [
            {new, 6}, %% needs fun
            {add_data_to_phase, 4}, %% needs ets table
            {clean_up, 1}, %% closes ets tables
            {accumulate_data, 2}, %% needs ets tables
            {get_slide_delta, 2}, %% needs ets tables
            {init_slide_state, 1}, %% needs ets tables
            {merge_states, 2} %% needs ets tables
           ],
           [
            {trigger_work, 2}, %% sends message
            {merge_phase_delta, 2}, %% needs ets tables
            {acc_add_element, 2} %% needs ets tables
           ]}
          %% , {mr_master, [{on, 2}, {init, 1}, {start_link, 2}], []}
        ],
    _ = [ tester:type_check_module(Mod, Excl, ExclPriv, Count)
          || {Mod, Excl, ExclPriv} <- Modules ],
    tester:unregister_type_checker({typedef, mr_state, fun_term, []}),
    tester:unregister_value_creator({typedef, mr_state, fun_term, []}),
    true.
