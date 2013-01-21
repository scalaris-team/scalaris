%% @copyright 2012, 2013 Zuse Institute Berlin

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
            rbr_consistency,%,
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
        rbr_consistency ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring_with_ids(?RT:get_replica_keys(0),
                                               [{config, [{log_path, PrivDir}]}]),
            %% necessary for the consistency check:
            unittest_helper:check_ring_size_fully_joined(4),

            Config;
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(1,
                                      [{config, [{log_path, PrivDir},
                                                 {leases, true}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

rbr_consistency(_Config) ->
    %% create an rbr entry
    %% update 1 to 3 of its replicas
    %% perform read in all quorum permutations
    %% (intercept read on a single dht node)
    %% output must be the old value or the new value
    %% if the new value was seen once, the old must not be readable again

    Nodes = pid_groups:find_all(dht_node),
    Key = "a",

    %% initialize key
    kv_on_cseq:write(Key, 1),

    %% select a replica
    Replicas = ?RT:get_replica_keys(?RT:hash_key(Key)),

    [ begin
          New = N+100,
          Old = case N of
                    1 -> 1;
                    _ -> N+99
                end,

          modify_rbr_at_key(R, N+100),

          %% intercept and drop a message at r1
          lists:foldl(read_quorum_without(Key), {Old, New}, Nodes)
      end || {R,N} <- lists:zip(Replicas, lists:seq(1,4))],

    ok.

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

modify_rbr_at_key(R, N) ->
    %% get a valid round number
    comm:send_local(pid_groups:find_a(dht_node),
                    {?lookup_aux, R, 0,
                     {prbr, read, kv, comm:this(),
                      R, unittest_rbr_consistency1,
                      fun prbr:noop_read_filter/1}}),
    receive
        {read_reply, AssignedRound, _, _} ->
            ok
    end,
    %% perform a write
    comm:send_local(pid_groups:find_a(dht_node),
                    {?lookup_aux, R, 0,
                     {prbr, write, kv, comm:this(),
                      R, AssignedRound, {[], false, N+1, N}, null,
                      fun prbr:noop_write_filter/3}}),
    receive
        {write_reply, R, _} ->
            ok
    end.

drop_prbr_read_request(Client, Tag) ->
    fun (Message, _State) ->
            case Message of
%%                {prbr, _, kv, ReqClient, Key, _Round, _RF} ->
                _ when element(1, Message) =:= prbr
                       andalso element(3, Message) =:= kv ->
                    ct:pal("Detected read, dropping it ~p, key ~p~n",
                           [self(), element(5, Message)]),
                    comm:send_local(Client, {Tag, done}),
                    drop_single;
                _ when element(1, Message) =:= prbr ->
                    false;
                _ -> false
            end
    end.

read_quorum_without(Key) ->
    fun (X, {Old, New}) ->
            gen_component:bp_set_cond(
              X,
              drop_prbr_read_request(self(), drop_prbr_read),
              drop_prbr_read),

            {ok, Val} = kv_on_cseq:read(Key),
            receive
                {drop_prbr_read, done} ->
                    gen_component:bp_del(X, drop_prbr_read),
                    ok
            end,
            cleanup({drop_prbr_read, done}),
            case Val of
                Old ->
                    {Old, New}; %% valid for next read
                New ->
                    {New, New}; %% old is no longer acceptable
                _ -> ?equals(Val, New)
            end
    end.

cleanup(Msg) ->
    receive Msg -> cleanup(Msg)
    after 0 -> ok
    end.
