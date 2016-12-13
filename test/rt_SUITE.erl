% @copyright 2010-2016 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Unit tests for the current ?RT module.
%% @end
-module(rt_SUITE).
-author('kruber@zib.de').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").
-include("client_types.hrl").

all() ->
    [tester_client_key_to_binary, tester_hash_key,
     next_hop, next_hop2,
     tester_get_split_key, tester_get_split_key_half,
     additional_tests].

suite() ->
    [
     {timetrap, {seconds, 40}}
    ].

register_value_creator() ->
    case lists:member(?RT, [rt_chord, rt_frt, rt_gfrt, rt_simple]) of
        true -> ok;
        false -> tester:register_value_creator({typedef, ?RT, key, []},
                                               ?RT, create_key, 2)
    end.

unregister_value_creator() ->
    case lists:member(?RT, [rt_chord, rt_frt, rt_gfrt, rt_simple]) of
        true -> ok;
        false -> tester:unregister_value_creator({typedef, ?RT, key, []})
    end.

init_per_suite(Config) ->
    register_value_creator(),
    unittest_helper:start_minimal_procs(Config, [], true).

end_per_suite(Config) ->
    unregister_value_creator(),
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

%% @doc Returns whether the default routing table from ?RT has chord-like keys.
-spec default_rt_has_chord_keys() -> boolean().
default_rt_has_chord_keys() ->
    lists:member(?RT, [rt_simple, rt_chord, rt_gfrtchord, rt_gfrtchord]).

-spec prop_client_key_to_binary(Key1::client_key(), Key2::client_key()) -> true | no_return().
prop_client_key_to_binary(Key1, Key2) ->
    Bin1 = ?RT:client_key_to_binary(Key1),
    Bin2 = ?RT:client_key_to_binary(Key2),
    ?implies(Key1 =/= Key2, Bin1 =/= Bin2).

tester_client_key_to_binary(_Config) ->
    tester:test(?MODULE, prop_client_key_to_binary, 2, 50000, [{threads, 2}]).

-spec prop_hash_key(Key::client_key()) -> true.
prop_hash_key(Key) ->
    % only verify that no exception is thrown during hashing
    ?RT:hash_key(Key),
    true.

tester_hash_key(_Config) ->
    tester:test(?MODULE, prop_hash_key, 1, 100000, [{threads, 2}]).

-spec number_to_key(N::non_neg_integer()) -> ?RT:key().
number_to_key(N) -> call_helper_fun(number_to_key, [N]).

next_hop(_Config) ->
    MyNode = node:new(comm:make_global(self()), number_to_key(0), 0),
    pid_groups:join_as(?MODULE, dht_node),
    Succ = node:new(fake_dht_node('.succ'), number_to_key(1), 0),
    Pred = node:new(fake_dht_node('.pred'), number_to_key(1000000), 0),
    Neighbors = nodelist:new_neighborhood(Pred, MyNode, Succ),
    DHTNodes = [fake_dht_node(X) || X <- lists:seq(1, 6)],
    RT_Keys = [{1, 1}, {2, 2}, {4, 3}, {8, 4}, {16, 5}, {32, 6}, {64, 7}],
    RT = call_helper_fun(create_rt, [RT_Keys, [node:pidX(Succ) | DHTNodes], Neighbors]),
    RMState = rm_loop:unittest_create_state(Neighbors, false),
    % note: dht_node_state:new/3 will call pid_groups:get_my(paxos_proposer)
    % which will fail here -> however, we don't need this process
    DB = db_dht:new(db_dht),
    State = dht_node_state:new(RT, RMState, DB),

    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 0, node:pidX(Pred)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 1, succ]), % succ is responsible
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 2, node:pidX(Succ)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 3, lists:nth(1, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 7, lists:nth(2, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 9, lists:nth(3, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 31, lists:nth(4, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 64, lists:nth(5, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 65, lists:nth(6, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 1000, lists:nth(6, DHTNodes)]),

    [exit(comm:make_local(Node), kill) || Node <- DHTNodes],
%%     exit(node:pidX(MyNode), kill),
    exit(comm:make_local(node:pidX(Succ)), kill),
    exit(comm:make_local(node:pidX(Pred)), kill),
    db_dht:close(DB),
    ok.

next_hop2(_Config) ->
    MyNode = node:new(comm:make_global(self()), number_to_key(0), 0),
    pid_groups:join_as(?MODULE, dht_node),
    Succ = node:new(fake_dht_node('.succ'), number_to_key(1), 0),
    SuccSucc = node:new(fake_dht_node('.succ.succ'), number_to_key(2), 0),
    Pred = node:new(fake_dht_node('.pred'), number_to_key(1000000), 0),
    DHTNodes = [fake_dht_node(X) || X <- lists:seq(1, 6)],
    RT_Keys = [{1, 1}, {4, 3}, {8, 4}, {16, 5}, {32, 6}, {64, 7}],
    Neighbors = nodelist:add_node(nodelist:new_neighborhood(Pred, MyNode, Succ),
                                  SuccSucc, 2, 2),
    RT = call_helper_fun(create_rt, [RT_Keys, [node:pidX(Succ) | DHTNodes], Neighbors]),
    %% log:pal("RT:~n~190.2p", [gb_trees:to_list(RT)]),
    RMState = rm_loop:unittest_create_state(Neighbors, false),
    % note: dht_node_state:new/3 will call pid_groups:get_my(paxos_proposer)
    % which will fail here -> however, we don't need this process
    DB = db_dht:new(db_dht),
    State = dht_node_state:new(RT, RMState, DB),

    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 0, node:pidX(Pred)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 1, succ]), % succ is responsible
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 2, node:pidX(Succ)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 3, node:pidX(SuccSucc)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 7, lists:nth(2, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 9, lists:nth(3, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 31, lists:nth(4, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 64, lists:nth(5, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 65, lists:nth(6, DHTNodes)]),
    call_helper_fun(check_next_hop, [State, node:pidX(Succ), 1000, lists:nth(6, DHTNodes)]),

    [exit(comm:make_local(GPid), kill) || GPid <- DHTNodes],
%%     exit(node:pidX(MyNode), kill),
    exit(comm:make_local(node:pidX(Succ)), kill),
    exit(comm:make_local(node:pidX(SuccSucc)), kill),
    exit(comm:make_local(node:pidX(Pred)), kill),
    db_dht:close(DB),
    ok.

-spec prop_get_split_key_half(Begin::?RT:key(), End::?RT:key() | plus_infinity) -> true.
prop_get_split_key_half(Begin, End_) ->
    End = case End_ of
              plus_infinity -> ?PLUS_INFINITY;
              _             -> End_
          end,
    SplitKey = ?RT:get_split_key(Begin, End, {1, 2}),

    I = case Begin =:= End of
            true -> intervals:all(); % full range
            _    -> intervals:new('[', Begin, End, ')')

        end,
    ?equals_w_note(intervals:in(SplitKey, I), true,
                   {"SplitKey", SplitKey}),

    call_helper_fun(check_split_key_half, [Begin, End, SplitKey]).

tester_get_split_key_half(_Config) ->
    prop_get_split_key_half(?MINUS_INFINITY, plus_infinity),
    tester:test(?MODULE, prop_get_split_key_half, 2, 10000, [{threads, 2}]).

-spec prop_get_split_key(Begin::?RT:key(), End::?RT:key() | plus_infinity, SplitFracA::1..100, SplitFracB::0..100) -> true.
prop_get_split_key(Begin, End_, SplitFracA, SplitFracB) ->
    End = case End_ of
              plus_infinity -> ?PLUS_INFINITY;
              _             -> End_
          end,
    SplitFraction = case SplitFracA =< SplitFracB of
                        true -> {SplitFracA, SplitFracB};
                        _    -> {SplitFracB, SplitFracA}
                    end,

%%     ct:pal("Begin: ~.0p, End: ~.0p, SplitFactor: ~.0p", [Begin, End, SplitFraction]),
    SplitKey = ?RT:get_split_key(Begin, End, SplitFraction),

    case erlang:element(1, SplitFraction) =:= 0 of
        true -> ?equals(SplitKey, Begin);
        _ ->
            I = case Begin =:= End of
                    true -> intervals:all(); % full range
                    _ when SplitFracA =:= SplitFracB andalso End =:= ?PLUS_INFINITY ->
                        intervals:all(); % hack - there is no interval including ?PLUS_INFINITY but intervals says 'yes' checking for element if the intervall is 'all'
                    _ when SplitFracA =:= SplitFracB ->
                        intervals:new('[', Begin, End, ']');
                    _    -> intervals:new('[', Begin, End, ')')

                end,
            ?equals_w_note(intervals:in(SplitKey, I), true,
                           {"SplitKey", SplitKey}),
            call_helper_fun(check_split_key, [Begin, End, SplitKey, SplitFraction])
    end,
    true.

tester_get_split_key(_Config) ->
    tester:test(?MODULE, prop_get_split_key, 4, 10000, [{threads, 2}]).

additional_tests(Config) ->
    call_helper_fun(additional_tests, [Config]).

%% helpers

-spec fake_dht_node(Suffix::atom()|integer()) -> comm:mypid().
fake_dht_node(Suffix) ->
    comm:make_global(
      element(1, unittest_helper:start_subprocess(
                fun() -> pid_groups:join_as({?MODULE, Suffix}, dht_node) end))).

-spec call_helper_fun(Fun::atom(), Args::list()) -> term().
call_helper_fun(Fun, Args) ->
    erlang:apply(erlang:list_to_atom("rt_SUITE_" ++ erlang:atom_to_list(?RT)), Fun, Args).
