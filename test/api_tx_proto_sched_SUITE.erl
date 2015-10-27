%% @copyright 2014-2015 Zuse Institute Berlin

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
%% @version $Id: api_tx_SUITE.erl 5051 2013-07-26 12:21:14Z kruber@zib.de $
-module(api_tx_proto_sched_SUITE).
-author('schintke@zib.de').
-vsn('$Id: api_tx_SUITE.erl 5051 2013-07-26 12:21:14Z kruber@zib.de $').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

-dialyzer({no_match, concurrent_2/1}). % tx_tm:is_txnew_enabled() looks constant

%% start proto scheduler for this suite
-define(proto_sched(Action), proto_sched_fun(Action)).
-include("api_tx_SUITE.hrl").


groups() ->
    [%% implementation in api_tx_SUITE.hrl
     %% (shared with api_tx_proto_sched_SUITE.erl)
     {proto_sched_ready, [sequence],
      proto_sched_ready_tests()},
     {only_in_proto_sched,
      %% [sequence, {repeat_until_any_fail, forever}],
      %% [sequence, {repeat_until_any_fail, 500}],
      [sequence],
      [concurrent_2]}
    ].


all()   -> [ {group, proto_sched_ready},
             {group, only_in_proto_sched}
           ].

suite() -> [ {timetrap, {seconds, 200}} ].

init_per_testcase(TestCase, Config) ->
    case TestCase of
        write_test_race_mult_rings -> %% this case creates its own ring
            ok;
        tester_encode_decode -> %% this case does not need a ring
            ok;
        _ ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
            timer:sleep(1000),
            ?ASSERT(ok =:= unittest_helper:check_ring_size_fully_joined(4)),
            unittest_helper:wait_for_stable_ring_deep(),
            ok
    end,
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

-spec proto_sched_fun(start | stop) -> ok.
proto_sched_fun(start) ->
    %% ct:pal("Starting proto scheduler"),
    proto_sched:thread_num(1),
    proto_sched:thread_begin();
proto_sched_fun(stop) ->
    %% is a ring running?
    case erlang:whereis(pid_groups) =:= undefined
             orelse pid_groups:find_a(proto_sched) =:= failed of
        true -> ok;
        false ->
            %% then finalize proto_sched run:
            %% try to call thread_end(): if this
            %% process was running the proto_sched
            %% thats fine, otherwise thread_end()
            %% will raise an exception
            proto_sched:thread_end(),
            proto_sched:wait_for_end(),
            ct:pal("Proto scheduler stats: ~.2p",
                   %%[proto_sched:info_shorten_messages(
                   %%   proto_sched:get_infos(), 200)]),
                   [lists:keydelete(nums_chosen_from, 1,
                                    lists:keydelete(delivered_msgs, 1,
                                                    proto_sched:get_infos()))]),
            proto_sched:cleanup()
    end.

%% @doc Reduces the number of tx calls so that the time taken by test methods
%%      with proto_sched does not increase too much.
-spec adapt_tx_runs(N::pos_integer()) -> pos_integer().
adapt_tx_runs(N) ->
    erlang:max(N div (10 * config:read(replication_factor)), 1).

concurrent_2(_Config) ->
    %% let two increments run concurrently

    %% initialize a key
    {ok} = api_tx:write("a", 0),

    Iterations = 6,
    Threads = 2,
    log:log("Concurrency test begins"),
    proto_sched:thread_num(Threads),
    case tx_tm:is_txnew_enabled() of
        true ->
            proto_sched:register_callback(
              fun api_tx_proto_sched_SUITE:rbr_invariant/3, on_deliver);
        _ -> ok
    end,
    F = fun(_X, 0) -> {ok};
           (X, Count) ->
                {TLog, {ok, Val}} = api_tx:read(api_tx:new_tlog(), "a"),
                case Val >= (Iterations - Count) of
                    false ->
                        %% I performed (Iterations - Count)
                        %% increments, so the counter should be at
                        %% least that high
                        log:log("Oops? Missing value"),
                        %% log:log("~p", [proto_sched:get_infos()])
                        ?DBG_ASSERT2(Val >= (Iterations - Count),
                                     {'missing increment'});
                    true -> ok
                end,
                {[], [{ok}, CommitRes]} =
                    api_tx:req_list(TLog, [{write, "a", Val+1}, {commit}]),
                case CommitRes of
                    {ok} ->
                        X(X, Count-1);
                    _ ->
                        log:log("Retry ~p, Val ~p, StillTodo ~p, CommitRes ~p",
                                [self(), Val, Count, CommitRes]),
                        X(X, Count)
                end
        end,
    [ spawn(fun() ->
                    proto_sched:thread_begin(),
                    Res = F(F, Iterations),
                    log:log("Res~p ~p", [X, Res]),
                    proto_sched:thread_end()
            end) || X <- lists:seq(1,Threads)],
    proto_sched:wait_for_end(),
    proto_sched:cleanup(),
    ?equals_w_note(api_tx:read("a"), {ok, Threads*Iterations},
                   wrong_result),
    ok.

%% callback function for proto_sched
-spec rbr_invariant(Src::comm:mypid(), Dest::comm:mypid(), Msg::comm:message()) -> ok.
rbr_invariant(_From, _To, _Msg) ->
    %% we are a callback function, which is not infected!
    %% we are executed in the context of proto_sched and are allowed
    %% to use receive, etc, as where the callback is called, no
    %% other important things happen...
    %% as the proto_sched mainly receives {log_send,...} events, we do
    %% not pick messages from its inbox, that we do not want to pick
    %% when using receive.

    %% get the key
    HashedKey = api_dht:hash_key("a"),
    Replicas = api_dht_raw:get_replica_keys(HashedKey),

    %% retrieve all replicas: we simply retrieve the whole ring and
    %% filter for the interesting keys
    DHTNodes = pid_groups:find_all(dht_node),

    RingData = [ begin
                     N ! {prbr, tab2list_raw, kv, self()},
                     receive
                         {kv, List} -> List
                     end
                 end
                 || N <- DHTNodes ],
    FlatRingData = lists:flatten(RingData),

    MyEntries = [ begin
                      Entry = lists:keyfind(X, 1, FlatRingData),
                      setelement(1, Entry, ShortName)
                  end || {X, ShortName} <- lists:zip(Replicas, [r0, r1, r2, r3]) ],

    %% print all replicas in a structured way
    case erlang:get(entries) of
        MyEntries -> ok;
        [A,B,C,D] ->
            %% print all changed replicas in a structured way
            [NA, NB, NC, ND] = MyEntries,
            X = [NA =:= A, NB =:= B, NC =:= C, ND =:= D],
            Format = lists:flatten([ "~n~10000.0p" || Y <- X, Y =:= false]),
            log:log(Format,
                    [ element(2, Y) || Y <- lists:zip(X, MyEntries),
                                       element(1, Y) =:= false]);
        _ ->
            %% print all replicas (more or less than 4?)
            log:log("~n~10000.0p", [MyEntries])
    end,

    erlang:put(entries, MyEntries),

    %% anaylse the replicas
    ok.


