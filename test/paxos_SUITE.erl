%  @copyright 2008-2016 Zuse Institute Berlin

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
%% @doc    Unit tests for src/paxos/*.erl
%% @end
%% @version $Id$
-module(paxos_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).
-include("scalaris.hrl").
-include("unittest.hrl").

all() -> [
          test_fast_acceptors_4, test_fast_acceptors_16,
          test_acceptors_4, test_acceptors_16,
          test_two_proposers,
          test_rnd_interleave
         ].

suite() ->
    [{timetrap, {seconds, 40}}].

init_per_suite(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(2, [{config, [{log_path, PrivDir}]}]),
    comm_server:set_local_address({127,0,0,1}, unittest_helper:get_scalaris_port()),
    Config.

end_per_suite(_Config) ->
    ok.

-spec make_groupname(Prefix::string(), Number::integer()) -> pid_groups:groupname().
make_groupname(Prefix, Number) ->
    {Prefix, Number}.

%% make proposers, acceptors, and learners
-spec make(P::pos_integer(), A::pos_integer(), L::pos_integer(), Prefix::atom())
        -> {Ps::[comm:mypid(),...], As::[comm:mypid(),...], Ls::[comm:mypid(),...]}.
make(P, A, L, Prefix) ->
    NumMDs = lists:max([P,A,L]),
    _  = [ msg_delay:start_link(make_groupname(Prefix, X))
             || X <- lists:seq(1, NumMDs)],
    Ps = [ comm:make_global(element(2, proposer:start_link(make_groupname(Prefix, X), paxos_proposer)))
             || X <- lists:seq(1, P)],
    As = [ comm:make_global(element(2, acceptor:start_link(make_groupname(Prefix, X), paxos_acceptor)))
             || X <- lists:seq(1, A)],
    Ls = [ comm:make_global(element(2, learner:start_link(make_groupname(Prefix, X), paxos_learner)))
             || X <- lists:seq(1,L)],
    {Ps, As, Ls}.

-spec collector(Count::pos_integer(), Owner::pid()) -> pid().
collector(Count, Owner) ->
    spawn(fun() ->
                  collect(Count),
                  Owner ! done
          end).

-spec collect(non_neg_integer()) -> ok.
collect(0) ->
    ok;
collect(Count) ->
    receive
        _ -> collect(Count - 1)
%%     after 2000 ->
%%             ct:pal("No further receives at count ~p", [Count]),
%%             collect(Count - 1)
    end.

-spec tester_fast_paxos(CountAcceptors::pos_integer(), Count::pos_integer(), Prefix::atom()) -> ok.
tester_fast_paxos(CountAcceptors, Count, Prefix) ->
    %% Count = 10,
    CountProposers = 1,
    %% CountAcceptors = 4,
    Majority = CountAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(CountProposers, CountAcceptors, 1, Prefix),

    Collector = comm:make_global(collector(Count, self())),

    _ = [ learner:start_paxosid(hd(Learners), Id, Majority, Collector, chocolate_chip_cookie)
            || Id <- lists:seq(1, Count)],
    _ = [ acceptor:start_paxosid(X, Id, Learners)
            || X <- Acceptors,  Id <- lists:seq(1, Count)],
    _ = [ proposer:start_paxosid(hd(Proposers), Id, Acceptors, ?prepared,
                                 Majority, CountProposers, 0)
            || Id <- lists:seq(1, Count)],
    receive done -> ok
    end,
    _ = [ gen_component:kill(comm:make_local(X))
          || X <- lists:flatten([Proposers, Acceptors, Learners])],
    ok.

-spec tester_paxos(CountAcceptors::pos_integer(), Count::pos_integer(), Prefix::atom()) -> ok.
tester_paxos(CountAcceptors, Count, Prefix) ->
    CountProposers = 1,
    Majority = CountAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(CountProposers, CountAcceptors, 1, Prefix),

    Collector = comm:make_global(collector(Count, self())),

    _ = spawn(fun() ->
                      [ learner:start_paxosid(hd(Learners), Id, Majority, Collector, chocolate_chip_cookie)
                          || Id <- lists:seq(1, Count)]
              end),
    _ = spawn(fun() ->
                      [ acceptor:start_paxosid(X, Id, Learners)
                          || X <- Acceptors,  Id <- lists:seq(1, Count)]
           end),
    _ = spawn(fun() ->
                      [ proposer:start_paxosid(hd(Proposers), Id, Acceptors,
                                               ?prepared, Majority, CountProposers)
                          || Id <- lists:seq(1, Count)]
              end),
    receive done -> ok
    end,
    _ = [ gen_component:kill(comm:make_local(X))
          || X <- lists:flatten([Proposers, Acceptors, Learners])],
    ok.

test_fast_acceptors_4(_Config) ->
    Count = 10000,
    Before = os:timestamp(),
    tester_fast_paxos(4, Count, test_acceptors_4),
    After = os:timestamp(),
    ct:pal("fast: acceptors: 4, throughput: ~p~n", [Count / (erlang:max(1, timer:now_diff(After, Before)) / 1000000.0)]),
    ok.

test_fast_acceptors_16(_Config) ->
    Count = 10000,
    Before = os:timestamp(),
    tester_fast_paxos(16, Count, test_acceptors_16),
    After = os:timestamp(),
    ct:pal("fast: acceptors: 16, throughput: ~p~n", [Count / (erlang:max(1, timer:now_diff(After, Before)) / 1000000.0)]),
    ok.

test_acceptors_4(_Config) ->
    Count = 10000,
    Before = os:timestamp(),
    tester_paxos(4, Count, test_acceptors_4),
    After = os:timestamp(),
    ct:pal("slow: acceptors: 4, throughput: ~p~n", [Count / (erlang:max(1, timer:now_diff(After, Before)) / 1000000.0)]),
    ok.

test_acceptors_16(_Config) ->
    Count = 10000,
    Before = os:timestamp(),
    tester_paxos(16, Count, test_acceptors_16),
    After = os:timestamp(),
    ct:pal("slow: acceptors: 16, throughput: ~p~n", [Count / (erlang:max(1, timer:now_diff(After, Before)) / 1000000.0)]),
    ok.

test_two_proposers(_Config) ->
    ct:pal("test_two_proposers ...~n"),
    CountProposers = 2,
    CountAcceptors = 4,
    Majority = CountAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(CountProposers, CountAcceptors, 1, two_proposers),

    %% start paxosids in the components
    learner:start_paxosid(hd(Learners), paxid123, Majority, comm:this(), cpaxid123),
    _ = [ acceptor:start_paxosid(X, paxid123, Learners) || X <- Acceptors ],
    [ Proposer1, Proposer2 ] = Proposers,

    %% set some breakpoints
    gen_component:bp_set(comm:make_local(Proposer1), acceptor_ack, bp1),
    %% initiate full paxos
    proposer:start_paxosid(Proposer1, paxid123, Acceptors,
                           ?prepared, Majority, length(Proposers), 3),
    proposer:start_paxosid(Proposer2, paxid123, Acceptors,
                           ?abort, Majority, length(Proposers), 2),

    %% should receive an abort
    receive {learner_decide, cpaxid123, _, Res1} = Any -> io:format("Expected abort Received ~p~n", [Any]) end,

    gen_component:bp_barrier(comm:make_local(Proposer1)),
    gen_component:bp_del(comm:make_local(Proposer1), bp1),
    gen_component:bp_cont(comm:make_local(Proposer1)),

    %% should receive also an abort as proposer1 was hold
    receive {learner_decide, cpaxid123, _, Res2} = Any2 ->
            io:format("Expected abort Received ~p~n", [Any2]) end,

    ?assert(Res1 =:= Res2),
    %%%%% now vice versa:
    io:format("Now vice versa~n"),

    %% start paxosids in the components
    learner:start_paxosid(hd(Learners), paxid124, Majority, comm:this(), cpaxid124),
    _ = [ acceptor:start_paxosid(X, paxid124, Learners) || X <- Acceptors ],
    [ Proposer1, Proposer2 ] = Proposers,
    %% set some breakpoints
    gen_component:bp_set(comm:make_local(Proposer2), acceptor_ack, bp2),
    %% initiate full paxos
    proposer:start_paxosid(Proposer1, paxid124, Acceptors,
                           ?prepared, Majority, length(Proposers), 1),
    proposer:start_paxosid(Proposer2, paxid124, Acceptors,
                           ?abort, Majority, length(Proposers), 2),

    %% should receive a prepared as proposer2 was hold
    receive {learner_decide, cpaxid124, _, Res3} = Any3 -> io:format("Expected prepared Received ~p~n", [Any3]) end,

    gen_component:bp_barrier(comm:make_local(Proposer2)),
    gen_component:bp_del(comm:make_local(Proposer2), bp2),
    gen_component:bp_cont(comm:make_local(Proposer2)),

    %% should receive also an abort
    receive
        {learner_decide, cpaxid124, _, Res4} = Any4 -> io:format("Expected prepared Received ~p~n", [Any4])
    end,

    ?assert(Res3 =:= Res4),
    ct:pal("done.~n"),
    _ = [ gen_component:kill(comm:make_local(X))
          || X <- lists:flatten([Proposers, Acceptors, Learners])],
    ok.

%% userdevguide-begin paxos_SUITE:random_interleaving_test
-spec prop_rnd_interleave(1..4, 4..16)
        -> true.
prop_rnd_interleave(NumProposers, NumAcceptors) ->
    ct:pal("Called with: paxos_SUITE:prop_rnd_interleave(~p, ~p).~n",
           [NumProposers, NumAcceptors]),
    Majority = NumAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(NumProposers, NumAcceptors, 1, rnd_interleave),
    %% set bp on all processes
    _ = [ gen_component:bp_set(comm:make_local(X), ?proposer_initialize, bp)
            ||  X <- Proposers],
    _ = [ gen_component:bp_set(comm:make_local(X), acceptor_initialize, bp)
            ||  X <- Acceptors ],
    _ = [ gen_component:bp_set(comm:make_local(X), learner_initialize, bp)
            || X <- Learners],
    %% start paxos instances
    _ = [ proposer:start_paxosid(X, paxidrndinterl, Acceptors,
                                 proposal, Majority, NumProposers, Y)
            || {X,Y} <- lists:zip(Proposers, lists:seq(1, NumProposers)) ],
    _ = [ acceptor:start_paxosid(X, paxidrndinterl, Learners)
            || X <- Acceptors ],
    _ = [ learner:start_paxosid(X, paxidrndinterl, Majority,
                                comm:this(), cpaxidrndinterl)
            || X <- Learners],
    %% randomly step through protocol
    Steps = step_until_decide(Proposers ++ Acceptors ++ Learners, cpaxidrndinterl, 0),
    ct:pal("Needed ~p steps~n", [Steps]),
    _ = [ gen_component:kill(comm:make_local(X))
          || X <- lists:flatten([Proposers, Acceptors, Learners])],
    true.

step_until_decide(Processes, PaxId, SumSteps) ->
    %% io:format("Step ~p~n", [SumSteps]),
    Runnable = [ X || X <- Processes, gen_component:runnable(comm:make_local(X)) ],
    case Runnable of
        [] ->
            ct:pal("No runnable processes of ~p~n", [length(Processes)]),
            timer:sleep(5), step_until_decide(Processes, PaxId, SumSteps);
        _ ->
            Num = randoms:uniform(length(Runnable)),
            _ = gen_component:bp_step(comm:make_local(lists:nth(Num, Runnable))),
            receive
                {learner_decide, cpaxidrndinterl, _, _Res} = _Any ->
                    %% io:format("Received ~p~n", [_Any]),
                    SumSteps
            after 0 -> step_until_decide(Processes, PaxId, SumSteps + 1)
            end
    end.
%% userdevguide-end paxos_SUITE:random_interleaving_test

test_rnd_interleave(_Config) ->
    tester:test(paxos_SUITE, prop_rnd_interleave, _Params = 2, _Iter = 100).
