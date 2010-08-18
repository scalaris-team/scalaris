%  Copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : paxos_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/paxos/*.erl
%%%
%%% Created :  20 Nov 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(paxos_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).
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
    file:set_cwd("../bin"),
    Pid = unittest_helper:make_ring(2),
    comm_server:set_local_address({127,0,0,1},14195),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    unittest_helper:stop_ring(Pid),
    ok.

%% make proposers, acceptors, and learners
make(P, A, L, Prefix) ->
    NumMDs = lists:max([P,A,L]),
    [ msg_delay:start_link({Prefix, X}) || X <- lists:seq(1,NumMDs)],
    Ps = [ comm:make_global(element(2, proposer:start_link({Prefix, X})))
           || X <- lists:seq(1,P)],
    As = [ comm:make_global(element(2, acceptor:start_link({Prefix, X})))
           || X <- lists:seq(1,A)],
    Ls = [ comm:make_global(element(2, learner:start_link({Prefix, X})))
           || X <- lists:seq(1,L)],
    {Ps, As, Ls}.

collector(Count, Owner) ->
    spawn(fun() ->
                  collect(Count),
                  Owner ! done
          end).

collect(0) ->
    ok;
collect(Count) ->
    receive
        _ -> collect(Count - 1)
%%     after 2000 ->
%%             ct:pal("No further receives at count ~p", [Count]),
%%             collect(Count - 1)
    end.

tester_fast_paxos(CountAcceptors, Count, Prefix) ->
    %% Count = 10,
    CountProposers = 1,
    %% CountAcceptors = 4,
    Majority = CountAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(CountProposers, CountAcceptors, 1, Prefix),

    Collector = comm:make_global(collector(Count, self())),

    [learner:start_paxosid(hd(Learners), Id, Majority, Collector, chocolate_chip_cookie)
     || Id <- lists:seq(1, Count)],
    [acceptor:start_paxosid(X, Id, Learners)
     || X <- Acceptors,  Id <- lists:seq(1, Count)],
    [proposer:start_paxosid(hd(Proposers), Id, Acceptors, prepared,
                            Majority, CountProposers, 0)
     || Id <- lists:seq(1, Count)],
    receive
        done ->
            ok
    end,
    ok.

tester_paxos(CountAcceptors, Count, Prefix) ->
    CountProposers = 1,
    Majority = CountAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(CountProposers, CountAcceptors, 1, Prefix),

    Collector = comm:make_global(collector(Count, self())),

    spawn(fun() ->
                  [learner:start_paxosid(hd(Learners), Id, Majority, Collector, chocolate_chip_cookie)
                   || Id <- lists:seq(1, Count)]
          end),
    spawn(fun() ->
                  [acceptor:start_paxosid(X, Id, Learners)
                   || X <- Acceptors,  Id <- lists:seq(1, Count)]
           end),
    spawn( fun() ->
                   [proposer:start_paxosid(hd(Proposers), Id, Acceptors,
                                           prepared, Majority, CountProposers)
                    || Id <- lists:seq(1, Count)]
           end),
    receive
        done ->
            ok
    end,
    ok.

test_fast_acceptors_4(_Config) ->
    Count = 10000,
    Before = erlang:now(),
    tester_fast_paxos(4, Count, "test_acceptors_4"),
    After = erlang:now(),
    ct:pal("fast: acceptors: 4, throughput: ~p", [Count / (timer:now_diff(After, Before) / 1000000.0)]),
    ok.

test_fast_acceptors_16(_Config) ->
    Count = 10000,
    Before = erlang:now(),
    tester_fast_paxos(16, Count, "test_acceptors_16"),
    After = erlang:now(),
    ct:pal("fast: acceptors: 16, throughput: ~p", [Count / (timer:now_diff(After, Before) / 1000000.0)]),
    ok.

test_acceptors_4(_Config) ->
    Count = 10000,
    Before = erlang:now(),
    tester_paxos(4, Count, "test_acceptors_4"),
    After = erlang:now(),
    ct:pal("slow: acceptors: 4, throughput: ~p", [Count / (timer:now_diff(After, Before) / 1000000.0)]),
    ok.

test_acceptors_16(_Config) ->
    Count = 10000,
    Before = erlang:now(),
    tester_paxos(16, Count, "test_acceptors_16"),
    After = erlang:now(),
    ct:pal("slow: acceptors: 16, throughput: ~p", [Count / (timer:now_diff(After, Before) / 1000000.0)]),
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
    [ acceptor:start_paxosid(X, paxid123, Learners) || X <- Acceptors ],
    [ Proposer1, Proposer2 ] = Proposers,

    %% set some breakpoints
    gen_component:bp_set(element(3, Proposer1), acceptor_ack, bp1),
    %% initiate full paxos
    proposer:start_paxosid(Proposer1, paxid123, Acceptors,
                           prepared, Majority, length(Proposers), 3),
    proposer:start_paxosid(Proposer2, paxid123, Acceptors,
                           abort, Majority, length(Proposers), 2),

    %% should receive an abort
    receive {learner_decide, cpaxid123, _, Res1} = Any -> io:format("Expected abort Received ~p~n", [Any]) end,

    gen_component:bp_barrier(element(3, Proposer1)),
    gen_component:bp_del(element(3, Proposer1), bp1),
    gen_component:bp_cont(element(3, Proposer1)),

    %% should receive also an abort as proposer1 was hold
    receive {learner_decide, cpaxid123, _, Res2} = Any2 ->
            io:format("Expected abort Received ~p~n", [Any2]) end,

    ?assert(Res1 =:= Res2),
    %%%%% now vice versa:
    io:format("Now vice versa~n"),

    %% start paxosids in the components
    learner:start_paxosid(hd(Learners), paxid124, Majority, comm:this(), cpaxid124),
    [ acceptor:start_paxosid(X, paxid124, Learners) || X <- Acceptors ],
    [ Proposer1, Proposer2 ] = Proposers,
    %% set some breakpoints
    gen_component:bp_set(element(3, Proposer2), acceptor_ack, bp2),
    %% initiate full paxos
    proposer:start_paxosid(Proposer1, paxid124, Acceptors,
                           prepared, Majority, length(Proposers), 1),
    proposer:start_paxosid(Proposer2, paxid124, Acceptors,
                           abort, Majority, length(Proposers), 2),

    %% should receive a prepared as proposer2 was hold
    receive {learner_decide, cpaxid124, _, Res3} = Any3 -> io:format("Expected prepared Received ~p~n", [Any3]) end,

    gen_component:bp_barrier(element(3, Proposer2)),
    gen_component:bp_del(element(3, Proposer2), bp2),
    gen_component:bp_cont(element(3, Proposer2)),

    %% should receive also an abort
    receive
        {learner_decide, cpaxid124, _, Res4} = Any4 -> io:format("Expected prepared Received ~p~n", [Any4])
    end,

    ?assert(Res3 =:= Res4),
    ct:pal("done.~n"),
    ok.

%% userdevguide-begin paxos_SUITE:random_interleaving_test
-spec(prop_rnd_interleave/3 :: (1..4, 4..16, {pos_integer(), pos_integer(), pos_integer()})
 -> boolean()).
prop_rnd_interleave(NumProposers, NumAcceptors, Seed) ->
    ct:pal("Called with: paxos_SUITE:prop_rnd_interleave(~p, ~p, ~p).~n",
           [NumProposers, NumAcceptors, Seed]),
    Majority = NumAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(NumProposers, NumAcceptors, 1, rnd_interleave),
    %% set bp on all processes
    [ gen_component:bp_set(element(3, X), proposer_initialize, bp)
      ||  X <- Proposers],
    [ gen_component:bp_set(element(3, X), acceptor_initialize, bp)
      ||  X <- Acceptors ],
    [ gen_component:bp_set(element(3, X), learner_initialize, bp)
      || X <- Learners],
    %% start paxos instances
    [ proposer:start_paxosid(X, paxidrndinterl, Acceptors,
                             proposal, Majority, NumProposers, Y)
      || {X,Y} <- lists:zip(Proposers, lists:seq(1, NumProposers)) ],
    [ acceptor:start_paxosid(X, paxidrndinterl, Learners)
      || X <- Acceptors ],
    [ learner:start_paxosid(X, paxidrndinterl, Majority,
                            comm:this(), cpaxidrndinterl)
      || X <- Learners],
    %% randomly step through protocol
    OldSeed = random:seed(Seed),
    Steps = step_until_decide(Proposers ++ Acceptors ++ Learners, cpaxidrndinterl, 0),
    ct:pal("Needed ~p steps~n", [Steps]),
    case OldSeed of
        undefined -> ok;
        _ -> random:seed(OldSeed)
    end,
    true.

step_until_decide(Processes, PaxId, SumSteps) ->
    %% io:format("Step ~p~n", [SumSteps]),
    Runnable = [ X || X <- Processes, gen_component:runnable(element(3,X)) ],
    case Runnable of
        [] ->
            ct:pal("No runnable processes of ~p~n", [length(Processes)]),
            timer:sleep(5), step_until_decide(Processes, PaxId, SumSteps);
        _ -> ok
    end,
    Num = random:uniform(length(Runnable)),
    gen_component:bp_step(element(3,lists:nth(Num, Runnable))),
    receive
        {learner_decide, cpaxidrndinterl, _, _Res} = _Any ->
            %% io:format("Received ~p~n", [_Any]),
            SumSteps
    after 0 -> step_until_decide(Processes, PaxId, SumSteps + 1)
    end.
%% userdevguide-end paxos_SUITE:random_interleaving_test

test_rnd_interleave(_Config) ->
    tester:test(paxos_SUITE, prop_rnd_interleave, _Params = 3, _Iter = 100).

wait_for(Name) ->
    case whereis(Name) of
        undefined ->
            wait_for(Name);
        _ ->
            ok
    end.
