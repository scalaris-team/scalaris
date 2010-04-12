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
-vsn('$Id$ ').

-compile(export_all).
-include("unittest.hrl").

all() -> [
          test_fast_acceptors_4, test_fast_acceptors_16,
          test_acceptors_4, test_acceptors_16
         ].

suite() ->
    [{timetrap, {seconds, 40}}].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    Pid = unittest_helper:make_ring(2),
    comm_port:set_local_address({127,0,0,1},14195),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    unittest_helper:stop_ring(Pid),
    ok.

%% make proposers, acceptors, and learners
make(P, A, L, Prefix) ->
    NumMDs = lists:max([P,A,L]),
    [ msg_delay:start_link({Prefix, X}) || X <- lists:seq(1,NumMDs)],
    Ps = [ cs_send:make_global(element(2, proposer:start_link({Prefix, X})))
           || X <- lists:seq(1,P)],
    As = [ cs_send:make_global(element(2, acceptor:start_link({Prefix, X})))
           || X <- lists:seq(1,A)],
    Ls = [ cs_send:make_global(element(2, learner:start_link({Prefix, X})))
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

    Collector = cs_send:make_global(collector(Count, self())),

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
    %Count = 10,
    CountProposers = 1,
    %CountAcceptors = 4,
    Majority = CountAcceptors div 2 + 1,
    {Proposers, Acceptors, Learners} =
        make(CountProposers, CountAcceptors, 1, Prefix),

    Collector = cs_send:make_global(collector(Count, self())),

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

wait_for(Name) ->
    case whereis(Name) of
        undefined ->
            wait_for(Name);
        _ ->
            ok
    end.
