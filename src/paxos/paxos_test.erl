% @copyright 2009, 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
%                 onScale solutions GmbH

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
%% @doc Part of generic Paxos-Consensus implementation
%%      Tests.
%% @end
-module(paxos_test).
-author('schintke@onscale.de').
-vsn('$Id$').

-export([run/0]).
-export([make/3]).

make(P, A, L) ->
    NumMDs = lists:max([P,A,L]),
    [ msg_delay:start_link(X) || X <- lists:seq(1,NumMDs)],
    Ps = [ comm:make_global(element(2, proposer:start_link(X)))
           || X <- lists:seq(1,P)],
    As = [ comm:make_global(element(2, acceptor:start_link(X)))
           || X <- lists:seq(1,A)],
    Ls = [ comm:make_global(element(2, learner:start_link(X)))
           || X <- lists:seq(1,L)],
    {Ps, As, Ls}.

run() ->
    % initiate a paxos
    % get a list of acceptors (1 here)

    {Proposers, Acceptors, Learners} = make(2,4,1),

    [ learner:start_paxosid(X, pid123, 3, comm:make_global(self()), mycookie)
      || X <- Learners ],
    [Proposer,Proposer2] = Proposers,

    MaxProposers = 2,

    io:format("=================~n"),
    io:format("make a fast paxos~n"),
    io:format("=================~n"),
    [ learner:start_paxosid(X, pid124, 3, comm:make_global(self()), mycookie)
      || X <- Learners ],
    [ acceptor:start_paxosid(X, pid124, Learners) || X <- Acceptors ],
    proposer:start_paxosid(Proposer, pid124, Acceptors, prepared, 3, MaxProposers, 0),
    receive
        {_,_,pid124,_} = Any4 -> io:format("Received: ~p~n", [Any4])
    end,

    timer:sleep(100),

    io:format("======================~n"),
    io:format("paxos with 2 proposers~n"),
    io:format("======================~n"),

    proposer:start_paxosid(Proposer, pid123, Acceptors, prepared,
                           3, MaxProposers),
%    timer:sleep(10),
    proposer:start_paxosid(Proposer2, pid123, Acceptors, abort, 3,
                           MaxProposers, 2),
%    timer:sleep(1000),
    proposer:trigger(Proposer2, pid123),
%    Learners = [comm:make_global(paxos:get_local_learner())],
    [ acceptor:start_paxosid(X, pid123, Learners) || X <- Acceptors ],
    timer:sleep(1000),
    proposer:trigger(Proposer2, pid123),
    proposer:trigger(Proposer, pid123),
    receive
        {_,_,pid123,_} = Any -> io:format("Received: ~p~n", [Any])
    end,
    receive
        {_,_,pid123,_} = Any2 -> io:format("Received: ~p~n", [Any2])
    end,
    receive
        {_,_,pid123,_} = Any3 -> io:format("Received: ~p~n", [Any3])
    end,

    ok.


