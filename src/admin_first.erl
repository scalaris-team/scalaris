%  @copyright 2007-2015 Zuse Institute Berlin

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
%% @author Florian Schintke <schintke@zib.de>
%% @doc    determine if first vm in this deployment
%% @end
%% @version $Id$
-module(admin_first).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([is_first_vm/0]).

-spec is_first_vm() -> boolean().
is_first_vm() ->
    config:read(start_type) =:= first
        orelse
    config:read(start_type) =:= first_nostart
        orelse
    (config:read(start_type) =:= quorum andalso has_first_quorum()).

has_first_quorum() ->
    KnownHosts = config:read(known_hosts),
    MyServicePerVM = comm:get(service_per_vm, comm:this()),
    case lists:member(MyServicePerVM, KnownHosts) of
        false ->
            false;
        true ->
            PaxosId = leader_election_for_first_quorum,
            MyProposer = comm:get('basic_services-paxos_proposer', comm:this()),
            MyAcceptor = comm:get('basic_services-paxos_acceptor', comm:this()),
            MyLearner = comm:get('basic_services-paxos_learner', comm:this()),
            Acceptors = [comm:get('basic_services-paxos_acceptor', Host) || Host <- KnownHosts],
            Learners = [comm:get('basic_services-paxos_learner', Host) || Host <- KnownHosts],
            Proposal = {leader, MyServicePerVM},
            MaxProposers = length(KnownHosts),
            Majority = quorum:majority_for_accept(MaxProposers),
            Rank = util:lists_index_of(MyServicePerVM, KnownHosts),
            ?ASSERT(Rank =/= not_found),

            acceptor:start_paxosid(MyAcceptor, PaxosId, Learners),
            learner:start_paxosid(MyLearner, PaxosId, Majority, comm:this(), client_cookie),
            proposer:start_paxosid(MyProposer, PaxosId,
                                   Acceptors, Proposal, Majority, MaxProposers, Rank),
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV(
                    {learner_decide, client_cookie, PaxosId, {leader, TheLeader}}, %%->
                    TheLeader =:= MyServicePerVM
                  )
            end
    end.
