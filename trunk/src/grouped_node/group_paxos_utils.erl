%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @version $Id$
-module(group_paxos_utils).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("group.hrl").

-export([propose/2, init_paxos/1, cleanup_paxos_states/1]).

-spec propose(proposal(), group_state:group_state()) ->
    {success, group_state:group_state()} | failed.
propose(Proposal, GroupState) ->
    case is_first_proposal_for_this_instance(GroupState) of
        true ->
            PaxosId = group_state:get_next_proposal_id(GroupState),
            Proposer = comm:make_global(pid_groups:get_my(paxos_proposer)),
            Acceptors = group_state:get_acceptors(GroupState),
            Majority = length(Acceptors) div 2 + 1,
            MaxProposers = length(Acceptors),
            proposer:start_paxosid(Proposer, PaxosId, Acceptors, Proposal, Majority,
                                   MaxProposers),
            {success, group_state:made_proposal(GroupState, PaxosId, Proposal)};
        _ ->
            failed
    end.

is_first_proposal_for_this_instance(GroupState) ->
    NextProposalVersion = group_state:get_next_proposal_id(GroupState),
    CurrentPaxosVersion = group_state:get_current_paxos_id(GroupState),
    NextProposalVersion == CurrentPaxosVersion.

-spec init_paxos(group_state:group_state()) ->
    group_state:group_state().
init_paxos(GroupState) ->
    PaxosId = group_state:get_next_paxos_id(GroupState),
    Learners = group_state:get_learners(GroupState),
    Proposer = pid_groups:get_my(paxos_proposer),
    Acceptor = pid_groups:get_my(paxos_acceptor),
    io:format("init_paxos ~p ~p~n", [PaxosId, Learners]),
    acceptor:start_paxosid_local(Acceptor, PaxosId, Learners),
    Learner = pid_groups:get_my(paxos_learner),
    Majority = group_state:get_size(GroupState) div 2 + 1,
    learner:start_paxosid_local(Learner, PaxosId, Majority, comm:this(), client_cookie),
    proposer:trigger(comm:make_global(Proposer), PaxosId),
    group_state:init_paxos(GroupState, PaxosId).


-spec cleanup_paxos_states(paxos_id()) -> ok.
cleanup_paxos_states({GroupId, PaxosId}) ->
    ok.

foo({GroupId, PaxosId}) ->
    case PaxosId > 2 of
        true ->
            Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
            learner:stop_paxosids(Learner, [{GroupId, PaxosId - 2}]),
            Proposer = comm:make_global(pid_groups:get_my(paxos_proposer)),
            proposer:stop_paxosids(Proposer, [{GroupId, PaxosId - 2}]),
            Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
            acceptor:stop_paxosids(Acceptor, [{GroupId, PaxosId - 2}]),
            ok;
        false ->
            ok
    end.

% @todo re-add cleanup-code
