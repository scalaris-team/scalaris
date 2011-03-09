%  @copyright 2007-2011 Zuse Institute Berlin

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

-spec propose(group_types:proposal(), group_view:view()) ->
    {success, group_view:view()} | failed.
propose(Proposal, View) ->
    InitialRound = group_view:get_index_in_group(View),
    case is_first_proposal_for_this_instance(View) of
        true ->
            PaxosId = group_view:get_next_proposal_id(View),
            Proposer = comm:make_global(pid_groups:get_my(paxos_proposer)),
            Acceptors = group_view:get_acceptors(View),
            Majority = quorum:majority_for_accept(length(Acceptors)),
            MaxProposers = length(Acceptors),
            proposer:start_paxosid(Proposer, PaxosId, Acceptors, Proposal, Majority,
                                   MaxProposers, InitialRound),
            {success, group_view:made_proposal(View, PaxosId, Proposal)};
        _ ->
            failed
    end.

is_first_proposal_for_this_instance(View) ->
    NextProposalVersion = group_view:get_next_proposal_id(View),
    CurrentPaxosVersion = group_view:get_current_paxos_id(View),
    NextProposalVersion == CurrentPaxosVersion.

-spec init_paxos(group_view:view()) ->
    group_view:view().
init_paxos(View) ->
    PaxosId = group_view:get_next_paxos_id(View),
    Learners = group_view:get_learners(View),
    _Proposer = pid_groups:get_my(paxos_proposer),
    Acceptor = pid_groups:get_my(paxos_acceptor),
    ?LOG("init_paxos ~p ~p~n", [PaxosId, Learners]),
    acceptor:start_paxosid_local(Acceptor, PaxosId, Learners),
    Learner = pid_groups:get_my(paxos_learner),
    Majority = quorum:majority_for_accept(group_view:get_size(View)),
    learner:start_paxosid_local(Learner, PaxosId, Majority, comm:this(), client_cookie),
    %proposer:trigger(comm:make_global(Proposer), PaxosId),
    group_view:init_paxos(View, PaxosId).


-spec cleanup_paxos_states(group_types:paxos_id()) -> ok.
cleanup_paxos_states({GroupId, PaxosId}) ->
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
