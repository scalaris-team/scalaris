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
%% @doc    group_state
%% @end
%% @version $Id$
-module(group_state).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new_group_state/1,
         get_group_id/1,
         get_version/1,
         get_size/1,
         get_learners/1,
         get_acceptors/1,
         get_members/1,
         get_interval/1,
         is_member/2,
         add_node/4,
         remove_node/2,
         made_proposal/3,
         remove_proposal/2,
         postpone_decision/3,
         get_postponed_decisions/1,
         remove_postponed_decision/3,
         get_proposal/2,
         init_paxos/2,
         get_next_paxos_id/1,
         get_current_paxos_id/1,
         get_next_proposal_id/1,
         get_next_expected_decision_id/1,
         get_new_group_id/2,
         split_group/4,
         get_group_node/1]).

-record(group_state, {group_id :: group_types:group_id(),
                      current_paxos_version,
                      next_proposal_version,
                      members :: group_types:group_member_list(),
                      version::non_neg_integer(),
                      interval::intervals:interval(),
                      acceptors,
                      learners,
                      proposals,
                      postponed_decisions :: gb_set()}).

-opaque group_state() :: #group_state{}.

-spec new_group_state(intervals:interval()) -> group_state().
new_group_state(Interval) ->
    GroupId = 1,
    Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
    Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
    #group_state{group_id = GroupId,
                 current_paxos_version=1,
                 next_proposal_version=-1,
                 members = [comm:this()],
                 version = 0,
                 interval = Interval,
                 acceptors = gb_trees:insert(comm:this(), Acceptor,
                                             gb_trees:empty()),
                 learners = gb_trees:insert(comm:this(), Learner,
                                            gb_trees:empty()),
                 proposals = gb_trees:empty(),
                 postponed_decisions = gb_sets:empty()}.

-spec get_group_id(group_state()) -> any().
get_group_id(#group_state{group_id=GroupId}) ->
    GroupId.

-spec get_interval(group_state()) -> intervals:interval().
get_interval(#group_state{interval=Interval}) ->
    Interval.

-spec get_version(group_state()) -> pos_integer().
get_version(#group_state{version=Version}) ->
    Version.

-spec get_size(group_state()) -> pos_integer().
get_size(#group_state{members=Members}) ->
    length(Members).

-spec get_acceptors(group_state()) -> list(comm:mypid()).
get_acceptors(#group_state{acceptors=Acceptors}) ->
    gb_trees:values(Acceptors).

-spec get_learners(group_state()) -> list(comm:mypid()).
get_learners(#group_state{learners=Learners}) ->
    gb_trees:values(Learners).

-spec get_members(group_state()) -> list(comm:mypid()).
get_members(#group_state{members=Members}) ->
    Members.

-spec is_member(group_state(), comm:mypid()) -> boolean().
is_member(GroupState, Pid) ->
    lists:member(Pid, GroupState#group_state.members).

-spec add_node(group_state(), Pid::comm:mypid(), Acceptor::comm:mypid(),
               Learner::comm:mypid()) -> group_state().
add_node(#group_state{members=Members, version=Version,
                      acceptors=Acceptors, learners=Learners} = GroupState,
         Pid, Acceptor, Learner) ->
    GroupState#group_state{
          members = [Pid | Members],
          version = Version + 1,
          acceptors = gb_trees:insert(Pid, Acceptor, Acceptors),
          learners = gb_trees:insert(Pid, Learner, Learners)
         }.

-spec remove_node(group_state(), Pid::comm:mypid()) -> group_state().
remove_node(#group_state{members=Members, version=Version,
                         acceptors=Acceptors, learners=Learners} = GroupState,
            Pid) ->
    GroupState#group_state{
      members = lists:delete(Pid, Members),
      version = Version + 1,
      acceptors = gb_trees:delete(Pid, Acceptors),
      learners = gb_trees:delete(Pid, Learners)
     }.

% @doc paxos_id for the next init_paxos call
-spec get_current_paxos_id(group_state()) -> group_types:paxos_id().
get_current_paxos_id(GroupState) ->
    get_next_expected_decision_id(GroupState).

% @doc paxos_id for the next init_paxos call
-spec get_next_paxos_id(group_state()) -> group_types:paxos_id().
get_next_paxos_id(#group_state{group_id=GroupId,
                               current_paxos_version=CurrentPaxosVersion}) ->
    {GroupId, CurrentPaxosVersion + 1}.

% @doc paxos_id for the next proposer:start_paxosid call
-spec get_next_proposal_id(group_state()) -> group_types:paxos_id().
get_next_proposal_id(#group_state{group_id=GroupId,
                                  next_proposal_version=NextProposalVersion}) ->
    {GroupId, NextProposalVersion}.

% @doc expected paxos_id for the next decision
-spec get_next_expected_decision_id(group_state()) -> group_types:paxos_id().
get_next_expected_decision_id(#group_state{group_id=GroupId,
                               current_paxos_version=CurrentPaxosVersion}) ->
    {GroupId, CurrentPaxosVersion}.

% @doc update state to reflect new proposal
-spec made_proposal(group_state(), group_types:paxos_id(), group_types:proposal()) -> group_state().
made_proposal(#group_state{group_id=GroupId,
                           current_paxos_version=CurrentPaxosVersion}
              = GroupState,
              {GroupId, PaxosVersion} = PaxosId,
             Proposal) ->
    GroupState#group_state{
      next_proposal_version = util:max(PaxosVersion + 1, CurrentPaxosVersion),
      proposals =
      gb_trees:insert(PaxosId, Proposal,
                      GroupState#group_state.proposals)}.

-spec postpone_decision(group_state(), group_types:paxos_id(), group_types:proposal()) -> group_state().
postpone_decision(#group_state{postponed_decisions = PostPonedDecisions} = GroupState, PaxosId, Proposal) ->
    GroupState#group_state{postponed_decisions = gb_sets:add({PaxosId, Proposal},
                                                             PostPonedDecisions)}.

-spec get_postponed_decisions(group_state()) -> list({group_types:paxos_id(), group_types:proposal()}).
get_postponed_decisions(#group_state{postponed_decisions = PostPonedDecisions}) ->
    gb_sets:to_list(PostPonedDecisions).

-spec remove_postponed_decision(group_state(), group_types:paxos_id(), group_types:proposal()) -> group_state().
remove_postponed_decision(#group_state{postponed_decisions = PostPonedDecisions} = GroupState,
                           PaxosId, Proposal) ->
    GroupState#group_state{postponed_decisions = gb_sets:delete_any({PaxosId, Proposal},
                                                                    PostPonedDecisions)}.

% @doc update state to reflect start of new paxos round
-spec init_paxos(group_state(), group_types:paxos_id()) -> group_state().
init_paxos(#group_state{group_id=GroupId, next_proposal_version=NextProposalVersion} = GroupState,
              {GroupId, PaxosVersion}) ->
    GroupState#group_state{
      current_paxos_version = PaxosVersion,
      next_proposal_version = util:max(PaxosVersion, NextProposalVersion)
      }.

-spec remove_proposal(group_state(), group_types:paxos_id()) -> group_state().
remove_proposal(GroupState, PaxosId) ->
    GroupState.

% @todo
    % GroupState#group_state{
    %   proposals =
    %   gb_trees:delete_any(PaxosId, GroupState#group_state.proposals)}.

-spec get_proposal(group_state(), group_types:paxos_id()) -> {value, group_types:proposal()} | none.
get_proposal(GroupState, PaxosId) ->
    gb_trees:lookup(PaxosId, GroupState#group_state.proposals).

-spec get_new_group_id(group_types:group_id(), left | right) ->
    group_types:group_id().
get_new_group_id(GroupId, left) ->
    GroupId bsl 1 + 0;
get_new_group_id(GroupId, right) ->
    GroupId bsl 1 + 1.

-spec split_group(OldGroupState::group_state(), NewGroupId::group_types:group_id(),
                  NewInterval::intervals:interval(), NewMemberList::list(comm:mypid())) ->
    group_state().
split_group(#group_state{acceptors=OldAcceptors,
                         learners=OldLearners,
                         members=OldMembers} = OldGroupState,
            NewGroupId, NewInterval, NewMemberList) ->
    % @todo: db
    FilterMembers = fun (Member) ->
                     not lists:member(Member, NewMemberList)
             end,
    RemovedMembers = lists:filter(FilterMembers, OldMembers),
    FilterMap = fun (OldNode, Map) ->
                     gb_trees:delete(OldNode, Map)
             end,
    Learners = lists:foldl(FilterMap, OldLearners, RemovedMembers),
    Acceptors = lists:foldl(FilterMap, OldAcceptors, RemovedMembers),
    OldGroupState#group_state{group_id = NewGroupId,
                 current_paxos_version=1,
                 next_proposal_version=-1,
                 members = NewMemberList,
                 version = 0,
                 interval = NewInterval,
                 acceptors = Acceptors,
                 learners = Learners,
                 proposals = gb_trees:empty()}.

-spec get_group_node(GroupState::group_state()) -> group_types:group_node().
get_group_node(#group_state{group_id = GroupId, version = Version,
                            members = Members}) ->
    {GroupId, Version, Members}.
