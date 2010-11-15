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
-module(group_view).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new/1,
         get_group_id/1,
         get_version/1,
         get_index_in_group/1,
         recalculate_index/1,
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

% view() must be serializable, i.e. don't put ets-tables in here
-record(view, {group_id :: group_types:group_id(),
               current_paxos_version,
               next_proposal_version,
               members :: group_types:group_member_list(),
               version::non_neg_integer(),
               interval::intervals:interval(),
               acceptors,
               learners,
               proposals,
               index_in_group::non_neg_integer(),
               postponed_decisions :: gb_set()}).

-opaque view() :: #view{}.

-spec new(intervals:interval()) -> view().
new(Interval) ->
    GroupId = 1,
    Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
    Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
    #view{group_id = GroupId,
          current_paxos_version=1,
          next_proposal_version=-1,
          members = [comm:this()],
          version = 0,
          index_in_group = 0,
          interval = Interval,
          acceptors = gb_trees:insert(comm:this(), Acceptor,
                                      gb_trees:empty()),
          learners = gb_trees:insert(comm:this(), Learner,
                                     gb_trees:empty()),
          proposals = gb_trees:empty(),
          postponed_decisions = gb_sets:empty()}.

-spec get_group_id(view()) -> any().
get_group_id(#view{group_id=GroupId}) ->
    GroupId.

-spec get_interval(view()) -> intervals:interval().
get_interval(#view{interval=Interval}) ->
    Interval.

-spec get_version(view()) -> pos_integer().
get_version(#view{version=Version}) ->
    Version.

-spec get_index_in_group(view()) -> non_neg_integer().
get_index_in_group(#view{index_in_group=Index}) ->
    Index.

-spec recalculate_index(view()) -> view().
recalculate_index(View) ->
    Index = index_of(comm:this(), View#view.members),
    View#view{index_in_group = Index}.

-spec get_size(view()) -> pos_integer().
get_size(#view{members=Members}) ->
    length(Members).

-spec get_acceptors(view()) -> list(comm:mypid()).
get_acceptors(#view{acceptors=Acceptors}) ->
    gb_trees:values(Acceptors).

-spec get_learners(view()) -> list(comm:mypid()).
get_learners(#view{learners=Learners}) ->
    gb_trees:values(Learners).

-spec get_members(view()) -> list(comm:mypid()).
get_members(#view{members=Members}) ->
    Members.

-spec is_member(view(), comm:mypid()) -> boolean().
is_member(View, Pid) ->
    lists:member(Pid, View#view.members).

-spec add_node(view(), Pid::comm:mypid(), Acceptor::comm:mypid(),
               Learner::comm:mypid()) -> view().
add_node(#view{members=Members, version=Version,
               acceptors=Acceptors, learners=Learners} = View,
         Pid, Acceptor, Learner) ->
    View#view{
      members = [Pid | Members],
      version = Version + 1,
      acceptors = gb_trees:insert(Pid, Acceptor, Acceptors),
      learners = gb_trees:insert(Pid, Learner, Learners)
     }.

-spec remove_node(view(), Pid::comm:mypid()) -> view().
remove_node(#view{members=Members, version=Version,
                  acceptors=Acceptors, learners=Learners} = View,
            Pid) ->
    View#view{
      members = lists:delete(Pid, Members),
      version = Version + 1,
      acceptors = gb_trees:delete(Pid, Acceptors),
      learners = gb_trees:delete(Pid, Learners)
     }.

% @doc paxos_id for the next init_paxos call
-spec get_current_paxos_id(view()) -> group_types:paxos_id().
get_current_paxos_id(View) ->
    get_next_expected_decision_id(View).

% @doc paxos_id for the next init_paxos call
-spec get_next_paxos_id(view()) -> group_types:paxos_id().
get_next_paxos_id(#view{group_id=GroupId,
                        current_paxos_version=CurrentPaxosVersion}) ->
    {GroupId, CurrentPaxosVersion + 1}.

% @doc paxos_id for the next proposer:start_paxosid call
-spec get_next_proposal_id(view()) -> group_types:paxos_id().
get_next_proposal_id(#view{group_id=GroupId,
                           next_proposal_version=NextProposalVersion}) ->
    {GroupId, NextProposalVersion}.

% @doc expected paxos_id for the next decision
-spec get_next_expected_decision_id(view()) -> group_types:paxos_id().
get_next_expected_decision_id(#view{group_id=GroupId,
                                    current_paxos_version=CurrentPaxosVersion}) ->
    {GroupId, CurrentPaxosVersion}.

% @doc update state to reflect new proposal
-spec made_proposal(view(), group_types:paxos_id(),
                    group_types:proposal()) -> view().
made_proposal(#view{group_id=GroupId,
                    current_paxos_version=CurrentPaxosVersion}
              = View,
              {GroupId, PaxosVersion} = PaxosId,
              Proposal) ->
    View#view{
      next_proposal_version = util:max(PaxosVersion + 1, CurrentPaxosVersion),
      proposals =
      gb_trees:insert(PaxosId, Proposal,
                      View#view.proposals)}.

-spec postpone_decision(view(), group_types:paxos_id(),
                        group_types:proposal()) -> view().
postpone_decision(#view{postponed_decisions = PostPonedDecisions} = View, PaxosId, Proposal) ->
    View#view{postponed_decisions = gb_sets:add({PaxosId, Proposal},
                                                PostPonedDecisions)}.

-spec get_postponed_decisions(view()) -> list({group_types:paxos_id(), group_types:proposal()}).
get_postponed_decisions(#view{postponed_decisions = PostPonedDecisions}) ->
    gb_sets:to_list(PostPonedDecisions).

-spec remove_postponed_decision(view(), group_types:paxos_id(), group_types:proposal()) ->
    view().
remove_postponed_decision(#view{postponed_decisions = PostPonedDecisions} = View,
                          PaxosId, Proposal) ->
    View#view{postponed_decisions = gb_sets:delete_any({PaxosId, Proposal},
                                                       PostPonedDecisions)}.

% @doc update state to reflect start of new paxos round
-spec init_paxos(view(), group_types:paxos_id()) -> view().
init_paxos(#view{group_id=GroupId, next_proposal_version=NextProposalVersion} = View,
           {GroupId, PaxosVersion}) ->
    View#view{
      current_paxos_version = PaxosVersion,
      next_proposal_version = util:max(PaxosVersion, NextProposalVersion)
     }.

-spec remove_proposal(view(), group_types:paxos_id()) -> view().
remove_proposal(View, PaxosId) ->
View#view{
  proposals =
  gb_trees:delete_any(PaxosId, View#view.proposals)}.

-spec get_proposal(view(), group_types:paxos_id()) ->
    {value, group_types:proposal()} | none.
get_proposal(View, PaxosId) ->
    gb_trees:lookup(PaxosId, View#view.proposals).

-spec get_new_group_id(group_types:group_id(), left | right) ->
    group_types:group_id().
get_new_group_id(GroupId, left) ->
    GroupId bsl 1 + 0;
get_new_group_id(GroupId, right) ->
    GroupId bsl 1 + 1.

-spec split_group(OldView::view(), NewGroupId::group_types:group_id(),
                  NewInterval::intervals:interval(), NewMemberList::list(comm:mypid())) ->
    view().
split_group(#view{acceptors=OldAcceptors,
                  learners=OldLearners,
                  members=OldMembers} = OldView,
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
    OldView#view{group_id = NewGroupId,
                 current_paxos_version=1,
                 next_proposal_version=-1,
                 members = NewMemberList,
                 version = 0,
                 interval = NewInterval,
                 acceptors = Acceptors,
                 learners = Learners,
                 proposals = gb_trees:empty()}.

-spec get_group_node(View::view()) -> group_types:group_node().
get_group_node(#view{group_id = GroupId, interval = Range, version = Version,
                     members = Members}) ->
    {GroupId, Range, Version, Members}.

index_of(_, []) ->
    error;
index_of(P, [P|_Rest]) ->
    1;
index_of(P, [_|Rest]) ->
    case index_of(P, Rest) of
        error -> error;
        Index -> 1 + Index
    end.

