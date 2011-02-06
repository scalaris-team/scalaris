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
%% @doc    view of the rsm
%% @end
%% @version $Id$
-module(rsm_view).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new_primary/2,
         get_members/1, is_member/2,
         add_node/4,
         get_next_paxos_id/1,
         get_current_paxos_id/1,
         get_next_proposal_id/1,
         get_next_expected_decision_id/1,
         get_learners/1,
         get_acceptors/1,
         get_size/1,
         get_index_in_rsm/1,
         recalculate_index/1,
         get_version/1,
         increment_version/1,
         init_paxos/2,
         made_proposal/3, get_proposal/2, remove_proposal/2,
         postpone_decision/3, get_postponed_decisions/1, remove_postponed_decision/3]).

% view() must be serializable, i.e. don't put ets-tables in here
-record(view_type, {rsm_id :: rsm_state:rsm_id(),
                    index_in_rsm :: non_neg_integer(),
                    current_paxos_version :: integer(),
                    next_proposal_version :: integer(),
                    members :: rsm_state:rsm_member_list(),
                    version::non_neg_integer(),
                    acceptors :: gb_tree(),
                    learners :: gb_tree(),
                    proposals :: gb_tree(),
                    postponed_decisions :: gb_set()
                   }).

-opaque view_type() :: #view_type{}.

-ifdef(with_export_type_support).
-export_type([view_type/0]).
-endif.

-spec new_primary(comm:mypid(), comm:mypid()) -> view_type().
new_primary(Acceptor, Learner) ->
    #view_type{
             rsm_id = uuid, %@todo
             current_paxos_version = 1,
             next_proposal_version = -1,
             members = [comm:this()],
             version = 0,
             index_in_rsm = 0,
             acceptors = gb_trees:insert(comm:this(), Acceptor,
                                         gb_trees:empty()),
             learners = gb_trees:insert(comm:this(), Learner,
                                        gb_trees:empty()),
             proposals = gb_trees:empty(),
             postponed_decisions = gb_sets:empty()
            }.

-spec get_members(view_type()) ->
    rsm_state:rsm_member_list().
get_members(View) ->
    View#view_type.members.

-spec is_member(view_type(), comm:mypid()) -> boolean().
is_member(View, Pid) ->
    lists:member(Pid, View#view_type.members).

-spec add_node(view_type(), Pid::comm:mypid(), Acceptor::comm:mypid(),
               Learner::comm:mypid()) -> view_type().
add_node(#view_type{members=Members, version=Version,
                    acceptors=Acceptors, learners=Learners} = View,
         Pid, Acceptor, Learner) ->
    View#view_type{
      members = [Pid | Members],
      version = Version + 1,
      acceptors = gb_trees:insert(Pid, Acceptor, Acceptors),
      learners = gb_trees:insert(Pid, Learner, Learners)
     }.

% @doc increment version of the view
-spec increment_version(view_type()) -> view_type().
increment_version(#view_type{version=Version} = View) ->
    View#view_type{version = Version + 1}.

% @doc paxos_id for the next init_paxos call
-spec get_current_paxos_id(view_type()) -> rsm_state:paxos_id().
get_current_paxos_id(View) ->
    get_next_expected_decision_id(View).

% @doc paxos_id for the next init_paxos call
-spec get_next_paxos_id(view_type()) -> rsm_state:paxos_id().
get_next_paxos_id(#view_type{rsm_id=RSMId,
                        current_paxos_version=CurrentPaxosVersion}) ->
    {RSMId, CurrentPaxosVersion + 1}.

% @doc paxos_id for the next proposer:start_paxosid call
-spec get_next_proposal_id(view_type()) -> rsm_state:paxos_id().
get_next_proposal_id(#view_type{rsm_id=RSMId,
                           next_proposal_version=NextProposalVersion}) ->
    {RSMId, NextProposalVersion}.

% @doc expected paxos_id for the next decision
-spec get_next_expected_decision_id(view_type()) -> group_types:paxos_id().
get_next_expected_decision_id(#view_type{rsm_id=RSMId,
                                    current_paxos_version=CurrentPaxosVersion}) ->
    {RSMId, CurrentPaxosVersion}.

-spec get_acceptors(view_type()) -> list(comm:mypid()).
get_acceptors(#view_type{acceptors=Acceptors}) ->
    gb_trees:values(Acceptors).

-spec get_learners(view_type()) -> list(comm:mypid()).
get_learners(#view_type{learners=Learners}) ->
    gb_trees:values(Learners).

% @doc return the number of members of this view
-spec get_size(view_type()) -> non_neg_integer().
get_size(View) ->
    length(View#view_type.members).

-spec get_version(view_type()) -> pos_integer().
get_version(#view_type{version=Version}) ->
    Version.

-spec get_index_in_rsm(view_type()) -> non_neg_integer().
get_index_in_rsm(#view_type{index_in_rsm=Index}) ->
    Index.

-spec recalculate_index(view_type()) -> view_type().
recalculate_index(View) ->
    Index = index_of(comm:this(), View#view_type.members),
    View#view_type{index_in_rsm = Index}.

-spec init_paxos(view_type(), rsm_state:paxos_id()) -> view_type().
init_paxos(#view_type{rsm_id=RSMId} = View, {RSMId, PaxosVersion}) ->
    NextProposalVersion = View#view_type.next_proposal_version,
    View#view_type{
      current_paxos_version = PaxosVersion,
      next_proposal_version = util:max(PaxosVersion, NextProposalVersion)
     }.

% @doc update state to reflect new proposal
-spec made_proposal(view_type(), rsm_state:paxos_id(),
                    rsm_state:proposal()) -> view_type().
made_proposal(#view_type{rsm_id=RSMId,
                    current_paxos_version=CurrentPaxosVersion}
              = View,
              {RSMId, PaxosVersion} = PaxosId,
              Proposal) ->
    View#view_type{
      next_proposal_version = util:max(PaxosVersion + 1, CurrentPaxosVersion),
      proposals =
      gb_trees:insert(PaxosId, Proposal,
                      View#view_type.proposals)}.

-spec get_proposal(view_type(), rsm_state:paxos_id()) ->
    {value, rsm_state:proposal()} | none.
get_proposal(View, PaxosId) ->
    gb_trees:lookup(PaxosId, View#view_type.proposals).

-spec remove_proposal(view_type(), rsm_state:paxos_id()) -> view_type().
remove_proposal(View, PaxosId) ->
View#view_type{
  proposals =
  gb_trees:delete_any(PaxosId, View#view_type.proposals)}.

-spec postpone_decision(view_type(), rsm_state:paxos_id(),
                        rsm_state:proposal()) -> view_type().
postpone_decision(#view_type{postponed_decisions = PostPonedDecisions} = View, PaxosId, Proposal) ->
    View#view_type{postponed_decisions = gb_sets:add({PaxosId, Proposal},
                                                     PostPonedDecisions)}.

-spec get_postponed_decisions(view_type()) -> list({rsm_state:paxos_id(), rsm_state:proposal()}).
get_postponed_decisions(#view_type{postponed_decisions = PostPonedDecisions}) ->
    gb_sets:to_list(PostPonedDecisions).

-spec remove_postponed_decision(view_type(), rsm_state:paxos_id(), rsm_state:proposal()) ->
    view_type().
remove_postponed_decision(#view_type{postponed_decisions = PostPonedDecisions} = View,
                          PaxosId, Proposal) ->
    View#view_type{postponed_decisions = gb_sets:delete_any({PaxosId, Proposal},
                                                            PostPonedDecisions)}.

index_of(_, []) ->
    error;
index_of(P, [P|_Rest]) ->
    1;
index_of(P, [_|Rest]) ->
    case index_of(P, Rest) of
        error -> error;
        Index -> 1 + Index
    end.
