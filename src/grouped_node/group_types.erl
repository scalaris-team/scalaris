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
-module(group_types).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-export([all/0]).

-type(group_id() :: non_neg_integer()).

-type(paxos_id() :: {group_id(), pos_integer()}).

-type(proposal() :: {group_node_join, Pid::comm:mypid(), Acceptor::comm:mypid(),
                      Learner::comm:mypid()}
                  | {group_node_remove, Pid::comm:mypid(), Proposer::comm:mypid()}
                  | {group_split, Proposer::comm:mypid(), SplitKey::?RT:key(),
                     LeftGroup::list(comm:mypid()), RightGroup::list(comm:mypid())}
                  | {read, Key::?RT:key(), Value::any(), Version::pos_integer(),
                     Client::comm:mypid(), Proposer::comm:mypid()}
                  | {write, Key::?RT:key(), Value::any(), Version::pos_integer(),
                     Client::comm:mypid(), Proposer::comm:mypid()}).

-type(decision_hint() :: my_proposal_won | had_no_proposal).

% for communication with other nodes, e.g. update_pred
-type(group_node() :: {GroupId::group_id(),
                       Range::intervals:interval(),
                       Version::non_neg_integer(),
                       Members::list(comm:mypid())}).

-type(group_member() :: comm:mypid()).
-type(group_member_list() :: list(group_member())).


-ifdef(with_export_type_support).
-export_type([group_id/0, paxos_id/0, proposal/0, decision_hint/0,
              group_node/0, group_member/0, group_member_list/0]).
-endif.

all() ->
    intervals:new('[', ?MINUS_INFINITY, ?PLUS_INFINITY, ']').
