%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Nico Kruber <kruber@zib.de>
%%% @doc Generic code for routing table implementations.
%%%
%%% Note: Including modules need to provide some types and functions, i.e.
%%% rt(), key(), export_rt_to_dht_node/4, to_pid_list/1
%%% @end
%% @version $Id$

%% userdevguide-begin rt_generic:check
%% @doc Notifies the dht_node and failure detector if the routing table changed.
%%      Provided for convenience (see check/6).
-include("types.hrl").

-spec check(Old::rt(), New::rt(), key(), node:node_type(),
            node:node_type()) -> ok.
check(Old, New, Id, Pred, Succ) ->
    check(Old, New, Id, Pred, Pred, Succ, Succ, true).

-spec check(Old::rt(), New::rt(), key(), node:node_type(),
            node:node_type(), ReportToFD::boolean()) -> ok.
check(Old, New, Id, Pred, Succ, ReportToFD) ->
    check(Old, New, Id, Pred, Pred, Succ, Succ, ReportToFD).

-spec check(Old::rt(), New::rt(), Id::key(),
            OldPred::node:node_type(), NewPred::node:node_type(),
            OldSucc::node:node_type(), NewSucc::node:node_type()) -> ok.
check(Old, New, Id, OldPred, NewPred, OldSucc, NewSucc) ->
    check(Old, New, Id, OldPred, NewPred, OldSucc, NewSucc, true).

%% @doc Notifies the dht_node if the routing table changed. Also updates the
%%      failure detector if ReportToFD is set.
-spec check(Old::rt(), New::rt(), MyId::key(),
            OldPred::node:node_type(), NewPred::node:node_type(),
            OldSucc::node:node_type(), NewSucc::node:node_type(),
            ReportToFD::boolean()) -> ok.
check(X, X, _Id, Pred, Pred, Succ, Succ, _) ->
    ok;
check(OldRT, NewRT, Id, _, NewPred, _, NewSucc, true) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rt_update, export_rt_to_dht_node(NewRT, Id, NewPred, NewSucc)}),
    % update failure detector:
    NewPids = to_pid_list(NewRT), OldPids = to_pid_list(OldRT),
    fd:update_subscriptions(OldPids, NewPids);
check(_OldRT, NewRT, Id, _, NewPred, _, NewSucc, false) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rt_update, export_rt_to_dht_node(NewRT, Id, NewPred, NewSucc)}).
%% userdevguide-end rt_generic:check
