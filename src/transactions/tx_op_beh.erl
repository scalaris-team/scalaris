%% @copyright 2009, 2010 onScale solutions GmbH

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc Part of generic transactions implementation.
%%      The behaviour of an operation in a transaction.
%% @version $Id$
-module(tx_op_beh).
-author('schintke@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-include("scalaris.hrl").
-callback work_phase(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                     rdht_tx:request()) -> ok.
-callback validate_prefilter(tx_tlog:tlog_entry()) -> [tx_tlog:tlog_entry()].
-callback validate(?DB:db(), tx_tlog:tlog_entry()) -> {?DB:db(), prepared | abort}.
-callback commit(?DB:db(), tx_tlog:tlog_entry(), prepared | abort) -> ?DB:db().
-callback abort(?DB:db(), tx_tlog:tlog_entry(), prepared | abort) -> ?DB:db().
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     %% do the work phase *asynchronously*, replies to local client with a msg
     %% work_phase(ClientPid, Id, Request) ->
     %%   msg {work_phase_reply, Id, TLogEntry}
     {work_phase, 3},
     %% May make several ones from a single TransLog item (item replication)
     %% validate_prefilter(TransLogEntry) ->
     %%   [TransLogEntries] (replicas)
     {validate_prefilter, 1},
     %% validate a single item
     %% validate(DB, RTLogentry) -> {DB, Proposal (prepared/abort)}
     {validate, 2},
     %% commit(DB, RTLogentry, OwnProposalWas)
     {commit, 3},
     %% abort(DB, RTLogentry, OwnProposalWas)
     {abort, 3}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
