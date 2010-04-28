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
-author('schintke@onscale.de').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
% for behaviour
-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %% do the work phase *asynchronously*, replies to local client with a msg
     %% work_phase(ClientPid, Id, Request) ->
     %%   msg {work_phase_reply, Id, TransLogEntry}
     {work_phase, 3},
     %% do the work phase *synchronously* based on an existing translog entry
     %% work_phase(TransLog, Request) -> NewTransLogEntry
     {work_phase, 2},
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
