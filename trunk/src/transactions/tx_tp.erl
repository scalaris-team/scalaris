%% @copyright 2009, 2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%%            and onScale solutions GmbH

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
%% @doc Part of generic transactions implementation using Paxos Commit
%%           The role of a transaction participant.
%% @version $Id$
-module(tx_tp).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-author('schintke@onscale.de').

%%% public interface

%%% functions for gen_component module and supervisor callbacks
-export([on_init_TP/2, on_tx_commitreply/3]).

%%
%% Attention: this is not a separate process it runs inside the cs_node
%%            to get access to the ?DB
%%

%% messages handled in cs_node context:
on_init_TP({Tid, RTMs, TM, RTLogEntry, ItemId, PaxId}, CS_State) ->
    ?TRACE("tx_tp:on_init_TP({..., ...})~n", []),
    %% need Acceptors (given via RTMs), Learner,
    %% validate locally via callback
    DB = cs_state:get_db(CS_State),
    {NewDB, Proposal} = apply(element(1, RTLogEntry),
                              validate,
                              [DB, RTLogEntry]),

    %% initiate a paxos proposer round 0 with the proposal
    InstanceID = erlang:get(instance_id),
    Proposer = cs_send:make_global(
                 process_dictionary:get_group_member(paxos_proposer)),
    proposer:start_paxosid_with_proxy(cs_send:this(), Proposer, PaxId,
                                     _Acceptors = RTMs, Proposal,
                                     _Maj = 3, _MaxProposers = 4, 0),
    %% send registerTP to each RTM (send with it the learner id)
    [ cs_send:send(X, {register_TP, {Tid, ItemId, PaxId, cs_send:this()}})
      || X <- [TM | RTMs]],
    %% (optimized: embed the proposer`s accept message in registerTP message)
    cs_state:set_db(CS_State, NewDB).

on_tx_commitreply({PaxosId, RTLogEntry}, Result, CS_State) ->
    ?TRACE("tx_tp:on_tx_commitreply({, ...})~n", []),
    %% inform callback on commit/abort to release locks etc.
    DB = cs_state:get_db(CS_State),
    NewDB = apply(element(1, RTLogEntry), Result, [DB, RTLogEntry]),
    %% delete corresponding proposer state
    Proposer = cs_send:make_global(
                 process_dictionary:get_group_member(paxos_proposer)),
    proposer:stop_paxosids(Proposer, [PaxosId]),
    cs_state:set_db(CS_State, NewDB).
