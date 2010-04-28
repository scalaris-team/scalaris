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
-export([init/0, on_init_TP/2, on_tx_commitreply/3]).

init() ->
    InstanceID = get(instance_id),
    Table = list_to_atom(lists:flatten(
                           io_lib:format("~p_tx_tp", [InstanceID]))),
    pdb:new(Table, [set, private, named_table]).

%%
%% Attention: this is not a separate process!!
%% It runs inside the dht_node to get access to the ?DB
%%

%% messages handled in dht_node context:
on_init_TP({Tid, RTMs, TM, RTLogEntry, ItemId, PaxId}, DHT_Node_State) ->
    ?TRACE("tx_tp:on_init_TP({..., ...})~n", []),
    %% need Acceptors (given via RTMs), Learner,
    %% validate locally via callback
    DB = dht_node_state:get_db(DHT_Node_State),
    {NewDB, Proposal} = apply(element(1, RTLogEntry),
                              validate,
                              [DB, RTLogEntry]),

    %% remember own proposal for lock release
    TP_DB = dht_node_state:get_tx_tp_db(DHT_Node_State),
    pdb:set({PaxId, Proposal}, TP_DB),

    %% initiate a paxos proposer round 0 with the proposal
    Proposer = cs_send:make_global(dht_node_state:get_my_proposer(DHT_Node_State)),
    proposer:start_paxosid_with_proxy(cs_send:this(), Proposer, PaxId,
                                     _Acceptors = RTMs, Proposal,
                                     _Maj = 3, _MaxProposers = 4, 0),
    %% send registerTP to each RTM (send with it the learner id)
    [ cs_send:send(X, {register_TP, {Tid, ItemId, PaxId, cs_send:this()}})
      || X <- [TM | RTMs]],
    %% (optimized: embed the proposer's accept message in registerTP message)
    dht_node_state:set_db(DHT_Node_State, NewDB).

on_tx_commitreply({PaxosId, RTLogEntry}, Result, DHT_Node_State) ->
    ?TRACE("tx_tp:on_tx_commitreply({, ...})~n", []),
    %% inform callback on commit/abort to release locks etc.
    DB = dht_node_state:get_db(DHT_Node_State),

    % get own proposal for lock release
    TP_DB = dht_node_state:get_tx_tp_db(DHT_Node_State),
    {PaxosId, Proposal} = pdb:get(PaxosId, TP_DB),

    NewDB = apply(element(1, RTLogEntry), Result, [DB, RTLogEntry, Proposal]),
    %% delete corresponding proposer state
    Proposer = cs_send:make_global(dht_node_state:get_my_proposer(DHT_Node_State)),
    proposer:stop_paxosids(Proposer, [PaxosId]),
    pdb:delete(PaxosId, TP_DB),
    dht_node_state:set_db(DHT_Node_State, NewDB).
