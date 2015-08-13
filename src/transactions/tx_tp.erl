%% @copyright 2009-2015 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Part of generic transactions implementation using Paxos Commit
%%           The role of a transaction participant.
%% @version $Id$
-module(tx_tp).
-author('schintke@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
%% -define(TRACE_SNAP(X, Y), ct:pal(X, Y)).
-define(TRACE_SNAP(X, Y), ?TRACE(X, Y)).

%%% public interface

%%% functions for gen_component module and supervisor callbacks
-export([init/0, on_init_TP/2]).
-export([on_do_commit_abort/4, on_do_commit_abort_fwd/7]).

-spec init() -> pdb:tableid().
init() ->
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = erlang:list_to_atom(pid_groups:group_to_filename(pid_groups:my_groupname()) ++ "_tx_tp"),
    %%Table = pdb:new(TableName, [set, protected, named_table]).
    %% use random table name provided by ets to *not* generate an atom
    _Table = pdb:new(?MODULE, [set]).

%%
%% Attention: this is not a separate process!!
%%            It runs inside the dht_node to get access to the db_dht
%%

-spec on_init_TP({tx_tm_rtm:tx_id(),
                  [comm:mypid()], [comm:mypid()], comm:mypid(),
                  tx_tlog:tlog_entry(),
                  tx_tm_rtm:tx_item_id(),
                  tx_tm_rtm:paxos_id(), tx_tlog:snap_number()},
                  dht_node_state:state()) -> dht_node_state:state().
%% messages handled in dht_node context:
%% PreCond: check for DB responsibility must still be valid (ref. lookup_fin handling)
on_init_TP({Tid, RTMs, Accs, TM, RTLogEntry, ItemId, PaxId, _SnapNo}, DHT_Node_State) ->
    %?TRACE("tx_tp:on_init_TP({..., ...})~n", []),
    %% validate locally via callback
    DB = dht_node_state:get(DHT_Node_State, db),
    LocalSnapNumber = snapshot_state:get_number(dht_node_state:get(DHT_Node_State,snapshot_state)),
    %% check only necessary in case of damaged routing
    %% but: check already done by lookup_fin which uses post_op and thus the check is still valid
%%     Key = tx_tlog:get_entry_key(RTLogEntry),
%%     case dht_node_state:is_db_responsible(Key, DHT_Node_State) of
%%         true ->
            {TmpDB, Proposal} =
                case tx_tlog:get_entry_operation(RTLogEntry) of
                    ?read ->
                        rdht_tx_read:validate(DB, LocalSnapNumber, RTLogEntry);
                    ?write ->
                        rdht_tx_write:validate(DB, LocalSnapNumber, RTLogEntry)
                end,
            %% initiate a paxos proposer round 0 with the proposal
            R = config:read(replication_factor),
            Proposer = comm:make_global(pid_groups:get_my(paxos_proposer)),
            proposer:start_paxosid(Proposer, PaxId,
                                   _Acceptors = Accs, Proposal,
                                   _Maj = quorum:majority_for_accept(R),
                                   _MaxProposers = R + 1, %% rtms + client
                                   0),
            %% send registerTP to each RTM (send with it the learner id)
            This = comm:this(),
            _ = [ comm:send(X, {?register_TP, {Tid, ItemId, PaxId, This}})
                    || X <- [TM | RTMs], unknown =/= X],
            %% (optimized: embed the proposer's accept message in registerTP message)
            %% remember own proposal for lock release
            TP_DB = dht_node_state:get(DHT_Node_State, tx_tp_db),
            pdb:set({PaxId, Proposal}, TP_DB),

            dht_node_state:set_db(DHT_Node_State, TmpDB)%;
%%         false ->
%%             %% forward commit to now responsible node
%%             dht_node_lookup:lookup_aux(
%%               DHT_Node_State, Key, 0, {?init_TP, _Params}),
%%             DHT_Node_State
%%     end
    .
-spec on_do_commit_abort({tx_tm_rtm:paxos_id(),
                          tx_tlog:tlog_entry(),
                          comm:mypid(),
                          tx_tm_rtm:tx_item_id()},
                         ?commit | ?abort, tx_tlog:snap_number(), dht_node_state:state())
                        -> dht_node_state:state().
on_do_commit_abort({PaxosId, RTLogEntry, TM, TMItemId} = Id, Result, TMSnapNo, DHT_Node_State) ->
    %?TRACE("tx_tp:on_do_commit_abort({, ...})~n", []),
    %% inform callback on commit/abort to release locks etc.
    % get own proposal for lock release
    TP_DB = dht_node_state:get(DHT_Node_State, tx_tp_db),
    case pdb:get(PaxosId, TP_DB) of
        {PaxosId, Proposal} ->
            NewDB = update_db_or_forward(TM, TMItemId, RTLogEntry, Result, Proposal, TMSnapNo, DHT_Node_State),
            %% delete corresponding proposer state
            Proposer = comm:make_global(pid_groups:get_my(paxos_proposer)),
            proposer:stop_paxosids(Proposer, [PaxosId]),
            pdb:delete(PaxosId, TP_DB),
            dht_node_state:set_db(DHT_Node_State, NewDB);
        undefined ->
            %% delay or forward commit until corresponding validate seen
            Key = tx_tlog:get_entry_key(RTLogEntry),
            case dht_node_state:is_db_responsible(Key, DHT_Node_State) of
                true ->
                    %% tx is already commited and we either are the
                    %% slow minority or we already got such a request
                    %% and already deleted the state,
                    %% so we claim we committed
                    %% msg_delay:send_local(
                    %%   1, self(), {?tp_do_commit_abort, Id, Result});
                    comm:send(TM, {?tp_committed, TMItemId});
                false ->
                    % we don't have an own proposal yet (no validate seen), so we forward msg as is.
                    api_dht_raw:unreliable_lookup(Key, {?tp_do_commit_abort, Id,
                                                        Result, TMSnapNo})
            end,
            DHT_Node_State
    end.

-spec on_do_commit_abort_fwd(comm:mypid(), tx_tm_rtm:tx_item_id(),
                             tx_tlog:tlog_entry(),
                             ?commit | ?abort, ?prepared | ?abort, non_neg_integer(),
                             dht_node_state:state())
                           -> dht_node_state:state().
on_do_commit_abort_fwd(TM, TMItemId, RTLogEntry, Result, OwnProposal, TMSnapNo, DHT_Node_State) ->
    NewDB = update_db_or_forward(TM, TMItemId, RTLogEntry, Result, OwnProposal, TMSnapNo, DHT_Node_State),
    dht_node_state:set_db(DHT_Node_State, NewDB).

update_db_or_forward(TM, TMItemId, RTLogEntry, Result, OwnProposal, TMSnapNo, DHT_Node_State) ->
    %% Check for DB responsibility:
    DB = dht_node_state:get(DHT_Node_State, db),
    Key = tx_tlog:get_entry_key(RTLogEntry),
    SnapState = dht_node_state:get(DHT_Node_State,snapshot_state),
    OwnSnapNo = snapshot_state:get_number(SnapState),
    case dht_node_state:is_db_responsible(Key, DHT_Node_State) of
        true ->
            ?TRACE("~p tx_tp:update_db_or_forward before commit/abort~n",[comm:this()]),
            ?TRACE("~p tx_tp:update_db_or_forward before db: ~p~n",[comm:this(),DB]),
            ?TRACE("~p tx_tp:update_db_or_forward before db data: ~p~n",[comm:this(),db_dht:get_data(DB)]),
            ?TRACE("~p tx_tp:update_db_or_forward before snapshot data: ~p~n",[comm:this(),db_dht:get_snapshot_data(DB)]),
            ?TRACE("~p tx_tp:update_db_or_forward incoming operation: ~p~n",[comm:this(),{tx_tlog:get_entry_operation(RTLogEntry), Result}]),
            Res =
                case tx_tlog:get_entry_operation(RTLogEntry) of
                    ?read when Result =:= ?abort ->
                        rdht_tx_read:abort(DB, RTLogEntry, OwnProposal, TMSnapNo, OwnSnapNo);
                    ?read when Result =:= ?commit ->
                        rdht_tx_read:commit(DB, RTLogEntry, OwnProposal, TMSnapNo, OwnSnapNo);
                    ?write when Result =:= ?abort ->
                        rdht_tx_write:abort(DB, RTLogEntry, OwnProposal, TMSnapNo, OwnSnapNo);
                    ?write when Result =:= ?commit ->
                        rdht_tx_write:commit(DB, RTLogEntry, OwnProposal, TMSnapNo, OwnSnapNo)
                end,
            comm:send(TM, {?tp_committed, TMItemId}),
            % check if snapshot is running and if so, if it's already done
            case snapshot_state:is_in_progress(SnapState) andalso db_dht:snapshot_is_running(Res) of
                true ->
                    case db_dht:snapshot_is_lockfree(Res) of
                        true ->
                            ?TRACE_SNAP("~p tx_tp:update_db_or_forward snapshot is finally done~n",[comm:this()]),
                            comm:send_local(self(), {local_snapshot_is_done});
                        _ ->
                            ?TRACE_SNAP("~p tx_tp:update_db_or_forward snapshot is still
                                        not done~n~p",[comm:this(), Res]),
                            ok
                    end;
                _ ->
                    ?TRACE_SNAP("~p tx_tp:update_db_or_forward something
                                else~n~p~n~p",[comm:this(), X, Res]),
                    ok
            end,
            Res;
        false ->
            %% forward commit to now responsible node
            api_dht_raw:unreliable_lookup(Key, {?tp_do_commit_abort_fwd,
                                                TM, TMItemId, RTLogEntry,
                                                Result, OwnProposal, TMSnapNo}),
           DB
    end.
