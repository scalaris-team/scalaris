%  Copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : tmanager.erl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : the manager or replicated manager for a transaction
%%% Created :  11 Feb 2007 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
%% @author Monika Moser <moser@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
%% @doc
-module(tmanager).

-author('moser@zib.de').
-vsn('$Id$').

-include("trecords.hrl").
-include("../include/scalaris.hrl").

-import(boot_logger).
-import(config).
-import(cs_send).
-import(ct).
-import(dict).
-import(erlang).
-import(io_lib).
-import(io).
-import(lists).
-import(?RT).
-import(timer).
-import(txlog).

-export([start_manager/6, start_manager_commit/6, start_replicated_manager/2]).

%% for timer module
-export([read_phase/1, init_phase/1, start_commit/1]).

%%==============================================================================
%% Information on the algorithms:
%%
%% 1) Ballot Numbers of the Paxos Protocol:
%%    * Each TP votes with the Ballot Number 1
%%    * Each initial leader gets Ballot Number 2
%%    * Replicated Transaction Managers get Ballot numbers > 2
%%
%%    Ballot numbers reflect the order of replicated transaction
%%      mangers. When the leader fails the next rTM takes over
%%      after the interval of (Ballot-2)*config:read(leader_detector_interval).
%%      rTMs do not really elect a leader, rather they behave like a
%%      leader after a certain amount of time. This is reasonable as
%%      the commit phase should not take longer than some seconds.
%%
%% TODO:
%%      RTMS send outcome to Owner in case the leader fails
%%      add unique references to messages on the outcome of a transaction
%%        in case two managers send the outcome, one message should be dropped
%%      Store outcome for TPs that did not receive a decision
%%==============================================================================

%% start a manager: it will execute the readphase and commit phase
start_manager(TransFun, SuccessFun, FailureFun, Owner, TID, InstanceId)->
    erlang:put(instance_id, InstanceId),
    {Res, ResVal} = tmanager:read_phase(TransFun),
    case Res of
        ok ->
            {ReadPVal, Items} = ResVal,
            commit_phase(Items, SuccessFun, ReadPVal, FailureFun, Owner, TID);
        fail->
            tsend:send_to_client(Owner, FailureFun(ResVal));
        abort ->
            tsend:send_to_client(Owner, SuccessFun({user_abort, ResVal}));
        _ ->
            io:format("readphase res: ~p ; resval: ~p~n", [Res, ResVal]),
            tsend:send_to_client(Owner, FailureFun(Res))
    end.

%% start a manager without a read phase
start_manager_commit(Items, SuccessFun, FailureFun, Owner, TID, InstanceId)->
    erlang:put(instance_id, InstanceId),
    commit_phase(Items, SuccessFun, empty ,FailureFun, Owner, TID).

%% readphase: execute the transaction function
%% it can fail, due to an indication in the TFun given by the user
%% or when operations on items fail, not catched by the user in the TFun
read_phase(TFun)->
    TLog = txlog:new(),
    {{TFunFlag, ReadVal}, TLog2} = try TFun(TLog)
                                   catch {abort, State} ->
                                           State
                                   end,
    %?TLOGN("TLOG in readphase ~p~n", [TLog2]),
    case TFunFlag of
        ok ->
            FailedItems = txlog:filter_by_status(TLog2, fail),
            case FailedItems of
                [] -> {ok, {ReadVal, trecords:create_items(TLog2)}};
                _ -> {fail, fail}
            end;
        _ -> %%TFunFlag == abort/fail ReadVal not_found, timeout, fail
            {TFunFlag, ReadVal}
    end.

%% commit phase
commit_phase(Items, SuccessFun, ReadPhaseResult, FailureFun, Owner, TID)->
    %% BEGIN only for time measurements
    %% _ItemsListLength = length(Items),
    %% boot_logger:transaction_log(io_lib:format("| ~p | | | | | | ~p ", [TID, _ItemsListLength])),
    %% END only for time measurements
    TMState1 = init_leader_state(TID, Items),
    {InitRes, TMState2} = tmanager:init_phase(TMState1),
    %% ?TLOGN("init phase: initres ~p", [InitRes]),
    case InitRes of
        ok ->
            erlang:send_after(config:read(tp_failure_timeout), self(),
                              {check_failed_tps}),
            {_TimeCP, TransRes} = util:tc(tmanager, start_commit, [TMState2]),
            ?TIMELOG("commit phase", _TimeCP/1000),
            %% ?TLOGN("Result of transaction: ~p", [TransRes]),
            case TransRes of
                commit ->
                    %% boot_logger:transaction_log(io_lib:format("| ~p | ~f | ~f |~f | commit | ~p", [TID, TimeRP/1000, TimeIP/1000, TimeCP/1000, ItemsListLength])),
                    %% io:format("| ~p | ~f | ~f |~f | commit | ~p~n", [TID, _TimeRP/1000, _TimeIP/1000, _TimeCP/1000, _ItemsListLength]),
                    tsend:send_to_client(Owner, SuccessFun({commit, ReadPhaseResult}));
                _ ->
                    %% boot_logger:transaction_log(io_lib:format("| ~p | ~f | ~f |~f | abort | ~p", [TID, TimeRP/1000, TimeIP/1000, TimeCP/1000, ItemsListLength])),
                    tsend:send_to_client(Owner, FailureFun(abort))
            end;
        _ ->
            ?TLOGN("Init Phase Failed ~p", [InitRes]),
            tsend:send_to_client(Owner, FailureFun(abort))
    end.

init_leader_state(TID, Items) ->
    {SelfKey, _TS} = TID,
    LeaderBallot = 2,
    TMState = trecords:new_tm_state(TID, Items, cs_send:this(),
                                    {SelfKey, cs_send:this(),
                                     LeaderBallot}),
    TMState#tm_state{myBallot=LeaderBallot}.

%% Init Phase, lookup all transaction participants and replicated managers
init_phase(TMState) ->
    TMMessage = {init_rtm,
                 trecords:new_tm_message(TMState#tm_state.transID,
                                         {cs_send:this(),
                                          TMState#tm_state.items})
                },
    tsend:send_to_rtms_with_lookup(TMState#tm_state.transID, TMMessage),

    TPMessage = {lookup_tp,
                 #tp_message{item_key = unknown,
                             message={cs_send:this()}
                            }
                },
    tsend:send_to_participants_with_lookup(TMState, TPMessage),
    erlang:send_after(config:read(transaction_lookup_timeout), self(),
                      {rtm_lookup_timeout}),
    receive_lookup_rtms_tps_repl(TMState).

receive_lookup_rtms_tps_repl(TMState)->
    receive
        {rtm, Address, RKey}->
            %% ?TLOGN("rtm for key ~p at ~p", [RKey, node(Address)]),
            Limit = config:read(replication_factor),
            NumRTMs = TMState#tm_state.rtms_found + 1,
%            io:format("rtms ~p ~n", [NumRTMs]),
            Ballot = NumRTMs + 1,
            TMState1 = TMState#tm_state
                         {rtms_found = NumRTMs,
                          rtms = [{RKey, Address, Ballot}
                                  | TMState#tm_state.rtms]},
            if (NumRTMs >= Limit)
               and (TMState1#tm_state.tps_found >= TMState1#tm_state.numitems)
               -> {ok, TMState1};
               true -> receive_lookup_rtms_tps_repl(TMState1)
            end;
        {tp, ItemKey, OrigKey, Address} ->
            %% io:format("tp ~n"),
            %% ?TLOGN("tp for key ~p at ~p ", [ItemKey, node(Address)]),
            Limit = config:read(replication_factor),
            TMState1 = add_tp(TMState, ItemKey, OrigKey, Address, Limit),
            if (TMState1#tm_state.tps_found >= TMState1#tm_state.numitems)
               and (TMState1#tm_state.rtms_found >= Limit)
               -> {ok, TMState1};
               true -> receive_lookup_rtms_tps_repl(TMState1)
            end;
        {rtm_lookup_timeout} ->
%            io:format("lookup timeout ~n"),
            Limit = config:read(quorum_factor),
            if (TMState#tm_state.tps_found >= TMState#tm_state.numitems)
               and (TMState#tm_state.rtms_found >= Limit)
               -> {ok, TMState};
               true ->
                    ?TLOGN("Found not enough RTMs and TPs~n", []),
                    tsend:send_to_rtms(TMState, {kill}),
                    {timeout, TMState}
            end
    end.

%% add a tp to the TMState
add_tp(TMState, ItemKey, OriginalKey, Address, Limit) ->
    %OriginalKey = ?RT:get_original_key(ItemKey),
    Item = trecords:items_get_item_by_key(TMState#tm_state.items, OriginalKey),
    NewTPs = [{ItemKey, Address} | Item#tm_item.tps],
    NewItem = Item#tm_item
                {tps = NewTPs,
                 tps_found = Item#tm_item.tps_found + 1},
    NewTPsReady = if (NewItem#tm_item.tps_found == Limit)
                     -> TMState#tm_state.tps_found + 1;
                     true -> TMState#tm_state.tps_found
                  end,
    TMState#tm_state{items =
                     trecords:items_update_item(TMState#tm_state.items,
                                                Item, NewItem),
                     tps_found = NewTPsReady}.

start_commit(TMState)->
    tsend:tell_rtms(TMState),
    [ tsend:send_prepare_item(TMState, Item)
      || Item <- TMState#tm_state.items ],
    loop(TMState).

start_replicated_manager(Message, InstanceId)->
    {Leader, Items} = Message#tm_message.message,
    RKey = Message#tm_message.tm_key,
    TransID = Message#tm_message.transaction_id,
    erlang:put(instance_id, InstanceId),
    cs_send:send(Leader, {rtm, cs_send:this(), RKey}),
    TMState = trecords:new_tm_state(TransID, Items, Leader,
                                    {RKey, cs_send:this(), unknownballot}),
    loop(TMState),
    % done: remove tid_tm_mapping.
    CSNodePid = process_dictionary:get_group_member(cs_node),
    CSNodePid ! {remove_tm_tid_mapping, TransID, cs_send:this()}.

loop(TMState)->
    receive
        {kill} ->
            %% Init Phase at the leader must have failed
            ?TLOGN("Got killed, an init phase must have failed", []),
            abort;
        {vote,_Sender, Vote} ->
            %% ?TLOGN("received vote ~p", [Vote]),
            TMState2 = tmanager_pac:process_vote(TMState, Vote),
            loop(TMState2);
        {vote_ack, Key, RKey, VoteDecision, Timestamp} ->
            %% ?TLOGN("received ack ~p", [RKey]),
            TMState2 = tmanager_pac:process_vote_ack(TMState, Key, RKey, VoteDecision, Timestamp),
            loop(TMState2);
        {read_vote, Vote} ->
            %% ?TLOGN("received read vote ~p", [Vote]),
            TMState2 = tmanager_pac:process_read_vote(TMState, Vote),
            loop(TMState2);
        {read_vote_ack, Key, RKey, Timestamp, StoredVote}->
            %% ?TLOGN("received read_vote_ack ~p", [RKey]),
            TMState2 = tmanager_pac:collect_rv_ack(TMState, Key, RKey, Timestamp, StoredVote),
            loop(TMState2);
        {rtms, RTMs, MyBallot} ->
            %% ?TLOGN("received rtms ~p", [RTMs]),
            TMState2 = TMState#tm_state{rtms = RTMs, myBallot = MyBallot},
            %% simple leader election
            if MyBallot > 2 % not the leader
               ->
                    erlang:send_after(time_become_leader(MyBallot), self(), {become_leader});
               true -> ok
            end,
            loop(TMState2);
        {check_failed_tps} ->
            %% ?TLOGN("received check_failed_tps", []),
            %% io:format("checking failed tps ~n", []),
            check_failed_tps(TMState),
            %%loop(TMState),
            abort;
        {decision, Decision} ->
            %%?TLOGN("received decision ~p", [Decision]),
            Decision;
        {rtm, Address, _RKey} ->
            %% late arrival of a RTM: kill it, it has nothing to do
            tsend:send(Address, {kill}),
            loop(TMState);
        {become_leader} ->
            ?TLOGN("I'm becoming a leader", []),
            tmanager_pac:vote_for_suspected(TMState),
            NewBal = TMState#tm_state.myBallot + config:read(replication_factor),
            erlang:send_after(time_become_leader(NewBal), self(), {become_leader}),
            loop(TMState#tm_state{myBallot = NewBal});
        _ ->
            %% io:format("TManager got unknown message ~p~n", [X]),
            %% ?TLOGN("unknown message ~p", [X]),
            loop(TMState)
    after config:read(tmanager_timeout)->
            if TMState#tm_state.rtms_found == 1
               -> ?TLOGN("Tmanager Timeout: in init phase", []),
                  %% loop(TMState);
                  ?TLOGN("Kill myself, an init phase must have failed", []),
                  abort;
               true ->
                    ?TLOGN("Tmanager Timeout: after init phase", []),
                    loop(TMState)
                    %% io:format("this should not happen! there is a transaction that did not get a decision~n", [])
                    %% loop(TMState)
            end
    end.


%%--------------------------------------------------------------------
%% Function: check_failed_tps/1 
%% Purpose:  check for which tps we have no decision for, start a read
%%               phase for the suspected TPs
%% Args: TMState - the state of the TM
%%--------------------------------------------------------------------

check_failed_tps(TMState)->
    %Vote abort for all TPs that seem to have failed
    tmanager_pac:vote_for_suspected(TMState),
    TMState.

time_become_leader(MyBallot)->
    (MyBallot - 2) * config:read(leader_detector_interval).
